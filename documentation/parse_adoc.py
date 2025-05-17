#!/usr/bin/env python3
"""
parse_adoc.py

Scan a directory (or list of file paths from stdin) for .adoc files,
compute file metadata and line-based metrics, record each result
in a SQLite database, storing file paths relative to the scanned directory,
with a separate definitions table and a join table for many-to-many mapping
between files and unique definition lines.

Usage:
    # scan a directory
    ./parse_adoc.py /path/to/docs --db files.db --summary

    # read file list from stdin
    find . -name '*.adoc' | ./parse_adoc.py - --db files.db

Outputs:
  - SQLite DB (default: files.db) with tables 'files', 'definitions', 'file_definitions'
  - If --summary, prints tab-separated summary of files to stdout

Errors and warnings go to stderr so they won't corrupt your data stream.
"""

import sys
import os
import argparse
import sqlite3
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# -----------------------------------------------------------------------------
# Logging config: send to stderr
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)

# -----------------------------------------------------------------------------
# SQLite schema: files, definitions, join table
# -----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    filename TEXT,
    created TIMESTAMP,
    modified TIMESTAMP,
    size INTEGER,
    total_lines INTEGER,
    alnum_start INTEGER,
    special_start INTEGER,
    comment_lines INTEGER,
    definition_count INTEGER
);
CREATE TABLE IF NOT EXISTS definitions (
    definition TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS file_definitions (
    path TEXT,
    definition TEXT,
    PRIMARY KEY(path, definition),
    FOREIGN KEY(path) REFERENCES files(path),
    FOREIGN KEY(definition) REFERENCES definitions(definition)
);
"""

# -----------------------------------------------------------------------------
# Analyze a single .adoc file
# -----------------------------------------------------------------------------

def analyze_file(path, base_dir):
    """
    Analyze file at 'path', compute metadata and line counts, collect definition lines.
    Returns dict with keys:
      - path (relative to base_dir)
      - filename
      - created, modified, size, total_lines, alnum_start, special_start, comment_lines, definition_count
      - definitions_list: list of unique definition lines (with leading ':' stripped)
    """
    try:
        st = os.stat(path)
    except OSError as e:
        logging.error(f"Cannot stat {path!r}: {e}")
        return None

    created = datetime.fromtimestamp(st.st_ctime).isoformat()
    modified = datetime.fromtimestamp(st.st_mtime).isoformat()
    size = st.st_size

    total = alnum = special = comments = 0
    definitions_set = set()

    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for raw in f:
                total += 1
                stripped = raw.lstrip()
                if not stripped:
                    continue
                if stripped.startswith('//'):
                    comments += 1
                elif stripped.startswith(':'):
                    # Definition line: strip leading ':' and whitespace
                    definition = stripped[1:].strip()
                    if definition:
                        definitions_set.add(definition)
                else:
                    first = stripped[0]
                    if first.isalnum():
                        alnum += 1
                    else:
                        special += 1
    except Exception as e:
        logging.error(f"Error reading {path!r}: {e}")
        return None

    # Compute relative path
    abs_base = os.path.abspath(base_dir)
    abs_path = os.path.abspath(path)
    if abs_path.startswith(abs_base + os.sep):
        rel = abs_path[len(abs_base) + 1:]
    else:
        rel = os.path.basename(abs_path)
    filename = os.path.basename(rel)

    return {
        'path': rel,
        'filename': filename,
        'created': created,
        'modified': modified,
        'size': size,
        'total_lines': total,
        'alnum_start': alnum,
        'special_start': special,
        'comment_lines': comments,
        'definition_count': len(definitions_set),
        'definitions_list': list(definitions_set)
    }

# -----------------------------------------------------------------------------
# Process files in parallel
# -----------------------------------------------------------------------------

def process_files(paths, base_dir, workers=4):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(lambda p: analyze_file(p, base_dir), paths):
            if result:
                yield result

# -----------------------------------------------------------------------------
# Insert into SQLite: files, definitions, and join table
# -----------------------------------------------------------------------------

def write_to_db(db_path, records):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(SCHEMA)

    # Prepare insert statements
    insert_file = (
        "INSERT OR REPLACE INTO files"
        " (path,filename,created,modified,size,total_lines,alnum_start,special_start,"
        "comment_lines,definition_count)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    insert_def = "INSERT OR IGNORE INTO definitions(definition) VALUES (?)"
    insert_join = "INSERT OR IGNORE INTO file_definitions(path,definition) VALUES (?,?)"

    # Insert each record and its definitions
    for r in records:
        cur.execute(insert_file, (
            r['path'], r['filename'], r['created'], r['modified'], r['size'],
            r['total_lines'], r['alnum_start'], r['special_start'],
            r['comment_lines'], r['definition_count']
        ))
        # Definitions
        for definition in r['definitions_list']:
            cur.execute(insert_def, (definition,))
            cur.execute(insert_join, (r['path'], definition))

    conn.commit()
    conn.close()
    return len(records)

# -----------------------------------------------------------------------------
# Main CLI entrypoint
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Index .adoc files into SQLite with relative paths and definitions"
    )
    parser.add_argument(
        'directory', nargs='?', default=None,
        help="Directory to scan for .adoc, or '-' to read file paths from stdin"
    )
    parser.add_argument(
        '--db', '-o', default='files.db',
        help="SQLite DB path (default: files.db)"
    )
    parser.add_argument(
        '--summary', action='store_true',
        help="Print TSV summary of files to stdout"
    )
    parser.add_argument(
        '--workers', type=int, default=4,
        help="Number of parallel workers (default: 4)"
    )
    args = parser.parse_args()

    # Determine base_dir and file list
    if args.directory == '-':
        base_dir = os.path.abspath(os.getcwd())
        files = (line.strip() for line in sys.stdin if line.strip())
    else:
        base_dir = os.path.abspath(args.directory) if args.directory else None
        if not base_dir or not os.path.isdir(base_dir):
            parser.print_usage()
            sys.exit(1)
        files = (
            os.path.join(root, name)
            for root, _, names in os.walk(base_dir)
            for name in names if name.lower().endswith('.adoc')
        )

    logging.info(f"Base directory: {base_dir}")
    logging.info("Starting analysis…")
    records = list(process_files(files, base_dir, workers=args.workers))
    logging.info(f"Analyzed {len(records)} files.")

    count = write_to_db(args.db, records)
    logging.info(f"Wrote metadata for {count} files to {args.db!r}")

    if args.summary:
        cols = [
            "path","filename","created","modified","size",
            "total","alnum","special","comments","defs"
        ]
        print("\t".join(cols))
        for r in records:
            row = [
                r['path'], r['filename'], r['created'], r['modified'],
                str(r['size']), str(r['total_lines']),
                str(r['alnum_start']), str(r['special_start']),
                str(r['comment_lines']), str(r['definition_count'])
            ]
            print("\t".join(row))

if __name__ == '__main__':
    main()
