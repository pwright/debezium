#!/usr/bin/env python3
"""
parse_adoc.py

Scan a directory (or list of file paths from stdin) for .adoc files,
compute file metadata and line-based metrics, record results
in a SQLite database, storing file paths relative to the scanned directory,
with separate tables for attributes, values_tbl, and their relationships to files.

Usage:
    # scan a directory
    ./parse_adoc.py /path/to/docs --db files.db --summary

    # read file list from stdin
    find . -name '*.adoc' | ./parse_adoc.py - --db files.db

Outputs:
  - SQLite DB (default: files.db) with tables:
    - files
    - attributes
    - values_tbl
    - file_attributes (file ↔ attribute)
    - attribute_values (attribute ↔ value)
  - If --summary, prints TSV summary of files

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
# SQLite schema: files, attributes, values_tbl, and join tables
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
CREATE TABLE IF NOT EXISTS attributes (
    attribute TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS values_tbl (
    value TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS file_attributes (
    path TEXT,
    attribute TEXT,
    PRIMARY KEY(path, attribute),
    FOREIGN KEY(path) REFERENCES files(path),
    FOREIGN KEY(attribute) REFERENCES attributes(attribute)
);
CREATE TABLE IF NOT EXISTS attribute_values (
    attribute TEXT,
    value TEXT,
    PRIMARY KEY(attribute, value),
    FOREIGN KEY(attribute) REFERENCES attributes(attribute),
    FOREIGN KEY(value) REFERENCES values_tbl(value)
);
"""

# -----------------------------------------------------------------------------
# Analyze a single .adoc file
# -----------------------------------------------------------------------------

def analyze_file(path, base_dir):
    """
    Analyze file at 'path':
    - Compute metadata and line-based metrics
    - Extract attribute:value definitions

    Returns a dict with:
      path (relative), filename,
      created, modified, size,
      total_lines, alnum_start, special_start, comment_lines, definition_count,
      definitions_list: list of (attribute, value) tuples
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
    definitions = set()

    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for raw in f:
                total += 1
                line = raw.lstrip()
                if not line:
                    continue
                if line.startswith('//'):
                    comments += 1
                    continue
                if line.startswith(':'):
                    # parse attribute:value
                    rest = line[1:].strip()
                    parts = rest.split(':', 1)
                    attribute = parts[0].strip()
                    value = parts[1].strip() if len(parts) > 1 else ''
                    if attribute:
                        definitions.add((attribute, value))
                    continue
                first = line[0]
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
        'definition_count': len(definitions),
        'definitions_list': list(definitions)
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
# Insert into SQLite: files, attributes, values_tbl, and joins
# -----------------------------------------------------------------------------

def write_to_db(db_path, records):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(SCHEMA)

    insert_file = (
        "INSERT OR REPLACE INTO files"
        " (path,filename,created,modified,size,total_lines,alnum_start,special_start,"
        "comment_lines,definition_count) VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    insert_attr = "INSERT OR IGNORE INTO attributes(attribute) VALUES (?)"
    insert_val = "INSERT OR IGNORE INTO values_tbl(value) VALUES (?)"
    insert_file_attr = "INSERT OR IGNORE INTO file_attributes(path,attribute) VALUES (?,?)"
    insert_attr_val = "INSERT OR IGNORE INTO attribute_values(attribute,value) VALUES (?,?)"

    for r in records:
        # files
        cur.execute(insert_file, (
            r['path'], r['filename'], r['created'], r['modified'], r['size'],
            r['total_lines'], r['alnum_start'], r['special_start'],
            r['comment_lines'], r['definition_count']
        ))
        # definitions → attributes & values_tbl
        for attr, val in r['definitions_list']:
            cur.execute(insert_attr, (attr,))
            cur.execute(insert_val, (val,))
            cur.execute(insert_file_attr, (r['path'], attr))
            cur.execute(insert_attr_val, (attr, val))

    conn.commit()
    conn.close()
    return len(records)

# -----------------------------------------------------------------------------
# Main CLI entrypoint
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Index .adoc files into SQLite with attributes and values"
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
