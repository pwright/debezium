#!/usr/bin/env python3
"""
parse_adoc.py

Scan a directory (or list of file paths from stdin) for .adoc files,
compute file metadata and line-based metrics, and record each result
in a SQLite database, storing file paths relative to the scanned directory
and the filename separately.

Usage:
    # scan a directory
    ./parse_adoc.py /path/to/docs --db files.db --summary

    # read file list from stdin
    find . -name '*.adoc' | ./parse_adoc.py - --db files.db

Outputs:
  - SQLite DB (default: files.db) with table 'files'
  - If --summary, prints tab-separated summary lines to stdout

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
# Configuration of logging: all logs to stderr
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)

# -----------------------------------------------------------------------------
# SQLite schema for file metadata
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
    definition_lines INTEGER
);
"""

# -----------------------------------------------------------------------------
# Analyze a single .adoc file
# -----------------------------------------------------------------------------

def analyze_file(path, base_dir):
    """
    Analyze file at 'path' and compute metadata and line counts.
    Returns dict with 'path' (relative to base_dir), 'filename', and metrics.
    """
    try:
        st = os.stat(path)
    except OSError as e:
        logging.error(f"Cannot stat {path!r}: {e}")
        return None

    created = datetime.fromtimestamp(st.st_ctime).isoformat()
    modified = datetime.fromtimestamp(st.st_mtime).isoformat()
    size = st.st_size

    total = alnum = special = comments = defs = 0

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
                    defs += 1
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
        'definition_lines': defs
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
# Insert records into SQLite
# -----------------------------------------------------------------------------

def write_to_db(db_path, records):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(SCHEMA)
    insert_q = (
        "INSERT OR REPLACE INTO files"
        " (path,filename,created,modified,size,total_lines,alnum_start,special_start,"
        "comment_lines,definition_lines) VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    to_insert = [(
        r['path'], r['filename'], r['created'], r['modified'], r['size'],
        r['total_lines'], r['alnum_start'], r['special_start'],
        r['comment_lines'], r['definition_lines']
    ) for r in records]
    cur.executemany(insert_q, to_insert)
    conn.commit()
    conn.close()
    return len(to_insert)

# -----------------------------------------------------------------------------
# Main CLI entrypoint
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Index .adoc files into SQLite with relative paths"
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
        help="Print TSV summary to stdout"
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
    elif args.directory:
        base_dir = os.path.abspath(args.directory)
        if not os.path.isdir(base_dir):
            logging.error(f"Not a directory: {base_dir!r}")
            sys.exit(1)
        files = (
            os.path.join(root, name)
            for root, _, names in os.walk(base_dir)
            for name in names if name.lower().endswith('.adoc')
        )
    else:
        parser.print_usage()
        sys.exit(1)

    logging.info(f"Base directory: {base_dir}")
    logging.info("Starting analysis…")
    records = list(process_files(files, base_dir, workers=args.workers))
    logging.info(f"Analyzed {len(records)} files.")

    count = write_to_db(args.db, records)
    logging.info(f"Wrote {count} records to {args.db!r}")

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
                str(r['comment_lines']), str(r['definition_lines'])
            ]
            print("\t".join(row))

if __name__ == '__main__':
    main()
