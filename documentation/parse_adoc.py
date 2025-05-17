#!/usr/bin/env python3
"""
parse_adoc.py

Scan a directory (or list of file paths from stdin) for .adoc files,
compute file metadata and line-based metrics, and record each result
in a SQLite database, storing file paths relative to the scanned directory.

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
# Schema creation
# -----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    created TIMESTAMP,
    modified TIMESTAMP,
    size INTEGER,
    total_lines    INTEGER,
    alnum_start    INTEGER,
    special_start  INTEGER,
    comment_lines  INTEGER,
    definition_lines INTEGER
);
"""

# -----------------------------------------------------------------------------
# Analyze a single file: return dict of metrics
# -----------------------------------------------------------------------------

def analyze_file(path, base_dir):
    """
    Read the file at `path` and compute:
      - created, modified, size
      - total_lines
      - alnum_start
      - special_start (non-alnum, excluding ':' and '/')
      - comment_lines (start with '//')
      - definition_lines (start with ':')

    Store `path` relative to `base_dir`.

    Returns a dict ready for DB insertion.
    """
    try:
        st = os.stat(path)
    except Exception as e:
        logging.error(f"Cannot stat {path!r}: {e}")
        return None

    created = datetime.fromtimestamp(st.st_ctime).isoformat()
    modified = datetime.fromtimestamp(st.st_mtime).isoformat()
    size = st.st_size

    total = alnum = special = comments = defs = 0

    try:
        with open(path, 'r', encoding='utf-8') as f:
            for raw in f:
                total += 1
                line = raw.lstrip()
                if not line:
                    continue
                # comment?
                if line.startswith('//'):
                    comments += 1
                    continue
                # definition?
                if line.startswith(':'):
                    defs += 1
                    continue
                first = line[0]
                if first.isalnum():
                    alnum += 1
                else:
                    special += 1
    except Exception as e:
        logging.error(f"Error reading {path!r}: {e}")
        return None

    rel_path = os.path.relpath(path, base_dir)

    return {
        'path': rel_path,
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
# Process a list of files (optionally in parallel)
# -----------------------------------------------------------------------------

def process_files(paths, base_dir, workers=4):
    """
    Given an iterable of file paths, analyze them (in a thread pool)
    and yield only the successful result dicts.
    """
    with ThreadPoolExecutor(max_workers=workers) as exe:
        for result in exe.map(lambda p: analyze_file(p, base_dir), paths):
            if result:
                yield result

# -----------------------------------------------------------------------------
# SQLite insertion
# -----------------------------------------------------------------------------

def write_to_db(db_path, records):
    """
    Given an iterable of record dicts, insert or replace them into the DB.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(SCHEMA)
    to_insert = [tuple(r[k] for k in (
        'path','created','modified','size',
        'total_lines','alnum_start','special_start',
        'comment_lines','definition_lines'
    )) for r in records]
    cur.executemany("""
        INSERT OR REPLACE INTO files
        (path,created,modified,size,total_lines,alnum_start,
         special_start,comment_lines,definition_lines)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, to_insert)
    conn.commit()
    conn.close()
    return len(to_insert)

# -----------------------------------------------------------------------------
# Main CLI
# -----------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Index .adoc files into SQLite (relative paths)")
    p.add_argument('directory', nargs='?', default=None,
                   help="Directory to scan for .adoc, or '-' to read file paths from stdin")
    p.add_argument('--db', '-o', default='files.db',
                   help="Output SQLite DB (default: files.db)")
    p.add_argument('--summary', action='store_true',
                   help="Print TSV summary to stdout after indexing")
    p.add_argument('--workers', type=int, default=4,
                   help="Number of worker threads (default: 4)")
    args = p.parse_args()

    # Determine base directory and file list
    if args.directory == '-':
        base_dir = os.getcwd()
        files = (line.strip() for line in sys.stdin if line.strip())
    elif args.directory:
        if not os.path.isdir(args.directory):
            logging.error(f"Not a directory: {args.directory!r}")
            sys.exit(1)
        base_dir = args.directory
        files = (
            os.path.join(root, fn)
            for root,_,fns in os.walk(args.directory)
            for fn in fns if fn.lower().endswith('.adoc')
        )
    else:
        p.print_usage()
        sys.exit(1)

    logging.info(f"Scanning directory: {base_dir}")
    logging.info("Starting analysis…")
    records = list(process_files(files, base_dir, workers=args.workers))
    logging.info(f"Analyzed {len(records)} files.")

    count = write_to_db(args.db, records)
    logging.info(f"Wrote {count} records to {args.db!r}")

    if args.summary:
        out = sys.stdout
        headers = [
            "path","created","modified","size",
            "total","alnum","special","comments","defs"
        ]
        out.write("\t".join(headers) + "\n")
        for r in records:
            row = [
                r['path'], r['created'], r['modified'],
                str(r['size']), str(r['total_lines']),
                str(r['alnum_start']), str(r['special_start']),
                str(r['comment_lines']), str(r['definition_lines'])
            ]
            out.write("\t".join(row) + "\n")

if __name__ == '__main__':
    main()
