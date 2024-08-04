#!/usr/bin/env python3

import sys
import re
import subprocess
import argparse
import csv
from collections import defaultdict
from bs4 import BeautifulSoup

# Enhanced list of AsciiDoc constructs and their regex patterns
ASCIIDOC_CONSTRUCTS = {
    "headings": r"^(={1,6})\s+",
    "bulleted_list": r"^(?:\*|-|\+)\s+",
    "ordered_list": r"^\d+\.\s+",
    "blockquote": r"^_{3,}",
    "literal_block": r"^(?:\+\+\+|----)$",
    "fenced_block": r"^```",
    "comment_block": r"^/{4,} ",
    "table": r"^\|===|^\.\|",
    "admonition": r"^(?:NOTE|TIP|IMPORTANT|CAUTION|WARNING):\s+",
    "example_block": r"^====$",
    "sidebar_block": r"^\*{4}$",
    "listing_block": r"^----$",
    "verse_block": r"^\[verse\]",
    "inline_bold": r"\*\*(.*?)\*\*",
    "inline_italic": r"__(.*?)__",
    "inline_monospace": r"`(.+?)`",
    "inline_code": r"`(.+?)`",
    "inline_subscript": r"~(.+?)~",
    "inline_superscript": r"\^(.+?)\^",
    "inline_underline": r"\+\+(.*?)\+\+",
    "inline_link": r"https?://[^\s]+",
    "inline_image": r"image::[^\[]+\[.*\]",
    "inline_macro": r"\{[a-zA-Z0-9_]+\}",
    "inline_attributes": r"^\:.*\:",
    "checklist": r"^\* \[.\] ",
    "description_list": r"^\S.*::$",
    "attribute_definitions": r"^\s*:(\w+):\s*",
    "cross_reference": r"<<.+?>>",
    "anchor": r"\[\[(.*?)\]\]",
    "inline_quote": r"^_{3,}",
    "literal_paragraph": r"^\+\+$",
}

# Corresponding HTML constructs
HTML_CONSTRUCTS = {
    "headings": r"^h[1-6]$",
    "bulleted_list": r"^ul$",
    "ordered_list": r"^ol$",
    "blockquote": r"^blockquote$",
    "literal_block": r"^pre$",
    "fenced_block": r"^code$",
    "comment_block": r"<!--",
    "table": r"^table$",
    "admonition": r"^div.admonitionblock",
    "example_block": r"^div.exampleblock",
    "sidebar_block": r"^div.sidebarblock",
    "listing_block": r"^pre$",
    "verse_block": r"^div.verseblock",
    "inline_bold": r"^strong$",
    "inline_italic": r"^em$",
    "inline_monospace": r"^code$",
    "inline_code": r"^code$",
    "inline_subscript": r"sub",
    "inline_superscript": r"sup",
    "inline_underline": r"u",
    "inline_link": r"a",
    "inline_image": r"img",
    "inline_macro": r"span",
    "inline_attributes": r"^span$",
    "checklist": r"^ul.checklist$",
    "description_list": r"^dl$",
    "attribute_definitions": r"meta",
    "cross_reference": r"a.xref",
    "anchor": r"a.anchor",
    "inline_quote": r"q",
    "literal_paragraph": r"p.literalblock",
}

def log_construct(filename, line_no, construct):
    """Log the found construct with its file, line number, and type."""
    print(f"Found {construct} in {filename} at line {line_no}", file=sys.stderr)

def parse_asciidoc(content, filename):
    """Parse AsciiDoc content and count occurrences of different constructs, logging each found construct."""
    stats = defaultdict(int)
    
    for line_no, line in enumerate(content.splitlines(), start=1):
        for construct, pattern in ASCIIDOC_CONSTRUCTS.items():
            if re.search(pattern, line):
                stats[construct] += 1
                log_construct(filename, line_no, construct)
    
    return stats

def convert_asciidoc_to_html(input_source):
    """Convert AsciiDoc content to HTML using asciidoctor."""
    try:
        if input_source == '-':
            content = sys.stdin.read()
            filename = "stdin"
        else:
            with open(input_source, 'r', encoding='utf-8') as file:
                content = file.read()
            filename = input_source

        # Use subprocess to call asciidoctor
        process = subprocess.run(['asciidoctor', '-o', '-', '-'], input=content, text=True, capture_output=True)
        if process.returncode != 0:
            raise Exception(f"asciidoctor failed with error: {process.stderr}")

        return content, process.stdout, filename

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def parse_html(content):
    """Parse HTML content and count occurrences of different constructs."""
    stats = defaultdict(int)
    soup = BeautifulSoup(content, 'html.parser')

    for tag in soup.find_all(True):
        for construct, pattern in HTML_CONSTRUCTS.items():
            if re.match(pattern, tag.name):
                stats[construct] += 1
            elif construct == "admonition" and tag.get('class') and 'admonitionblock' in tag['class']:
                stats[construct] += 1
            elif construct == "checklist" and tag.get('class') and 'checklist' in tag['class']:
                stats[construct] += 1
            elif construct == "inline_link" and tag.name == "a" and tag.get('href'):
                stats[construct] += 1
            elif construct == "cross_reference" and tag.get('class') and 'xref' in tag['class']:
                stats[construct] += 1
            elif construct == "anchor" and tag.get('class') and 'anchor' in tag['class']:
                stats[construct] += 1
            elif construct == "literal_paragraph" and tag.get('class') and 'literalblock' in tag['class']:
                stats[construct] += 1

    return stats

def output_csv(asciidoc_stats, html_stats, output_file):
    """Output the stats to a CSV file or stdout."""
    fieldnames = ['AsciiDoc Construct', 'HTML Construct', 'AsciiDoc Count', 'HTML Count']

    with (open(output_file, 'w', newline='', encoding='utf-8') if output_file else sys.stdout) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for construct in set(asciidoc_stats.keys()).union(html_stats.keys()):
            writer.writerow({
                'AsciiDoc Construct': construct,
                'HTML Construct': HTML_CONSTRUCTS.get(construct, "N/A"),
                'AsciiDoc Count': asciidoc_stats.get(construct, 0),
                'HTML Count': html_stats.get(construct, 0)
            })

def main(input_source, output_file=None):
    """Main function to parse AsciiDoc, convert to HTML, parse the HTML, and output the statistics."""
    try:
        print("Parsing AsciiDoc content...", file=sys.stderr)
        content, html_content, filename = convert_asciidoc_to_html(input_source)
        asciidoc_stats = parse_asciidoc(content, filename)
        print("AsciiDoc parsing complete.", file=sys.stderr)

        print("Parsing HTML content...", file=sys.stderr)
        html_stats = parse_html(html_content)
        print("HTML parsing complete.", file=sys.stderr)

        output_csv(asciidoc_stats, html_stats, output_file)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse AsciiDoc, convert to HTML, and generate statistics for both constructs.")
    parser.add_argument('input_source', help="Input file or '-' for stdin.")
    parser.add_argument('-o', '--output', help="Output file to save the results in CSV format, otherwise output to stdout.")
    
    args = parser.parse_args()
    
    main(args.input_source, args.output)

