"""
Scan ALL files: sample 5 lines, detect delimiter, find email/name columns, output a mapping plan.
Run: python email_processor/scan_all_files.py
"""
import csv
import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, SUPPORTED_EXTENSIONS, SKIP_PATTERNS, EXCLUDE_FILE_PATTERNS

def detect_delimiter(sample_lines: List[str]) -> str:
    """Detect delimiter from sample lines."""
    if not sample_lines:
        return ','
    # Count potential delimiters in joined sample
    text = '\n'.join(sample_lines)
    counts = {
        ',': text.count(','),
        '\t': text.count('\t'),
        '|': text.count('|'),
        ';': text.count(';'),
    }
    # Pick most common
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ','

def parse_line(line: str, delimiter: str) -> List[str]:
    """Parse a line respecting quotes."""
    try:
        return next(csv.reader([line], delimiter=delimiter, quotechar='"'))
    except:
        return line.split(delimiter)

def find_email_column(rows: List[List[str]]) -> Tuple[int, str]:
    """Find which column index contains emails. Returns (index, sample_value)."""
    for row in rows:
        for i, val in enumerate(row):
            val = val.strip().strip('"')
            if '@' in val and '.' in val.split('@')[-1]:
                return i, val
    return -1, ''

def find_name_columns(rows: List[List[str]], header: List[str]) -> Dict[str, int]:
    """Guess first_name and last_name column indices from header or content."""
    mapping = {}
    header_lower = [h.lower().strip().strip('"') for h in header] if header else []
    
    # Check header for known names
    name_patterns = {
        'first_name': ['first_name', 'firstname', 'first name', 'fname', 'first'],
        'last_name': ['last_name', 'lastname', 'last name', 'lname', 'last'],
        'email': ['email', 'e-mail', 'email_address', 'emailaddress'],
        'phone': ['phone', 'telephone', 'mobile', 'cell'],
        'address': ['address', 'street', 'address1'],
        'city': ['city', 'town'],
        'state': ['state', 'st', 'province'],
        'zip': ['zip', 'zipcode', 'postal', 'zip_code'],
    }
    
    for field, patterns in name_patterns.items():
        for i, h in enumerate(header_lower):
            if any(p == h or p in h for p in patterns):
                mapping[field] = i
                break
    
    return mapping

def analyze_file(path: Path) -> Dict[str, Any]:
    """Analyze a single file and return its format info."""
    result = {
        'path': str(path),
        'filename': path.name,
        'size_mb': round(path.stat().st_size / 1e6, 2),
        'sample_lines': [],
        'delimiter': ',',
        'has_header': False,
        'email_col': -1,
        'email_sample': '',
        'column_mapping': {},
        'total_columns': 0,
        'error': None,
    }
    
    try:
        # Read first 10 lines
        lines = []
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            for i, line in enumerate(f):
                if i >= 10:
                    break
                line = line.strip()
                if line:
                    lines.append(line)
        
        if not lines:
            result['error'] = 'Empty file'
            return result
        
        result['sample_lines'] = lines[:5]
        
        # Detect delimiter
        delim = detect_delimiter(lines)
        result['delimiter'] = repr(delim)
        
        # Parse lines
        parsed = [parse_line(l, delim) for l in lines]
        
        # Check if first row is header (no @ in it, or has "email" text)
        first_row = parsed[0] if parsed else []
        first_lower = ' '.join(first_row).lower()
        has_header = 'email' in first_lower or not any('@' in v for v in first_row)
        result['has_header'] = has_header
        
        header = first_row if has_header else [f'col_{i}' for i in range(len(first_row))]
        data_rows = parsed[1:] if has_header else parsed
        
        result['total_columns'] = len(header)
        
        # Find email column
        email_idx, email_sample = find_email_column(data_rows if data_rows else parsed)
        result['email_col'] = email_idx
        result['email_sample'] = email_sample
        
        # Find name columns from header
        mapping = find_name_columns(data_rows, header)
        if email_idx >= 0:
            mapping['email'] = email_idx
        result['column_mapping'] = mapping
        result['header'] = header[:15]  # First 15 column names
        
    except Exception as e:
        result['error'] = str(e)[:100]
    
    return result

def main():
    print("=" * 80)
    print("  SCANNING ALL DATA FILES")
    print("=" * 80)
    
    # Find all files
    all_files = []
    for root, dirs, files in os.walk(BASE_DIR):
        dirs[:] = [d for d in dirs if d not in SKIP_PATTERNS and not d.startswith('.')]
        for f in files:
            path = Path(root) / f
            if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue
            if any(skip in str(path).lower() for skip in SKIP_PATTERNS):
                continue
            if any(p in f.lower() for p in EXCLUDE_FILE_PATTERNS):
                continue
            all_files.append(path)
    
    print(f"\nFound {len(all_files)} data files to analyze.\n")
    
    # Analyze each
    results = []
    for i, path in enumerate(all_files):
        print(f"[{i+1}/{len(all_files)}] {path.name[:50]}...", end=" ", flush=True)
        info = analyze_file(path)
        results.append(info)
        if info['error']:
            print(f"ERROR: {info['error']}")
        elif info['email_col'] < 0:
            print(f"NO EMAIL FOUND - delim={info['delimiter']} cols={info['total_columns']}")
        else:
            print(f"OK - delim={info['delimiter']} email_col={info['email_col']} cols={info['total_columns']}")
    
    # Summary
    print("\n" + "=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    
    no_email = [r for r in results if r['email_col'] < 0 and not r['error']]
    errors = [r for r in results if r['error']]
    ok = [r for r in results if r['email_col'] >= 0]
    
    print(f"\n  Files with email found: {len(ok)}")
    print(f"  Files with NO email found: {len(no_email)}")
    print(f"  Files with errors: {len(errors)}")
    
    # Show details of problem files
    if no_email:
        print("\n" + "-" * 40)
        print("  FILES WITH NO EMAIL DETECTED:")
        print("-" * 40)
        for r in no_email[:20]:
            print(f"\n  {r['filename']}")
            print(f"    Size: {r['size_mb']} MB, Delimiter: {r['delimiter']}, Columns: {r['total_columns']}")
            print(f"    Header: {r.get('header', [])[:8]}")
            print(f"    Sample lines:")
            for line in r['sample_lines'][:3]:
                print(f"      {line[:100]}...")
    
    # Show delimiter distribution
    delim_counts = {}
    for r in results:
        d = r['delimiter']
        delim_counts[d] = delim_counts.get(d, 0) + 1
    print("\n  Delimiter distribution:")
    for d, c in sorted(delim_counts.items(), key=lambda x: -x[1]):
        print(f"    {d}: {c} files")
    
    # Show email column distribution
    col_counts = {}
    for r in ok:
        c = r['email_col']
        col_counts[c] = col_counts.get(c, 0) + 1
    print("\n  Email column index distribution (0-indexed):")
    for c, cnt in sorted(col_counts.items(), key=lambda x: -x[1]):
        print(f"    Column {c}: {cnt} files")
    
    # Write full report
    report_path = BASE_DIR / 'output' / 'file_scan_report.txt'
    report_path.parent.mkdir(exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        for r in results:
            f.write(f"\n{'='*60}\n")
            f.write(f"FILE: {r['filename']}\n")
            f.write(f"Path: {r['path']}\n")
            f.write(f"Size: {r['size_mb']} MB\n")
            f.write(f"Delimiter: {r['delimiter']}\n")
            f.write(f"Has header: {r['has_header']}\n")
            f.write(f"Total columns: {r['total_columns']}\n")
            f.write(f"Email column: {r['email_col']}\n")
            f.write(f"Email sample: {r['email_sample']}\n")
            f.write(f"Column mapping: {r['column_mapping']}\n")
            f.write(f"Header: {r.get('header', [])}\n")
            f.write(f"Error: {r['error']}\n")
            f.write("Sample lines:\n")
            for line in r['sample_lines']:
                f.write(f"  {line[:200]}\n")
    
    print(f"\n  Full report written to: {report_path}")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
