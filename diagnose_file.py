"""
Diagnose file parsing step by step.
Usage: python email_processor/diagnose_file.py <filepath>
"""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from parser import read_file_flexible_csv, detect_encoding, detect_delimiter, transform_dataframe, detect_schema
from cleaner import clean_and_validate_records

def diagnose(filepath):
    path = Path(filepath)
    print(f"File: {path}")
    print(f"Size: {path.stat().st_size / 1e6:.1f} MB")

    encoding = detect_encoding(path)
    delimiter = detect_delimiter(path, encoding)
    print(f"Encoding: {encoding}, Delimiter: {repr(delimiter)}")

    # 1) Count raw lines and check email column
    print("\nStep 1: Raw file scan (first 10k rows)...")
    with open(path, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        rows_with_email = 0
        total = 0
        col2_samples = []
        for i, row in enumerate(reader):
            if i >= 10000:
                break
            total += 1
            # Check column 2 for email
            if len(row) > 2:
                val = row[2].strip()
                if i < 5:
                    col2_samples.append(val)
                if val and "@" in val:
                    rows_with_email += 1
    print(f"  Rows scanned: {total}")
    print(f"  Rows with email in col 2: {rows_with_email}")
    print(f"  Rows without email in col 2: {total - rows_with_email}")
    print(f"  First 5 col[2] values: {col2_samples}")

    # 2) Our flexible CSV reader
    print("\nStep 2: Flexible CSV reader...")
    df = read_file_flexible_csv(path, encoding, delimiter)
    if df is None:
        print("  ERROR: read_file_flexible_csv returned None")
        return
    print(f"  Total rows read: {len(df)}")
    cols = list(df.columns)
    print(f"  Columns ({len(cols)}): {cols[:15]}...")
    
    # Check which column has emails
    email_col_found = None
    for col in cols:
        sample = df[col].head(100).dropna().astype(str)
        with_at = sample.str.contains("@", na=False).sum()
        if with_at > 10:
            print(f"  Column '{col}' has {with_at}/100 sample rows with @")
            email_col_found = col
            break
    
    if email_col_found is None:
        print("  WARNING: No column with significant @ content found!")

    # 3) Schema detection
    print("\nStep 3: Schema detection...")
    schema_type, mapping = detect_schema(df)
    print(f"  Schema type: {schema_type}")
    print(f"  Column mapping: {mapping}")

    # 4) Transform (first 10k)
    print("\nStep 4: Transform to records (first 10k rows)...")
    df_sample = df.head(10000)
    file_info = {
        "filename": path.name,
        "path": path,
        "data_source": "test",
        "is_clicker": False,
        "is_opener": False,
        "is_validated": False,
        "modified_date": None,
    }
    records = transform_dataframe(df_sample, file_info)
    with_email = sum(1 for r in records if r.get("email"))
    print(f"  Records created: {len(records)}")
    print(f"  Records with email: {with_email}")
    print(f"  Records without email: {len(records) - with_email}")

    # Show sample records
    print("\n  Sample records (first 5):")
    for r in records[:5]:
        print(f"    email={r.get('email')}, first_name={r.get('first_name')}, last_name={r.get('last_name')}")

    # 5) Clean and validate
    print("\nStep 5: Clean and validate...")
    valid, rejection = clean_and_validate_records(records)
    print(f"  Valid records: {len(valid)}")
    print(f"  Rejections: {rejection}")

    print("\n  Sample valid records (first 5):")
    for r in valid[:5]:
        print(f"    email={r.get('email')}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default to 1Million file
        diagnose("glenndata/1Million-AutoLeads-3500Type-RR-03-10-22.csv")
    else:
        diagnose(sys.argv[1])
