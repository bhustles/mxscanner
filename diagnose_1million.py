"""
Diagnose 1Million-AutoLeads file: show 1000 lines + where rows are dropped.
Run: python email_processor/diagnose_1million.py
"""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, OUTPUT_DIR
from parser import read_file_flexible_csv, detect_encoding, detect_delimiter, transform_dataframe, detect_schema
from cleaner import clean_and_validate_records

def main():
    # Find the file
    candidates = list(BASE_DIR.glob("**/1Million*AutoLeads*.csv"))
    if not candidates:
        print("No 1Million-AutoLeads CSV found.")
        return
    path = candidates[0]
    print(f"File: {path}")
    print(f"Size: {path.stat().st_size / 1e6:.1f} MB\n")

    encoding = detect_encoding(path)
    delimiter = detect_delimiter(path, encoding)

    # 1) Raw: count lines and how many have @ in column 2 (0-indexed)
    total_lines = 0
    with_email_col2 = 0
    first_1000_lines = []
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        for i, row in enumerate(reader):
            total_lines += 1
            if i >= 1000 and first_1000_lines:
                continue  # stop collecting after 1000
            if i < 1000:
                first_1000_lines.append(row)
            if len(row) > 2 and row[2].strip() and "@" in row[2]:
                with_email_col2 += 1
            if total_lines % 10_000 == 0 and total_lines > 0:
                print(f"  Read {total_lines:,} rows so far...")
            if total_lines >= 50_000:
                break

    print(f"Total rows read (first 50k): {total_lines:,}")
    print(f"Rows with non-empty email in column_2 (0-indexed): {with_email_col2:,}\n")

    # Write first 1000 lines to output so you can open them
    out_file = OUTPUT_DIR / "1Million_sample_1000lines.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for row in first_1000_lines:
            w.writerow(row)
    print(f"Wrote first 1000 lines to: {out_file}\n")

    # 2) Our parser: how many rows do we get?
    df = read_file_flexible_csv(path, encoding, delimiter)
    if df is None:
        print("read_file_flexible_csv returned None")
        return
    print(f"Parser (flexible CSV) row count: {len(df):,}")
    if len(df) > 0:
        cols = list(df.columns)
        print(f"Columns: {cols[:15]}...")
        # Which column has @?
        for c in cols:
            sample = df[c].dropna().astype(str)
            if len(sample) and sample.str.contains("@", na=False).any():
                print(f"Email column (has @): {c}")
                break

    # 3) Transform: how many have email?
    file_info = {"filename": path.name, "data_source": "diagnose", "is_clicker": False, "is_opener": False, "is_validated": False, "modified_date": None}
    records = transform_dataframe(df, file_info)
    with_email = sum(1 for r in records if r.get("email"))
    print(f"After transform_dataframe: {len(records):,} records, {with_email:,} with email\n")

    # 4) Clean/validate: how many pass?
    valid, rejection = clean_and_validate_records(records)
    print(f"After clean_and_validate: {len(valid):,} valid")
    print(f"Rejections: {rejection}\n")

    print("Open the sample file to verify format:")
    print(f"  {out_file}")

if __name__ == "__main__":
    main()
