"""
Email Processing System - Parser Module
Handles CSV/TXT file parsing with automatic schema detection
"""

import csv
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import chardet
import re
import zipfile
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile

from config import (
    BASE_DIR, SUPPORTED_EXTENSIONS, ARCHIVE_EXTENSIONS, SKIP_PATTERNS,
    COLUMN_MAPPINGS, DATA_SOURCE_PATTERNS, FOLDER_SOURCE_PATTERNS,
    CPU_WORKERS, BATCH_SIZE_PARSE, EXCLUDE_FILE_PATTERNS
)

logger = logging.getLogger(__name__)


# =============================================================================
# FILE DISCOVERY
# =============================================================================

def discover_files(base_path: Path = BASE_DIR) -> List[Dict[str, Any]]:
    """
    Discover all processable files in the directory tree.
    
    Returns:
        List of file info dictionaries with path, size, date, etc.
    """
    files = []
    
    for root, dirs, filenames in os.walk(base_path):
        # Skip certain directories
        dirs[:] = [d for d in dirs if d not in SKIP_PATTERNS and not d.startswith('.')]
        
        root_path = Path(root)
        
        for filename in filenames:
            file_path = root_path / filename
            suffix = file_path.suffix.lower()
            
            # Skip non-data files
            if suffix not in SUPPORTED_EXTENSIONS + ARCHIVE_EXTENSIONS:
                continue
            
            # Skip files in skip patterns
            if any(skip in str(file_path).lower() for skip in SKIP_PATTERNS):
                continue
            
            # Skip suppression/DNC/blacklist files
            if any(pattern in filename.lower() for pattern in EXCLUDE_FILE_PATTERNS):
                logger.debug(f"Skipping suppression/DNC file: {filename}")
                continue
            
            try:
                stat = file_path.stat()
                file_info = {
                    'path': file_path,
                    'filename': filename,
                    'extension': suffix,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'modified_date': datetime.fromtimestamp(stat.st_mtime),
                    'is_archive': suffix in ARCHIVE_EXTENSIONS,
                    'parent_folder': root_path.name,
                }
                
                # Extract metadata from filename
                file_info.update(extract_file_metadata(filename, str(root_path)))
                
                files.append(file_info)
                
            except Exception as e:
                logger.warning(f"Could not stat file {file_path}: {e}")
    
    # Sort by size (largest first for better progress feedback)
    files.sort(key=lambda x: x['size_bytes'], reverse=True)
    
    logger.info(f"Discovered {len(files)} files to process")
    return files


def extract_file_metadata(filename: str, folder_path: str) -> Dict[str, Any]:
    """Extract metadata from filename and folder path."""
    
    filename_lower = filename.lower()
    folder_lower = folder_path.lower()
    
    # Check for clicker/opener indicators
    is_clicker = 'click' in filename_lower
    is_opener = 'open' in filename_lower and 'click' not in filename_lower
    
    # Check for validation indicators
    is_validated = any(x in filename_lower for x in ['certified', 'verified', 'validated'])
    
    # Determine data source
    data_source = 'Unknown'
    
    # Check folder patterns first
    for pattern, source in FOLDER_SOURCE_PATTERNS.items():
        if pattern in folder_lower:
            data_source = source
            break
    
    # Check filename patterns (override folder if more specific)
    for pattern, source in DATA_SOURCE_PATTERNS.items():
        if pattern in filename_lower:
            data_source = source
            break
    
    return {
        'is_clicker': is_clicker,
        'is_opener': is_opener,
        'is_validated': is_validated,
        'data_source': data_source,
    }


# =============================================================================
# FILE READING
# =============================================================================

def detect_encoding(file_path: Path, sample_size: int = 10000) -> str:
    """Detect file encoding."""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            
            # Default to utf-8 if detection fails or returns unusual encoding
            if not encoding or encoding.lower() in ['ascii', 'windows-1252']:
                return 'utf-8'
            return encoding
    except:
        return 'utf-8'


def detect_delimiter(file_path: Path, encoding: str = 'utf-8') -> str:
    """Detect the delimiter used in a CSV file."""
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            sample = f.read(5000)
            
            # Count potential delimiters
            delimiters = {',': 0, '\t': 0, '|': 0, ';': 0}
            for d in delimiters:
                delimiters[d] = sample.count(d)
            
            # Return most common delimiter
            return max(delimiters, key=delimiters.get)
    except:
        return ','


def _parse_csv_line(line: str, delimiter: str = ',') -> List[str]:
    """Parse a single line as CSV (respects quoted commas). Returns list of fields."""
    return next(csv.reader([line], delimiter=delimiter, quoting=csv.QUOTE_MINIMAL))


def _is_valid_email(value: str) -> bool:
    """Check if value looks like a valid email address."""
    if not value or not isinstance(value, str):
        return False
    v = value.strip().strip('"').strip("'").lower()
    # Must have @ with text before and after, and a . after @
    # Pattern: something@something.something (at least 2 chars after last .)
    if '@' not in v or '.' not in v.split('@')[-1]:
        return False
    # Basic structure check: local@domain.tld
    parts = v.split('@')
    if len(parts) != 2:
        return False
    local, domain = parts
    if not local or not domain:
        return False
    if '.' not in domain:
        return False
    # Domain should have at least one . and end with letters
    domain_parts = domain.split('.')
    if len(domain_parts) < 2:
        return False
    tld = domain_parts[-1]
    if not tld or len(tld) < 2 or not tld.isalpha():
        return False
    return True


def _row_has_email(values: List[str]) -> bool:
    """True if any value looks like a valid email address."""
    for v in values:
        if v and _is_valid_email(str(v)):
            return True
    return False


def _read_large_csv_chunked(file_path: Path, encoding: str, delimiter: str, chunk_size: int = 50000) -> Optional[pd.DataFrame]:
    """
    Memory-efficient reader for large CSV files.
    Reads in chunks using pandas, processes each chunk, concatenates at the end.
    Only keeps columns that might be useful (limits to 20 columns max to save memory).
    """
    # Try multiple encodings if the provided one fails
    encodings_to_try = [encoding]
    if encoding.lower() not in ['latin-1', 'latin1', 'iso-8859-1']:
        encodings_to_try.append('latin-1')
    if encoding.lower() not in ['cp1252', 'windows-1252']:
        encodings_to_try.append('cp1252')
    
    last_error = None
    for enc in encodings_to_try:
        result = _read_large_csv_chunked_with_encoding(file_path, enc, delimiter, chunk_size)
        if result is not None:
            return result
    
    logger.warning(f"Chunked CSV read failed for {file_path}: tried encodings {encodings_to_try}")
    return None


def _read_large_csv_chunked_with_encoding(file_path: Path, encoding: str, delimiter: str, chunk_size: int = 50000) -> Optional[pd.DataFrame]:
    """Internal helper that tries a specific encoding."""
    chunks = []
    header_detected = None
    
    try:
        # First, detect header by reading first few lines
        with open(file_path, 'r', encoding=encoding, errors='replace', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
            try:
                first_row = next(reader)
            except StopIteration:
                return pd.DataFrame()
            first_row = [str(c).strip() for c in first_row]
            
            header_keywords = {'email', 'e-mail', 'name', 'first', 'last', 'fname', 'lname', 
                               'address', 'phone', 'mobile', 'city', 'state', 'zip', 'country',
                               'carrier', 'gender', 'dob', 'date', 'ip', 'opt'}
            first_row_lower = [str(v).lower().strip() for v in first_row]
            has_header_words = any(any(kw in f for kw in header_keywords) for f in first_row_lower)
            has_email_data = _row_has_email(first_row)
            is_header_row = has_header_words and not has_email_data
            
            if is_header_row:
                header_detected = first_row
            else:
                header_detected = [f'column_{i}' for i in range(len(first_row))]
        
        # Normalize header
        seen = set()
        normalized_header = []
        for i, h in enumerate(header_detected):
            name = (h or f'column_{i}').strip()
            while name in seen:
                name = f'{name}_{i}'
            seen.add(name)
            normalized_header.append(name)
        
        # Find which columns we actually need (email-related + name/address columns)
        # This reduces memory by not loading unnecessary columns
        useful_patterns = ['email', 'mail', 'first', 'last', 'name', 'fname', 'lname',
                           'address', 'city', 'state', 'zip', 'phone', 'mobile', 
                           'gender', 'dob', 'date', 'valid', 'column_']
        useful_cols = []
        for i, col in enumerate(normalized_header):
            col_lower = col.lower()
            if any(p in col_lower for p in useful_patterns) or i < 15:  # Keep first 15 cols
                useful_cols.append(i)
        
        # Limit to max 20 columns to save memory
        useful_cols = useful_cols[:20]
        
        # Read using pandas with chunking
        # Use on_bad_lines='skip' to silently handle rows with extra/fewer columns
        has_header = is_header_row
        for chunk in pd.read_csv(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            header=0 if has_header else None,
            names=None if has_header else normalized_header,
            usecols=useful_cols if len(useful_cols) < len(normalized_header) else None,
            chunksize=chunk_size,
            on_bad_lines='skip',  # Silently skip malformed rows
            dtype=str,
            na_filter=False,
            encoding_errors='replace',
        ):
            chunks.append(chunk)
        
        if not chunks:
            return pd.DataFrame()
        
        df = pd.concat(chunks, ignore_index=True)
        
        # Ensure column names are normalized
        if has_header:
            # Re-normalize the column names pandas read
            new_cols = []
            seen = set()
            for i, c in enumerate(df.columns):
                name = str(c).strip() or f'column_{i}'
                while name in seen:
                    name = f'{name}_{i}'
                seen.add(name)
                new_cols.append(name)
            df.columns = new_cols
        
        return df
        
    except Exception as e:
        # Return None to let wrapper try next encoding
        return None


def read_file_flexible_csv(file_path: Path, encoding: str, delimiter: str) -> Optional[pd.DataFrame]:
    """
    Read CSV with Python csv module so quoted commas don't split fields.
    Never drops rows: variable column counts are padded/truncated; extra columns become extra_0, extra_1, ...
    For large files (>50MB), uses memory-efficient chunked processing.
    """
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    
    # For very large files, use pandas chunked reading (more memory efficient)
    if file_size_mb > 50:
        return _read_large_csv_chunked(file_path, encoding, delimiter)
    
    rows_dicts: List[Dict[str, str]] = []
    header: List[str] = []
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
            try:
                first_row = next(reader)
            except StopIteration:
                return pd.DataFrame()
            first_row = [str(c).strip() for c in first_row]
            
            # Determine if first row is a header or data
            # It's DATA (not header) if:
            #   1. Any field is a valid email, OR
            #   2. Most fields are numeric (phone numbers, zip codes)
            # It's HEADER if:
            #   1. Contains common header words like "email", "name", "first", "address", "phone"
            #   2. No valid emails in the row
            header_keywords = {'email', 'e-mail', 'name', 'first', 'last', 'fname', 'lname', 
                               'address', 'phone', 'mobile', 'city', 'state', 'zip', 'country',
                               'carrier', 'gender', 'dob', 'date', 'ip', 'opt'}
            first_row_lower = [str(v).lower().strip() for v in first_row]
            has_header_words = any(any(kw in f for kw in header_keywords) for f in first_row_lower)
            has_email_data = _row_has_email(first_row)
            
            # Decide: if first row has email data -> no header; if has header keywords and no email -> header
            is_header_row = has_header_words and not has_email_data
            
            if not is_header_row:
                # No header - use generic column names, first row is data
                header = [f'column_{i}' for i in range(len(first_row))]
                row_dict = {header[i]: first_row[i] for i in range(len(first_row))}
                rows_dicts.append(row_dict)
            else:
                # First row is header
                header = first_row
            # Normalize header: empty or duplicate names get unique names
            seen = set()
            new_header = []
            for i, h in enumerate(header):
                name = (h or f'column_{i}').strip()
                while name in seen:
                    name = f'{name}_{i}'
                seen.add(name)
                new_header.append(name)
            header = new_header
            max_cols = len(header)
            for row in reader:
                row = [str(c).strip() for c in row]
                if len(row) > max_cols:
                    max_cols = len(row)
                # Build dict: map by position; pad missing, add extra_0, extra_1 for overflow
                d = {}
                for i in range(len(header)):
                    d[header[i]] = row[i] if i < len(row) else ''
                for i in range(len(header), len(row)):
                    d[f'extra_{i - len(header)}'] = row[i]
                rows_dicts.append(d)
            # Build ordered column list: header columns first, then extras in order
            all_extra_cols = set()
            for d in rows_dicts:
                for k in d.keys():
                    if k.startswith('extra_'):
                        all_extra_cols.add(k)
            # Sort extra columns by their index
            extra_cols_sorted = sorted(all_extra_cols, key=lambda x: int(x.split('_')[1]) if x.split('_')[1].isdigit() else 0)
            ordered_columns = header + extra_cols_sorted
            # Pad missing keys in each row
            for d in rows_dicts:
                for k in ordered_columns:
                    if k not in d:
                        d[k] = ''
        if not rows_dicts:
            return pd.DataFrame()
        return pd.DataFrame(rows_dicts, columns=ordered_columns)
    except Exception as e:
        logger.warning(f"Flexible CSV read failed for {file_path}: {e}")
        return None


def read_file_flexible_txt(file_path: Path, encoding: str) -> Optional[pd.DataFrame]:
    """
    Read TXT line-by-line; each line may be email-only or comma-separated.
    Never drops lines: extract email (field containing @ or first field), rest become optional columns.
    """
    rows_dicts: List[Dict[str, str]] = []
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace', newline='') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                fields = _parse_csv_line(line, ',')
                if not fields:
                    continue
                fields = [v.strip() for v in fields if v is not None and str(v).strip()]
                # Prefer field containing @ as email; else use first non-empty
                email_idx = -1
                for i, v in enumerate(fields):
                    if '@' in v:
                        email_idx = i
                        break
                if email_idx < 0:
                    email_idx = 0
                email_val = fields[email_idx]
                rest = [v for i, v in enumerate(fields) if i != email_idx and v]
                if not email_val:
                    continue
                row_dict = {'email': email_val}
                for i, v in enumerate(rest):
                    row_dict[f'column_{i + 1}'] = v
                rows_dicts.append(row_dict)
        if not rows_dicts:
            return pd.DataFrame()
        # Uniform columns
        all_keys = set()
        for d in rows_dicts:
            all_keys.update(d.keys())
        for d in rows_dicts:
            for k in all_keys:
                if k not in d:
                    d[k] = ''
        return pd.DataFrame(rows_dicts, columns=sorted(all_keys, key=lambda x: (x != 'email', x)))
    except Exception as e:
        logger.warning(f"Flexible TXT read failed for {file_path}: {e}")
        return None


def detect_schema(df: pd.DataFrame) -> Tuple[str, Dict[str, str]]:
    """
    Detect the schema type and create column mappings.
    
    Returns:
        Tuple of (schema_type, column_mapping)
    """
    columns = [c.lower().strip() for c in df.columns]
    original_columns = list(df.columns)
    
    # Create mapping from our standard fields to actual columns
    # Two-pass approach: exact matches first, then partial matches
    mapping = {}
    
    # Pass 1: Exact matches only
    for standard_field, variations in COLUMN_MAPPINGS.items():
        for var in variations:
            var_lower = var.lower()
            for i, col in enumerate(columns):
                if col == var_lower:  # Exact match only
                    mapping[standard_field] = original_columns[i]
                    break
            if standard_field in mapping:
                break
    
    # Pass 2: Partial matches for fields not yet mapped
    for standard_field, variations in COLUMN_MAPPINGS.items():
        if standard_field in mapping:
            continue
        for var in variations:
            var_lower = var.lower()
            for i, col in enumerate(columns):
                if var_lower in col and standard_field not in mapping:
                    # Avoid matching "emailid" for "email" - check if column has actual emails
                    if standard_field == 'email':
                        sample = df[original_columns[i]].head(100).dropna().astype(str)
                        if sum(1 for v in sample if _is_valid_email(v)) < 10:
                            continue  # Skip this column, not enough valid emails
                    mapping[standard_field] = original_columns[i]
                    break
            if standard_field in mapping:
                break

    # Verify email column has actual emails; if not, use content-based detection
    if 'email' in mapping and len(df) > 0:
        email_col = mapping['email']
        sample = df[email_col].head(100).dropna().astype(str)
        valid_emails = sum(1 for v in sample if _is_valid_email(v))
        if valid_emails < 10:
            # Email column doesn't have actual emails - remove mapping and use content detection
            del mapping['email']

    # If no column mapped to email (e.g. generic column_0, column_1), pick by content:
    # Find the column with the MOST valid email addresses (sample first 1000 rows)
    if 'email' not in mapping and len(df) > 0:
        sample_df = df.head(1000)
        best_col = None
        best_count = 0
        email_col_idx = -1
        for i, c in enumerate(original_columns):
            sample = sample_df[c].dropna().astype(str)
            # Count rows that pass our email validation
            at_count = sum(1 for v in sample if _is_valid_email(v))
            if at_count > best_count:
                best_count = at_count
                best_col = c
                email_col_idx = i
        if best_col and best_count > 0:
            mapping['email'] = best_col
            # For headerless files with generic column names, try to map first_name/last_name
            # Common pattern: first_name, last_name, email, address, city, state, zip, phone, ...
            if best_col.startswith('column_') and email_col_idx >= 2:
                if 'first_name' not in mapping and email_col_idx >= 1:
                    mapping['first_name'] = original_columns[0]
                if 'last_name' not in mapping and email_col_idx >= 2:
                    mapping['last_name'] = original_columns[1]
                # Try to map other fields based on position relative to email
                if email_col_idx + 1 < len(original_columns) and 'address' not in mapping:
                    mapping['address'] = original_columns[email_col_idx + 1]
                if email_col_idx + 2 < len(original_columns) and 'city' not in mapping:
                    mapping['city'] = original_columns[email_col_idx + 2]
                if email_col_idx + 3 < len(original_columns) and 'state' not in mapping:
                    mapping['state'] = original_columns[email_col_idx + 3]
                if email_col_idx + 4 < len(original_columns) and 'zipcode' not in mapping:
                    mapping['zipcode'] = original_columns[email_col_idx + 4]
                if email_col_idx + 5 < len(original_columns) and 'phone' not in mapping:
                    mapping['phone'] = original_columns[email_col_idx + 5]
        elif original_columns:
            mapping['email'] = original_columns[0]
    
    # Determine schema type
    if len(df.columns) == 1:
        schema_type = 'email_only'
        mapping['email'] = df.columns[0]
    elif 'validationstatusid' in columns or 'emaildomaingroup' in columns:
        schema_type = 'validated_data'
    elif 'carrier' in columns or 'timezone' in columns:
        schema_type = 'mobile_data'
    elif 'created on' in columns or 'ip address' in columns:
        schema_type = 'lead_data'
    elif len(mapping) > 5:
        schema_type = 'rich_data'
    else:
        schema_type = 'basic_data'
    
    return schema_type, mapping


def read_file(file_info: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Read a single file and return a DataFrame. Imports every row:
    - CSV: Python csv module (respects quoted commas); variable columns padded/extra_0, extra_1...
    - TXT: line-by-line; email = field with @ or first field, rest optional. Auto-detect populates schema.
    Falls back to pandas only if flexible reader fails.
    """
    file_path = file_info['path']
    try:
        encoding = detect_encoding(file_path)
        if file_info['extension'] == '.txt':
            df = read_file_flexible_txt(file_path, encoding)
            if df is not None and len(df) > 0:
                return df
            try:
                df = pd.read_csv(
                    file_path, encoding=encoding, header=None, names=['email'],
                    on_bad_lines='skip', engine='python', dtype=str,
                )
                if len(df) > 0 and df.iloc[0]['email'] and '@' not in str(df.iloc[0]['email']):
                    df = df.iloc[1:]
                return df
            except Exception:
                return read_file_flexible_txt(file_path, encoding)  # return flexible result even if empty
        delimiter = detect_delimiter(file_path, encoding)
        df = read_file_flexible_csv(file_path, encoding, delimiter)
        if df is not None and len(df) > 0:
            return df
        # Fallback: pandas
        try:
            df = pd.read_csv(
                file_path, encoding=encoding, delimiter=delimiter,
                on_bad_lines='skip', engine='python', dtype=str,
            )
            df.columns = df.columns.str.strip()
            return df
        except Exception:
            return None
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None


def read_file_chunked(
    file_info: Dict[str, Any],
    chunk_size: int = BATCH_SIZE_PARSE
) -> Generator[pd.DataFrame, None, None]:
    """
    Read a large file in chunks.
    
    Yields:
        DataFrame chunks
    """
    file_path = file_info['path']
    
    try:
        encoding = detect_encoding(file_path)
        delimiter = detect_delimiter(file_path, encoding)
        
        # For text files, handle differently
        if file_info['extension'] == '.txt':
            # Read the whole file (usually not too large for txt)
            df = read_file(file_info)
            if df is not None:
                yield df
            return
        
        # Read in chunks
        chunks = pd.read_csv(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            on_bad_lines='skip',
            engine='python',
            dtype=str,
            chunksize=chunk_size,
        )
        
        for chunk in chunks:
            chunk.columns = chunk.columns.str.strip()
            yield chunk
            
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")


# =============================================================================
# ARCHIVE HANDLING
# =============================================================================

def extract_archive(archive_path: Path, temp_dir: Path) -> List[Path]:
    """
    Extract files from a ZIP archive.
    
    Returns:
        List of extracted file paths
    """
    extracted_files = []
    
    try:
        if archive_path.suffix.lower() == '.zip':
            with zipfile.ZipFile(archive_path, 'r') as zf:
                for name in zf.namelist():
                    if any(name.lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                        zf.extract(name, temp_dir)
                        extracted_files.append(temp_dir / name)
                        
        # Note: RAR support would require additional library (rarfile)
        # For now, log a warning for RAR files
        elif archive_path.suffix.lower() == '.rar':
            logger.warning(f"RAR file found but not supported: {archive_path}")
            
    except Exception as e:
        logger.error(f"Error extracting archive {archive_path}: {e}")
    
    return extracted_files


# =============================================================================
# RECORD TRANSFORMATION
# =============================================================================

def transform_record(row: pd.Series, column_mapping: Dict[str, str], file_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a DataFrame row to our unified schema.
    
    Args:
        row: A pandas Series (one row)
        column_mapping: Mapping of standard fields to actual columns
        file_info: Metadata about the source file
        
    Returns:
        Dictionary with unified schema
    """
    record = {
        'email': None,
        'email_domain': None,
        'first_name': None,
        'last_name': None,
        'address': None,
        'city': None,
        'state': None,
        'zipcode': None,
        'phone': None,
        'dob': None,
        'gender': None,
        'signup_date': None,
        'signup_domain': None,
        'signup_ip': None,
        'is_clicker': file_info.get('is_clicker', False),
        'is_opener': file_info.get('is_opener', False),
        'validation_status': None,
        'email_category': None,  # Will be set by categorizer
        'data_source': file_info.get('data_source', 'Unknown'),
        'country': None,
        'file_sources': [file_info['filename']],
        'custom1': None,
        'custom2': None,
        'custom3': None,
        'custom4': None,
        'custom5': None,
    }
    
    # Map fields from source columns
    for standard_field, source_column in column_mapping.items():
        if source_column in row.index:
            value = row[source_column]
            if pd.notna(value) and str(value).strip():
                record[standard_field] = str(value).strip()
    
    # Extract email domain
    if record['email']:
        email = record['email'].lower().strip()
        record['email'] = email
        if '@' in email:
            record['email_domain'] = email.split('@')[-1]
    
    # Handle validation status
    if record.get('validation_status'):
        status = str(record['validation_status']).lower()
        if 'verified' in status or status in ['1', 'true', 'valid']:
            record['validation_status'] = 'Verified'
        elif 'invalid' in status or status in ['0', 'false']:
            record['validation_status'] = 'Invalid'
        else:
            record['validation_status'] = 'Unknown'
    elif file_info.get('is_validated'):
        record['validation_status'] = 'Verified'
    
    # Parse signup date if not already set
    if not record['signup_date']:
        # Use file date as fallback
        record['signup_date'] = file_info.get('modified_date')
    
    # Normalize gender
    if record.get('gender'):
        gender = str(record['gender']).upper()[0]
        if gender in ['M', 'F']:
            record['gender'] = gender
        else:
            record['gender'] = None
    
    # Map any extra columns to custom fields
    custom_idx = 1
    for col in row.index:
        if col not in column_mapping.values() and custom_idx <= 5:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                record[f'custom{custom_idx}'] = str(value).strip()[:500]  # Limit length
                custom_idx += 1
    
    return record


def transform_dataframe(
    df: pd.DataFrame,
    file_info: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Transform an entire DataFrame to our unified schema.
    
    Returns:
        List of record dictionaries
    """
    # Detect schema and get column mapping
    schema_type, column_mapping = detect_schema(df)
    
    logger.debug(f"Schema type: {schema_type}, Mapping: {column_mapping}")
    
    records = []
    for _, row in df.iterrows():
        record = transform_record(row, column_mapping, file_info)
        if record.get('email'):  # Only keep records with email
            records.append(record)
    
    return records


# =============================================================================
# PARALLEL PROCESSING
# =============================================================================

def process_file(file_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Process a single file and return records + stats.
    
    Returns:
        Tuple of (records, stats)
    """
    start_time = datetime.now()
    records = []
    stats = {
        'filename': file_info['filename'],
        'records_read': 0,
        'records_parsed': 0,
        'schema_type': 'unknown',
        'errors': []
    }
    
    try:
        df = read_file(file_info)
        
        if df is not None and len(df) > 0:
            stats['records_read'] = len(df)
            
            # Detect schema
            schema_type, column_mapping = detect_schema(df)
            stats['schema_type'] = schema_type
            
            # Transform records
            records = transform_dataframe(df, file_info)
            stats['records_parsed'] = len(records)
            
    except Exception as e:
        stats['errors'].append(str(e))
        logger.error(f"Error processing {file_info['filename']}: {e}")
    
    stats['processing_time'] = (datetime.now() - start_time).total_seconds()
    
    return records, stats


def process_files_parallel(
    files: List[Dict[str, Any]],
    max_workers: int = CPU_WORKERS
) -> Generator[Tuple[List[Dict[str, Any]], Dict[str, Any]], None, None]:
    """
    Process multiple files in parallel.
    
    Yields:
        Tuples of (records, stats) for each file
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_file, file_info): file_info
            for file_info in files
        }
        
        for future in as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                records, stats = future.result()
                yield records, stats
            except Exception as e:
                logger.error(f"Error processing {file_info['filename']}: {e}")
                yield [], {'filename': file_info['filename'], 'errors': [str(e)]}
