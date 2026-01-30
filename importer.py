"""
Email Processing System - External Data Importer
Fast import with enrichment upsert, MX status lookup, and file source tracking
"""

import os
import re
import csv
import io
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Callable, Generator
from dataclasses import dataclass, field

from config import DATABASE, BATCH_SIZE_LOAD
from database import get_connection
from categorizer import get_domain_info
from cleaner import clean_email, is_valid_email_format as is_valid_email

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

# Larger batch size for faster imports (500K vs 100K default)
IMPORT_BATCH_SIZE = 200_000  # Larger batches for faster imports

# Regex patterns for schema detection
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
IP_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
PHONE_PATTERN = re.compile(r'^[\d\-\(\)\s\.]{7,20}$')
DATE_PATTERN = re.compile(r'^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}')
ZIP_PATTERN = re.compile(r'^\d{5}(-\d{4})?$')
STATE_CODES = {'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
               'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
               'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
               'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'}
GENDER_VALUES = {'M', 'F', 'MALE', 'FEMALE', 'm', 'f', 'male', 'female'}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ImportProgress:
    """Track import progress for UI updates."""
    status: str = 'idle'  # idle, scanning, importing, complete, error
    current_file: str = ''
    current_file_index: int = 0
    total_files: int = 0
    records_in_current_file: int = 0
    records_processed_current: int = 0
    
    # Totals across all files
    total_records_processed: int = 0
    total_new_records: int = 0
    total_enriched_records: int = 0
    total_skipped: int = 0
    
    # Category breakdown
    big4_count: int = 0
    cable_count: int = 0
    gi_valid_count: int = 0
    gi_dead_count: int = 0
    gi_new_domain_count: int = 0
    
    error_message: str = ''
    start_time: datetime = field(default_factory=datetime.now)
    log_messages: List[str] = field(default_factory=list)  # Real-time log
    
    def add_log(self, message: str):
        """Add a timestamped log message."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_messages.append(f"[{timestamp}] {message}")
        # Keep only last 50 messages
        if len(self.log_messages) > 50:
            self.log_messages = self.log_messages[-50:]
    
    def to_dict(self) -> Dict[str, Any]:
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.total_records_processed / elapsed if elapsed > 0 else 0
        return {
            'status': self.status,
            'current_file': self.current_file,
            'current_file_index': self.current_file_index,
            'total_files': self.total_files,
            'records_in_current_file': self.records_in_current_file,
            'records_processed_current': self.records_processed_current,
            'total_records_processed': self.total_records_processed,
            'total_new_records': self.total_new_records,
            'total_enriched_records': self.total_enriched_records,
            'total_skipped': self.total_skipped,
            'big4_count': self.big4_count,
            'cable_count': self.cable_count,
            'gi_valid_count': self.gi_valid_count,
            'gi_dead_count': self.gi_dead_count,
            'gi_new_domain_count': self.gi_new_domain_count,
            'error_message': self.error_message,
            'elapsed_seconds': int(elapsed),
            'rate_per_second': int(rate),
            'log_messages': self.log_messages,
        }


@dataclass 
class FileInfo:
    """Information about a file to import."""
    path: Path
    filename: str
    size_bytes: int
    size_mb: float
    column_count: int
    detected_type: str  # 'opener', 'clicker', 'email_only', 'unknown'
    row_count_estimate: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'filename': self.filename,
            'path': str(self.path),
            'size_bytes': self.size_bytes,
            'size_mb': self.size_mb,
            'column_count': self.column_count,
            'detected_type': self.detected_type,
            'row_count_estimate': self.row_count_estimate,
        }


# =============================================================================
# GLOBAL STATE
# =============================================================================

_import_progress = ImportProgress()
_import_lock = threading.Lock()
_stop_requested = False


def get_progress() -> ImportProgress:
    """Get current import progress."""
    with _import_lock:
        return _import_progress


def request_stop():
    """Request the import to stop."""
    global _stop_requested
    _stop_requested = True


# =============================================================================
# SCHEMA DETECTION
# =============================================================================

def detect_field_type(value: str) -> str:
    """Detect the type of a field value."""
    if not value or value.strip() == '':
        return 'empty'
    
    value = value.strip()
    
    # Email check first (most important)
    if '@' in value and EMAIL_PATTERN.match(value):
        return 'email'
    
    # IP address
    if IP_PATTERN.match(value):
        return 'ip'
    
    # Date patterns
    if DATE_PATTERN.match(value):
        return 'date'
    
    # US State code (2 letters)
    if len(value) == 2 and value.upper() in STATE_CODES:
        return 'state'
    
    # ZIP code
    if ZIP_PATTERN.match(value):
        return 'zip'
    
    # Gender
    if value.upper() in GENDER_VALUES or value in GENDER_VALUES:
        return 'gender'
    
    # Phone (digits with formatting)
    digits_only = re.sub(r'\D', '', value)
    if len(digits_only) >= 7 and len(digits_only) <= 15 and PHONE_PATTERN.match(value):
        return 'phone'
    
    # URL/domain
    if value.startswith('http') or '.com' in value or '.net' in value or '.org' in value:
        return 'domain'
    
    # Default to text
    return 'text'


def analyze_columns(rows: List[List[str]]) -> List[Dict[str, Any]]:
    """Analyze columns to determine their types based on sample rows."""
    if not rows:
        return []
    
    num_cols = max(len(row) for row in rows)
    column_types = []
    
    for col_idx in range(num_cols):
        type_counts = {}
        for row in rows:
            if col_idx < len(row):
                field_type = detect_field_type(row[col_idx])
                type_counts[field_type] = type_counts.get(field_type, 0) + 1
        
        # Determine primary type (excluding empty)
        non_empty = {k: v for k, v in type_counts.items() if k != 'empty'}
        if non_empty:
            primary_type = max(non_empty, key=non_empty.get)
        else:
            primary_type = 'empty'
        
        column_types.append({
            'index': col_idx,
            'primary_type': primary_type,
            'type_counts': type_counts,
        })
    
    return column_types


def create_column_mapping(column_types: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create mapping from standard fields to column indices."""
    mapping = {}
    
    # First pass: find email column (required)
    for col in column_types:
        if col['primary_type'] == 'email':
            mapping['email'] = col['index']
            break
    
    if 'email' not in mapping:
        # No email column found - can't process
        return {}
    
    email_idx = mapping['email']
    
    # Map other fields by type
    for col in column_types:
        idx = col['index']
        ptype = col['primary_type']
        
        if ptype == 'ip' and 'signup_ip' not in mapping:
            mapping['signup_ip'] = idx
        elif ptype == 'date' and 'signup_date' not in mapping:
            mapping['signup_date'] = idx
        elif ptype == 'state' and 'state' not in mapping:
            mapping['state'] = idx
        elif ptype == 'zip' and 'zipcode' not in mapping:
            mapping['zipcode'] = idx
        elif ptype == 'gender' and 'gender' not in mapping:
            mapping['gender'] = idx
        elif ptype == 'phone' and 'phone' not in mapping:
            mapping['phone'] = idx
        elif ptype == 'domain' and 'signup_domain' not in mapping:
            mapping['signup_domain'] = idx
    
    # Heuristic: columns before email are often first_name, last_name
    # Columns after email are often address, city, state, zip, phone
    text_cols = [c for c in column_types if c['primary_type'] == 'text']
    
    if email_idx >= 2:
        # Likely: first_name, last_name, email, ...
        if 'first_name' not in mapping and len(text_cols) > 0:
            # First text column before email
            before_email = [c for c in text_cols if c['index'] < email_idx]
            if len(before_email) >= 1:
                mapping['first_name'] = before_email[0]['index']
            if len(before_email) >= 2:
                mapping['last_name'] = before_email[1]['index']
    
    # Map remaining text columns after email as address, city
    after_email = [c for c in text_cols if c['index'] > email_idx and c['index'] not in mapping.values()]
    field_order = ['address', 'city']
    for i, field in enumerate(field_order):
        if field not in mapping and i < len(after_email):
            mapping[field] = after_email[i]['index']
    
    return mapping


# =============================================================================
# FILE SCANNING
# =============================================================================

def scan_directory(dir_path: str) -> List[FileInfo]:
    """Scan a directory for importable files."""
    path = Path(dir_path)
    if not path.exists():
        raise ValueError(f"Directory not found: {dir_path}")
    
    files = []
    supported_extensions = {'.csv', '.txt'}
    
    for item in path.iterdir():
        # Skip completed files
        if item.name.endswith('.complete'):
            continue
        if item.is_file() and item.suffix.lower() in supported_extensions:
            try:
                size = item.stat().st_size
                col_count, row_estimate = _quick_analyze_file(item)
                detected_type = _detect_file_type(item.name)
                
                files.append(FileInfo(
                    path=item,
                    filename=item.name,
                    size_bytes=size,
                    size_mb=round(size / (1024 * 1024), 2),
                    column_count=col_count,
                    detected_type=detected_type,
                    row_count_estimate=row_estimate,
                ))
            except Exception as e:
                logger.warning(f"Could not analyze file {item}: {e}")
    
    # Sort by size descending
    files.sort(key=lambda f: f.size_bytes, reverse=True)
    return files


def _quick_analyze_file(file_path: Path) -> Tuple[int, int]:
    """Quick analysis: count columns from first line, estimate row count."""
    col_count = 0
    row_estimate = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline()
            if first_line:
                col_count = len(first_line.split(','))
            
            # Estimate rows from file size and first line length
            line_len = len(first_line) if first_line else 50
            row_estimate = int(file_path.stat().st_size / max(line_len, 1))
    except:
        pass
    
    return col_count, row_estimate


def _detect_file_type(filename: str) -> str:
    """Detect file type from filename."""
    name_lower = filename.lower()
    
    if 'click' in name_lower:
        return 'clicker'
    elif 'open' in name_lower:
        return 'opener'
    elif 'pure' in name_lower or 'email' in name_lower:
        return 'email_only'
    else:
        return 'unknown'


def preview_file(file_path: str, num_rows: int = 10) -> Dict[str, Any]:
    """Preview a file with schema detection."""
    path = Path(file_path)
    
    if not path.exists():
        return {'error': f'File not found: {file_path}'}
    
    rows = []
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= num_rows:
                    break
                rows.append(row)
    except Exception as e:
        return {'error': str(e)}
    
    if not rows:
        return {'error': 'File is empty'}
    
    column_types = analyze_columns(rows)
    column_mapping = create_column_mapping(column_types)
    
    return {
        'filename': path.name,
        'rows': rows,
        'column_types': column_types,
        'column_mapping': column_mapping,
        'detected_type': _detect_file_type(path.name),
    }


# =============================================================================
# MX STATUS LOOKUP
# =============================================================================

def batch_lookup_mx_status(domains: List[str]) -> Dict[str, Optional[bool]]:
    """
    Batch lookup MX status for domains from domain_mx table.
    Returns dict: domain -> is_valid (True/False/None if not found)
    """
    if not domains:
        return {}
    
    result = {d: None for d in domains}
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if domain_mx table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'domain_mx'
                )
            """)
            if not cursor.fetchone()[0]:
                cursor.close()
                return result
            
            # Batch query
            unique_domains = list(set(domains))
            
            # Process in chunks to avoid query size limits
            chunk_size = 10000
            for i in range(0, len(unique_domains), chunk_size):
                chunk = unique_domains[i:i + chunk_size]
                placeholders = ','.join(['%s'] * len(chunk))
                cursor.execute(f"""
                    SELECT domain, is_valid 
                    FROM domain_mx 
                    WHERE domain IN ({placeholders})
                """, chunk)
                
                for row in cursor.fetchall():
                    result[row[0]] = row[1]
            
            cursor.close()
    except Exception as e:
        logger.warning(f"MX lookup failed: {e}")
    
    return result


# =============================================================================
# FAST IMPORT
# =============================================================================

def import_files(
    files: List[Dict[str, Any]],
    data_source: str = 'External Import',
    progress_callback: Optional[Callable[[ImportProgress], None]] = None
) -> Dict[str, Any]:
    """
    Import multiple files with enrichment upsert.
    
    Args:
        files: List of file dicts with 'path' and 'filename' keys
        data_source: Label for data_source field
        progress_callback: Optional callback for progress updates
        
    Returns:
        Summary dict with import statistics
    """
    global _import_progress, _stop_requested
    _stop_requested = False
    
    with _import_lock:
        _import_progress = ImportProgress()
        _import_progress.status = 'importing'
        _import_progress.total_files = len(files)
        _import_progress.start_time = datetime.now()
    
    try:
        for file_idx, file_info in enumerate(files):
            if _stop_requested:
                with _import_lock:
                    _import_progress.status = 'stopped'
                break
            
            file_path = Path(file_info['path'])
            filename = file_info.get('filename', file_path.name)
            
            with _import_lock:
                _import_progress.current_file = filename
                _import_progress.current_file_index = file_idx + 1
                _import_progress.records_processed_current = 0
                _import_progress.add_log(f"Starting file: {filename}")
            
            # Import single file
            _import_single_file(
                file_path=file_path,
                filename=filename,
                data_source=data_source,
                progress_callback=progress_callback,
            )
            
            # Rename file to .complete after successful import
            with _import_lock:
                _import_progress.add_log(f"Completed: {filename}")
            try:
                new_path = file_path.with_suffix(file_path.suffix + '.complete')
                file_path.rename(new_path)
                print(f"Renamed {filename} to {new_path.name}")
            except Exception as e:
                print(f"Could not rename {filename}: {e}")
        
        with _import_lock:
            if _import_progress.status != 'stopped':
                _import_progress.status = 'complete'
        
    except Exception as e:
        logger.error(f"Import error: {e}")
        with _import_lock:
            _import_progress.status = 'error'
            _import_progress.error_message = str(e)
    
    if progress_callback:
        progress_callback(_import_progress)
    
    return _import_progress.to_dict()


def _import_single_file(
    file_path: Path,
    filename: str,
    data_source: str,
    progress_callback: Optional[Callable[[ImportProgress], None]] = None,
):
    """Import a single file with enrichment upsert."""
    global _import_progress
    
    # Detect file type for is_opener/is_clicker flags
    file_type = _detect_file_type(filename)
    is_opener = file_type == 'opener'
    is_clicker = file_type == 'clicker'
    
    # First pass: detect schema from sample
    sample_rows = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                sample_rows.append(row)
    except Exception as e:
        logger.error(f"Error reading {filename}: {e}")
        return
    
    if not sample_rows:
        return
    
    column_types = analyze_columns(sample_rows)
    column_mapping = create_column_mapping(column_types)
    
    if 'email' not in column_mapping:
        logger.warning(f"No email column detected in {filename}, skipping")
        return
    
    # Process file in batches
    batch = []
    batch_domains = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            for row_idx, row in enumerate(reader):
                if _stop_requested:
                    break
                
                # Parse record
                record = _parse_row(row, column_mapping, filename, data_source, is_opener, is_clicker)
                
                if record and record.get('email'):
                    batch.append(record)
                    if record.get('email_domain'):
                        batch_domains.add(record['email_domain'])
                
                # Process batch when full
                if len(batch) >= IMPORT_BATCH_SIZE:
                    _process_batch(batch, batch_domains)
                    
                    with _import_lock:
                        _import_progress.records_processed_current = row_idx + 1
                    
                    if progress_callback:
                        progress_callback(_import_progress)
                    
                    batch = []
                    batch_domains = set()
            
            # Process remaining batch
            if batch:
                _process_batch(batch, batch_domains)
                
                with _import_lock:
                    _import_progress.records_processed_current = row_idx + 1
                
                if progress_callback:
                    progress_callback(_import_progress)
    
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")


def _parse_row(
    row: List[str],
    column_mapping: Dict[str, int],
    filename: str,
    data_source: str,
    is_opener: bool,
    is_clicker: bool,
) -> Optional[Dict[str, Any]]:
    """Parse a CSV row into a record dict."""
    
    # Get email
    email_idx = column_mapping.get('email')
    if email_idx is None or email_idx >= len(row):
        return None
    
    raw_email = row[email_idx].strip()
    email = clean_email(raw_email)
    
    if not email or not is_valid_email(email):
        return None
    
    # Extract domain
    email_domain = email.split('@')[-1] if '@' in email else None
    
    # Get domain info (category, provider, brand)
    domain_info = get_domain_info(email_domain) if email_domain else {}
    
    # Build record
    record = {
        'email': email,
        'email_domain': email_domain,
        'email_provider': domain_info.get('provider'),
        'email_brand': domain_info.get('brand'),
        'email_category': domain_info.get('category', 'General_Internet'),
        'is_opener': is_opener,
        'is_clicker': is_clicker,
        'data_source': data_source,
        'file_sources': [filename],
    }
    
    # Map other fields
    field_mapping = {
        'first_name': 'first_name',
        'last_name': 'last_name',
        'address': 'address',
        'city': 'city',
        'state': 'state',
        'zipcode': 'zipcode',
        'phone': 'phone',
        'gender': 'gender',
        'signup_ip': 'signup_ip',
        'signup_date': 'signup_date',
        'signup_domain': 'signup_domain',
    }
    
    for field, col_name in field_mapping.items():
        col_idx = column_mapping.get(col_name)
        if col_idx is not None and col_idx < len(row):
            value = row[col_idx].strip()
            if value and value.lower() not in ('null', 'none', ''):
                record[field] = value
    
    return record


def _process_batch(records: List[Dict[str, Any]], domains: set):
    """Process a batch of records: MX lookup + upsert to database."""
    global _import_progress
    
    print(f"DEBUG: _process_batch called with {len(records)} records")  # Debug
    
    if not records:
        return
    
    # Batch MX lookup for GI domains only
    gi_domains = [d for d in domains if d]  # Will filter by category in lookup
    mx_status = batch_lookup_mx_status(list(gi_domains))
    
    # Count categories for progress
    new_count = 0
    enriched_count = 0
    
    for record in records:
        category = record.get('email_category', 'General_Internet')
        domain = record.get('email_domain')
        
        with _import_lock:
            _import_progress.total_records_processed += 1
            
            if category == 'Big4_ISP':
                _import_progress.big4_count += 1
            elif category == 'Cable_Provider':
                _import_progress.cable_count += 1
            else:  # General_Internet
                mx_valid = mx_status.get(domain)
                if mx_valid is True:
                    _import_progress.gi_valid_count += 1
                elif mx_valid is False:
                    _import_progress.gi_dead_count += 1
                else:
                    _import_progress.gi_new_domain_count += 1
    
    # Upsert to database
    inserted, updated = _upsert_batch(records)
    
    with _import_lock:
        _import_progress.total_new_records += inserted
        _import_progress.total_enriched_records += updated
        if inserted > 0 or updated > 0:
            _import_progress.add_log(f"Batch: {len(records):,} records → {inserted:,} new, {updated:,} enriched")


def _upsert_batch(records: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Upsert batch using COPY + INSERT ON CONFLICT.
    Returns (inserted_count, updated_count).
    """
    print(f"DEBUG: _upsert_batch called with {len(records)} records")  # Debug
    if not records:
        return 0, 0
    
    columns = [
        'email', 'email_domain', 'email_provider', 'email_brand',
        'first_name', 'last_name', 'address',
        'city', 'state', 'zipcode', 'phone', 'dob', 'gender',
        'signup_date', 'signup_domain', 'signup_ip',
        'is_clicker', 'is_opener', 'validation_status',
        'email_category', 'quality_score', 'data_source', 'country', 'file_sources',
        'custom1', 'custom2', 'custom3', 'custom4', 'custom5'
    ]
    
    # Build COPY buffer
    buffer = io.StringIO()
    
    for record in records:
        row = []
        for col in columns:
            value = record.get(col)
            if value is None:
                row.append('\\N')
            elif col == 'file_sources' and isinstance(value, list):
                cleaned = [str(v).replace(',', '_').replace('{', '').replace('}', '').replace('"', '') for v in value]
                row.append('{' + ','.join(cleaned) + '}')
            elif isinstance(value, bool):
                row.append('t' if value else 'f')
            else:
                str_val = str(value)
                str_val = str_val.replace('\\', '\\\\')
                str_val = str_val.replace('\t', ' ')
                str_val = str_val.replace('\n', ' ')
                str_val = str_val.replace('\r', ' ')
                row.append(str_val)
        buffer.write('\t'.join(row) + '\n')
    
    buffer.seek(0)
    
    inserted = 0
    updated = 0
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Create temp table with TEXT for all string fields (no length limits)
            cursor.execute("""
                DROP TABLE IF EXISTS import_staging;
                CREATE TEMP TABLE import_staging (
                    email TEXT,
                    email_domain TEXT,
                    email_provider TEXT,
                    email_brand TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    address TEXT,
                    city TEXT,
                    state TEXT,
                    zipcode TEXT,
                    phone TEXT,
                    dob TEXT,
                    gender TEXT,
                    signup_date TEXT,
                    signup_domain TEXT,
                    signup_ip TEXT,
                    is_clicker BOOLEAN,
                    is_opener BOOLEAN,
                    validation_status TEXT,
                    email_category TEXT,
                    quality_score SMALLINT,
                    data_source TEXT,
                    country TEXT,
                    file_sources TEXT[],
                    custom1 TEXT,
                    custom2 TEXT,
                    custom3 TEXT,
                    custom4 TEXT,
                    custom5 TEXT
                )
            """)
            
            # COPY into staging
            cursor.copy_from(
                buffer,
                'import_staging',
                columns=columns,
                null='\\N'
            )
            
            # Get count before
            cursor.execute("SELECT COUNT(*) FROM emails")
            count_before = cursor.fetchone()[0]
            
            # Upsert with enrichment (truncate, strip time from dates, M/F gender)
            cursor.execute("""
                INSERT INTO emails (
                    email, email_domain, email_provider, email_brand,
                    first_name, last_name, address,
                    city, state, zipcode, phone, dob, gender,
                    signup_date, signup_domain, signup_ip,
                    is_clicker, is_opener, validation_status,
                    email_category, quality_score, data_source, country, file_sources,
                    custom1, custom2, custom3, custom4, custom5
                )
                SELECT 
                    LEFT(email, 320),
                    LEFT(email_domain, 255),
                    LEFT(email_provider, 50),
                    LEFT(email_brand, 50),
                    LEFT(first_name, 100),
                    LEFT(last_name, 100),
                    address,
                    LEFT(city, 100),
                    LEFT(state, 50),
                    LEFT(zipcode, 20),
                    LEFT(phone, 50),
                    CASE 
                        WHEN LEFT(dob, 10) ~ '^\\d{4}-\\d{2}-\\d{2}$' AND LEFT(dob, 10) != '0000-00-00' THEN LEFT(dob, 10)::DATE 
                        ELSE NULL 
                    END,
                    CASE 
                        WHEN UPPER(gender) IN ('MALE', 'M') THEN 'M'
                        WHEN UPPER(gender) IN ('FEMALE', 'F') THEN 'F'
                        ELSE LEFT(gender, 1)
                    END,
                    CASE 
                        WHEN LEFT(signup_date, 10) ~ '^\\d{4}-\\d{2}-\\d{2}$' AND LEFT(signup_date, 10) != '0000-00-00' THEN LEFT(signup_date, 10)::TIMESTAMP 
                        ELSE NULL 
                    END,
                    LEFT(signup_domain, 255),
                    LEFT(signup_ip, 45),
                    is_clicker, is_opener,
                    LEFT(validation_status, 50),
                    LEFT(email_category, 50),
                    quality_score,
                    LEFT(data_source, 100),
                    LEFT(country, 10),
                    file_sources,
                    custom1, custom2, custom3, custom4, custom5
                FROM import_staging
                ON CONFLICT (email) DO UPDATE SET
                    first_name = COALESCE(EXCLUDED.first_name, emails.first_name),
                    last_name = COALESCE(EXCLUDED.last_name, emails.last_name),
                    address = COALESCE(EXCLUDED.address, emails.address),
                    city = COALESCE(EXCLUDED.city, emails.city),
                    state = COALESCE(EXCLUDED.state, emails.state),
                    zipcode = COALESCE(EXCLUDED.zipcode, emails.zipcode),
                    phone = COALESCE(EXCLUDED.phone, emails.phone),
                    dob = COALESCE(EXCLUDED.dob, emails.dob),
                    gender = COALESCE(EXCLUDED.gender, emails.gender),
                    signup_date = COALESCE(EXCLUDED.signup_date, emails.signup_date),
                    signup_domain = COALESCE(EXCLUDED.signup_domain, emails.signup_domain),
                    signup_ip = COALESCE(EXCLUDED.signup_ip, emails.signup_ip),
                    is_clicker = emails.is_clicker OR EXCLUDED.is_clicker,
                    is_opener = emails.is_opener OR EXCLUDED.is_opener,
                    quality_score = GREATEST(COALESCE(emails.quality_score, 0), COALESCE(EXCLUDED.quality_score, 0)),
                    file_sources = array_cat(emails.file_sources, EXCLUDED.file_sources),
                    updated_at = NOW()
            """)
            
            affected = cursor.rowcount
            
            # Get count after
            cursor.execute("SELECT COUNT(*) FROM emails")
            count_after = cursor.fetchone()[0]
            
            inserted = count_after - count_before
            updated = affected - inserted
            
            cursor.close()
            conn.commit()
            
    except Exception as e:
        logger.error(f"Upsert batch failed: {e}")
        print(f"Upsert batch failed: {e}")  # Debug print to console
        # Add error to progress log
        with _import_lock:
            _import_progress.add_log(f"ERROR: {str(e)[:100]}")
    
    print(f"DEBUG: Upsert returning inserted={inserted}, updated={updated}")  # Debug
    return max(0, inserted), max(0, updated)
