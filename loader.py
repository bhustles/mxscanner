"""
Email Processing System - Loader Module
Handles bulk loading of records into PostgreSQL
"""

import logging
from typing import List, Dict, Any, Generator, Tuple
from datetime import datetime
import io
import csv

from database import (
    get_connection, get_cursor, bulk_insert_emails,
    log_file_processing, log_processing_stats
)
from config import BATCH_SIZE_LOAD

logger = logging.getLogger(__name__)


# =============================================================================
# BATCH LOADING
# =============================================================================

def load_records_batch(
    records: List[Dict[str, Any]],
    batch_size: int = BATCH_SIZE_LOAD,
    on_conflict: str = 'skip'
) -> int:
    """
    Load records in batches.
    
    Args:
        records: List of record dictionaries
        batch_size: Records per batch
        on_conflict: 'skip' or 'update'
        
    Returns:
        Total records loaded
    """
    if not records:
        return 0
    
    total_loaded = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            loaded = bulk_insert_emails(batch, on_conflict)
            total_loaded += loaded
            logger.debug(f"Loaded batch {i // batch_size + 1}: {loaded} records")
        except Exception as e:
            logger.error(f"Error loading batch {i // batch_size + 1}: {e}")
    
    return total_loaded


def load_records_streaming(
    records_generator: Generator[Dict[str, Any], None, None],
    batch_size: int = BATCH_SIZE_LOAD,
    on_conflict: str = 'skip',
    progress_callback=None
) -> int:
    """
    Load records from a generator in streaming fashion.
    
    Args:
        records_generator: Generator yielding record dictionaries
        batch_size: Records per batch
        on_conflict: 'skip' or 'update'
        progress_callback: Optional callback(loaded_count) for progress updates
        
    Returns:
        Total records loaded
    """
    total_loaded = 0
    batch = []
    
    for record in records_generator:
        batch.append(record)
        
        if len(batch) >= batch_size:
            loaded = bulk_insert_emails(batch, on_conflict)
            total_loaded += loaded
            batch = []
            
            if progress_callback:
                progress_callback(total_loaded)
    
    # Load remaining records
    if batch:
        loaded = bulk_insert_emails(batch, on_conflict)
        total_loaded += loaded
        
        if progress_callback:
            progress_callback(total_loaded)
    
    return total_loaded


# =============================================================================
# DIRECT COPY LOADING (Fastest)
# =============================================================================

def prepare_record_for_copy(record: Dict[str, Any]) -> List[str]:
    """Prepare a record for PostgreSQL COPY format."""
    
    columns = [
        'email', 'email_domain', 'email_provider', 'email_brand',
        'first_name', 'last_name', 'address',
        'city', 'state', 'zipcode', 'phone', 'dob', 'gender',
        'signup_date', 'signup_domain', 'signup_ip',
        'is_clicker', 'is_opener', 'validation_status',
        'email_category', 'quality_score', 'data_source', 'country', 'file_sources',
        'custom1', 'custom2', 'custom3', 'custom4', 'custom5'
    ]
    
    row = []
    for col in columns:
        value = record.get(col)
        
        if value is None:
            row.append('\\N')
        elif col == 'file_sources' and isinstance(value, list):
            # PostgreSQL array format - simple format without quotes for simple strings
            # Format: {value1,value2,value3}
            cleaned = [str(v).replace(',', '_').replace('{', '').replace('}', '').replace('"', '') for v in value]
            row.append('{' + ','.join(cleaned) + '}')
        elif col == 'signup_date':
            # Normalize signup_date to YYYY-MM-DD format
            if hasattr(value, 'strftime'):
                row.append(value.strftime('%Y-%m-%d'))
            elif value:
                date_str = str(value).strip()
                if ' ' in date_str:
                    date_str = date_str.split(' ')[0]
                try:
                    from datetime import datetime
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            row.append(dt.strftime('%Y-%m-%d'))
                            break
                        except ValueError:
                            continue
                    else:
                        row.append(date_str[:10] if date_str else '')
                except:
                    row.append(date_str[:10] if date_str else '')
            else:
                row.append('')
        elif col == 'dob':
            # Normalize date of birth to YYYY-MM-DD format
            if hasattr(value, 'strftime'):
                row.append(value.strftime('%Y-%m-%d'))
            elif value:
                dob_str = str(value).strip()
                if ' ' in dob_str:
                    dob_str = dob_str.split(' ')[0]
                try:
                    from datetime import datetime
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                        try:
                            dt = datetime.strptime(dob_str, fmt)
                            row.append(dt.strftime('%Y-%m-%d'))
                            break
                        except ValueError:
                            continue
                    else:
                        row.append(dob_str[:10])
                except:
                    row.append(dob_str[:10])
            else:
                row.append('')
        elif col == 'gender':
            # Normalize gender to M/F or empty
            if value:
                g = str(value).strip().lower()
                if g in ('m', 'male'):
                    row.append('M')
                elif g in ('f', 'female'):
                    row.append('F')
                else:
                    row.append('')
            else:
                row.append('')
        elif col in ('is_clicker', 'is_opener', 'is_validated'):
            # Normalize boolean fields
            if isinstance(value, bool):
                row.append('t' if value else 'f')
            elif value:
                v = str(value).lower().strip()
                if v in ('true', 't', 'yes', 'y', '1'):
                    row.append('t')
                else:
                    row.append('f')
            else:
                row.append('f')
        elif isinstance(value, bool):
            row.append('t' if value else 'f')
        elif col == 'quality_score' and isinstance(value, (int, float)):
            row.append(str(int(value)))
        else:
            # Escape special characters for COPY format
            str_val = str(value)
            str_val = str_val.replace('\\', '\\\\')
            str_val = str_val.replace('\t', '\\t')
            str_val = str_val.replace('\n', '\\n')
            str_val = str_val.replace('\r', '\\r')
            row.append(str_val)
    
    return row


def load_records_copy(
    records: List[Dict[str, Any]],
    table_name: str = 'emails'
) -> int:
    """
    Load records using PostgreSQL COPY command (fastest method).
    
    Returns:
        Number of records loaded
    """
    if not records:
        return 0
    
    columns = [
        'email', 'email_domain', 'email_provider', 'email_brand',
        'first_name', 'last_name', 'address',
        'city', 'state', 'zipcode', 'phone', 'dob', 'gender',
        'signup_date', 'signup_domain', 'signup_ip',
        'is_clicker', 'is_opener', 'validation_status',
        'email_category', 'quality_score', 'data_source', 'country', 'file_sources',
        'custom1', 'custom2', 'custom3', 'custom4', 'custom5'
    ]
    
    # Create buffer
    buffer = io.StringIO()
    
    for record in records:
        row = prepare_record_for_copy(record)
        buffer.write('\t'.join(row) + '\n')
    
    buffer.seek(0)
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Use COPY
            cursor.copy_from(
                buffer,
                table_name,
                columns=columns,
                null='\\N'
            )
            
            loaded = cursor.rowcount if cursor.rowcount > 0 else len(records)
            cursor.close()
            conn.commit()
            
            return loaded
            
    except Exception as e:
        logger.error(f"COPY load failed: {e}")
        # Fall back to batch insert
        return load_records_batch(records, on_conflict='skip')


# =============================================================================
# UPSERT LOADING
# =============================================================================

def upsert_records(
    records: List[Dict[str, Any]],
    batch_size: int = BATCH_SIZE_LOAD
) -> Tuple[int, int]:
    """
    Upsert records (insert or update existing).
    
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    inserted = 0
    updated = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        # Use bulk_insert_emails with update
        loaded = bulk_insert_emails(batch, on_conflict='update')
        
        # We can't easily distinguish inserts from updates here
        # so we just track total
        inserted += loaded
    
    return inserted, updated


# =============================================================================
# PROGRESS TRACKING
# =============================================================================

class LoaderProgress:
    """Track loading progress."""
    
    def __init__(self, total_records: int = 0):
        self.total_records = total_records
        self.loaded_records = 0
        self.failed_records = 0
        self.start_time = datetime.now()
    
    def update(self, loaded: int, failed: int = 0):
        """Update progress."""
        self.loaded_records += loaded
        self.failed_records += failed
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.loaded_records / elapsed if elapsed > 0 else 0
        
        remaining = self.total_records - self.loaded_records - self.failed_records
        eta_seconds = remaining / rate if rate > 0 else 0
        
        return {
            'loaded': self.loaded_records,
            'failed': self.failed_records,
            'total': self.total_records,
            'percent': round(self.loaded_records / self.total_records * 100, 1) if self.total_records > 0 else 0,
            'rate_per_second': round(rate, 0),
            'elapsed_seconds': round(elapsed, 0),
            'eta_seconds': round(eta_seconds, 0),
        }
    
    def log_progress(self):
        """Log current progress."""
        p = self.get_progress()
        logger.info(
            f"Progress: {p['loaded']:,}/{p['total']:,} ({p['percent']}%) | "
            f"Rate: {p['rate_per_second']:,.0f}/sec | "
            f"ETA: {p['eta_seconds']:.0f}s"
        )


# =============================================================================
# LOGGING HELPERS
# =============================================================================

def log_file_result(
    file_info: Dict[str, Any],
    records_loaded: int,
    records_processed: int,
    processing_time: float,
    errors: List[str] = None
):
    """Log file processing result to database."""
    
    log_file_processing(
        filename=file_info['filename'],
        filepath=str(file_info['path']),
        file_date=file_info.get('modified_date'),
        records_in_file=file_info.get('size_bytes', 0),  # Approximate
        records_processed=records_processed,
        records_loaded=records_loaded,
        schema_detected=file_info.get('schema_type', 'unknown'),
        data_source=file_info.get('data_source', 'Unknown'),
        is_clicker=file_info.get('is_clicker', False),
        is_opener=file_info.get('is_opener', False),
        processing_time=int(processing_time),
        errors='; '.join(errors) if errors else None
    )


def log_final_stats(
    files_processed: int,
    total_records: int,
    records_loaded: int,
    duplicates: int,
    invalid_emails: int,
    role_emails: int,
    country_tld: int,
    processing_time: int,
    notes: str = None
):
    """Log final processing statistics."""
    
    log_processing_stats(
        files_processed=files_processed,
        total_records_read=total_records,
        records_loaded=records_loaded,
        duplicates_found=duplicates,
        invalid_emails=invalid_emails,
        role_emails_filtered=role_emails,
        country_tld_filtered=country_tld,
        processing_time_seconds=processing_time,
        notes=notes
    )
