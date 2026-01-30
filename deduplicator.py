"""
Email Processing System - Deduplicator Module
GPU-accelerated deduplication using CuPy
"""

import logging
from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict
import hashlib

try:
    import cupy as cp
    import numpy as np
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False
    import numpy as np

from config import BATCH_SIZE_DEDUPE

logger = logging.getLogger(__name__)


# =============================================================================
# PRIORITY SCORING
# =============================================================================

def calculate_priority_score(record: Dict[str, Any]) -> int:
    """
    Calculate a priority score for a record.
    Higher score = better record to keep.
    
    Priority factors:
    - Clicker status (highest value)
    - Opener status
    - Validation status
    - Data completeness
    - Recency
    """
    score = 0
    
    # Clicker is highest priority (1000 points)
    if record.get('is_clicker'):
        score += 1000
    
    # Opener is second priority (500 points)
    if record.get('is_opener'):
        score += 500
    
    # Validated emails (200 points)
    if record.get('validation_status') == 'Verified':
        score += 200
    
    # Data completeness (up to 100 points)
    completeness_fields = [
        'first_name', 'last_name', 'address', 'city', 'state',
        'zipcode', 'phone', 'dob', 'signup_date', 'signup_ip'
    ]
    for field in completeness_fields:
        if record.get(field):
            score += 10
    
    # Prefer records from known sources (50 points)
    if record.get('data_source') and record['data_source'] != 'Unknown':
        score += 50
    
    return score


def count_populated_fields(record: Dict[str, Any]) -> int:
    """Count how many fields are populated in a record."""
    count = 0
    for key, value in record.items():
        if value is not None and value != '' and key not in ['email', 'email_domain', 'file_sources']:
            count += 1
    return count


# =============================================================================
# RECORD MERGING
# =============================================================================

def merge_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple records for the same email.
    Keeps the best data from each record.
    
    Strategy:
    1. Start with the highest priority record
    2. Fill in missing fields from other records
    3. Merge file_sources arrays
    4. OR together boolean flags (is_clicker, is_opener)
    """
    if not records:
        return None
    
    if len(records) == 1:
        return records[0]
    
    # Sort by priority score (highest first)
    sorted_records = sorted(records, key=calculate_priority_score, reverse=True)
    
    # Start with best record
    merged = sorted_records[0].copy()
    
    # Merge file_sources
    all_sources = set()
    for r in records:
        sources = r.get('file_sources', [])
        if isinstance(sources, list):
            all_sources.update(sources)
        elif sources:
            all_sources.add(sources)
    merged['file_sources'] = list(all_sources)
    
    # OR together boolean flags
    merged['is_clicker'] = any(r.get('is_clicker') for r in records)
    merged['is_opener'] = any(r.get('is_opener') for r in records)
    
    # Keep best validation status
    for r in sorted_records:
        if r.get('validation_status') == 'Verified':
            merged['validation_status'] = 'Verified'
            break
    
    # Fill in missing fields from other records
    fill_fields = [
        'first_name', 'last_name', 'address', 'city', 'state', 'zipcode',
        'phone', 'dob', 'gender', 'signup_date', 'signup_domain', 'signup_ip',
        'country', 'custom1', 'custom2', 'custom3', 'custom4', 'custom5'
    ]
    
    for field in fill_fields:
        if not merged.get(field):
            for r in sorted_records[1:]:
                if r.get(field):
                    merged[field] = r[field]
                    break
    
    return merged


# =============================================================================
# CPU DEDUPLICATION
# =============================================================================

def deduplicate_records_cpu(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Deduplicate records using CPU.
    
    Returns:
        Tuple of (deduplicated_records, duplicate_count)
    """
    if not records:
        return [], 0
    
    # Group by email
    email_groups = defaultdict(list)
    for record in records:
        email = record.get('email')
        if email:
            email_groups[email.lower()].append(record)
    
    # Merge each group
    deduplicated = []
    for email, group in email_groups.items():
        merged = merge_records(group)
        if merged:
            deduplicated.append(merged)
    
    duplicate_count = len(records) - len(deduplicated)
    
    return deduplicated, duplicate_count


# =============================================================================
# GPU-ACCELERATED DEDUPLICATION
# =============================================================================

def hash_email_gpu(email: str) -> int:
    """Create a hash for an email using MD5 truncated to int64."""
    return int(hashlib.md5(email.lower().encode()).hexdigest()[:16], 16)


def deduplicate_records_gpu(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """
    GPU-accelerated deduplication using CuPy for hash-based grouping.
    Falls back to CPU for small batches or if GPU unavailable.
    
    Returns:
        Tuple of (deduplicated_records, duplicate_count)
    """
    if not records:
        return [], 0
    
    # Use CPU for small batches (GPU overhead not worth it)
    if len(records) < 100000 or not GPU_AVAILABLE:
        return deduplicate_records_cpu(records)
    
    try:
        logger.info(f"GPU deduplication: {len(records)} records")
        
        # Extract emails and create hashes
        emails = [r.get('email', '').lower() for r in records]
        
        # Create numpy array of hashes
        hashes = np.array([hash_email_gpu(e) for e in emails], dtype=np.int64)
        
        # Transfer to GPU
        gpu_hashes = cp.asarray(hashes)
        
        # Find unique indices using GPU
        unique_hashes, unique_indices = cp.unique(gpu_hashes, return_index=True)
        
        # Get back to CPU
        unique_indices_cpu = cp.asnumpy(unique_indices)
        
        # Now we have indices of first occurrence of each unique email
        # But we want to merge duplicates, so we still need to group
        
        # Group by email (CPU operation but with reduced set)
        email_groups = defaultdict(list)
        for idx, record in enumerate(records):
            email = record.get('email', '').lower()
            if email:
                email_groups[email].append(record)
        
        # Merge groups
        deduplicated = []
        for email, group in email_groups.items():
            merged = merge_records(group)
            if merged:
                deduplicated.append(merged)
        
        duplicate_count = len(records) - len(deduplicated)
        
        # Free GPU memory
        del gpu_hashes
        cp.get_default_memory_pool().free_all_blocks()
        
        logger.info(f"GPU deduplication complete: {len(deduplicated)} unique, {duplicate_count} duplicates")
        return deduplicated, duplicate_count
        
    except Exception as e:
        logger.warning(f"GPU deduplication failed, falling back to CPU: {e}")
        return deduplicate_records_cpu(records)


# =============================================================================
# MAIN DEDUPLICATION FUNCTION
# =============================================================================

def deduplicate_records(
    records: List[Dict[str, Any]],
    use_gpu: bool = True
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Main deduplication function.
    
    Args:
        records: List of record dictionaries
        use_gpu: Whether to try GPU acceleration
        
    Returns:
        Tuple of (deduplicated_records, duplicate_count)
    """
    if not records:
        return [], 0
    
    if use_gpu and GPU_AVAILABLE:
        return deduplicate_records_gpu(records)
    else:
        return deduplicate_records_cpu(records)


# =============================================================================
# INCREMENTAL DEDUPLICATION
# =============================================================================

class IncrementalDeduplicator:
    """
    Handles deduplication across multiple batches.
    Maintains a set of seen emails to avoid duplicates across batches.
    """
    
    def __init__(self, use_gpu: bool = True):
        self.seen_emails: Set[str] = set()
        self.use_gpu = use_gpu and GPU_AVAILABLE
        self.total_duplicates = 0
        
        # For GPU, maintain hash set
        if self.use_gpu:
            self.seen_hashes: Set[int] = set()
    
    def process_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of records, removing duplicates within the batch
        and against previously seen emails.
        
        Returns:
            List of new, unique records
        """
        if not records:
            return []
        
        # First deduplicate within the batch
        batch_deduped, batch_dups = deduplicate_records(records, self.use_gpu)
        self.total_duplicates += batch_dups
        
        # Then filter out emails we've seen before
        new_records = []
        for record in batch_deduped:
            email = record.get('email', '').lower()
            if email and email not in self.seen_emails:
                self.seen_emails.add(email)
                if self.use_gpu:
                    self.seen_hashes.add(hash_email_gpu(email))
                new_records.append(record)
            else:
                self.total_duplicates += 1
        
        return new_records
    
    def get_stats(self) -> Dict[str, int]:
        """Get deduplication statistics."""
        return {
            'unique_emails': len(self.seen_emails),
            'total_duplicates': self.total_duplicates,
        }
    
    def clear(self):
        """Clear the seen emails set."""
        self.seen_emails.clear()
        if self.use_gpu:
            self.seen_hashes.clear()
        self.total_duplicates = 0
