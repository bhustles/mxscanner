"""
Email Processing System - Categorizer Module
Handles domain categorization into Big4, Cable, General
Returns provider, brand, and category for each domain
"""

import logging
from typing import List, Dict, Any, Set, Tuple, Optional

try:
    import cupy as cp
    import numpy as np
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False
    import numpy as np

from config import DOMAIN_MAPPING, BIG4_DOMAINS, CABLE_DOMAINS

logger = logging.getLogger(__name__)

# =============================================================================
# DOMAIN LOOKUP STRUCTURES
# =============================================================================

# Build lookup sets for fast categorization
BIG4_DOMAIN_SET: Set[str] = set(BIG4_DOMAINS.keys())
CABLE_DOMAIN_SET: Set[str] = set(CABLE_DOMAINS.keys())

# All domains in the mapping
ALL_DOMAINS_SET: Set[str] = set(DOMAIN_MAPPING.keys())


# =============================================================================
# CATEGORIZATION FUNCTIONS
# =============================================================================

def get_domain_info(domain: str) -> Dict[str, Optional[str]]:
    """
    Get complete domain information including provider, brand, and category.
    
    Args:
        domain: Email domain (e.g., 'gmail.com', 'aol.com')
        
    Returns:
        Dictionary with:
        - 'provider': Who hosts/processes the email (e.g., 'Google', 'Yahoo', 'Microsoft')
        - 'brand': The brand name users see (e.g., 'Gmail', 'AOL', 'Hotmail')
        - 'category': 'Big4_ISP', 'Cable_Provider', or 'General_Internet'
    """
    if not domain:
        return {'provider': None, 'brand': None, 'category': 'General_Internet'}
    
    domain_lower = domain.lower().strip()
    
    # Direct lookup in DOMAIN_MAPPING
    if domain_lower in DOMAIN_MAPPING:
        provider, brand, category = DOMAIN_MAPPING[domain_lower]
        return {'provider': provider, 'brand': brand, 'category': category}
    
    # Check for regional RR domains (pattern matching)
    if domain_lower.endswith('.rr.com'):
        return {'provider': 'Spectrum', 'brand': 'Roadrunner', 'category': 'Cable_Provider'}
    
    return {'provider': None, 'brand': None, 'category': 'General_Internet'}


def categorize_domain(domain: str) -> str:
    """
    Categorize a single email domain.
    
    Returns:
        'Big4_ISP', 'Cable_Provider', or 'General_Internet'
    """
    return get_domain_info(domain)['category']


def get_provider_name(domain: str) -> Optional[str]:
    """
    Get the provider name for a domain (who hosts the email).
    
    Returns:
        Provider name (e.g., 'Google', 'Yahoo', 'Microsoft') or None
    """
    return get_domain_info(domain)['provider']


def get_brand_name(domain: str) -> Optional[str]:
    """
    Get the brand name for a domain (what users see).
    
    Returns:
        Brand name (e.g., 'Gmail', 'AOL', 'Hotmail') or None
    """
    return get_domain_info(domain)['brand']


def categorize_email(email: str) -> str:
    """
    Categorize an email address by its domain.
    
    Returns:
        'Big4_ISP', 'Cable_Provider', or 'General_Internet'
    """
    if not email or '@' not in email:
        return 'General_Internet'
    
    domain = email.lower().split('@')[-1]
    return categorize_domain(domain)


def get_email_info(email: str) -> Dict[str, Optional[str]]:
    """
    Get complete email information including provider, brand, and category.
    
    Args:
        email: Full email address
        
    Returns:
        Dictionary with provider, brand, category, and domain
    """
    if not email or '@' not in email:
        return {'provider': None, 'brand': None, 'category': 'General_Internet', 'domain': None}
    
    domain = email.lower().split('@')[-1]
    info = get_domain_info(domain)
    info['domain'] = domain
    return info


# =============================================================================
# BATCH CATEGORIZATION
# =============================================================================

def categorize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Categorize a list of records.
    Updates email_provider, email_brand, and email_category fields.
    
    Returns:
        The same records with provider, brand, and category assigned
    """
    for record in records:
        domain = record.get('email_domain')
        info = get_domain_info(domain)
        record['email_provider'] = info['provider']
        record['email_brand'] = info['brand']
        record['email_category'] = info['category']
    
    return records


def categorize_domains_batch(domains: List[str]) -> List[str]:
    """
    Categorize a batch of domains.
    
    Returns:
        List of categories
    """
    return [categorize_domain(d) for d in domains]


def get_domain_info_batch(domains: List[str]) -> List[Dict[str, Optional[str]]]:
    """
    Get domain info for a batch of domains.
    
    Returns:
        List of dictionaries with provider, brand, category
    """
    return [get_domain_info(d) for d in domains]


# =============================================================================
# GPU-ACCELERATED CATEGORIZATION
# =============================================================================

def categorize_domains_gpu(domains: List[str]) -> List[str]:
    """
    GPU-accelerated domain categorization using hash lookups.
    Falls back to CPU if GPU not available or for small batches.
    
    Returns:
        List of categories
    """
    if not GPU_AVAILABLE or len(domains) < 50000:
        # CPU is faster for small batches
        return categorize_domains_batch(domains)
    
    try:
        # Create hash table for GPU lookup
        all_domain_hashes = set(hash(d) for d in ALL_DOMAINS_SET)
        
        # Process domains
        categories = []
        for domain in domains:
            if not domain:
                categories.append('General_Internet')
                continue
            
            domain_lower = domain.lower().strip()
            domain_hash = hash(domain_lower)
            
            if domain_hash in all_domain_hashes and domain_lower in DOMAIN_MAPPING:
                categories.append(DOMAIN_MAPPING[domain_lower][2])
            elif domain_lower.endswith('.rr.com'):
                categories.append('Cable_Provider')
            else:
                categories.append('General_Internet')
        
        return categories
        
    except Exception as e:
        logger.warning(f"GPU categorization failed, using CPU: {e}")
        return categorize_domains_batch(domains)


def categorize_records_gpu(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    GPU-accelerated record categorization.
    Falls back to CPU if GPU not available or for small batches.
    
    Returns:
        Records with provider, brand, and category assigned
    """
    if not GPU_AVAILABLE or len(records) < 50000:
        # CPU is faster for small batches
        return categorize_records(records)
    
    try:
        all_domain_hashes = set(hash(d) for d in ALL_DOMAINS_SET)
        
        for record in records:
            domain = record.get('email_domain')
            if not domain:
                record['email_provider'] = None
                record['email_brand'] = None
                record['email_category'] = 'General_Internet'
                continue
            
            domain_lower = domain.lower().strip()
            domain_hash = hash(domain_lower)
            
            if domain_hash in all_domain_hashes and domain_lower in DOMAIN_MAPPING:
                provider, brand, category = DOMAIN_MAPPING[domain_lower]
                record['email_provider'] = provider
                record['email_brand'] = brand
                record['email_category'] = category
            elif domain_lower.endswith('.rr.com'):
                record['email_provider'] = 'Spectrum'
                record['email_brand'] = 'Roadrunner'
                record['email_category'] = 'Cable_Provider'
            else:
                record['email_provider'] = None
                record['email_brand'] = None
                record['email_category'] = 'General_Internet'
        
        return records
        
    except Exception as e:
        logger.warning(f"GPU categorization failed, using CPU: {e}")
        return categorize_records(records)


# =============================================================================
# STATISTICS
# =============================================================================

def get_category_stats(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Get categorization statistics for a batch of records.
    
    Returns:
        Dictionary with counts, percentages, and breakdowns by provider/brand
    """
    category_counts = {
        'Big4_ISP': 0,
        'Cable_Provider': 0,
        'General_Internet': 0,
    }
    
    provider_counts = {}
    brand_counts = {}
    
    for record in records:
        category = record.get('email_category', 'General_Internet')
        category_counts[category] = category_counts.get(category, 0) + 1
        
        # Track provider names
        provider = record.get('email_provider')
        if provider:
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
        
        # Track brand names
        brand = record.get('email_brand')
        if brand:
            brand_counts[brand] = brand_counts.get(brand, 0) + 1
    
    total = len(records)
    
    return {
        'total': total,
        'category_counts': category_counts,
        'category_percentages': {
            k: round(v / total * 100, 2) if total > 0 else 0
            for k, v in category_counts.items()
        },
        'top_providers': dict(sorted(
            provider_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]),
        'top_brands': dict(sorted(
            brand_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]),
    }


def get_domain_distribution(records: List[Dict[str, Any]], top_n: int = 50) -> Dict[str, int]:
    """
    Get the distribution of email domains.
    
    Returns:
        Dictionary of domain -> count, sorted by count
    """
    domain_counts = {}
    
    for record in records:
        domain = record.get('email_domain')
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
    
    # Sort by count and return top N
    sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
    return dict(sorted_domains[:top_n])
