"""
Email Processing System - Cleaner Module
Handles email validation, cleaning, and filtering
"""

import re
import logging
from typing import List, Dict, Any, Tuple, Set
from datetime import datetime

try:
    import cupy as cp
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False

from config import (
    ROLE_EMAIL_PATTERNS, ROLE_PATTERNS_ANYWHERE, COUNTRY_TLDS
)

logger = logging.getLogger(__name__)

# =============================================================================
# EMAIL VALIDATION PATTERNS
# =============================================================================

# Basic email regex pattern
EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)

# Invalid email patterns
INVALID_PATTERNS = [
    re.compile(r'^\d+\.\d+\.\d+\.\d+$'),  # IP addresses
    re.compile(r'^[^@]+$'),  # No @ symbol
    re.compile(r'@.*@'),  # Multiple @ symbols
    re.compile(r'^[.\-_]'),  # Starts with special char
    re.compile(r'[.\-_]@'),  # Special char before @
    re.compile(r'\.{2,}'),  # Multiple consecutive dots
    re.compile(r'\s'),  # Contains whitespace
]

# Typo corrections for common domain mistakes
DOMAIN_TYPO_CORRECTIONS = {
    'gmial.com': 'gmail.com',
    'gmai.com': 'gmail.com',
    'gamil.com': 'gmail.com',
    'gmail.con': 'gmail.com',
    'gmail.co': 'gmail.com',
    'gmal.com': 'gmail.com',
    'hotmal.com': 'hotmail.com',
    'hotmai.com': 'hotmail.com',
    'hotmail.con': 'hotmail.com',
    'hotmial.com': 'hotmail.com',
    'yaho.com': 'yahoo.com',
    'yahooo.com': 'yahoo.com',
    'yahoo.con': 'yahoo.com',
    'yahho.com': 'yahoo.com',
    'outllok.com': 'outlook.com',
    'outlok.com': 'outlook.com',
    'outlook.con': 'outlook.com',
    'aoll.com': 'aol.com',
    'aol.con': 'aol.com',
    'comcast.ner': 'comcast.net',
    'comast.net': 'comcast.net',
}


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def is_valid_email_format(email: str) -> bool:
    """Check if email has valid format."""
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    
    # Check length
    if len(email) < 5 or len(email) > 320:
        return False
    
    # Must have exactly one @
    if email.count('@') != 1:
        return False
    
    # Check against invalid patterns
    for pattern in INVALID_PATTERNS:
        if pattern.search(email):
            return False
    
    # Check basic format
    if not EMAIL_PATTERN.match(email):
        return False
    
    return True


def is_role_email(email: str) -> bool:
    """Check if email is a role/trap account."""
    if not email:
        return False
    
    email_lower = email.lower()
    prefix = email_lower.split('@')[0]
    
    # Check if starts with role pattern
    for pattern in ROLE_EMAIL_PATTERNS:
        if email_lower.startswith(pattern):
            return True
    
    # Check if contains role patterns anywhere
    for pattern in ROLE_PATTERNS_ANYWHERE:
        if pattern in prefix:
            return True
    
    return False


def has_country_tld(email: str) -> bool:
    """Check if email has a country-specific TLD (excluding US)."""
    if not email:
        return False
    
    email_lower = email.lower()
    
    for tld in COUNTRY_TLDS:
        if email_lower.endswith(tld):
            return True
    
    return False


def fix_domain_typo(email: str) -> str:
    """Fix common domain typos."""
    if not email or '@' not in email:
        return email
    
    prefix, domain = email.rsplit('@', 1)
    domain_lower = domain.lower()
    
    if domain_lower in DOMAIN_TYPO_CORRECTIONS:
        return f"{prefix}@{DOMAIN_TYPO_CORRECTIONS[domain_lower]}"
    
    return email


# =============================================================================
# DATA CLEANING
# =============================================================================

def clean_email(email: str) -> str:
    """Clean and normalize an email address."""
    if not email:
        return None
    
    # Convert to string if needed
    email = str(email).strip().lower()
    
    # Remove common prefixes/suffixes that sometimes appear
    email = email.strip('<>"\' ')
    
    # Remove mailto: prefix
    if email.startswith('mailto:'):
        email = email[7:]
    
    # Fix common typos
    email = fix_domain_typo(email)
    
    return email if email else None


def clean_phone(phone: str) -> str:
    """Clean and normalize phone number."""
    if not phone:
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', str(phone))
    
    # US phone numbers should be 10 or 11 digits
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]  # Remove country code
    
    if len(digits) == 10:
        return digits
    
    # Return original cleaned if not standard format
    return digits if digits else None


def clean_name(name: str) -> str:
    """Clean and normalize a name."""
    if not name:
        return None
    
    name = str(name).strip()
    
    # Remove special characters except hyphen and apostrophe
    name = re.sub(r'[^a-zA-Z\s\-\']', '', name)
    
    # Title case
    name = name.title()
    
    # Remove if too short or looks invalid
    if len(name) < 2 or name.isdigit():
        return None
    
    return name if name else None


def clean_state(state: str) -> str:
    """Clean and normalize state to 2-letter abbreviation."""
    if not state:
        return None
    
    state = str(state).strip().upper()
    
    # If already 2 letters, return it
    if len(state) == 2 and state.isalpha():
        return state
    
    # Common state name to abbreviation mapping
    state_map = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC',
    }
    
    return state_map.get(state, state[:2] if len(state) >= 2 else None)


def clean_zipcode(zipcode: str) -> str:
    """Clean and normalize zipcode."""
    if not zipcode:
        return None
    
    # Get just digits
    digits = re.sub(r'\D', '', str(zipcode))
    
    # US ZIP codes are 5 or 9 digits
    if len(digits) >= 5:
        return digits[:5]  # Return just the 5-digit ZIP
    
    return None


def parse_date(date_str: str) -> datetime:
    """Try to parse a date string into datetime."""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Common date formats to try
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
        '%m/%d/%y',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%d-%m-%Y',
        '%B %d, %Y',
        '%b %d, %Y',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            continue
    
    return None


# =============================================================================
# QUALITY SCORING
# =============================================================================

# Suspicious patterns in email prefixes that indicate low quality
SUSPICIOUS_PATTERNS = ['test', 'fake', 'temp', 'xxx', '123', 'asdf', 'qwerty', 
                       'spam', 'trash', 'junk', 'noreply', 'donotreply']


def calculate_quality_score(record: Dict[str, Any]) -> int:
    """
    Calculate a quality/intent score for an email record.
    Higher scores indicate more likely to be a real, engaged person.
    
    Score ranges:
        80-100: High intent - clickers with complete data on major ISPs
        60-79: Good quality - real emails with some engagement signals
        40-59: Average - likely real but unverified
        20-39: Low quality - missing data, suspicious patterns
        0-19: Poor quality - likely invalid or disposable
    
    Args:
        record: Dictionary containing email record data
        
    Returns:
        Integer score from 0-100
    """
    score = 50  # Base score
    
    # =========================================================================
    # ENGAGEMENT SIGNALS (+20 max)
    # =========================================================================
    if record.get('is_clicker'):
        score += 15   # Clicked = proven engagement
    if record.get('is_opener'):
        score += 5    # Opened = some engagement
    
    # =========================================================================
    # DATA COMPLETENESS (+25 max)
    # =========================================================================
    if record.get('first_name'):
        score += 5
    if record.get('last_name'):
        score += 5
    if record.get('phone'):
        score += 7    # Phone = more committed
    if record.get('address'):
        score += 3
    if record.get('city') and record.get('state'):
        score += 3
    if record.get('zipcode'):
        score += 2
    
    # =========================================================================
    # VALIDATION STATUS (+15 max / -20 penalty)
    # =========================================================================
    validation = str(record.get('validation_status', '')).lower()
    if validation in ('valid', 'deliverable', 'ok', 'good', 'verified'):
        score += 15
    elif validation in ('risky', 'unknown', 'catchall', 'catch-all', 'accept-all'):
        score += 5
    elif validation in ('invalid', 'undeliverable', 'bad', 'bounce', 'hard_bounce'):
        score -= 20
    
    # =========================================================================
    # DOMAIN QUALITY (+10 max)
    # =========================================================================
    category = record.get('email_category')
    if category == 'Big4_ISP':
        score += 10   # Real ISPs = real people
    elif category == 'Cable_Provider':
        score += 8    # Cable/ISP = real people
    # General_Internet: no bonus (could be corporate or throwaway)
    
    # =========================================================================
    # EMAIL FORMAT ANALYSIS (-30 max penalty)
    # =========================================================================
    email = str(record.get('email', ''))
    prefix = email.split('@')[0] if '@' in email else ''
    
    if prefix:
        # Random-looking emails (lots of numbers)
        digit_count = sum(c.isdigit() for c in prefix)
        digit_ratio = digit_count / len(prefix)
        if digit_ratio > 0.5:
            score -= 15   # Too many numbers (like abc123456789@)
        elif digit_ratio > 0.3:
            score -= 5    # Moderate numbers
        
        # Very short or very long prefixes
        if len(prefix) < 4:
            score -= 10   # Likely fake (a@, ab@, abc@)
        if len(prefix) > 30:
            score -= 5    # Suspicious length
        
        # Contains suspicious patterns
        prefix_lower = prefix.lower()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in prefix_lower:
                score -= 15
                break  # Only penalize once
        
        # All same character (aaaa@, 11111@)
        if len(set(prefix)) <= 2 and len(prefix) > 3:
            score -= 10
    
    # =========================================================================
    # RECENCY BONUS
    # =========================================================================
    # If signup_date exists and is recent (within last 2 years), small bonus
    signup_date = record.get('signup_date')
    if signup_date:
        try:
            from datetime import datetime, timedelta
            if isinstance(signup_date, datetime):
                age_days = (datetime.now() - signup_date).days
                if age_days < 365:  # Less than 1 year old
                    score += 5
                elif age_days < 730:  # Less than 2 years old
                    score += 2
        except:
            pass  # Ignore date parsing errors
    
    # Clamp to 0-100
    return max(0, min(100, score))


def score_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calculate quality scores for a list of records.
    Updates the 'quality_score' field in each record.
    
    Returns:
        The same records with quality_score assigned
    """
    for record in records:
        record['quality_score'] = calculate_quality_score(record)
    return records


# =============================================================================
# BATCH CLEANING
# =============================================================================

def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Clean a single record."""
    
    # Clean email
    record['email'] = clean_email(record.get('email'))
    
    # Update domain after cleaning
    if record['email'] and '@' in record['email']:
        record['email_domain'] = record['email'].split('@')[-1]
    
    # Clean other fields
    record['first_name'] = clean_name(record.get('first_name'))
    record['last_name'] = clean_name(record.get('last_name'))
    record['phone'] = clean_phone(record.get('phone'))
    record['state'] = clean_state(record.get('state'))
    record['zipcode'] = clean_zipcode(record.get('zipcode'))
    
    # Clean city
    if record.get('city'):
        record['city'] = str(record['city']).strip().title()
    
    # Parse signup date if it's a string
    if record.get('signup_date') and isinstance(record['signup_date'], str):
        record['signup_date'] = parse_date(record['signup_date'])
    
    return record


def validate_record(record: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate a record and return (is_valid, reason).
    
    Returns:
        Tuple of (is_valid, rejection_reason or None)
    """
    email = record.get('email')
    
    # Must have email
    if not email:
        return False, 'missing_email'
    
    # Check format
    if not is_valid_email_format(email):
        return False, 'invalid_format'
    
    # Check for role emails
    if is_role_email(email):
        return False, 'role_email'
    
    # Check for country TLDs
    if has_country_tld(email):
        return False, 'country_tld'
    
    return True, None


def clean_and_validate_records(
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Clean and validate a list of records.
    
    Returns:
        Tuple of (valid_records, rejection_counts)
    """
    valid_records = []
    rejection_counts = {
        'missing_email': 0,
        'invalid_format': 0,
        'role_email': 0,
        'country_tld': 0,
    }
    
    for record in records:
        # Clean the record
        record = clean_record(record)
        
        # Validate
        is_valid, reason = validate_record(record)
        
        if is_valid:
            valid_records.append(record)
        else:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
    
    return valid_records, rejection_counts


# =============================================================================
# GPU-ACCELERATED VALIDATION (CuPy)
# =============================================================================

def validate_emails_gpu(emails: List[str]) -> List[bool]:
    """
    Validate emails using GPU acceleration where possible.
    Falls back to CPU if GPU not available.
    
    Returns:
        List of boolean values indicating validity
    """
    if not GPU_AVAILABLE or len(emails) < 10000:
        # Use CPU for small batches or if GPU not available
        return [is_valid_email_format(e) for e in emails]
    
    try:
        # Convert to numpy for GPU processing
        import numpy as np
        
        # For now, we'll use CPU validation but structure is ready for GPU
        # GPU regex is complex; we'll use GPU for other operations
        return [is_valid_email_format(e) for e in emails]
        
    except Exception as e:
        logger.warning(f"GPU validation failed, falling back to CPU: {e}")
        return [is_valid_email_format(e) for e in emails]
