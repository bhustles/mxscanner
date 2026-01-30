"""
MX Domain Validator
Validates email domains by checking MX records and classifies by mail host
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import psycopg2
import threading
import time
import json
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any, Generator
from dataclasses import dataclass, asdict

from config import DATABASE
from dns_pool import get_dns_pool, resolve_mx
from mx_patterns import classify_mx

# DNS server IP -> display name (for persistent stats)
DNS_SERVER_DISPLAY = {
    '8.8.8.8': 'Google-1', '8.8.4.4': 'Google-2',
    '1.1.1.1': 'Cloudflare-1', '1.0.0.1': 'Cloudflare-2',
    '208.67.222.222': 'OpenDNS-1', '208.67.220.220': 'OpenDNS-2',
    '9.9.9.10': 'Quad9-1', '149.112.112.10': 'Quad9-2',
    '4.2.2.1': 'Level3-1', '4.2.2.2': 'Level3-2',
    '64.6.64.6': 'Verisign-1', '64.6.65.6': 'Verisign-2',
}

# =============================================================================
# SKIP DOMAINS - Built from config.py (Big4 + Cable = not GI)
# =============================================================================

# Import from config.py - any domain in DOMAIN_MAPPING is NOT General Internet
from config import DOMAIN_MAPPING

# SKIP_DOMAINS = all domains from config.py (Big4_ISP + Cable_Provider)
SKIP_DOMAINS = set(DOMAIN_MAPPING.keys())

print(f"[MX Validator] Loaded {len(SKIP_DOMAINS)} Big4/Cable domains from config.py to skip", flush=True)

# =============================================================================
# VALIDATOR STATE
# =============================================================================

@dataclass
class ValidatorState:
    """Current state of the validator."""
    status: str = 'idle'  # idle, running, paused, stopping, complete
    total_domains: int = 0
    checked: int = 0
    valid: int = 0
    dead: int = 0
    valid_emails: int = 0  # Running count of emails on valid domains
    dead_emails: int = 0  # Running count of emails on dead domains
    errors: int = 0
    rate: float = 0.0
    start_time: Optional[float] = None
    categories: Dict[str, int] = None
    
    def __post_init__(self):
        if self.categories is None:
            self.categories = {
                'Google': 0, 'Microsoft': 0, 'Yahoo': 0,
                'General_Internet': 0, 'Real_GI': 0, 
                'Dead': 0, 'Parked': 0, 'Other': 0
            }
    
    def to_dict(self):
        return asdict(self)


# Global state
_state = ValidatorState()
_state_lock = threading.Lock()
_stop_event = threading.Event()
_pause_event = threading.Event()
_log_queue = queue.Queue(maxsize=1000)


def get_state() -> ValidatorState:
    """Get current validator state."""
    with _state_lock:
        return ValidatorState(**_state.to_dict())


def reset_state():
    """Reset state for a new run."""
    global _state
    with _state_lock:
        _state = ValidatorState()
    _stop_event.clear()
    _pause_event.clear()
    # Clear log queue
    while not _log_queue.empty():
        try:
            _log_queue.get_nowait()
        except:
            break


def load_stats_from_db(count_unchecked: bool = False):
    """
    Load current stats from the database.
    This allows stats to persist across Flask restarts.
    
    Args:
        count_unchecked: If True, count unchecked domains (slow). If False, just load domain_mx stats (fast).
    """
    global _state
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Get checked stats: only rows we've looked up (checked_at set). Don't count unchecked.
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE checked_at IS NOT NULL) as checked,
                SUM(CASE WHEN checked_at IS NOT NULL AND is_valid THEN 1 ELSE 0 END) as valid,
                SUM(CASE WHEN checked_at IS NOT NULL AND NOT is_valid THEN 1 ELSE 0 END) as dead,
                SUM(CASE WHEN checked_at IS NOT NULL AND is_valid THEN email_count ELSE 0 END) as valid_emails,
                SUM(CASE WHEN checked_at IS NOT NULL AND NOT is_valid THEN email_count ELSE 0 END) as dead_emails
            FROM domain_mx
        """)
        row = cursor.fetchone()
        checked = row[0] or 0
        valid = row[1] or 0
        dead = row[2] or 0
        valid_emails = row[3] or 0
        dead_emails = row[4] or 0
        
        # Get category breakdown (only checked domains)
        cursor.execute("""
            SELECT mx_category, COUNT(*) 
            FROM domain_mx 
            WHERE mx_category IS NOT NULL AND checked_at IS NOT NULL
            GROUP BY mx_category
        """)
        categories = {
            'Google': 0, 'Microsoft': 0, 'Yahoo': 0,
            'HostGator': 0, 'GoDaddy': 0, '1and1': 0,
            'Zoho': 0, 'Real_GI': 0, 'Dead': 0, 'Parked': 0, 'Other': 0
        }
        for cat, count in cursor.fetchall():
            if cat in categories:
                categories[cat] = count
            else:
                categories['Other'] = categories.get('Other', 0) + count
        # Dead card must match summary: use is_valid count, not mx_category='Dead'
        categories['Dead'] = dead
        
        cursor.close()
        conn.close()
        
        # Update state
        with _state_lock:
            _state.checked = checked
            _state.valid = valid
            _state.dead = dead
            _state.valid_emails = valid_emails
            _state.dead_emails = dead_emails
            _state.categories = categories
            # Keep existing total if we have one, otherwise estimate from checked
            if _state.total_domains == 0:
                _state.total_domains = checked  # Will be updated when scan starts
        
        print(f"Loaded stats: {checked:,} checked, {valid:,} valid, {dead:,} dead", flush=True)
        return True
    except Exception as e:
        print(f"Error loading stats from DB: {e}")
        return False


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_db():
    """Get a database connection."""
    return psycopg2.connect(**DATABASE)


def create_domain_mx_table():
    """Create the domain_mx table if it doesn't exist."""
    print("  - Getting DB connection...", flush=True)
    conn = get_db()
    cursor = conn.cursor()
    
    print("  - Creating domain_mx table...", flush=True)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domain_mx (
            domain VARCHAR(255) PRIMARY KEY,
            mx_records TEXT[],
            mx_priority INT[],
            mx_primary VARCHAR(255),
            mx_category VARCHAR(50),
            mx_host_provider VARCHAR(100),
            is_valid BOOLEAN DEFAULT true,
            email_count INT,
            checked_at TIMESTAMP,
            error_message TEXT
        )
    """)
    
    # Check if domain_mx indexes exist (fast check)
    cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'domain_mx'")
    existing_indexes = [row[0] for row in cursor.fetchall()]
    
    if 'idx_domain_mx_category' not in existing_indexes:
        print("  - Creating idx_domain_mx_category...", flush=True)
        cursor.execute("CREATE INDEX idx_domain_mx_category ON domain_mx(mx_category)")
    if 'idx_domain_mx_valid' not in existing_indexes:
        print("  - Creating idx_domain_mx_valid...", flush=True)
        cursor.execute("CREATE INDEX idx_domain_mx_valid ON domain_mx(is_valid)")
    if 'idx_domain_mx_provider' not in existing_indexes:
        print("  - Creating idx_domain_mx_provider...", flush=True)
        cursor.execute("CREATE INDEX idx_domain_mx_provider ON domain_mx(mx_host_provider)")
    print("  - Domain_mx indexes ready", flush=True)
    
    # Add dns_server column to domain_mx if not exists (for persistent DNS stats)
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='domain_mx' AND column_name='dns_server') THEN
                ALTER TABLE domain_mx ADD COLUMN dns_server VARCHAR(50);
            END IF;
        END $$;
    """)
    # Add is_gi column so we only scan General Internet domains (~500K), not all 3.7M
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='domain_mx' AND column_name='is_gi') THEN
                ALTER TABLE domain_mx ADD COLUMN is_gi BOOLEAN DEFAULT false;
            END IF;
        END $$;
    """)
    
    print("  - Ensuring emails columns exist...", flush=True)
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='emails' AND column_name='mx_category') THEN
                ALTER TABLE emails ADD COLUMN mx_category VARCHAR(50);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='emails' AND column_name='mx_valid') THEN
                ALTER TABLE emails ADD COLUMN mx_valid BOOLEAN DEFAULT true;
            END IF;
        END $$;
    """)
    
    # Check if indexes exist before trying to create (faster than CREATE IF NOT EXISTS on large tables)
    cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'emails' AND indexname = 'idx_emails_domain'")
    if not cursor.fetchone():
        print("  - Creating emails domain index (this may take a while on large tables)...", flush=True)
        cursor.execute("CREATE INDEX idx_emails_domain ON emails(email_domain)")
    else:
        print("  - Emails domain index already exists", flush=True)
    
    cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'emails' AND indexname = 'idx_emails_category_domain'")
    if not cursor.fetchone():
        print("  - Creating emails category_domain index...", flush=True)
        cursor.execute("CREATE INDEX idx_emails_category_domain ON emails(email_category, email_domain)")
    else:
        print("  - Emails category_domain index already exists", flush=True)
    
    print("  - Committing...", flush=True)
    conn.commit()
    cursor.close()
    conn.close()
    print("  - Table setup complete!", flush=True)


def get_gi_domains() -> List[tuple]:
    """
    Get unique General Internet domains only (not Big4/Cable) with their email counts.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT email_domain, COUNT(*) as email_count
        FROM emails 
        WHERE email_domain IS NOT NULL
          AND email_domain != ''
          AND email_category = 'General_Internet'
        GROUP BY email_domain
        ORDER BY email_count DESC
    """)
    domains = cursor.fetchall()
    cursor.close()
    conn.close()
    return domains


def get_unchecked_domains(limit: int = None, only_gi: bool = True) -> List[tuple]:
    """
    Get domains that haven't been MX-checked yet.
    
    SIMPLE LOGIC: If domain is NOT in SKIP_DOMAINS (from config.py), it's GI.
    No need for is_gi flag - config.py is the source of truth.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    print("DEBUG: Checking domain_mx table...", flush=True)
    cursor.execute("SELECT COUNT(*) FROM domain_mx")
    total_domains = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE checked_at IS NOT NULL")
    already_checked = cursor.fetchone()[0]
    print(f"DEBUG: domain_mx total: {total_domains:,}, already checked: {already_checked:,}", flush=True)
    print(f"DEBUG: Skipping {len(SKIP_DOMAINS)} Big4/Cable domains from config.py", flush=True)
    
    skip_tuple = tuple(SKIP_DOMAINS) if SKIP_DOMAINS else ('__none__',)
    
    # Simple: unchecked domains that are NOT Big4/Cable (from config.py)
    sql = """
        SELECT domain, COALESCE(email_count, 0)
        FROM domain_mx
        WHERE checked_at IS NULL
          AND domain NOT IN %s
        ORDER BY email_count DESC
    """
    if limit:
        sql += f" LIMIT {limit}"
    
    cursor.execute(sql, (skip_tuple,))
    domains = cursor.fetchall()
    print(f"DEBUG: Unchecked GI domains to scan: {len(domains):,}", flush=True)
    
    cursor.close()
    conn.close()
    return domains


# =============================================================================
# BATCHED DATABASE WRITES - Much faster than one-by-one
# =============================================================================
_write_buffer = []
_write_buffer_lock = threading.Lock()
_shared_conn = None

def _get_shared_conn():
    """Get or create shared database connection."""
    global _shared_conn
    if _shared_conn is None or _shared_conn.closed:
        _shared_conn = get_db()
    return _shared_conn

def flush_write_buffer():
    """Flush buffered results to database in one batch. Call at end of scan."""
    global _write_buffer
    
    with _write_buffer_lock:
        if not _write_buffer:
            return 0
        batch = _write_buffer.copy()
        _write_buffer = []
    
    return _write_batch_to_db(batch)

def _write_batch_to_db(batch):
    """Write a batch of results to the database."""
    if not batch:
        return 0
    
    try:
        conn = _get_shared_conn()
        cursor = conn.cursor()
        
        from psycopg2.extras import execute_values
        
        values = []
        for item in batch:
            domain = item['domain']
            mx_records = item['mx_records']
            category = item['category']
            provider = item['provider']
            email_count = item['email_count']
            error_message = item.get('error')
            dns_server = item.get('dns_server')
            
            if mx_records is None:
                values.append((domain, None, None, None, 'Dead', 'NXDOMAIN', False, email_count, error_message or 'NXDOMAIN', dns_server))
            elif len(mx_records) == 0:
                values.append((domain, [], [], None, 'Dead', 'No MX', False, email_count, 'No MX records', dns_server))
            else:
                mx_hosts = [mx[1] for mx in mx_records]
                mx_priorities = [mx[0] for mx in mx_records]
                primary_mx = mx_hosts[0] if mx_hosts else None
                values.append((domain, mx_hosts, mx_priorities, primary_mx, category, provider, True, email_count, None, dns_server))
        
        execute_values(cursor, """
            INSERT INTO domain_mx (domain, mx_records, mx_priority, mx_primary,
                                   mx_category, mx_host_provider, is_valid,
                                   email_count, error_message, dns_server)
            VALUES %s
            ON CONFLICT (domain) DO UPDATE SET
                mx_records = EXCLUDED.mx_records,
                mx_priority = EXCLUDED.mx_priority,
                mx_primary = EXCLUDED.mx_primary,
                mx_category = EXCLUDED.mx_category,
                mx_host_provider = EXCLUDED.mx_host_provider,
                is_valid = EXCLUDED.is_valid,
                email_count = EXCLUDED.email_count,
                checked_at = NOW(),
                error_message = EXCLUDED.error_message,
                dns_server = EXCLUDED.dns_server
        """, values, page_size=500)
        
        conn.commit()
        cursor.close()
        
        # Count valid/dead in this batch
        try:
            valid_count = sum(1 for v in values if v[6] == True)  # is_valid is index 6
            dead_count = len(values) - valid_count
        except:
            valid_count = 0
            dead_count = 0
        
        # Emit flush event for the dashboard (non-blocking, don't crash if this fails)
        try:
            log_result({
                'domain': 'DB_FLUSH',
                'provider': f'{len(batch)} domains',
                'category': 'Flush',
                'is_valid': True,
                'email_count': 0,
                'valid_count': valid_count,
                'dead_count': dead_count,
                'batch_size': len(batch)
            })
        except Exception as log_err:
            print(f"Warning: Failed to log flush event: {log_err}", flush=True)
        
        return len(batch)
        
    except Exception as e:
        print(f"Error in batch write ({len(batch)} items): {e}", flush=True)
        log_result({
            'domain': 'DB_FLUSH',
            'provider': f'ERROR: {str(e)[:50]}',
            'category': 'Error',
            'is_valid': False,
            'email_count': 0,
            'batch_size': len(batch) if batch else 0
        })
        try:
            conn.rollback()
        except:
            pass
        return 0

def save_mx_result(domain: str, mx_records: Optional[List[tuple]], 
                   category: str, provider: str, email_count: int,
                   error_message: str = None, dns_server: str = None):
    """Buffer MX result for batch writing (FAST - no DB hit per domain)."""
    global _write_buffer
    
    with _write_buffer_lock:
        _write_buffer.append({
            'domain': domain,
            'mx_records': mx_records,
            'category': category,
            'provider': provider,
            'email_count': email_count,
            'error': error_message,
            'dns_server': dns_server
        })
        buffer_len = len(_write_buffer)
    
    # Flush every 500 results (outside the lock)
    if buffer_len >= 500:
        batch = None
        with _write_buffer_lock:
            if len(_write_buffer) >= 500:
                batch = _write_buffer[:500]
                _write_buffer = _write_buffer[500:]
        if batch:
            _write_batch_to_db(batch)


def batch_apply_emails(domains_per_batch: int = 500):
    """
    Batch update emails with MX data from domain_mx.
    Updates emails for 500 domains at a time using efficient JOIN.
    Shows progress in the Live Log.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    # Get domains that have MX data (we'll update in batches)
    cursor.execute("SELECT domain FROM domain_mx")
    all_domains = [row[0] for row in cursor.fetchall()]
    
    if len(all_domains) == 0:
        log_result({
            'domain': 'SYNC',
            'provider': 'No domains to sync',
            'category': 'Info',
            'is_valid': True,
            'email_count': 0
        })
        cursor.close()
        conn.close()
        return 0
    
    total_domains = len(all_domains)
    log_result({
        'domain': 'SYNC',
        'provider': f'Syncing {total_domains:,} domains in batches of {domains_per_batch}...',
        'category': 'Info',
        'is_valid': True,
        'email_count': 0
    })
    
    total_updated = 0
    start_time = time.time()
    
    # Process in batches of 500 domains
    for i in range(0, total_domains, domains_per_batch):
        batch = all_domains[i:i + domains_per_batch]
        
        # Update all emails for this batch of domains in ONE query
        cursor.execute("""
            UPDATE emails e
            SET mx_category = d.mx_category,
                mx_valid = d.is_valid
            FROM domain_mx d
            WHERE e.email_domain = d.domain
              AND e.email_domain IN %s
              AND (e.mx_category IS DISTINCT FROM d.mx_category 
                   OR e.mx_valid IS DISTINCT FROM d.is_valid)
        """, (tuple(batch),))
        
        total_updated += cursor.rowcount
        conn.commit()
        
        # Log progress every 5000 domains
        domains_done = min(i + domains_per_batch, total_domains)
        if domains_done % 5000 == 0 or domains_done == total_domains:
            elapsed = time.time() - start_time
            rate = domains_done / elapsed if elapsed > 0 else 0
            eta = (total_domains - domains_done) / rate if rate > 0 else 0
            log_result({
                'domain': 'SYNC',
                'provider': f'{domains_done:,}/{total_domains:,} domains, {total_updated:,} emails, {rate:.0f} dom/sec, ETA {eta:.0f}s',
                'category': 'Info',
                'is_valid': True,
                'email_count': total_updated
            })
    
    cursor.close()
    conn.close()
    
    elapsed = time.time() - start_time
    log_result({
        'domain': 'SYNC',
        'provider': f'Complete: {total_updated:,} emails in {elapsed:.1f}s',
        'category': 'Info',
        'is_valid': True,
        'email_count': total_updated
    })
    
    return total_updated


def update_emails_from_mx(batch_size: int = 100000):
    """Update emails table with MX categories from domain_mx table.
    
    Processes domain by domain to avoid massive transactions.
    Each domain's emails are updated in one go (usually < 100k per domain).
    """
    conn = get_db()
    cursor = conn.cursor()
    
    # Add columns if they don't exist
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='emails' AND column_name='mx_category') THEN
                ALTER TABLE emails ADD COLUMN mx_category VARCHAR(50);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='emails' AND column_name='mx_valid') THEN
                ALTER TABLE emails ADD COLUMN mx_valid BOOLEAN DEFAULT true;
            END IF;
        END $$;
    """)
    conn.commit()
    
    # Create index on email_domain if it doesn't exist (speeds up the join)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_emails_domain ON emails(email_domain);
    """)
    conn.commit()
    
    # Get all domains with their MX data - process one domain at a time
    cursor.execute("""
        SELECT domain, mx_category, is_valid 
        FROM domain_mx 
        ORDER BY email_count DESC
    """)
    domains = cursor.fetchall()
    
    total_updated = 0
    processed = 0
    
    for domain, mx_category, is_valid in domains:
        # Update all emails for this domain
        cursor.execute("""
            UPDATE emails 
            SET mx_category = %s, mx_valid = %s
            WHERE email_domain = %s
              AND (mx_category IS DISTINCT FROM %s OR mx_valid IS DISTINCT FROM %s)
        """, (mx_category, is_valid, domain, mx_category, is_valid))
        
        total_updated += cursor.rowcount
        conn.commit()  # Commit after each domain to release locks
        
        processed += 1
        if processed % 1000 == 0:
            print(f"Processed {processed}/{len(domains)} domains, {total_updated:,} emails updated")
    
    cursor.close()
    conn.close()
    
    return total_updated


# =============================================================================
# VALIDATION WORKER
# =============================================================================

def validate_domain(domain: str, email_count: int) -> Dict[str, Any]:
    """
    Validate a single domain and return the result.
    
    Returns:
        Dict with domain, mx_records, category, provider, is_valid, dns_server
    """
    result = {
        'domain': domain,
        'email_count': email_count,
        'mx_records': None,
        'category': 'Dead',
        'provider': 'Unknown',
        'is_valid': False,
        'dns_server': None,
        'error': None
    }
    
    try:
        # Single attempt, 0.5s timeout (fast fail; dead domains can be rescanned later)
        mx_records, dns_server = resolve_mx(domain, timeout=0.5)
        result['mx_records'] = mx_records
        result['dns_server'] = dns_server
        
        if mx_records is None:
            result['category'] = 'Dead'
            result['provider'] = 'NXDOMAIN'
            result['is_valid'] = False
        elif len(mx_records) == 0:
            result['category'] = 'Dead'
            result['provider'] = 'No MX'
            result['is_valid'] = False
        else:
            primary_mx = mx_records[0][1]
            category, provider = classify_mx(primary_mx)
            result['category'] = category
            result['provider'] = provider
            result['is_valid'] = True
            
    except Exception as e:
        result['error'] = str(e)
        result['category'] = 'Error'
        result['provider'] = 'Error'
    
    return result


def log_result(result: Dict[str, Any]):
    """Add a result to the log queue for streaming."""
    try:
        # Map DNS IPs to friendly names
        dns_names = {
            '8.8.8.8': 'Google-1',
            '8.8.4.4': 'Google-2',
            '1.1.1.1': 'Cloudflare-1',
            '1.0.0.1': 'Cloudflare-2',
            '208.67.222.222': 'OpenDNS-1',
            '208.67.220.220': 'OpenDNS-2',
            '9.9.9.10': 'Quad9-1',
            '149.112.112.10': 'Quad9-2',
            '4.2.2.1': 'Level3-1',
            '4.2.2.2': 'Level3-2',
            '64.6.64.6': 'Verisign-1',
            '64.6.65.6': 'Verisign-2',
        }
        
        dns_server = result.get('dns_server')
        dns_display = dns_names.get(dns_server, dns_server) if dns_server else 'Unknown'
        
        log_entry = {
            'type': 'log',
            'timestamp': datetime.now().isoformat(),
            'domain': result['domain'],
            'mx': result['provider'],
            'category': result['category'],
            'is_valid': result['is_valid'],
            'email_count': result['email_count'],
            'dns_server': dns_display
        }
        
        # Add flush-specific fields if present
        if 'valid_count' in result:
            log_entry['valid_count'] = result['valid_count']
        if 'dead_count' in result:
            log_entry['dead_count'] = result['dead_count']
        if 'batch_size' in result:
            log_entry['batch_size'] = result['batch_size']
            
        _log_queue.put_nowait(log_entry)
    except queue.Full:
        pass  # Drop old logs if queue is full
    except Exception as e:
        # Don't let logging crash the scanner
        print(f"Warning: log_result error: {e}", flush=True)


def get_log_stream() -> Generator[str, None, None]:
    """Generator that yields log entries as SSE events."""
    # Wait a moment for the scan to actually start
    time.sleep(1)
    
    iterations = 0
    while True:
        iterations += 1
        state = get_state()
        
        # Only consider complete if:
        # 1. Status is idle/complete AND
        # 2. We've been running for a while (iterations > 10 = 5+ seconds) AND
        # 3. Queue is empty
        # This prevents premature "complete" during slow startup
        if state.status in ['idle', 'complete'] and _log_queue.empty() and iterations > 10:
            yield f"data: {json.dumps({'type': 'complete'})}\n\n"
            break
        
        # Send stats update
        stats_event = {
            'type': 'stats',
            'stats': {
                'total': state.total_domains,
                'checked': state.checked,
                'valid': state.valid,
                'dead': state.dead,
                'valid_emails': state.valid_emails,
                'dead_emails': state.dead_emails,
                'rate': round(state.rate, 1),
                'status': state.status
            },
            'categories': state.categories
        }
        yield f"data: {json.dumps(stats_event)}\n\n"
        
        # Send any pending log entries
        try:
            while True:
                entry = _log_queue.get_nowait()
                yield f"data: {json.dumps(entry)}\n\n"
        except queue.Empty:
            pass
        
        time.sleep(0.5)


# =============================================================================
# MAIN VALIDATION PROCESS
# =============================================================================

def run_validation(workers: int = 16, batch_size: int = 2000, resume: bool = True):
    """
    Run the MX validation process.
    
    Args:
        workers: Number of parallel workers
        batch_size: Domains per submission batch (smaller = smoother UI, 2000 = good balance)
        resume: If True, skip already-checked domains (check each domain once)
    """
    print("=== RUN_VALIDATION CALLED ===", flush=True)
    global _state
    
    # Create table if needed
    print("Creating table if needed...", flush=True)
    create_domain_mx_table()
    
    print("Starting MX validation...", flush=True)
    
    if resume:
        # Load existing stats from DB first (fast query)
        print("Loading existing stats from database...", flush=True)
        load_stats_from_db()
        print("Finding unchecked domains (this may take a moment)...", flush=True)
        domains = get_unchecked_domains()
        print(f"Found {len(domains):,} unchecked domains to scan", flush=True)
        # Update total = checked + remaining
        with _state_lock:
            _state.total_domains = _state.checked + len(domains)
    else:
        reset_state()
        print("Getting all GI domains (fresh scan)...")
        domains = get_gi_domains()
        print(f"Found {len(domains):,} total domains")
        with _state_lock:
            _state.total_domains = len(domains)
    
    # Nothing to scan: don't start executor, report complete
    if len(domains) == 0:
        with _state_lock:
            _state.status = 'complete'
        log_result({
            'domain': 'SYSTEM',
            'provider': 'No unchecked domains; nothing to scan.',
            'category': 'Info',
            'is_valid': True,
            'email_count': 0
        })
        print("No unchecked domains; nothing to scan.", flush=True)
        return get_state()
    
    with _state_lock:
        _state.status = 'running'
        _state.start_time = time.time()
    
    log_result({
        'domain': 'SYSTEM',
        'provider': f'Starting validation of {len(domains):,} domains',
        'category': 'Info',
        'is_valid': True,
        'email_count': 0
    })
    
    print("Getting DNS pool...", flush=True)
    dns_pool = get_dns_pool()
    print(f"DNS pool ready: {dns_pool}", flush=True)
    
    checked_count = 0
    last_rate_time = time.time()
    last_rate_count = 0
    
    print(f"Starting ThreadPoolExecutor with {workers} workers...", flush=True)
    print(f"Stop event before executor: {_stop_event.is_set()}", flush=True)
    print(f"Pause event before executor: {_pause_event.is_set()}", flush=True)
    
    # Clear events to ensure clean start
    _stop_event.clear()
    _pause_event.clear()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        print("Executor started, beginning batch processing...", flush=True)
        print(f"Total batches to process: {len(range(0, len(domains), batch_size))}", flush=True)
        # Submit in batches
        for i in range(0, len(domains), batch_size):
            if i == 0:
                print(f"Processing first batch (0 to {batch_size})...", flush=True)
            if i > 0 and i % 1000 == 0:
                print(f"Starting batch {i}...", flush=True)
            # Check for stop
            if _stop_event.is_set():
                print(f"Stop event detected at batch {i}, exiting...", flush=True)
                with _state_lock:
                    _state.status = 'stopping'
                break
            
            # Handle pause
            while _pause_event.is_set() and not _stop_event.is_set():
                with _state_lock:
                    _state.status = 'paused'
                time.sleep(0.5)
            
            if _stop_event.is_set():
                break
            
            with _state_lock:
                _state.status = 'running'
            
            batch = domains[i:i + batch_size]
            if i == 0:
                print(f"First batch has {len(batch)} domains", flush=True)
            futures = {
                executor.submit(validate_domain, domain, count): (domain, count)
                for domain, count in batch
            }
            if i == 0:
                print(f"Submitted {len(futures)} futures, waiting for completion...", flush=True)
            
            futures_completed = 0
            for future in as_completed(futures):
                futures_completed += 1
                if i == 0 and futures_completed == 1:
                    print(f"First future completed!", flush=True)
                if _stop_event.is_set():
                    print(f"Stop event during futures processing", flush=True)
                    break
                
                domain, count = futures[future]
                try:
                    result = future.result()
                    
                    # Debug: print first few results
                    if futures_completed <= 3:
                        print(f"Result {futures_completed}: {result['domain']} -> {result['category']}", flush=True)
                    
                    # Save to database
                    if futures_completed <= 3:
                        print(f"  Saving result {futures_completed}...", flush=True)
                    dns_ip = result.get('dns_server')
                    dns_display = DNS_SERVER_DISPLAY.get(dns_ip, dns_ip) if dns_ip else None
                    save_mx_result(
                        result['domain'],
                        result['mx_records'],
                        result['category'],
                        result['provider'],
                        result['email_count'],
                        result.get('error'),
                        dns_display
                    )
                    if futures_completed <= 3:
                        print(f"  Saved result {futures_completed}!", flush=True)
                    
                    # Update state
                    with _state_lock:
                        _state.checked += 1
                        email_count = result.get('email_count', 0)
                        if result['is_valid']:
                            _state.valid += 1
                            _state.valid_emails += email_count
                        else:
                            _state.dead += 1
                            _state.dead_emails += email_count
                        
                        # Update category counts
                        cat = result['category']
                        if cat in _state.categories:
                            _state.categories[cat] += 1
                        else:
                            _state.categories['Other'] = _state.categories.get('Other', 0) + 1
                        
                        checked_count = _state.checked
                    
                    # Log result
                    log_result(result)
                    
                    # Calculate rate every 100 domains
                    if checked_count % 100 == 0:
                        now = time.time()
                        elapsed = now - last_rate_time
                        if elapsed > 0:
                            rate = (checked_count - last_rate_count) / elapsed
                            with _state_lock:
                                _state.rate = rate
                            last_rate_time = now
                            last_rate_count = checked_count
                        print(f"Progress: {checked_count:,} domains checked, rate: {rate:.1f}/sec", flush=True)
                    
                except Exception as e:
                    with _state_lock:
                        _state.errors += 1
                    log_result({
                        'domain': domain,
                        'provider': str(e),
                        'category': 'Error',
                        'is_valid': False,
                        'email_count': count
                    })
    
    # Scan complete - emails table NOT updated yet
    # Run mx_domain_ops.py --apply to apply MX data to emails
    # Flush any remaining buffered results to database
    remaining = flush_write_buffer()
    if remaining > 0:
        print(f"Flushed final {remaining} results to database", flush=True)
    
    print("\n=== MX SCAN COMPLETE ===", flush=True)
    print("Domain MX data saved to domain_mx table", flush=True)
    print("To apply to emails table, run: python mx_domain_ops.py --apply", flush=True)
    print("Or run all post-processing steps: python mx_domain_ops.py --all", flush=True)
    
    # Final state
    with _state_lock:
        if _stop_event.is_set():
            _state.status = 'stopped'
        else:
            _state.status = 'complete'
    
    log_result({
        'domain': 'SYSTEM',
        'provider': f'Validation complete. {_state.checked:,} checked, {_state.valid:,} valid, {_state.dead:,} dead',
        'category': 'Info',
        'is_valid': True,
        'email_count': 0
    })
    
    return get_state()


def start_validation_async(workers: int = 32, resume: bool = True):
    """Start validation in a background thread."""
    global _state
    print(f"=== START_VALIDATION_ASYNC called with workers={workers}, resume={resume} ===", flush=True)
    
    # Set status to 'running' IMMEDIATELY to prevent race condition with SSE stream
    with _state_lock:
        _state.status = 'running'
    print("=== STATUS SET TO RUNNING ===", flush=True)
    
    def run_with_error_handling():
        print("=== THREAD STARTED ===", flush=True)
        try:
            # 2000 domains per batch = smooth throughput, no long pauses between submits
            run_validation(workers, 2000, resume)
        except Exception as e:
            print(f"VALIDATION ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # Reset status on error
            with _state_lock:
                _state.status = 'idle'
        print("=== THREAD FINISHED ===", flush=True)
    
    thread = threading.Thread(target=run_with_error_handling, daemon=True)
    thread.start()
    print("=== THREAD LAUNCHED ===", flush=True)
    return thread


def get_dead_domain_count() -> int:
    """Get count of domains marked as dead."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def reset_dead_domains() -> int:
    """
    Reset dead domains to unchecked so they get rescanned.
    Updates (does not delete): clears checked_at, mx_primary, etc. so they stay in domain_mx
    and get_unchecked_domains() will return them.
    Returns:
        Number of domains reset
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
    count = cursor.fetchone()[0]
    if count > 0:
        cursor.execute("""
            UPDATE domain_mx SET
                checked_at = NULL,
                mx_primary = NULL,
                mx_records = NULL,
                mx_priority = NULL,
                mx_category = NULL,
                mx_host_provider = NULL,
                is_valid = true,
                error_message = NULL,
                dns_server = NULL
            WHERE is_valid = false
        """)
        conn.commit()
    cursor.close()
    conn.close()
    return count


def start_dead_rescan_async(workers: int = 32) -> tuple:
    """
    Reset dead domains and start rescanning them.
    
    Returns:
        (thread, dead_count) tuple
    """
    # Reset dead domains first
    dead_count = reset_dead_domains()
    
    if dead_count == 0:
        return None, 0
    
    # Reset the state counters for fresh scan
    global _state
    with _state_lock:
        _state.dead = 0
        _state.dead_emails = 0
    
    # Start validation - the domains we just deleted will be picked up as unchecked
    thread = threading.Thread(target=run_validation, args=(workers, 2000, True), daemon=True)
    thread.start()
    
    return thread, dead_count


def pause_validation():
    """Pause the validation process."""
    _pause_event.set()


def resume_validation():
    """Resume the validation process."""
    _pause_event.clear()


def stop_validation():
    """Stop the validation process."""
    _stop_event.set()
    _pause_event.clear()


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='MX Domain Validator')
    parser.add_argument('--workers', '-w', type=int, default=16, help='Number of parallel workers')
    parser.add_argument('--fresh', action='store_true', help='Start fresh (recheck all domains)')
    parser.add_argument('--reset-dead', action='store_true', help='Reset all dead domains to unchecked, then run scan')
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print("  MX DOMAIN VALIDATOR")
    print("=" * 60 + "\n")
    
    if args.reset_dead:
        n = reset_dead_domains()
        print(f"Reset {n:,} dead domains to unchecked. Starting scan...\n")
    
    state = run_validation(workers=args.workers, resume=not args.fresh)
    
    print("\n" + "=" * 60)
    print("  VALIDATION COMPLETE")
    print("=" * 60)
    print(f"  Total domains: {state.total_domains:,}")
    print(f"  Checked: {state.checked:,}")
    print(f"  Valid: {state.valid:,}")
    print(f"  Dead: {state.dead:,}")
    print(f"  Errors: {state.errors:,}")
    print("\n  By Category:")
    for cat, count in sorted(state.categories.items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"    {cat}: {count:,}")
    print("=" * 60 + "\n")
