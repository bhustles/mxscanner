"""
DNS Server Pool - LOCK-FREE VERSION
High-performance DNS resolution with minimal thread contention
"""

import random
import time
from typing import Optional, List, Tuple
import dns.resolver
import threading

# =============================================================================
# PUBLIC DNS SERVERS
# =============================================================================

DNS_SERVERS = [
    '8.8.8.8', '8.8.4.4',           # Google
    '1.1.1.1', '1.0.0.1',           # Cloudflare
    '208.67.222.222', '208.67.220.220',  # OpenDNS
    '9.9.9.10', '149.112.112.10',   # Quad9 unfiltered
    '4.2.2.1', '4.2.2.2',           # Level3
    '64.6.64.6', '64.6.65.6',       # Verisign
]

# Cooldown tracking - servers that timed out won't be used for COOLDOWN_SECONDS
COOLDOWN_SECONDS = 4.0
_server_cooldowns = {}  # server -> timestamp when cooldown expires

# Thread-local storage for per-thread resolvers (avoids creating new ones)
_thread_local = threading.local()

# Simple atomic counter for round-robin (race conditions OK - just for distribution)
_server_index = 0


def _is_server_available(server: str) -> bool:
    """Check if server is available (not in cooldown)."""
    if server not in _server_cooldowns:
        return True
    return time.time() >= _server_cooldowns[server]


def _put_server_on_cooldown(server: str):
    """Put a server on cooldown after a timeout."""
    _server_cooldowns[server] = time.time() + COOLDOWN_SECONDS


def _get_resolver(server: str, timeout: float) -> dns.resolver.Resolver:
    """Get or create a resolver for this thread."""
    # Each thread gets its own resolver to avoid contention
    if not hasattr(_thread_local, 'resolvers'):
        _thread_local.resolvers = {}
    
    if server not in _thread_local.resolvers:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = [server]
        resolver.timeout = timeout
        resolver.lifetime = timeout
        _thread_local.resolvers[server] = resolver
    
    return _thread_local.resolvers[server]


def resolve_mx(domain: str, timeout: float = 1.5) -> Tuple[Optional[List[tuple]], Optional[str]]:
    """
    Resolve MX records - LOCK-FREE, high performance.
    Servers that timeout are put on 4-second cooldown.
    
    Returns:
        Tuple of (mx_records, dns_server_used)
    """
    global _server_index
    
    # Get next server (no lock - race condition is fine, just distributes load)
    idx = _server_index
    _server_index = (idx + 1) % len(DNS_SERVERS)
    
    # Default to first server in rotation (never return None for server)
    last_server = DNS_SERVERS[idx % len(DNS_SERVERS)]
    servers_tried = 0
    
    # Try up to 4 servers (skip those on cooldown)
    for offset in range(len(DNS_SERVERS)):
        if servers_tried >= 3:
            break
            
        server = DNS_SERVERS[(idx + offset) % len(DNS_SERVERS)]
        
        # Skip servers on cooldown (but if we haven't tried any, use it anyway)
        if not _is_server_available(server) and servers_tried > 0:
            continue
        
        servers_tried += 1
        last_server = server
        
        # Increase timeout slightly on retries
        attempt_timeout = timeout + (servers_tried - 1) * 0.5
        
        try:
            resolver = _get_resolver(server, attempt_timeout)
            answers = resolver.resolve(domain, 'MX')
            
            mx_records = [(rdata.preference, str(rdata.exchange).rstrip('.')) 
                          for rdata in answers]
            mx_records.sort(key=lambda x: x[0])
            return (mx_records, server)
            
        except dns.resolver.NXDOMAIN:
            # Domain doesn't exist - verify with one more server
            if servers_tried < 2:
                continue
            return (None, server)
        except dns.resolver.NoAnswer:
            return ([], server)
        except dns.exception.Timeout:
            # Timeout - put this server on cooldown
            _put_server_on_cooldown(server)
            continue
        except (dns.resolver.NoNameservers, Exception):
            continue
    
    return (None, last_server)


# Legacy compatibility
class DNSPool:
    """Legacy wrapper - just calls the lock-free function."""
    def __init__(self, servers=None, timeout=1.5):
        self.timeout = timeout
        self.servers = servers or DNS_SERVERS
    
    def resolve_mx(self, domain: str):
        return resolve_mx(domain, self.timeout)
    
    def get_stats(self):
        return {'total_requests': 0, 'errors': 0, 'servers_count': len(DNS_SERVERS)}


_dns_pool = None

def get_dns_pool():
    global _dns_pool
    if _dns_pool is None:
        _dns_pool = DNSPool()
    return _dns_pool
