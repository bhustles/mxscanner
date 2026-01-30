"""
DNS Server Pool - LOCK-FREE VERSION
High-performance DNS resolution with minimal thread contention
"""

import random
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

# Thread-local storage for per-thread resolvers (avoids creating new ones)
_thread_local = threading.local()

# Simple atomic counter for round-robin (race conditions OK - just for distribution)
_server_index = 0


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


def resolve_mx(domain: str, timeout: float = 2.0, max_retries: int = 2) -> Tuple[Optional[List[tuple]], Optional[str]]:
    """
    Resolve MX records - LOCK-FREE, high performance.
    Retries with different DNS servers on timeout/error.
    Returns:
        Tuple of (mx_records, dns_server_used)
    """
    global _server_index
    
    last_server = None
    
    for attempt in range(max_retries):
        # Get next server (no lock - race condition is fine, just distributes load)
        idx = _server_index
        _server_index = (idx + 1) % len(DNS_SERVERS)
        server = DNS_SERVERS[idx % len(DNS_SERVERS)]
        last_server = server
        
        try:
            resolver = _get_resolver(server, timeout)
            answers = resolver.resolve(domain, 'MX')
            
            mx_records = [(rdata.preference, str(rdata.exchange).rstrip('.')) 
                          for rdata in answers]
            mx_records.sort(key=lambda x: x[0])
            return (mx_records, server)
            
        except dns.resolver.NXDOMAIN:
            # Domain definitely doesn't exist - no retry needed
            return (None, server)
        except dns.resolver.NoAnswer:
            # Domain exists but no MX records - no retry needed
            return ([], server)
        except (dns.resolver.NoNameservers, dns.exception.Timeout, Exception):
            # Timeout or error - try next server
            continue
    
    # All retries failed
    return (None, last_server)


# Legacy compatibility
class DNSPool:
    """Legacy wrapper - just calls the lock-free function."""
    def __init__(self, servers=None, timeout=2.0):
        self.timeout = timeout
        self.servers = servers or DNS_SERVERS
    
    def resolve_mx(self, domain: str):
        return resolve_mx(domain, self.timeout, max_retries=2)
    
    def get_stats(self):
        return {'total_requests': 0, 'errors': 0, 'servers_count': len(DNS_SERVERS)}


_dns_pool = None

def get_dns_pool():
    global _dns_pool
    if _dns_pool is None:
        _dns_pool = DNSPool()
    return _dns_pool
