"""
MX Server Fingerprint Scanner
Scans MX servers for SSH/SMTP banners to identify spamtrap infrastructure
"""

import socket
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import DATABASE
import time
import re

# Known spamtrap fingerprint patterns
SPAMTRAP_PATTERNS = {
    'ssh': [
        r'OpenSSH_9\.2p1.*Debian.*deb12',  # Same as h-email.net / skrimple
        r'OpenSSH_9\.2p1.*Debian',          # Debian 12 with OpenSSH 9.2
    ],
    'smtp': [
        r'^220\s+\S+\s+ESMTP\s*$',  # Generic "220 hostname ESMTP" with nothing else
    ]
}

# Banners that indicate legitimate mail servers (not spamtraps)
LEGIT_PATTERNS = {
    'smtp': [
        r'Postfix',
        r'Microsoft',
        r'Exchange',
        r'Google',
        r'Exim',
        r'Sendmail',
        r'Zimbra',
        r'Dovecot',
        r'Barracuda',
        r'Proofpoint',
        r'Mimecast',
    ]
}


def get_banner(host, port, timeout=5):
    """Get banner from a service."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        banner = s.recv(1024).decode(errors='ignore').strip()
        s.close()
        return banner
    except socket.timeout:
        return None
    except Exception as e:
        return None


def get_smtp_ehlo(host, port=587, timeout=5):
    """Get SMTP banner and EHLO response."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        banner = s.recv(1024).decode(errors='ignore').strip()
        s.send(b'EHLO scanner.local\r\n')
        ehlo = s.recv(2048).decode(errors='ignore').strip()
        s.send(b'QUIT\r\n')
        s.close()
        return banner, ehlo
    except:
        return None, None


def scan_mx_server(mx_host):
    """Scan a single MX server for fingerprints."""
    result = {
        'mx_host': mx_host,
        'ssh_banner': None,
        'smtp_banner': None,
        'smtp_ehlo': None,
        'smtp_port': None,
        'is_suspicious': False,
        'suspicion_reason': None,
    }
    
    # Try SSH (port 22)
    result['ssh_banner'] = get_banner(mx_host, 22, timeout=5)
    
    # Try SMTP (port 587 first, then 25)
    for port in [587, 25]:
        banner, ehlo = get_smtp_ehlo(mx_host, port, timeout=5)
        if banner:
            result['smtp_banner'] = banner
            result['smtp_ehlo'] = ehlo
            result['smtp_port'] = port
            break
    
    # Check for spamtrap patterns
    suspicions = []
    
    # Check SSH banner
    if result['ssh_banner']:
        for pattern in SPAMTRAP_PATTERNS['ssh']:
            if re.search(pattern, result['ssh_banner'], re.IGNORECASE):
                suspicions.append(f"SSH matches spamtrap pattern: {pattern}")
    
    # Check SMTP banner
    if result['smtp_banner']:
        # Check if it's a generic ESMTP banner (suspicious)
        for pattern in SPAMTRAP_PATTERNS['smtp']:
            if re.search(pattern, result['smtp_banner'], re.IGNORECASE):
                # But not if it has legit software names
                is_legit = False
                for legit_pattern in LEGIT_PATTERNS['smtp']:
                    if re.search(legit_pattern, result['smtp_banner'], re.IGNORECASE):
                        is_legit = True
                        break
                    if result['smtp_ehlo'] and re.search(legit_pattern, result['smtp_ehlo'], re.IGNORECASE):
                        is_legit = True
                        break
                if not is_legit:
                    suspicions.append("Generic ESMTP banner (no software identified)")
    
    # Combined suspicion: Debian 12 SSH + Generic ESMTP = very suspicious
    if result['ssh_banner'] and result['smtp_banner']:
        if 'Debian' in result['ssh_banner'] and 'deb12' in result['ssh_banner']:
            if re.search(r'^220\s+\S+\s+ESMTP\s*$', result['smtp_banner']):
                suspicions.append("Debian 12 + Generic ESMTP (matches known spamtrap infrastructure)")
    
    if suspicions:
        result['is_suspicious'] = True
        result['suspicion_reason'] = '; '.join(suspicions)
    
    return result


def run_scan(workers=10, limit=None):
    """Run the fingerprint scan on all unique GI MX servers."""
    conn = psycopg2.connect(**DATABASE)
    cur = conn.cursor()
    
    # Add fingerprint columns if they don't exist
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='domain_mx' AND column_name='ssh_banner') THEN
                ALTER TABLE domain_mx ADD COLUMN ssh_banner TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='domain_mx' AND column_name='smtp_banner') THEN
                ALTER TABLE domain_mx ADD COLUMN smtp_banner TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='domain_mx' AND column_name='fingerprint_scanned') THEN
                ALTER TABLE domain_mx ADD COLUMN fingerprint_scanned TIMESTAMP;
            END IF;
        END $$;
    """)
    conn.commit()
    
    # Get unique MX servers from valid GI domains (not already scanned)
    query = """
        SELECT DISTINCT mx_primary
        FROM domain_mx
        WHERE is_valid = TRUE 
          AND mx_category = 'General_Internet'
          AND is_spamtrap IS NOT TRUE
          AND mx_primary IS NOT NULL
          AND fingerprint_scanned IS NULL
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    mx_servers = [row[0] for row in cur.fetchall()]
    
    print(f"Found {len(mx_servers):,} MX servers to scan")
    
    if not mx_servers:
        print("No servers to scan!")
        return
    
    # Scan with thread pool
    scanned = 0
    suspicious_count = 0
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scan_mx_server, mx): mx for mx in mx_servers}
        
        for future in as_completed(futures):
            mx = futures[future]
            try:
                result = future.result()
                scanned += 1
                
                # Update database
                cur.execute("""
                    UPDATE domain_mx
                    SET ssh_banner = %s,
                        smtp_banner = %s,
                        fingerprint_scanned = NOW()
                    WHERE mx_primary = %s
                """, (result['ssh_banner'], result['smtp_banner'], mx))
                
                # Print progress
                elapsed = time.time() - start_time
                rate = scanned / elapsed if elapsed > 0 else 0
                eta = (len(mx_servers) - scanned) / rate if rate > 0 else 0
                
                status = ""
                if result['is_suspicious']:
                    suspicious_count += 1
                    status = f" ⚠️  SUSPICIOUS: {result['suspicion_reason'][:60]}"
                
                ssh_info = result['ssh_banner'][:40] if result['ssh_banner'] else 'N/A'
                smtp_info = result['smtp_banner'][:40] if result['smtp_banner'] else 'N/A'
                
                print(f"[{scanned:,}/{len(mx_servers):,}] {mx[:35]:35} SSH: {ssh_info:42} SMTP: {smtp_info}{status}")
                
                # Commit every 50 servers
                if scanned % 50 == 0:
                    conn.commit()
                    
            except Exception as e:
                print(f"Error scanning {mx}: {e}")
    
    conn.commit()
    
    # Summary
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE")
    print(f"{'='*60}")
    print(f"Scanned: {scanned:,} MX servers in {elapsed:.1f}s ({scanned/elapsed:.1f}/sec)")
    print(f"Suspicious: {suspicious_count:,}")
    
    # Show suspicious servers
    if suspicious_count > 0:
        print(f"\n⚠️  SUSPICIOUS SERVERS:")
        cur.execute("""
            SELECT DISTINCT mx_primary, ssh_banner, smtp_banner, 
                   COUNT(*) as domain_count, SUM(email_count) as email_count
            FROM domain_mx
            WHERE fingerprint_scanned IS NOT NULL
              AND is_spamtrap IS NOT TRUE
              AND (
                  (ssh_banner LIKE '%OpenSSH_9.2p1%' AND ssh_banner LIKE '%Debian%' AND ssh_banner LIKE '%deb12%')
                  OR (smtp_banner ~ '^220\s+\S+\s+ESMTP\s*$')
              )
            GROUP BY mx_primary, ssh_banner, smtp_banner
            ORDER BY email_count DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}")
            print(f"    SSH: {row[1]}")
            print(f"    SMTP: {row[2]}")
            print(f"    Domains: {row[3]:,}, Emails: {row[4]:,}")
    
    conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Scan MX servers for fingerprints')
    parser.add_argument('--workers', '-w', type=int, default=10, help='Number of parallel workers')
    parser.add_argument('--limit', '-l', type=int, default=None, help='Limit number of servers to scan')
    args = parser.parse_args()
    
    run_scan(workers=args.workers, limit=args.limit)
