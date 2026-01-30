#!/usr/bin/env python3
"""
MX Domain Operations - Prepare and manage domain MX scanning workflow

Usage:
    python mx_domain_ops.py --backfill           # Step 1: Extract domains from emails
    python mx_domain_ops.py --move-undeliverable # Step 3: Move undeliverables to separate table
    python mx_domain_ops.py --cluster            # Step 4: CLUSTER emails by domain
    python mx_domain_ops.py --apply              # Step 5: Apply MX data to emails (chunked)
    python mx_domain_ops.py --all                # Run all steps in order (after MX scan)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import psycopg2
import time
import argparse
from typing import List, Tuple
from config import DATABASE

def get_db():
    """Get database connection."""
    return psycopg2.connect(
        host=DATABASE['host'],
        port=DATABASE['port'],
        database=DATABASE['database'],
        user=DATABASE['user'],
        password=DATABASE['password']
    )


# =============================================================================
# STEP 1: BACKFILL DOMAINS FROM EMAILS
# =============================================================================

def backfill_domains(gi_only: bool = False):
    """
    Extract unique domains from emails table and populate domain_mx.
    If gi_only=True, only General Internet domains (~500K). Otherwise all domains (~3.7M).
    Always sets is_gi so the scanner only checks GI domains and doesn't re-check 3.7M.
    """
    print("\n" + "=" * 70)
    print("  STEP 1: BACKFILL DOMAINS FROM EMAILS" + (" (GI only)" if gi_only else ""))
    print("=" * 70 + "\n")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Ensure is_gi column exists (scanner uses it to only scan ~500K GI domains)
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='domain_mx' AND column_name='is_gi') THEN
                ALTER TABLE domain_mx ADD COLUMN is_gi BOOLEAN DEFAULT false;
            END IF;
        END $$;
    """)
    conn.commit()
    
    print("Checking current state...", flush=True)
    cursor.execute("SELECT COUNT(*) FROM emails")
    email_count = cursor.fetchone()[0]
    print(f"  Emails in table: {email_count:,}", flush=True)
    
    cursor.execute("SELECT COUNT(*) FROM domain_mx")
    existing_domains = cursor.fetchone()[0]
    print(f"  Domains in domain_mx: {existing_domains:,}", flush=True)
    
    start_time = time.time()
    if gi_only:
        print("\nExtracting General Internet domains only (~500K)...", flush=True)
        cursor.execute("""
            INSERT INTO domain_mx (domain, email_count, is_gi)
            SELECT email_domain, COUNT(*), true
            FROM emails
            WHERE email_domain IS NOT NULL AND email_domain != ''
              AND email_category = 'General_Internet'
            GROUP BY email_domain
            ON CONFLICT (domain) DO UPDATE SET
                email_count = domain_mx.email_count + EXCLUDED.email_count,
                is_gi = true
        """)
    else:
        print(f"\nExtracting all unique domains from {email_count:,} emails...", flush=True)
        cursor.execute("""
            INSERT INTO domain_mx (domain, email_count)
            SELECT email_domain, COUNT(*)
            FROM emails
            WHERE email_domain IS NOT NULL AND email_domain != ''
            GROUP BY email_domain
            ON CONFLICT (domain) DO UPDATE SET
                email_count = domain_mx.email_count + EXCLUDED.email_count
        """)
        # Mark which domains are GI so scanner only checks those (~500K not 3.7M)
        print("Setting is_gi for General Internet domains (so scanner only checks ~500K)...", flush=True)
        cursor.execute("""
            UPDATE domain_mx SET is_gi = true
            WHERE domain IN (
                SELECT DISTINCT email_domain FROM emails
                WHERE email_category = 'General_Internet' AND email_domain IS NOT NULL AND email_domain != ''
            )
        """)
    
    conn.commit()
    elapsed = time.time() - start_time
    
    cursor.execute("SELECT COUNT(*) FROM domain_mx")
    total_domains = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
    gi_count = cursor.fetchone()[0]
    new_domains = total_domains - existing_domains
    
    print(f"\n[DONE] Backfill complete in {elapsed:.1f}s")
    print(f"  Total domains in domain_mx: {total_domains:,}")
    print(f"  General Internet (is_gi=true, will be scanned): {gi_count:,}")
    print(f"  New domains added: {new_domains:,}")
    print(f"  MX scan will only check the {gi_count:,} GI domains.\n")
    
    cursor.close()
    conn.close()


# =============================================================================
# STEP 3: MOVE UNDELIVERABLES TO SEPARATE TABLE
# =============================================================================

def move_undeliverables(batch_size: int = 500):
    """
    Move emails with undeliverable domains to emails_undeliverable table.
    Keeps data but removes it from main table for faster clustering.
    """
    print("\n" + "=" * 70)
    print("  STEP 3: MOVE UNDELIVERABLES TO SEPARATE TABLE")
    print("=" * 70 + "\n")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check for undeliverable domains
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
    undeliverable_count = cursor.fetchone()[0]
    
    if undeliverable_count == 0:
        print("No undeliverable domains found. Run MX scan first (mx_validator.py)")
        cursor.close()
        conn.close()
        return
    
    print(f"Found {undeliverable_count:,} undeliverable domains")
    
    # Create backup table if not exists
    print("\nCreating emails_undeliverable table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS emails_undeliverable (LIKE emails INCLUDING ALL)
    """)
    conn.commit()
    print("  [OK] Table ready")
    
    # Get list of undeliverable domains
    print("\nFetching undeliverable domain list...")
    cursor.execute("SELECT domain FROM domain_mx WHERE is_valid = false")
    undeliverable_domains = [row[0] for row in cursor.fetchall()]
    print(f"  {len(undeliverable_domains):,} domains to process")
    
    # Process in batches
    print(f"\nMoving emails in batches of {batch_size} domains...")
    total_moved = 0
    start_time = time.time()
    
    for i in range(0, len(undeliverable_domains), batch_size):
        batch = undeliverable_domains[i:i + batch_size]
        batch_start = time.time()
        
        # Insert into backup table
        cursor.execute("""
            INSERT INTO emails_undeliverable
            SELECT e.* 
            FROM emails e 
            WHERE e.email_domain IN %s
            ON CONFLICT (email) DO NOTHING
        """, (tuple(batch),))
        
        inserted = cursor.rowcount
        
        # Delete from main table
        cursor.execute("""
            DELETE FROM emails 
            WHERE email_domain IN %s
        """, (tuple(batch),))
        
        deleted = cursor.rowcount
        conn.commit()
        
        total_moved += deleted
        batch_elapsed = time.time() - batch_start
        
        if (i + batch_size) % 2500 == 0 or i + batch_size >= len(undeliverable_domains):
            processed_domains = min(i + batch_size, len(undeliverable_domains))
            elapsed = time.time() - start_time
            rate = processed_domains / elapsed if elapsed > 0 else 0
            eta = (len(undeliverable_domains) - processed_domains) / rate if rate > 0 else 0
            print(f"  [{processed_domains:,}/{len(undeliverable_domains):,}] Moved {total_moved:,} emails | "
                  f"{rate:.0f} dom/s | ETA {eta:.0f}s", flush=True)
    
    elapsed = time.time() - start_time
    
    # Verify counts
    cursor.execute("SELECT COUNT(*) FROM emails")
    remaining = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM emails_undeliverable")
    moved = cursor.fetchone()[0]
    
    print(f"\n[DONE] Move complete in {elapsed:.1f}s")
    print(f"  Emails moved to backup: {moved:,}")
    print(f"  Emails remaining (deliverable): {remaining:,}")
    print(f"  Ready for CLUSTER (run with --cluster next)\n")
    
    cursor.close()
    conn.close()


# =============================================================================
# STEP 4: CLUSTER EMAILS BY DOMAIN
# =============================================================================

def cluster_emails():
    """
    Physically reorder emails table by email_domain for faster chunked updates.
    This can take 10-30 minutes depending on table size.
    """
    print("\n" + "=" * 70)
    print("  STEP 4: CLUSTER EMAILS BY DOMAIN")
    print("=" * 70 + "\n")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check table size
    cursor.execute("SELECT COUNT(*) FROM emails")
    email_count = cursor.fetchone()[0]
    print(f"Clustering {email_count:,} emails by domain...")
    print("(This may take 10-30 minutes...)", flush=True)
    print("")
    
    # Ensure index exists
    print("Ensuring index exists...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_domain ON emails(email_domain)")
    conn.commit()
    print("  [OK] Index ready")
    
    # CLUSTER
    print("\nClustering table (physically reordering by domain)...")
    start_time = time.time()
    
    cursor.execute("CLUSTER emails USING idx_email_domain")
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"  [OK] CLUSTER complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")
    
    # ANALYZE for fresh stats
    print("\nUpdating table statistics...")
    cursor.execute("ANALYZE emails")
    conn.commit()
    print("  [OK] Statistics updated")
    
    print(f"\n[DONE] Cluster complete")
    print(f"  Emails are now physically grouped by domain on disk")
    print(f"  Ready for chunked updates (run with --apply next)\n")
    
    cursor.close()
    conn.close()


# =============================================================================
# STEP 5: APPLY MX DATA TO EMAILS (CHUNKED)
# =============================================================================

def apply_mx_to_emails(domains_per_batch: int = 500):
    """
    Update emails table with MX data from domain_mx in chunks.
    Sets mx_category, mx_valid, mx_host_provider on each email.
    """
    print("\n" + "=" * 70)
    print("  STEP 5: APPLY MX DATA TO EMAILS (CHUNKED)")
    print("=" * 70 + "\n")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Ensure columns exist
    print("Ensuring email columns exist...")
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
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='emails' AND column_name='mx_host_provider') THEN
                ALTER TABLE emails ADD COLUMN mx_host_provider VARCHAR(100);
            END IF;
        END $$;
    """)
    conn.commit()
    print("  [OK] Columns ready")
    
    # Get deliverable domains
    print("\nFetching deliverable domains...")
    cursor.execute("SELECT domain FROM domain_mx WHERE is_valid = true")
    all_domains = [row[0] for row in cursor.fetchall()]
    total_domains = len(all_domains)
    
    if total_domains == 0:
        print("No deliverable domains found. Run MX scan first (mx_validator.py)")
        cursor.close()
        conn.close()
        return
    
    print(f"  {total_domains:,} deliverable domains to apply")
    
    # Apply in batches
    print(f"\nApplying MX data in batches of {domains_per_batch} domains...")
    total_updated = 0
    start_time = time.time()
    
    for i in range(0, total_domains, domains_per_batch):
        batch = all_domains[i:i + domains_per_batch]
        
        cursor.execute("""
            UPDATE emails e
            SET mx_category = d.mx_category,
                mx_valid = d.is_valid,
                mx_host_provider = d.mx_host_provider
            FROM domain_mx d
            WHERE e.email_domain = d.domain
              AND e.email_domain IN %s
              AND (e.mx_category IS DISTINCT FROM d.mx_category 
                   OR e.mx_valid IS DISTINCT FROM d.is_valid
                   OR e.mx_host_provider IS DISTINCT FROM d.mx_host_provider)
        """, (tuple(batch),))
        
        updated = cursor.rowcount
        total_updated += updated
        conn.commit()
        
        if (i + domains_per_batch) % 5000 == 0 or i + domains_per_batch >= total_domains:
            domains_done = min(i + domains_per_batch, total_domains)
            elapsed = time.time() - start_time
            rate = domains_done / elapsed if elapsed > 0 else 0
            eta = (total_domains - domains_done) / rate if rate > 0 else 0
            print(f"  [{domains_done:,}/{total_domains:,}] Updated {total_updated:,} emails | "
                  f"{rate:.0f} dom/s | ETA {eta:.0f}s", flush=True)
    
    elapsed = time.time() - start_time
    
    print(f"\n[DONE] Apply complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Total emails updated: {total_updated:,}")
    print(f"  All emails now have MX category, validity, and provider data\n")
    
    cursor.close()
    conn.close()


# =============================================================================
# RUN ALL STEPS (AFTER MX SCAN)
# =============================================================================

def run_all_post_mx():
    """
    Run all steps after MX scan is complete:
    3. Move undeliverables
    4. CLUSTER
    5. Apply MX data
    """
    print("\n" + "=" * 70)
    print("  RUNNING ALL POST-MX STEPS")
    print("=" * 70 + "\n")
    
    move_undeliverables()
    cluster_emails()
    apply_mx_to_emails()
    
    print("\n" + "=" * 70)
    print("  ALL STEPS COMPLETE")
    print("=" * 70 + "\n")


# =============================================================================
# CLI
# =============================================================================

def set_gi_flag_only():
    """
    One-time fix: set is_gi=true on domain_mx for all General Internet domains.
    Run this if you already have 3.7M rows and want the scanner to only check ~500K GI.
    Does not re-insert; only updates is_gi. Keeps your existing mx_primary/checked data.
    """
    print("\n" + "=" * 70)
    print("  SET is_gi FLAG (so scanner only checks General Internet domains)")
    print("=" * 70 + "\n")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='domain_mx' AND column_name='is_gi') THEN
                ALTER TABLE domain_mx ADD COLUMN is_gi BOOLEAN DEFAULT false;
            END IF;
        END $$;
    """)
    conn.commit()
    print("Updating is_gi for General Internet domains (from emails)...", flush=True)
    cursor.execute("""
        UPDATE domain_mx SET is_gi = true
        WHERE domain IN (
            SELECT DISTINCT email_domain FROM emails
            WHERE email_category = 'General_Internet' AND email_domain IS NOT NULL AND email_domain != ''
        )
    """)
    updated = cursor.rowcount
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
    gi_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"Done. is_gi=true set for {gi_count:,} domains. MX scan will only check these (~500K), not all 3.7M.\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MX Domain Operations')
    parser.add_argument('--backfill', action='store_true', 
                       help='Step 1: Extract domains from emails to domain_mx')
    parser.add_argument('--gi-only', action='store_true',
                       help='With --backfill: only insert General Internet domains (~500K)')
    parser.add_argument('--set-gi-only', action='store_true',
                       help='One-time fix: set is_gi on existing domain_mx so scanner only checks ~500K GI')
    parser.add_argument('--move-undeliverable', action='store_true',
                       help='Step 3: Move undeliverables to separate table')
    parser.add_argument('--cluster', action='store_true',
                       help='Step 4: CLUSTER emails by domain')
    parser.add_argument('--apply', action='store_true',
                       help='Step 5: Apply MX data to emails (chunked)')
    parser.add_argument('--all', action='store_true',
                       help='Run all post-MX steps (3,4,5)')
    
    args = parser.parse_args()
    
    if args.set_gi_only:
        set_gi_flag_only()
        sys.exit(0)
    
    if not any([args.backfill, args.move_undeliverable, args.cluster, args.apply, args.all]):
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.backfill:
            backfill_domains(gi_only=getattr(args, 'gi_only', False))
        elif args.move_undeliverable:
            move_undeliverables()
        elif args.cluster:
            cluster_emails()
        elif args.apply:
            apply_mx_to_emails()
        elif args.all:
            run_all_post_mx()
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Operation cancelled by user\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[ERROR] {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
