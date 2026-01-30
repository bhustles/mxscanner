"""
Email Processing System - Export Tools
Export campaign segments from the database

Usage:
    python export_tools.py --category Big4_ISP --clickers-only --limit 50000 --output campaign.csv
    python export_tools.py --stats
    python export_tools.py --query "SELECT * FROM emails WHERE state = 'TX' LIMIT 1000" --output texas.csv
"""

import sys
import argparse
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

sys.path.insert(0, str(Path(__file__).parent))

from config import BASE_DIR, OUTPUT_DIR
from database import (
    init_connection_pool, get_cursor, get_connection,
    get_email_count, get_category_counts, get_source_counts,
    close_connection_pool
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# QUERY BUILDERS
# =============================================================================

def build_export_query(
    category: str = None,
    data_source: str = None,
    clickers_only: bool = False,
    openers_only: bool = False,
    validated_only: bool = False,
    states: List[str] = None,
    exclude_states: List[str] = None,
    limit: int = None,
    offset: int = 0,
    order_by: str = 'signup_date DESC',
    columns: List[str] = None
) -> str:
    """Build a SQL query for exporting records."""
    
    # Default columns
    if not columns:
        columns = [
            'email', 'first_name', 'last_name', 'address', 'city', 'state', 'zipcode',
            'phone', 'dob', 'gender', 'signup_date', 'signup_ip',
            'is_clicker', 'is_opener', 'validation_status',
            'email_category', 'data_source'
        ]
    
    query = f"SELECT {', '.join(columns)} FROM emails WHERE 1=1"
    
    # Add filters
    if category:
        query += f" AND email_category = '{category}'"
    
    if data_source:
        query += f" AND data_source = '{data_source}'"
    
    if clickers_only:
        query += " AND is_clicker = TRUE"
    
    if openers_only:
        query += " AND is_opener = TRUE"
    
    if validated_only:
        query += " AND validation_status = 'Verified'"
    
    if states:
        states_str = ', '.join(f"'{s}'" for s in states)
        query += f" AND state IN ({states_str})"
    
    if exclude_states:
        exclude_str = ', '.join(f"'{s}'" for s in exclude_states)
        query += f" AND state NOT IN ({exclude_str})"
    
    # Order and limit
    if order_by:
        query += f" ORDER BY {order_by}"
    
    if limit:
        query += f" LIMIT {limit}"
    
    if offset:
        query += f" OFFSET {offset}"
    
    return query


# =============================================================================
# EXPORT FUNCTIONS
# =============================================================================

def export_to_csv(
    query: str,
    output_path: Path,
    include_header: bool = True
) -> int:
    """
    Export query results to CSV file.
    
    Returns:
        Number of records exported
    """
    init_connection_pool()
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                if include_header:
                    writer.writerow(columns)
                
                count = 0
                while True:
                    rows = cursor.fetchmany(10000)
                    if not rows:
                        break
                    
                    for row in rows:
                        # Convert any special types
                        clean_row = []
                        for val in row:
                            if val is None:
                                clean_row.append('')
                            elif isinstance(val, (list, tuple)):
                                clean_row.append('; '.join(str(v) for v in val))
                            elif isinstance(val, datetime):
                                clean_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                            else:
                                clean_row.append(str(val))
                        writer.writerow(clean_row)
                        count += 1
                
                cursor.close()
                return count
                
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


def export_segment(
    output_path: Path,
    category: str = None,
    data_source: str = None,
    clickers_only: bool = False,
    openers_only: bool = False,
    validated_only: bool = False,
    states: List[str] = None,
    limit: int = None
) -> int:
    """
    Export a segment of emails to CSV.
    
    Returns:
        Number of records exported
    """
    query = build_export_query(
        category=category,
        data_source=data_source,
        clickers_only=clickers_only,
        openers_only=openers_only,
        validated_only=validated_only,
        states=states,
        limit=limit
    )
    
    logger.info(f"Exporting with query: {query[:100]}...")
    
    count = export_to_csv(query, output_path)
    
    logger.info(f"Exported {count:,} records to {output_path}")
    return count


# =============================================================================
# STATISTICS
# =============================================================================

def print_stats():
    """Print database statistics."""
    
    init_connection_pool()
    
    print("\n" + "=" * 60)
    print("  EMAIL DATABASE STATISTICS")
    print("=" * 60)
    
    total = get_email_count()
    print(f"\n  Total Emails: {total:,}")
    
    print("\n  By Category:")
    print("  " + "-" * 40)
    for category, count in sorted(get_category_counts().items(), key=lambda x: x[1], reverse=True):
        pct = count / total * 100 if total > 0 else 0
        print(f"    {category}: {count:,} ({pct:.1f}%)")
    
    print("\n  By Data Source (Top 15):")
    print("  " + "-" * 40)
    for source, count in list(get_source_counts().items())[:15]:
        pct = count / total * 100 if total > 0 else 0
        print(f"    {source}: {count:,} ({pct:.1f}%)")
    
    # Additional stats
    with get_cursor() as cursor:
        # Clickers
        cursor.execute("SELECT COUNT(*) FROM emails WHERE is_clicker = TRUE")
        clickers = cursor.fetchone()[0]
        
        # Openers
        cursor.execute("SELECT COUNT(*) FROM emails WHERE is_opener = TRUE")
        openers = cursor.fetchone()[0]
        
        # Verified
        cursor.execute("SELECT COUNT(*) FROM emails WHERE validation_status = 'Verified'")
        verified = cursor.fetchone()[0]
        
        # With phone
        cursor.execute("SELECT COUNT(*) FROM emails WHERE phone IS NOT NULL")
        with_phone = cursor.fetchone()[0]
        
        # With address
        cursor.execute("SELECT COUNT(*) FROM emails WHERE address IS NOT NULL")
        with_address = cursor.fetchone()[0]
    
    print("\n  High-Intent Signals:")
    print("  " + "-" * 40)
    print(f"    Clickers: {clickers:,} ({clickers/total*100:.1f}%)")
    print(f"    Openers: {openers:,} ({openers/total*100:.1f}%)")
    print(f"    Verified: {verified:,} ({verified/total*100:.1f}%)")
    
    print("\n  Data Completeness:")
    print("  " + "-" * 40)
    print(f"    With Phone: {with_phone:,} ({with_phone/total*100:.1f}%)")
    print(f"    With Address: {with_address:,} ({with_address/total*100:.1f}%)")
    
    print("\n" + "=" * 60 + "\n")
    
    close_connection_pool()


def print_domain_stats(top_n: int = 30):
    """Print top domains."""
    
    init_connection_pool()
    
    print("\n  Top Email Domains:")
    print("  " + "-" * 50)
    
    with get_cursor() as cursor:
        cursor.execute(f"""
            SELECT email_domain, COUNT(*) as cnt 
            FROM emails 
            GROUP BY email_domain 
            ORDER BY cnt DESC 
            LIMIT {top_n}
        """)
        
        for domain, count in cursor.fetchall():
            print(f"    {domain}: {count:,}")
    
    print()
    close_connection_pool()


# =============================================================================
# SAMPLE QUERIES
# =============================================================================

def run_sample_queries():
    """Run and display sample queries."""
    
    init_connection_pool()
    
    queries = [
        ("Big 4 ISP Clickers (count)", 
         "SELECT COUNT(*) FROM emails WHERE email_category = 'Big4_ISP' AND is_clicker = TRUE"),
        
        ("Cable Provider Openers (count)",
         "SELECT COUNT(*) FROM emails WHERE email_category = 'Cable_Provider' AND is_opener = TRUE"),
        
        ("Verified emails from Glenn data (count)",
         "SELECT COUNT(*) FROM emails WHERE data_source = 'Glenn' AND validation_status = 'Verified'"),
        
        ("Texas emails (count)",
         "SELECT COUNT(*) FROM emails WHERE state = 'TX'"),
        
        ("High-intent (clicker + verified) count",
         "SELECT COUNT(*) FROM emails WHERE is_clicker = TRUE AND validation_status = 'Verified'"),
    ]
    
    print("\n  Sample Query Results:")
    print("  " + "-" * 50)
    
    with get_cursor() as cursor:
        for name, query in queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                print(f"    {name}: {result:,}")
            except Exception as e:
                print(f"    {name}: ERROR - {e}")
    
    print()
    close_connection_pool()


# =============================================================================
# CLI
# =============================================================================

def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(
        description='Email Export Tools - Export campaign segments'
    )
    
    # Mode selection
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--domains', action='store_true', help='Show top domains')
    parser.add_argument('--samples', action='store_true', help='Run sample queries')
    
    # Export options
    parser.add_argument('--output', '-o', type=str, help='Output CSV file path')
    parser.add_argument('--category', '-c', type=str, 
                        choices=['Big4_ISP', 'Cable_Provider', 'General_Internet'],
                        help='Filter by email category')
    parser.add_argument('--source', '-s', type=str, help='Filter by data source')
    parser.add_argument('--clickers-only', action='store_true', help='Only clickers')
    parser.add_argument('--openers-only', action='store_true', help='Only openers')
    parser.add_argument('--validated-only', action='store_true', help='Only verified emails')
    parser.add_argument('--states', type=str, help='Comma-separated list of states to include')
    parser.add_argument('--limit', '-l', type=int, help='Maximum records to export')
    parser.add_argument('--query', '-q', type=str, help='Custom SQL query to execute')
    
    args = parser.parse_args()
    
    # Handle different modes
    if args.stats:
        print_stats()
        return
    
    if args.domains:
        print_domain_stats()
        return
    
    if args.samples:
        run_sample_queries()
        return
    
    # Export mode
    if args.query and args.output:
        # Custom query export
        output_path = Path(args.output)
        count = export_to_csv(args.query, output_path)
        print(f"Exported {count:,} records to {output_path}")
        return
    
    if args.output:
        # Segment export
        output_path = Path(args.output)
        states = args.states.split(',') if args.states else None
        
        count = export_segment(
            output_path=output_path,
            category=args.category,
            data_source=args.source,
            clickers_only=args.clickers_only,
            openers_only=args.openers_only,
            validated_only=args.validated_only,
            states=states,
            limit=args.limit
        )
        
        print(f"\nExported {count:,} records to {output_path}")
        return
    
    # Default: show stats
    print_stats()


if __name__ == '__main__':
    main()
