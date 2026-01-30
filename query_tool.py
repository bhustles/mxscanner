"""
Email Database Query Tool
Interactive SQL queries against your email database
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config import DATABASE
import psycopg2
from tabulate import tabulate

def connect():
    """Connect to database."""
    return psycopg2.connect(
        host=DATABASE['host'],
        port=DATABASE['port'],
        database=DATABASE['database'],
        user=DATABASE['user'],
        password=DATABASE['password']
    )

def run_query(sql, limit=20):
    """Run a query and display results."""
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(sql)
    
    if cursor.description:  # SELECT query
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(limit)
        print(tabulate(rows, headers=columns, tablefmt='psql'))
        print(f"\n(Showing {len(rows)} rows)")
    else:
        print(f"Query executed. Rows affected: {cursor.rowcount}")
    
    cursor.close()
    conn.close()

def show_stats():
    """Show database statistics."""
    conn = connect()
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("  EMAIL DATABASE STATISTICS")
    print("="*60)
    
    # Total count
    cursor.execute("SELECT COUNT(*) FROM emails")
    total = cursor.fetchone()[0]
    print(f"\n  Total Emails: {total:,}")
    
    # By category
    print("\n  By Category:")
    cursor.execute("""
        SELECT email_category, COUNT(*) as cnt, 
               ROUND(COUNT(*)::numeric / (SELECT COUNT(*) FROM emails) * 100, 1) as pct
        FROM emails 
        GROUP BY email_category 
        ORDER BY cnt DESC
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,} ({row[2]}%)")
    
    # By provider
    print("\n  By Provider (Top 10):")
    cursor.execute("""
        SELECT email_provider, COUNT(*) as cnt
        FROM emails 
        WHERE email_provider IS NOT NULL
        GROUP BY email_provider 
        ORDER BY cnt DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")
    
    # By brand
    print("\n  By Brand (Top 10):")
    cursor.execute("""
        SELECT email_brand, COUNT(*) as cnt
        FROM emails 
        WHERE email_brand IS NOT NULL
        GROUP BY email_brand 
        ORDER BY cnt DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")
    
    # By quality score
    print("\n  By Quality Score:")
    cursor.execute("""
        SELECT 
            CASE 
                WHEN quality_score >= 80 THEN 'High (80-100)'
                WHEN quality_score >= 60 THEN 'Good (60-79)'
                WHEN quality_score >= 40 THEN 'Average (40-59)'
                WHEN quality_score >= 20 THEN 'Low (20-39)'
                WHEN quality_score IS NOT NULL THEN 'Poor (0-19)'
                ELSE 'Not Scored'
            END as quality,
            COUNT(*) as cnt
        FROM emails 
        GROUP BY quality
        ORDER BY MIN(COALESCE(quality_score, -1)) DESC
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")
    
    # Clickers/Openers
    cursor.execute("SELECT COUNT(*) FROM emails WHERE is_clicker = true")
    clickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM emails WHERE is_opener = true")
    openers = cursor.fetchone()[0]
    print(f"\n  Engagement:")
    print(f"    Clickers: {clickers:,}")
    print(f"    Openers: {openers:,}")
    
    print("\n" + "="*60)
    cursor.close()
    conn.close()

def sample_emails(category=None, provider=None, min_score=None, limit=10):
    """Show sample emails with filters."""
    conn = connect()
    cursor = conn.cursor()
    
    sql = "SELECT email, email_provider, email_brand, quality_score, is_clicker, first_name, city, state FROM emails WHERE 1=1"
    params = []
    
    if category:
        sql += " AND email_category = %s"
        params.append(category)
    if provider:
        sql += " AND email_provider = %s"
        params.append(provider)
    if min_score:
        sql += " AND quality_score >= %s"
        params.append(min_score)
    
    sql += f" ORDER BY quality_score DESC NULLS LAST LIMIT {limit}"
    
    cursor.execute(sql, params)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    print(tabulate(rows, headers=columns, tablefmt='psql'))
    cursor.close()
    conn.close()

def interactive():
    """Interactive query mode."""
    print("\n" + "="*60)
    print("  EMAIL DATABASE - INTERACTIVE QUERY TOOL")
    print("="*60)
    print("\nCommands:")
    print("  stats        - Show database statistics")
    print("  sample       - Show sample emails")
    print("  yahoo        - Show Yahoo-hosted emails")
    print("  high         - Show high-quality emails (score >= 80)")
    print("  clickers     - Show clicker emails")
    print("  sql <query>  - Run custom SQL")
    print("  export       - Export to CSV")
    print("  quit         - Exit")
    print("-"*60)
    
    while True:
        try:
            cmd = input("\nquery> ").strip().lower()
            
            if not cmd:
                continue
            elif cmd == 'quit' or cmd == 'exit':
                break
            elif cmd == 'stats':
                show_stats()
            elif cmd == 'sample':
                sample_emails()
            elif cmd == 'yahoo':
                sample_emails(provider='Yahoo', limit=15)
            elif cmd == 'high':
                sample_emails(min_score=80, limit=15)
            elif cmd == 'clickers':
                conn = connect()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT email, email_provider, quality_score, first_name, city, state 
                    FROM emails WHERE is_clicker = true 
                    ORDER BY quality_score DESC LIMIT 15
                """)
                columns = [desc[0] for desc in cursor.description]
                print(tabulate(cursor.fetchall(), headers=columns, tablefmt='psql'))
                cursor.close()
                conn.close()
            elif cmd.startswith('sql '):
                sql = cmd[4:].strip()
                run_query(sql)
            elif cmd == 'export':
                print("Use: python export_tools.py --help")
            else:
                print(f"Unknown command: {cmd}")
                
        except KeyboardInterrupt:
            print("\n")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    # Install tabulate if not present
    try:
        from tabulate import tabulate
    except ImportError:
        print("Installing tabulate...")
        import subprocess
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'tabulate', '-q'])
        from tabulate import tabulate
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stats':
            show_stats()
        elif sys.argv[1] == 'interactive' or sys.argv[1] == '-i':
            interactive()
        else:
            # Treat as SQL
            run_query(' '.join(sys.argv[1:]))
    else:
        interactive()
