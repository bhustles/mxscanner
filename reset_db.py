"""
Reset the database tables to apply new schema changes.
Run this before main.py after schema changes.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from config import DATABASE

import psycopg2

def reset_tables():
    """Drop and recreate tables with new schema."""
    
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=DATABASE['host'],
        port=DATABASE['port'],
        database=DATABASE['database'],
        user=DATABASE['user'],
        password=DATABASE['password']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("Dropping existing tables...")
    cursor.execute("DROP TABLE IF EXISTS emails CASCADE")
    cursor.execute("DROP TABLE IF EXISTS processing_stats CASCADE")
    cursor.execute("DROP TABLE IF EXISTS file_processing_log CASCADE")
    
    print("Tables dropped successfully!")
    print("\nNow run: python email_processor/main.py --test")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    reset_tables()
