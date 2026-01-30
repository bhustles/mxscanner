#!/usr/bin/env python3
import psycopg2
from config import DATABASE

conn = psycopg2.connect(**DATABASE)
cur = conn.cursor()

print("=" * 60)
print("EMAILS TABLE (by category)")
print("=" * 60)
cur.execute("""
    SELECT email_category, COUNT(DISTINCT email_domain), COUNT(*) 
    FROM emails 
    WHERE email_domain IS NOT NULL 
    GROUP BY email_category 
    ORDER BY 3 DESC
""")
for row in cur.fetchall():
    cat = row[0] or "NULL"
    print(f"  {cat}: {row[1]:,} domains, {row[2]:,} emails")

print()
print("=" * 60)
print("DOMAIN_MX TABLE")
print("=" * 60)
cur.execute("SELECT COUNT(*) FROM domain_mx")
print(f"Total domains:              {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM domain_mx WHERE checked_at IS NOT NULL")
print(f"Checked (scanned):          {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM domain_mx WHERE checked_at IS NULL")
print(f"Unchecked (not scanned):    {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
print(f"is_gi = true:               {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true AND checked_at IS NULL")
print(f"is_gi=true AND unchecked:   {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
print(f"Dead (is_valid=false):      {cur.fetchone()[0]:,}")

conn.close()
