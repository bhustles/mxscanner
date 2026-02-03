import psycopg2
from config import DATABASE

conn = psycopg2.connect(**DATABASE)
cur = conn.cursor()

# First, add is_spamtrap column if it doesn't exist
cur.execute("""
    DO $$ 
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='domain_mx' AND column_name='is_spamtrap') THEN
            ALTER TABLE domain_mx ADD COLUMN is_spamtrap BOOLEAN DEFAULT FALSE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='emails' AND column_name='is_spamtrap') THEN
            ALTER TABLE emails ADD COLUMN is_spamtrap BOOLEAN DEFAULT FALSE;
        END IF;
    END $$;
""")
conn.commit()
print("Added is_spamtrap columns if needed")

# Mark h-email.net domains as spamtraps
cur.execute("""
    UPDATE domain_mx 
    SET is_spamtrap = TRUE, is_valid = FALSE, mx_category = 'Spamtrap'
    WHERE mx_primary = 'mail.h-email.net' 
       OR 'mail.h-email.net' = ANY(mx_records)
""")
h_email_count = cur.rowcount
print(f"Marked {h_email_count:,} domains with mail.h-email.net as spamtraps")

# Find skrimp domains
cur.execute("""
    SELECT domain, mx_primary, email_count 
    FROM domain_mx 
    WHERE mx_primary ILIKE '%skrimp%' 
       OR array_to_string(mx_records, ',') ILIKE '%skrimp%'
    ORDER BY email_count DESC
""")
skrimp_domains = cur.fetchall()
print(f"\nFound {len(skrimp_domains)} domains with 'skrimp' in MX:")
for row in skrimp_domains[:20]:
    print(f"  {row[0]} -> {row[1]} ({row[2]:,} emails)")

# Mark skrimp domains as spamtraps
cur.execute("""
    UPDATE domain_mx 
    SET is_spamtrap = TRUE, is_valid = FALSE, mx_category = 'Spamtrap'
    WHERE mx_primary ILIKE '%skrimp%' 
       OR array_to_string(mx_records, ',') ILIKE '%skrimp%'
""")
skrimp_count = cur.rowcount
print(f"\nMarked {skrimp_count:,} skrimp domains as spamtraps")

# Now update emails table to mark spamtrap emails
cur.execute("""
    UPDATE emails e
    SET is_spamtrap = TRUE
    FROM domain_mx d
    WHERE SPLIT_PART(e.email, '@', 2) = d.domain
      AND d.is_spamtrap = TRUE
""")
emails_marked = cur.rowcount
print(f"Marked {emails_marked:,} emails as spamtraps")

conn.commit()

# Summary
cur.execute("SELECT COUNT(*) FROM domain_mx WHERE is_spamtrap = TRUE")
total_trap_domains = cur.fetchone()[0]
cur.execute("SELECT COALESCE(SUM(email_count), 0) FROM domain_mx WHERE is_spamtrap = TRUE")
total_trap_emails = cur.fetchone()[0]

print(f"\n=== SPAMTRAP SUMMARY ===")
print(f"Total spamtrap domains: {total_trap_domains:,}")
print(f"Total spamtrap emails: {int(total_trap_emails):,}")

conn.close()
