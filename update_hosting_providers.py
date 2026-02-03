"""
Update hosting_provider field in domain_mx based on MX hostname patterns
"""
import psycopg2
from config import DATABASE

conn = psycopg2.connect(**DATABASE)
cur = conn.cursor()

# Add hosting_provider column if not exists
cur.execute("""
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='domain_mx' AND column_name='hosting_provider') THEN
            ALTER TABLE domain_mx ADD COLUMN hosting_provider VARCHAR(50);
        END IF;
    END $$;
""")
conn.commit()
print("Added hosting_provider column if needed")

# Map MX patterns to hosting providers
providers = [
    ('bluehost.com', 'Bluehost'),
    ('hostgator.com', 'HostGator'),
    ('hostgator.com.br', 'HostGator'),
    ('websitewelcome.com', 'HostGator'),  # HostGator brand
    ('secureserver.net', 'GoDaddy'),
    ('emailsrvr.com', 'Rackspace'),
    ('mail.ovh.net', 'OVH'),
    ('zoho.com', 'Zoho'),
    ('zoho.eu', 'Zoho'),
    ('registrar-servers.com', 'Namecheap'),
    ('mailspamprotection.com', 'SpamExperts'),
    ('mxthunder.com', 'MXRoute'),
    ('mailgun.org', 'Mailgun'),
    ('sendgrid.net', 'SendGrid'),
    ('amazonses.com', 'Amazon SES'),
    ('pphosted.com', 'Proofpoint'),
    ('mimecast.com', 'Mimecast'),
    ('barracudanetworks.com', 'Barracuda'),
    ('inmotionhosting.com', 'InMotion'),
    ('dreamhost.com', 'DreamHost'),
    ('pair.com', 'pair Networks'),
    ('1and1.com', '1&1 IONOS'),
    ('ionos.com', '1&1 IONOS'),
    ('fastmail.com', 'Fastmail'),
    ('messagingengine.com', 'Fastmail'),
    ('protonmail.ch', 'ProtonMail'),
    ('hover.com', 'Hover'),
    ('name.com', 'Name.com'),
    ('netsolmail.net', 'Network Solutions'),
    ('netsol.com', 'Network Solutions'),
    ('emailmg.net', 'Network Solutions'),
    ('a2hosting.com', 'A2 Hosting'),
    ('hostmonster.com', 'HostMonster'),
    ('justhost.com', 'JustHost'),
    ('siteground.com', 'SiteGround'),
    ('liquidweb.com', 'Liquid Web'),
    ('mediatemple.net', 'Media Temple'),
    ('wpengine.com', 'WP Engine'),
    ('cloudflare.net', 'Cloudflare'),
    ('titan.email', 'Titan Email'),
    ('icloud.com', 'Apple iCloud'),
    ('me.com', 'Apple iCloud'),
    ('mac.com', 'Apple iCloud'),
    ('yandex.ru', 'Yandex'),
    ('yandex.net', 'Yandex'),
    ('mailru.com', 'Mail.ru'),
    ('qq.com', 'Tencent QQ'),
    ('163.com', 'NetEase'),
    ('126.com', 'NetEase'),
]

total = 0
for pattern, provider in providers:
    cur.execute("""
        UPDATE domain_mx 
        SET hosting_provider = %s
        WHERE mx_primary ILIKE %s
          AND (hosting_provider IS NULL OR hosting_provider = '')
    """, (provider, f'%{pattern}%'))
    count = cur.rowcount
    if count > 0:
        print(f'{provider}: {count:,} domains')
        total += count

conn.commit()
print(f'\nTotal updated: {total:,}')

# Show summary by hosting provider
cur.execute("""
    SELECT hosting_provider, COUNT(*) as domains, SUM(email_count) as emails
    FROM domain_mx 
    WHERE hosting_provider IS NOT NULL
    GROUP BY hosting_provider
    ORDER BY SUM(email_count) DESC
    LIMIT 25
""")
print('\n' + '='*60)
print('HOSTING PROVIDER SUMMARY')
print('='*60)
print(f'{"Provider":<25} {"Domains":>10} {"Emails":>15}')
print('-'*60)
for r in cur.fetchall():
    print(f'{r[0]:<25} {r[1]:>10,} {int(r[2] or 0):>15,}')

# Count unidentified
cur.execute("""
    SELECT COUNT(*), SUM(email_count) FROM domain_mx 
    WHERE mx_category = 'General_Internet' 
      AND is_valid = TRUE 
      AND (hosting_provider IS NULL OR hosting_provider = '')
""")
r = cur.fetchone()
print(f'\n{"Unidentified GI":<25} {r[0]:>10,} {int(r[1] or 0):>15,}')

conn.close()
