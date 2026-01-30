"""
MX Host Classification Patterns
Maps MX server hostnames to provider categories
"""

# =============================================================================
# MX PATTERN MATCHING
# =============================================================================

MX_PATTERNS = {
    # Google Workspace
    'google': [
        'aspmx.l.google.com',
        'alt1.aspmx.l.google.com',
        'alt2.aspmx.l.google.com',
        'aspmx2.googlemail.com',
        'aspmx3.googlemail.com',
        'googlemail.com',
        'google.com',
    ],
    
    # Microsoft 365 / Office 365
    'microsoft': [
        'mail.protection.outlook.com',
        'olc.protection.outlook.com',
        'outlook-com.olc.protection.outlook.com',
        'outlook.com',
        'hotmail.com',
        'microsoft.com',
    ],
    
    # Yahoo
    'yahoo': [
        'yahoodns.net',
        'mail.yahoo.com',
        'mx-biz.mail.am0.yahoodns.net',
        'yahoo.com',
        'yahoomail.com',
    ],
    
    # HostGator
    'hostgator': [
        'hostgator.com',
        'websitewelcome.com',
        'hostmonster.com',
        'bluehost.com',
    ],
    
    # GoDaddy
    'godaddy': [
        'secureserver.net',
        'domaincontrol.com',
        'godaddy.com',
        'smtp.secureserver.net',
        'mailstore1.secureserver.net',
    ],
    
    # 1&1 / IONOS
    '1and1': [
        '1and1.com',
        'ionos.com',
        'schlund.de',
        '1und1.de',
        'kundenserver.de',
        'mx00.ionos.com',
        'mx01.ionos.com',
    ],
    
    # Zoho
    'zoho': [
        'zoho.com',
        'zohomail.com',
        'zoho.eu',
        'mx.zoho.com',
        'mx2.zoho.com',
        'mx3.zoho.com',
    ],
    
    # SiteGround
    'siteground': [
        'siteground.com',
        'mailspamprotection.com',
        'sgcpanel.com',
    ],
    
    # OVHcloud
    'ovhcloud': [
        'ovh.net',
        'ovh.com',
        'mx1.ovh.net',
        'mx2.ovh.net',
        'mx3.ovh.net',
    ],
    
    # ProtonMail
    'protonmail': [
        'protonmail.ch',
        'protonmail.com',
        'pm.me',
    ],
    
    # Rackspace
    'rackspace': [
        'emailsrvr.com',
        'rackspace.com',
    ],
    
    # Mimecast
    'mimecast': [
        'mimecast.com',
        'mimecast-offshore.com',
    ],
    
    # Amazon SES / AWS
    'amazon': [
        'amazonses.com',
        'amazonaws.com',
        'aws.com',
    ],
    
    # Namecheap
    'namecheap': [
        'namecheap.com',
        'privateemail.com',
        'registrar-servers.com',
        'mx1.privateemail.com',
        'mx2.privateemail.com',
    ],
    
    # DreamHost
    'dreamhost': [
        'dreamhost.com',
        'newdream.net',
    ],
    
    # Network Solutions
    'networksolutions': [
        'netsolmail.net',
        'networksolutions.com',
    ],
    
    # iCloud / Apple
    'apple': [
        'icloud.com',
        'apple.com',
        'me.com',
    ],
    
    # Fastmail
    'fastmail': [
        'fastmail.com',
        'messagingengine.com',
    ],
    
    # Barracuda
    'barracuda': [
        'barracudanetworks.com',
        'barracuda.com',
    ],
    
    # Proofpoint
    'proofpoint': [
        'pphosted.com',
        'proofpoint.com',
    ],
    
    # Postmark
    'postmark': [
        'postmarkapp.com',
    ],
    
    # SendGrid
    'sendgrid': [
        'sendgrid.net',
        'sendgrid.com',
    ],
    
    # Mailgun
    'mailgun': [
        'mailgun.org',
        'mailgun.com',
    ],
    
    # ==========================================================================
    # PARKED/EXPIRED DOMAINS - HIGH RISK SPAM TRAPS
    # ==========================================================================
    'parked': [
        # Sedo
        'sedoparking.com',
        'sedo.com',
        # GoDaddy Parking
        'parkingcrew.net',
        'domainparking.com',
        'cashparking.com',
        'parkeddomain.com',
        # Bodis
        'bodis.com',
        # Above.com
        'above.com',
        # HugeDomains
        'hugedomains.com',
        # Afternic
        'afternic.com',
        # ParkLogic
        'parklogic.com',
        # Fabulous
        'fabulous.com',
        # DAN.com
        'dan.com',
        'undeveloped.com',
        # Skenzo
        'skenzo.com',
        # Other parking services
        'parkingpage.net',
        'parked.com',
        'parkednicely.com',
        'domainsponsor.com',
        'oversee.net',
        'dsgeneration.com',
        'dsparking.com',
        'trafficz.com',
        'namedrive.com',
        'domainmonster.com',
        'domainnamesales.com',
        'buydomains.com',
    ],
}

# Categories that indicate "Big4 hosted but GI domain"
BIG4_MX_PROVIDERS = {'google', 'microsoft', 'yahoo'}

# Categories that indicate shared/cheap hosting - NOW TREATED AS GI (General_Internet)
SHARED_HOSTING_PROVIDERS = {'hostgator', 'godaddy', '1and1', 'namecheap', 'dreamhost', 'networksolutions', 'siteground', 'ovhcloud'}

# These hosting providers should be categorized as General_Internet (GI)
GI_HOSTING_PROVIDERS = {'hostgator', 'godaddy', '1and1', 'namecheap', 'dreamhost', 'networksolutions', 'siteground', 'ovhcloud', 'zoho', 'rackspace'}

# Enterprise/business providers (good deliverability)
ENTERPRISE_PROVIDERS = {'proofpoint', 'mimecast', 'barracuda'}

# HIGH RISK - Parked/expired domains (spam traps)
PARKED_PROVIDERS = {'parked'}


def classify_mx(mx_hostname: str) -> tuple:
    """
    Classify an MX hostname into a category.
    
    Returns:
        Tuple of (category, provider_name)
        category: 'Google', 'Microsoft', 'Yahoo', 'HostGator', 'GoDaddy', '1and1', 
                  'Zoho', 'ProtonMail', 'Rackspace', 'Real_GI', etc.
        provider_name: More detailed name
    """
    if not mx_hostname:
        return ('Dead', 'No MX')
    
    mx_lower = mx_hostname.lower()
    
    for provider, patterns in MX_PATTERNS.items():
        for pattern in patterns:
            if pattern in mx_lower:
                # Hosting providers -> General_Internet (GI)
                if provider in GI_HOSTING_PROVIDERS:
                    return ('General_Internet', mx_hostname)
                
                # Return formatted category name for non-GI providers
                category_map = {
                    'google': 'Google',
                    'microsoft': 'Microsoft',
                    'yahoo': 'Yahoo',
                    'protonmail': 'ProtonMail',
                    'mimecast': 'Mimecast',
                    'amazon': 'Amazon',
                    'apple': 'Apple',
                    'fastmail': 'Fastmail',
                    'barracuda': 'Barracuda',
                    'proofpoint': 'Proofpoint',
                    'postmark': 'Postmark',
                    'sendgrid': 'SendGrid',
                    'mailgun': 'Mailgun',
                    'parked': 'Parked',  # SPAM TRAP - high risk
                }
                return (category_map.get(provider, provider.title()), mx_hostname)
    
    # No match - this is a "Real GI" with their own mail server
    return ('Real_GI', mx_hostname)


def get_mx_quality_modifier(category: str) -> int:
    """
    Get a quality score modifier based on MX category.
    
    Returns:
        Positive or negative modifier to add to quality score
    """
    if category in ['Google', 'Microsoft', 'Yahoo', 'Apple']:
        return 5  # Big4 hosted = good deliverability
    elif category in ['Proofpoint', 'Mimecast', 'Barracuda']:
        return 3  # Enterprise = good
    elif category in ['ProtonMail', 'Fastmail']:
        return 2  # Reputable providers
    elif category == 'General_Internet':
        return 0  # GI hosting providers = neutral (valid but shared hosting)
    elif category == 'Parked':
        return -100  # SPAM TRAP - parked/expired domain
    elif category == 'Dead':
        return -100  # Invalid
    else:
        return 0  # Real_GI = neutral
