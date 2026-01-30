"""
Email Processing System - Configuration
Optimized for: Ryzen 9 5950X (16-core), 64GB RAM, RTX 5080 (16GB), 3.6TB NVMe
"""

import os
import tempfile
from pathlib import Path

# =============================================================================
# PATHS
# =============================================================================

BASE_DIR = Path(r"c:\Users\mdbar\OneDrive\Documents\Email Data 2022")
EMAIL_PROCESSOR_DIR = BASE_DIR / "email_processor"
LOG_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"
# Temp dir for worker output (avoids sending huge lists through multiprocessing queue)
WORKER_TEMP_DIR = Path(tempfile.gettempdir()) / "email_processor_worker"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(exist_ok=True)

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

DATABASE = {
    "host": "localhost",
    "port": 5432,
    "database": "email_master",
    "user": "postgres",
    "password": "postgres123",
}

# Connection string for psycopg2
DB_CONNECTION_STRING = f"host={DATABASE['host']} port={DATABASE['port']} dbname={DATABASE['database']} user={DATABASE['user']} password={DATABASE['password']}"

# =============================================================================
# PROCESSING SETTINGS
# =============================================================================

# Number of CPU workers for parallel processing
CPU_WORKERS = 16  # Match your Ryzen 9 5950X cores

# Batch sizes
BATCH_SIZE_PARSE = 500_000      # Records to parse at once
BATCH_SIZE_DEDUPE = 10_000_000  # Records for GPU deduplication
BATCH_SIZE_LOAD = 100_000       # Records per PostgreSQL COPY

# Memory limits (in GB)
MAX_MEMORY_GB = 48  # Leave 16GB for system/GPU

# GPU Settings
USE_GPU = True
GPU_MEMORY_LIMIT_GB = 14  # Leave 2GB for display

# =============================================================================
# FILE PATTERNS
# =============================================================================

# File extensions to process
SUPPORTED_EXTENSIONS = ['.csv', '.txt']
ARCHIVE_EXTENSIONS = ['.zip', '.rar']

# Files/folders to skip
SKIP_PATTERNS = [
    'email_processor',
    'logs',
    'output',
    '__pycache__',
    '.git',
]

# File patterns to exclude (suppression lists, DNC, etc.)
EXCLUDE_FILE_PATTERNS = [
    'suppress',
    'dnc',
    'spamhaus',
    'blacklist',
    'trap',
    'block',
    'seed',
    'federal_dnc',
    'do_not',
    'donotcall',
    'do-not',
]

# =============================================================================
# UNIFIED DOMAIN MAPPING
# Structure: domain -> (provider, brand, category)
# - provider: Who HOSTS/processes the email (for deliverability)
# - brand: The brand name users see (for segmentation)
# - category: Big4_ISP, Cable_Provider, or General_Internet
# =============================================================================

DOMAIN_MAPPING = {
    # =========================================================================
    # GOOGLE (Provider: Google)
    # =========================================================================
    'gmail.com': ('Google', 'Gmail', 'Big4_ISP'),
    'googlemail.com': ('Google', 'Gmail', 'Big4_ISP'),
    
    # =========================================================================
    # MICROSOFT (Provider: Microsoft)
    # =========================================================================
    'hotmail.com': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'outlook.com': ('Microsoft', 'Outlook', 'Big4_ISP'),
    'live.com': ('Microsoft', 'Live', 'Big4_ISP'),
    'msn.com': ('Microsoft', 'MSN', 'Big4_ISP'),
    'hotmail.co.uk': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'hotmail.fr': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'hotmail.de': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'hotmail.it': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'hotmail.es': ('Microsoft', 'Hotmail', 'Big4_ISP'),
    'outlook.co.uk': ('Microsoft', 'Outlook', 'Big4_ISP'),
    'live.co.uk': ('Microsoft', 'Live', 'Big4_ISP'),
    
    # =========================================================================
    # YAHOO-HOSTED DOMAINS (Provider: Yahoo)
    # Yahoo now hosts ALL of these domains - they share the same mail servers
    # =========================================================================
    
    # --- Yahoo Brand ---
    'yahoo.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.at': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.be': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.bg': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.ca': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.cl': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.id': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.il': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.in': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.kr': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.nz': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.th': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.uk': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.co.za': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.ar': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.au': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.br': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.co': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.hk': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.hr': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.mx': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.my': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.pe': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.ph': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.sg': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.tr': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.tw': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.ua': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.ve': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.com.vn': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.cz': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.de': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.dk': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.ee': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.es': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.fi': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.fr': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.gr': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.hu': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.ie': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.in': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.it': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.lt': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.lv': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.nl': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.no': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.pl': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.pt': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.ro': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.se': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'yahoo.sk': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'ymail.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'rocketmail.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'y7mail.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'myyahoo.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'kimo.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'ygm.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    
    # --- AOL Brand (Yahoo-hosted) ---
    'aol.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.at': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.be': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.ch': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.cl': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.co.nz': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.co.uk': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.ar': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.au': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.br': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.co': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.mx': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.tr': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.com.ve': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.cz': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.de': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.dk': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.es': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.fi': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.fr': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.hk': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.in': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.it': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.jp': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.kr': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.nl': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.pl': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.ru': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.se': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aol.tw': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aim.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aolchina.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aolnews.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'aolvideo.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'compuserve.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'cs.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'csi.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'love.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'wow.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'games.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'netscape.com': ('Yahoo', 'AOL', 'Big4_ISP'),
    'netscape.net': ('Yahoo', 'AOL', 'Big4_ISP'),
    
    # --- Verizon Brand (Yahoo-hosted) ---
    'verizon.net': ('Yahoo', 'Verizon', 'Big4_ISP'),
    'bellatlantic.net': ('Yahoo', 'Verizon', 'Big4_ISP'),
    'gte.net': ('Yahoo', 'Verizon', 'Big4_ISP'),
    
    # --- Frontier Brand (Yahoo-hosted) ---
    'frontier.com': ('Yahoo', 'Frontier', 'Big4_ISP'),
    'frontiernet.net': ('Yahoo', 'Frontier', 'Big4_ISP'),
    'myfrontiermail.com': ('Yahoo', 'Frontier', 'Big4_ISP'),
    'newnorth.net': ('Yahoo', 'Frontier', 'Big4_ISP'),
    'citlink.net': ('Yahoo', 'Frontier', 'Big4_ISP'),
    'epix.net': ('Yahoo', 'Frontier', 'Big4_ISP'),
    
    # --- Cox Brand (Yahoo-hosted) ---
    'cox.net': ('Yahoo', 'Cox', 'Big4_ISP'),
    
    # --- Rogers Brand (Yahoo-hosted) ---
    'rogers.com': ('Yahoo', 'Rogers', 'Big4_ISP'),
    
    # --- Sky Brand (Yahoo-hosted) ---
    'sky.com': ('Yahoo', 'Sky', 'Big4_ISP'),
    
    # --- AT&T Family (Yahoo-hosted since 2017) ---
    'att.net': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'att.com': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'sbcglobal.net': ('Yahoo', 'SBCGlobal', 'Big4_ISP'),
    'bellsouth.net': ('Yahoo', 'BellSouth', 'Big4_ISP'),
    'pacbell.net': ('Yahoo', 'PacBell', 'Big4_ISP'),
    'swbell.net': ('Yahoo', 'SWBell', 'Big4_ISP'),
    'ameritech.net': ('Yahoo', 'Ameritech', 'Big4_ISP'),
    'nvbell.net': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'snet.net': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'prodigy.net': ('Yahoo', 'Prodigy', 'Big4_ISP'),
    'flash.net': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'worldnet.att.net': ('Yahoo', 'AT&T', 'Big4_ISP'),
    'btinternet.com': ('Yahoo', 'BT', 'Big4_ISP'),
    
    # --- Other Yahoo-hosted domains ---
    'aprilshowersflorists.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'asylum.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'bloomoffaribault.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'dogsinthenews.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'geocities.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'goowy.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'lemondrop.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'mcom.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'netbusiness.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'robertgillingsproductions.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'safesocial.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'simivalleyflowers.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'spinner.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'switched.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'urlesque.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'vincentthepoet.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'when.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'wild4music.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    'wmconnect.com': ('Yahoo', 'Yahoo', 'Big4_ISP'),
    
    # =========================================================================
    # CABLE PROVIDERS (NOT Yahoo-hosted)
    # =========================================================================
    
    # --- Comcast/Xfinity ---
    'comcast.net': ('Comcast', 'Comcast', 'Cable_Provider'),
    'comcast.com': ('Comcast', 'Comcast', 'Cable_Provider'),
    'xfinity.com': ('Comcast', 'Comcast', 'Cable_Provider'),
    
    # --- Charter/Spectrum ---
    'charter.net': ('Charter', 'Spectrum', 'Cable_Provider'),
    'charter.com': ('Charter', 'Spectrum', 'Cable_Provider'),
    'spectrum.net': ('Charter', 'Spectrum', 'Cable_Provider'),
    'brighthouse.com': ('Charter', 'Spectrum', 'Cable_Provider'),
    
    # --- Time Warner / Roadrunner (regional variants) ---
    'rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'twc.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'roadrunner.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'nc.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'sc.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'tx.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'wi.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'maine.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'neo.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'cinci.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'columbus.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'hawaii.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'satx.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'austin.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'tampabay.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'socal.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'nycap.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'rochester.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'woh.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'ec.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'kc.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'san.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'triad.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    'insight.rr.com': ('Spectrum', 'Roadrunner', 'Cable_Provider'),
    
    # NOTE: AT&T Family domains moved to Yahoo-hosted section above (they use Yahoo mail servers)
    
    # --- CenturyLink/Qwest/Embarq ---
    'centurylink.net': ('CenturyLink', 'CenturyLink', 'Cable_Provider'),
    'centurylink.com': ('CenturyLink', 'CenturyLink', 'Cable_Provider'),
    'q.com': ('CenturyLink', 'Qwest', 'Cable_Provider'),
    'qwest.net': ('CenturyLink', 'Qwest', 'Cable_Provider'),
    'qwest.com': ('CenturyLink', 'Qwest', 'Cable_Provider'),
    'embarqmail.com': ('CenturyLink', 'Embarq', 'Cable_Provider'),
    'centurytel.net': ('CenturyLink', 'CenturyTel', 'Cable_Provider'),
    
    # --- Optimum/Cablevision ---
    'optonline.net': ('Altice', 'Optimum', 'Cable_Provider'),
    'optimum.net': ('Altice', 'Optimum', 'Cable_Provider'),
    'cablevision.com': ('Altice', 'Optimum', 'Cable_Provider'),
    
    # --- Mediacom ---
    'mediacombb.net': ('Mediacom', 'Mediacom', 'Cable_Provider'),
    'mchsi.com': ('Mediacom', 'Mediacom', 'Cable_Provider'),
    
    # --- Suddenlink/Altice ---
    'suddenlink.net': ('Altice', 'Suddenlink', 'Cable_Provider'),
    'suddenlinkmail.com': ('Altice', 'Suddenlink', 'Cable_Provider'),
    
    # --- Windstream ---
    'windstream.net': ('Windstream', 'Windstream', 'Cable_Provider'),
    'windstream.com': ('Windstream', 'Windstream', 'Cable_Provider'),
    
    # --- WOW (WideOpenWest) ---
    'wideopenwest.com': ('WOW', 'WOW', 'Cable_Provider'),
    'wowway.com': ('WOW', 'WOW', 'Cable_Provider'),
    
    # --- RCN ---
    'rcn.com': ('RCN', 'RCN', 'Cable_Provider'),
    'rcn.net': ('RCN', 'RCN', 'Cable_Provider'),
    
    # --- Atlantic Broadband ---
    'atlanticbb.net': ('Atlantic Broadband', 'Atlantic Broadband', 'Cable_Provider'),
    
    # --- Cable One/Sparklight ---
    'cableone.net': ('Sparklight', 'Sparklight', 'Cable_Provider'),
    'sparklight.net': ('Sparklight', 'Sparklight', 'Cable_Provider'),
    
    # --- Midco ---
    'midcomail.com': ('Midco', 'Midco', 'Cable_Provider'),
    'midco.net': ('Midco', 'Midco', 'Cable_Provider'),
    
    # --- Armstrong ---
    'armstrong.com': ('Armstrong', 'Armstrong', 'Cable_Provider'),
    'zoominternet.net': ('Armstrong', 'Armstrong', 'Cable_Provider'),
    
    # --- Adelphia (defunct but may have data) ---
    'adelphia.net': ('Adelphia', 'Adelphia', 'Cable_Provider'),
    
    # --- EarthLink ---
    'earthlink.net': ('EarthLink', 'EarthLink', 'Cable_Provider'),
    'earthlink.com': ('EarthLink', 'EarthLink', 'Cable_Provider'),
    'mindspring.com': ('EarthLink', 'MindSpring', 'Cable_Provider'),
    
    # --- Other ISPs ---
    'juno.com': ('Juno', 'Juno', 'Cable_Provider'),
    'netzero.net': ('NetZero', 'NetZero', 'Cable_Provider'),
    'netzero.com': ('NetZero', 'NetZero', 'Cable_Provider'),
    'peoplepc.com': ('PeoplePC', 'PeoplePC', 'Cable_Provider'),
    'usa.net': ('USA.net', 'USA.net', 'Cable_Provider'),
    'excite.com': ('Excite', 'Excite', 'Cable_Provider'),
    
    # --- Apple/iCloud ---
    'icloud.com': ('Apple', 'iCloud', 'Cable_Provider'),
    'me.com': ('Apple', 'iCloud', 'Cable_Provider'),
    'mac.com': ('Apple', 'iCloud', 'Cable_Provider'),
    
    # --- Satellite ISPs ---
    'hughesnet.com': ('HughesNet', 'HughesNet', 'Cable_Provider'),
    'starband.net': ('Starband', 'Starband', 'Cable_Provider'),
    'wildblue.net': ('ViaSat', 'WildBlue', 'Cable_Provider'),
    'exede.net': ('ViaSat', 'Exede', 'Cable_Provider'),
    'starlink.com': ('SpaceX', 'Starlink', 'Cable_Provider'),
    
    # --- Mobile Carriers ---
    't-mobile.com': ('T-Mobile', 'T-Mobile', 'Cable_Provider'),
    'sprint.com': ('T-Mobile', 'Sprint', 'Cable_Provider'),
    
    # --- Regional/Municipal ISPs ---
    'sonic.net': ('Sonic', 'Sonic', 'Cable_Provider'),
    'toast.net': ('Toast.net', 'Toast.net', 'Cable_Provider'),
    'grandecom.net': ('Grande', 'Grande', 'Cable_Provider'),
    'gvtc.com': ('GVTC', 'GVTC', 'Cable_Provider'),
    'dallas.net': ('Dallas.net', 'Dallas.net', 'Cable_Provider'),
    'swcp.com': ('SWCP', 'SWCP', 'Cable_Provider'),
    'chibardun.net': ('Chibardun', 'Chibardun', 'Cable_Provider'),
    'tds.net': ('TDS', 'TDS', 'Cable_Provider'),
    'tdstelecom.com': ('TDS', 'TDS', 'Cable_Provider'),
    'socket.net': ('Socket', 'Socket', 'Cable_Provider'),
    'everestkc.net': ('Everest', 'Everest', 'Cable_Provider'),
    'sunflower.com': ('Sunflower', 'Sunflower', 'Cable_Provider'),
    'southslope.net': ('South Slope', 'South Slope', 'Cable_Provider'),
    'myfairpoint.net': ('FairPoint', 'FairPoint', 'Cable_Provider'),
    'metrocast.net': ('MetroCast', 'MetroCast', 'Cable_Provider'),
    'atlanticbbn.net': ('Atlantic BBN', 'Atlantic BBN', 'Cable_Provider'),
    'bresnan.net': ('Bresnan', 'Bresnan', 'Cable_Provider'),
    'bendbroadband.com': ('Bend Broadband', 'Bend Broadband', 'Cable_Provider'),
    'wavecable.com': ('Wave', 'Wave', 'Cable_Provider'),
    'clearwire.net': ('Clearwire', 'Clearwire', 'Cable_Provider'),
    
    # --- Legacy/Free ISPs ---
    'mail.com': ('Mail.com', 'Mail.com', 'Cable_Provider'),
    'email.com': ('Email.com', 'Email.com', 'Cable_Provider'),
    'usa.com': ('USA.com', 'USA.com', 'Cable_Provider'),
    'post.com': ('Post.com', 'Post.com', 'Cable_Provider'),
    'iname.com': ('iName', 'iName', 'Cable_Provider'),
    'lycos.com': ('Lycos', 'Lycos', 'Cable_Provider'),
    'inbox.com': ('Inbox.com', 'Inbox.com', 'Cable_Provider'),
    
    # --- Frontier/Consolidated ---
    'consolidated.net': ('Consolidated', 'Consolidated', 'Cable_Provider'),
    'fairpoint.net': ('FairPoint', 'FairPoint', 'Cable_Provider'),
    'hawaiiantel.net': ('Hawaiian Tel', 'Hawaiian Tel', 'Cable_Provider'),
    'cincbell.net': ('Cincinnati Bell', 'Cincinnati Bell', 'Cable_Provider'),
    'fuse.net': ('Cincinnati Bell', 'Fuse', 'Cable_Provider'),
    'qwestoffice.net': ('CenturyLink', 'Qwest', 'Cable_Provider'),
    'uswest.net': ('CenturyLink', 'US West', 'Cable_Provider'),
}

# =============================================================================
# BACKWARD COMPATIBILITY - Generate old-style dicts from DOMAIN_MAPPING
# =============================================================================

# Build lookup sets for fast categorization
BIG4_DOMAINS = {
    domain: info[1]  # brand
    for domain, info in DOMAIN_MAPPING.items()
    if info[2] == 'Big4_ISP'
}

CABLE_DOMAINS = {
    domain: info[1]  # brand
    for domain, info in DOMAIN_MAPPING.items()
    if info[2] == 'Cable_Provider'
}

# =============================================================================
# COUNTRY TLDs TO EXCLUDE
# =============================================================================

COUNTRY_TLDS = [
    '.co.uk', '.uk', '.ca', '.au', '.de', '.fr', '.it', '.es', '.nl', '.be',
    '.at', '.ch', '.se', '.no', '.dk', '.fi', '.pl', '.cz', '.ru', '.ua',
    '.br', '.mx', '.ar', '.co', '.cl', '.pe', '.in', '.cn', '.jp', '.kr',
    '.tw', '.hk', '.sg', '.my', '.ph', '.th', '.vn', '.id', '.nz', '.za',
    '.ie', '.pt', '.gr', '.hu', '.ro', '.bg', '.sk', '.si', '.hr', '.rs',
    '.il', '.ae', '.sa', '.eg', '.tr', '.pk'
]

# =============================================================================
# ROLE EMAIL PATTERNS TO FILTER
# =============================================================================

ROLE_EMAIL_PATTERNS = [
    # High Risk - Definite traps/role accounts
    'legal@', 'lawyer@', 'attorney@', 'counsel@', 'litigation@',
    'spam@', 'abuse@', 'postmaster@', 'mailer-daemon@', 'mailerdaemon@',
    'noreply@', 'no-reply@', 'no_reply@', 'donotreply@', 'do-not-reply@',
    'bounce@', 'bounces@', 'return@', 'unsubscribe@',
    
    # Medium Risk - Likely role accounts
    'admin@', 'administrator@', 'root@', 'sysadmin@', 'webmaster@', 'hostmaster@',
    'info@', 'information@', 'contact@', 'hello@', 'enquiry@', 'inquiry@',
    'support@', 'help@', 'helpdesk@', 'service@', 'customerservice@',
    'sales@', 'marketing@', 'advertising@', 'press@', 'media@', 'pr@',
    'billing@', 'accounts@', 'accounting@', 'finance@', 'payments@',
    'hr@', 'humanresources@', 'recruiting@', 'careers@', 'jobs@',
    'security@', 'privacy@', 'compliance@', 'gdpr@',
    'feedback@', 'suggestions@', 'complaints@',
    'newsletter@', 'news@', 'updates@', 'alerts@',
    'orders@', 'order@', 'shipping@', 'returns@',
    'test@', 'testing@', 'demo@', 'example@',
]

# Role patterns to match anywhere in the email prefix
ROLE_PATTERNS_ANYWHERE = [
    'spam', 'abuse', 'legal', 'lawyer', 'attorney',
]

# =============================================================================
# DATA SOURCE MAPPING
# =============================================================================

# Filename patterns -> Normalized source name
DATA_SOURCE_PATTERNS = {
    'bulldogs': 'Bulldogs',
    'bulldog': 'Bulldogs',
    'glenn': 'Glenn',
    'autoleads': 'AutoLeads',
    'auto-leads': 'AutoLeads',
    'auto_leads': 'AutoLeads',
    'jet': 'Jet Marketing',
    'jetgi': 'Jet Marketing',
    'jet_gi': 'Jet Marketing',
    'jetmarketing': 'Jet Marketing',
    'ryans': 'Ryans Guy',
    'ryan': 'Ryans Guy',
    'gio': 'GIO',
    'ipost': 'iPost',
    'ricky': 'Ricky Jeff',
    'rickyjeff': 'Ricky Jeff',
    'hq4ads': 'HQ4Ads',
    'hq4': 'HQ4Ads',
    'debt': 'Debt Leads',
    'flex': 'Flex Campaign',
    '140036': '140036 Campaign',
    'aol': 'AOL List',
    'yahoo': 'Yahoo List',
    'comcast': 'Comcast List',
    'hotmail': 'Hotmail List',
    'master': 'Master List',
}

# Folder patterns -> Source name
FOLDER_SOURCE_PATTERNS = {
    'glenndata': 'Glenn',
    'newgi': 'New GI',
    'email ipost': 'iPost',
    'new gi october': 'New GI October',
    'gi_jeff': 'GI Jeff',
    'sms_emails': 'SMS Emails',
}

# =============================================================================
# SCHEMA DETECTION PATTERNS
# =============================================================================

# Column name variations for unified schema mapping
COLUMN_MAPPINGS = {
    'email': ['email', 'e-mail', 'e_mail', 'email_address', 'emailaddress', 'e-mail address', 'email address'],
    'first_name': ['first_name', 'firstname', 'first name', 'fname', 'first'],
    'last_name': ['last_name', 'lastname', 'last name', 'lname', 'last'],
    'address': ['address', 'address1', 'street', 'street_address', 'streetaddress', 'formattedaddress'],
    'city': ['city', 'town'],
    'state': ['state', 'st', 'region', 'province'],
    'zipcode': ['zipcode', 'zip_code', 'zip', 'zip code', 'postalcode', 'postal_code', 'postal code'],
    'phone': ['phone', 'phone_home', 'phonehome', 'homephone', 'mobile', 'mobilephone', 'telephone', 'tel'],
    'dob': ['dob', 'dateofbirth', 'date_of_birth', 'birthdate', 'birth_date', 'dayofbirth'],
    'gender': ['gender', 'sex'],
    'signup_date': ['signup_date', 'signupdate', 'created_on', 'createdon', 'creationdate', 'creation_date', 'date', 'signup date'],
    'signup_ip': ['signup_ip', 'signupip', 'ip_address', 'ipaddress', 'ip address', 'ip'],
    'signup_domain': ['signup_domain', 'signupdomain', 'signupsource', 'signup_source', 'source'],
    'validation_status': ['validation_status', 'validationstatus', 'validationstatusid', 'status'],
    'country': ['country'],
}

# =============================================================================
# LOGGING
# =============================================================================

LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE = LOG_DIR / 'email_processing.log'
