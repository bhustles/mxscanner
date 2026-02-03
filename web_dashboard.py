"""
Email Database Web Dashboard
A simple web interface to query and explore your email data
"""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from flask import Flask, render_template_string, request, jsonify, Response
from config import DATABASE
import psycopg2
import json

app = Flask(__name__)

# MX Validator imports (lazy load to avoid import errors if dnspython not installed)
_mx_validator = None

def get_mx_validator():
    """Lazy load MX validator module."""
    global _mx_validator
    if _mx_validator is None:
        try:
            import mx_validator as mv
            _mx_validator = mv
        except ImportError as e:
            print(f"MX Validator not available: {e}")
            print("Install dnspython: pip install dnspython")
    return _mx_validator

# =============================================================================
# STATS CACHE - Avoid counting 38M rows on every request
# =============================================================================
STATS_CACHE = {
    'data': None,
    'timestamp': 0,
    'ttl': 30  # Cache for 30 seconds
}

def get_cached_stats():
    """Get stats from cache, refresh if stale."""
    now = time.time()
    if STATS_CACHE['data'] is None or (now - STATS_CACHE['timestamp']) > STATS_CACHE['ttl']:
        STATS_CACHE['data'] = fetch_stats_from_db()
        STATS_CACHE['timestamp'] = now
    return STATS_CACHE['data']

def fetch_stats_from_db():
    """Fetch all stats in a single optimized query."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Single query with multiple counts - much faster than separate queries
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE email_category = 'Big4_ISP') as big4,
                COUNT(*) FILTER (WHERE email_category = 'Cable_Provider') as cable,
                COUNT(*) FILTER (WHERE email_category = 'General_Internet') as gi,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE quality_score >= 80) as high_quality
            FROM emails
        """)
        row = cursor.fetchone()
        
        stats = {
            'total': row[0] or 0,
            'big4': row[1] or 0,
            'cable': row[2] or 0,
            'gi': row[3] or 0,
            'clickers': row[4] or 0,
            'high_quality': row[5] or 0
        }
        
        # Get providers
        cursor.execute("""
            SELECT email_provider, COUNT(*) FROM emails 
            WHERE email_provider IS NOT NULL
            GROUP BY email_provider ORDER BY COUNT(*) DESC LIMIT 15
        """)
        stats['providers'] = [[r[0], r[1]] for r in cursor.fetchall()]
        
        # Get quality distribution
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN quality_score >= 80 THEN 'High (80-100)'
                    WHEN quality_score >= 60 THEN 'Good (60-79)'
                    WHEN quality_score >= 40 THEN 'Average (40-59)'
                    WHEN quality_score >= 20 THEN 'Low (20-39)'
                    WHEN quality_score IS NOT NULL THEN 'Poor (0-19)'
                    ELSE 'Not Scored'
                END as tier,
                COUNT(*)
            FROM emails GROUP BY tier
            ORDER BY MIN(COALESCE(quality_score, -1)) DESC
        """)
        stats['quality'] = list(cursor.fetchall())
        
        cursor.close()
        conn.close()
        return stats
    except Exception as e:
        print(f"Error fetching stats: {e}")
        return {
            'total': 0, 'big4': 0, 'cable': 0, 'gi': 0, 
            'clickers': 0, 'high_quality': 0, 'providers': [], 'quality': []
        }

# HTML Template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Email Database Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e; color: #eee; padding: 20px;
        }
        h1 { color: #00d4ff; margin-bottom: 20px; }
        h2 { color: #00d4ff; margin: 20px 0 10px; font-size: 1.2em; }
        .container { max-width: 1400px; margin: 0 auto; }
        .stats-grid { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 15px; margin-bottom: 30px;
        }
        .stat-card {
            background: #16213e; padding: 20px; border-radius: 10px;
            border-left: 4px solid #00d4ff;
        }
        .stat-card h3 { color: #888; font-size: 0.9em; margin-bottom: 5px; }
        .stat-card .value { font-size: 2em; color: #00d4ff; font-weight: bold; }
        .stat-card .sub { color: #666; font-size: 0.85em; }
        
        .section { background: #16213e; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #333; }
        th { background: #0f3460; color: #00d4ff; }
        tr:hover { background: #1f4068; }
        
        .bar { 
            background: #0f3460; height: 20px; border-radius: 3px; 
            position: relative; margin: 5px 0;
        }
        .bar-fill { 
            background: linear-gradient(90deg, #00d4ff, #0099cc); 
            height: 100%; border-radius: 3px; 
        }
        .bar-label { position: absolute; right: 10px; top: 2px; font-size: 0.8em; }
        
        .query-box { 
            width: 100%; padding: 15px; background: #0a0a1a; border: 1px solid #333;
            color: #0f0; font-family: monospace; font-size: 14px; border-radius: 5px;
            margin-bottom: 10px;
        }
        button {
            background: #00d4ff; color: #000; border: none; padding: 10px 20px;
            border-radius: 5px; cursor: pointer; font-weight: bold;
        }
        button:hover { background: #00a8cc; }
        
        .filters { display: flex; gap: 15px; flex-wrap: wrap; margin-bottom: 15px; }
        .filter-group { display: flex; flex-direction: column; }
        .filter-group label { font-size: 0.8em; color: #888; margin-bottom: 3px; }
        select, input { 
            padding: 8px; background: #0a0a1a; border: 1px solid #333; 
            color: #eee; border-radius: 5px;
        }
        
        #results { margin-top: 20px; overflow-x: auto; }
        .loading { color: #00d4ff; font-style: italic; }
        .error { color: #ff6b6b; }
        
        /* Tab Navigation */
        .tabs { display: flex; gap: 5px; margin-bottom: 20px; border-bottom: 2px solid #0f3460; padding-bottom: 10px; }
        .tab-btn { 
            background: #16213e; color: #888; border: none; padding: 12px 24px;
            border-radius: 5px 5px 0 0; cursor: pointer; font-weight: bold; font-size: 0.95em;
            transition: all 0.2s;
        }
        .tab-btn:hover { background: #1f4068; color: #ccc; }
        .tab-btn.active { background: #0f3460; color: #00d4ff; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        
        /* MX Validator Styles */
        .mx-terminal {
            background: #0a0a0a; border: 1px solid #333; border-radius: 5px;
            height: 400px; overflow-y: auto; padding: 15px; font-family: 'Consolas', 'Monaco', monospace;
            font-size: 13px; line-height: 1.4;
        }
        .mx-terminal::-webkit-scrollbar { width: 8px; }
        .mx-terminal::-webkit-scrollbar-track { background: #1a1a1a; }
        .mx-terminal::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
        .mx-log-line { margin: 2px 0; }
        .mx-log-time { color: #666; }
        .mx-log-domain { color: #00d4ff; }
        .mx-log-arrow { color: #666; }
        .mx-log-mx { color: #0f0; }
        .mx-log-category { padding: 2px 6px; border-radius: 3px; font-size: 0.85em; margin-left: 8px; }
        .mx-cat-google { background: #1a73e8; color: #fff; }
        .mx-cat-microsoft { background: #00a4ef; color: #fff; }
        .mx-cat-yahoo { background: #720e9e; color: #fff; }
        .mx-cat-hostgator { background: #f37321; color: #fff; }
        .mx-cat-godaddy { background: #1bdbdb; color: #000; }
        .mx-cat-dead { background: #dc3545; color: #fff; }
        .mx-cat-realgi { background: #28a745; color: #fff; }
        .mx-cat-other { background: #6c757d; color: #fff; }
        
        .mx-stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .mx-stat { background: #0f3460; padding: 15px; border-radius: 5px; text-align: center; }
        .mx-stat-value { font-size: 1.8em; font-weight: bold; color: #00d4ff; }
        .mx-stat-label { font-size: 0.85em; color: #888; }
        
        /* Toggle Switch */
        .switch { position: relative; display: inline-block; width: 50px; height: 26px; }
        .switch input { opacity: 0; width: 0; height: 0; }
        .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #333; transition: .3s; border-radius: 26px; }
        .slider:before { position: absolute; content: ""; height: 20px; width: 20px; left: 3px; bottom: 3px; background-color: #666; transition: .3s; border-radius: 50%; }
        input:checked + .slider { background-color: #00d4ff; }
        input:checked + .slider:before { transform: translateX(24px); background-color: #fff; }
        
        .mx-controls { margin-bottom: 20px; display: flex; gap: 10px; align-items: center; }
        .mx-controls button { padding: 10px 20px; }
        .btn-start { background: #28a745; }
        .btn-start:hover { background: #218838; }
        .btn-pause { background: #ffc107; color: #000; }
        .btn-pause:hover { background: #e0a800; }
        .btn-stop { background: #dc3545; }
        .btn-stop:hover { background: #c82333; }
        
        .mx-progress { flex: 1; margin-left: 20px; }
        .mx-progress-bar { background: #0f3460; height: 24px; border-radius: 12px; overflow: hidden; }
        .mx-progress-fill { background: linear-gradient(90deg, #00d4ff, #28a745); height: 100%; transition: width 0.3s; }
        .mx-progress-text { font-size: 0.9em; color: #888; margin-top: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Email Database Dashboard</h1>
        
        <!-- Tab Navigation -->
        <div class="tabs">
            <button type="button" class="tab-btn" onclick="showTab('stats')">Stats</button>
            <button type="button" class="tab-btn" onclick="showTab('query')">Query Tool</button>
            <button type="button" class="tab-btn active" onclick="showTab('mx')">MX Validator</button>
            <button type="button" class="tab-btn" onclick="showTab('import')">Import Data</button>
            <button type="button" class="tab-btn" onclick="showTab('config')">Domain Config</button>
            <button type="button" class="tab-btn" onclick="showTab('cloudflare')">Cloudflare</button>
            <button type="button" class="tab-btn" onclick="showTab('reputation')">Domain Reputation</button>
        </div>
        
        <!-- STATS TAB -->
        <div id="tab-stats" class="tab-content">
        
        <p style="color: #666; font-size: 0.85em; margin-bottom: 12px;">
            Cache updated: <span id="stats-updated">-</span> | 
            <button onclick="loadDetailedStats()" style="font-size: 12px; padding: 2px 8px;">Load Cached Stats</button>
            <button onclick="recalculateStats()" style="font-size: 12px; padding: 2px 8px; margin-left: 5px; background: #fd7e14;">Recalculate (slow)</button>
            <span id="stats-loading" style="display:none; color: #ffc107; margin-left: 10px;">Loading...</span>
        </p>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
        
        <!-- LEFT COLUMN -->
        <div>
        
        <!-- MASTER STATS TABLE -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #00d4ff; margin: 0 0 10px 0; font-size: 14px;">Category Totals</h3>
            <table style="width: 100%; font-size: 13px; border-collapse: collapse;">
                <tr style="border-bottom: 2px solid #444;">
                    <td style="font-weight: bold; padding: 4px 0;">Category</td>
                    <td style="text-align: right; font-weight: bold; padding: 4px 8px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 4px 8px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 4px 8px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 4px 8px;">Clickers</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 4px 8px;">Openers</td>
                </tr>
                <tr style="background: #1a1a1a; border-bottom: 1px solid #333;">
                    <td style="font-weight: bold; padding: 6px 0;">TOTAL</td>
                    <td style="text-align: right; font-weight: bold; padding: 6px 8px;" id="stat-total">-</td>
                    <td style="text-align: right; color: #28a745; padding: 6px 8px;" id="stat-good-total">-</td>
                    <td style="text-align: right; color: #dc3545; padding: 6px 8px;" id="stat-dead-total">-</td>
                    <td style="text-align: right; padding: 6px 8px;" id="stat-clickers">-</td>
                    <td style="text-align: right; padding: 6px 8px;" id="stat-openers">-</td>
                </tr>
                <tr style="border-bottom: 1px solid #333;">
                    <td style="color: #ffc107; padding: 4px 0;">Big4 ISPs</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-big4-total">-</td>
                    <td style="text-align: right; color: #28a745; padding: 4px 8px;" id="stat-big4-good">-</td>
                    <td style="text-align: right; color: #dc3545; padding: 4px 8px;" id="stat-big4-dead">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-clickers-big4">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-openers-big4">-</td>
                </tr>
                <tr style="border-bottom: 1px solid #333;">
                    <td style="color: #17a2b8; padding: 4px 0;">2nd Level Big4</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-2nd-big4-total">-</td>
                    <td style="text-align: right; color: #28a745; padding: 4px 8px;" id="stat-2nd-big4-good">-</td>
                    <td style="text-align: right; color: #dc3545; padding: 4px 8px;" id="stat-2nd-big4-dead">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-2nd-big4-clickers">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-2nd-big4-openers">-</td>
                </tr>
                <tr style="border-bottom: 1px solid #333;">
                    <td style="color: #6f42c1; padding: 4px 0;">Cable Providers</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-cable-total">-</td>
                    <td style="text-align: right; color: #28a745; padding: 4px 8px;" id="stat-cable-good">-</td>
                    <td style="text-align: right; color: #dc3545; padding: 4px 8px;" id="stat-cable-dead">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-clickers-cable">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-openers-cable">-</td>
                </tr>
                <tr style="border-bottom: 1px solid #333;">
                    <td style="color: #20c997; padding: 4px 0;">General Internet</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-gi-total">-</td>
                    <td style="text-align: right; color: #28a745; padding: 4px 8px;" id="stat-gi-good">-</td>
                    <td style="text-align: right; color: #dc3545; padding: 4px 8px;" id="stat-gi-dead">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-clickers-gi">-</td>
                    <td style="text-align: right; padding: 4px 8px;" id="stat-openers-gi">-</td>
                </tr>
            </table>
        </div>
        
        <!-- BIG 4 BREAKDOWN -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #ffc107; margin: 0 0 10px 0; font-size: 14px;">Big 4 Breakdown</h3>
            <table style="width: 100%; font-size: 11px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #444;">
                    <td style="font-weight: bold; padding: 3px 0;">Provider</td>
                    <td style="text-align: right; font-weight: bold; padding: 3px 3px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 3px 3px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 3px 3px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #00d4ff; padding: 3px 3px;">High</td>
                    <td style="text-align: right; font-weight: bold; color: #ffc107; padding: 3px 3px;">Med</td>
                    <td style="text-align: right; font-weight: bold; color: #6c757d; padding: 3px 3px;">Low</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 3px 3px;">Click</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 3px 3px;">Open</td>
                    <td style="text-align: right; font-weight: bold; color: #888; padding: 3px 3px;">Doms</td>
                </tr>
                <tr><td>Gmail</td><td style="text-align: right;" id="stat-gmail">-</td><td style="text-align: right; color: #28a745;" id="stat-gmail-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-gmail-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-gmail-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-gmail-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-gmail-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-gmail-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-gmail-open">-</td><td style="text-align: right; color: #888;" id="stat-gmail-domains">-</td></tr>
                <tr><td>Yahoo</td><td style="text-align: right;" id="stat-yahoo">-</td><td style="text-align: right; color: #28a745;" id="stat-yahoo-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-yahoo-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-yahoo-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-yahoo-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-yahoo-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-yahoo-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-yahoo-open">-</td><td style="text-align: right; color: #888;" id="stat-yahoo-domains">-</td></tr>
                <tr><td>Outlook</td><td style="text-align: right;" id="stat-outlook">-</td><td style="text-align: right; color: #28a745;" id="stat-outlook-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-outlook-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-outlook-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-outlook-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-outlook-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-outlook-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-outlook-open">-</td><td style="text-align: right; color: #888;" id="stat-outlook-domains">-</td></tr>
            </table>
        </div>
        
        <!-- CABLE PROVIDER BREAKDOWN -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #6f42c1; margin: 0 0 10px 0; font-size: 14px;">Cable Provider Breakdown</h3>
            <table style="width: 100%; font-size: 11px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #444;">
                    <td style="font-weight: bold; padding: 3px 0;">Provider</td>
                    <td style="text-align: right; font-weight: bold; padding: 3px 3px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 3px 3px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 3px 3px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #00d4ff; padding: 3px 3px;">High</td>
                    <td style="text-align: right; font-weight: bold; color: #ffc107; padding: 3px 3px;">Med</td>
                    <td style="text-align: right; font-weight: bold; color: #6c757d; padding: 3px 3px;">Low</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 3px 3px;">Click</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 3px 3px;">Open</td>
                    <td style="text-align: right; font-weight: bold; color: #888; padding: 3px 3px;">Doms</td>
                </tr>
                <tr><td>Comcast</td><td style="text-align: right;" id="stat-comcast">-</td><td style="text-align: right; color: #28a745;" id="stat-comcast-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-comcast-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-comcast-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-comcast-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-comcast-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-comcast-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-comcast-open">-</td><td style="text-align: right; color: #888;" id="stat-comcast-domains">-</td></tr>
                <tr><td>Spectrum/RR</td><td style="text-align: right;" id="stat-spectrum">-</td><td style="text-align: right; color: #28a745;" id="stat-spectrum-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-spectrum-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-spectrum-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-spectrum-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-spectrum-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-spectrum-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-spectrum-open">-</td><td style="text-align: right; color: #888;" id="stat-spectrum-domains">-</td></tr>
                <tr><td>CenturyLink</td><td style="text-align: right;" id="stat-centurylink">-</td><td style="text-align: right; color: #28a745;" id="stat-centurylink-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-centurylink-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-centurylink-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-centurylink-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-centurylink-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-centurylink-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-centurylink-open">-</td><td style="text-align: right; color: #888;" id="stat-centurylink-domains">-</td></tr>
                <tr><td>EarthLink</td><td style="text-align: right;" id="stat-earthlink">-</td><td style="text-align: right; color: #28a745;" id="stat-earthlink-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-earthlink-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-earthlink-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-earthlink-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-earthlink-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-earthlink-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-earthlink-open">-</td><td style="text-align: right; color: #888;" id="stat-earthlink-domains">-</td></tr>
                <tr><td>Windstream</td><td style="text-align: right;" id="stat-windstream">-</td><td style="text-align: right; color: #28a745;" id="stat-windstream-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-windstream-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-windstream-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-windstream-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-windstream-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-windstream-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-windstream-open">-</td><td style="text-align: right; color: #888;" id="stat-windstream-domains">-</td></tr>
                <tr><td>Optimum</td><td style="text-align: right;" id="stat-optimum">-</td><td style="text-align: right; color: #28a745;" id="stat-optimum-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-optimum-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-optimum-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-optimum-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-optimum-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-optimum-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-optimum-open">-</td><td style="text-align: right; color: #888;" id="stat-optimum-domains">-</td></tr>
            </table>
        </div>
        
        <!-- 2ND LEVEL BIG4 BREAKDOWN -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #17a2b8; margin: 0 0 10px 0; font-size: 14px;">2nd Level Big4 (GI on Big4 MX)</h3>
            <table style="width: 100%; font-size: 11px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #444;">
                    <td style="font-weight: bold; padding: 3px 0;">Provider</td>
                    <td style="text-align: right; font-weight: bold; padding: 3px 3px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 3px 3px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 3px 3px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #00d4ff; padding: 3px 3px;">High</td>
                    <td style="text-align: right; font-weight: bold; color: #ffc107; padding: 3px 3px;">Med</td>
                    <td style="text-align: right; font-weight: bold; color: #6c757d; padding: 3px 3px;">Low</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 3px 3px;">Click</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 3px 3px;">Open</td>
                    <td style="text-align: right; font-weight: bold; color: #888; padding: 3px 3px;">Doms</td>
                </tr>
                <tr><td>Google</td><td style="text-align: right;" id="stat-google-hosted">-</td><td style="text-align: right; color: #28a745;" id="stat-google-hosted-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-google-hosted-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-google-hosted-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-google-hosted-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-google-hosted-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-google-hosted-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-google-hosted-open">-</td><td style="text-align: right; color: #888;" id="stat-google-hosted-domains">-</td></tr>
                <tr><td>Microsoft</td><td style="text-align: right;" id="stat-microsoft-hosted">-</td><td style="text-align: right; color: #28a745;" id="stat-microsoft-hosted-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-microsoft-hosted-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-microsoft-hosted-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-microsoft-hosted-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-microsoft-hosted-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-microsoft-hosted-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-microsoft-hosted-open">-</td><td style="text-align: right; color: #888;" id="stat-microsoft-hosted-domains">-</td></tr>
                <tr><td>Yahoo</td><td style="text-align: right;" id="stat-yahoo-hosted">-</td><td style="text-align: right; color: #28a745;" id="stat-yahoo-hosted-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-yahoo-hosted-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-yahoo-hosted-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-yahoo-hosted-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-yahoo-hosted-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-yahoo-hosted-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-yahoo-hosted-open">-</td><td style="text-align: right; color: #888;" id="stat-yahoo-hosted-domains">-</td></tr>
            </table>
        </div>
        
        </div><!-- END LEFT COLUMN -->
        
        <!-- RIGHT COLUMN -->
        <div>
        
        <!-- GI HOSTING PROVIDERS BREAKDOWN -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #fd7e14; margin: 0 0 10px 0; font-size: 14px;">GI Hosting Breakdown (by MX)</h3>
            <table style="width: 100%; font-size: 11px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #444;">
                    <td style="font-weight: bold; padding: 3px 0;">Provider</td>
                    <td style="text-align: right; font-weight: bold; padding: 3px 3px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 3px 3px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 3px 3px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #00d4ff; padding: 3px 3px;">High</td>
                    <td style="text-align: right; font-weight: bold; color: #ffc107; padding: 3px 3px;">Med</td>
                    <td style="text-align: right; font-weight: bold; color: #6c757d; padding: 3px 3px;">Low</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 3px 3px;">Click</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 3px 3px;">Open</td>
                    <td style="text-align: right; font-weight: bold; color: #888; padding: 3px 3px;">Doms</td>
                </tr>
                <tr style="background: #1a1a2e;"><td style="font-weight: bold;">Apple</td><td style="text-align: right;" id="stat-apple">-</td><td style="text-align: right; color: #28a745;" id="stat-apple-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-apple-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-apple-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-apple-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-apple-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-apple-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-apple-open">-</td><td style="text-align: right; color: #888;" id="stat-apple-domains">3</td></tr>
                <tr><td>GoDaddy</td><td style="text-align: right;" id="stat-godaddy">-</td><td style="text-align: right; color: #28a745;" id="stat-godaddy-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-godaddy-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-godaddy-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-godaddy-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-godaddy-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-godaddy-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-godaddy-open">-</td><td style="text-align: right; color: #888;" id="stat-godaddy-domains">-</td></tr>
                <tr><td>1&1/IONOS</td><td style="text-align: right;" id="stat-1and1">-</td><td style="text-align: right; color: #28a745;" id="stat-1and1-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-1and1-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-1and1-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-1and1-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-1and1-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-1and1-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-1and1-open">-</td><td style="text-align: right; color: #888;" id="stat-1and1-domains">-</td></tr>
                <tr><td>HostGator</td><td style="text-align: right;" id="stat-hostgator">-</td><td style="text-align: right; color: #28a745;" id="stat-hostgator-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-hostgator-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-hostgator-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-hostgator-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-hostgator-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-hostgator-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-hostgator-open">-</td><td style="text-align: right; color: #888;" id="stat-hostgator-domains">-</td></tr>
                <tr><td>Namecheap</td><td style="text-align: right;" id="stat-namecheap">-</td><td style="text-align: right; color: #28a745;" id="stat-namecheap-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-namecheap-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-namecheap-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-namecheap-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-namecheap-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-namecheap-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-namecheap-open">-</td><td style="text-align: right; color: #888;" id="stat-namecheap-domains">-</td></tr>
                <tr><td>Zoho</td><td style="text-align: right;" id="stat-zoho">-</td><td style="text-align: right; color: #28a745;" id="stat-zoho-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-zoho-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-zoho-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-zoho-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-zoho-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-zoho-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-zoho-open">-</td><td style="text-align: right; color: #888;" id="stat-zoho-domains">-</td></tr>
                <tr><td>Fastmail</td><td style="text-align: right;" id="stat-fastmail">-</td><td style="text-align: right; color: #28a745;" id="stat-fastmail-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-fastmail-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-fastmail-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-fastmail-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-fastmail-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-fastmail-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-fastmail-open">-</td><td style="text-align: right; color: #888;" id="stat-fastmail-domains">-</td></tr>
                <tr><td>Amazon SES</td><td style="text-align: right;" id="stat-amazonses">-</td><td style="text-align: right; color: #28a745;" id="stat-amazonses-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-amazonses-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-amazonses-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-amazonses-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-amazonses-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-amazonses-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-amazonses-open">-</td><td style="text-align: right; color: #888;" id="stat-amazonses-domains">-</td></tr>
                <tr><td>ProtonMail</td><td style="text-align: right;" id="stat-protonmail">-</td><td style="text-align: right; color: #28a745;" id="stat-protonmail-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-protonmail-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-protonmail-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-protonmail-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-protonmail-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-protonmail-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-protonmail-open">-</td><td style="text-align: right; color: #888;" id="stat-protonmail-domains">-</td></tr>
                <tr><td>Cloudflare</td><td style="text-align: right;" id="stat-cloudflare">-</td><td style="text-align: right; color: #28a745;" id="stat-cloudflare-good">-</td><td style="text-align: right; color: #dc3545;" id="stat-cloudflare-dead">-</td><td style="text-align: right; color: #00d4ff;" id="stat-cloudflare-high">-</td><td style="text-align: right; color: #ffc107;" id="stat-cloudflare-med">-</td><td style="text-align: right; color: #6c757d;" id="stat-cloudflare-low">-</td><td style="text-align: right; color: #e83e8c;" id="stat-cloudflare-click">-</td><td style="text-align: right; color: #17a2b8;" id="stat-cloudflare-open">-</td><td style="text-align: right; color: #888;" id="stat-cloudflare-domains">-</td></tr>
                <tr style="border-top: 1px solid #444;"><td style="padding-top: 6px;">GI Unique Domains</td><td colspan="9" style="text-align: right; padding-top: 6px;" id="stat-gi-domains">-</td></tr>
            </table>
        </div>
        
        <!-- TOP 10 GI DOMAINS -->
        <div class="section" style="padding: 12px;">
            <h3 style="color: #20c997; margin: 0 0 10px 0; font-size: 14px;">Top 10 GI Domains (by volume)</h3>
            <table style="width: 100%; font-size: 11px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #444;">
                    <td style="font-weight: bold; padding: 3px 0;">Domain</td>
                    <td style="text-align: right; font-weight: bold; padding: 3px 3px;">Total</td>
                    <td style="text-align: right; font-weight: bold; color: #28a745; padding: 3px 3px;">Good</td>
                    <td style="text-align: right; font-weight: bold; color: #dc3545; padding: 3px 3px;">Dead</td>
                    <td style="text-align: right; font-weight: bold; color: #00d4ff; padding: 3px 3px;">High</td>
                    <td style="text-align: right; font-weight: bold; color: #ffc107; padding: 3px 3px;">Med</td>
                    <td style="text-align: right; font-weight: bold; color: #6c757d; padding: 3px 3px;">Low</td>
                    <td style="text-align: right; font-weight: bold; color: #e83e8c; padding: 3px 3px;">Click</td>
                    <td style="text-align: right; font-weight: bold; color: #17a2b8; padding: 3px 3px;">Open</td>
                </tr>
                <tbody id="top-gi-domains-body">
                    <tr><td colspan="9" style="text-align: center; color: #666; padding: 10px;">Click "Recalculate" to load</td></tr>
                </tbody>
            </table>
        </div>
        
        </div><!-- END RIGHT COLUMN -->
        
        </div><!-- END GRID -->
        
        <!-- INTENT SCORE KEY -->
        <div style="margin-top: 20px; padding: 12px; background: #1a1a2e; border-radius: 6px; border: 1px solid #333;">
            <h4 style="color: #00d4ff; margin: 0 0 10px 0; font-size: 13px;">Intent Score Key</h4>
            <div style="display: flex; gap: 30px; font-size: 11px; margin-bottom: 12px;">
                <div><span style="color: #00d4ff; font-weight: bold;">High (70-100)</span> - Premium quality, verified, engaged</div>
                <div><span style="color: #ffc107; font-weight: bold;">Med (40-69)</span> - Moderate quality, some engagement</div>
                <div><span style="color: #6c757d; font-weight: bold;">Low (0-39)</span> - Low quality, unverified, or no score</div>
            </div>
            
            <h4 style="color: #888; margin: 10px 0 8px 0; font-size: 12px; border-top: 1px solid #333; padding-top: 10px;">Quality Weighting System</h4>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; font-size: 11px; color: #aaa;">
                <div>
                    <div style="color: #28a745; font-weight: bold; margin-bottom: 5px;">Positive Signals (+points)</div>
                    <div>+20 - Has clicked (is_clicker)</div>
                    <div>+15 - Has opened (is_opener)</div>
                    <div>+10 - Has full name (first + last)</div>
                    <div>+10 - Has phone number</div>
                    <div>+10 - Has physical address</div>
                    <div>+10 - Has date of birth</div>
                    <div>+5 - Has city/state/zip</div>
                    <div>+5 - Has signup date</div>
                    <div>+5 - Big4/Cable domain (deliverable)</div>
                </div>
                <div>
                    <div style="color: #dc3545; font-weight: bold; margin-bottom: 5px;">Negative Signals (-points)</div>
                    <div>-30 - Dead/No MX (undeliverable)</div>
                    <div>-20 - Invalid email format</div>
                    <div>-15 - Disposable/temp domain</div>
                    <div>-10 - Role-based (info@, admin@)</div>
                    <div>-10 - Missing all PII data</div>
                    <div>-5 - Unknown/unverified domain</div>
                </div>
            </div>
            <div style="margin-top: 10px; font-size: 10px; color: #666;">
                Note: Scores are imported from source files. Max score = 100. Emails with engagement (clickers/openers) receive highest weight.
            </div>
        </div>
        
        </div><!-- END STATS TAB -->
        
        <!-- QUERY TAB -->
        <div id="tab-query" class="tab-content">
        
        <!-- Query Tool -->
        <div class="section">
            <h2>Query Tool - Advanced Search</h2>
            
            <!-- BASIC FILTERS (Always Visible) -->
            <div class="filter-section">
                <h4 style="color: #00d4ff; margin: 0 0 10px 0; border-bottom: 1px solid #333; padding-bottom: 5px;">Basic Filters</h4>
                <div class="filters">
                    <div class="filter-group">
                        <label>Email Search</label>
                        <input type="text" id="email_search" placeholder="email or partial" style="width: 150px;">
                    </div>
                    <div class="filter-group">
                        <label>Provider</label>
                        <select id="provider">
                            <option value="">All</option>
                            <optgroup label="Big 4 ISPs">
                                <option value="Yahoo">Yahoo</option>
                                <option value="Google">Google</option>
                                <option value="Microsoft">Microsoft</option>
                            </optgroup>
                            <optgroup label="Cable/Telecom">
                                <option value="Comcast">Comcast/Xfinity</option>
                                <option value="AT&T">AT&T</option>
                                <option value="Charter">Charter/Spectrum</option>
                                <option value="Spectrum">Spectrum/Roadrunner</option>
                                <option value="CenturyLink">CenturyLink</option>
                                <option value="Altice">Altice/Optimum</option>
                                <option value="Apple">Apple/iCloud</option>
                                <option value="EarthLink">EarthLink</option>
                                <option value="Windstream">Windstream</option>
                                <option value="Mediacom">Mediacom</option>
                                <option value="Juno">Juno</option>
                                <option value="NetZero">NetZero</option>
                            </optgroup>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Category</label>
                        <select id="category">
                            <option value="">All</option>
                            <option value="Big4_ISP">Big 4 ISP</option>
                            <option value="Cable_Provider">Cable Provider</option>
                            <option value="General_Internet">General Internet</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Domain</label>
                        <input type="text" id="domain" placeholder="e.g. gmail.com" style="width: 120px;">
                    </div>
                    <div class="filter-group">
                        <label>State</label>
                        <select id="state">
                            <option value="">All</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Per Page</label>
                        <select id="limit">
                            <option value="50">50</option>
                            <option value="100">100</option>
                            <option value="500" selected>500</option>
                            <option value="1000">1000</option>
                            <option value="2500">2500</option>
                            <option value="5000">5000</option>
                        </select>
                    </div>
                </div>
            </div>
            
            <!-- PERSONAL INFO (Collapsible) -->
            <div class="filter-section">
                <h4 style="color: #ffc107; margin: 10px 0; cursor: pointer; border-bottom: 1px solid #333; padding-bottom: 5px;" onclick="toggleFilterSection('personal-filters')">
                    Personal Info <span id="personal-filters-toggle" style="float: right;">+</span>
                </h4>
                <div id="personal-filters" class="filters" style="display: none;">
                    <div class="filter-group">
                        <label>First Name</label>
                        <input type="text" id="first_name" placeholder="John" style="width: 100px;">
                    </div>
                    <div class="filter-group">
                        <label>Last Name</label>
                        <input type="text" id="last_name" placeholder="Smith" style="width: 100px;">
                    </div>
                    <div class="filter-group">
                        <label>City</label>
                        <select id="city" style="max-width: 180px;">
                            <option value="">All Cities</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Zipcode</label>
                        <select id="zipcode" style="max-width: 120px;">
                            <option value="">All Zips</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Gender</label>
                        <select id="gender">
                            <option value="">Any</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Country</label>
                        <select id="country">
                            <option value="">All</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Has Phone</label>
                        <select id="has_phone">
                            <option value="">Any</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Has DOB</label>
                        <select id="has_dob">
                            <option value="">Any</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                </div>
            </div>
            
            <!-- ENGAGEMENT (Collapsible) -->
            <div class="filter-section">
                <h4 style="color: #28a745; margin: 10px 0; cursor: pointer; border-bottom: 1px solid #333; padding-bottom: 5px;" onclick="toggleFilterSection('engagement-filters')">
                    Engagement <span id="engagement-filters-toggle" style="float: right;">+</span>
                </h4>
                <div id="engagement-filters" class="filters" style="display: none;">
                    <div class="filter-group">
                        <label>Is Clicker</label>
                        <select id="clickers">
                            <option value="">Any</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Is Opener</label>
                        <select id="openers">
                            <option value="">Any</option>
                            <option value="true">Yes</option>
                            <option value="false">No</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Quality Tier</label>
                        <select id="quality_tier">
                            <option value="">Any</option>
                            <option value="high">High (70-100)</option>
                            <option value="mid">Mid (40-69)</option>
                            <option value="low">Low (0-39)</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Validation Status</label>
                        <select id="validation_status">
                            <option value="">All</option>
                        </select>
                    </div>
                </div>
            </div>
            
            <!-- SOURCE TRACKING (Collapsible) -->
            <div class="filter-section">
                <h4 style="color: #17a2b8; margin: 10px 0; cursor: pointer; border-bottom: 1px solid #333; padding-bottom: 5px;" onclick="toggleFilterSection('source-filters')">
                    Source Tracking <span id="source-filters-toggle" style="float: right;">+</span>
                </h4>
                <div id="source-filters" class="filters" style="display: none;">
                    <div class="filter-group">
                        <label>Data Source</label>
                        <select id="data_source" style="max-width: 180px;">
                            <option value="">All</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>File Source</label>
                        <select id="file_source" style="max-width: 180px;">
                            <option value="">All Files</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Signup Domain</label>
                        <input type="text" id="signup_domain" placeholder="example.com" style="width: 120px;">
                    </div>
                    <div class="filter-group">
                        <label>Signup IP</label>
                        <input type="text" id="signup_ip" placeholder="192.168.x.x" style="width: 120px;">
                    </div>
                    <div class="filter-group">
                        <label>Signup After</label>
                        <input type="date" id="signup_date_from" style="width: 130px;">
                    </div>
                    <div class="filter-group">
                        <label>Signup Before</label>
                        <input type="date" id="signup_date_to" style="width: 130px;">
                    </div>
                </div>
            </div>
            
            <!-- LIST BUILDER -->
            <div class="filter-section" style="background: #0d2137; border: 2px solid #00d4ff; border-radius: 8px; padding: 15px; margin-top: 15px;">
                <h4 style="color: #00d4ff; margin: 0 0 15px 0;">List Builder - Combine Multiple Segments</h4>
                
                <div style="display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 15px;">
                    <select id="list-builder-preset" style="padding: 8px; min-width: 200px;">
                        <optgroup label="By Category">
                            <option value="all_clickers">ALL Clickers</option>
                            <option value="all_openers">ALL Openers</option>
                            <option value="gi_clickers">GI Clickers</option>
                            <option value="gi_openers">GI Openers</option>
                            <option value="cable_clickers">Cable Provider Clickers</option>
                            <option value="cable_openers">Cable Provider Openers</option>
                            <option value="big4_clickers">Big4 ISP Clickers</option>
                            <option value="big4_openers">Big4 ISP Openers</option>
                        </optgroup>
                        <optgroup label="Big4 ISP Providers">
                            <option value="google_clickers">Google/Gmail Clickers</option>
                            <option value="google_openers">Google/Gmail Openers</option>
                            <option value="yahoo_clickers">Yahoo Clickers</option>
                            <option value="yahoo_openers">Yahoo Openers</option>
                            <option value="microsoft_clickers">Microsoft/Outlook Clickers</option>
                            <option value="microsoft_openers">Microsoft/Outlook Openers</option>
                            <option value="aol_clickers">AOL Clickers</option>
                            <option value="aol_openers">AOL Openers</option>
                        </optgroup>
                        <optgroup label="2nd Level Big4 (GI on Big4 MX)">
                            <option value="2nd_google_clickers">GI on Google MX - Clickers</option>
                            <option value="2nd_google_openers">GI on Google MX - Openers</option>
                            <option value="2nd_microsoft_clickers">GI on Microsoft MX - Clickers</option>
                            <option value="2nd_microsoft_openers">GI on Microsoft MX - Openers</option>
                            <option value="2nd_yahoo_clickers">GI on Yahoo MX - Clickers</option>
                            <option value="2nd_yahoo_openers">GI on Yahoo MX - Openers</option>
                            <option value="2nd_big4_all_clickers">All 2nd Level Big4 Clickers</option>
                            <option value="2nd_big4_all_openers">All 2nd Level Big4 Openers</option>
                        </optgroup>
                        <optgroup label="Cable/Other Providers">
                            <option value="apple_clickers">Apple/iCloud Clickers</option>
                            <option value="apple_openers">Apple/iCloud Openers</option>
                            <option value="spectrum_clickers">Spectrum Clickers</option>
                            <option value="spectrum_openers">Spectrum Openers</option>
                            <option value="comcast_clickers">Comcast/Xfinity Clickers</option>
                            <option value="comcast_openers">Comcast/Xfinity Openers</option>
                            <option value="att_clickers">AT&T Clickers</option>
                            <option value="att_openers">AT&T Openers</option>
                            <option value="godaddy_clickers">GoDaddy Clickers</option>
                            <option value="earthlink_clickers">EarthLink Clickers</option>
                        </optgroup>
                        <optgroup label="Quality Tiers">
                            <option value="high_quality">High Quality (70+)</option>
                            <option value="med_quality">Medium Quality (40-69)</option>
                            <option value="verified_all">All Verified</option>
                        </optgroup>
                    </select>
                    <button onclick="addToListBuilder()" style="background: #17a2b8;">+ Add Segment</button>
                    <button onclick="addCurrentFilters()" style="background: #6c757d;">+ Add Current Filters</button>
                </div>
                
                <div id="list-builder-segments" style="min-height: 40px; background: #1a1a2e; border-radius: 5px; padding: 10px; margin-bottom: 10px;">
                    <span style="color: #666; font-size: 12px;">No segments added yet. Select presets above or use "Add Current Filters".</span>
                </div>
                
                <div style="display: flex; gap: 10px; align-items: center;">
                    <button onclick="clearListBuilder()" style="background: #dc3545; font-size: 12px;">Clear All</button>
                    <button onclick="previewListBuilder()" style="background: #ffc107; color: #000; font-size: 12px;">Preview Count</button>
                    <button onclick="exportListBuilder()" style="background: #28a745; font-size: 12px;">Export Combined List</button>
                    <label style="display: flex; align-items: center; gap: 5px; font-size: 12px; color: #888; cursor: pointer;">
                        <input type="checkbox" id="export-email-only" style="cursor: pointer;"> Email only
                    </label>
                    <span id="list-builder-count" style="color: #888; font-size: 12px;"></span>
                </div>
            </div>
            
            <!-- Action Buttons -->
            <div style="margin-top: 15px;">
                <button onclick="runQuery(1)">Search</button>
                <button onclick="clearAllFilters()" style="background: #6c757d;">Clear Filters</button>
                <button onclick="exportCSV()" style="background: #28a745;">Export CSV</button>
                <span id="result-count" style="margin-left: 20px; color: #888;"></span>
            </div>
            
            <!-- Pagination controls -->
            <div id="pagination" style="margin-top: 15px; display: none;">
                <button onclick="prevPage()" id="prevBtn" disabled>Previous</button>
                <span id="pageInfo" style="margin: 0 15px; color: #888;">Page 1</span>
                <button onclick="nextPage()" id="nextBtn">Next</button>
            </div>
            
            <div id="results"></div>
        </div>
        
        <!-- Custom SQL -->
        <div class="section">
            <h2>Custom SQL</h2>
            <textarea class="query-box" id="sql" rows="3">SELECT email, email_provider, email_brand, quality_score, is_clicker, city, state FROM emails LIMIT 20</textarea>
            <button onclick="runSQL()">Execute SQL</button>
            <div id="sql-results"></div>
        </div>
        
        </div><!-- END QUERY TAB -->
        
        <!-- MX VALIDATOR TAB (default tab - loads fast, no heavy counts) -->
        <div id="tab-mx" class="tab-content active">
        
        <div class="section">
            <h2>MX Domain Validator</h2>
            <p style="color: #888; margin-bottom: 15px;">Validate General Internet domains by checking MX records. Classifies domains by mail host provider and identifies dead domains.</p>
            <p style="color: #666; font-size: 0.9em; margin-bottom: 15px;">Only <strong>General Internet</strong> domains are scanned (GI only). Big4/Cable and other known-good domains in domain_mx are skipped, so &quot;Total&quot; is the GI count (~850K), not the full domain_mx row count (~3.7M).</p>
            
            <!-- MX Stats -->
            <div class="mx-stats-grid">
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-total">0</div>
                    <div class="mx-stat-label">Total (GI only)</div>
                </div>
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-checked">0</div>
                    <div class="mx-stat-label">Checked</div>
                </div>
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-remaining" style="color: #ffc107;">0</div>
                    <div class="mx-stat-label">Remaining</div>
                </div>
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-valid" style="color: #28a745;">0</div>
                    <div class="mx-stat-label">Valid (domains)</div>
                    <div class="mx-stat-sub" id="mx-valid-emails" style="color: #28a745; font-size: 0.85em;">0 emails</div>
                </div>
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-dead" style="color: #dc3545;">0</div>
                    <div class="mx-stat-label">Dead (domains)</div>
                    <div class="mx-stat-sub" id="mx-dead-emails" style="color: #dc3545; font-size: 0.85em;">0 emails</div>
                </div>
                <div class="mx-stat">
                    <div class="mx-stat-value" id="mx-rate">0</div>
                    <div class="mx-stat-label">Rate/sec</div>
                </div>
            </div>
            
            <!-- Controls -->
            <div class="mx-controls">
                <label style="margin-right: 10px; color: #888;">Workers:</label>
                <select id="mx-workers" style="margin-right: 15px; padding: 6px 10px; background: #1a1a2e; color: #fff; border: 1px solid #333; border-radius: 4px;">
                    <option value="16">16</option>
                    <option value="32">32</option>
                    <option value="48">48</option>
                    <option value="64" selected>64</option>
                    <option value="76">76</option>
                    <option value="88">88</option>
                    <option value="100">100</option>
                    <option value="112">112</option>
                    <option value="124">124</option>
                    <option value="136">136</option>
                    <option value="148">148</option>
                    <option value="160">160</option>
                    <option value="176">176</option>
                    <option value="188">188</option>
                    <option value="200">200</option>
                </select>
                <button type="button" class="btn-start" id="mx-start-btn" onclick="startMxScan()">Start Scan</button>
                <button type="button" style="background: #ffc107; color: #000;" id="mx-reset-dead-only-btn" onclick="resetDeadOnly()" title="Reset dead domains to unchecked (no scan)">Reset dead only</button>
                <button type="button" style="background: #6f42c1;" id="mx-sync-gi-btn" onclick="discoverNewDomains()" title="Find new GI domains from imported emails and add them for MX scanning">Discover New Domains</button>
                <button type="button" class="btn-pause" onclick="pauseMxScan()" id="mx-pause-btn" disabled>Pause</button>
                <button type="button" class="btn-stop" onclick="stopMxScan()" id="mx-stop-btn" disabled>Stop</button>
                <button type="button" style="background: #17a2b8; margin-left: 20px;" onclick="applyMxResults()">Apply to Emails</button>
                <div class="mx-progress">
                    <div class="mx-progress-bar">
                        <div class="mx-progress-fill" id="mx-progress-fill" style="width: 0%"></div>
                    </div>
                    <div class="mx-progress-text" id="mx-progress-text">Ready to scan</div>
                </div>
            </div>
            
            <!-- Live Log -->
            <h3 style="color: #00d4ff; margin: 20px 0 10px;">Live Log (Domain Results) - ETA: <span id="mx-eta" style="color: #ffc107;">calculating...</span></h3>
            <div class="mx-terminal" id="mx-terminal" style="height: 250px;">
                <div class="mx-log-line" style="color: #666;">// MX Validator ready. Click "Start Scan" to begin checking domains.</div>
                <div class="mx-log-line" style="color: #666;">// Will check GI domains (unchecked only) using rotating DNS servers.</div>
            </div>
            
            <!-- DB Commits Log -->
            <h3 style="color: #28a745; margin: 20px 0 10px;">DB Commits (500 domain batches)</h3>
            <div class="mx-terminal" id="mx-flush-terminal" style="height: 150px; border-color: #28a745;">
                <div class="mx-log-line" style="color: #666;">// Database commits will appear here as batches of 500 are written.</div>
                <div class="mx-log-line" style="color: #666;">// Each line = 500 domains saved to domain_mx table.</div>
            </div>
            
            <!-- Category Breakdown (domains + email counts per category) -->
            <h3 style="color: #00d4ff; margin: 20px 0 10px;">By MX Category (domains / emails)</h3>
            <div id="mx-categories" class="stats-grid">
                <div class="stat-card"><h3>Google Workspace</h3><div class="value" id="mx-cat-google">0</div><div class="sub" id="mx-cat-google-emails">0 emails</div></div>
                <div class="stat-card"><h3>Microsoft 365</h3><div class="value" id="mx-cat-microsoft">0</div><div class="sub" id="mx-cat-microsoft-emails">0 emails</div></div>
                <div class="stat-card"><h3>Yahoo Hosted</h3><div class="value" id="mx-cat-yahoo">0</div><div class="sub" id="mx-cat-yahoo-emails">0 emails</div></div>
                <div class="stat-card"><h3>HostGator</h3><div class="value" id="mx-cat-hostgator">0</div><div class="sub" id="mx-cat-hostgator-emails">0 emails</div></div>
                <div class="stat-card"><h3>GoDaddy</h3><div class="value" id="mx-cat-godaddy">0</div><div class="sub" id="mx-cat-godaddy-emails">0 emails</div></div>
                <div class="stat-card"><h3>Real GI</h3><div class="value" id="mx-cat-realgi">0</div><div class="sub" id="mx-cat-realgi-emails">0 emails</div></div>
                <div class="stat-card"><h3>Parked (SPAM TRAP)</h3><div class="value" id="mx-cat-parked" style="color: #ff6b6b;">0</div><div class="sub" id="mx-cat-parked-emails">0 emails</div></div>
                <div class="stat-card" style="cursor: pointer;" onclick="showDeadDomains()"><h3>Dead/Invalid (click to view)</h3><div class="value" id="mx-cat-dead" style="color: #dc3545;">0</div><div class="sub" id="mx-cat-dead-emails">0 emails</div></div>
            </div>
            
            <!-- DNS Server Performance (Valid/Dead) -->
            <h3 style="color: #00d4ff; margin: 20px 0 10px;">DNS Server Performance (Valid/Dead) <button onclick="resetDnsStats()" style="font-size: 12px; padding: 3px 10px; margin-left: 10px;">Clear Stats</button></h3>
            <div id="mx-dns-servers" class="stats-grid" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));">
                <div class="stat-card"><h3>Google-1</h3><div class="value" id="dns-google1" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Google-2</h3><div class="value" id="dns-google2" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Cloudflare-1</h3><div class="value" id="dns-cloudflare1" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Cloudflare-2</h3><div class="value" id="dns-cloudflare2" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>OpenDNS-1</h3><div class="value" id="dns-opendns1" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>OpenDNS-2</h3><div class="value" id="dns-opendns2" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Quad9-1</h3><div class="value" id="dns-quad91" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Quad9-2</h3><div class="value" id="dns-quad92" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Level3-1</h3><div class="value" id="dns-level31" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Level3-2</h3><div class="value" id="dns-level32" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Verisign-1</h3><div class="value" id="dns-verisign1" style="font-size: 14px;">0 / 0</div></div>
                <div class="stat-card"><h3>Verisign-2</h3><div class="value" id="dns-verisign2" style="font-size: 14px;">0 / 0</div></div>
            </div>
        </div>
        
        </div><!-- END MX TAB -->
        
        <!-- IMPORT DATA TAB -->
        <div id="tab-import" class="tab-content">
        <div class="section">
            <h2>Import External Data</h2>
            <p style="color: #888; margin-bottom: 15px;">Import email data from external files with enrichment upsert. Same email in multiple files = combined/enriched record.</p>
            
            <!-- Directory Input -->
            <div style="display: flex; gap: 10px; margin-bottom: 15px; align-items: center;">
                <label style="color: #aaa;">Directory:</label>
                <input type="text" id="import-dir" style="flex: 1; padding: 8px; background: #1a1a1a; border: 1px solid #333; color: #fff; border-radius: 4px;" 
                       placeholder="C:\\EmailData\\DataRickyJeffSep22\\DataRickyJeffSep22" value="C:\\EmailData\\DataRickyJeffSep22\\DataRickyJeffSep22">
                <button onclick="openDirBrowser()" style="background: #6c757d;">Browse...</button>
                <button onclick="scanImportDir()">Scan Files</button>
            </div>
            
            <!-- Directory Browser Modal -->
            <div id="dir-browser-modal" style="display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.8); z-index: 1000; padding: 50px;">
                <div style="background: #1a1a1a; border: 1px solid #333; border-radius: 10px; max-width: 600px; margin: 0 auto; max-height: 70vh; display: flex; flex-direction: column;">
                    <div style="padding: 15px 20px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center;">
                        <h3 style="margin: 0; color: #00d4ff;">Select Directory</h3>
                        <button onclick="closeDirBrowser()" style="background: #dc3545; padding: 5px 15px;">Close</button>
                    </div>
                    <div style="padding: 10px 20px; border-bottom: 1px solid #333; background: #252525;">
                        <div style="display: flex; gap: 10px; align-items: center;">
                            <span style="color: #888;">Path:</span>
                            <input type="text" id="dir-browser-path" style="flex: 1; padding: 6px; background: #1a1a1a; border: 1px solid #333; color: #fff; border-radius: 4px;" value="C:\\EmailData">
                            <button onclick="navigateToPath()" style="padding: 6px 12px;">Go</button>
                        </div>
                    </div>
                    <div id="dir-browser-list" style="flex: 1; overflow-y: auto; padding: 10px 20px;">
                        <div style="color: #666;">Loading...</div>
                    </div>
                    <div style="padding: 15px 20px; border-top: 1px solid #333; display: flex; justify-content: flex-end; gap: 10px;">
                        <button onclick="selectCurrentDir()" style="background: #28a745;">Select This Directory</button>
                    </div>
                </div>
            </div>
            
            <div style="display: flex; gap: 10px; margin-bottom: 15px; align-items: center;">
                <label style="color: #aaa;">Data Source Label:</label>
                <input type="text" id="import-source" style="width: 250px; padding: 8px; background: #1a1a1a; border: 1px solid #333; color: #fff; border-radius: 4px;" 
                       placeholder="RickyJeff_Sep22" value="RickyJeff_Sep22">
            </div>
            
            <!-- File List -->
            <div style="background: #1a1a1a; border: 1px solid #333; border-radius: 5px; padding: 10px; margin-bottom: 15px; max-height: 300px; overflow-y: auto;">
                <div style="display: flex; align-items: center; margin-bottom: 10px; padding-bottom: 10px; border-bottom: 1px solid #333;">
                    <input type="checkbox" id="import-select-all" checked onchange="toggleAllImportFiles()" style="margin-right: 10px;">
                    <label for="import-select-all" style="color: #aaa; font-weight: bold;">Select All Files</label>
                </div>
                <div id="import-file-list" style="color: #666;">
                    Click "Scan Directory" to list files...
                </div>
            </div>
            
            <!-- Action Buttons -->
            <div style="display: flex; gap: 10px; margin-bottom: 20px;">
                <button onclick="previewSelectedFile()" style="background: #6c757d;">Preview Selected</button>
                <button onclick="startImport()" style="background: #28a745;" id="btn-start-import">Start Import</button>
                <button onclick="stopImport()" style="background: #dc3545; display: none;" id="btn-stop-import">Stop Import</button>
            </div>
            
            <!-- Progress Section -->
            <div id="import-progress-section" style="display: none;">
                <h3 style="color: #00d4ff; margin: 20px 0 10px;">Import Progress</h3>
                <div style="background: #1a1a1a; border: 1px solid #333; border-radius: 5px; padding: 15px;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                        <span>Status: <strong id="import-status" style="color: #ffc107;">Idle</strong></span>
                        <span>File: <strong id="import-current-file">-</strong> (<span id="import-file-progress">0/0</span>)</span>
                    </div>
                    
                    <!-- Progress Bar -->
                    <div style="background: #333; border-radius: 5px; height: 25px; margin-bottom: 15px; overflow: hidden;">
                        <div id="import-progress-bar" style="background: linear-gradient(90deg, #28a745, #20c997); height: 100%; width: 0%; transition: width 0.3s; display: flex; align-items: center; justify-content: center;">
                            <span id="import-progress-pct" style="color: #fff; font-weight: bold; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);">0%</span>
                        </div>
                    </div>
                    
                    <!-- Stats -->
                    <div class="stats-grid" style="grid-template-columns: repeat(4, 1fr); gap: 10px;">
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.85em;">Processed</div>
                            <div style="color: #00d4ff; font-size: 1.2em; font-weight: bold;" id="import-total-processed">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.85em;">New Records</div>
                            <div style="color: #28a745; font-size: 1.2em; font-weight: bold;" id="import-new-records">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.85em;">Enriched</div>
                            <div style="color: #ffc107; font-size: 1.2em; font-weight: bold;" id="import-enriched">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.85em;">Rate</div>
                            <div style="color: #17a2b8; font-size: 1.2em; font-weight: bold;" id="import-rate">0/s</div>
                        </div>
                    </div>
                    
                    <!-- Category Breakdown -->
                    <h4 style="color: #aaa; margin: 15px 0 10px;">By Category (MX Status)</h4>
                    <div class="stats-grid" style="grid-template-columns: repeat(5, 1fr); gap: 10px;">
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.8em;">Big4 ISP</div>
                            <div style="color: #28a745; font-size: 1.1em;" id="import-big4">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.8em;">Cable</div>
                            <div style="color: #28a745; font-size: 1.1em;" id="import-cable">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.8em;">GI Valid</div>
                            <div style="color: #28a745; font-size: 1.1em;" id="import-gi-valid">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.8em;">GI Dead</div>
                            <div style="color: #dc3545; font-size: 1.1em;" id="import-gi-dead">0</div>
                        </div>
                        <div style="background: #252525; padding: 10px; border-radius: 5px; text-align: center;">
                            <div style="color: #888; font-size: 0.8em;">GI New</div>
                            <div style="color: #ffc107; font-size: 1.1em;" id="import-gi-new">0</div>
                        </div>
                    </div>
                    
                    <!-- Import Log -->
                    <h4 style="color: #aaa; margin: 15px 0 10px;">Import Log</h4>
                    <div id="import-log" style="background: #0a0a0a; border: 1px solid #333; border-radius: 5px; padding: 10px; height: 150px; overflow-y: auto; font-family: monospace; font-size: 12px; color: #888;">
                        <div style="color: #555;">Waiting for import to start...</div>
                    </div>
                </div>
            </div>
            
            <!-- Preview Modal -->
            <div id="import-preview-modal" style="display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.8); z-index: 1000; padding: 50px;">
                <div style="background: #1a1a1a; border: 1px solid #333; border-radius: 10px; max-width: 1000px; margin: 0 auto; max-height: 80vh; overflow: auto;">
                    <div style="padding: 20px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center;">
                        <h3 style="margin: 0; color: #00d4ff;">File Preview</h3>
                        <button onclick="closePreviewModal()" style="background: #dc3545;">Close</button>
                    </div>
                    <div id="import-preview-content" style="padding: 20px;">
                        Loading...
                    </div>
                </div>
            </div>
        </div>
        </div><!-- END IMPORT TAB -->
        
        <!-- DOMAIN CONFIG TAB -->
        <div id="tab-config" class="tab-content">
        <div class="section">
            <h2>Domain Configuration</h2>
            <p style="color: #888; margin-bottom: 15px;">View and manage Big4 ISP and Cable Provider domain mappings. Changes are saved to config.py.</p>
            
            <div style="display: flex; gap: 20px; margin-bottom: 20px;">
                <button onclick="loadDomainConfig()">Refresh Lists</button>
                <button onclick="showAddDomainForm('Big4_ISP')" style="background: #28a745;">+ Add Big4 Domain</button>
                <button onclick="showAddDomainForm('Cable_Provider')" style="background: #17a2b8;">+ Add Cable Domain</button>
            </div>
            
            <!-- Add Domain Form (hidden by default) -->
            <div id="add-domain-form" style="display: none; background: #0f3460; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #00d4ff; margin-bottom: 10px;">Add New Domain</h3>
                <div style="display: flex; gap: 10px; flex-wrap: wrap; align-items: flex-end;">
                    <div class="filter-group">
                        <label>Domain</label>
                        <input type="text" id="new-domain" placeholder="example.com" style="width: 150px;">
                    </div>
                    <div class="filter-group">
                        <label>Provider</label>
                        <input type="text" id="new-provider" placeholder="Yahoo" style="width: 120px;">
                    </div>
                    <div class="filter-group">
                        <label>Brand</label>
                        <input type="text" id="new-brand" placeholder="AT&T" style="width: 120px;">
                    </div>
                    <div class="filter-group">
                        <label>Category</label>
                        <select id="new-category">
                            <option value="Big4_ISP">Big4_ISP</option>
                            <option value="Cable_Provider">Cable_Provider</option>
                        </select>
                    </div>
                    <button onclick="addDomain()" style="background: #28a745;">Add</button>
                    <button onclick="hideAddDomainForm()" style="background: #6c757d;">Cancel</button>
                </div>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <!-- Big4 ISP List -->
                <div>
                    <h3 style="color: #00d4ff; margin-bottom: 10px;">Big4 ISP Domains (<span id="big4-count">0</span>)</h3>
                    <div id="big4-list" style="max-height: 500px; overflow-y: auto; background: #0a0a1a; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 12px;">
                        Loading...
                    </div>
                </div>
                
                <!-- Cable Provider List -->
                <div>
                    <h3 style="color: #17a2b8; margin-bottom: 10px;">Cable Provider Domains (<span id="cable-count">0</span>)</h3>
                    <div id="cable-list" style="max-height: 500px; overflow-y: auto; background: #0a0a1a; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 12px;">
                        Loading...
                    </div>
                </div>
            </div>
        </div>
        </div><!-- END CONFIG TAB -->
        
        <!-- CLOUDFLARE TAB -->
        <div id="tab-cloudflare" class="tab-content">
        <div class="section">
            <h2>Cloudflare Security Manager</h2>
            <p style="color: #888; margin-bottom: 15px;">Manage security settings per domain. Toggle features on/off with one click.</p>
            
            <button onclick="loadCloudflareZones()" style="margin-bottom: 20px;">Refresh Zones</button>
            
            <div id="cf-zones-container">
                <p style="color: #666;">Click "Refresh Zones" to load your Cloudflare domains...</p>
            </div>
        </div>
        </div><!-- END CLOUDFLARE TAB -->
        
        <!-- DOMAIN REPUTATION TAB -->
        <div id="tab-reputation" class="tab-content">
        <div class="section">
            <h2>Domain Reputation Checker</h2>
            <p style="color: #888; margin-bottom: 15px;">Check your domains against spam blacklists: Spamhaus, SpamCop, SURBL, URIBL, Barracuda, and more.</p>
            
            <div style="display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap;">
                <input type="text" id="rep-domain-input" placeholder="Enter domain (e.g., example.com)" style="flex: 1; min-width: 250px; padding: 10px; font-size: 14px;">
                <button onclick="addReputationDomain()" style="white-space: nowrap;">Add Domain</button>
                <button onclick="refreshAllReputation()" style="background: #28a745; white-space: nowrap;">Refresh All</button>
            </div>
            
            <div style="margin-bottom: 15px;">
                <label style="color: #888; font-size: 12px;">Quick Add from Your Cloudflare Zones:</label>
                <button onclick="importCloudflareDomainsToReputation()" style="margin-left: 10px; background: #f6821f; font-size: 12px; padding: 5px 10px;">Import CF Domains</button>
            </div>
            
            <div id="rep-results-container">
                <table class="data-table" style="width: 100%;">
                    <thead>
                        <tr>
                            <th style="text-align: left;">Domain</th>
                            <th style="text-align: center;">Status</th>
                            <th style="text-align: center;">Listed</th>
                            <th style="text-align: left;">Details</th>
                            <th style="text-align: center;">Quick Links</th>
                            <th style="text-align: center;">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="rep-domains-tbody">
                        <tr><td colspan="6" style="color: #666; text-align: center; padding: 20px;">Add domains above to check their reputation...</td></tr>
                    </tbody>
                </table>
            </div>
            
            <div style="margin-top: 20px; padding: 15px; background: #0a0a1a; border-radius: 8px;">
                <h4 style="color: #00d4ff; margin: 0 0 10px 0;">Blacklists Checked (14 total)</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px; font-size: 12px;">
                    <div><strong>Spamhaus DBL</strong> - Domain Block List</div>
                    <div><strong>Spamhaus ZEN</strong> - Combined blocklist</div>
                    <div><strong>SpamCop</strong> - User-reported spam</div>
                    <div><strong>SURBL Multi</strong> - Spam URI Realtime BL</div>
                    <div><strong>URIBL Black</strong> - High-confidence spam</div>
                    <div><strong>URIBL Grey</strong> - Suspicious domains</div>
                    <div><strong>URIBL Red</strong> - Highest confidence spam</div>
                    <div><strong>Barracuda</strong> - Enterprise blocklist</div>
                    <div><strong>SpamRats DYNA</strong> - Dynamic IP ranges</div>
                    <div><strong>SpamRats NOPTR</strong> - No reverse DNS</div>
                    <div><strong>SORBS</strong> - Spam & Open Relay BL</div>
                    <div><strong>Invaluement</strong> - Spammer domains</div>
                    <div><strong>PSBL</strong> - Passive Spam Block List</div>
                    <div><strong>CBL</strong> - Composite Blocking List</div>
                </div>
            </div>
            
            <div style="margin-top: 15px; padding: 15px; background: #0a0a1a; border-radius: 8px;">
                <h4 style="color: #f6821f; margin: 0 0 10px 0;">Quick Links (Manual Lookup)</h4>
                <div style="font-size: 12px; color: #aaa;">
                    <div style="margin-bottom: 5px;"><strong style="color: #f6821f;">Talos</strong> - Cisco Talos Intelligence (email/web reputation)</div>
                    <div style="margin-bottom: 5px;"><strong style="color: #00d4ff;">MX</strong> - MXToolbox SuperTool (100+ blacklists)</div>
                    <div style="margin-bottom: 5px;"><strong style="color: #28a745;">VT</strong> - VirusTotal (malware/security analysis)</div>
                </div>
            </div>
            
            <div style="margin-top: 15px; padding: 15px; background: #1a1a2e; border-radius: 8px; border-left: 3px solid #ffc107;">
                <h4 style="color: #ffc107; margin: 0 0 10px 0;">How to Delist</h4>
                <div style="font-size: 12px; color: #aaa;">
                    <div style="margin-bottom: 5px;"><strong>Spamhaus:</strong> <a href="https://check.spamhaus.org/" target="_blank" style="color: #00d4ff;">check.spamhaus.org</a></div>
                    <div style="margin-bottom: 5px;"><strong>SpamCop:</strong> <a href="https://www.spamcop.net/bl.shtml" target="_blank" style="color: #00d4ff;">spamcop.net/bl.shtml</a></div>
                    <div style="margin-bottom: 5px;"><strong>URIBL:</strong> <a href="https://admin.uribl.com/" target="_blank" style="color: #00d4ff;">admin.uribl.com</a></div>
                    <div style="margin-bottom: 5px;"><strong>Barracuda:</strong> <a href="https://www.barracudacentral.org/lookups" target="_blank" style="color: #00d4ff;">barracudacentral.org/lookups</a></div>
                </div>
            </div>
        </div>
        </div><!-- END REPUTATION TAB -->
        
        <!-- Dead Domains Modal -->
        <div id="dead-modal" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 1000;">
            <div style="background: #16213e; margin: 50px auto; padding: 20px; border-radius: 10px; max-width: 800px; max-height: 80vh; overflow-y: auto;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h2 style="color: #00d4ff; margin: 0;">Dead Domains (spot check these)</h2>
                    <button onclick="closeDeadModal()" style="background: #dc3545;">X Close</button>
                </div>
                <p style="color: #888; margin-bottom: 15px;">Click a domain to copy it, then verify with: <code style="background: #0a0a1a; padding: 3px 8px; border-radius: 3px;">nslookup -type=mx DOMAIN</code></p>
                <div id="dead-domains-list" style="font-family: monospace; font-size: 13px;">Loading...</div>
            </div>
        </div>
        
    </div>
    
    <script>
        // Global variables for domain reputation
        var repDomains = {};
        
        function formatNum(n) { 
            if (n === null || n === undefined) return '0';
            return n.toLocaleString(); 
        }
        
        function refreshStats() {
            fetch('/api/stats')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) return;
                    document.getElementById('stat-total').textContent = formatNum(data.total);
                    document.getElementById('stat-big4').textContent = formatNum(data.big4);
                    document.getElementById('stat-cable').textContent = formatNum(data.cable);
                    document.getElementById('stat-gi').textContent = formatNum(data.gi || 0);
                    document.getElementById('stat-clickers').textContent = formatNum(data.clickers);
                    document.getElementById('stat-high-quality').textContent = formatNum(data.high_quality);
                    var total = data.total || 1;
                    document.getElementById('stat-big4-pct').textContent = (data.big4 / total * 100).toFixed(1) + '%';
                    document.getElementById('stat-cable-pct').textContent = (data.cable / total * 100).toFixed(1) + '%';
                    document.getElementById('stat-gi-pct').textContent = ((data.gi || 0) / total * 100).toFixed(1) + '%';
                    document.getElementById('stats-updated').textContent = new Date().toLocaleTimeString();
                    document.getElementById('providers-content').innerHTML = '<p class="sub">Loading...</p>';
                    document.getElementById('quality-content').innerHTML = '<p class="sub">Loading...</p>';
                })
                .catch(function() {});
        }
        var statsRefreshInterval = null;
        function ensureStatsLoaded() {
            refreshStats();
        }
        
        var currentPage = 1;
        var totalResults = 0;
        var perPage = 500;
        
        // Toggle collapsible filter sections
        function toggleFilterSection(sectionId) {
            var section = document.getElementById(sectionId);
            var toggle = document.getElementById(sectionId + '-toggle');
            if (section.style.display === 'none') {
                section.style.display = 'flex';
                toggle.textContent = '-';
            } else {
                section.style.display = 'none';
                toggle.textContent = '+';
            }
        }
        
        // Clear all filter inputs
        function clearAllFilters() {
            // Text inputs
            var textInputs = ['email_search', 'domain', 'first_name', 'last_name', 'signup_domain', 'signup_ip', 'signup_date_from', 'signup_date_to'];
            textInputs.forEach(function(id) {
                var el = document.getElementById(id);
                if (el) el.value = '';
            });
            // Selects - reset to first option
            var selects = ['provider', 'category', 'state', 'city', 'zipcode', 'gender', 'country', 'has_phone', 'has_dob', 'clickers', 'openers', 'quality_tier', 'validation_status', 'data_source', 'file_source'];
            selects.forEach(function(id) {
                var el = document.getElementById(id);
                if (el) el.selectedIndex = 0;
            });
            document.getElementById('result-count').textContent = '';
        }
        
        // Load filter dropdown options from API
        function loadFilterOptions() {
            // Load states
            fetch('/api/filters/states')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('state');
                    if (select && data.states) {
                        data.states.forEach(function(s) {
                            var opt = document.createElement('option');
                            opt.value = s.value;
                            opt.textContent = s.value + ' (' + formatNum(s.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load countries
            fetch('/api/filters/countries')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('country');
                    if (select && data.countries) {
                        data.countries.forEach(function(c) {
                            var opt = document.createElement('option');
                            opt.value = c.value;
                            opt.textContent = c.value + ' (' + formatNum(c.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load data sources
            fetch('/api/filters/data-sources')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('data_source');
                    if (select && data.sources) {
                        data.sources.forEach(function(s) {
                            var opt = document.createElement('option');
                            opt.value = s.value;
                            opt.textContent = s.value + ' (' + formatNum(s.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load validation statuses
            fetch('/api/filters/validation-statuses')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('validation_status');
                    if (select && data.statuses) {
                        data.statuses.forEach(function(s) {
                            var opt = document.createElement('option');
                            opt.value = s.value;
                            opt.textContent = s.value + ' (' + formatNum(s.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load genders with counts
            fetch('/api/filters/genders')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('gender');
                    if (select && data.genders) {
                        data.genders.forEach(function(g) {
                            var opt = document.createElement('option');
                            opt.value = g.value;
                            var label = g.value === 'M' ? 'Male' : (g.value === 'F' ? 'Female' : g.value);
                            opt.textContent = label + ' (' + formatNum(g.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load cities with counts (top 200)
            fetch('/api/filters/cities')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('city');
                    if (select && data.cities) {
                        data.cities.forEach(function(c) {
                            var opt = document.createElement('option');
                            opt.value = c.value;
                            opt.textContent = c.value + ' (' + formatNum(c.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
            
            // Load zipcodes with counts (top 200)
            fetch('/api/filters/zipcodes')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('zipcode');
                    if (select && data.zipcodes) {
                        data.zipcodes.forEach(function(z) {
                            var opt = document.createElement('option');
                            opt.value = z.value;
                            opt.textContent = z.value + ' (' + formatNum(z.count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });

            // Load file sources
            fetch('/api/file-sources')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var select = document.getElementById('file_source');
                    if (select && data.sources) {
                        data.sources.forEach(function(s) {
                            var opt = document.createElement('option');
                            opt.value = s.filename;
                            opt.textContent = s.filename + ' (' + formatNum(s.email_count) + ')';
                            select.appendChild(opt);
                        });
                    }
                });
        }
        
        // =====================================================
        // LIST BUILDER
        // =====================================================
        var listBuilderSegments = [];
        var presetCounts = {};  // Cache for preset counts
        
        var presetDefinitions = {
            // All Categories
            'all_clickers': {name: 'ALL Clickers', filters: {clickers: 'true'}},
            'all_openers': {name: 'ALL Openers', filters: {openers: 'true'}},
            // By Category
            'gi_clickers': {name: 'GI Clickers', filters: {category: 'General_Internet', clickers: 'true'}},
            'gi_openers': {name: 'GI Openers', filters: {category: 'General_Internet', openers: 'true'}},
            'cable_clickers': {name: 'Cable Provider Clickers', filters: {category: 'Cable_Provider', clickers: 'true'}},
            'cable_openers': {name: 'Cable Provider Openers', filters: {category: 'Cable_Provider', openers: 'true'}},
            'big4_clickers': {name: 'Big4 ISP Clickers', filters: {category: 'Big4_ISP', clickers: 'true'}},
            'big4_openers': {name: 'Big4 ISP Openers', filters: {category: 'Big4_ISP', openers: 'true'}},
            // Big4 ISP Providers
            'google_clickers': {name: 'Google/Gmail Clickers', filters: {provider: 'Google', clickers: 'true'}},
            'google_openers': {name: 'Google/Gmail Openers', filters: {provider: 'Google', openers: 'true'}},
            'yahoo_clickers': {name: 'Yahoo Clickers', filters: {provider: 'Yahoo', clickers: 'true'}},
            'yahoo_openers': {name: 'Yahoo Openers', filters: {provider: 'Yahoo', openers: 'true'}},
            'microsoft_clickers': {name: 'Microsoft/Outlook Clickers', filters: {provider: 'Microsoft', clickers: 'true'}},
            'microsoft_openers': {name: 'Microsoft/Outlook Openers', filters: {provider: 'Microsoft', openers: 'true'}},
            'aol_clickers': {name: 'AOL Clickers', filters: {provider: 'AOL', clickers: 'true'}},
            'aol_openers': {name: 'AOL Openers', filters: {provider: 'AOL', openers: 'true'}},
            // 2nd Level Big4 (GI emails hosted on Big4 MX servers)
            '2nd_google_clickers': {name: 'GI on Google MX - Clickers', filters: {category: 'General_Internet', mx_category: 'Google', clickers: 'true'}},
            '2nd_google_openers': {name: 'GI on Google MX - Openers', filters: {category: 'General_Internet', mx_category: 'Google', openers: 'true'}},
            '2nd_microsoft_clickers': {name: 'GI on Microsoft MX - Clickers', filters: {category: 'General_Internet', mx_category: 'Microsoft', clickers: 'true'}},
            '2nd_microsoft_openers': {name: 'GI on Microsoft MX - Openers', filters: {category: 'General_Internet', mx_category: 'Microsoft', openers: 'true'}},
            '2nd_yahoo_clickers': {name: 'GI on Yahoo MX - Clickers', filters: {category: 'General_Internet', mx_category: 'Yahoo', clickers: 'true'}},
            '2nd_yahoo_openers': {name: 'GI on Yahoo MX - Openers', filters: {category: 'General_Internet', mx_category: 'Yahoo', openers: 'true'}},
            '2nd_big4_all_clickers': {name: 'All 2nd Level Big4 Clickers', filters: {category: 'General_Internet', mx_category_big4: 'true', clickers: 'true'}},
            '2nd_big4_all_openers': {name: 'All 2nd Level Big4 Openers', filters: {category: 'General_Internet', mx_category_big4: 'true', openers: 'true'}},
            // Cable/Other Providers
            'apple_clickers': {name: 'Apple/iCloud Clickers', filters: {provider: 'Apple', clickers: 'true'}},
            'apple_openers': {name: 'Apple/iCloud Openers', filters: {provider: 'Apple', openers: 'true'}},
            'spectrum_clickers': {name: 'Spectrum Clickers', filters: {provider: 'Spectrum', clickers: 'true'}},
            'spectrum_openers': {name: 'Spectrum Openers', filters: {provider: 'Spectrum', openers: 'true'}},
            'comcast_clickers': {name: 'Comcast/Xfinity Clickers', filters: {provider: 'Comcast', clickers: 'true'}},
            'comcast_openers': {name: 'Comcast/Xfinity Openers', filters: {provider: 'Comcast', openers: 'true'}},
            'att_clickers': {name: 'AT&T Clickers', filters: {provider: 'AT&T', clickers: 'true'}},
            'att_openers': {name: 'AT&T Openers', filters: {provider: 'AT&T', openers: 'true'}},
            'godaddy_clickers': {name: 'GoDaddy Clickers', filters: {provider: 'GoDaddy', clickers: 'true'}},
            'earthlink_clickers': {name: 'EarthLink Clickers', filters: {provider: 'EarthLink', clickers: 'true'}},
            // Quality
            'high_quality': {name: 'High Quality (70+)', filters: {quality: 'high'}},
            'med_quality': {name: 'Medium Quality (40-69)', filters: {quality: 'medium'}},
            'verified_all': {name: 'All Verified', filters: {validation_status: 'verified'}}
        };
        
        function loadPresetCounts() {
            // Load counts for all presets to show in dropdown
            var presetKeys = Object.keys(presetDefinitions);
            var segments = presetKeys.map(function(key) {
                return {key: key, filters: presetDefinitions[key].filters};
            });
            
            fetch('/api/list-builder/preset-counts', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({presets: segments})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.counts) {
                    presetCounts = data.counts;
                    updatePresetDropdown();
                }
            })
            .catch(function(e) { console.log('Failed to load preset counts:', e); });
        }
        
        function updatePresetDropdown() {
            var select = document.getElementById('list-builder-preset');
            var options = select.querySelectorAll('option[value]');
            options.forEach(function(opt) {
                var key = opt.value;
                if (key && presetCounts[key] !== undefined) {
                    var preset = presetDefinitions[key];
                    if (preset) {
                        var count = presetCounts[key];
                        var countStr = count >= 1000 ? (count/1000).toFixed(1) + 'k' : count;
                        opt.textContent = preset.name + ' (' + countStr + ')';
                    }
                }
            });
        }
        
        function addToListBuilder() {
            var select = document.getElementById('list-builder-preset');
            var presetKey = select.value;
            var preset = presetDefinitions[presetKey];
            
            if (!preset) {
                alert('Please select a preset');
                return;
            }
            
            // Check if already added
            if (listBuilderSegments.some(function(s) { return s.key === presetKey; })) {
                alert(preset.name + ' is already in the list');
                return;
            }
            
            listBuilderSegments.push({
                key: presetKey,
                name: preset.name,
                filters: preset.filters,
                count: presetCounts[presetKey] || 0
            });
            
            renderListBuilderSegments();
            updateRunningTotal();
        }
        
        function addCurrentFilters() {
            var filters = {};
            var name = [];
            
            var provider = document.getElementById('provider').value;
            var category = document.getElementById('category').value;
            var clickers = document.getElementById('clickers').value;
            var openers = document.getElementById('openers').value;
            var quality = document.getElementById('quality').value;
            var domain = document.getElementById('domain').value;
            var state = document.getElementById('state').value;
            
            if (provider) { filters.provider = provider; name.push(provider); }
            if (category) { filters.category = category; name.push(category.replace('_', ' ')); }
            if (clickers === 'true') { filters.clickers = 'true'; name.push('Clickers'); }
            if (openers === 'true') { filters.openers = 'true'; name.push('Openers'); }
            if (quality) { filters.quality = quality; name.push(quality + ' quality'); }
            if (domain) { filters.domain = domain; name.push(domain); }
            if (state) { filters.state = state; name.push(state); }
            
            if (Object.keys(filters).length === 0) {
                alert('Please set some filters first');
                return;
            }
            
            var segmentName = name.join(' + ') || 'Custom Filter';
            var segmentKey = 'custom_' + Date.now();
            
            listBuilderSegments.push({
                key: segmentKey,
                name: segmentName,
                filters: filters,
                count: 0  // Will be calculated in running total
            });
            
            renderListBuilderSegments();
            updateRunningTotal();
        }
        
        function removeFromListBuilder(index) {
            listBuilderSegments.splice(index, 1);
            renderListBuilderSegments();
            updateRunningTotal();
        }
        
        function renderListBuilderSegments() {
            var container = document.getElementById('list-builder-segments');
            
            if (listBuilderSegments.length === 0) {
                container.innerHTML = '<span style="color: #666; font-size: 12px;">No segments added yet. Select presets above or use "Add Current Filters".</span>';
                document.getElementById('list-builder-count').textContent = '';
                return;
            }
            
            var html = '';
            listBuilderSegments.forEach(function(segment, index) {
                var countStr = '';
                if (segment.count) {
                    countStr = segment.count >= 1000 ? ' (' + (segment.count/1000).toFixed(1) + 'k)' : ' (' + segment.count + ')';
                }
                html += '<span style="display: inline-block; background: #17a2b8; color: #fff; padding: 5px 10px; border-radius: 15px; margin: 3px; font-size: 12px;">';
                html += segment.name + countStr;
                html += ' <span onclick="removeFromListBuilder(' + index + ')" style="cursor: pointer; margin-left: 5px; color: #ff6b6b;">&times;</span>';
                html += '</span>';
            });
            
            container.innerHTML = html;
        }
        
        function updateRunningTotal() {
            if (listBuilderSegments.length === 0) {
                document.getElementById('list-builder-count').textContent = '';
                return;
            }
            
            document.getElementById('list-builder-count').textContent = 'Calculating...';
            
            fetch('/api/list-builder/count', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({segments: listBuilderSegments})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('list-builder-count').textContent = 'Error';
                } else {
                    var totalStr = data.total >= 1000 ? (data.total/1000).toFixed(1) + 'k' : data.total;
                    document.getElementById('list-builder-count').innerHTML = '<strong style="color: #00d4ff; font-size: 14px;">Running Total: ' + data.total.toLocaleString() + ' unique emails</strong>';
                }
            })
            .catch(function(e) {
                document.getElementById('list-builder-count').textContent = 'Error';
            });
        }
        
        function clearListBuilder() {
            listBuilderSegments = [];
            renderListBuilderSegments();
            document.getElementById('list-builder-count').textContent = '';
        }
        
        function previewListBuilder() {
            if (listBuilderSegments.length === 0) {
                alert('Add some segments first');
                return;
            }
            
            document.getElementById('list-builder-count').textContent = 'Counting...';
            
            fetch('/api/list-builder/count', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({segments: listBuilderSegments})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('list-builder-count').textContent = 'Error: ' + data.error;
                } else {
                    document.getElementById('list-builder-count').textContent = 'Total: ' + data.total.toLocaleString() + ' unique emails';
                }
            })
            .catch(function(e) {
                document.getElementById('list-builder-count').textContent = 'Error: ' + e;
            });
        }
        
        function exportListBuilder() {
            if (listBuilderSegments.length === 0) {
                alert('Add some segments first');
                return;
            }
            
            var emailOnly = document.getElementById('export-email-only').checked;
            document.getElementById('list-builder-count').textContent = 'Preparing export...';
            
            fetch('/api/list-builder/export', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({segments: listBuilderSegments, email_only: emailOnly})
            })
            .then(function(response) {
                if (!response.ok) throw new Error('Export failed');
                return response.blob();
            })
            .then(function(blob) {
                var url = window.URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.href = url;
                var suffix = emailOnly ? '_emails_only' : '';
                a.download = 'combined_list' + suffix + '_' + new Date().toISOString().slice(0,10) + '.csv';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
                document.getElementById('list-builder-count').textContent = 'Export complete!';
            })
            .catch(function(e) {
                document.getElementById('list-builder-count').textContent = 'Export error: ' + e;
            });
        }
        
        function runQuery(page) {
            page = page || 1;
            currentPage = page;
            perPage = parseInt(document.getElementById('limit').value);
            var offset = (page - 1) * perPage;
            
            // Build params with all filter fields
            var params = new URLSearchParams();
            
            // Helper to add non-empty params
            function addParam(name, elementId) {
                var el = document.getElementById(elementId);
                if (el && el.value && el.value.trim() !== '') {
                    params.append(name, el.value.trim());
                }
            }
            
            // Basic filters
            addParam('email_search', 'email_search');
            addParam('provider', 'provider');
            addParam('category', 'category');
            addParam('domain', 'domain');
            addParam('state', 'state');
            
            // Personal info
            addParam('first_name', 'first_name');
            addParam('last_name', 'last_name');
            addParam('city', 'city');
            addParam('zipcode', 'zipcode');
            addParam('gender', 'gender');
            addParam('country', 'country');
            addParam('has_phone', 'has_phone');
            addParam('has_dob', 'has_dob');
            
            // Engagement
            addParam('clickers', 'clickers');
            addParam('openers', 'openers');
            addParam('quality_tier', 'quality_tier');
            addParam('validation_status', 'validation_status');
            
            // Source tracking
            addParam('data_source', 'data_source');
            addParam('file_source', 'file_source');
            addParam('signup_domain', 'signup_domain');
            addParam('signup_ip', 'signup_ip');
            addParam('signup_date_from', 'signup_date_from');
            addParam('signup_date_to', 'signup_date_to');
            
            // Pagination
            params.append('limit', perPage);
            params.append('offset', offset);
            
            document.getElementById('results').innerHTML = '<p class="loading">Loading...</p>';
            document.getElementById('result-count').textContent = 'Searching...';
            
            fetch('/api/query?' + params)
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) {
                        document.getElementById('results').innerHTML = '<p class="error">' + data.error + '</p>';
                        document.getElementById('result-count').textContent = 'Error';
                        return;
                    }
                    totalResults = data.total_count || data.count;
                    var totalPages = Math.ceil(totalResults / perPage);
                    
                    // Update result count
                    document.getElementById('result-count').textContent = 'Found ' + formatNum(totalResults) + ' matching records';
                    
                    // Show/hide pagination
                    var paginationDiv = document.getElementById('pagination');
                    if (totalResults > perPage) {
                        paginationDiv.style.display = 'block';
                        document.getElementById('pageInfo').textContent = 'Page ' + currentPage + ' of ' + totalPages;
                        document.getElementById('prevBtn').disabled = (currentPage <= 1);
                        document.getElementById('nextBtn').disabled = (currentPage >= totalPages);
                    } else {
                        paginationDiv.style.display = 'none';
                    }
                    
                    var html = '<p>Showing ' + data.count + ' of ' + formatNum(totalResults) + ' results</p><table><tr>';
                    var i, j;
                    for (i = 0; i < data.columns.length; i++) {
                        html += '<th>' + data.columns[i] + '</th>';
                    }
                    html += '</tr>';
                    for (i = 0; i < data.rows.length; i++) {
                        html += '<tr>';
                        for (j = 0; j < data.rows[i].length; j++) {
                            var cell = data.rows[i][j];
                            html += '<td>' + (cell !== null ? cell : '') + '</td>';
                        }
                        html += '</tr>';
                    }
                    html += '</table>';
                    document.getElementById('results').innerHTML = html;
                })
                .catch(function(e) {
                    document.getElementById('results').innerHTML = '<p class="error">Error: ' + e + '</p>';
                    document.getElementById('result-count').textContent = 'Error';
                });
        }
        
        function prevPage() {
            if (currentPage > 1) runQuery(currentPage - 1);
        }
        
        function nextPage() {
            runQuery(currentPage + 1);
        }
        
        function runSQL() {
            var sql = document.getElementById('sql').value;
            document.getElementById('sql-results').innerHTML = '<p class="loading">Executing...</p>';
            
            fetch('/api/sql', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({sql: sql})
            })
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) {
                        document.getElementById('sql-results').innerHTML = '<p class="error">' + data.error + '</p>';
                        return;
                    }
                    var html = '<p>' + data.count + ' rows</p><table><tr>';
                    var i, j;
                    for (i = 0; i < data.columns.length; i++) {
                        html += '<th>' + data.columns[i] + '</th>';
                    }
                    html += '</tr>';
                    for (i = 0; i < data.rows.length; i++) {
                        html += '<tr>';
                        for (j = 0; j < data.rows[i].length; j++) {
                            var cell = data.rows[i][j];
                            html += '<td>' + (cell !== null ? cell : '') + '</td>';
                        }
                        html += '</tr>';
                    }
                    html += '</table>';
                    document.getElementById('sql-results').innerHTML = html;
                })
                .catch(function(e) {
                    document.getElementById('sql-results').innerHTML = '<p class="error">Error</p>';
                });
        }
        
        function exportCSV() {
            // Build params with all filter fields (same as runQuery)
            var params = new URLSearchParams();
            
            function addParam(name, elementId) {
                var el = document.getElementById(elementId);
                if (el && el.value && el.value.trim() !== '') {
                    params.append(name, el.value.trim());
                }
            }
            
            // All filters
            addParam('email_search', 'email_search');
            addParam('provider', 'provider');
            addParam('category', 'category');
            addParam('domain', 'domain');
            addParam('state', 'state');
            addParam('first_name', 'first_name');
            addParam('last_name', 'last_name');
            addParam('city', 'city');
            addParam('zipcode', 'zipcode');
            addParam('gender', 'gender');
            addParam('country', 'country');
            addParam('has_phone', 'has_phone');
            addParam('has_dob', 'has_dob');
            addParam('clickers', 'clickers');
            addParam('openers', 'openers');
            addParam('quality_tier', 'quality_tier');
            addParam('validation_status', 'validation_status');
            addParam('data_source', 'data_source');
            addParam('file_source', 'file_source');
            addParam('signup_domain', 'signup_domain');
            addParam('signup_ip', 'signup_ip');
            addParam('signup_date_from', 'signup_date_from');
            addParam('signup_date_to', 'signup_date_to');
            
            params.append('limit', '50000');
            window.location.href = '/api/export?' + params;
        }
        
        // Load detailed stats for Stats tab
        function loadDetailedStats() {
            document.getElementById('stats-loading').style.display = 'inline';
            fetch('/api/stats/detailed')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                document.getElementById('stats-loading').style.display = 'none';
                if (data.error) {
                    console.error('Stats error:', data.error, data.trace);
                    alert('Error loading stats: ' + data.error);
                    return;
                }
                
                // Update timestamp from cache
                if (data.cache_updated) {
                    var cacheDate = new Date(data.cache_updated);
                    document.getElementById('stats-updated').textContent = cacheDate.toLocaleString();
                } else {
                    document.getElementById('stats-updated').textContent = 'Never';
                }
                
                // Summary totals - use helper to avoid null errors
                function setStatText(id, val) {
                    var el = document.getElementById(id);
                    if (el) el.textContent = val;
                }
                
                setStatText('stat-total', formatNum(data.total));
                setStatText('stat-good-total', formatNum(data.good_total));
                setStatText('stat-dead-total', formatNum(data.dead_total));
                
                // Clickers & Openers totals
                setStatText('stat-clickers', formatNum(data.clickers));
                setStatText('stat-openers', formatNum(data.openers));
                
                // Big4 row
                setStatText('stat-big4-total', formatNum(data.big4_total));
                setStatText('stat-big4-good', formatNum(data.big4_good));
                setStatText('stat-big4-dead', formatNum(data.big4_dead));
                setStatText('stat-clickers-big4', formatNum(data.clickers_big4));
                setStatText('stat-openers-big4', formatNum(data.openers_big4));
                
                // Cable row
                setStatText('stat-cable-total', formatNum(data.cable_total));
                setStatText('stat-cable-good', formatNum(data.cable_good));
                setStatText('stat-cable-dead', formatNum(data.cable_dead));
                setStatText('stat-clickers-cable', formatNum(data.clickers_cable));
                setStatText('stat-openers-cable', formatNum(data.openers_cable));
                
                // GI row
                setStatText('stat-gi-total', formatNum(data.gi_total));
                setStatText('stat-gi-good', formatNum(data.gi_good));
                setStatText('stat-gi-dead', formatNum(data.gi_dead));
                setStatText('stat-clickers-gi', formatNum(data.clickers_gi));
                setStatText('stat-openers-gi', formatNum(data.openers_gi));
                
                // Big4 breakdown - full stats for each (Total, Good, Dead, High, Med, Low, Click, Open, Doms)
                var big4Providers = ['gmail', 'yahoo', 'outlook'];
                big4Providers.forEach(function(p) {
                    setStatText('stat-' + p, formatNum(data[p]));
                    setStatText('stat-' + p + '-good', formatNum(data[p + '_good']));
                    setStatText('stat-' + p + '-dead', formatNum(data[p + '_dead']));
                    setStatText('stat-' + p + '-high', formatNum(data[p + '_high']));
                    setStatText('stat-' + p + '-med', formatNum(data[p + '_med']));
                    setStatText('stat-' + p + '-low', formatNum(data[p + '_low']));
                    setStatText('stat-' + p + '-click', formatNum(data[p + '_click']));
                    setStatText('stat-' + p + '-open', formatNum(data[p + '_open']));
                    setStatText('stat-' + p + '-domains', formatNum(data[p + '_domains']));
                });
                
                // Cable Provider breakdown
                var cableProviders = ['comcast', 'spectrum', 'centurylink', 'earthlink', 'windstream', 'optimum'];
                cableProviders.forEach(function(p) {
                    setStatText('stat-' + p, formatNum(data[p]));
                    setStatText('stat-' + p + '-good', formatNum(data[p + '_good']));
                    setStatText('stat-' + p + '-dead', formatNum(data[p + '_dead']));
                    setStatText('stat-' + p + '-high', formatNum(data[p + '_high']));
                    setStatText('stat-' + p + '-med', formatNum(data[p + '_med']));
                    setStatText('stat-' + p + '-low', formatNum(data[p + '_low']));
                    setStatText('stat-' + p + '-click', formatNum(data[p + '_click']));
                    setStatText('stat-' + p + '-open', formatNum(data[p + '_open']));
                    setStatText('stat-' + p + '-domains', formatNum(data[p + '_domains']));
                });
                
                // 2nd Level Big4 - main table row
                setStatText('stat-2nd-big4-total', formatNum(data['2nd_big4_total']));
                setStatText('stat-2nd-big4-good', formatNum(data['2nd_big4_good']));
                setStatText('stat-2nd-big4-dead', formatNum(data['2nd_big4_dead']));
                setStatText('stat-2nd-big4-clickers', formatNum(data['2nd_big4_clickers']));
                setStatText('stat-2nd-big4-openers', formatNum(data['2nd_big4_openers']));
                
                // 2nd Level Big4 - breakdown with all columns
                var big4Hosted = ['google-hosted', 'microsoft-hosted', 'yahoo-hosted'];
                big4Hosted.forEach(function(p) {
                    var key = p.replace('-', '_');
                    setStatText('stat-' + p, formatNum(data[key]));
                    setStatText('stat-' + p + '-good', formatNum(data[key + '_good']));
                    setStatText('stat-' + p + '-dead', formatNum(data[key + '_dead']));
                    setStatText('stat-' + p + '-high', formatNum(data[key + '_high']));
                    setStatText('stat-' + p + '-med', formatNum(data[key + '_med']));
                    setStatText('stat-' + p + '-low', formatNum(data[key + '_low']));
                    setStatText('stat-' + p + '-click', formatNum(data[key + '_click']));
                    setStatText('stat-' + p + '-open', formatNum(data[key + '_open']));
                    setStatText('stat-' + p + '-domains', formatNum(data[key + '_domains']));
                });
                
                // GI Hosting providers - full stats for each
                var giProviders = ['apple', 'godaddy', '1and1', 'hostgator', 'namecheap', 'zoho', 'fastmail', 'amazonses', 'protonmail', 'cloudflare'];
                giProviders.forEach(function(p) {
                    setStatText('stat-' + p, formatNum(data[p]));
                    setStatText('stat-' + p + '-good', formatNum(data[p + '_good']));
                    setStatText('stat-' + p + '-dead', formatNum(data[p + '_dead']));
                    setStatText('stat-' + p + '-high', formatNum(data[p + '_high']));
                    setStatText('stat-' + p + '-med', formatNum(data[p + '_med']));
                    setStatText('stat-' + p + '-low', formatNum(data[p + '_low']));
                    setStatText('stat-' + p + '-click', formatNum(data[p + '_click']));
                    setStatText('stat-' + p + '-open', formatNum(data[p + '_open']));
                    setStatText('stat-' + p + '-domains', formatNum(data[p + '_domains']));
                });
                
                // GI domains count
                setStatText('stat-gi-domains', formatNum(data.gi_domains));
                
                // Top 10 GI Domains
                var tbody = document.getElementById('top-gi-domains-body');
                if (tbody && data.top_gi_domains && data.top_gi_domains.length > 0) {
                    var html = '';
                    data.top_gi_domains.forEach(function(d) {
                        html += '<tr>' +
                            '<td style="max-width: 120px; overflow: hidden; text-overflow: ellipsis;">' + d.domain + '</td>' +
                            '<td style="text-align: right;">' + formatNum(d.total) + '</td>' +
                            '<td style="text-align: right; color: #28a745;">' + formatNum(d.good) + '</td>' +
                            '<td style="text-align: right; color: #dc3545;">' + formatNum(d.dead) + '</td>' +
                            '<td style="text-align: right; color: #00d4ff;">' + formatNum(d.high) + '</td>' +
                            '<td style="text-align: right; color: #ffc107;">' + formatNum(d.med) + '</td>' +
                            '<td style="text-align: right; color: #6c757d;">' + formatNum(d.low) + '</td>' +
                            '<td style="text-align: right; color: #e83e8c;">' + formatNum(d.click) + '</td>' +
                            '<td style="text-align: right; color: #17a2b8;">' + formatNum(d.open) + '</td>' +
                            '</tr>';
                    });
                    tbody.innerHTML = html;
                }
            })
            .catch(function(e) {
                document.getElementById('stats-loading').style.display = 'none';
                console.error('Failed to load stats:', e);
            });
        }
        
        // Recalculate stats (slow - updates cache)
        function recalculateStats() {
            if (!confirm('This will recalculate all stats from the database.\\nThis takes about 10 seconds. Continue?')) return;
            document.getElementById('stats-loading').style.display = 'inline';
            document.getElementById('stats-loading').textContent = 'Recalculating...';
            fetch('/api/stats/refresh', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                document.getElementById('stats-loading').style.display = 'none';
                if (data.success) {
                    alert('Stats recalculated! ' + data.stats_updated + ' stats updated.');
                    loadDetailedStats();
                } else {
                    alert('Error: ' + (data.error || 'Unknown error'));
                }
            })
            .catch(function(e) {
                document.getElementById('stats-loading').style.display = 'none';
                alert('Error recalculating stats: ' + e);
            });
        }
        
        // Load stats when Stats tab is shown
        var originalShowTab = typeof showTab === 'function' ? showTab : null;
        
        function loadFileSources() {
            fetch('/api/file-sources')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var select = document.getElementById('file_source');
                if (!select) return;
                
                // Keep first option (All Files)
                select.innerHTML = '<option value="">All Files</option>';
                
                if (data.sources && data.sources.length > 0) {
                    for (var i = 0; i < data.sources.length; i++) {
                        var s = data.sources[i];
                        var opt = document.createElement('option');
                        opt.value = s.filename;
                        opt.textContent = s.filename + ' (' + formatNum(s.email_count) + ')';
                        select.appendChild(opt);
                    }
                }
            })
            .catch(function(e) {
                console.error('Error loading file sources:', e);
            });
        }
        
        // Load file sources on page load
        document.addEventListener('DOMContentLoaded', function() {
            loadFileSources();
            loadFilterOptions();
        });
        
        // =====================================================
        // TAB NAVIGATION
        // =====================================================
        function showTab(tabName) {
            try {
                // Hide all tab content
                var allTabs = document.getElementsByClassName('tab-content');
                for (var i = 0; i < allTabs.length; i++) {
                    allTabs[i].className = 'tab-content';
                }
                // Remove active from all buttons
                var allBtns = document.getElementsByClassName('tab-btn');
                for (var i = 0; i < allBtns.length; i++) {
                    allBtns[i].className = 'tab-btn';
                }
                // Show selected tab
                var content = document.getElementById('tab-' + tabName);
                if (content) content.className = 'tab-content active';
                // Highlight clicked button
                if (tabName === 'stats' && allBtns[0]) allBtns[0].className = 'tab-btn active';
                if (tabName === 'query' && allBtns[1]) allBtns[1].className = 'tab-btn active';
                if (tabName === 'mx' && allBtns[2]) allBtns[2].className = 'tab-btn active';
                if (tabName === 'config' && allBtns[3]) allBtns[3].className = 'tab-btn active';
                // Stats tab: do NOT auto-refresh - user must click "Load Cached Stats" or "Recalculate"
                // if (tabName === 'stats') { loadDetailedStats(); }
                // Load domain config when config tab is shown
                if (tabName === 'config') {
                    try { loadDomainConfig(); } catch(e) { console.log('Config error:', e); }
                }
                // Load reputation data when reputation tab is shown
                if (tabName === 'reputation') {
                    try { 
                        if (typeof repDomains !== 'undefined' && Object.keys(repDomains).length === 0) {
                            loadReputationDomains(); 
                        }
                    } catch(e) { console.log('Reputation error:', e); }
                }
                // Load preset counts when query tab is shown
                if (tabName === 'query') {
                    try {
                        if (typeof loadPresetCounts === 'function' && Object.keys(presetCounts).length === 0) {
                            loadPresetCounts();
                        }
                    } catch(e) { console.log('Query preset counts error:', e); }
                }
            } catch(e) {
                alert('Tab error: ' + e.message);
            }
        }
        
        // =====================================================
        // DOMAIN CONFIG
        // =====================================================
        function loadDomainConfig() {
            fetch('/api/config/domains')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('big4-list').innerHTML = '<span style="color: #ff6b6b;">Error: ' + data.error + '</span>';
                    return;
                }
                // Render Big4 list
                var big4Html = '';
                var big4 = data.big4 || [];
                document.getElementById('big4-count').textContent = big4.length;
                big4.forEach(function(d) {
                    big4Html += '<div style="padding: 4px 0; border-bottom: 1px solid #333; display: flex; justify-content: space-between;">';
                    big4Html += '<span style="color: #00d4ff;">' + d.domain + '</span>';
                    big4Html += '<span style="color: #888;">' + d.provider + ' / ' + d.brand + '</span>';
                    big4Html += '</div>';
                });
                document.getElementById('big4-list').innerHTML = big4Html || '<span style="color: #888;">No domains</span>';
                
                // Render Cable list
                var cableHtml = '';
                var cable = data.cable || [];
                document.getElementById('cable-count').textContent = cable.length;
                cable.forEach(function(d) {
                    cableHtml += '<div style="padding: 4px 0; border-bottom: 1px solid #333; display: flex; justify-content: space-between;">';
                    cableHtml += '<span style="color: #17a2b8;">' + d.domain + '</span>';
                    cableHtml += '<span style="color: #888;">' + d.provider + ' / ' + d.brand + '</span>';
                    cableHtml += '</div>';
                });
                document.getElementById('cable-list').innerHTML = cableHtml || '<span style="color: #888;">No domains</span>';
            })
            .catch(function(e) {
                document.getElementById('big4-list').innerHTML = '<span style="color: #ff6b6b;">Failed to load: ' + e + '</span>';
            });
        }
        
        function showAddDomainForm(category) {
            document.getElementById('add-domain-form').style.display = 'block';
            document.getElementById('new-category').value = category;
            document.getElementById('new-domain').value = '';
            document.getElementById('new-provider').value = '';
            document.getElementById('new-brand').value = '';
            document.getElementById('new-domain').focus();
        }
        
        function hideAddDomainForm() {
            document.getElementById('add-domain-form').style.display = 'none';
        }
        
        function addDomain() {
            var domain = document.getElementById('new-domain').value.trim().toLowerCase();
            var provider = document.getElementById('new-provider').value.trim();
            var brand = document.getElementById('new-brand').value.trim();
            var category = document.getElementById('new-category').value;
            
            if (!domain || !provider || !brand) {
                alert('Please fill in all fields');
                return;
            }
            
            fetch('/api/config/domains/add', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain, provider: provider, brand: brand, category: category})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    alert('Error: ' + data.error);
                    return;
                }
                alert('Added ' + domain + ' to ' + category);
                hideAddDomainForm();
                loadDomainConfig();
            })
            .catch(function(e) {
                alert('Failed: ' + e);
            });
        }
        
        // =====================================================
        // CLOUDFLARE MANAGER
        // =====================================================
        function loadCloudflareZones() {
            var container = document.getElementById('cf-zones-container');
            container.innerHTML = '<p style="color: #17a2b8;">Loading zones...</p>';
            
            fetch('/api/cloudflare/zones')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    container.innerHTML = '<p style="color: #ff6b6b;">Error: ' + data.error + '</p>';
                    return;
                }
                
                if (!data.zones || data.zones.length === 0) {
                    container.innerHTML = '<p style="color: #ffc107;">No zones found. Check your API token permissions.</p>';
                    return;
                }
                
                var html = '';
                data.zones.forEach(function(zone) {
                    html += '<div class="cf-zone-card" id="cf-zone-' + zone.id + '" style="background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 15px; margin-bottom: 15px;">';
                    html += '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">';
                    html += '<h3 style="color: #00d4ff; margin: 0;">' + zone.name + '</h3>';
                    html += '<div style="display: flex; align-items: center; gap: 10px;">';
                    html += '<span style="color: ' + (zone.status === 'active' ? '#28a745' : '#ffc107') + '; font-size: 12px;">' + zone.status.toUpperCase() + '</span>';
                    html += '<button class="cf-remove-btn" data-zone="' + zone.id + '" data-domain="' + zone.name + '" style="padding: 3px 8px; font-size: 10px; background: #dc3545;">Remove</button>';
                    html += '</div>';
                    html += '</div>';
                    
                    html += '<div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px;">';
                    
                    // Bot Fight toggle
                    html += '<div class="cf-toggle-row" style="background: #0d1b2a; padding: 12px; border-radius: 6px;">';
                    html += '<div style="display: flex; justify-content: space-between; align-items: center;">';
                    html += '<div><strong style="color: #fff;">Bot Fight</strong><br><span style="color: #888; font-size: 11px;">Browser check + High security</span></div>';
                    html += '<label class="switch"><input type="checkbox" id="cf-' + zone.id + '-botfight" data-zone="' + zone.id + '" data-feature="bot_fight"><span class="slider"></span></label>';
                    html += '</div></div>';
                    
                    // US Only toggle
                    html += '<div class="cf-toggle-row" style="background: #0d1b2a; padding: 12px; border-radius: 6px;">';
                    html += '<div style="display: flex; justify-content: space-between; align-items: center;">';
                    html += '<div><strong style="color: #fff;">US Only</strong><br><span style="color: #888; font-size: 11px;">Block non-US traffic</span></div>';
                    html += '<label class="switch"><input type="checkbox" id="cf-' + zone.id + '-usonly" data-zone="' + zone.id + '" data-feature="us_only"><span class="slider"></span></label>';
                    html += '</div></div>';
                    
                    // Block Scanners toggle
                    html += '<div class="cf-toggle-row" style="background: #0d1b2a; padding: 12px; border-radius: 6px;">';
                    html += '<div style="display: flex; justify-content: space-between; align-items: center;">';
                    html += '<div><strong style="color: #fff;">Block Scanners</strong><br><span style="color: #888; font-size: 11px;">Gmail, Outlook, SES scanners</span></div>';
                    html += '<label class="switch"><input type="checkbox" id="cf-' + zone.id + '-scanners" data-zone="' + zone.id + '" data-feature="block_scanners"><span class="slider"></span></label>';
                    html += '</div></div>';
                    
                    // Block Dupes toggle with time dropdown
                    html += '<div class="cf-toggle-row" style="background: #0d1b2a; padding: 12px; border-radius: 6px;">';
                    html += '<div style="display: flex; justify-content: space-between; align-items: center;">';
                    html += '<div><strong style="color: #fff;">Block Dupes</strong><br>';
                    html += '<span style="color: #888; font-size: 10px;">IP + Cookie rate limit</span><br>';
                    html += '<select id="cf-' + zone.id + '-dupes-time" style="margin-top: 5px; padding: 4px; background: #1a1a2e; border: 1px solid #444; color: #fff; border-radius: 3px; font-size: 11px;">';
                    html += '<option value="10" selected>10 sec (Free)</option>';
                    html += '<option value="60">1 min (Pro+)</option>';
                    html += '<option value="120">2 min (Pro+)</option>';
                    html += '<option value="300">5 min (Pro+)</option>';
                    html += '<option value="600">10 min (Pro+)</option>';
                    html += '<option value="3600">1 hour (Pro+)</option>';
                    html += '</select></div>';
                    html += '<label class="switch"><input type="checkbox" id="cf-' + zone.id + '-dupes" data-zone="' + zone.id + '" data-feature="block_dupes"><span class="slider"></span></label>';
                    html += '</div></div>';
                    
                    // Redirect URL for blocked traffic
                    html += '<div class="cf-toggle-row" style="background: #0d1b2a; padding: 12px; border-radius: 6px; grid-column: span 2;">';
                    html += '<div><strong style="color: #fff;">Blocked Traffic Redirect</strong><br>';
                    html += '<span style="color: #888; font-size: 10px;">Redirect blocked visitors to this URL instead of showing error</span><br>';
                    html += '<div style="display: flex; gap: 10px; margin-top: 8px;">';
                    html += '<input type="text" id="cf-' + zone.id + '-redirect-url" placeholder="https://example.com/blocked" style="flex: 1; padding: 6px; background: #1a1a2e; border: 1px solid #444; color: #fff; border-radius: 3px; font-size: 12px;">';
                    html += '<button class="cf-redirect-btn" data-zone="' + zone.id + '" style="padding: 6px 12px; font-size: 11px;">Set Redirect</button>';
                    html += '</div></div></div>';
                    
                    html += '</div>';
                    html += '<p id="cf-' + zone.id + '-status" style="color: #666; font-size: 11px; margin: 10px 0 0 0;">Loading status...</p>';
                    html += '</div>';
                    
                    // Load status for this zone
                    setTimeout(function() { loadZoneStatus(zone.id); }, 100);
                });
                
                container.innerHTML = html;
                
                // Add event listeners to all toggle checkboxes
                var toggles = container.querySelectorAll('input[type="checkbox"][data-zone]');
                toggles.forEach(function(toggle) {
                    toggle.addEventListener('change', function() {
                        var zoneId = this.getAttribute('data-zone');
                        var feature = this.getAttribute('data-feature');
                        toggleCfFeature(zoneId, feature, this.checked);
                    });
                });
                
                // Add event listeners to redirect buttons
                var redirectBtns = container.querySelectorAll('.cf-redirect-btn');
                redirectBtns.forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        var zoneId = this.getAttribute('data-zone');
                        setCfRedirect(zoneId);
                    });
                });
                
                // Add event listeners to remove buttons
                var removeBtns = container.querySelectorAll('.cf-remove-btn');
                removeBtns.forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        var domain = this.getAttribute('data-domain');
                        removeCfZone(domain);
                    });
                });
            })
            .catch(function(e) {
                container.innerHTML = '<p style="color: #ff6b6b;">Failed: ' + e + '</p>';
            });
        }
        
        function loadZoneStatus(zoneId) {
            fetch('/api/cloudflare/zones/' + zoneId + '/status')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) return;
                
                // Set toggle states
                var botfight = document.getElementById('cf-' + zoneId + '-botfight');
                var usonly = document.getElementById('cf-' + zoneId + '-usonly');
                var scanners = document.getElementById('cf-' + zoneId + '-scanners');
                var dupes = document.getElementById('cf-' + zoneId + '-dupes');
                var status = document.getElementById('cf-' + zoneId + '-status');
                
                if (botfight) botfight.checked = data.bot_fight || data.browser_check;
                if (usonly) usonly.checked = data.us_only;
                if (scanners) scanners.checked = data.block_scanners;
                if (dupes) dupes.checked = data.block_dupes;
                if (status) status.textContent = 'Security Level: ' + (data.security_level || 'medium').toUpperCase();
            })
            .catch(function() {});
        }
        
        function toggleCfFeature(zoneId, feature, enabled) {
            var statusEl = document.getElementById('cf-' + zoneId + '-status');
            if (statusEl) statusEl.textContent = 'Updating ' + feature + '...';
            
            var payload = {feature: feature, enabled: enabled};
            
            // Add time value and redirect URL for block_dupes
            if (feature === 'block_dupes') {
                var timeSelect = document.getElementById('cf-' + zoneId + '-dupes-time');
                if (timeSelect) {
                    payload.duration = parseInt(timeSelect.value);
                }
                // Include redirect URL if set
                var redirectInput = document.getElementById('cf-' + zoneId + '-redirect-url');
                if (redirectInput && redirectInput.value.trim()) {
                    payload.redirect_url = redirectInput.value.trim();
                }
            }
            
            fetch('/api/cloudflare/zones/' + zoneId + '/toggle', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    if (statusEl) statusEl.textContent = 'Error: ' + data.error;
                    // Revert checkbox
                    var cb = document.getElementById('cf-' + zoneId + '-' + feature.replace('_', ''));
                    if (cb) cb.checked = !enabled;
                    return;
                }
                if (statusEl) statusEl.textContent = feature + ' ' + (enabled ? 'enabled' : 'disabled') + ' - ' + new Date().toLocaleTimeString();
            })
            .catch(function(e) {
                if (statusEl) statusEl.textContent = 'Failed: ' + e;
            });
        }
        
        function setCfRedirect(zoneId) {
            var urlInput = document.getElementById('cf-' + zoneId + '-redirect-url');
            var statusEl = document.getElementById('cf-' + zoneId + '-status');
            var redirectUrl = urlInput ? urlInput.value.trim() : '';
            
            if (!redirectUrl) {
                alert('Please enter a redirect URL');
                return;
            }
            
            if (statusEl) statusEl.textContent = 'Setting redirect...';
            
            fetch('/api/cloudflare/zones/' + zoneId + '/redirect', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({redirect_url: redirectUrl})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    if (statusEl) statusEl.textContent = 'Error: ' + data.error;
                    return;
                }
                if (statusEl) statusEl.textContent = 'Redirect set to ' + redirectUrl + ' - ' + new Date().toLocaleTimeString();
            })
            .catch(function(e) {
                if (statusEl) statusEl.textContent = 'Failed: ' + e;
            });
        }
        
        function removeCfZone(domain) {
            if (!confirm('Remove ' + domain + ' from managed CF zones?\\n\\nThis will NOT delete any rules from Cloudflare, just remove it from this dashboard.')) {
                return;
            }
            
            fetch('/api/cloudflare/zones/remove', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    alert('Error: ' + data.error);
                    return;
                }
                // Refresh the zones list
                loadCloudflareZones();
            })
            .catch(function(e) {
                alert('Failed: ' + e);
            });
        }
        
        // =====================================================
        // DOMAIN REPUTATION CHECKER
        // =====================================================
        
        function loadReputationDomains() {
            fetch('/api/reputation/domains')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                repDomains = data.domains || {};
                renderReputationTable();
            })
            .catch(function(e) {
                console.error('Failed to load reputation domains:', e);
            });
        }
        
        function renderReputationTable() {
            var tbody = document.getElementById('rep-domains-tbody');
            if (!tbody) return;
            
            var domains = Object.keys(repDomains);
            if (domains.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" style="color: #666; text-align: center; padding: 20px;">Add domains above to check their reputation...</td></tr>';
                return;
            }
            
            var html = '';
            domains.sort().forEach(function(domain) {
                var result = repDomains[domain];
                var isClean = result.clean;
                var listedCount = result.listed_count || 0;
                
                var statusColor = isClean ? '#28a745' : (listedCount > 2 ? '#dc3545' : '#ffc107');
                var statusText = isClean ? 'CLEAN' : (listedCount + ' LISTED');
                
                // Build details
                var details = [];
                (result.blacklists || []).forEach(function(bl) {
                    if (bl.listed) {
                        var reason = bl.reason ? ' (' + bl.reason + ')' : '';
                        details.push('<span style="color: #dc3545;">' + bl.name + reason + '</span>');
                    }
                });
                var detailsHtml = details.length > 0 ? details.join(', ') : '<span style="color: #28a745;">All clear</span>';
                
                // Build quick links
                var urls = result.lookup_urls || {};
                var linksHtml = '';
                if (urls['Talos Intelligence']) {
                    linksHtml += '<a href="' + urls['Talos Intelligence'] + '" target="_blank" style="color: #f6821f; font-size: 11px; margin-right: 6px;" title="Cisco Talos">Talos</a>';
                }
                if (urls['MXToolbox']) {
                    linksHtml += '<a href="' + urls['MXToolbox'] + '" target="_blank" style="color: #00d4ff; font-size: 11px; margin-right: 6px;" title="MXToolbox">MX</a>';
                }
                if (urls['VirusTotal']) {
                    linksHtml += '<a href="' + urls['VirusTotal'] + '" target="_blank" style="color: #28a745; font-size: 11px;" title="VirusTotal">VT</a>';
                }
                if (!linksHtml) {
                    // Fallback if no URLs in result
                    linksHtml = '<a href="https://talosintelligence.com/reputation_center/lookup?search=' + domain + '" target="_blank" style="color: #f6821f; font-size: 11px; margin-right: 6px;">Talos</a>';
                    linksHtml += '<a href="https://mxtoolbox.com/SuperTool.aspx?action=blacklist%3a' + domain + '" target="_blank" style="color: #00d4ff; font-size: 11px; margin-right: 6px;">MX</a>';
                    linksHtml += '<a href="https://www.virustotal.com/gui/domain/' + domain + '" target="_blank" style="color: #28a745; font-size: 11px;">VT</a>';
                }
                
                html += '<tr>';
                html += '<td style="font-weight: bold;">' + domain + '</td>';
                html += '<td style="text-align: center;"><span style="color: ' + statusColor + '; font-weight: bold;">' + statusText + '</span></td>';
                html += '<td style="text-align: center;">' + listedCount + ' / ' + (result.blacklists || []).length + '</td>';
                html += '<td style="font-size: 12px;">' + detailsHtml + '</td>';
                html += '<td style="text-align: center;">' + linksHtml + '</td>';
                html += '<td style="text-align: center; white-space: nowrap;">';
                html += '<button class="rep-cf-btn" data-domain="' + domain + '" style="padding: 3px 8px; font-size: 11px; margin-right: 5px; background: #f6821f;" title="Enable CF Protection">CF</button>';
                html += '<button class="rep-recheck-btn" data-domain="' + domain + '" style="padding: 3px 8px; font-size: 11px; margin-right: 5px;">Check</button>';
                html += '<button class="rep-remove-btn" data-domain="' + domain + '" style="padding: 3px 8px; font-size: 11px; background: #dc3545;">X</button>';
                html += '</td>';
                html += '</tr>';
            });
            
            tbody.innerHTML = html;
            
            // Attach event listeners to buttons
            var recheckBtns = tbody.querySelectorAll('.rep-recheck-btn');
            recheckBtns.forEach(function(btn) {
                btn.addEventListener('click', function() {
                    checkSingleDomain(this.getAttribute('data-domain'));
                });
            });
            
            var removeBtns = tbody.querySelectorAll('.rep-remove-btn');
            removeBtns.forEach(function(btn) {
                btn.addEventListener('click', function() {
                    removeDomain(this.getAttribute('data-domain'));
                });
            });
            
            var cfBtns = tbody.querySelectorAll('.rep-cf-btn');
            cfBtns.forEach(function(btn) {
                btn.addEventListener('click', function() {
                    enableCfProtection(this.getAttribute('data-domain'), this);
                });
            });
        }
        
        function addReputationDomain() {
            var input = document.getElementById('rep-domain-input');
            var domain = input.value.trim().toLowerCase();
            if (!domain) {
                alert('Please enter a domain');
                return;
            }
            
            // Remove protocol if present
            if (domain.indexOf('://') > -1) {
                domain = domain.split('://')[1];
            }
            domain = domain.split('/')[0];
            
            input.value = '';
            input.disabled = true;
            
            fetch('/api/reputation/check', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                input.disabled = false;
                if (data.error) {
                    alert('Error: ' + data.error);
                    return;
                }
                repDomains[domain] = data;
                renderReputationTable();
            })
            .catch(function(e) {
                input.disabled = false;
                alert('Failed: ' + e);
            });
        }
        
        function checkSingleDomain(domain) {
            var row = document.querySelector('tr td:first-child');
            
            fetch('/api/reputation/check', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    alert('Error: ' + data.error);
                    return;
                }
                repDomains[domain] = data;
                renderReputationTable();
            })
            .catch(function(e) {
                alert('Failed: ' + e);
            });
        }
        
        function removeDomain(domain) {
            if (!confirm('Remove ' + domain + ' from monitoring?')) return;
            
            fetch('/api/reputation/remove', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                delete repDomains[domain];
                renderReputationTable();
            })
            .catch(function(e) {
                alert('Failed: ' + e);
            });
        }
        
        function enableCfProtection(domain, btn) {
            var options = [
                'US Only (block non-US traffic)',
                'Block Scanners (bots, link checkers)',
                'Block Dupes (rate limit clicks)',
                'All Protection (recommended)'
            ];
            
            var choice = prompt(
                'Enable Cloudflare protection for ' + domain + ':\\n\\n' +
                '1 = US Only\\n' +
                '2 = Block Scanners\\n' +
                '3 = Block Dupes (10 sec rate limit)\\n' +
                '4 = ALL (recommended)\\n\\n' +
                'Enter choice (1-4):',
                '4'
            );
            
            if (!choice) return;
            
            var features = [];
            if (choice === '1' || choice === '4') features.push('us_only');
            if (choice === '2' || choice === '4') features.push('block_scanners');
            if (choice === '3' || choice === '4') features.push('block_dupes');
            
            if (features.length === 0) {
                alert('Invalid choice');
                return;
            }
            
            btn.disabled = true;
            btn.textContent = '...';
            
            fetch('/api/reputation/cf-protect', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({domain: domain, features: features})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                btn.disabled = false;
                btn.textContent = 'CF';
                
                if (data.error) {
                    alert('Error: ' + data.error);
                } else if (data.not_found) {
                    alert(domain + ' is not in your Cloudflare zones.\\nAdd it to Cloudflare first.');
                } else {
                    var msg = 'CF Protection enabled for ' + domain + ':\\n';
                    if (data.results) {
                        for (var f in data.results) {
                            var r = data.results[f];
                            msg += '\\n' + f + ': ' + (r.success ? 'OK' : 'Failed');
                        }
                    }
                    msg += '\\n\\nSwitching to Cloudflare tab...';
                    alert(msg);
                    
                    // Switch to Cloudflare tab and refresh zones
                    showTab('cloudflare');
                    loadCloudflareZones();
                }
            })
            .catch(function(e) {
                btn.disabled = false;
                btn.textContent = 'CF';
                alert('Failed: ' + e);
            });
        }
        
        function refreshAllReputation() {
            var btn = document.querySelector('#tab-reputation button[onclick="refreshAllReputation()"]');
            if (btn) {
                btn.disabled = true;
                btn.textContent = 'Checking...';
            }
            
            fetch('/api/reputation/refresh-all', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = 'Refresh All';
                }
                if (data.domains) {
                    repDomains = data.domains;
                    renderReputationTable();
                }
            })
            .catch(function(e) {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = 'Refresh All';
                }
                alert('Failed: ' + e);
            });
        }
        
        function importCloudflareDomainsToReputation() {
            var btn = document.querySelector('button[onclick="importCloudflareDomainsToReputation()"]');
            if (btn) {
                btn.disabled = true;
                btn.textContent = 'Importing...';
            }
            
            fetch('/api/reputation/import-cf', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = 'Import CF Domains';
                }
                if (data.domains) {
                    repDomains = data.domains;
                    renderReputationTable();
                    alert('Imported ' + Object.keys(data.domains).length + ' domains');
                }
            })
            .catch(function(e) {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = 'Import CF Domains';
                }
                alert('Failed: ' + e);
            });
        }
        
        // =====================================================
        // MX VALIDATOR
        // =====================================================
        var mxRunning = false;
        var mxPaused = false;
        var mxEventSource = null;
        var mxStats = { total: 0, checked: 0, valid: 0, dead: 0, valid_emails: 0, dead_emails: 0, rate: 0 };
        var mxCategories = { google: 0, microsoft: 0, yahoo: 0, hostgator: 0, godaddy: 0, realgi: 0, dead: 0 };
        var dnsServerStats = {
            'Google-1': {valid: 0, dead: 0}, 'Google-2': {valid: 0, dead: 0},
            'Cloudflare-1': {valid: 0, dead: 0}, 'Cloudflare-2': {valid: 0, dead: 0},
            'OpenDNS-1': {valid: 0, dead: 0}, 'OpenDNS-2': {valid: 0, dead: 0},
            'Quad9-1': {valid: 0, dead: 0}, 'Quad9-2': {valid: 0, dead: 0},
            'Level3-1': {valid: 0, dead: 0}, 'Level3-2': {valid: 0, dead: 0},
            'Verisign-1': {valid: 0, dead: 0}, 'Verisign-2': {valid: 0, dead: 0}
        };
        
        function updateDnsServerDisplay() {
            for (var server in dnsServerStats) {
                var stats = dnsServerStats[server];
                var id = 'dns-' + server.toLowerCase().replace(/-/g, '');
                var elem = document.getElementById(id);
                if (elem) {
                    var total = stats.valid + stats.dead;
                    var validPct = total > 0 ? ((stats.valid / total) * 100).toFixed(0) : 0;
                    elem.innerHTML = '<span style="color: #28a745;">' + stats.valid + '</span> / <span style="color: #dc3545;">' + stats.dead + '</span><br><small style="color: #888;">' + validPct + '% valid</small>';
                }
            }
        }
        
        var dnsStatsLocked = false;  // When true, don't load from DB API
        
        function resetDnsStats() {
            for (var server in dnsServerStats) {
                dnsServerStats[server].valid = 0;
                dnsServerStats[server].dead = 0;
            }
            updateDnsServerDisplay();
            flushCount = 0;
            totalFlushed = 0;
            dnsStatsLocked = true;  // Prevent API from overwriting
            console.log('DNS stats cleared and locked (will only show new results)');
        }
        
        // Auto-reset DNS stats on page load
        resetDnsStats();
        
        function addMxLog(domain, mx, category, dnsServer) {
            var terminal = document.getElementById('mx-terminal');
            var time = new Date().toLocaleTimeString();
            var catClass = 'mx-cat-' + category.toLowerCase().replace(/[^a-z]/g, '');
            if (!catClass.match(/google|microsoft|yahoo|hostgator|godaddy|dead|realgi/)) catClass = 'mx-cat-other';
            
            if (dnsServer && dnsServerStats[dnsServer]) {
                if (category.toLowerCase().indexOf('dead') >= 0 || category.toLowerCase() === 'nxdomain') {
                    dnsServerStats[dnsServer].dead++;
                } else {
                    dnsServerStats[dnsServer].valid++;
                }
                updateDnsServerDisplay();
            }
            
            var dnsInfo = dnsServer ? ' <span style="color: #666; font-size: 0.9em;">[' + dnsServer + ']</span>' : '';
            var line = document.createElement('div');
            line.className = 'mx-log-line';
            line.innerHTML = '<span class="mx-log-time">[' + time + ']</span> ' +
                '<span class="mx-log-domain">' + domain + '</span> ' +
                '<span class="mx-log-arrow">&rarr;</span> ' +
                '<span class="mx-log-mx">' + mx + '</span>' +
                '<span class="mx-log-category ' + catClass + '">' + category + '</span>' + dnsInfo;
            terminal.appendChild(line);
            terminal.scrollTop = terminal.scrollHeight;
            
            while (terminal.children.length > 500) {
                terminal.removeChild(terminal.firstChild);
            }
        }
        
        var flushCount = 0;
        var totalFlushed = 0;
        
        function addFlushLog(info, category, validCount, deadCount) {
            var terminal = document.getElementById('mx-flush-terminal');
            var time = new Date().toLocaleTimeString();
            flushCount++;
            totalFlushed += 500;
            
            var isError = category === 'Error';
            var color = isError ? '#dc3545' : '#28a745';
            var validStr = validCount !== undefined ? validCount : '?';
            var deadStr = deadCount !== undefined ? deadCount : '?';
            
            var line = document.createElement('div');
            line.className = 'mx-log-line';
            line.innerHTML = '<span class="mx-log-time">[' + time + ']</span> ' +
                '<span style="color: ' + color + '; font-weight: bold;">COMMIT #' + flushCount + '</span> ' +
                '<span style="color: #888;">' + info + '</span> ' +
                '<span style="color: #28a745;">' + validStr + ' valid</span> / ' +
                '<span style="color: #dc3545;">' + deadStr + ' dead</span> ' +
                '<span style="color: #666;">(total: ' + formatNum(totalFlushed) + ')</span>';
            terminal.appendChild(line);
            terminal.scrollTop = terminal.scrollHeight;
            
            while (terminal.children.length > 200) {
                terminal.removeChild(terminal.firstChild);
            }
        }
        
        var mxCategoryEmails = { Google: 0, Microsoft: 0, Yahoo: 0, HostGator: 0, GoDaddy: 0, Real_GI: 0, Parked: 0, Dead: 0 };
        
        function updateMxStats() {
            document.getElementById('mx-total').textContent = formatNum(mxStats.total);
            document.getElementById('mx-checked').textContent = formatNum(mxStats.checked);
            var remaining = Math.max(0, mxStats.total - mxStats.checked);
            document.getElementById('mx-remaining').textContent = formatNum(remaining);
            document.getElementById('mx-valid').textContent = formatNum(mxStats.valid);
            document.getElementById('mx-dead').textContent = formatNum(mxStats.dead);
            document.getElementById('mx-rate').textContent = mxStats.rate;
            var validEm = document.getElementById('mx-valid-emails');
            var deadEm = document.getElementById('mx-dead-emails');
            if (validEm) validEm.textContent = formatNum(mxStats.valid_emails || 0) + ' emails';
            if (deadEm) deadEm.textContent = formatNum(mxStats.dead_emails || 0) + ' emails';
            
            var pct = mxStats.total ? (mxStats.checked / mxStats.total * 100) : 0;
            document.getElementById('mx-progress-fill').style.width = pct + '%';
            document.getElementById('mx-progress-text').textContent = 
                formatNum(mxStats.checked) + ' / ' + formatNum(mxStats.total) + ' (' + pct.toFixed(1) + '%)';
            
            // Calculate ETA
            var etaEl = document.getElementById('mx-eta');
            if (etaEl && mxStats.rate > 0 && mxStats.total > mxStats.checked) {
                var remaining = mxStats.total - mxStats.checked;
                var secondsLeft = remaining / mxStats.rate;
                var hours = Math.floor(secondsLeft / 3600);
                var mins = Math.floor((secondsLeft % 3600) / 60);
                if (hours > 0) {
                    etaEl.textContent = hours + 'h ' + mins + 'm remaining';
                } else if (mins > 0) {
                    etaEl.textContent = mins + ' min remaining';
                } else {
                    etaEl.textContent = 'almost done';
                }
            } else if (etaEl) {
                etaEl.textContent = mxStats.status === 'complete' ? 'complete' : 'calculating...';
            }
            
            document.getElementById('mx-cat-google').textContent = formatNum(mxCategories.Google || mxCategories.google || 0);
            document.getElementById('mx-cat-microsoft').textContent = formatNum(mxCategories.Microsoft || mxCategories.microsoft || 0);
            document.getElementById('mx-cat-yahoo').textContent = formatNum(mxCategories.Yahoo || mxCategories.yahoo || 0);
            document.getElementById('mx-cat-hostgator').textContent = formatNum(mxCategories.HostGator || mxCategories.hostgator || 0);
            document.getElementById('mx-cat-godaddy').textContent = formatNum(mxCategories.GoDaddy || mxCategories.godaddy || 0);
            document.getElementById('mx-cat-realgi').textContent = formatNum(mxCategories.Real_GI || mxCategories.realgi || 0);
            document.getElementById('mx-cat-parked').textContent = formatNum(mxCategories.Parked || mxCategories.parked || 0);
            document.getElementById('mx-cat-dead').textContent = formatNum(mxCategories.Dead || mxCategories.dead || 0);
            var ce = mxCategoryEmails;
            setCatEmail('mx-cat-google-emails', ce.Google);
            setCatEmail('mx-cat-microsoft-emails', ce.Microsoft);
            setCatEmail('mx-cat-yahoo-emails', ce.Yahoo);
            setCatEmail('mx-cat-hostgator-emails', ce.HostGator);
            setCatEmail('mx-cat-godaddy-emails', ce.GoDaddy);
            setCatEmail('mx-cat-realgi-emails', ce.Real_GI);
            setCatEmail('mx-cat-parked-emails', ce.Parked);
            setCatEmail('mx-cat-dead-emails', mxStats.dead_emails != null ? mxStats.dead_emails : ce.Dead);
        }
        function setCatEmail(id, n) {
            var el = document.getElementById(id);
            if (el) el.textContent = formatNum(n || 0) + ' emails';
        }
        
        function startMxScan() {
            try {
                if (mxRunning) { alert('Already running'); return; }
                
                addMxLog('SYSTEM', 'Starting MX validation scan...', 'Info');
                
                var workers = parseInt(document.getElementById('mx-workers').value, 10) || 12;
                
                fetch('/api/mx/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({workers: workers, resume: true})
                })
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) {
                        addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                        alert('API Error: ' + data.error);
                        return;
                    }
                    
                    mxRunning = true;
                    mxPaused = false;
                    
                    document.getElementById('mx-start-btn').disabled = true;
                    var resetBtn = document.getElementById('mx-reset-dead-btn');
                    if (resetBtn) resetBtn.disabled = true;
                    document.getElementById('mx-pause-btn').disabled = false;
                    document.getElementById('mx-stop-btn').disabled = false;
                    
                    addMxLog('SYSTEM', 'Scan started with ' + data.workers + ' workers', 'Info');
                    
                    // Connect to SSE stream for real-time updates
                    connectMxStream();
                })
                .catch(function(e) {
                    addMxLog('SYSTEM', 'Failed to start: ' + e, 'Error');
                    alert('Fetch error: ' + e);
                });
            } catch(e) {
                alert('startMxScan error: ' + e.message);
            }
        }
        
        function resetDeadOnly() {
            if (!confirm('Reset all dead domains to unchecked? (No scan will start)')) return;
            addMxLog('SYSTEM', 'Resetting dead domains to unchecked...', 'Info');
            fetch('/api/mx/reset-dead', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                    alert('Error: ' + data.error);
                    return;
                }
                addMxLog('SYSTEM', 'Reset ' + formatNum(data.reset || 0) + ' dead domains to unchecked.', 'Info');
                alert('Done! Reset ' + formatNum(data.reset || 0) + ' dead domains. Click Start Scan when ready.');
                checkMxStatus(); // Refresh stats
            })
            .catch(function(e) {
                addMxLog('SYSTEM', 'Failed: ' + e, 'Error');
                alert('Fetch error: ' + e);
            });
        }
        
        function discoverNewDomains() {
            if (!confirm('Discover new GI domains from imported emails? This will find domains not yet in the MX table and add them for scanning.')) return;
            document.getElementById('mx-sync-gi-btn').disabled = true;
            addMxLog('SYSTEM', 'Discovering new GI domains from imported emails...', 'Info');
            fetch('/api/mx/sync-gi', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                document.getElementById('mx-sync-gi-btn').disabled = false;
                if (data.error) {
                    addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                    alert('Error: ' + data.error);
                    return;
                }
                var msg = 'Discovery complete! Found ' + formatNum(data.domains_inserted) + ' new domains. Ready to scan: ' + formatNum(data.unchecked_to_scan);
                addMxLog('SYSTEM', msg, 'Info');
                alert('Discovery complete!\\n\\nNew domains found: ' + formatNum(data.domains_inserted) + '\\nReady to scan: ' + formatNum(data.unchecked_to_scan));
                checkMxStatus(); // Refresh stats
            })
            .catch(function(e) {
                document.getElementById('mx-sync-gi-btn').disabled = false;
                addMxLog('SYSTEM', 'Failed: ' + e, 'Error');
                alert('Fetch error: ' + e);
            });
        }
        
        function connectMxStream() {
            if (typeof(EventSource) !== "undefined") {
                mxEventSource = new EventSource('/api/mx/stream');
                
                mxEventSource.onmessage = function(event) {
                    var data = JSON.parse(event.data);
                    if (data.type === 'stats') {
                        mxStats = data.stats || mxStats;
                        mxCategories = data.categories || mxCategories;
                        updateMxStats();
                        
                        // Update status
                        if (data.stats && data.stats.status === 'complete') {
                            addMxLog('SYSTEM', 'Scan complete!', 'Info');
                            stopMxScan();
                        } else if (data.stats && data.stats.status === 'paused') {
                            mxPaused = true;
                            document.getElementById('mx-pause-btn').textContent = 'Resume';
                        } else if (data.stats && data.stats.status === 'running') {
                            mxPaused = false;
                            document.getElementById('mx-pause-btn').textContent = 'Pause';
                        }
                    } else if (data.type === 'log') {
                        if (data.domain === 'DB_FLUSH') {
                            addFlushLog(data.mx, data.category, data.valid_count, data.dead_count);
                        } else {
                            addMxLog(data.domain, data.mx, data.category, data.dns_server);
                        }
                    } else if (data.type === 'complete') {
                        addMxLog('SYSTEM', 'Scan complete!', 'Info');
                        stopMxScan();
                    } else if (data.type === 'error') {
                        addMxLog('SYSTEM', 'Error: ' + data.message, 'Error');
                    }
                };
                
                mxEventSource.onerror = function() {
                    // Connection lost - try to reconnect or show status
                    if (mxRunning) {
                        setTimeout(function() {
                            if (mxRunning) {
                                addMxLog('SYSTEM', 'Reconnecting to stream...', 'Warning');
                                connectMxStream();
                            }
                        }, 2000);
                    }
                };
            }
        }
        
        function runMxDemo() {
            // Demo data for preview
            var demoData = [
                ['example.com', 'aspmx.l.google.com', 'Google'],
                ['mybusiness.net', 'mx1.hostgator.com', 'HostGator'],
                ['oldsite.org', 'NXDOMAIN', 'Dead'],
                ['shop123.com', 'mail.protection.outlook.com', 'Microsoft'],
                ['creative.co', 'mx.zoho.com', 'Zoho'],
                ['deadlink.xyz', 'TIMEOUT', 'Dead'],
                ['localshop.biz', 'mx1.emailsrvr.com', 'Rackspace'],
                ['techstartup.io', 'aspmx.l.google.com', 'Google'],
                ['myblog.me', 'mx-biz.mail.am0.yahoodns.net', 'Yahoo'],
                ['ecommerce.store', 'secureserver.net', 'GoDaddy'],
                ['portfolio.design', 'mail.protonmail.ch', 'ProtonMail'],
                ['nonprofit.org', 'mail.protection.outlook.com', 'Microsoft'],
                ['realestate.homes', 'mail1.realgi-server.com', 'Real_GI'],
                ['vintage.shop', 'mx.ionos.com', '1and1'],
            ];
            
            mxStats.total = 523847;
            var idx = 0;
            
            var demoInterval = setInterval(function() {
                if (!mxRunning || mxPaused) {
                    if (!mxRunning) clearInterval(demoInterval);
                    return;
                }
                
                var item = demoData[idx % demoData.length];
                addMxLog(item[0], item[1], item[2]);
                
                mxStats.checked += Math.floor(Math.random() * 50) + 20;
                if (mxStats.checked > mxStats.total) mxStats.checked = mxStats.total;
                
                var cat = item[2].toLowerCase().replace(/[^a-z]/g, '');
                if (cat === 'dead') { mxStats.dead++; mxCategories.dead++; }
                else { mxStats.valid++; }
                
                if (cat === 'google') mxCategories.google += Math.floor(Math.random() * 5) + 1;
                else if (cat === 'microsoft') mxCategories.microsoft += Math.floor(Math.random() * 3) + 1;
                else if (cat === 'yahoo') mxCategories.yahoo += Math.floor(Math.random() * 2) + 1;
                else if (cat === 'hostgator') mxCategories.hostgator += Math.floor(Math.random() * 2) + 1;
                else if (cat === 'godaddy') mxCategories.godaddy += Math.floor(Math.random() * 2) + 1;
                else mxCategories.realgi += Math.floor(Math.random() * 3) + 1;
                
                mxStats.rate = Math.floor(Math.random() * 200) + 700;
                updateMxStats();
                
                idx++;
            }, 300);
        }
        
        function pauseMxScan() {
            fetch('/api/mx/pause', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                    return;
                }
                mxPaused = (data.status === 'paused');
                document.getElementById('mx-pause-btn').textContent = mxPaused ? 'Resume' : 'Pause';
                addMxLog('SYSTEM', mxPaused ? 'Scan paused' : 'Scan resumed', 'Info');
            });
        }
        
        function stopMxScan() {
            fetch('/api/mx/stop', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                mxRunning = false;
                mxPaused = false;
                if (mxEventSource) {
                    mxEventSource.close();
                    mxEventSource = null;
                }
                
                document.getElementById('mx-start-btn').disabled = false;
                var resetBtn = document.getElementById('mx-reset-dead-btn');
                if (resetBtn) resetBtn.disabled = false;
                document.getElementById('mx-pause-btn').disabled = true;
                document.getElementById('mx-stop-btn').disabled = true;
                document.getElementById('mx-pause-btn').textContent = 'Pause';
                
                addMxLog('SYSTEM', 'Scan stopped', 'Info');
            });
        }
        
        // Check initial MX status on page load (also load persisted DNS stats from DB)
        function checkMxStatus() {
            fetch('/api/mx/status')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) return;
                
                mxStats.total = data.total || 0;
                mxStats.checked = data.checked || 0;
                mxStats.valid = data.valid || 0;
                mxStats.dead = data.dead || 0;
                mxStats.valid_emails = data.valid_emails || 0;
                mxStats.dead_emails = data.dead_emails || 0;
                mxStats.rate = data.rate || 0;
                mxCategories = data.categories || mxCategories;
                updateMxStats();
                
                if (data.status === 'running' || data.status === 'paused') {
                    mxRunning = true;
                    mxPaused = (data.status === 'paused');
                    document.getElementById('mx-start-btn').disabled = true;
                    document.getElementById('mx-pause-btn').disabled = false;
                    document.getElementById('mx-stop-btn').disabled = false;
                    document.getElementById('mx-pause-btn').textContent = mxPaused ? 'Resume' : 'Pause';
                    connectMxStream();
                }
            })
            .catch(function() {});
            
            // Load category email counts (emails per MX category from domain_mx)
            fetch('/api/mx/category-email-counts')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error || !data.categories) return;
                var ce = data.categories;
                mxCategoryEmails = {
                    Google: ce.Google || 0, Microsoft: ce.Microsoft || 0, Yahoo: ce.Yahoo || 0,
                    HostGator: ce.HostGator || 0, GoDaddy: ce.GoDaddy || 0, Real_GI: ce.Real_GI || 0,
                    Parked: ce.Parked || 0, Dead: ce.Dead || 0
                };
                updateMxStats();
            })
            .catch(function() {});
            
            // Load persisted DNS server stats from DB (survives restart)
            // Skip if stats were manually cleared (dnsStatsLocked)
            if (!dnsStatsLocked) {
                fetch('/api/mx/dns-stats')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var servers = data.servers || {};
                    for (var name in servers) {
                        if (dnsServerStats[name]) {
                            dnsServerStats[name].valid = servers[name].valid || 0;
                            dnsServerStats[name].dead = servers[name].dead || 0;
                        }
                    }
                    updateDnsServerDisplay();
                })
                .catch(function() {});
            }
        }
        
        // Check status when MX tab is shown
        document.addEventListener('DOMContentLoaded', function() {
            // Initial check after small delay
            setTimeout(checkMxStatus, 500);
        });
        
        function applyMxResults() {
            if (!confirm('This will update all emails with MX categories from the domain_mx table. Continue?')) return;
            
            addMxLog('SYSTEM', 'Applying MX results to emails table...', 'Info');
            
            fetch('/api/mx/apply', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                    alert('Error: ' + data.error);
                } else {
                    addMxLog('SYSTEM', 'Updated ' + formatNum(data.updated) + ' emails with MX categories', 'Info');
                    alert('Success! Updated ' + formatNum(data.updated) + ' emails.');
                }
            })
            .catch(function(e) {
                addMxLog('SYSTEM', 'Failed: ' + e, 'Error');
                alert('Error: ' + e);
            });
        }
        
        function showDeadDomains() {
            document.getElementById('dead-modal').style.display = 'block';
            document.getElementById('dead-domains-list').innerHTML = '<p class="loading">Loading dead domains...</p>';
            
            fetch('/api/mx/dead-domains')
            .then(function(r) {
                if (!r.ok) {
                    return r.text().then(function(text) {
                        throw new Error(r.status + ': ' + (text ? text.substring(0, 200) : r.statusText));
                    });
                }
                return r.json();
            })
            .then(function(data) {
                if (data.error && !data.domains) {
                    document.getElementById('dead-domains-list').innerHTML = '<p class="error">' + data.error + '</p>';
                    return;
                }
                var domains = data.domains || [];
                var count = data.count || 0;
                var html = '<p style="color: #888; margin-bottom: 10px;">Found ' + formatNum(count) + ' dead domains. Showing first 500.</p>';
                html += '<div style="max-height: 500px; overflow-y: auto;">';
                var i;
                for (i = 0; i < domains.length; i++) {
                    var d = domains[i];
                    var bgColor = '#1e2a3d';
                    if (d.error_message && d.error_message.indexOf('NXDOMAIN') >= 0) {
                        bgColor = '#3d1e1e';
                    }
                    html += '<div style="padding: 8px; margin: 3px 0; background: ' + bgColor + '; border-radius: 3px; display: flex; justify-content: space-between;">';
                    html += '<span style="color: #00d4ff;">' + (d.domain || '') + '</span>';
                    html += '<span style="color: #666; font-size: 0.85em;">' + (d.error_message || 'Unknown') + ' (' + formatNum(d.email_count || 0) + ' emails)</span>';
                    html += '</div>';
                }
                html += '</div>';
                document.getElementById('dead-domains-list').innerHTML = html;
            })
            .catch(function(e) {
                document.getElementById('dead-domains-list').innerHTML = '<p class="error">Error: ' + (e.message || 'Failed to load') + '</p>';
            });
        }
        
        function closeDeadModal() {
            document.getElementById('dead-modal').style.display = 'none';
        }
        
        function copyToClipboard(text) {
            navigator.clipboard.writeText(text).then(function() {
                console.log('Copied: ' + text);
            });
        }
        
        // Close modal when clicking outside
        document.addEventListener('click', function(e) {
            if (e.target.id === 'dead-modal') {
                closeDeadModal();
            }
        });
        
        // =====================================================
        // IMPORT DATA TAB FUNCTIONS
        // =====================================================
        
        var importFiles = [];
        var importStatusInterval = null;
        var currentBrowsePath = 'C:\\EmailData';
        
        // Directory Browser Functions
        function openDirBrowser() {
            document.getElementById('dir-browser-modal').style.display = 'block';
            currentBrowsePath = document.getElementById('import-dir').value || 'C:\\EmailData';
            document.getElementById('dir-browser-path').value = currentBrowsePath;
            loadDirContents(currentBrowsePath);
        }
        
        function closeDirBrowser() {
            document.getElementById('dir-browser-modal').style.display = 'none';
        }
        
        function navigateToPath() {
            var path = document.getElementById('dir-browser-path').value;
            if (path) {
                loadDirContents(path);
            }
        }
        
        function loadDirContents(path) {
            currentBrowsePath = path;
            document.getElementById('dir-browser-path').value = path;
            document.getElementById('dir-browser-list').innerHTML = '<div style="color: #ffc107;">Loading...</div>';
            
            fetch('/api/browse-dir', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({path: path})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('dir-browser-list').innerHTML = '<div style="color: #dc3545;">Error: ' + data.error + '</div>';
                    return;
                }
                
                var container = document.getElementById('dir-browser-list');
                container.innerHTML = '';
                
                // Parent directory link
                if (data.parent) {
                    var parentDiv = document.createElement('div');
                    parentDiv.style.cssText = 'padding: 10px; cursor: pointer; border-bottom: 1px solid #333; display: flex; align-items: center;';
                    parentDiv.innerHTML = '<span style="color: #ffc107; margin-right: 10px;">📁</span><span style="color: #ffc107;">..</span><span style="color: #666; margin-left: 10px;">(Parent Directory)</span>';
                    parentDiv.onmouseover = function() { this.style.background = '#252525'; };
                    parentDiv.onmouseout = function() { this.style.background = 'transparent'; };
                    parentDiv.onclick = function() { loadDirContents(data.parent); };
                    container.appendChild(parentDiv);
                }
                
                // Directories
                if (data.directories && data.directories.length > 0) {
                    for (var i = 0; i < data.directories.length; i++) {
                        (function(dir) {
                            var dirDiv = document.createElement('div');
                            dirDiv.style.cssText = 'padding: 10px; cursor: pointer; border-bottom: 1px solid #252525; display: flex; align-items: center;';
                            dirDiv.innerHTML = '<span style="color: #69db7c; margin-right: 10px;">📁</span><span style="color: #ddd;">' + dir.name + '</span>';
                            dirDiv.onmouseover = function() { this.style.background = '#252525'; };
                            dirDiv.onmouseout = function() { this.style.background = 'transparent'; };
                            dirDiv.onclick = function() { loadDirContents(dir.path); };
                            container.appendChild(dirDiv);
                        })(data.directories[i]);
                    }
                }
                
                // Files (info only)
                if (data.files && data.files.length > 0) {
                    var filesHeader = document.createElement('div');
                    filesHeader.style.cssText = 'padding: 10px 10px 5px; color: #666; font-size: 0.85em; border-top: 1px solid #333; margin-top: 10px;';
                    filesHeader.textContent = 'Files in this directory:';
                    container.appendChild(filesHeader);
                    
                    var maxFiles = Math.min(data.files.length, 10);
                    for (var i = 0; i < maxFiles; i++) {
                        var fileDiv = document.createElement('div');
                        fileDiv.style.cssText = 'padding: 5px 10px; color: #888; font-size: 0.9em;';
                        fileDiv.innerHTML = '<span style="margin-right: 10px;">📄</span>' + data.files[i].name + '<span style="color: #666; margin-left: 10px;">(' + data.files[i].size_mb + ' MB)</span>';
                        container.appendChild(fileDiv);
                    }
                    if (data.files.length > 10) {
                        var moreDiv = document.createElement('div');
                        moreDiv.style.cssText = 'padding: 5px 10px; color: #666; font-size: 0.85em;';
                        moreDiv.textContent = '...and ' + (data.files.length - 10) + ' more files';
                        container.appendChild(moreDiv);
                    }
                }
                
                if ((!data.directories || data.directories.length === 0) && (!data.files || data.files.length === 0)) {
                    var emptyDiv = document.createElement('div');
                    emptyDiv.style.cssText = 'padding: 20px; color: #666; text-align: center;';
                    emptyDiv.textContent = 'Empty directory';
                    container.appendChild(emptyDiv);
                }
            })
            .catch(function(e) {
                document.getElementById('dir-browser-list').innerHTML = '<div style="color: #dc3545;">Error: ' + e + '</div>';
            });
        }
        
        function selectCurrentDir() {
            document.getElementById('import-dir').value = currentBrowsePath;
            closeDirBrowser();
            scanImportDir();
        }
        
        function scanImportDir() {
            var dir = document.getElementById('import-dir').value;
            if (!dir) {
                alert('Please enter a directory path');
                return;
            }
            
            document.getElementById('import-file-list').innerHTML = '<div style="color: #ffc107;">Scanning...</div>';
            
            fetch('/api/import/scan-dir', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({path: dir})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('import-file-list').innerHTML = '<div style="color: #dc3545;">Error: ' + data.error + '</div>';
                    return;
                }
                
                importFiles = data.files || [];
                renderImportFileList();
            })
            .catch(function(e) {
                document.getElementById('import-file-list').innerHTML = '<div style="color: #dc3545;">Error: ' + e + '</div>';
            });
        }
        
        function renderImportFileList() {
            var html = '';
            if (importFiles.length === 0) {
                html = '<div style="color: #888;">No importable files found.</div>';
            } else {
                for (var i = 0; i < importFiles.length; i++) {
                    var f = importFiles[i];
                    var typeColor = f.detected_type === 'clicker' ? '#ff6b6b' : (f.detected_type === 'opener' ? '#69db7c' : '#888');
                    html += '<div style="display: flex; align-items: center; padding: 8px 0; border-bottom: 1px solid #252525;">';
                    html += '<input type="checkbox" class="import-file-cb" data-idx="' + i + '" checked style="margin-right: 10px;">';
                    html += '<span style="flex: 1; color: #ddd;">' + f.filename + '</span>';
                    html += '<span style="width: 80px; color: #888; text-align: right;">' + f.size_mb + ' MB</span>';
                    html += '<span style="width: 60px; color: #888; text-align: right;">' + f.column_count + ' cols</span>';
                    html += '<span style="width: 80px; color: ' + typeColor + '; text-align: right; text-transform: capitalize;">' + f.detected_type + '</span>';
                    html += '</div>';
                }
            }
            document.getElementById('import-file-list').innerHTML = html;
        }
        
        function toggleAllImportFiles() {
            var checked = document.getElementById('import-select-all').checked;
            var checkboxes = document.querySelectorAll('.import-file-cb');
            for (var i = 0; i < checkboxes.length; i++) {
                checkboxes[i].checked = checked;
            }
        }
        
        function getSelectedImportFiles() {
            var selected = [];
            var checkboxes = document.querySelectorAll('.import-file-cb:checked');
            for (var i = 0; i < checkboxes.length; i++) {
                var idx = parseInt(checkboxes[i].getAttribute('data-idx'));
                if (importFiles[idx]) {
                    selected.push(importFiles[idx]);
                }
            }
            return selected;
        }
        
        function previewSelectedFile() {
            var selected = getSelectedImportFiles();
            if (selected.length === 0) {
                alert('Please select a file to preview');
                return;
            }
            
            var file = selected[0];
            document.getElementById('import-preview-modal').style.display = 'block';
            document.getElementById('import-preview-content').innerHTML = '<div style="color: #ffc107;">Loading preview...</div>';
            
            fetch('/api/import/preview', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({path: file.path, filename: file.filename})
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('import-preview-content').innerHTML = '<div style="color: #dc3545;">Error: ' + data.error + '</div>';
                    return;
                }
                
                var html = '<h4 style="color: #aaa;">File: ' + data.filename + ' (Type: ' + data.detected_type + ')</h4>';
                
                // Column mapping
                html += '<h5 style="color: #888; margin-top: 15px;">Detected Column Mapping:</h5>';
                html += '<div style="background: #252525; padding: 10px; border-radius: 5px; margin-bottom: 15px;">';
                for (var field in data.column_mapping) {
                    html += '<span style="color: #00d4ff; margin-right: 15px;">' + field + ': col ' + data.column_mapping[field] + '</span>';
                }
                html += '</div>';
                
                // Sample rows
                html += '<h5 style="color: #888;">Sample Rows (first 10):</h5>';
                html += '<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse; font-size: 0.85em;">';
                for (var i = 0; i < data.rows.length; i++) {
                    html += '<tr>';
                    for (var j = 0; j < data.rows[i].length; j++) {
                        var val = data.rows[i][j] || '';
                        if (val.length > 30) val = val.substring(0, 30) + '...';
                        var bgColor = i % 2 === 0 ? '#1a1a1a' : '#252525';
                        html += '<td style="padding: 5px 8px; border: 1px solid #333; background: ' + bgColor + '; color: #ddd;">' + val + '</td>';
                    }
                    html += '</tr>';
                }
                html += '</table></div>';
                
                document.getElementById('import-preview-content').innerHTML = html;
            })
            .catch(function(e) {
                document.getElementById('import-preview-content').innerHTML = '<div style="color: #dc3545;">Error: ' + e + '</div>';
            });
        }
        
        function closePreviewModal() {
            document.getElementById('import-preview-modal').style.display = 'none';
        }
        
        function startImport() {
            var selected = getSelectedImportFiles();
            if (selected.length === 0) {
                alert('Please select files to import');
                return;
            }
            
            var dataSource = document.getElementById('import-source').value || 'External Import';
            
            if (!confirm('Import ' + selected.length + ' file(s) with data source "' + dataSource + '"?\\n\\nThis will use enrichment upsert (same email = combined data).')) {
                return;
            }
            
            // Show progress section, hide start button
            document.getElementById('import-progress-section').style.display = 'block';
            document.getElementById('btn-start-import').style.display = 'none';
            document.getElementById('btn-stop-import').style.display = 'inline-block';
            document.getElementById('import-status').textContent = 'Starting...';
            document.getElementById('import-status').style.color = '#ffc107';
            
            // Start import
            fetch('/api/import/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    files: selected,
                    data_source: dataSource
                })
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    alert('Error: ' + data.error);
                    resetImportUI();
                    return;
                }
                
                // Start polling for status
                importStatusInterval = setInterval(pollImportStatus, 1000);
            })
            .catch(function(e) {
                alert('Error: ' + e);
                resetImportUI();
            });
        }
        
        function pollImportStatus() {
            fetch('/api/import/status')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                updateImportProgress(data);
                
                if (data.status === 'complete' || data.status === 'error' || data.status === 'stopped') {
                    clearInterval(importStatusInterval);
                    importStatusInterval = null;
                    
                    document.getElementById('btn-start-import').style.display = 'inline-block';
                    document.getElementById('btn-stop-import').style.display = 'none';
                    
                    if (data.status === 'complete') {
                        document.getElementById('import-status').textContent = 'Complete!';
                        document.getElementById('import-status').style.color = '#28a745';
                    } else if (data.status === 'error') {
                        document.getElementById('import-status').textContent = 'Error: ' + data.error_message;
                        document.getElementById('import-status').style.color = '#dc3545';
                    } else {
                        document.getElementById('import-status').textContent = 'Stopped';
                        document.getElementById('import-status').style.color = '#ffc107';
                    }
                }
            })
            .catch(function(e) {
                console.error('Status poll error:', e);
            });
        }
        
        function updateImportProgress(data) {
            document.getElementById('import-status').textContent = data.status;
            document.getElementById('import-current-file').textContent = data.current_file || '-';
            document.getElementById('import-file-progress').textContent = data.current_file_index + '/' + data.total_files;
            
            // Progress bar
            var pct = data.total_files > 0 ? Math.round((data.current_file_index / data.total_files) * 100) : 0;
            document.getElementById('import-progress-bar').style.width = pct + '%';
            document.getElementById('import-progress-pct').textContent = pct + '%';
            
            // Stats
            document.getElementById('import-total-processed').textContent = formatNum(data.total_records_processed);
            document.getElementById('import-new-records').textContent = formatNum(data.total_new_records);
            document.getElementById('import-enriched').textContent = formatNum(data.total_enriched_records);
            document.getElementById('import-rate').textContent = formatNum(data.rate_per_second) + '/s';
            
            // Category breakdown
            document.getElementById('import-big4').textContent = formatNum(data.big4_count);
            document.getElementById('import-cable').textContent = formatNum(data.cable_count);
            document.getElementById('import-gi-valid').textContent = formatNum(data.gi_valid_count);
            document.getElementById('import-gi-dead').textContent = formatNum(data.gi_dead_count);
            document.getElementById('import-gi-new').textContent = formatNum(data.gi_new_domain_count);
            
            // Update log panel
            if (data.log_messages && data.log_messages.length > 0) {
                var logDiv = document.getElementById('import-log');
                var logHtml = '';
                for (var i = 0; i < data.log_messages.length; i++) {
                    var msg = data.log_messages[i];
                    var color = '#888';
                    if (msg.indexOf('ERROR') >= 0) color = '#dc3545';
                    else if (msg.indexOf('Completed') >= 0) color = '#28a745';
                    else if (msg.indexOf('Starting') >= 0) color = '#17a2b8';
                    else if (msg.indexOf('Batch') >= 0) color = '#6c757d';
                    logHtml += '<div style="color: ' + color + ';">' + msg + '</div>';
                }
                logDiv.innerHTML = logHtml;
                logDiv.scrollTop = logDiv.scrollHeight;  // Auto-scroll to bottom
            }
        }
        
        function stopImport() {
            if (!confirm('Stop the import? Progress will be saved.')) {
                return;
            }
            
            fetch('/api/import/stop', {method: 'POST'})
            .then(function(r) { return r.json(); })
            .then(function(data) {
                document.getElementById('import-status').textContent = 'Stopping...';
            });
        }
        
        function resetImportUI() {
            document.getElementById('btn-start-import').style.display = 'inline-block';
            document.getElementById('btn-stop-import').style.display = 'none';
            if (importStatusInterval) {
                clearInterval(importStatusInterval);
                importStatusInterval = null;
            }
        }
    </script>
</body>
</html>
"""

def get_db():
    return psycopg2.connect(
        host=DATABASE['host'],
        port=DATABASE['port'],
        database=DATABASE['database'],
        user=DATABASE['user'],
        password=DATABASE['password']
    )

@app.route('/')
def dashboard():
    # Don't load stats on page load - MX tab is default, stats loaded on demand
    class Stats:
        total = 0
        big4 = 0
        cable = 0
        gi = 0
        clickers = 0
        high_quality = 0
    s = Stats()
    providers = []
    quality = []
    
    return render_template_string(DASHBOARD_HTML, stats=s, providers=providers, quality=quality)

@app.route('/api/stats')
def api_stats():
    """Return current DB counts for dashboard auto-refresh (cached)."""
    try:
        stats = get_cached_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/stats/detailed')
def api_stats_detailed():
    """Return stats from cache (instant) or calculate fresh."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Read from cache (instant!)
        cursor.execute("SELECT stat_name, stat_value, updated_at FROM stats_cache")
        rows = cursor.fetchall()
        
        stats = {}
        updated_at = None
        for name, value, ts in rows:
            stats[name] = value
            if updated_at is None or (ts and ts > updated_at):
                updated_at = ts
        
        # Add aliases and defaults
        stats['good_big4'] = stats.get('big4_good', 0)
        stats['good_cable'] = stats.get('cable_good', 0)
        stats['good_gi'] = stats.get('gi_good', 0)
        stats['good_all'] = stats.get('good_total', 0)
        if 'gi_domains' not in stats:
            stats['gi_domains'] = 0
        stats['cache_updated'] = updated_at.isoformat() if updated_at else None
        
        # All provider defaults - base stats, good, dead, quality tiers, clickers, openers, domains
        # Big4 providers
        for p in ['gmail', 'yahoo', 'outlook']:
            for suffix in ['', '_good', '_dead', '_high', '_med', '_low', '_click', '_open', '_domains']:
                if p + suffix not in stats:
                    stats[p + suffix] = 0
        
        # Cable providers
        for p in ['comcast', 'spectrum', 'centurylink', 'earthlink', 'windstream', 'optimum']:
            for suffix in ['', '_good', '_dead', '_high', '_med', '_low', '_click', '_open', '_domains']:
                if p + suffix not in stats:
                    stats[p + suffix] = 0
        
        # 2nd Level Big4 totals
        for key in ['2nd_big4_total', '2nd_big4_good', '2nd_big4_dead', '2nd_big4_clickers', '2nd_big4_openers', '2nd_big4_domains']:
            if key not in stats:
                stats[key] = 0
        
        # 2nd Level Big4 breakdown
        for p in ['google_hosted', 'microsoft_hosted', 'yahoo_hosted']:
            for suffix in ['', '_good', '_dead', '_domains', '_high', '_med', '_low', '_click', '_open']:
                if p + suffix not in stats:
                    stats[p + suffix] = 0
        
        # GI Hosting providers
        for p in ['apple', 'godaddy', '1and1', 'hostgator', 'namecheap', 'zoho', 'fastmail', 'amazonses', 'protonmail', 'cloudflare']:
            for suffix in ['', '_good', '_dead', '_domains', '_high', '_med', '_low', '_click', '_open']:
                if p + suffix not in stats:
                    stats[p + suffix] = 0
        
        # Top 10 GI Domains - read from JSON file (calculated during Recalculate)
        import json
        import os
        top_gi_file = os.path.join(os.path.dirname(__file__), 'top_gi_domains.json')
        if os.path.exists(top_gi_file):
            try:
                with open(top_gi_file, 'r') as f:
                    stats['top_gi_domains'] = json.load(f)
            except:
                stats['top_gi_domains'] = []
        else:
            stats['top_gi_domains'] = []
        
        cursor.close()
        conn.close()
        
        return jsonify(stats)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/stats/refresh', methods=['POST'])
def api_stats_refresh():
    """Recalculate and update stats cache."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Fast GROUP BY query
        cursor.execute("""
            SELECT 
                email_category,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE mx_valid = true OR mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE is_opener = true) as openers
            FROM emails
            GROUP BY email_category
        """)
        
        stats = {'total': 0, 'good_total': 0, 'dead_total': 0, 'clickers': 0, 'openers': 0}
        
        for row in cursor.fetchall():
            cat, total, good, dead, clickers, openers = row
            stats['total'] += total
            stats['good_total'] += good
            stats['dead_total'] += dead
            stats['clickers'] += clickers
            stats['openers'] += openers
            
            if cat == 'Big4_ISP':
                stats['big4_total'] = total
                stats['big4_good'] = good
                stats['big4_dead'] = dead
                stats['clickers_big4'] = clickers
                stats['openers_big4'] = openers
            elif cat == 'Cable_Provider':
                stats['cable_total'] = total
                stats['cable_good'] = good
                stats['cable_dead'] = dead
                stats['clickers_cable'] = clickers
                stats['openers_cable'] = openers
            elif cat == 'General_Internet':
                stats['gi_total'] = total
                stats['gi_good'] = good
                stats['gi_dead'] = dead
                stats['clickers_gi'] = clickers
                stats['openers_gi'] = openers
        
        # Provider breakdown (Big 4 = Gmail, Yahoo, Outlook) with all stats including domain count
        cursor.execute("""
            SELECT email_provider,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE mx_valid = true OR mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE quality_score >= 40 AND quality_score < 70) as med,
                COUNT(*) FILTER (WHERE quality_score < 40 OR quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE is_opener = true) as openers,
                COUNT(DISTINCT email_domain) as domains
            FROM emails 
            WHERE email_provider IN ('Google', 'Yahoo', 'Microsoft')
            GROUP BY email_provider
        """)
        for row in cursor.fetchall():
            provider, total, good, dead, high, med, low, clickers, openers, domains = row
            if provider == 'Google':
                stats['gmail'] = total
                stats['gmail_good'] = good
                stats['gmail_dead'] = dead
                stats['gmail_high'] = high
                stats['gmail_med'] = med
                stats['gmail_low'] = low
                stats['gmail_click'] = clickers
                stats['gmail_open'] = openers
                stats['gmail_domains'] = domains
            elif provider == 'Yahoo':
                stats['yahoo'] = total
                stats['yahoo_good'] = good
                stats['yahoo_dead'] = dead
                stats['yahoo_high'] = high
                stats['yahoo_med'] = med
                stats['yahoo_low'] = low
                stats['yahoo_click'] = clickers
                stats['yahoo_open'] = openers
                stats['yahoo_domains'] = domains
            elif provider == 'Microsoft':
                stats['outlook'] = total
                stats['outlook_good'] = good
                stats['outlook_dead'] = dead
                stats['outlook_high'] = high
                stats['outlook_med'] = med
                stats['outlook_low'] = low
                stats['outlook_click'] = clickers
                stats['outlook_open'] = openers
                stats['outlook_domains'] = domains
        
        # Cable Provider breakdown (Comcast, Spectrum/RR, CenturyLink, EarthLink, Windstream, Optimum)
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN email_domain IN ('comcast.net', 'comcast.com', 'xfinity.com') THEN 'comcast'
                    WHEN email_domain LIKE '%.rr.com' OR email_domain IN ('charter.net', 'charter.com', 'spectrum.net', 'brighthouse.com', 'rr.com', 'twc.com', 'roadrunner.com') THEN 'spectrum'
                    WHEN email_domain IN ('centurylink.net', 'centurylink.com', 'centurytel.net', 'q.com', 'qwest.net', 'qwest.com', 'embarqmail.com', 'qwestoffice.net', 'uswest.net') THEN 'centurylink'
                    WHEN email_domain IN ('earthlink.net', 'earthlink.com', 'mindspring.com') THEN 'earthlink'
                    WHEN email_domain IN ('windstream.net', 'windstream.com') THEN 'windstream'
                    WHEN email_domain IN ('optonline.net', 'optimum.net', 'cablevision.com') THEN 'optimum'
                END as provider,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE mx_valid = true OR mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE quality_score >= 40 AND quality_score < 70) as med,
                COUNT(*) FILTER (WHERE quality_score < 40 OR quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE is_opener = true) as openers,
                COUNT(DISTINCT email_domain) as domains
            FROM emails 
            WHERE email_category = 'Cable_Provider'
              AND (
                  email_domain IN ('comcast.net', 'comcast.com', 'xfinity.com',
                                   'charter.net', 'charter.com', 'spectrum.net', 'brighthouse.com', 'rr.com', 'twc.com', 'roadrunner.com',
                                   'centurylink.net', 'centurylink.com', 'centurytel.net', 'q.com', 'qwest.net', 'qwest.com', 'embarqmail.com', 'qwestoffice.net', 'uswest.net',
                                   'earthlink.net', 'earthlink.com', 'mindspring.com',
                                   'windstream.net', 'windstream.com',
                                   'optonline.net', 'optimum.net', 'cablevision.com')
                  OR email_domain LIKE '%.rr.com'
              )
            GROUP BY 1
        """)
        for row in cursor.fetchall():
            if row[0]:
                provider = row[0]
                stats[provider] = row[1]
                stats[f'{provider}_good'] = row[2]
                stats[f'{provider}_dead'] = row[3]
                stats[f'{provider}_high'] = row[4]
                stats[f'{provider}_med'] = row[5]
                stats[f'{provider}_low'] = row[6]
                stats[f'{provider}_click'] = row[7]
                stats[f'{provider}_open'] = row[8]
                stats[f'{provider}_domains'] = row[9]
        
        # Apple (iCloud/me.com/mac.com) with all stats
        cursor.execute("""
            SELECT COUNT(*) as total,
                COUNT(*) FILTER (WHERE mx_valid = true OR mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE quality_score >= 40 AND quality_score < 70) as med,
                COUNT(*) FILTER (WHERE quality_score < 40 OR quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE is_opener = true) as openers
            FROM emails WHERE email_domain IN ('icloud.com', 'me.com', 'mac.com')
        """)
        apple = cursor.fetchone()
        stats['apple'] = apple[0]
        stats['apple_good'] = apple[1]
        stats['apple_dead'] = apple[2]
        stats['apple_domains'] = 3
        stats['apple_high'] = apple[3]
        stats['apple_med'] = apple[4]
        stats['apple_low'] = apple[5]
        stats['apple_click'] = apple[6]
        stats['apple_open'] = apple[7]
        
        # 2nd Level Big4: GI domains hosted on Google/Microsoft/Yahoo MX
        # Join emails with domain_mx to get counts based on mx_category
        cursor.execute("""
            SELECT 
                dm.mx_category,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE e.mx_valid = true OR e.mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE e.mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE e.quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE e.quality_score >= 40 AND e.quality_score < 70) as med,
                COUNT(*) FILTER (WHERE e.quality_score < 40 OR e.quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE e.is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE e.is_opener = true) as openers,
                COUNT(DISTINCT e.email_domain) as domains
            FROM emails e
            JOIN domain_mx dm ON e.email_domain = dm.domain
            WHERE e.email_category = 'General_Internet'
              AND dm.mx_category IN ('Google', 'Microsoft', 'Yahoo')
            GROUP BY dm.mx_category
        """)
        
        # Initialize 2nd level totals
        stats['2nd_big4_total'] = 0
        stats['2nd_big4_good'] = 0
        stats['2nd_big4_dead'] = 0
        stats['2nd_big4_clickers'] = 0
        stats['2nd_big4_openers'] = 0
        stats['2nd_big4_domains'] = 0
        
        for row in cursor.fetchall():
            mx_cat, total, good, dead, high, med, low, clickers, openers, domains = row
            stats['2nd_big4_total'] += total
            stats['2nd_big4_good'] += good
            stats['2nd_big4_dead'] += dead
            stats['2nd_big4_clickers'] += clickers
            stats['2nd_big4_openers'] += openers
            stats['2nd_big4_domains'] += domains
            
            if mx_cat == 'Google':
                stats['google_hosted'] = total
                stats['google_hosted_good'] = good
                stats['google_hosted_dead'] = dead
                stats['google_hosted_domains'] = domains
                stats['google_hosted_high'] = high
                stats['google_hosted_med'] = med
                stats['google_hosted_low'] = low
                stats['google_hosted_click'] = clickers
                stats['google_hosted_open'] = openers
            elif mx_cat == 'Microsoft':
                stats['microsoft_hosted'] = total
                stats['microsoft_hosted_good'] = good
                stats['microsoft_hosted_dead'] = dead
                stats['microsoft_hosted_domains'] = domains
                stats['microsoft_hosted_high'] = high
                stats['microsoft_hosted_med'] = med
                stats['microsoft_hosted_low'] = low
                stats['microsoft_hosted_click'] = clickers
                stats['microsoft_hosted_open'] = openers
            elif mx_cat == 'Yahoo':
                stats['yahoo_hosted'] = total
                stats['yahoo_hosted_good'] = good
                stats['yahoo_hosted_dead'] = dead
                stats['yahoo_hosted_domains'] = domains
                stats['yahoo_hosted_high'] = high
                stats['yahoo_hosted_med'] = med
                stats['yahoo_hosted_low'] = low
                stats['yahoo_hosted_click'] = clickers
                stats['yahoo_hosted_open'] = openers
        
        # GI Hosting providers breakdown (by mx_category from domain_mx)
        cursor.execute("""
            SELECT 
                dm.mx_category,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE e.mx_valid = true OR e.mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE e.mx_valid = false) as dead,
                COUNT(DISTINCT e.email_domain) as domains,
                COUNT(*) FILTER (WHERE e.quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE e.quality_score >= 40 AND e.quality_score < 70) as med,
                COUNT(*) FILTER (WHERE e.quality_score < 40 OR e.quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE e.is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE e.is_opener = true) as openers
            FROM emails e
            JOIN domain_mx dm ON e.email_domain = dm.domain
            WHERE e.email_category = 'General_Internet'
              AND dm.mx_category IN ('Fastmail', 'ProtonMail', 'Amazon')
            GROUP BY dm.mx_category
        """)
        
        provider_map = {
            'Fastmail': 'fastmail',
            'ProtonMail': 'protonmail',
            'Amazon': 'amazonses',
        }
        
        for row in cursor.fetchall():
            mx_cat, total, good, dead, domains, high, med, low, clickers, openers = row
            if mx_cat in provider_map:
                key = provider_map[mx_cat]
                stats[key] = total
                stats[key + '_good'] = good
                stats[key + '_dead'] = dead
                stats[key + '_domains'] = domains
                stats[key + '_high'] = high
                stats[key + '_med'] = med
                stats[key + '_low'] = low
                stats[key + '_click'] = clickers
                stats[key + '_open'] = openers
        
        # GoDaddy, 1&1, HostGator, Namecheap, Zoho, Cloudflare - check mx_primary patterns
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN dm.mx_primary ILIKE '%secureserver.net%' THEN 'godaddy'
                    WHEN dm.mx_primary ILIKE '%ionos%' OR dm.mx_primary ILIKE '%1and1%' THEN '1and1'
                    WHEN dm.mx_primary ILIKE '%hostgator%' OR dm.mx_primary ILIKE '%websitewelcome%' THEN 'hostgator'
                    WHEN dm.mx_primary ILIKE '%privateemail.com%' OR dm.mx_primary ILIKE '%registrar-servers%' THEN 'namecheap'
                    WHEN dm.mx_primary ILIKE '%zoho%' THEN 'zoho'
                    WHEN dm.mx_primary ILIKE '%cloudflare.net%' THEN 'cloudflare'
                    ELSE 'other'
                END as provider,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE e.mx_valid = true OR e.mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE e.mx_valid = false) as dead,
                COUNT(DISTINCT e.email_domain) as domains,
                COUNT(*) FILTER (WHERE e.quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE e.quality_score >= 40 AND e.quality_score < 70) as med,
                COUNT(*) FILTER (WHERE e.quality_score < 40 OR e.quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE e.is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE e.is_opener = true) as openers
            FROM emails e
            JOIN domain_mx dm ON e.email_domain = dm.domain
            WHERE e.email_category = 'General_Internet'
              AND dm.mx_category IN ('Real_GI', 'General_Internet')
              AND (
                  dm.mx_primary ILIKE '%secureserver.net%' OR
                  dm.mx_primary ILIKE '%ionos%' OR dm.mx_primary ILIKE '%1and1%' OR
                  dm.mx_primary ILIKE '%hostgator%' OR dm.mx_primary ILIKE '%websitewelcome%' OR
                  dm.mx_primary ILIKE '%privateemail.com%' OR dm.mx_primary ILIKE '%registrar-servers%' OR
                  dm.mx_primary ILIKE '%zoho%' OR
                  dm.mx_primary ILIKE '%cloudflare.net%'
              )
            GROUP BY 1
        """)
        
        for row in cursor.fetchall():
            provider, total, good, dead, domains, high, med, low, clickers, openers = row
            if provider != 'other':
                stats[provider] = total
                stats[f'{provider}_good'] = good
                stats[f'{provider}_dead'] = dead
                stats[f'{provider}_domains'] = domains
                stats[f'{provider}_high'] = high
                stats[f'{provider}_med'] = med
                stats[f'{provider}_low'] = low
                stats[f'{provider}_click'] = clickers
                stats[f'{provider}_open'] = openers
        
        # GI Unique Domains count
        cursor.execute("""
            SELECT COUNT(DISTINCT email_domain) FROM emails WHERE email_category = 'General_Internet'
        """)
        stats['gi_domains'] = cursor.fetchone()[0]
        
        # Top 10 GI Domains (calculated during recalculate, stored as JSON)
        cursor.execute("""
            SELECT 
                email_domain,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE mx_valid = true OR mx_valid IS NULL) as good,
                COUNT(*) FILTER (WHERE mx_valid = false) as dead,
                COUNT(*) FILTER (WHERE quality_score >= 70) as high,
                COUNT(*) FILTER (WHERE quality_score >= 40 AND quality_score < 70) as med,
                COUNT(*) FILTER (WHERE quality_score < 40 OR quality_score IS NULL) as low,
                COUNT(*) FILTER (WHERE is_clicker = true) as clickers,
                COUNT(*) FILTER (WHERE is_opener = true) as openers
            FROM emails 
            WHERE email_category = 'General_Internet'
              AND email_domain NOT IN ('icloud.com', 'me.com', 'mac.com')
            GROUP BY email_domain
            ORDER BY total DESC
            LIMIT 10
        """)
        top_gi = []
        for row in cursor.fetchall():
            top_gi.append({
                'domain': row[0],
                'total': row[1],
                'good': row[2],
                'dead': row[3],
                'high': row[4],
                'med': row[5],
                'low': row[6],
                'click': row[7],
                'open': row[8]
            })
        
        # Update cache
        for name, value in stats.items():
            cursor.execute("""
                INSERT INTO stats_cache (stat_name, stat_value, updated_at) VALUES (%s, %s, NOW())
                ON CONFLICT (stat_name) DO UPDATE SET stat_value = EXCLUDED.stat_value, updated_at = NOW()
            """, (name, value))
        
        # Store top_gi_domains as JSON file
        import json
        import os
        top_gi_json = json.dumps(top_gi)
        top_gi_file = os.path.join(os.path.dirname(__file__), 'top_gi_domains.json')
        with open(top_gi_file, 'w') as f:
            f.write(top_gi_json)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'stats_updated': len(stats), 'top_gi_calculated': len(top_gi)})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/query')
def api_query():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Build WHERE clause with all filter parameters
        where_parts = ["1=1"]
        params = []
        
        # Basic filters
        if request.args.get('email_search'):
            email_search = request.args.get('email_search').strip().lower()
            if email_search:
                where_parts.append("LOWER(email) LIKE %s")
                params.append(f"%{email_search}%")
        if request.args.get('provider'):
            where_parts.append("email_provider = %s")
            params.append(request.args.get('provider'))
        if request.args.get('category'):
            where_parts.append("email_category = %s")
            params.append(request.args.get('category'))
        if request.args.get('domain'):
            domain = request.args.get('domain').strip().lower()
            if domain:
                where_parts.append("LOWER(email_domain) LIKE %s")
                params.append(f"%{domain}%")
        if request.args.get('state'):
            where_parts.append("UPPER(state) = %s")
            params.append(request.args.get('state').upper())
        
        # Personal info filters
        if request.args.get('first_name'):
            where_parts.append("LOWER(first_name) LIKE %s")
            params.append(f"%{request.args.get('first_name').strip().lower()}%")
        if request.args.get('last_name'):
            where_parts.append("LOWER(last_name) LIKE %s")
            params.append(f"%{request.args.get('last_name').strip().lower()}%")
        if request.args.get('city'):
            where_parts.append("LOWER(city) LIKE %s")
            params.append(f"%{request.args.get('city').strip().lower()}%")
        if request.args.get('zipcode'):
            where_parts.append("zipcode LIKE %s")
            params.append(f"{request.args.get('zipcode').strip()}%")
        if request.args.get('gender'):
            g = request.args.get('gender')
            if g == 'M':
                where_parts.append("(gender = 'M' OR UPPER(gender) = 'MALE')")
            elif g == 'F':
                where_parts.append("(gender = 'F' OR UPPER(gender) = 'FEMALE')")
            else:
                where_parts.append("gender = %s")
                params.append(g)
        if request.args.get('country'):
            where_parts.append("country = %s")
            params.append(request.args.get('country'))
        if request.args.get('has_phone') == 'true':
            where_parts.append("phone IS NOT NULL AND phone != ''")
        elif request.args.get('has_phone') == 'false':
            where_parts.append("(phone IS NULL OR phone = '')")
        if request.args.get('has_dob') == 'true':
            where_parts.append("dob IS NOT NULL")
        elif request.args.get('has_dob') == 'false':
            where_parts.append("dob IS NULL")
        
        # Engagement filters
        # Clickers/Openers - use OR logic when both are "true"
        clickers_val = request.args.get('clickers')
        openers_val = request.args.get('openers')
        if clickers_val == 'true' and openers_val == 'true':
            where_parts.append("(is_clicker = true OR is_opener = true)")
        else:
            if clickers_val == 'true':
                where_parts.append("is_clicker = true")
            elif clickers_val == 'false':
                where_parts.append("is_clicker = false")
            if openers_val == 'true':
                where_parts.append("is_opener = true")
            elif openers_val == 'false':
                where_parts.append("is_opener = false")
        # Quality tier filter (high: 70-100, mid: 40-69, low: 0-39)
        quality_tier = request.args.get('quality_tier')
        if quality_tier == 'high':
            where_parts.append("quality_score >= 70")
        elif quality_tier == 'mid':
            where_parts.append("quality_score >= 40 AND quality_score < 70")
        elif quality_tier == 'low':
            where_parts.append("quality_score < 40")
        if request.args.get('validation_status'):
            where_parts.append("validation_status = %s")
            params.append(request.args.get('validation_status'))
        
        # Source tracking filters
        if request.args.get('data_source'):
            where_parts.append("data_source = %s")
            params.append(request.args.get('data_source'))
        if request.args.get('file_source'):
            where_parts.append("%s = ANY(file_sources)")
            params.append(request.args.get('file_source'))
        if request.args.get('signup_domain'):
            where_parts.append("LOWER(signup_domain) LIKE %s")
            params.append(f"%{request.args.get('signup_domain').strip().lower()}%")
        if request.args.get('signup_ip'):
            where_parts.append("signup_ip LIKE %s")
            params.append(f"{request.args.get('signup_ip').strip()}%")
        if request.args.get('signup_date_from'):
            where_parts.append("signup_date >= %s")
            params.append(request.args.get('signup_date_from'))
        if request.args.get('signup_date_to'):
            where_parts.append("signup_date <= %s")
            params.append(request.args.get('signup_date_to'))
        
        where_clause = " AND ".join(where_parts)
        
        # Get total count for pagination
        count_sql = f"SELECT COUNT(*) FROM emails WHERE {where_clause}"
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]
        
        # Get paginated results with more columns
        limit = min(int(request.args.get('limit', 500)), 5000)
        offset = int(request.args.get('offset', 0))
        
        sql = f"""SELECT email, email_domain, email_provider, email_brand, email_category, 
                         quality_score, is_clicker, is_opener, first_name, last_name, 
                         city, state, zipcode, phone, gender,
                         CAST(dob AS TEXT) as dob,
                         CAST(signup_date AS TEXT) as signup_date, 
                         data_source
                  FROM emails WHERE {where_clause}
                  ORDER BY quality_score DESC NULLS LAST
                  LIMIT {limit} OFFSET {offset}"""
        
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'columns': columns,
            'rows': [[str(c) if c is not None else None for c in row] for row in rows],
            'count': len(rows),
            'total_count': total_count
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/sql', methods=['POST'])
def api_sql():
    try:
        data = request.get_json()
        sql = data.get('sql', '').strip()
        
        # Basic security - only allow SELECT
        if not sql.upper().startswith('SELECT'):
            return jsonify({'error': 'Only SELECT queries allowed'})
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(sql)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(500)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'columns': columns,
            'rows': [[str(c) if c is not None else None for c in row] for row in rows],
            'count': len(rows)
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/export')
def api_export():
    from flask import Response
    import csv
    import io
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Build WHERE clause with all filter parameters (same as /api/query)
        where_parts = ["1=1"]
        params = []
        
        # Basic filters
        if request.args.get('email_search'):
            email_search = request.args.get('email_search').strip().lower()
            if email_search:
                where_parts.append("LOWER(email) LIKE %s")
                params.append(f"%{email_search}%")
        if request.args.get('provider'):
            where_parts.append("email_provider = %s")
            params.append(request.args.get('provider'))
        if request.args.get('category'):
            where_parts.append("email_category = %s")
            params.append(request.args.get('category'))
        if request.args.get('domain'):
            domain = request.args.get('domain').strip().lower()
            if domain:
                where_parts.append("LOWER(email_domain) LIKE %s")
                params.append(f"%{domain}%")
        if request.args.get('state'):
            where_parts.append("UPPER(state) = %s")
            params.append(request.args.get('state').upper())
        
        # Personal info filters
        if request.args.get('first_name'):
            where_parts.append("LOWER(first_name) LIKE %s")
            params.append(f"%{request.args.get('first_name').strip().lower()}%")
        if request.args.get('last_name'):
            where_parts.append("LOWER(last_name) LIKE %s")
            params.append(f"%{request.args.get('last_name').strip().lower()}%")
        if request.args.get('city'):
            where_parts.append("LOWER(city) LIKE %s")
            params.append(f"%{request.args.get('city').strip().lower()}%")
        if request.args.get('zipcode'):
            where_parts.append("zipcode LIKE %s")
            params.append(f"{request.args.get('zipcode').strip()}%")
        if request.args.get('gender'):
            g = request.args.get('gender')
            if g == 'M':
                where_parts.append("(gender = 'M' OR UPPER(gender) = 'MALE')")
            elif g == 'F':
                where_parts.append("(gender = 'F' OR UPPER(gender) = 'FEMALE')")
            else:
                where_parts.append("gender = %s")
                params.append(g)
        if request.args.get('country'):
            where_parts.append("country = %s")
            params.append(request.args.get('country'))
        if request.args.get('has_phone') == 'true':
            where_parts.append("phone IS NOT NULL AND phone != ''")
        elif request.args.get('has_phone') == 'false':
            where_parts.append("(phone IS NULL OR phone = '')")
        if request.args.get('has_dob') == 'true':
            where_parts.append("dob IS NOT NULL")
        elif request.args.get('has_dob') == 'false':
            where_parts.append("dob IS NULL")
        
        # Engagement filters
        # Clickers/Openers - use OR logic when both are "true"
        clickers_val = request.args.get('clickers')
        openers_val = request.args.get('openers')
        if clickers_val == 'true' and openers_val == 'true':
            where_parts.append("(is_clicker = true OR is_opener = true)")
        else:
            if clickers_val == 'true':
                where_parts.append("is_clicker = true")
            elif clickers_val == 'false':
                where_parts.append("is_clicker = false")
            if openers_val == 'true':
                where_parts.append("is_opener = true")
            elif openers_val == 'false':
                where_parts.append("is_opener = false")
        # Quality tier filter (high: 70-100, mid: 40-69, low: 0-39)
        quality_tier = request.args.get('quality_tier')
        if quality_tier == 'high':
            where_parts.append("quality_score >= 70")
        elif quality_tier == 'mid':
            where_parts.append("quality_score >= 40 AND quality_score < 70")
        elif quality_tier == 'low':
            where_parts.append("quality_score < 40")
        if request.args.get('validation_status'):
            where_parts.append("validation_status = %s")
            params.append(request.args.get('validation_status'))
        
        # Source tracking filters
        if request.args.get('data_source'):
            where_parts.append("data_source = %s")
            params.append(request.args.get('data_source'))
        if request.args.get('file_source'):
            where_parts.append("%s = ANY(file_sources)")
            params.append(request.args.get('file_source'))
        if request.args.get('signup_domain'):
            where_parts.append("LOWER(signup_domain) LIKE %s")
            params.append(f"%{request.args.get('signup_domain').strip().lower()}%")
        if request.args.get('signup_ip'):
            where_parts.append("signup_ip LIKE %s")
            params.append(f"{request.args.get('signup_ip').strip()}%")
        if request.args.get('signup_date_from'):
            where_parts.append("signup_date >= %s")
            params.append(request.args.get('signup_date_from'))
        if request.args.get('signup_date_to'):
            where_parts.append("signup_date <= %s")
            params.append(request.args.get('signup_date_to'))
        
        where_clause = " AND ".join(where_parts)
        limit = min(int(request.args.get('limit', 50000)), 100000)
        
        sql = f"""SELECT email, email_domain, email_provider, email_brand, email_category, 
                         quality_score, is_clicker, is_opener, first_name, last_name,
                         phone, city, state, zipcode, gender, 
                         CAST(dob AS TEXT) as dob,
                         CAST(signup_date AS TEXT) as signup_date, 
                         CAST(created_at AS TEXT) as created_at,
                         data_source
                  FROM emails WHERE {where_clause}
                  ORDER BY quality_score DESC NULLS LAST
                  LIMIT {limit}"""
        
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=email_export.csv'}
        )
    except Exception as e:
        return str(e), 500


# =============================================================================
# MX VALIDATOR API ENDPOINTS
# =============================================================================

@app.route('/api/mx/start', methods=['POST'])
def api_mx_start():
    """Start the MX validation process."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available. Install dnspython: pip install dnspython'}), 500
    
    try:
        state = mv.get_state()
        if state.status == 'running':
            return jsonify({'error': 'Validation already running'}), 400
        
        workers = request.json.get('workers', 32) if request.json else 32
        resume = request.json.get('resume', True) if request.json else True
        
        mv.start_validation_async(workers=workers, resume=resume)
        time.sleep(0.5)  # Give it time to start
        
        return jsonify({'status': 'started', 'workers': workers, 'resume': resume})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/reset-dead-and-start', methods=['POST'])
def api_mx_reset_dead_and_start():
    """Reset all dead domains to unchecked, then start the MX scan."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available'}), 500
    
    try:
        state = mv.get_state()
        if state.status == 'running':
            return jsonify({'error': 'Validation already running'}), 400
        
        workers = request.json.get('workers', 32) if request.json else 32
        reset_count = mv.reset_dead_domains()
        mv.start_validation_async(workers=workers, resume=True)
        time.sleep(0.5)
        return jsonify({'status': 'started', 'workers': workers, 'reset_dead': reset_count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# DOMAIN CONFIG API ENDPOINTS
# =============================================================================

@app.route('/api/config/domains')
def api_config_domains():
    """Get all Big4 and Cable domain mappings from config."""
    try:
        from config import DOMAIN_MAPPING
        
        big4 = []
        cable = []
        
        for domain, info in sorted(DOMAIN_MAPPING.items()):
            provider, brand, category = info
            entry = {'domain': domain, 'provider': provider, 'brand': brand}
            if category == 'Big4_ISP':
                big4.append(entry)
            elif category == 'Cable_Provider':
                cable.append(entry)
        
        return jsonify({'big4': big4, 'cable': cable})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config/domains/add', methods=['POST'])
def api_config_domains_add():
    """Add a new domain to the config file."""
    try:
        data = request.get_json()
        domain = data.get('domain', '').strip().lower()
        provider = data.get('provider', '').strip()
        brand = data.get('brand', '').strip()
        category = data.get('category', 'Cable_Provider')
        
        if not domain or not provider or not brand:
            return jsonify({'error': 'Missing required fields'}), 400
        
        if category not in ('Big4_ISP', 'Cable_Provider'):
            return jsonify({'error': 'Invalid category'}), 400
        
        # Read the config file
        import os
        config_path = os.path.join(os.path.dirname(__file__), 'config.py')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if domain already exists
        if f"'{domain}':" in content or f'"{domain}":' in content:
            return jsonify({'error': f'Domain {domain} already exists in config'}), 400
        
        # Find where to insert the new domain (before the closing brace of DOMAIN_MAPPING)
        # We'll add it at the end of the appropriate section
        if category == 'Big4_ISP':
            # Add before "# ========" line that starts Cable section
            marker = "    # =========================================================================\n    # CABLE PROVIDERS"
            new_entry = f"    '{domain}': ('{provider}', '{brand}', 'Big4_ISP'),\n\n"
            content = content.replace(marker, new_entry + marker)
        else:
            # Add before the closing brace of DOMAIN_MAPPING
            marker = "}\n\n# =============================================================================\n# BACKWARD COMPATIBILITY"
            new_entry = f"    '{domain}': ('{provider}', '{brand}', 'Cable_Provider'),\n"
            content = content.replace(marker, new_entry + marker)
        
        # Write back
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Reload the config module
        import importlib
        import config
        importlib.reload(config)
        
        return jsonify({'status': 'ok', 'domain': domain, 'category': category})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/detailed-counts')
def api_mx_detailed_counts():
    """Get detailed counts: each Big4 domain, each Cable domain, and total GI."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Big4 domains - count each individually
        big4_domains = [
            'gmail.com', 'googlemail.com',
            'hotmail.com', 'outlook.com', 'live.com', 'msn.com',
            'yahoo.com', 'ymail.com', 'rocketmail.com',
            'aol.com', 'aim.com',
            'verizon.net', 'att.net', 'sbcglobal.net', 'bellsouth.net'
        ]
        big4_counts = []
        for domain in big4_domains:
            cursor.execute("SELECT COUNT(*) FROM emails WHERE email_domain = %s", (domain,))
            count = cursor.fetchone()[0]
            if count > 0:
                big4_counts.append({'domain': domain, 'count': count})
        big4_counts.sort(key=lambda x: -x['count'])
        
        # Cable domains
        cable_domains = [
            'comcast.net', 'xfinity.com',
            'charter.net', 'spectrum.net',
            'cox.net',
            'optimum.net', 'optonline.net',
            'twc.com', 'roadrunner.com',
            'earthlink.net',
            'centurylink.net', 'centurytel.net',
            'windstream.net',
            'mediacombb.net'
        ]
        cable_counts = []
        for domain in cable_domains:
            cursor.execute("SELECT COUNT(*) FROM emails WHERE email_domain = %s", (domain,))
            count = cursor.fetchone()[0]
            if count > 0:
                cable_counts.append({'domain': domain, 'count': count})
        # Also check .rr.com domains (Roadrunner regional)
        cursor.execute("SELECT COUNT(*) FROM emails WHERE email_domain LIKE '%%.rr.com'")
        rr_count = cursor.fetchone()[0]
        if rr_count > 0:
            cable_counts.append({'domain': '*.rr.com (Roadrunner)', 'count': rr_count})
        cable_counts.sort(key=lambda x: -x['count'])
        
        # Totals by category
        cursor.execute("SELECT COUNT(*) FROM emails WHERE email_category = 'Big4_ISP'")
        total_big4 = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM emails WHERE email_category = 'Cable_Provider'")
        total_cable = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM emails WHERE email_category = 'General_Internet'")
        total_gi = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM emails")
        total_all = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'big4': {'domains': big4_counts, 'total': total_big4},
            'cable': {'domains': cable_counts, 'total': total_cable},
            'general_internet': {'total': total_gi},
            'all_emails': total_all
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/domain-counts')
def api_mx_domain_counts():
    """Get domain counts for debugging."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Emails table by category
        cursor.execute("""
            SELECT email_category, COUNT(DISTINCT email_domain), COUNT(*) 
            FROM emails WHERE email_domain IS NOT NULL 
            GROUP BY email_category ORDER BY 3 DESC
        """)
        emails_by_cat = [{'category': r[0] or 'NULL', 'domains': r[1], 'emails': r[2]} for r in cursor.fetchall()]
        
        # domain_mx counts
        cursor.execute("SELECT COUNT(*) FROM domain_mx")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE checked_at IS NOT NULL")
        checked = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE checked_at IS NULL")
        unchecked = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
        gi_true = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true AND checked_at IS NULL")
        gi_unchecked = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
        dead = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'emails_by_category': emails_by_cat,
            'domain_mx': {
                'total': total,
                'checked': checked,
                'unchecked': unchecked,
                'is_gi_true': gi_true,
                'gi_unchecked': gi_unchecked,
                'dead': dead
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/reset-dead', methods=['POST'])
def api_mx_reset_dead():
    """Reset all dead domains to unchecked. Does NOT start scan."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
        count = cursor.fetchone()[0]
        if count > 0:
            # Simple reset: clear checked status so they get rescanned
            cursor.execute("""
                UPDATE domain_mx SET
                    checked_at = NULL,
                    mx_primary = NULL,
                    mx_records = NULL,
                    mx_priority = NULL,
                    mx_category = NULL,
                    mx_host_provider = NULL,
                    is_valid = true,
                    error_message = NULL,
                    dns_server = NULL
                WHERE is_valid = false
            """)
            conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'status': 'ok', 'reset': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/full-reset', methods=['POST'])
def api_mx_full_reset():
    """
    FULL RESET:
    1. Set is_gi=false on ALL domain_mx rows first
    2. Set is_gi=true ONLY on domains that are General_Internet in emails table
    3. Reset ALL dead domains to unchecked
    4. Reset ALL checked GI domains to unchecked (fresh scan)
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Step 1: Clear is_gi on everything
        cursor.execute("UPDATE domain_mx SET is_gi = false")
        cleared = cursor.rowcount
        conn.commit()
        
        # Step 2: Set is_gi=true ONLY for General_Internet domains from emails table
        cursor.execute("""
            UPDATE domain_mx SET is_gi = true
            WHERE domain IN (
                SELECT DISTINCT email_domain FROM emails
                WHERE email_category = 'General_Internet' 
                  AND email_domain IS NOT NULL AND email_domain != ''
            )
        """)
        gi_marked = cursor.rowcount
        conn.commit()
        
        # Step 3: Reset ALL domains with is_gi=true to unchecked (fresh scan)
        cursor.execute("""
            UPDATE domain_mx SET
                checked_at = NULL,
                mx_primary = NULL,
                mx_records = NULL,
                mx_priority = NULL,
                mx_category = NULL,
                mx_host_provider = NULL,
                is_valid = true,
                error_message = NULL,
                dns_server = NULL
            WHERE is_gi = true
        """)
        reset_for_scan = cursor.rowcount
        conn.commit()
        
        # Count final state
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
        total_gi = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true AND checked_at IS NULL")
        unchecked_gi = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
        dead = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'cleared_is_gi': cleared,
            'marked_as_gi': gi_marked,
            'reset_for_fresh_scan': reset_for_scan,
            'final_gi_count': total_gi,
            'unchecked_gi': unchecked_gi,
            'dead_count': dead
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/sync-gi', methods=['POST'])
def api_mx_sync_gi():
    """
    SYNC GI FLAG (preserves scan progress):
    1. Find all domains in emails table that are General_Internet
    2. Add missing domains to domain_mx
    3. Set is_gi=true for all GI domains (without clearing existing scans)
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Get count before
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
        gi_before = cursor.fetchone()[0]
        
        # Step 1: Insert any missing GI domains into domain_mx
        cursor.execute("""
            INSERT INTO domain_mx (domain, email_count, is_gi)
            SELECT e.email_domain, COUNT(*), true
            FROM emails e
            WHERE e.email_category = 'General_Internet'
              AND e.email_domain IS NOT NULL AND e.email_domain != ''
              AND NOT EXISTS (SELECT 1 FROM domain_mx d WHERE d.domain = e.email_domain)
            GROUP BY e.email_domain
        """)
        inserted = cursor.rowcount
        conn.commit()
        
        # Step 2: Update is_gi=true for existing GI domains that aren't flagged
        cursor.execute("""
            UPDATE domain_mx SET is_gi = true
            WHERE is_gi = false
              AND domain IN (
                SELECT DISTINCT email_domain FROM emails
                WHERE email_category = 'General_Internet' 
                  AND email_domain IS NOT NULL AND email_domain != ''
            )
        """)
        updated = cursor.rowcount
        conn.commit()
        
        # Get count after
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true")
        gi_after = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_gi = true AND checked_at IS NULL")
        unchecked = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'gi_before': gi_before,
            'gi_after': gi_after,
            'domains_inserted': inserted,
            'domains_flagged': updated,
            'unchecked_to_scan': unchecked
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/pause', methods=['POST'])
def api_mx_pause():
    """Pause the MX validation process."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available'}), 500
    
    try:
        state = mv.get_state()
        if state.status == 'paused':
            mv.resume_validation()
            return jsonify({'status': 'resumed'})
        else:
            mv.pause_validation()
            return jsonify({'status': 'paused'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/stop', methods=['POST'])
def api_mx_stop():
    """Stop the MX validation process."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available'}), 500
    
    try:
        mv.stop_validation()
        return jsonify({'status': 'stopped'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/status')
def api_mx_status():
    """Get the current MX validation status."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available'}), 500
    
    try:
        state = mv.get_state()
        return jsonify({
            'status': state.status,
            'total': state.total_domains,
            'checked': state.checked,
            'valid': state.valid,
            'dead': state.dead,
            'valid_emails': getattr(state, 'valid_emails', 0),
            'dead_emails': getattr(state, 'dead_emails', 0),
            'errors': state.errors,
            'rate': round(state.rate, 1),
            'categories': state.categories
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/stream')
def api_mx_stream():
    """SSE endpoint for real-time MX validation logs."""
    mv = get_mx_validator()
    if not mv:
        def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': 'MX Validator not available'})}\n\n"
        return Response(error_stream(), mimetype='text/event-stream')
    
    return Response(
        mv.get_log_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/api/mx/apply', methods=['POST'])
def api_mx_apply():
    """Apply MX categories to emails table."""
    mv = get_mx_validator()
    if not mv:
        return jsonify({'error': 'MX Validator not available'}), 500
    
    try:
        updated = mv.update_emails_from_mx()
        return jsonify({'status': 'success', 'updated': updated})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/mx/dead-domains')
def api_mx_dead_domains():
    """Get list of dead domains for spot checking."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Check if domain_mx exists (may not exist before first backfill)
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'domain_mx'
            )
        """)
        if not cursor.fetchone()[0]:
            cursor.close()
            conn.close()
            return jsonify({'domains': [], 'count': 0, 'error': 'domain_mx table not found. Run mx_domain_ops.py --backfill first.'})
        
        cursor.execute("""
            SELECT domain, error_message, email_count, checked_at
            FROM domain_mx
            WHERE is_valid = false
            ORDER BY email_count DESC
            LIMIT 500
        """)
        
        domains = []
        for row in cursor.fetchall():
            domains.append({
                'domain': row[0],
                'error_message': row[1] if row[1] else None,
                'email_count': int(row[2]) if row[2] is not None else 0,
                'checked_at': str(row[3]) if row[3] else None
            })
        
        cursor.execute("SELECT COUNT(*) FROM domain_mx WHERE is_valid = false")
        total_dead = int(cursor.fetchone()[0])
        
        cursor.close()
        conn.close()
        
        return jsonify({'domains': domains, 'count': total_dead})
    except Exception as e:
        return jsonify({'error': str(e), 'domains': [], 'count': 0}), 500


@app.route('/api/mx/category-email-counts')
def api_mx_category_email_counts():
    """Get SUM(email_count) per mx_category from domain_mx (emails per category)."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'domain_mx'
            )
        """)
        if not cursor.fetchone()[0]:
            cursor.close()
            conn.close()
            return jsonify({'categories': {}, 'error': 'domain_mx table not found'})
        cursor.execute("""
            SELECT mx_category, COALESCE(SUM(email_count), 0)
            FROM domain_mx
            WHERE mx_category IS NOT NULL
            GROUP BY mx_category
        """)
        categories = {}
        for row in cursor.fetchall():
            categories[row[0]] = int(row[1])
        cursor.close()
        conn.close()
        return jsonify({'categories': categories})
    except Exception as e:
        return jsonify({'categories': {}, 'error': str(e)})


@app.route('/api/mx/dns-stats')
def api_mx_dns_stats():
    """Get DNS server valid/dead counts from domain_mx (persisted, survives restart)."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'domain_mx' AND column_name = 'dns_server'
            )
        """)
        if not cursor.fetchone()[0]:
            cursor.close()
            conn.close()
            return jsonify({'servers': {}})
        
        cursor.execute("""
            SELECT dns_server, is_valid, COUNT(*)
            FROM domain_mx
            WHERE dns_server IS NOT NULL AND dns_server != ''
            GROUP BY dns_server, is_valid
        """)
        
        servers = {}
        for row in cursor.fetchall():
            name, is_valid, count = row[0], row[1], int(row[2])
            if name not in servers:
                servers[name] = {'valid': 0, 'dead': 0}
            if is_valid:
                servers[name]['valid'] = count
            else:
                servers[name]['dead'] = count
        
        cursor.close()
        conn.close()
        return jsonify({'servers': servers})
    except Exception as e:
        return jsonify({'servers': {}, 'error': str(e)})


# =============================================================================
# IMPORT DATA API ENDPOINTS
# =============================================================================

# Lazy load importer module
_importer = None
_import_thread = None

def get_importer():
    """Lazy load importer module."""
    global _importer
    if _importer is None:
        try:
            import importer as imp
            _importer = imp
        except ImportError as e:
            print(f"Importer not available: {e}")
    return _importer


@app.route('/api/browse-dir', methods=['POST'])
def api_browse_dir():
    """Browse directories for the directory picker."""
    import os
    from pathlib import Path
    
    try:
        data = request.get_json()
        dir_path = data.get('path', 'C:\\')
        
        path = Path(dir_path)
        if not path.exists():
            return jsonify({'error': f'Path not found: {dir_path}'})
        
        if not path.is_dir():
            return jsonify({'error': f'Not a directory: {dir_path}'})
        
        result = {
            'current': str(path),
            'parent': str(path.parent) if path.parent != path else None,
            'directories': [],
            'files': []
        }
        
        try:
            for item in sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                if item.name.startswith('.'):
                    continue
                    
                if item.is_dir():
                    result['directories'].append({
                        'name': item.name,
                        'path': str(item)
                    })
                elif item.suffix.lower() in ['.csv', '.txt']:
                    try:
                        size = item.stat().st_size
                        result['files'].append({
                            'name': item.name,
                            'path': str(item),
                            'size_mb': round(size / (1024 * 1024), 2)
                        })
                    except:
                        pass
        except PermissionError:
            return jsonify({'error': f'Permission denied: {dir_path}'})
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/import/scan-dir', methods=['POST'])
def api_import_scan_dir():
    """Scan a directory for importable files."""
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available'}), 500
    
    try:
        data = request.get_json()
        dir_path = data.get('path', '')
        
        if not dir_path:
            return jsonify({'error': 'No path provided'})
        
        files = imp.scan_directory(dir_path)
        return jsonify({
            'files': [f.to_dict() for f in files],
            'count': len(files)
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/import/preview', methods=['POST'])
def api_import_preview():
    """Preview a file with schema detection."""
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available'}), 500
    
    try:
        data = request.get_json()
        file_path = data.get('path', '')
        
        if not file_path:
            return jsonify({'error': 'No path provided'})
        
        result = imp.preview_file(file_path)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/import/start', methods=['POST'])
def api_import_start():
    """Start importing files."""
    global _import_thread
    
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available'}), 500
    
    try:
        data = request.get_json()
        files = data.get('files', [])
        data_source = data.get('data_source', 'External Import')
        
        if not files:
            return jsonify({'error': 'No files provided'})
        
        # Check if already running
        progress = imp.get_progress()
        if progress.status == 'importing':
            return jsonify({'error': 'Import already in progress'})
        
        # Start import in background thread
        import threading
        _import_thread = threading.Thread(
            target=imp.import_files,
            args=(files, data_source),
            daemon=True
        )
        _import_thread.start()
        
        return jsonify({'status': 'started', 'file_count': len(files)})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/import/status')
def api_import_status():
    """Get current import status."""
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available', 'status': 'error'})
    
    try:
        progress = imp.get_progress()
        return jsonify(progress.to_dict())
    except Exception as e:
        return jsonify({'error': str(e), 'status': 'error'})


@app.route('/api/import/stop', methods=['POST'])
def api_import_stop():
    """Stop the running import."""
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available'}), 500
    
    try:
        imp.request_stop()
        return jsonify({'status': 'stop_requested'})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/import/reset', methods=['POST'])
def api_import_reset():
    """Force reset stuck import state."""
    imp = get_importer()
    if not imp:
        return jsonify({'error': 'Importer module not available'}), 500
    
    try:
        imp.reset_progress()
        return jsonify({'status': 'reset', 'message': 'Import state reset to idle'})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/file-sources')
def api_file_sources():
    """Get list of unique file sources."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT unnest(file_sources) as filename, COUNT(*) as email_count
            FROM emails
            WHERE file_sources IS NOT NULL
            GROUP BY 1
            ORDER BY 1 ASC
        """)
        
        sources = [{'filename': row[0], 'email_count': int(row[1])} for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        return jsonify({'sources': sources, 'count': len(sources)})
    except Exception as e:
        return jsonify({'sources': [], 'error': str(e)})


@app.route('/api/filters/states')
def api_filter_states():
    """Get list of unique states for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT UPPER(state) as state, COUNT(*) as cnt
            FROM emails
            WHERE state IS NOT NULL AND state != ''
            GROUP BY UPPER(state)
            ORDER BY cnt DESC
            LIMIT 100
        """)
        states = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'states': states})
    except Exception as e:
        return jsonify({'states': [], 'error': str(e)})


@app.route('/api/filters/countries')
def api_filter_countries():
    """Get list of unique countries for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT country, COUNT(*) as cnt
            FROM emails
            WHERE country IS NOT NULL AND country != ''
            GROUP BY country
            ORDER BY cnt DESC
            LIMIT 50
        """)
        countries = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'countries': countries})
    except Exception as e:
        return jsonify({'countries': [], 'error': str(e)})


@app.route('/api/filters/data-sources')
def api_filter_data_sources():
    """Get list of unique data sources for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT data_source, COUNT(*) as cnt
            FROM emails
            WHERE data_source IS NOT NULL AND data_source != ''
            GROUP BY data_source
            ORDER BY cnt DESC
            LIMIT 100
        """)
        sources = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'sources': sources})
    except Exception as e:
        return jsonify({'sources': [], 'error': str(e)})


@app.route('/api/filters/validation-statuses')
def api_filter_validation_statuses():
    """Get list of unique validation statuses for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT validation_status, COUNT(*) as cnt
            FROM emails
            WHERE validation_status IS NOT NULL AND validation_status != ''
            GROUP BY validation_status
            ORDER BY cnt DESC
        """)
        statuses = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'statuses': statuses})
    except Exception as e:
        return jsonify({'statuses': [], 'error': str(e)})


@app.route('/api/filters/genders')
def api_filter_genders():
    """Get list of unique genders with counts for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN UPPER(gender) IN ('M', 'MALE') THEN 'M'
                    WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'F'
                    ELSE gender
                END as normalized_gender,
                COUNT(*) as cnt
            FROM emails
            WHERE gender IS NOT NULL AND gender != ''
            GROUP BY normalized_gender
            ORDER BY cnt DESC
        """)
        genders = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'genders': genders})
    except Exception as e:
        return jsonify({'genders': [], 'error': str(e)})


@app.route('/api/filters/cities')
def api_filter_cities():
    """Get list of top cities with counts for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT INITCAP(city) as city, COUNT(*) as cnt
            FROM emails
            WHERE city IS NOT NULL AND city != ''
            GROUP BY INITCAP(city)
            ORDER BY cnt DESC
            LIMIT 200
        """)
        cities = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'cities': cities})
    except Exception as e:
        return jsonify({'cities': [], 'error': str(e)})


@app.route('/api/filters/zipcodes')
def api_filter_zipcodes():
    """Get list of top zipcodes with counts for filter dropdown."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT LEFT(zipcode, 5) as zip5, COUNT(*) as cnt
            FROM emails
            WHERE zipcode IS NOT NULL AND zipcode != ''
            GROUP BY LEFT(zipcode, 5)
            ORDER BY cnt DESC
            LIMIT 200
        """)
        zipcodes = [{'value': row[0], 'count': int(row[1])} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify({'zipcodes': zipcodes})
    except Exception as e:
        return jsonify({'zipcodes': [], 'error': str(e)})


# =============================================================================
# CLOUDFLARE API ENDPOINTS
# =============================================================================

@app.route('/api/cloudflare/zones')
def api_cloudflare_zones():
    """Get all Cloudflare zones."""
    try:
        import cloudflare as cf
        zones = cf.list_zones()
        return jsonify({
            'zones': [{'id': z['id'], 'name': z['name'], 'status': z['status']} for z in zones],
            'managed_count': len(zones)
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/cloudflare/zones/remove', methods=['POST'])
def api_cloudflare_remove_zone():
    """Remove a domain from the managed zones list."""
    try:
        import cloudflare as cf
        data = request.get_json()
        domain = data.get('domain', '').strip().lower()
        
        if not domain:
            return jsonify({'error': 'No domain provided'})
        
        cf.remove_managed_zone(domain)
        return jsonify({'success': True, 'domain': domain})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/cloudflare/zones/<zone_id>/status')
def api_cloudflare_zone_status(zone_id):
    """Get security status for a zone."""
    try:
        import cloudflare as cf
        status = cf.get_zone_security_status(zone_id)
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/cloudflare/zones/<zone_id>/toggle', methods=['POST'])
def api_cloudflare_toggle(zone_id):
    """Toggle a security feature for a zone."""
    try:
        import cloudflare as cf
        data = request.get_json()
        feature = data.get('feature')
        enabled = data.get('enabled', False)
        
        if feature == 'bot_fight':
            result = cf.toggle_bot_fight(zone_id, enabled)
        elif feature == 'us_only':
            # Get current rule ID if exists
            status = cf.get_zone_security_status(zone_id)
            result = cf.toggle_us_only(zone_id, enabled, status.get('us_only_rule_id'))
        elif feature == 'block_scanners':
            status = cf.get_zone_security_status(zone_id)
            result = cf.toggle_block_scanners(zone_id, enabled, status.get('block_scanners_rule_id'))
        elif feature == 'block_dupes':
            status = cf.get_zone_security_status(zone_id)
            duration = data.get('duration', 10)  # Default 10 seconds (free plan)
            redirect_url = data.get('redirect_url')  # Optional redirect URL
            result = cf.toggle_block_dupes(zone_id, enabled, status.get('block_dupes_rule_id'), duration, redirect_url)
        else:
            return jsonify({'error': f'Unknown feature: {feature}'})
        
        return jsonify(result)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/cloudflare/zones/<zone_id>/redirect', methods=['POST'])
def api_cloudflare_redirect(zone_id):
    """Set redirect URL for blocked traffic."""
    try:
        import cloudflare as cf
        data = request.get_json()
        redirect_url = data.get('redirect_url', '')
        
        if not redirect_url:
            return jsonify({'error': 'No redirect URL provided'})
        
        result = cf.set_blocked_redirect(zone_id, redirect_url)
        return jsonify(result)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


# =====================================================
# DOMAIN REPUTATION API
# =====================================================

@app.route('/api/reputation/domains')
def api_reputation_domains():
    """Get all monitored domains and their cached results."""
    try:
        import domain_reputation as dr
        cache = dr.get_cached_results()
        return jsonify(cache)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/reputation/check', methods=['POST'])
def api_reputation_check():
    """Check a single domain and add to monitoring."""
    try:
        import domain_reputation as dr
        data = request.get_json()
        domain = data.get('domain', '').strip().lower()
        
        if not domain:
            return jsonify({'error': 'No domain provided'})
        
        # Remove protocol if present
        domain = domain.replace('https://', '').replace('http://', '').split('/')[0]
        
        result = dr.add_custom_domain(domain)
        return jsonify(result)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/reputation/remove', methods=['POST'])
def api_reputation_remove():
    """Remove a domain from monitoring."""
    try:
        import domain_reputation as dr
        data = request.get_json()
        domain = data.get('domain', '').strip().lower()
        
        if not domain:
            return jsonify({'error': 'No domain provided'})
        
        dr.remove_custom_domain(domain)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/reputation/refresh-all', methods=['POST'])
def api_reputation_refresh():
    """Refresh checks for all monitored domains."""
    try:
        import domain_reputation as dr
        results = dr.refresh_all_domains()
        cache = dr.get_cached_results()
        return jsonify(cache)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/reputation/import-cf', methods=['POST'])
def api_reputation_import_cf():
    """Import domains from Cloudflare zones."""
    try:
        import domain_reputation as dr
        import cloudflare as cf
        
        # Get all CF zones
        zones = cf.list_zones()
        
        # Add each domain
        for zone in zones:
            domain = zone.get('name', '')
            if domain:
                dr.add_custom_domain(domain)
        
        # Return all results
        results = dr.refresh_all_domains()
        cache = dr.get_cached_results()
        return jsonify(cache)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/reputation/cf-protect', methods=['POST'])
def api_reputation_cf_protect():
    """Enable Cloudflare protection for a specific domain and add to managed list."""
    try:
        import cloudflare as cf
        data = request.get_json()
        domain = data.get('domain', '').strip().lower()
        features = data.get('features', [])
        
        if not domain:
            return jsonify({'error': 'No domain provided'})
        
        if not features:
            return jsonify({'error': 'No features selected'})
        
        # Find the zone in ALL zones (not just managed)
        zones = cf.list_all_zones()
        zone_id = None
        for zone in zones:
            if zone.get('name', '').lower() == domain:
                zone_id = zone.get('id')
                break
        
        if not zone_id:
            return jsonify({'not_found': True, 'error': f'Domain {domain} not found in Cloudflare zones'})
        
        # Add to managed zones list (so it shows in CF tab)
        cf.add_managed_zone(domain)
        
        # Get current status
        status = cf.get_zone_security_status(zone_id)
        
        results = {}
        
        # Enable requested features
        if 'us_only' in features:
            result = cf.toggle_us_only(zone_id, True, status.get('us_only_rule_id'))
            results['us_only'] = result
        
        if 'block_scanners' in features:
            result = cf.toggle_block_scanners(zone_id, True, status.get('block_scanners_rule_id'))
            results['block_scanners'] = result
        
        if 'block_dupes' in features:
            result = cf.toggle_block_dupes(zone_id, True, status.get('block_dupes_rule_id'), duration=10)
            results['block_dupes'] = result
        
        return jsonify({'success': True, 'domain': domain, 'zone_id': zone_id, 'results': results})
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


# =====================================================
# LIST BUILDER API
# =====================================================

def build_segment_where_clause(filters):
    """Build SQL WHERE clause from segment filters."""
    conditions = []
    
    if filters.get('category'):
        conditions.append(f"e.email_category = '{filters['category']}'")
    
    if filters.get('provider'):
        conditions.append(f"e.email_provider = '{filters['provider']}'")
    
    if filters.get('clickers') == 'true':
        conditions.append("e.is_clicker = true")
    
    if filters.get('openers') == 'true':
        conditions.append("e.is_opener = true")
    
    if filters.get('quality') == 'high':
        conditions.append("e.quality_score >= 70")
    elif filters.get('quality') == 'medium':
        conditions.append("e.quality_score >= 40 AND e.quality_score < 70")
    elif filters.get('quality') == 'low':
        conditions.append("e.quality_score < 40")
    
    if filters.get('domain'):
        conditions.append(f"e.email_domain = '{filters['domain']}'")
    
    if filters.get('state'):
        conditions.append(f"UPPER(e.state) = '{filters['state'].upper()}'")
    
    if filters.get('validation_status') == 'verified':
        conditions.append("e.validation_status = 'verified'")
    
    # 2nd Level Big4 - MX category filter (requires JOIN with domain_mx)
    if filters.get('mx_category'):
        conditions.append(f"dm.mx_category = '{filters['mx_category']}'")
    
    # All 2nd Level Big4 - matches Google, Microsoft, or Yahoo MX
    if filters.get('mx_category_big4') == 'true':
        conditions.append("dm.mx_category IN ('Google', 'Microsoft', 'Yahoo')")
    
    return " AND ".join(conditions) if conditions else "1=1"


def needs_mx_join(filters):
    """Check if filter requires JOIN with domain_mx table."""
    return filters.get('mx_category') is not None or filters.get('mx_category_big4') == 'true'


@app.route('/api/list-builder/preset-counts', methods=['POST'])
def api_list_builder_preset_counts():
    """Get counts for all presets to populate dropdown."""
    try:
        data = request.get_json()
        presets = data.get('presets', [])
        
        conn = get_db()
        cursor = conn.cursor()
        
        counts = {}
        for preset in presets:
            key = preset.get('key')
            filters = preset.get('filters', {})
            where = build_segment_where_clause(filters)
            
            # Check if we need JOIN with domain_mx for mx_category filters
            if needs_mx_join(filters):
                sql = f"""SELECT COUNT(*) FROM emails e 
                         JOIN domain_mx dm ON e.email_domain = dm.domain 
                         WHERE {where}"""
            else:
                sql = f"SELECT COUNT(*) FROM emails e WHERE {where}"
            cursor.execute(sql)
            counts[key] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({'counts': counts})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/list-builder/count', methods=['POST'])
def api_list_builder_count():
    """Get total unique email count for combined segments."""
    try:
        data = request.get_json()
        segments = data.get('segments', [])
        
        if not segments:
            return jsonify({'error': 'No segments provided'})
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Build UNION query for all segments
        union_parts = []
        for segment in segments:
            filters = segment.get('filters', {})
            where = build_segment_where_clause(filters)
            
            # Check if we need JOIN with domain_mx for mx_category filters
            if needs_mx_join(filters):
                union_parts.append(f"""SELECT DISTINCT e.email FROM emails e 
                                      JOIN domain_mx dm ON e.email_domain = dm.domain 
                                      WHERE {where}""")
            else:
                union_parts.append(f"SELECT DISTINCT e.email FROM emails e WHERE {where}")
        
        # Count unique emails across all segments
        sql = f"SELECT COUNT(DISTINCT email) FROM ({' UNION '.join(union_parts)}) AS combined"
        cursor.execute(sql)
        total = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({'total': total})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/list-builder/export', methods=['POST'])
def api_list_builder_export():
    """Export combined list from all segments - streaming for large exports."""
    try:
        data = request.get_json()
        segments = data.get('segments', [])
        email_only = data.get('email_only', False)
        
        # Ensure boolean
        if isinstance(email_only, str):
            email_only = email_only.lower() == 'true'
        
        if not segments:
            return jsonify({'error': 'No segments provided'})
        
        conn = get_db()
        cursor = conn.cursor(name='list_builder_export')  # Server-side cursor for streaming
        
        # Build UNION query for all segments
        union_parts = []
        for segment in segments:
            filters = segment.get('filters', {})
            where = build_segment_where_clause(filters)
            
            # Check if we need JOIN with domain_mx for mx_category filters
            if email_only:
                # Email only export
                if needs_mx_join(filters):
                    union_parts.append(f"""
                        SELECT DISTINCT e.email
                        FROM emails e
                        JOIN domain_mx dm ON e.email_domain = dm.domain
                        WHERE {where}
                    """)
                else:
                    union_parts.append(f"""
                        SELECT DISTINCT e.email
                        FROM emails e WHERE {where}
                    """)
            else:
                # Full export with all fields
                if needs_mx_join(filters):
                    union_parts.append(f"""
                        SELECT DISTINCT e.email, e.email_domain, e.email_provider, e.email_brand, e.email_category,
                               e.quality_score, e.is_clicker, e.is_opener, e.first_name, e.last_name,
                               e.phone, e.city, e.state, e.zipcode, e.gender,
                               CAST(e.dob AS TEXT) as dob,
                               CAST(e.signup_date AS TEXT) as signup_date,
                               CAST(e.created_at AS TEXT) as created_at,
                               e.data_source
                        FROM emails e
                        JOIN domain_mx dm ON e.email_domain = dm.domain
                        WHERE {where}
                    """)
                else:
                    union_parts.append(f"""
                        SELECT DISTINCT e.email, e.email_domain, e.email_provider, e.email_brand, e.email_category,
                               e.quality_score, e.is_clicker, e.is_opener, e.first_name, e.last_name,
                               e.phone, e.city, e.state, e.zipcode, e.gender,
                               CAST(e.dob AS TEXT) as dob,
                               CAST(e.signup_date AS TEXT) as signup_date,
                               CAST(e.created_at AS TEXT) as created_at,
                               e.data_source
                        FROM emails e WHERE {where}
                    """)
        
        if email_only:
            sql = ' UNION '.join(union_parts) + " ORDER BY email"
        else:
            sql = ' UNION '.join(union_parts) + " ORDER BY quality_score DESC NULLS LAST"
        
        cursor.execute(sql)
        
        # Stream response for large exports
        import io
        import csv
        from flask import Response
        
        def generate():
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            if email_only:
                writer.writerow(['email'])
            else:
                writer.writerow(['email', 'email_domain', 'email_provider', 'email_brand', 'email_category',
                                'quality_score', 'is_clicker', 'is_opener', 'first_name', 'last_name',
                                'phone', 'city', 'state', 'zipcode', 'gender', 'dob', 'signup_date', 
                                'created_at', 'data_source'])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
            
            # Stream rows in batches
            batch_size = 10000
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
            
            cursor.close()
            conn.close()
        
        filename = 'emails_only.csv' if email_only else 'combined_list.csv'
        return Response(
            generate(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
        
    except Exception as e:
        import traceback
        print(f"Export error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})


if __name__ == '__main__':
    print("\n" + "="*50)
    print("  EMAIL DATABASE WEB DASHBOARD")
    print("="*50)
    print("\n  Open your browser to: http://localhost:5000")
    print("\n  Press Ctrl+C to stop the server")
    print("="*50 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
