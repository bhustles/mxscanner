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
        </div>
        
        <!-- STATS TAB -->
        <div id="tab-stats" class="tab-content">
        
        <!-- Stats Cards (auto-refresh) -->
        <p style="color: #666; font-size: 0.85em; margin-bottom: 8px;">Auto-refresh every 2s | Last updated: <span id="stats-updated">-</span> | Count goes up as the pipeline loads (commits per file)</p>
        <div class="stats-grid" id="stats-cards">
            <div class="stat-card">
                <h3>Total Emails</h3>
                <div class="value" id="stat-total">{{ "{:,}".format(stats.total) }}</div>
            </div>
            <div class="stat-card">
                <h3>Big 4 ISPs</h3>
                <div class="value" id="stat-big4">{{ "{:,}".format(stats.big4) }}</div>
                <div class="sub" id="stat-big4-pct">{{ "%.1f"|format(stats.big4 / stats.total * 100 if stats.total else 0) }}%</div>
            </div>
            <div class="stat-card">
                <h3>Cable Providers</h3>
                <div class="value" id="stat-cable">{{ "{:,}".format(stats.cable) }}</div>
                <div class="sub" id="stat-cable-pct">{{ "%.1f"|format(stats.cable / stats.total * 100 if stats.total else 0) }}%</div>
            </div>
            <div class="stat-card">
                <h3>General Internet</h3>
                <div class="value" id="stat-gi">{{ "{:,}".format(stats.gi) }}</div>
                <div class="sub" id="stat-gi-pct">{{ "%.1f"|format(stats.gi / stats.total * 100 if stats.total else 0) }}%</div>
            </div>
            <div class="stat-card">
                <h3>Clickers</h3>
                <div class="value" id="stat-clickers">{{ "{:,}".format(stats.clickers) }}</div>
            </div>
            <div class="stat-card">
                <h3>High Quality (80+)</h3>
                <div class="value" id="stat-high-quality">{{ "{:,}".format(stats.high_quality) }}</div>
            </div>
        </div>
        
        <!-- Provider Distribution (auto-refresh) -->
        <div class="section" id="providers-section">
            <h2>By Email Provider</h2>
            <div id="providers-content">
                {% for provider, count in providers[:10] %}
                <div style="margin: 10px 0;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>{{ provider or 'Unknown' }}</span>
                        <span>{{ "{:,}".format(count) }}</span>
                    </div>
                    <div class="bar">
                        <div class="bar-fill" style="width: {{ (count / stats.total * 100) if stats.total else 0 }}%"></div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <!-- Quality Distribution (auto-refresh) -->
        <div class="section" id="quality-section">
            <h2>Quality Score Distribution</h2>
            <div id="quality-content">
                {% for tier, count in quality %}
                <div style="margin: 10px 0;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>{{ tier }}</span>
                        <span>{{ "{:,}".format(count) }} ({{ "%.1f"|format(count / stats.total * 100 if stats.total else 0) }}%)</span>
                    </div>
                    <div class="bar">
                        <div class="bar-fill" style="width: {{ (count / stats.total * 100) if stats.total else 0 }}%"></div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        </div><!-- END STATS TAB -->
        
        <!-- QUERY TAB -->
        <div id="tab-query" class="tab-content">
        
        <!-- Query Tool -->
        <div class="section">
            <h2>Query Tool</h2>
            <div class="filters">
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
                    <label>Domain Search</label>
                    <input type="text" id="domain" placeholder="e.g. gmail.com" style="width: 120px;">
                </div>
                <div class="filter-group">
                    <label>Min Quality</label>
                    <select id="min_score">
                        <option value="">Any</option>
                        <option value="80">High (80+)</option>
                        <option value="60">Good (60+)</option>
                        <option value="40">Average (40+)</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label>State</label>
                    <input type="text" id="state" placeholder="e.g. FL" maxlength="2" style="width: 60px;">
                </div>
                <div class="filter-group">
                    <label>Clickers Only</label>
                    <select id="clickers">
                        <option value="">No</option>
                        <option value="true">Yes</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label>Source File</label>
                    <select id="file_source" style="max-width: 180px;">
                        <option value="">All Files</option>
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
            <button onclick="runQuery(1)">Search</button>
            <button onclick="exportCSV()" style="background: #28a745;">Export CSV</button>
            
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
                <button type="button" style="background: #6f42c1;" id="mx-sync-gi-btn" onclick="syncGiDomains()" title="Sync is_gi flag with emails table (finds missing GI domains)">Sync GI Domains</button>
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
        
        function runQuery(page) {
            page = page || 1;
            currentPage = page;
            perPage = parseInt(document.getElementById('limit').value);
            var offset = (page - 1) * perPage;
            
            var params = new URLSearchParams({
                provider: document.getElementById('provider').value,
                category: document.getElementById('category').value,
                domain: document.getElementById('domain').value.toLowerCase().trim(),
                min_score: document.getElementById('min_score').value,
                state: document.getElementById('state').value.toUpperCase(),
                clickers: document.getElementById('clickers').value,
                limit: perPage,
                offset: offset
            });
            
            document.getElementById('results').innerHTML = '<p class="loading">Loading...</p>';
            
            fetch('/api/query?' + params)
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) {
                        document.getElementById('results').innerHTML = '<p class="error">' + data.error + '</p>';
                        return;
                    }
                    totalResults = data.total_count || data.count;
                    var totalPages = Math.ceil(totalResults / perPage);
                    var html = '<p>Showing ' + data.count + ' results</p><table><tr>';
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
                    document.getElementById('results').innerHTML = '<p class="error">Error</p>';
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
            var params = new URLSearchParams({
                provider: document.getElementById('provider').value,
                category: document.getElementById('category').value,
                domain: document.getElementById('domain').value.toLowerCase().trim(),
                min_score: document.getElementById('min_score').value,
                state: document.getElementById('state').value.toUpperCase(),
                clickers: document.getElementById('clickers').value,
                file_source: document.getElementById('file_source').value,
                limit: '50000'
            });
            window.location.href = '/api/export?' + params;
        }
        
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
                // Load stats only when stats tab is shown
                if (tabName === 'stats') {
                    try { ensureStatsLoaded(); } catch(e) { console.log('Stats error:', e); }
                }
                // Load domain config when config tab is shown
                if (tabName === 'config') {
                    try { loadDomainConfig(); } catch(e) { console.log('Config error:', e); }
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
        
        function syncGiDomains() {
            if (!confirm('Sync GI domains from emails table? This will find missing domains and flag them for scanning (preserves existing scan results).')) return;
            document.getElementById('mx-sync-gi-btn').disabled = true;
            addMxLog('SYSTEM', 'Syncing GI domains from emails table...', 'Info');
            fetch('/api/mx/sync-gi', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                document.getElementById('mx-sync-gi-btn').disabled = false;
                if (data.error) {
                    addMxLog('SYSTEM', 'Error: ' + data.error, 'Error');
                    alert('Error: ' + data.error);
                    return;
                }
                var msg = 'GI Sync complete! Before: ' + formatNum(data.gi_before) + ' -> After: ' + formatNum(data.gi_after) + '. Inserted ' + formatNum(data.domains_inserted) + ' new, flagged ' + formatNum(data.domains_flagged) + ' existing. Ready to scan: ' + formatNum(data.unchecked_to_scan);
                addMxLog('SYSTEM', msg, 'Info');
                alert('Sync complete! GI domains: ' + formatNum(data.gi_before) + ' -> ' + formatNum(data.gi_after) + '. Inserted: ' + formatNum(data.domains_inserted) + ', Flagged: ' + formatNum(data.domains_flagged) + '. Ready to scan: ' + formatNum(data.unchecked_to_scan));
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


@app.route('/api/query')
def api_query():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_parts = ["1=1"]
        params = []
        
        if request.args.get('provider'):
            where_parts.append("email_provider = %s")
            params.append(request.args.get('provider'))
        if request.args.get('category'):
            where_parts.append("email_category = %s")
            params.append(request.args.get('category'))
        if request.args.get('domain'):
            # Support partial domain matching
            domain = request.args.get('domain').strip().lower()
            if domain:
                where_parts.append("email_domain LIKE %s")
                params.append(f"%{domain}%")
        if request.args.get('min_score'):
            where_parts.append("quality_score >= %s")
            params.append(int(request.args.get('min_score')))
        if request.args.get('state'):
            where_parts.append("state = %s")
            params.append(request.args.get('state'))
        if request.args.get('clickers') == 'true':
            where_parts.append("is_clicker = true")
        
        where_clause = " AND ".join(where_parts)
        
        # Get total count for pagination
        count_sql = f"SELECT COUNT(*) FROM emails WHERE {where_clause}"
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]
        
        # Get paginated results
        limit = min(int(request.args.get('limit', 500)), 5000)
        offset = int(request.args.get('offset', 0))
        
        sql = f"""SELECT email, email_domain, email_provider, email_brand, email_category, 
                         quality_score, is_clicker, first_name, city, state 
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
        
        # Build WHERE clause
        where_parts = ["1=1"]
        params = []
        
        if request.args.get('provider'):
            where_parts.append("email_provider = %s")
            params.append(request.args.get('provider'))
        if request.args.get('category'):
            where_parts.append("email_category = %s")
            params.append(request.args.get('category'))
        if request.args.get('domain'):
            domain = request.args.get('domain').strip().lower()
            if domain:
                where_parts.append("email_domain LIKE %s")
                params.append(f"%{domain}%")
        if request.args.get('min_score'):
            where_parts.append("quality_score >= %s")
            params.append(int(request.args.get('min_score')))
        if request.args.get('state'):
            where_parts.append("state = %s")
            params.append(request.args.get('state'))
        if request.args.get('clickers') == 'true':
            where_parts.append("is_clicker = true")
        if request.args.get('file_source'):
            where_parts.append("%s = ANY(file_sources)")
            params.append(request.args.get('file_source'))
        
        where_clause = " AND ".join(where_parts)
        limit = min(int(request.args.get('limit', 50000)), 100000)
        
        sql = f"""SELECT email, email_domain, email_provider, email_brand, email_category, 
                         quality_score, is_clicker, is_opener, first_name, last_name,
                         phone, city, state, zipcode
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
            ORDER BY 2 DESC
        """)
        
        sources = [{'filename': row[0], 'email_count': int(row[1])} for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        return jsonify({'sources': sources, 'count': len(sources)})
    except Exception as e:
        return jsonify({'sources': [], 'error': str(e)})


if __name__ == '__main__':
    print("\n" + "="*50)
    print("  EMAIL DATABASE WEB DASHBOARD")
    print("="*50)
    print("\n  Open your browser to: http://localhost:5000")
    print("\n  Press Ctrl+C to stop the server")
    print("="*50 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
