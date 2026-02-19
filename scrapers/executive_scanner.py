"""
Executive Scanner — Track C-suite and key executive changes at target companies.

Scans for new hires, departures, promotions at tracked companies.
Classifies titles by priority (high/medium/low) and sends Discord alerts
for high-priority changes.

Usage:
    python3 scrapers/executive_scanner.py --company "Citadel"
    python3 scrapers/executive_scanner.py --all
    python3 scrapers/executive_scanner.py --prospects
    python3 scrapers/executive_scanner.py --find-structure --company "Point72"
"""

import os
import sys
import re
import sqlite3
import argparse
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path
from scrapers.search_client import search_general as search_web

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


# =============================================================================
# TITLE CLASSIFICATION
# =============================================================================

# Patterns: (regex, title_category, priority)
_TITLE_PATTERNS = [
    # HIGH PRIORITY — C-suite with real estate relevance
    (r'\b(?:chief\s+executive|ceo|president)\b', 'ceo', 'high'),
    (r'\b(?:chief\s+financial|cfo)\b', 'cfo', 'high'),
    (r'\b(?:chief\s+operating|coo)\b', 'coo', 'high'),
    (r'\b(?:chief\s+revenue|cro)\b', 'cro', 'high'),
    # Real estate / facilities / workplace — HIGH
    (r'\b(?:chief\s+(?:real\s*estate|workplace)\s*officer)\b', 'real_estate', 'high'),
    (r'\breal\s*estate\b.*\b(?:head|vp|vice\s*president|director|svp|evp|president|officer)\b',
     'real_estate', 'high'),
    (r'\b(?:head|vp|vice\s*president|director|svp|evp)\b.*\breal\s*estate\b',
     'real_estate', 'high'),
    (r'\b(?:head|vp|vice\s*president|director|svp|evp)\b.*\bfacilit(?:y|ies)\b',
     'facilities', 'high'),
    (r'\b(?:chief\s+workplace|head\s+of\s+workplace)\b', 'facilities', 'high'),
    # MEDIUM PRIORITY
    (r'\b(?:chief\s+strategy|cso)\b', 'cso', 'medium'),
    (r'\b(?:general\s+counsel|chief\s+legal|senior\s+general\s+counsel|clg)\b',
     'legal', 'medium'),
    (r'\bmanaging\s+partner\b', 'managing_partner', 'medium'),
    (r'\b(?:chief\s+technology|cto)\b', 'other_c_suite', 'medium'),
    (r'\b(?:chief\s+marketing|cmo)\b', 'other_c_suite', 'medium'),
    (r'\b(?:chief\s+human|chro|chief\s+people|cpo)\b', 'other_c_suite', 'medium'),
    (r'\b(?:chief\s+information|cio)\b', 'other_c_suite', 'medium'),
    (r'\b(?:chief\s+investment)\b', 'other_c_suite', 'medium'),
    (r'\boffice\s+manager\b', 'facilities', 'medium'),
    (r'\bworkplace\s+experience\b', 'facilities', 'medium'),
]


def classify_title(title: str) -> Tuple[str, str]:
    """
    Classify an executive title into (title_category, priority).

    Returns:
        Tuple of (title_category, priority) where:
        - title_category: ceo, cfo, coo, cro, cso, real_estate, facilities,
                          legal, managing_partner, other_c_suite, other
        - priority: high, medium, low
    """
    if not title:
        return ('other', 'low')

    title_lower = title.lower().strip()

    for pattern, category, priority in _TITLE_PATTERNS:
        if re.search(pattern, title_lower):
            return (category, priority)

    # Catch-all: any remaining C-suite or VP/Director
    if re.search(r'\bchief\b', title_lower):
        return ('other_c_suite', 'low')
    if re.search(r'\b(?:vp|vice\s*president|svp|evp|director)\b', title_lower):
        return ('other', 'low')

    return ('other', 'low')


def classify_change_type(new_title: str, old_company: str, new_company: str,
                         old_title: str = None) -> str:
    """Determine the change_type for an executive change."""
    cat, _ = classify_title(new_title)

    # Map title category to specific change types
    cat_to_change = {
        'ceo': 'new_ceo', 'cfo': 'new_cfo', 'coo': 'new_coo',
        'cro': 'new_cro', 'cso': 'new_cso',
        'real_estate': 'new_re_head', 'facilities': 'new_facilities',
        'legal': 'new_gc', 'managing_partner': 'new_managing_partner',
    }

    if cat in cat_to_change:
        return cat_to_change[cat]

    # Generic classification
    if old_company and new_company and old_company.lower() == new_company.lower():
        if old_title and new_title:
            return 'promoted'
        return 'lateral'

    if old_company and new_company:
        return 'hired'

    return 'hired'


# =============================================================================
# SEARCH QUERIES
# =============================================================================

_SEARCH_TEMPLATES = [
    '"{company}" new CEO',
    '"{company}" new CFO',
    '"{company}" appoints',
    '"{company}" names new',
    '"{company}" hires',
    '"{company}" executive',
    '"{company}" head of real estate',
    '"{company}" facilities director',
]

_STRUCTURE_TEMPLATES = [
    '"{company}" leadership team',
    '"{company}" executives',
    '"{company}" management team',
    '"{company}" senior leadership',
]


# =============================================================================
# PARSING
# =============================================================================

def _parse_headline_basic(headline: str, company_name: str) -> Optional[Dict]:
    """
    Basic regex parsing of a headline for person name, title, company.

    Returns dict with: person_name, title, company, old_company (if found)
    """
    if not headline:
        return None

    # Patterns for common headline formats
    patterns = [
        # "Company Appoints/Names/Hires John Smith as CFO"
        re.compile(
            r'(?:' + re.escape(company_name) + r')\s+(?:appoints|names|hires|promotes|elevates)\s+'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:as\s+)?(.+)',
            re.IGNORECASE),
        # "John Smith Named/Appointed CFO of Company"
        re.compile(
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+'
            r'(?:named|appointed|joins|hired|promoted)\s+(?:as\s+)?(.+?)(?:\s+(?:at|of)\s+)',
            re.IGNORECASE),
        # "John Smith to Join Company as CFO"
        re.compile(
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+to\s+(?:join|lead)\s+.+?\s+as\s+(.+)',
            re.IGNORECASE),
    ]

    for pat in patterns:
        m = pat.search(headline)
        if m:
            person = m.group(1).strip()
            title = m.group(2).strip()
            # Clean title of trailing junk
            title = re.sub(r'\s*[-–—|,]\s*.*$', '', title)
            title = re.sub(r'\s+$', '', title)
            if len(person.split()) >= 2 and len(title) > 2:
                return {
                    'person_name': person,
                    'title': title,
                    'company': company_name,
                    'old_company': None,
                }

    return None


def _parse_with_llm(headline: str, snippet: str, company_name: str) -> Optional[Dict]:
    """
    Use Claude API to parse a headline/snippet into structured executive data.
    Falls back to None if API unavailable.
    """
    if not HAS_ANTHROPIC:
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None

    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = f"""Extract executive change info from this headline and snippet.
Company context: {company_name}

Headline: {headline}
Snippet: {snippet}

Return ONLY valid JSON (no markdown) with these fields:
- person_name: full name
- new_title: their new title
- new_company: company they joined
- old_title: previous title (if mentioned)
- old_company: previous company (if mentioned)
- change_type: hired/departed/promoted/lateral

If this is NOT about an executive change, return {{"skip": true}}"""

        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        # Extract JSON
        if text.startswith('{'):
            data = json.loads(text)
            if data.get('skip'):
                return None
            return data
    except Exception as e:
        logger.debug(f"LLM parse failed: {e}")

    return None


# =============================================================================
# DEDUPE
# =============================================================================

def _check_change_exists(cur, person_name: str, company_name: str,
                         effective_date: str = None) -> bool:
    """
    Check if an executive change already exists.
    Dedupe: same person + same company + within 30 days.
    """
    if effective_date:
        try:
            dt = datetime.strptime(effective_date, '%Y-%m-%d')
        except Exception:
            dt = datetime.now()
    else:
        dt = datetime.now()

    date_min = (dt - timedelta(days=30)).strftime('%Y-%m-%d')
    date_max = (dt + timedelta(days=30)).strftime('%Y-%m-%d')

    cur.execute("""
        SELECT id FROM executive_changes
        WHERE LOWER(person_name) = LOWER(?)
        AND (LOWER(company_name) = LOWER(?) OR LOWER(new_company) = LOWER(?))
        AND effective_date BETWEEN ? AND ?
    """, (person_name, company_name, company_name, date_min, date_max))

    return cur.fetchone() is not None


def _link_company_id(cur, company_name: str) -> Optional[int]:
    """Find company_id if company exists in companies table."""
    if not company_name:
        return None
    cur.execute("""
        SELECT id FROM companies
        WHERE LOWER(name) LIKE LOWER(?)
        ORDER BY LENGTH(name) ASC LIMIT 1
    """, (f"%{company_name}%",))
    row = cur.fetchone()
    return row[0] if row else None


# =============================================================================
# DISCORD ALERTS
# =============================================================================

def send_executive_alert(change_id: int, db_path: str = None):
    """
    Send Discord alert for an executive change.
    Marks sent_to_discord = 1 after sending.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM executive_changes WHERE id = ?", (change_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False

    r = dict(row)
    badge = {'high': '🔴 HIGH', 'medium': '🟡 MED', 'low': '🟢 LOW'}.get(
        r.get('priority', 'low'), '🟢 LOW')

    from_part = f" (from {r['old_company']})" if r.get('old_company') else ""
    if r.get('change_type', '').startswith('new_'):
        from_part = f" ({r['change_type'].replace('new_', 'new ').replace('_', ' ')})"
        if r.get('old_company'):
            from_part = f" (from {r['old_company']})"

    msg = f"**{badge}:** {r['person_name']} → {r.get('new_title', r.get('old_title', '?'))} " \
          f"at {r['company_name']}{from_part}"

    if r.get('headline'):
        msg += f"\n> {r['headline'][:120]}"
    if r.get('source_url'):
        msg += f"\n{r['source_url']}"

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    sent = False

    if webhook_url and HAS_REQUESTS:
        try:
            requests.post(webhook_url, json={"content": msg}, timeout=10)
            sent = True
        except Exception as e:
            logger.warning(f"Discord send failed: {e}")
    else:
        logger.info(f"Discord alert (no webhook): {msg}")

    cur.execute("UPDATE executive_changes SET sent_to_discord = 1 WHERE id = ?",
                (change_id,))
    conn.commit()
    conn.close()
    return sent


def send_unsent_alerts(db_path: str = None):
    """Send all unsent high-priority alerts."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM executive_changes
        WHERE sent_to_discord = 0 AND priority = 'high'
        ORDER BY created_at ASC
    """)
    ids = [r[0] for r in cur.fetchall()]
    conn.close()

    for cid in ids:
        send_executive_alert(cid, db_path)


# =============================================================================
# SCANNING
# =============================================================================

def scan_company_executives(company_id: int = None, company_name: str = None,
                            db_path: str = None, dry_run: bool = False) -> List[Dict]:
    """
    Scan a single company for executive news.

    Args:
        company_id: ID in companies table
        company_name: Company name (used if company_id not provided)
        db_path: Database path
        dry_run: If True, parse but don't insert

    Returns:
        List of parsed changes found
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Resolve company name
    if company_id and not company_name:
        cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
        row = cur.fetchone()
        if row:
            company_name = row['name']
        else:
            conn.close()
            return []
    elif not company_name:
        conn.close()
        return []

    logger.info(f"Scanning executives for: {company_name}")
    changes_found = []
    today = datetime.now().strftime('%Y-%m-%d')

    for template in _SEARCH_TEMPLATES:
        query = template.format(company=company_name)

        try:
            results = search_web(query)
        except Exception as e:
            logger.warning(f"Search failed for '{query}': {e}")
            continue

        if not results:
            continue

        for result in results[:5]:  # Top 5 per query
            headline = result.get('title', '')
            snippet = result.get('snippet', '')
            url = result.get('link', '')
            date_str = result.get('date', today)

            # Try basic parse first
            parsed = _parse_headline_basic(headline, company_name)

            # Fall back to LLM
            if not parsed:
                parsed = _parse_with_llm(headline, snippet, company_name)

            if not parsed or not parsed.get('person_name'):
                continue

            person = parsed['person_name']
            new_title = parsed.get('new_title') or parsed.get('title', '')
            old_company = parsed.get('old_company')
            old_title = parsed.get('old_title')

            title_cat, priority = classify_title(new_title)
            change_type = classify_change_type(new_title, old_company, company_name, old_title)

            change = {
                'person_name': person,
                'new_title': new_title,
                'company_name': company_name,
                'company_id': company_id or _link_company_id(cur, company_name),
                'old_title': old_title,
                'old_company': old_company,
                'change_type': change_type,
                'priority': priority,
                'effective_date': date_str,
                'source': 'web_search',
                'source_url': url,
                'headline': headline,
            }

            # Dedupe
            if _check_change_exists(cur, person, company_name, date_str):
                logger.debug(f"Duplicate: {person} at {company_name}")
                continue

            changes_found.append(change)

            if not dry_run:
                cur.execute("""
                    INSERT INTO executive_changes
                    (company_id, company_name, person_name, old_title, new_title,
                     old_company, new_company, change_type, priority,
                     effective_date, source, source_url, headline)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    change['company_id'], company_name, person,
                    old_title, new_title, old_company, company_name,
                    change_type, priority, date_str, 'web_search', url, headline,
                ))
                change_id = cur.lastrowid

                # Immediate alert for high priority
                if priority == 'high':
                    conn.commit()
                    send_executive_alert(change_id, db_path)

        time.sleep(1)  # Rate limiting between queries

    if not dry_run:
        conn.commit()
    conn.close()

    logger.info(f"Found {len(changes_found)} changes for {company_name}")
    return changes_found


def scan_all_executives(company_filter: str = 'prospects_and_targets',
                        db_path: str = None, dry_run: bool = False,
                        max_companies: int = None) -> Dict:
    """
    Daily scan for executive changes.

    Args:
        company_filter: 'all', 'prospects_and_targets', 'major_firms'
        db_path: Database path
        dry_run: If True, parse but don't insert
        max_companies: Limit number of companies scanned

    Returns:
        Dict with total_scanned, total_changes, high_priority, changes list
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if company_filter == 'all':
        cur.execute("SELECT id, name FROM companies ORDER BY name")
    elif company_filter == 'major_firms':
        cur.execute("""
            SELECT id, name FROM companies
            WHERE sector IN ('hedge_fund', 'private_equity', 'investment_bank',
                             'asset_management', 'venture_capital')
            OR category = 'institutional'
            ORDER BY name
        """)
    else:  # prospects_and_targets
        cur.execute("""
            SELECT id, name FROM companies
            WHERE status IN ('prospect', 'high_growth_target')
            ORDER BY name
        """)

    companies = [(r['id'], r['name']) for r in cur.fetchall()]
    conn.close()

    if max_companies:
        companies = companies[:max_companies]

    all_changes = []
    for cid, cname in companies:
        changes = scan_company_executives(cid, cname, db_path, dry_run)
        all_changes.extend(changes)
        time.sleep(2)  # Rate limit between companies

    high_count = sum(1 for c in all_changes if c['priority'] == 'high')

    return {
        'total_scanned': len(companies),
        'total_changes': len(all_changes),
        'high_priority': high_count,
        'changes': all_changes,
    }


def find_executive_structure(company_id: int = None, company_name: str = None,
                             db_path: str = None, dry_run: bool = False) -> List[Dict]:
    """
    On-demand deep scan for a company's full C-suite/leadership.
    Searches for leadership team pages and parses executive listings.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if company_id and not company_name:
        cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
        row = cur.fetchone()
        if row:
            company_name = row['name']

    if not company_name:
        conn.close()
        return []

    logger.info(f"Finding executive structure for: {company_name}")
    executives_found = []

    for template in _STRUCTURE_TEMPLATES:
        query = template.format(company=company_name)
        try:
            results = search_web(query)
        except Exception as e:
            logger.warning(f"Search failed: {e}")
            continue

        if not results:
            continue

        # Use LLM to parse leadership listings from snippets
        for result in results[:3]:
            snippet = result.get('snippet', '')
            url = result.get('link', '')

            if not HAS_ANTHROPIC or not os.environ.get("ANTHROPIC_API_KEY"):
                # Basic: look for "Name, Title" patterns in snippet
                name_title_pairs = re.findall(
                    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*[,–—-]\s*'
                    r'((?:Chief|CEO|CFO|COO|CRO|President|Managing|Head|VP|Director|'
                    r'General\s+Counsel|Partner)[^,;.]{3,40})',
                    snippet
                )
                for name, title in name_title_pairs:
                    cat, pri = classify_title(title)
                    executives_found.append({
                        'person_name': name.strip(),
                        'title': title.strip(),
                        'title_category': cat,
                        'priority': pri,
                        'company_name': company_name,
                        'company_id': company_id or _link_company_id(cur, company_name),
                        'source_url': url,
                    })
                continue

            try:
                client = anthropic.Anthropic()
                prompt = f"""Extract all executives/leaders from this snippet about {company_name}.

Snippet: {snippet}

Return ONLY valid JSON array. Each element: {{"name": "...", "title": "..."}}
If no executives found, return []"""

                resp = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = resp.content[0].text.strip()
                if text.startswith('['):
                    execs = json.loads(text)
                    for ex in execs:
                        if ex.get('name') and ex.get('title'):
                            cat, pri = classify_title(ex['title'])
                            executives_found.append({
                                'person_name': ex['name'],
                                'title': ex['title'],
                                'title_category': cat,
                                'priority': pri,
                                'company_name': company_name,
                                'company_id': company_id or _link_company_id(
                                    cur, company_name),
                                'source_url': url,
                            })
            except Exception as e:
                logger.debug(f"LLM structure parse failed: {e}")

        time.sleep(1)

    # Dedupe by person name
    seen = set()
    unique = []
    for ex in executives_found:
        key = ex['person_name'].lower()
        if key not in seen:
            seen.add(key)
            unique.append(ex)

    # Insert into executives table
    if not dry_run:
        today = datetime.now().strftime('%Y-%m-%d')
        for ex in unique:
            # Check if already exists
            cur.execute("""
                SELECT id FROM executives
                WHERE LOWER(person_name) = LOWER(?)
                AND company_id = ?
            """, (ex['person_name'], ex.get('company_id')))
            if cur.fetchone():
                # Update last_verified
                cur.execute("""
                    UPDATE executives SET last_verified = ?, title = ?,
                    title_category = ?, priority = ?
                    WHERE LOWER(person_name) = LOWER(?) AND company_id = ?
                """, (today, ex['title'], ex['title_category'], ex['priority'],
                      ex['person_name'], ex.get('company_id')))
            else:
                cur.execute("""
                    INSERT INTO executives
                    (company_id, company_name, person_name, title,
                     title_category, priority, last_verified, bio_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (ex.get('company_id'), company_name, ex['person_name'],
                      ex['title'], ex['title_category'], ex['priority'],
                      today, ex.get('source_url')))

        conn.commit()

    conn.close()
    logger.info(f"Found {len(unique)} executives at {company_name}")
    return unique


# =============================================================================
# QUERY HELPERS
# =============================================================================

def get_executives(company_id: int = None, priority: str = None,
                   db_path: str = None) -> List[Dict]:
    """Get executives, optionally filtered by company and/or priority."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = "SELECT e.*, c.name as linked_company FROM executives e " \
            "LEFT JOIN companies c ON e.company_id = c.id WHERE 1=1"
    params = []

    if company_id:
        query += " AND e.company_id = ?"
        params.append(company_id)
    if priority:
        query += " AND e.priority = ?"
        params.append(priority)

    query += " ORDER BY e.priority ASC, e.title_category ASC, e.person_name ASC"

    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_recent_changes(days: int = 7, priority: str = None,
                       company_id: int = None, db_path: str = None) -> List[Dict]:
    """Get recent executive changes."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = """
        SELECT ec.*, c.name as linked_company
        FROM executive_changes ec
        LEFT JOIN companies c ON ec.company_id = c.id
        WHERE ec.created_at >= datetime('now', ?)
    """
    params = [f'-{days} days']

    if priority:
        query += " AND ec.priority = ?"
        params.append(priority)
    if company_id:
        query += " AND ec.company_id = ?"
        params.append(company_id)

    query += " ORDER BY ec.effective_date DESC, ec.created_at DESC"

    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_executive_briefing_data(db_path: str = None) -> Dict:
    """Get executive data for morning briefing."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Changes in last 24 hours
    cur.execute("""
        SELECT * FROM executive_changes
        WHERE created_at >= datetime('now', '-1 day')
        ORDER BY priority ASC, effective_date DESC
    """)
    recent = [dict(r) for r in cur.fetchall()]

    high = [r for r in recent if r['priority'] == 'high']
    medium = [r for r in recent if r['priority'] == 'medium']

    conn.close()

    return {
        'high_priority_changes': high,
        'medium_count': len(medium),
        'total_24h': len(recent),
    }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Executive Scanner')
    parser.add_argument('--company', help='Company name to scan')
    parser.add_argument('--all', action='store_true', help='Scan all companies')
    parser.add_argument('--prospects', action='store_true',
                        help='Scan prospects and targets only')
    parser.add_argument('--major-firms', action='store_true',
                        help='Scan major institutional firms')
    parser.add_argument('--find-structure', action='store_true',
                        help='Deep scan for full leadership structure')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse but do not insert')
    parser.add_argument('--max', type=int, default=None,
                        help='Max companies to scan')
    parser.add_argument('--recent', type=int, default=None,
                        help='Show recent changes (days)')

    args = parser.parse_args()

    if args.recent is not None:
        changes = get_recent_changes(days=args.recent)
        if not changes:
            print("No recent executive changes found.")
            return
        for c in changes:
            badge = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(
                c.get('priority', ''), '')
            from_part = f" (from {c['old_company']})" if c.get('old_company') else ""
            print(f"{badge} {c['person_name']} → "
                  f"{c.get('new_title', '?')} at {c['company_name']}{from_part} "
                  f"[{c.get('effective_date', '?')}]")
        return

    if args.find_structure and args.company:
        execs = find_executive_structure(company_name=args.company,
                                         dry_run=args.dry_run)
        print(f"\nExecutive Structure: {args.company}")
        print("=" * 50)
        for ex in execs:
            badge = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(
                ex.get('priority', ''), '')
            print(f"  {badge} {ex['person_name']} — {ex['title']}")
        return

    if args.company:
        changes = scan_company_executives(company_name=args.company,
                                           dry_run=args.dry_run)
        for c in changes:
            badge = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(
                c.get('priority', ''), '')
            print(f"{badge} {c['person_name']} → {c.get('new_title', '?')} "
                  f"at {c['company_name']}")
        if not changes:
            print(f"No new executive changes found for {args.company}")
        return

    if args.all:
        filt = 'all'
    elif args.major_firms:
        filt = 'major_firms'
    else:
        filt = 'prospects_and_targets'

    result = scan_all_executives(company_filter=filt, dry_run=args.dry_run,
                                  max_companies=args.max)
    print(f"\nExecutive Scanner Results")
    print(f"{'=' * 40}")
    print(f"Companies scanned: {result['total_scanned']}")
    print(f"Changes found:     {result['total_changes']}")
    print(f"High priority:     {result['high_priority']}")

    for c in result['changes']:
        badge = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(
            c.get('priority', ''), '')
        print(f"  {badge} {c['person_name']} → {c.get('new_title', '?')} "
              f"at {c['company_name']}")


if __name__ == '__main__':
    main()
