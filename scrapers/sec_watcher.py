"""
SEC Filing Watcher — Monitor target companies for relevant SEC filings.

Uses public SEC EDGAR API (no API key required).
Monitors: 10-K, 10-Q, 8-K, S-1
Extracts: lease commitments, office mentions, headcount, expansion plans
"""

import os
import sys
import json
import sqlite3
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path

EDGAR_COMPANY_URL = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt={start}&enddt={end}&forms={forms}"
EDGAR_FILINGS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# SEC requires a user-agent header
HEADERS = {
    "User-Agent": "RelationshipEngine/1.0 (graham@cbre.com)",
    "Accept": "application/json"
}

# Filing types we care about
MONITORED_FORMS = ['10-K', '10-Q', '8-K', 'S-1', 'S-1/A']

# Keywords that signal CRE relevance
RE_KEYWORDS = [
    r'office\s+(?:space|lease|expansion|relocation|consolidation)',
    r'square\s+feet',
    r'(?:new|additional)\s+(?:office|headquarters)',
    r'lease\s+(?:commitment|obligation|agreement|renewal|expir)',
    r'headcount\s+(?:growth|increase|reduction)',
    r'(?:expand|relocat|consolidat)(?:e|ed|ing|ion)',
    r'real\s+estate',
    r'operating\s+leases?',
    r'right.of.use\s+assets?',
]

RE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in RE_KEYWORDS]


def _get_conn(db_path=None):
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_json(url):
    """Fetch JSON from URL with proper headers."""
    try:
        import urllib.request
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  Fetch error: {e}")
        return None


def get_company_cik(company_name):
    """Look up CIK number for a company from SEC EDGAR."""
    url = f"https://efts.sec.gov/LATEST/search-index?q=%22{company_name}%22&forms=10-K"
    data = _fetch_json(url)
    if data and data.get('hits', {}).get('hits'):
        first = data['hits']['hits'][0]
        source = first.get('_source', {})
        return source.get('entity_id'), source.get('entity_name')
    return None, None


def get_recent_filings(cik, forms=None, days_back=90):
    """Get recent filings for a CIK number."""
    if not cik:
        return []

    # Pad CIK to 10 digits
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    data = _fetch_json(url)
    if not data:
        return []

    filings = []
    recent = data.get('filings', {}).get('recent', {})
    if not recent:
        return []

    forms_list = recent.get('form', [])
    dates = recent.get('filingDate', [])
    accessions = recent.get('accessionNumber', [])
    descriptions = recent.get('primaryDocDescription', [])

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    target_forms = set(forms or MONITORED_FORMS)

    for i in range(len(forms_list)):
        form_type = forms_list[i] if i < len(forms_list) else ''
        filing_date = dates[i] if i < len(dates) else ''
        accession = accessions[i] if i < len(accessions) else ''
        desc = descriptions[i] if i < len(descriptions) else ''

        if form_type not in target_forms:
            continue
        if filing_date < cutoff:
            continue

        accession_clean = accession.replace('-', '')
        filing_url = (f"https://www.sec.gov/Archives/edgar/data/"
                     f"{cik_padded}/{accession_clean}/{accession}-index.htm")

        filings.append({
            'filing_type': form_type,
            'filing_date': filing_date,
            'description': desc,
            'filing_url': filing_url,
            'accession': accession,
        })

    return filings


def scan_filing_for_re_signals(text):
    """Scan text for real estate signals. Returns list of matched keywords."""
    matches = []
    for pattern in RE_PATTERNS:
        found = pattern.findall(text)
        if found:
            matches.extend(found)
    return list(set(matches))


def scan_company(company_id, company_name, cik=None, db_path=None, dry_run=False):
    """Scan SEC filings for a single company."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    results = []

    # Look up CIK if not provided
    if not cik:
        cik, _ = get_company_cik(company_name)

    if not cik:
        conn.close()
        return results

    filings = get_recent_filings(cik, days_back=90)

    for f in filings:
        # Check for duplicate
        cur.execute("""
            SELECT id FROM sec_filings
            WHERE company_id = ? AND filing_type = ? AND filing_date = ?
        """, (company_id, f['filing_type'], f['filing_date']))

        if cur.fetchone():
            continue

        filing_record = {
            'company_name': company_name,
            'company_id': company_id,
            'cik': str(cik),
            'filing_type': f['filing_type'],
            'filing_date': f['filing_date'],
            'description': f['description'],
            'filing_url': f['filing_url'],
        }

        if not dry_run:
            cur.execute("""
                INSERT INTO sec_filings
                (company_name, company_id, cik, filing_type, filing_date,
                 description, filing_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (company_name, company_id, str(cik), f['filing_type'],
                  f['filing_date'], f['description'], f['filing_url']))

        results.append(filing_record)

    if not dry_run:
        conn.commit()
    conn.close()
    return results


def scan_all_companies(db_path=None, dry_run=False, max_companies=50):
    """Scan SEC filings for all prospect/target companies."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name FROM companies
        WHERE status IN ('prospect', 'high_growth_target', 'active_client', 'watching')
        LIMIT ?
    """, (max_companies,))
    companies = [(r['id'], r['name']) for r in cur.fetchall()]
    conn.close()

    total_new = 0
    for company_id, company_name in companies:
        print(f"  Scanning: {company_name}...")
        results = scan_company(company_id, company_name, db_path=db_path, dry_run=dry_run)
        total_new += len(results)
        for r in results:
            print(f"    New: {r['filing_type']} ({r['filing_date']})")

    return {'companies_scanned': len(companies), 'new_filings': total_new}


def get_recent_filings_db(days=30, company_id=None, db_path=None):
    """Get recently discovered filings from database."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    conditions = [f"filing_date >= date('now', '-{days} days')"]
    params = []

    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)

    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT * FROM sec_filings
        WHERE {where}
        ORDER BY filing_date DESC
    """, params)

    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='SEC Filing Watcher')
    parser.add_argument('--company', help='Company name to scan')
    parser.add_argument('--all', action='store_true', help='Scan all target companies')
    parser.add_argument('--recent', type=int, default=30, help='Show recent filings (days)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--max', type=int, default=50)
    args = parser.parse_args()

    if args.company:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM companies WHERE name LIKE ?", (f"%{args.company}%",))
        row = cur.fetchone()
        conn.close()
        if row:
            results = scan_company(row['id'], row['name'], dry_run=args.dry_run)
            print(f"Found {len(results)} new filings for {row['name']}")
        else:
            print(f"Company not found: {args.company}")

    elif args.all:
        stats = scan_all_companies(dry_run=args.dry_run, max_companies=args.max)
        print(f"\nScanned {stats['companies_scanned']} companies, "
              f"found {stats['new_filings']} new filings")

    else:
        filings = get_recent_filings_db(days=args.recent)
        if filings:
            for f in filings:
                signal = "📄" if not f.get('office_expansion_signal') else "🏢"
                print(f"{signal} {f['filing_date']} | {f['company_name']} | "
                      f"{f['filing_type']} | {f.get('description', '')[:60]}")
        else:
            print("No recent filings. Run --all to scan.")
