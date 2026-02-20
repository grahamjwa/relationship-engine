"""
YC Company Monitor — Track Y Combinator companies for CRE opportunities.

Scrapes public YC directory (ycombinator.com/companies).
Filters: NYC-based, fintech, >20 employees.
Auto-adds promising ones to companies table as prospects.
"""

import os
import sys
import json
import sqlite3
import re
from datetime import datetime
from typing import Optional, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path

YC_API_URL = "https://yc-oss.github.io/api/batches/{batch}.json"
YC_COMPANIES_URL = "https://yc-oss.github.io/api/companies/all.json"

# Target criteria
TARGET_CITIES = ['new york', 'nyc', 'manhattan', 'brooklyn']
TARGET_SECTORS = ['fintech', 'finance', 'insurance', 'real estate', 'proptech',
                  'enterprise', 'b2b', 'saas']
MIN_HEADCOUNT = 20


def _get_conn(db_path=None):
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_json(url):
    """Fetch JSON from URL."""
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={
            "User-Agent": "RelationshipEngine/1.0"
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  Fetch error: {e}")
        return None


def fetch_yc_companies():
    """Fetch all YC companies from public API."""
    data = _fetch_json(YC_COMPANIES_URL)
    if data and isinstance(data, list):
        return data
    return []


def is_target_company(company):
    """Check if a YC company meets our target criteria."""
    # City check
    location = (company.get('location', '') or '').lower()
    city_match = any(c in location for c in TARGET_CITIES)

    # Sector check
    tags = (company.get('tags', []) or [])
    if isinstance(tags, str):
        tags = [tags]
    description = (company.get('one_liner', '') or company.get('description', '') or '').lower()
    tags_lower = [t.lower() for t in tags]

    sector_match = (
        any(s in description for s in TARGET_SECTORS) or
        any(s in t for s in TARGET_SECTORS for t in tags_lower)
    )

    # Headcount check
    headcount = company.get('team_size', 0) or 0

    return city_match and (sector_match or headcount >= MIN_HEADCOUNT)


def sync_yc_companies(db_path=None, dry_run=False, force=False):
    """Sync YC companies to database. Only adds new ones."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    print("Fetching YC companies...")
    companies = fetch_yc_companies()
    print(f"  Fetched {len(companies)} total YC companies")

    new_count = 0
    target_count = 0

    for co in companies:
        name = co.get('name', '')
        if not name:
            continue

        is_target = is_target_company(co)
        if is_target:
            target_count += 1

        # Check if already in yc_companies
        cur.execute("SELECT id FROM yc_companies WHERE company_name = ?", (name,))
        if cur.fetchone() and not force:
            continue

        batch = co.get('batch', '')
        website = co.get('website', co.get('url', ''))
        description = co.get('one_liner', co.get('description', ''))
        sector = ', '.join(co.get('tags', [])[:3]) if co.get('tags') else ''
        headcount = co.get('team_size', 0) or 0
        location = co.get('location', '')
        funding_stage = co.get('stage', '')

        if not dry_run:
            cur.execute("""
                INSERT OR REPLACE INTO yc_companies
                (company_name, yc_batch, website, description, sector,
                 headcount_est, funding_stage, hq_city, is_target, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
            """, (name, batch, website, description, sector,
                  headcount, funding_stage, location, 1 if is_target else 0))

        new_count += 1

    if not dry_run:
        conn.commit()

    conn.close()
    return {
        'total_fetched': len(companies),
        'targets_found': target_count,
        'new_added': new_count,
    }


def promote_to_prospects(db_path=None, dry_run=False):
    """Add targeted YC companies to main companies table as prospects."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM yc_companies
        WHERE is_target = 1
    """)
    targets = [dict(r) for r in cur.fetchall()]

    promoted = 0
    for yc in targets:
        # Check if already in companies
        cur.execute("SELECT id FROM companies WHERE name LIKE ?",
                   (f"%{yc['company_name']}%",))
        if cur.fetchone():
            continue

        if not dry_run:
            cur.execute("""
                INSERT INTO companies (name, type, status, sector, hq_city, notes)
                VALUES (?, 'tenant', 'prospect', ?, ?, ?)
            """, (yc['company_name'],
                  yc.get('sector', 'tech'),
                  yc.get('hq_city', 'New York'),
                  f"YC {yc.get('yc_batch', '')} | {yc.get('description', '')[:100]}"))

        promoted += 1
        print(f"  Promoted: {yc['company_name']} (YC {yc.get('yc_batch', '')})")

    if not dry_run:
        conn.commit()

    conn.close()
    return promoted


def get_yc_targets(db_path=None):
    """Get all targeted YC companies."""
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM yc_companies
        WHERE is_target = 1
        ORDER BY headcount_est DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='YC Company Monitor')
    parser.add_argument('--sync', action='store_true', help='Sync from YC directory')
    parser.add_argument('--promote', action='store_true', help='Promote targets to prospects')
    parser.add_argument('--targets', action='store_true', help='Show current targets')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.sync:
        stats = sync_yc_companies(dry_run=args.dry_run)
        print(f"\nFetched: {stats['total_fetched']}")
        print(f"Targets: {stats['targets_found']}")
        print(f"New added: {stats['new_added']}")

    elif args.promote:
        count = promote_to_prospects(dry_run=args.dry_run)
        print(f"\nPromoted {count} YC companies to prospects")

    elif args.targets:
        targets = get_yc_targets()
        if targets:
            for t in targets[:20]:
                print(f"  {t['company_name']} (YC {t.get('yc_batch', '?')}) — "
                      f"{t.get('headcount_est', '?')} employees — {t.get('hq_city', '?')}")
        else:
            print("No targets. Run --sync first.")

    else:
        parser.print_help()
