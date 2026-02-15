#!/usr/bin/env python3
"""
Relationship Engine — Funding Scraper
Searches for recent funding rounds for tracked companies via Brave Search API.
Usage: python3 -m scrapers.funding_scraper (from ~/relationship_engine/)
       python3 scrapers/funding_scraper.py
"""

import sqlite3
import os
import re
import json
import requests
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/relationship_engine/.env"))

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def search_brave(query, count=5):
    """Search Brave and return results."""
    if not BRAVE_API_KEY:
        print("  ⚠ BRAVE_API_KEY not set in .env")
        return []
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY
    }
    params = {"q": query, "count": count, "freshness": "pm"}  # past month
    try:
        resp = requests.get(BRAVE_SEARCH_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("web", {}).get("results", [])
    except Exception as e:
        print(f"  ⚠ Search error: {e}")
        return []

def parse_funding_amount(text):
    """Try to extract a dollar amount from text."""
    patterns = [
        r'\$(\d+(?:\.\d+)?)\s*[Bb]illion', 
        r'\$(\d+(?:\.\d+)?)\s*[Mm]illion',
        r'\$(\d+(?:\.\d+)?)\s*[Mm]',
        r'\$(\d+(?:\.\d+)?)\s*[Bb]',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            amount = float(match.group(1))
            if 'billion' in pattern.lower() or pattern.endswith("[Bb]'):"):
                return amount * 1_000_000_000
            if 'b]' in pattern.lower():
                return amount * 1_000_000_000
            return amount * 1_000_000
    return None

def parse_round_type(text):
    """Try to extract round type from text."""
    text_lower = text.lower()
    round_types = [
        'series a', 'series b', 'series c', 'series d', 'series e', 'series f',
        'seed', 'pre-seed', 'growth equity', 'growth round',
        'ipo', 'spac', 'debt financing', 'bridge round',
        'series a+', 'series b+', 'series c+',
    ]
    for rt in round_types:
        if rt in text_lower:
            return rt.title()
    if 'funding' in text_lower or 'raised' in text_lower or 'round' in text_lower:
        return 'Unknown Round'
    return None

def is_duplicate(conn, company_id, source_url):
    """Check if we already have this funding event."""
    if not source_url:
        return False
    row = conn.execute(
        "SELECT id FROM funding_events WHERE company_id = ? AND source_url = ?",
        (company_id, source_url)
    ).fetchone()
    return row is not None

def run_scraper(verbose=True):
    """Main scraper function."""
    conn = get_db()
    
    # Get target companies
    companies = conn.execute(
        "SELECT id, name FROM companies WHERE status IN ('high_growth_target', 'prospect', 'watching')"
    ).fetchall()
    
    if verbose:
        print(f"\n  Scanning {len(companies)} companies for funding events...")
    
    total_added = 0
    
    for company in companies:
        query = f"{company['name']} funding round 2025 2026"
        if verbose:
            print(f"\n  Searching: {company['name']}")
        
        results = search_brave(query)
        
        for result in results:
            title = result.get("title", "")
            description = result.get("description", "")
            url = result.get("url", "")
            combined = f"{title} {description}"
            
            # Skip if not actually about funding
            funding_keywords = ['funding', 'raised', 'series', 'round', 'investment', 'valuation', 'ipo', 'venture']
            if not any(kw in combined.lower() for kw in funding_keywords):
                continue
            
            # Skip if not about this company
            if company['name'].lower() not in combined.lower():
                continue
            
            # Skip duplicates
            if is_duplicate(conn, company['id'], url):
                if verbose:
                    print(f"    ↳ Skip (duplicate): {title[:60]}")
                continue
            
            amount = parse_funding_amount(combined)
            round_type = parse_round_type(combined)
            
            if round_type:
                conn.execute(
                    "INSERT INTO funding_events (company_id, event_date, round_type, amount, "
                    "source_url, notes) VALUES (?, ?, ?, ?, ?, ?)",
                    (company['id'], date.today().isoformat(), round_type, amount,
                     url, f"Auto-scraped: {title[:200]}")
                )
                conn.commit()
                total_added += 1
                if verbose:
                    amt_str = f"${amount:,.0f}" if amount else "amount unknown"
                    print(f"    ✓ {round_type} — {amt_str}")
    
    if verbose:
        print(f"\n  Done. {total_added} new funding events added.")
    
    conn.close()
    return total_added

if __name__ == "__main__":
    run_scraper()
