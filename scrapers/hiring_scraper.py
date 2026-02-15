#!/usr/bin/env python3
"""
Relationship Engine — Hiring Signal Scraper
Searches for hiring signals and office expansion news via Brave Search API.
Usage: python3 -m scrapers.hiring_scraper (from ~/relationship_engine/)
       python3 scrapers/hiring_scraper.py
"""

import sqlite3
import os
import re
import requests
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/relationship_engine/.env"))

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"

# These keywords trigger high relevance — RE decision maker hires or explicit expansion
HIGH_RELEVANCE_KEYWORDS = [
    'vp real estate', 'vice president real estate',
    'head of real estate', 'director of real estate',
    'head of workplace', 'director of workplace',
    'head of facilities', 'director of facilities',
    'office expansion', 'new office', 'new headquarters',
    'relocating', 'expanding office', 'lease sign',
    'square feet', 'sf office',
]

MEDIUM_RELEVANCE_KEYWORDS = [
    'hiring new york', 'hiring nyc', 'hiring manhattan',
    'headcount growth', 'expanding team', 'new jobs',
    'opening office',
]

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
    params = {"q": query, "count": count, "freshness": "pm"}
    try:
        resp = requests.get(BRAVE_SEARCH_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("web", {}).get("results", [])
    except Exception as e:
        print(f"  ⚠ Search error: {e}")
        return []

def classify_relevance(text):
    """Classify signal relevance based on keywords."""
    text_lower = text.lower()
    for kw in HIGH_RELEVANCE_KEYWORDS:
        if kw in text_lower:
            return 'high'
    for kw in MEDIUM_RELEVANCE_KEYWORDS:
        if kw in text_lower:
            return 'medium'
    return 'low'

def classify_signal_type(text):
    """Determine signal type from text."""
    text_lower = text.lower()
    if any(kw in text_lower for kw in ['hiring', 'job posting', 'job opening', 'careers', 'recruiting']):
        if any(kw in text_lower for kw in ['vp', 'head of', 'director', 'chief', 'svp', 'evp']):
            return 'leadership_hire'
        return 'job_posting'
    if any(kw in text_lower for kw in ['new office', 'expansion', 'relocat', 'headquarter', 'lease']):
        return 'new_office'
    if any(kw in text_lower for kw in ['headcount', 'growing team', 'employees']):
        return 'headcount_growth'
    if any(kw in text_lower for kw in ['announce', 'press release', 'news']):
        return 'press_announcement'
    return 'press_announcement'

def extract_role_title(text):
    """Try to extract a specific role title."""
    patterns = [
        r'(VP|Vice President|Head|Director|Chief|SVP|EVP)\s+(?:of\s+)?(Real Estate|Workplace|Facilities|Operations|Office)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None

def is_duplicate(conn, company_id, source_url):
    """Check if we already have this signal."""
    if not source_url:
        return False
    row = conn.execute(
        "SELECT id FROM hiring_signals WHERE company_id = ? AND source_url = ?",
        (company_id, source_url)
    ).fetchone()
    return row is not None

def run_scraper(verbose=True):
    """Main scraper function."""
    conn = get_db()
    
    companies = conn.execute(
        "SELECT id, name FROM companies WHERE status IN ('high_growth_target', 'prospect', 'watching')"
    ).fetchall()
    
    if verbose:
        print(f"\n  Scanning {len(companies)} companies for hiring signals...")
    
    total_added = 0
    
    for company in companies:
        query = f"{company['name']} hiring New York office expansion"
        if verbose:
            print(f"\n  Searching: {company['name']}")
        
        results = search_brave(query)
        
        for result in results:
            title = result.get("title", "")
            description = result.get("description", "")
            url = result.get("url", "")
            combined = f"{title} {description}"
            
            # Skip if not about this company
            if company['name'].lower() not in combined.lower():
                continue
            
            # Skip if not actually about hiring/expansion
            hiring_keywords = ['hiring', 'job', 'office', 'expansion', 'headcount', 'team', 'lease',
                             'relocat', 'headquarter', 'workplace', 'facilities']
            if not any(kw in combined.lower() for kw in hiring_keywords):
                continue
            
            # Skip duplicates
            if is_duplicate(conn, company['id'], url):
                if verbose:
                    print(f"    ↳ Skip (duplicate): {title[:60]}")
                continue
            
            relevance = classify_relevance(combined)
            signal_type = classify_signal_type(combined)
            role_title = extract_role_title(combined)
            
            conn.execute(
                "INSERT INTO hiring_signals (company_id, signal_date, signal_type, role_title, "
                "location, details, source_url, relevance, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (company['id'], date.today().isoformat(), signal_type, role_title,
                 "New York, NY", f"{title[:200]}", url, relevance,
                 f"Auto-scraped: {description[:200]}")
            )
            conn.commit()
            total_added += 1
            if verbose:
                icon = "🔴" if relevance == 'high' else "🟡" if relevance == 'medium' else "⚪"
                print(f"    {icon} [{relevance}] {signal_type}: {title[:60]}")
    
    if verbose:
        print(f"\n  Done. {total_added} new hiring signals added.")
    
    conn.close()
    return total_added

if __name__ == "__main__":
    run_scraper()
