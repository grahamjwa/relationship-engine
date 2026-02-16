"""
Signal Pipeline for Relationship Engine
Orchestrates search, classification, and database insertion.
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

from search_client import search_funding, search_hiring, search_lease, get_remaining_searches
from signal_classifier import classify_batch
from graph_engine import get_db_path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def get_target_companies(db_path: Optional[str] = None) -> List[Dict]:
    """
    Get companies to scan from database.
    Returns companies with status: high_growth_target, prospect, watching
    """
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, name, status, sector
        FROM companies
        WHERE status IN ('high_growth_target', 'prospect', 'watching')
        ORDER BY 
            CASE status 
                WHEN 'high_growth_target' THEN 1 
                WHEN 'prospect' THEN 2 
                ELSE 3 
            END
    """)
    
    companies = [dict(row) for row in cur.fetchall()]
    conn.close()
    
    return companies


def insert_funding_event(
    db_path: str,
    company_id: int,
    round_type: str,
    amount: Optional[str],
    lead_investor: Optional[str],
    source_url: str,
    confidence: float
) -> int:
    """Insert a funding event into the database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Parse amount to float first
    amount_float = None
    if amount:
        try:
            cleaned = amount.replace('$', '').replace(',', '').replace(' million', '000000').replace(' billion', '000000000')
            amount_float = float(cleaned)
        except:
            amount_float = None
    
    # Check for duplicate: same URL OR same company + similar amount within 7 days
    cur.execute("""
        SELECT id FROM funding_events 
        WHERE company_id = ? AND (
            source_url = ?
            OR (
                amount IS NOT NULL 
                AND ? IS NOT NULL
                AND ABS(amount - ?) < (? * 0.15)
                AND event_date >= date('now', '-7 days')
            )
        )
    """, (company_id, source_url, amount_float, amount_float, amount_float if amount_float else 1))
    
    if cur.fetchone():
        conn.close()
        return -1  # Duplicate
    
    cur.execute("""
        INSERT INTO funding_events 
            (company_id, round_type, amount, lead_investor, event_date, source_url)
        VALUES (?, ?, ?, ?, date('now'), ?)
    """, (company_id, round_type, amount_float, lead_investor, source_url))
    
    event_id = cur.lastrowid
    conn.commit()
    conn.close()
    
    return event_id


def insert_hiring_signal(
    db_path: str,
    company_id: int,
    signal_type: str,
    description: str,
    relevance: str,
    source_url: str,
    confidence: float
) -> int:
    """Insert a hiring signal into the database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Check for duplicate
    cur.execute("""
        SELECT id FROM hiring_signals 
        WHERE company_id = ? AND source_url = ?
    """, (company_id, source_url))
    
    if cur.fetchone():
        conn.close()
        return -1  # Duplicate
    
    # Map signal_type to valid enum values
    valid_types = ['job_posting', 'headcount_growth', 'new_office', 'leadership_hire', 'press_announcement']
    if signal_type == 'hiring':
        signal_type = 'job_posting'
    elif signal_type == 'expansion':
        signal_type = 'new_office'
    elif signal_type not in valid_types:
        signal_type = 'press_announcement'
    
    # Ensure relevance is valid
    if relevance not in ('high', 'medium', 'low'):
        relevance = 'medium'
    
    cur.execute("""
        INSERT INTO hiring_signals 
            (company_id, signal_type, details, relevance, signal_date, source_url)
        VALUES (?, ?, ?, ?, date('now'), ?)
    """, (company_id, signal_type, description, relevance, source_url))
    
    signal_id = cur.lastrowid
    conn.commit()
    conn.close()
    
    return signal_id


def log_scan(
    db_path: str,
    company_id: int,
    scan_type: str,
    results_found: int,
    signals_inserted: int,
    tokens_used: int
):
    """Log a scan to the scan_log table."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            scan_type TEXT,
            results_found INTEGER,
            signals_inserted INTEGER,
            tokens_used INTEGER,
            scanned_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        INSERT INTO scan_log (company_id, scan_type, results_found, signals_inserted, tokens_used)
        VALUES (?, ?, ?, ?, ?)
    """, (company_id, scan_type, results_found, signals_inserted, tokens_used))
    
    conn.commit()
    conn.close()


def scan_company(
    company_id: int,
    company_name: str,
    scan_types: List[str] = ["funding", "hiring"],
    db_path: Optional[str] = None,
    verbose: bool = True
) -> Dict:
    """
    Scan a single company for signals.
    
    Args:
        company_id: Database ID of company
        company_name: Name of company
        scan_types: List of scan types: "funding", "hiring", "lease"
        db_path: Path to database
        verbose: Print progress
    
    Returns:
        Summary dict with counts
    """
    if db_path is None:
        db_path = get_db_path()
    
    summary = {
        "company": company_name,
        "funding_found": 0,
        "hiring_found": 0,
        "lease_found": 0,
        "total_inserted": 0,
        "tokens_used": 0
    }
    
    if verbose:
        print(f"\nScanning: {company_name}")
    
    # Funding scan
    if "funding" in scan_types:
        if verbose:
            print(f"  Searching funding...")
        
        results = search_funding(company_name)
        if results:
            classified = classify_batch(company_name, results, skip_irrelevant=True)
            
            for item in classified:
                cls = item.get("classification", {})
                if cls.get("category") == "funding" and cls.get("confidence", 0) >= 0.7:
                    event_id = insert_funding_event(
                        db_path=db_path,
                        company_id=company_id,
                        round_type=cls.get("details", {}).get("amount", "unknown"),
                        amount=cls.get("details", {}).get("amount"),
                        lead_investor=cls.get("details", {}).get("investor"),
                        source_url=item.get("link", ""),
                        confidence=cls.get("confidence", 0)
                    )
                    if event_id > 0:
                        summary["funding_found"] += 1
                        summary["total_inserted"] += 1
                
                summary["tokens_used"] += cls.get("_tokens_used", 0)
        
        log_scan(db_path, company_id, "funding", len(results), summary["funding_found"], summary["tokens_used"])
    
    # Hiring scan
    if "hiring" in scan_types:
        if verbose:
            print(f"  Searching hiring signals...")
        
        results = search_hiring(company_name)
        if results:
            classified = classify_batch(company_name, results, skip_irrelevant=True)
            
            for item in classified:
                cls = item.get("classification", {})
                if cls.get("category") in ("hiring", "expansion") and cls.get("confidence", 0) >= 0.7:
                    # Determine relevance
                    role = str(cls.get("details", {}).get("role", "") or "").lower()
                    if any(x in role for x in ["real estate", "facilities", "workplace", "office"]):
                        relevance = "high"
                    else:
                        relevance = "medium"
                    
                    signal_id = insert_hiring_signal(
                        db_path=db_path,
                        company_id=company_id,
                        signal_type=cls.get("category"),
                        description=cls.get("summary", ""),
                        relevance=relevance,
                        source_url=item.get("link", ""),
                        confidence=cls.get("confidence", 0)
                    )
                    if signal_id > 0:
                        summary["hiring_found"] += 1
                        summary["total_inserted"] += 1
                
                summary["tokens_used"] += cls.get("_tokens_used", 0)
        
        log_scan(db_path, company_id, "hiring", len(results), summary["hiring_found"], summary["tokens_used"])
    
    if verbose:
        print(f"  Found: {summary['funding_found']} funding, {summary['hiring_found']} hiring")
    
    return summary


def run_signal_scan(
    scan_types: List[str] = ["funding", "hiring"],
    max_companies: int = 10,
    db_path: Optional[str] = None,
    verbose: bool = True
) -> Dict:
    """
    Run signal scan across all target companies.
    
    Args:
        scan_types: Types of scans to run
        max_companies: Maximum companies to scan (for quota management)
        db_path: Path to database
        verbose: Print progress
    
    Returns:
        Summary of all scans
    """
    if db_path is None:
        db_path = get_db_path()
    
    # Check quota
    account = get_remaining_searches()
    if "error" not in account:
        remaining = account.get("plan_searches_left", 0)
        if verbose:
            print(f"SerpApi searches remaining: {remaining}")
        if remaining < max_companies * len(scan_types):
            print(f"Warning: Low quota. Limiting scan.")
            max_companies = remaining // len(scan_types)
    
    companies = get_target_companies(db_path)[:max_companies]
    
    if verbose:
        print(f"\nScanning {len(companies)} companies for: {', '.join(scan_types)}")
    
    total_summary = {
        "companies_scanned": 0,
        "funding_found": 0,
        "hiring_found": 0,
        "total_inserted": 0,
        "tokens_used": 0
    }
    
    for company in companies:
        result = scan_company(
            company_id=company["id"],
            company_name=company["name"],
            scan_types=scan_types,
            db_path=db_path,
            verbose=verbose
        )
        
        total_summary["companies_scanned"] += 1
        total_summary["funding_found"] += result["funding_found"]
        total_summary["hiring_found"] += result["hiring_found"]
        total_summary["total_inserted"] += result["total_inserted"]
        total_summary["tokens_used"] += result["tokens_used"]
    
    if verbose:
        print(f"\n--- Scan Complete ---")
        print(f"Companies scanned: {total_summary['companies_scanned']}")
        print(f"Funding events found: {total_summary['funding_found']}")
        print(f"Hiring signals found: {total_summary['hiring_found']}")
        print(f"Total inserted: {total_summary['total_inserted']}")
        print(f"Tokens used: {total_summary['tokens_used']}")
    
    # Post to Discord if webhook configured
    _post_scan_summary(total_summary)
    
    return total_summary


def _post_scan_summary(summary: Dict):
    """Post scan summary to Discord."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url or not HAS_REQUESTS:
        return
    
    message = (
        f"**Signal Scan Complete**\n"
        f"Companies scanned: {summary['companies_scanned']}\n"
        f"Funding events: {summary['funding_found']}\n"
        f"Hiring signals: {summary['hiring_found']}\n"
        f"Total new records: {summary['total_inserted']}\n"
        f"Tokens used: {summary['tokens_used']}"
    )
    
    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception:
        pass


if __name__ == "__main__":
    # Test with limited scan
    print("Testing signal pipeline...")
    run_signal_scan(max_companies=2, verbose=True)
