"""
Bulk Hedge Fund Signal Scanner
Scans all 30+ institutional/hedge fund companies for funding and hiring signals.
Designed for manual or scheduled execution when API quota allows.

Usage:
    python jobs/bulk_hedge_fund_scan.py                  # scan all, funding+hiring
    python jobs/bulk_hedge_fund_scan.py --max 10         # limit to 10 companies
    python jobs/bulk_hedge_fund_scan.py --funding-only   # skip hiring scan
    python jobs/bulk_hedge_fund_scan.py --dry-run        # list targets without scanning
"""

import os
import sys
import sqlite3
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import get_db_path
from scrapers.signal_pipeline import scan_company, get_remaining_searches

try:
    from scrapers.executive_tracker import run_movement_scan
    HAS_EXEC_TRACKER = True
except ImportError:
    HAS_EXEC_TRACKER = False


def get_hedge_funds(db_path=None):
    """Get all institutional/hedge fund companies from DB."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, category, status, opportunity_score
        FROM companies
        WHERE category = 'institutional'
           OR sector IN ('hedge_fund', 'private_equity', 'asset_management')
           OR type = 'investor'
        ORDER BY
            CASE status
                WHEN 'high_growth_target' THEN 1
                WHEN 'prospect' THEN 2
                WHEN 'watching' THEN 3
                ELSE 4
            END,
            opportunity_score DESC
    """)
    companies = [dict(row) for row in cur.fetchall()]
    conn.close()
    return companies


def run_bulk_scan(max_companies=None, scan_types=None, db_path=None,
                  dry_run=False, include_exec=False, verbose=True):
    """
    Run bulk signal scan across all hedge fund targets.

    Args:
        max_companies: Cap on number of companies (None = all)
        scan_types: List of scan types (default: ["funding", "hiring"])
        db_path: Path to DB
        dry_run: If True, list targets but don't scan
        include_exec: If True, also run executive movement scan
        verbose: Print progress

    Returns:
        Summary dict
    """
    if db_path is None:
        db_path = get_db_path()
    if scan_types is None:
        scan_types = ["funding", "hiring"]

    companies = get_hedge_funds(db_path)
    if max_companies:
        companies = companies[:max_companies]

    print(f"\n{'='*60}")
    print(f"BULK HEDGE FUND SIGNAL SCAN")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Targets: {len(companies)} companies")
    print(f"Scan types: {', '.join(scan_types)}")
    print(f"{'='*60}\n")

    if dry_run:
        print("DRY RUN — listing targets only:\n")
        for i, c in enumerate(companies, 1):
            score = c.get('opportunity_score') or 0
            print(f"  {i:2d}. {c['name']:40s} | {c['status']:20s} | score={score:.1f}")
        print(f"\nTotal: {len(companies)} companies")
        print(f"Estimated API calls: {len(companies) * len(scan_types)} searches")
        return {"companies": len(companies), "dry_run": True}

    # Check quota before starting
    account = get_remaining_searches()
    if "error" not in account:
        remaining = account.get("plan_searches_left", 0)
        needed = len(companies) * len(scan_types)
        print(f"SerpApi quota: {remaining} remaining, {needed} needed")
        if remaining < needed:
            cap = remaining // max(len(scan_types), 1)
            print(f"WARNING: Insufficient quota. Capping at {cap} companies.")
            companies = companies[:cap]
    else:
        print("Could not check SerpApi quota. Proceeding cautiously.")

    total = {
        "companies_scanned": 0,
        "funding_found": 0,
        "hiring_found": 0,
        "total_inserted": 0,
        "tokens_used": 0,
        "errors": 0,
        "exec_movements": 0
    }

    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}] {company['name']}")
        try:
            result = scan_company(
                company_id=company["id"],
                company_name=company["name"],
                scan_types=scan_types,
                db_path=db_path,
                verbose=verbose
            )
            total["companies_scanned"] += 1
            total["funding_found"] += result.get("funding_found", 0)
            total["hiring_found"] += result.get("hiring_found", 0)
            total["total_inserted"] += result.get("total_inserted", 0)
            total["tokens_used"] += result.get("tokens_used", 0)
        except Exception as e:
            print(f"  ERROR: {e}")
            total["errors"] += 1

    # Executive movement scan (optional)
    if include_exec and HAS_EXEC_TRACKER:
        print(f"\n{'='*60}")
        print("EXECUTIVE MOVEMENT SCAN")
        print(f"{'='*60}")
        try:
            movements = run_movement_scan(db_path)
            total["exec_movements"] = len(movements)
        except Exception as e:
            print(f"Executive scan error: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE")
    print(f"{'='*60}")
    print(f"Companies scanned: {total['companies_scanned']}")
    print(f"Funding events:    {total['funding_found']}")
    print(f"Hiring signals:    {total['hiring_found']}")
    print(f"Total inserted:    {total['total_inserted']}")
    print(f"Errors:            {total['errors']}")
    print(f"Tokens used:       {total['tokens_used']}")
    if include_exec:
        print(f"Exec movements:    {total['exec_movements']}")
    print(f"{'='*60}\n")

    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk hedge fund signal scanner")
    parser.add_argument("--max", type=int, default=None, help="Max companies to scan")
    parser.add_argument("--funding-only", action="store_true", help="Skip hiring scan")
    parser.add_argument("--hiring-only", action="store_true", help="Skip funding scan")
    parser.add_argument("--dry-run", action="store_true", help="List targets without scanning")
    parser.add_argument("--include-exec", action="store_true", help="Also run executive movement scan")
    parser.add_argument("--db", type=str, default=None, help="Path to database")
    args = parser.parse_args()

    scan_types = ["funding", "hiring"]
    if args.funding_only:
        scan_types = ["funding"]
    elif args.hiring_only:
        scan_types = ["hiring"]

    run_bulk_scan(
        max_companies=args.max,
        scan_types=scan_types,
        db_path=args.db,
        dry_run=args.dry_run,
        include_exec=args.include_exec
    )
