"""
Introduction Request Module for Relationship Engine
Finds optimal intro paths and generates professional email drafts.
"""

import os
import sys
import sqlite3
import logging
import argparse
from typing import Optional, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.graph_engine import get_db_path, build_graph, _node_key, find_shortest_path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _get_contact_info(contact_id: int, db_path: str = None) -> Dict:
    """Fetch contact details from database."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.id, c.first_name, c.last_name, c.title, c.email,
            comp.name as company_name, comp.industry
        FROM contacts c
        LEFT JOIN companies comp ON c.company_id = comp.id
        WHERE c.id = ?
    """, (contact_id,))
    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)
    return {}


def _get_team_contacts(db_path: str = None) -> List[Dict]:
    """Get all team contacts for finding intro paths."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.id, c.first_name, c.last_name, c.title,
            COALESCE(comp.name, 'Internal') as company_name
        FROM contacts c
        LEFT JOIN companies comp ON c.company_id = comp.id
        WHERE c.role_level = 'team' OR (comp.id IS NOT NULL AND comp.status = 'team_affiliated')
        ORDER BY c.first_name, c.last_name
    """)
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def find_intro_path(target_contact_id: int, db_path: str = None) -> Optional[Dict]:
    """
    Find the best introduction path from team to target contact.
    Uses graph shortest path to identify the optimal intermediary.

    Returns dict with:
        - path: list of node keys
        - path_names: list of contact/company names
        - hops: number of degrees of separation
        - intermediaries: list of dicts with contact info for each hop
        - error: error message if path not found
    """
    if db_path is None:
        db_path = get_db_path()

    # Get target contact info
    target_info = _get_contact_info(target_contact_id, db_path)
    if not target_info:
        return {"error": f"Target contact {target_contact_id} not found"}

    target_node = _node_key("contact", target_contact_id)

    # Build graph and find best team member as source
    graph = build_graph(db_path)
    team_contacts = _get_team_contacts(db_path)

    best_path = None
    best_hops = float('inf')
    best_source = None

    for team_member in team_contacts:
        team_node = _node_key("contact", team_member["id"])
        path, weight = find_shortest_path(graph, team_node, target_node)

        if path and len(path) - 1 < best_hops:
            best_path = path
            best_hops = len(path) - 1
            best_source = team_member

    if not best_path:
        return {"error": f"No introduction path found to contact {target_contact_id}"}

    # Build intermediaries list with details
    intermediaries = []
    for i, node in enumerate(best_path):
        if node.startswith("contact_"):
            contact_id = int(node.split("_")[1])
            info = _get_contact_info(contact_id, db_path)
            if info:
                intermediaries.append({
                    "position": i,
                    "name": f"{info['first_name']} {info['last_name']}",
                    "title": info.get('title'),
                    "company": info.get('company_name'),
                    "contact_id": contact_id
                })
        elif node.startswith("company_"):
            company_id = int(node.split("_")[1])
            conn = _get_conn(db_path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
            row = cur.fetchone()
            conn.close()
            if row:
                intermediaries.append({
                    "position": i,
                    "name": row["name"],
                    "type": "company"
                })

    path_names = [inter["name"] for inter in intermediaries]

    return {
        "target_contact_id": target_contact_id,
        "target_name": f"{target_info['first_name']} {target_info['last_name']}",
        "target_title": target_info.get('title'),
        "target_company": target_info.get('company_name'),
        "source_contact_id": best_source["id"],
        "source_name": f"{best_source['first_name']} {best_source['last_name']}",
        "source_title": best_source.get('title'),
        "path": best_path,
        "path_names": path_names,
        "hops": best_hops,
        "intermediaries": intermediaries
    }


def generate_intro_email(requester_name: str, target_contact: Dict,
                        mutual_connection: Dict, context: str = "") -> str:
    """
    Generate a professional introduction request email.

    Args:
        requester_name: Name of person requesting the intro
        target_contact: Dict with target contact info (name, title, company)
        mutual_connection: Dict with mutual connection info (name, title, company)
        context: Additional context about the deal/relationship (optional)

    Returns:
        Professional email draft as string
    """
    target_name = target_contact.get('target_name', 'their contact')
    target_title = target_contact.get('target_title', 'their contact')
    target_company = target_contact.get('target_company', 'their company')

    mutual_name = mutual_connection.get('name', 'the mutual connection')
    mutual_title = mutual_connection.get('title', '')
    mutual_company = mutual_connection.get('company', '')

    # Build mutual connection reference
    mutual_ref = mutual_name
    if mutual_title:
        mutual_ref += f", {mutual_title}"
    if mutual_company:
        mutual_ref += f" at {mutual_company}"

    # Determine pronoun for target
    target_ref = f"{target_name}"
    if target_title:
        target_ref += f", {target_title}"
    if target_company:
        target_ref += f" at {target_company}"

    context_line = ""
    if context:
        context_line = f"\n\nContext: {context}\n"

    email = f"""Subject: Introduction Request — {target_name}

Hi {mutual_name},

I hope this message finds you well. I've been impressed by the work {mutual_ref} has been doing, and I believe there would be significant value in connecting with {target_ref}.
{context_line}
Would you be open to introducing us? I'd be happy to coordinate timing and can keep it brief.

Thank you for considering this request.

Best regards,
{requester_name}"""

    return email


def log_intro_request(target_contact_id: int, intro_path: Dict, email_draft: str,
                     db_path: str = None) -> bool:
    """
    Log an introduction request to the outreach_log table.

    Args:
        target_contact_id: Target contact ID
        intro_path: Result dict from find_intro_path()
        email_draft: Generated email text
        db_path: Database path

    Returns:
        True if logged successfully, False otherwise
    """
    if db_path is None:
        db_path = get_db_path()

    try:
        # Extract company_id from target contact
        conn = _get_conn(db_path)
        cur = conn.cursor()
        cur.execute("SELECT company_id FROM contacts WHERE id = ?", (target_contact_id,))
        row = cur.fetchone()

        if not row or not row["company_id"]:
            logger.warning(f"Could not find company for contact {target_contact_id}")
            conn.close()
            return False

        target_company_id = row["company_id"]

        # Build context from intro path
        path_str = " → ".join(intro_path.get('path_names', []))
        context = f"Path: {path_str} ({intro_path.get('hops', 0)} hops)"

        # Log to outreach_log
        cur.execute("""
            INSERT INTO outreach_log
                (target_company_id, outreach_type, outreach_date, context, outcome)
            VALUES (?, ?, date('now'), ?, ?)
        """, (target_company_id, 'intro_request', context, 'pending'))

        conn.commit()
        conn.close()

        logger.info(f"Logged intro request for contact {target_contact_id} to company {target_company_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to log intro request: {e}")
        return False


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate introduction request and email draft"
    )
    parser.add_argument("--target-id", type=int, required=True,
                       help="Target contact ID")
    parser.add_argument("--requester", type=str, default="You",
                       help="Your name (for email signature)")
    parser.add_argument("--context", type=str, default="",
                       help="Additional context for the intro")
    parser.add_argument("--db", type=str, default=None,
                       help="Database path (optional)")
    parser.add_argument("--log", action="store_true",
                       help="Log the request to outreach_log")

    args = parser.parse_args()

    # Find intro path
    path_result = find_intro_path(args.target_id, args.db)

    if "error" in path_result:
        print(f"Error: {path_result['error']}")
        return 1

    print("\n" + "="*70)
    print("INTRODUCTION PATH ANALYSIS")
    print("="*70)
    print(f"\nTarget: {path_result['target_name']} ({path_result['target_title']})")
    print(f"         at {path_result['target_company']}")
    print(f"\nSource: {path_result['source_name']} ({path_result['source_title']})")
    print(f"\nPath ({path_result['hops']} hops):")
    for i, inter in enumerate(path_result['intermediaries']):
        print(f"  {i+1}. {inter['name']}" +
              (f" - {inter.get('title', '')}" if inter.get('title') else "") +
              (f" @ {inter.get('company', '')}" if inter.get('company') else ""))

    # Generate email
    if path_result['hops'] == 1:
        # Direct connection
        mutual_info = {
            "name": path_result['source_name'],
            "title": path_result['source_title'],
            "company": path_result.get('source_company', '')
        }
    else:
        # Indirect - use first intermediary after source
        mutual_inter = path_result['intermediaries'][1]
        mutual_info = {
            "name": mutual_inter['name'],
            "title": mutual_inter.get('title', ''),
            "company": mutual_inter.get('company', '')
        }

    email = generate_intro_email(args.requester, path_result, mutual_info, args.context)

    print("\n" + "="*70)
    print("DRAFT EMAIL")
    print("="*70)
    print(email)

    # Log if requested
    if args.log:
        success = log_intro_request(args.target_id, path_result, email, args.db)
        if success:
            print("\n✓ Request logged to outreach_log")
        else:
            print("\n✗ Failed to log request")
            return 1

    return 0


if __name__ == "__main__":
    exit(main())
