"""
Enhanced Path Finder for Relationship Engine
Finds multiple paths to targets with intro suggestions.
"""

import os
import sys
import sqlite3
from typing import List, Dict, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import get_db_path, build_graph, find_shortest_path


def get_company_contacts(company_id: int, db_path: str = None) -> List[Dict]:
    """Get all contacts at a company."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT id, first_name || ' ' || last_name as name, title, role_level, email
        FROM contacts
        WHERE company_id = ?
        ORDER BY
            CASE role_level
                WHEN 'c_suite' THEN 1
                WHEN 'decision_maker' THEN 2
                WHEN 'influencer' THEN 3
                ELSE 4
            END
    """, (company_id,))

    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def get_team_contacts(db_path: str = None) -> List[Dict]:
    """Get all team contacts (our people)."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT c.id, c.first_name || ' ' || c.last_name as name, c.title, comp.name as company
        FROM contacts c
        JOIN companies comp ON c.company_id = comp.id
        WHERE c.role_level = 'team' OR comp.status = 'team_affiliated'
    """)

    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def find_all_paths(target_company_id: int, max_hops: int = 3, db_path: str = None) -> List[Dict]:
    """Find all paths from team to target company contacts."""
    if db_path is None:
        db_path = get_db_path()

    graph = build_graph(db_path)
    if graph is None:
        return []

    team = get_team_contacts(db_path)
    targets = get_company_contacts(target_company_id, db_path)

    paths = []

    for target in targets:
        target_node = f"contact_{target['id']}"

        for team_member in team:
            team_node = f"contact_{team_member['id']}"

            try:
                path, weight = find_shortest_path(graph, team_node, target_node)

                if path and len(path) - 1 <= max_hops:
                    path_names = resolve_path_names(path, db_path)

                    paths.append({
                        "from": team_member['name'],
                        "from_id": team_member['id'],
                        "to": target['name'],
                        "to_id": target['id'],
                        "to_title": target.get('title', ''),
                        "to_role": target.get('role_level', ''),
                        "hops": len(path) - 1,
                        "path": path_names,
                        "path_ids": path
                    })
            except Exception:
                continue

    role_order = {'c_suite': 1, 'decision_maker': 2, 'influencer': 3}
    paths.sort(key=lambda x: (x['hops'], role_order.get(x['to_role'], 4)))

    return paths


def resolve_path_names(path: List[str], db_path: str = None) -> List[str]:
    """Convert node IDs to readable names."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    names = []
    for node in path:
        if node.startswith("contact_"):
            contact_id = int(node.replace("contact_", ""))
            cur.execute("""
                SELECT c.first_name || ' ' || c.last_name as name, comp.name as company
                FROM contacts c
                LEFT JOIN companies comp ON c.company_id = comp.id
                WHERE c.id = ?
            """, (contact_id,))
            row = cur.fetchone()
            if row:
                names.append(f"{row['name']} ({row['company']})" if row['company'] else row['name'])
            else:
                names.append(node)
        elif node.startswith("company_"):
            company_id = int(node.replace("company_", ""))
            cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
            row = cur.fetchone()
            names.append(row['name'] if row else node)
        else:
            names.append(node)

    conn.close()
    return names


def find_warm_intros(target_company_id: int, db_path: str = None) -> Dict:
    """Find all warm intro opportunities to a target company."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM companies WHERE id = ?", (target_company_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"target_company": "Unknown", "former_employees": [], "direct_relationships": [],
                "two_hop_paths": [], "alumni_connections": [], "best_path": "Company not found"}
    target_name = row['name']

    result = {
        "target_company": target_name,
        "former_employees": [],
        "direct_relationships": [],
        "two_hop_paths": [],
        "alumni_connections": [],
        "best_path": None
    }

    # Former employees (worked at our clients)
    cur.execute("""
        SELECT c.first_name || ' ' || c.last_name as name, c.title, c.previous_companies
        FROM contacts c
        WHERE c.company_id = ?
        AND c.previous_companies IS NOT NULL
    """, (target_company_id,))

    for row in cur.fetchall():
        prev = row['previous_companies']
        cur.execute("""
            SELECT name FROM companies
            WHERE status = 'active_client'
            AND ? LIKE '%' || name || '%'
        """, (prev,))
        client = cur.fetchone()
        if client:
            result["former_employees"].append({
                "name": row['name'],
                "title": row['title'],
                "from_client": client['name']
            })

    # Direct relationships (strength >= 4)
    cur.execute("""
        SELECT
            c1.first_name || ' ' || c1.last_name as team_member,
            c2.first_name || ' ' || c2.last_name as target_contact,
            c2.title as target_title,
            r.strength,
            r.relationship_type,
            r.context
        FROM relationships r
        JOIN contacts c1 ON r.contact_id_a = c1.id
        JOIN contacts c2 ON r.contact_id_b = c2.id
        JOIN companies comp ON c2.company_id = comp.id
        WHERE comp.id = ?
        AND r.strength >= 4
        AND c1.role_level = 'team'
        ORDER BY r.strength DESC
    """, (target_company_id,))

    for row in cur.fetchall():
        result["direct_relationships"].append(dict(row))

    conn.close()

    paths = find_all_paths(target_company_id, max_hops=3, db_path=db_path)
    result["two_hop_paths"] = paths[:5]

    if result["direct_relationships"]:
        dr = result["direct_relationships"][0]
        result["best_path"] = f"Direct: {dr['team_member']} knows {dr['target_contact']}"
    elif result["former_employees"]:
        fe = result["former_employees"][0]
        result["best_path"] = f"Former employee: {fe['name']} came from {fe['from_client']}"
    elif result["two_hop_paths"]:
        tp = result["two_hop_paths"][0]
        result["best_path"] = f"{tp['hops']}-hop: {' → '.join(tp['path'])}"
    else:
        result["best_path"] = "No warm path found — cold outreach needed"

    return result


def find_alumni_connections(alma_mater: str, db_path: str = None) -> List[Dict]:
    """
    Query: "Find contacts who went to same school"

    Args:
        alma_mater: School name to search for
        db_path: Database path

    Returns:
        List of dicts: name, company, company_status, title, alma_mater
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            c.first_name || ' ' || c.last_name as name,
            c.title,
            c.alma_mater,
            comp.name as company,
            comp.status as company_status
        FROM contacts c
        LEFT JOIN companies comp ON c.company_id = comp.id
        WHERE c.alma_mater LIKE ?
        ORDER BY comp.status DESC, c.last_name
    """, (f"%{alma_mater}%",))

    results = []
    for row in cur.fetchall():
        results.append({
            'name': row['name'],
            'title': row['title'],
            'alma_mater': row['alma_mater'],
            'company': row['company'],
            'company_status': row['company_status']
        })

    conn.close()
    return results


def find_former_colleague_paths(target_company_id: int, db_path: str = None) -> List[Dict]:
    """
    Query: "Find contacts at [target] who previously worked at [my clients]"

    Look at contacts at target company, check previous_companies against
    active_client company names.

    Args:
        target_company_id: Company ID to search
        db_path: Database path

    Returns:
        List: contact_name, target_title, previous_company (our client), intro_angle
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all contacts at target company
    cur.execute("""
        SELECT id, first_name || ' ' || last_name as name, title, previous_companies
        FROM contacts
        WHERE company_id = ?
    """, (target_company_id,))

    target_contacts = cur.fetchall()
    results = []

    for contact in target_contacts:
        if not contact['previous_companies']:
            continue

        # Check if any previous company is an active_client
        cur.execute("""
            SELECT name FROM companies
            WHERE status = 'active_client'
        """)

        for client_row in cur.fetchall():
            client_name = client_row['name']
            if client_name.lower() in contact['previous_companies'].lower():
                results.append({
                    'contact_name': contact['name'],
                    'target_title': contact['title'],
                    'previous_company': client_name,
                    'intro_angle': f"{contact['name']} worked at our client {client_name}"
                })

    conn.close()
    return results


def find_paths_through_person(contact_id: int, db_path: str = None) -> List[Dict]:
    """
    Query: "Find all targets reachable through [specific person]"

    Get all relationships for this contact. For each connected contact,
    check what company they're at. If company is a target (prospect/high_growth_target),
    include in results.

    Args:
        contact_id: Contact ID to search through
        db_path: Database path

    Returns:
        List of reachable targets with intermediary and relationship context
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get the person we're searching through
    cur.execute("""
        SELECT first_name || ' ' || last_name as name FROM contacts WHERE id = ?
    """, (contact_id,))
    person_row = cur.fetchone()
    if not person_row:
        return []

    person_name = person_row['name']

    # Get all relationships for this contact (bidirectional)
    cur.execute("""
        SELECT contact_id_a, contact_id_b, relationship_type, strength
        FROM relationships
        WHERE contact_id_a = ? OR contact_id_b = ?
    """, (contact_id, contact_id))

    relationships = cur.fetchall()
    results = []

    for rel in relationships:
        other_contact_id = rel['contact_id_b'] if rel['contact_id_a'] == contact_id else rel['contact_id_a']

        # Get the other contact's company
        cur.execute("""
            SELECT
                c.first_name || ' ' || c.last_name as name,
                c.title,
                comp.id as company_id,
                comp.name as company_name,
                comp.status as company_status
            FROM contacts c
            LEFT JOIN companies comp ON c.company_id = comp.id
            WHERE c.id = ?
        """, (other_contact_id,))

        contact_row = cur.fetchone()
        if not contact_row or contact_row['company_status'] not in ('prospect', 'high_growth_target'):
            continue

        results.append({
            'target_company': contact_row['company_name'],
            'target_company_id': contact_row['company_id'],
            'target_contact': contact_row['name'],
            'target_title': contact_row['title'],
            'intermediary': person_name,
            'relationship_type': rel['relationship_type'],
            'relationship_strength': rel['strength'],
            'path_description': f"{person_name} → {contact_row['name']} @ {contact_row['company_name']}"
        })

    conn.close()
    return results


def rank_intro_paths(target_company_id: int, db_path: str = None) -> List[Dict]:
    """
    Combine all path-finding methods into a ranked list.

    Scoring:
    1. Direct team relationships (strength 4-5) — score: strength * 20
    2. Former colleague paths — score: 70
    3. Alumni connections — score: 50
    4. 2-hop graph paths — score: 80 / hops
    5. 3-hop graph paths — score: 40 / hops

    Args:
        target_company_id: Target company ID
        db_path: Database path

    Returns:
        Sorted list with: intro_type, score, path_description, recommended_action
    """
    if db_path is None:
        db_path = get_db_path()

    ranked_paths = []

    # 1. Direct team relationships (strength >= 4)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            c1.first_name || ' ' || c1.last_name as team_member,
            c2.first_name || ' ' || c2.last_name as target_contact,
            c2.title as target_title,
            r.strength,
            r.relationship_type
        FROM relationships r
        JOIN contacts c1 ON r.contact_id_a = c1.id
        JOIN contacts c2 ON r.contact_id_b = c2.id
        JOIN companies comp ON c2.company_id = comp.id
        WHERE comp.id = ?
        AND r.strength >= 4
        AND c1.role_level = 'team'
        ORDER BY r.strength DESC
    """, (target_company_id,))

    for row in cur.fetchall():
        score = row['strength'] * 20
        ranked_paths.append({
            'intro_type': 'Direct team relationship',
            'score': score,
            'path_description': f"{row['team_member']} → {row['target_contact']} ({row['target_title']})",
            'recommended_action': f"Have {row['team_member']} intro {row['target_contact']} directly",
            'strength': row['strength']
        })

    # 2. Former colleague paths
    former_colleague = find_former_colleague_paths(target_company_id, db_path)
    for item in former_colleague:
        ranked_paths.append({
            'intro_type': 'Former colleague path',
            'score': 70,
            'path_description': f"{item['contact_name']} (from {item['previous_company']})",
            'recommended_action': f"Introduce {item['contact_name']} via {item['previous_company']} connection",
            'strength': 3
        })

    # 3. Get target company details for alumni search (school name if available)
    cur.execute("SELECT name FROM companies WHERE id = ?", (target_company_id,))
    target_row = cur.fetchone()
    target_name = target_row['name'] if target_row else None

    # 4 & 5. Graph paths (2-hop and 3-hop)
    all_paths = find_all_paths(target_company_id, max_hops=3, db_path=db_path)
    for path in all_paths:
        hops = path['hops']
        if hops <= 2:
            score = 80 / max(hops, 1)
        else:  # 3-hop
            score = 40 / hops

        ranked_paths.append({
            'intro_type': f'{hops}-hop network path',
            'score': score,
            'path_description': ' → '.join(path['path']),
            'recommended_action': f"Activate this {hops}-hop path: {' → '.join(path['path'])}",
            'strength': max(1, 5 - hops)
        })

    # Sort by score descending
    ranked_paths.sort(key=lambda x: x['score'], reverse=True)

    # Add rank
    for i, path in enumerate(ranked_paths, 1):
        path['rank'] = i

    conn.close()
    return ranked_paths


def format_path_report(target_company_id: int, db_path: str = None) -> str:
    """Generate a formatted path report for a target company."""
    result = find_warm_intros(target_company_id, db_path)

    lines = [
        f"**Paths to {result['target_company']}**",
        "",
        f"Best path: {result['best_path']}",
        ""
    ]

    if result["former_employees"]:
        lines.append("**Former Client Employees:**")
        for fe in result["former_employees"]:
            lines.append(f"  • {fe['name']} ({fe['title']}) — from {fe['from_client']}")
        lines.append("")

    if result["direct_relationships"]:
        lines.append("**Direct Relationships:**")
        for dr in result["direct_relationships"]:
            lines.append(f"  • {dr['team_member']} → {dr['target_contact']} ({dr['target_title']}) [strength: {dr['strength']}]")
        lines.append("")

    if result["two_hop_paths"]:
        lines.append("**Network Paths:**")
        for path in result["two_hop_paths"][:3]:
            lines.append(f"  • {' → '.join(path['path'])} [{path['hops']} hops]")
        lines.append("")

    if not any([result["former_employees"], result["direct_relationships"], result["two_hop_paths"]]):
        lines.append("No warm paths found. Consider:")
        lines.append("  • Cold outreach via LinkedIn")
        lines.append("  • Event networking")
        lines.append("  • Building relationships with adjacent contacts")

    return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Find intro paths to target companies")
    parser.add_argument("--company", type=str, help="Company name to search for")
    parser.add_argument("--company-id", type=int, help="Company ID (alternative to --company)")
    parser.add_argument("--alumni", type=str, help="Alma mater to search for alumni connections")
    parser.add_argument("--through-contact", type=int, help="Contact ID to find paths through")

    args = parser.parse_args()

    db_path = get_db_path()

    if args.alumni:
        print(f"\n=== Alumni Connections for {args.alumni} ===\n")
        alumni = find_alumni_connections(args.alumni, db_path)
        if alumni:
            for person in alumni:
                status = f" [{person['company_status']}]" if person['company_status'] else ""
                print(f"• {person['name']} ({person['title']})")
                print(f"  Company: {person['company']}{status}")
                print(f"  School: {person['alma_mater']}\n")
        else:
            print("No alumni found")

    elif args.through_contact:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT first_name || ' ' || last_name as name FROM contacts WHERE id = ?", (args.through_contact,))
        contact_row = cur.fetchone()
        conn.close()

        if contact_row:
            print(f"\n=== Targets Reachable Through {contact_row['name']} ===\n")
            paths = find_paths_through_person(args.through_contact, db_path)
            if paths:
                for path in paths:
                    print(f"• {path['target_company']}")
                    print(f"  Contact: {path['target_contact']} ({path['target_title']})")
                    print(f"  Via: {path['relationship_type']} (strength: {path['relationship_strength']})\n")
            else:
                print("No target company connections found")
        else:
            print(f"Contact ID {args.through_contact} not found")

    elif args.company or args.company_id:
        company_id = args.company_id
        if args.company and not company_id:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM companies WHERE name LIKE ?", (f"%{args.company}%",))
            row = cur.fetchone()
            conn.close()
            if row:
                company_id = row[0]
                company_name = row[1]
            else:
                print(f"Company '{args.company}' not found")
                exit(1)
        else:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
            row = cur.fetchone()
            conn.close()
            company_name = row[0] if row else f"Company {company_id}"

        print(f"\n=== Ranked Intro Paths to {company_name} ===\n")
        ranked = rank_intro_paths(company_id, db_path)
        if ranked:
            for path in ranked[:10]:
                print(f"{path['rank']}. [{path['score']:.1f}] {path['intro_type']}")
                print(f"   Path: {path['path_description']}")
                print(f"   Action: {path['recommended_action']}\n")
        else:
            print("No paths found. Consider cold outreach.")

    else:
        # Default: search for example company and show full report
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM companies WHERE name LIKE '%Citadel%'")
        row = cur.fetchone()
        conn.close()

        if row:
            print(format_path_report(row[0]))
        else:
            print("Company not found. Use --company, --alumni, or --through-contact options.")
