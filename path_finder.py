"""
Enhanced Path Finder for Relationship Engine
Finds multiple paths to targets with intro suggestions.
"""

import os
import sys
import sqlite3
from typing import List, Dict, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import get_db_path, build_graph, find_shortest_path

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False


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
                # Find shortest path
                if nx.has_path(graph, team_node, target_node):
                    path = nx.shortest_path(graph, team_node, target_node)
                    
                    if len(path) - 1 <= max_hops:
                        # Resolve path to names
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
            except:
                continue
    
    # Sort by hops, then by target role level
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
                JOIN companies comp ON c.company_id = comp.id
                WHERE c.id = ?
            """, (contact_id,))
            row = cur.fetchone()
            if row:
                names.append(f"{row['name']} ({row['company']})")
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
    
    # Get target company name
    cur.execute("SELECT name FROM companies WHERE id = ?", (target_company_id,))
    target_name = cur.fetchone()['name']
    
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
        # Check if previous company is a client
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
    
    # Get network paths
    paths = find_all_paths(target_company_id, max_hops=3, db_path=db_path)
    result["two_hop_paths"] = paths[:5]  # Top 5 shortest
    
    # Set best path
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
    # Test with Citadel
    import sys
    
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM companies WHERE name LIKE '%Citadel%'")
    row = cur.fetchone()
    conn.close()
    
    if row:
        print(format_path_report(row[0]))
    else:
        print("Company not found")
