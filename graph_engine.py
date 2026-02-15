"""
Graph Engine for Relationship Intelligence System
Builds weighted directed graph from SQLite database and computes centrality metrics.
"""

import sqlite3
import math
from datetime import datetime
from pathlib import Path

try:
    import networkx as nx
    from community import community_louvain
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False


def get_db_path():
    return Path.home() / "relationship_engine" / "data" / "relationship_engine.db"


def calculate_decay(last_date, decay_rate=0.1):
    if not last_date:
        return 0.5
    if isinstance(last_date, str):
        try:
            last_date = datetime.strptime(last_date.split()[0], "%Y-%m-%d")
        except ValueError:
            return 0.5
    months = (datetime.now() - last_date).days / 30.0
    return math.exp(-decay_rate * max(0, months))


def calculate_edge_weight(strength, decay, confidence=0.7):
    normalized = (strength or 3) / 5.0
    return normalized * decay * confidence


def build_graph(db_path=None):
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    if HAS_NETWORKX:
        G = nx.DiGraph()
    else:
        G = {"nodes": {}, "edges": []}
    
    # Load contacts
    cur.execute("SELECT id, first_name, last_name, title, company_id, role_level FROM contacts")
    contacts = cur.fetchall()
    
    for row in contacts:
        node_id = f"contact_{row['id']}"
        node_data = {
            "type": "contact",
            "name": f"{row['first_name']} {row['last_name']}",
            "title": row["title"],
            "company_id": row["company_id"],
            "role_level": row["role_level"]
        }
        if HAS_NETWORKX:
            G.add_node(node_id, **node_data)
        else:
            G["nodes"][node_id] = node_data
    
    # Load companies
    cur.execute("SELECT id, name, type, status, sector FROM companies")
    companies = cur.fetchall()
    
    for row in companies:
        node_id = f"company_{row['id']}"
        node_data = {
            "type": "company",
            "name": row["name"],
            "company_type": row["type"],
            "status": row["status"],
            "sector": row["sector"]
        }
        if HAS_NETWORKX:
            G.add_node(node_id, **node_data)
        else:
            G["nodes"][node_id] = node_data
    
    # Load relationships using actual schema
    cur.execute("""
        SELECT id, contact_id_a, contact_id_b, relationship_type, 
               strength, direction, context, updated_at
        FROM relationships
    """)
    relationships = cur.fetchall()
    
    for row in relationships:
        source = f"contact_{row['contact_id_a']}"
        target = f"contact_{row['contact_id_b']}"
        
        strength = row["strength"] or 3
        decay = calculate_decay(row["updated_at"])
        weight = calculate_edge_weight(strength, decay)
        
        edge_data = {
            "weight": weight,
            "relationship_type": row["relationship_type"],
            "strength": strength,
            "decay": decay,
            "context": row["context"],
            "direction": row["direction"]
        }
        
        direction = row["direction"] or "bidirectional"
        
        if HAS_NETWORKX:
            if direction in ("bidirectional", "a_to_b"):
                G.add_edge(source, target, **edge_data)
            if direction in ("bidirectional", "b_to_a"):
                G.add_edge(target, source, **edge_data)
        else:
            if direction in ("bidirectional", "a_to_b"):
                G["edges"].append((source, target, edge_data))
            if direction in ("bidirectional", "b_to_a"):
                G["edges"].append((target, source, edge_data))
    
    # Add contact-to-company edges
    for row in contacts:
        if row["company_id"]:
            contact_node = f"contact_{row['id']}"
            company_node = f"company_{row['company_id']}"
            edge_data = {"weight": 0.8, "relationship_type": "works_at", "strength": 5, "decay": 1.0}
            if HAS_NETWORKX:
                G.add_edge(contact_node, company_node, **edge_data)
            else:
                G["edges"].append((contact_node, company_node, edge_data))
    
    conn.close()
    return G


def compute_centrality(G):
    if HAS_NETWORKX:
        centrality = {}
        for node in G.nodes():
            out_weight = sum(d.get("weight", 0) for _, _, d in G.out_edges(node, data=True))
            centrality[node] = out_weight
        return centrality
    else:
        centrality = {node: 0.0 for node in G["nodes"]}
        for source, target, data in G["edges"]:
            centrality[source] += data.get("weight", 0)
        return centrality


def compute_two_hop_leverage(G):
    if HAS_NETWORKX:
        leverage = {}
        for node in G.nodes():
            total = 0.0
            for _, neighbor, d1 in G.out_edges(node, data=True):
                for _, target, d2 in G.out_edges(neighbor, data=True):
                    if target != node:
                        total += d1.get("weight", 0) * d2.get("weight", 0)
            leverage[node] = total
        return leverage
    else:
        adj = {}
        for source, target, data in G["edges"]:
            if source not in adj:
                adj[source] = []
            adj[source].append((target, data.get("weight", 0)))
        
        leverage = {node: 0.0 for node in G["nodes"]}
        for node in G["nodes"]:
            total = 0.0
            for neighbor, w1 in adj.get(node, []):
                for target, w2 in adj.get(neighbor, []):
                    if target != node:
                        total += w1 * w2
            leverage[node] = total
        return leverage


def detect_clusters(G):
    if HAS_NETWORKX:
        try:
            G_undirected = G.to_undirected()
            if G_undirected.number_of_edges() > 0:
                return community_louvain.best_partition(G_undirected)
            return {node: i for i, node in enumerate(G.nodes())}
        except:
            return {node: i for i, node in enumerate(G.nodes())}
    else:
        clusters = {}
        cluster_id = 0
        company_clusters = {}
        for node_id, data in G["nodes"].items():
            if data["type"] == "contact" and data.get("company_id"):
                comp_id = data["company_id"]
                if comp_id not in company_clusters:
                    company_clusters[comp_id] = cluster_id
                    cluster_id += 1
                clusters[node_id] = company_clusters[comp_id]
            else:
                clusters[node_id] = cluster_id
                cluster_id += 1
        return clusters


def find_shortest_path(G, source, target):
    if HAS_NETWORKX:
        try:
            path = nx.shortest_path(G, source, target, weight=lambda u, v, d: 1.0 / (d.get("weight", 0.1) + 0.01))
            total = sum(G[path[i]][path[i+1]].get("weight", 0) for i in range(len(path)-1))
            return path, total
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None, None
    else:
        from collections import deque
        adj = {}
        for s, t, data in G["edges"]:
            if s not in adj:
                adj[s] = []
            adj[s].append((t, data.get("weight", 0)))
        
        visited = {source}
        queue = deque([(source, [source], 0.0)])
        while queue:
            node, path, weight = queue.popleft()
            if node == target:
                return path, weight
            for neighbor, w in adj.get(node, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor], weight + w))
        return None, None


def save_scores_to_db(centrality, leverage, clusters, db_path=None):
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    for table in ["contacts", "companies"]:
        for col, ctype in [("centrality_score", "REAL"), ("leverage_score", "REAL"), ("cluster_id", "INTEGER")]:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ctype}")
            except sqlite3.OperationalError:
                pass
    
    for node_id, score in centrality.items():
        if node_id.startswith("contact_"):
            cid = int(node_id.split("_")[1])
            cur.execute("UPDATE contacts SET centrality_score=?, leverage_score=?, cluster_id=? WHERE id=?",
                       (score, leverage.get(node_id, 0), clusters.get(node_id, 0), cid))
        elif node_id.startswith("company_"):
            cid = int(node_id.split("_")[1])
            cur.execute("UPDATE companies SET centrality_score=?, leverage_score=?, cluster_id=? WHERE id=?",
                       (score, leverage.get(node_id, 0), clusters.get(node_id, 0), cid))
    
    conn.commit()
    conn.close()


def get_top_centrality(n=20, db_path=None):
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    results = []
    
    cur.execute("""SELECT 'contact' as type, first_name || ' ' || last_name as name, 
                   centrality_score, leverage_score, cluster_id FROM contacts 
                   WHERE centrality_score IS NOT NULL ORDER BY centrality_score DESC LIMIT ?""", (n,))
    results.extend([dict(row) for row in cur.fetchall()])
    
    cur.execute("""SELECT 'company' as type, name, centrality_score, leverage_score, cluster_id 
                   FROM companies WHERE centrality_score IS NOT NULL ORDER BY centrality_score DESC LIMIT ?""", (n,))
    results.extend([dict(row) for row in cur.fetchall()])
    
    conn.close()
    results.sort(key=lambda x: x.get("centrality_score", 0) or 0, reverse=True)
    return results[:n]


def get_top_leverage(n=20, db_path=None):
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    results = []
    
    cur.execute("""SELECT 'contact' as type, first_name || ' ' || last_name as name,
                   centrality_score, leverage_score, cluster_id FROM contacts 
                   WHERE leverage_score IS NOT NULL ORDER BY leverage_score DESC LIMIT ?""", (n,))
    results.extend([dict(row) for row in cur.fetchall()])
    
    cur.execute("""SELECT 'company' as type, name, centrality_score, leverage_score, cluster_id 
                   FROM companies WHERE leverage_score IS NOT NULL ORDER BY leverage_score DESC LIMIT ?""", (n,))
    results.extend([dict(row) for row in cur.fetchall()])
    
    conn.close()
    results.sort(key=lambda x: x.get("leverage_score", 0) or 0, reverse=True)
    return results[:n]


def compute_all(db_path=None, verbose=True):
    if db_path is None:
        db_path = get_db_path()
    
    if verbose:
        print(f"Building graph from {db_path}...")
    
    G = build_graph(db_path)
    
    if HAS_NETWORKX:
        node_count = G.number_of_nodes()
        edge_count = G.number_of_edges()
    else:
        node_count = len(G["nodes"])
        edge_count = len(G["edges"])
    
    if verbose:
        print(f"Graph built: {node_count} nodes, {edge_count} edges")
        print("Computing centrality...")
    
    centrality = compute_centrality(G)
    
    if verbose:
        print("Computing 2-hop leverage...")
    
    leverage = compute_two_hop_leverage(G)
    
    if verbose:
        print("Detecting clusters...")
    
    clusters = detect_clusters(G)
    
    if verbose:
        print("Saving scores to database...")
    
    save_scores_to_db(centrality, leverage, clusters, db_path)
    
    if verbose:
        print("\nTop 5 by Centrality:")
        for i, row in enumerate(get_top_centrality(5, db_path), 1):
            print(f"  {i}. {row['name']} ({row['type']}): {row.get('centrality_score', 0):.4f}")
        
        print("\nTop 5 by 2-Hop Leverage:")
        for i, row in enumerate(get_top_leverage(5, db_path), 1):
            print(f"  {i}. {row['name']} ({row['type']}): {row.get('leverage_score', 0):.4f}")
        
        print(f"\nClusters detected: {len(set(clusters.values()))}")
        print("Done.")
    
    return {"nodes": node_count, "edges": edge_count, "clusters": len(set(clusters.values()))}


if __name__ == "__main__":
    compute_all()
