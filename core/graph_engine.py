"""
Graph Intelligence Layer for Relationship Engine
Custom directed weighted graph implementation (no networkx dependency).
Computes centrality, leverage, clusters, and shortest paths.
"""

import sqlite3
import math
import os
import heapq
from datetime import datetime, date
from typing import Optional
from collections import defaultdict

def _resolve_db_path() -> str:
    """
    Resolve database path with fallback chain:
    1. RE_DB_PATH environment variable
    2. private_data/relationship_engine.db (live DB, .gitignored)
    3. data/relationship_engine.db (dev/seed DB)
    """
    # Environment override
    env_path = os.environ.get("RE_DB_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Private data (live DB)
    private = os.path.join(root, "private_data", "relationship_engine.db")
    if os.path.exists(private):
        return private

    # Fallback to data/ (seed/dev)
    return os.path.join(root, "data", "relationship_engine.db")


DB_PATH = _resolve_db_path()


# ============================================================
# LIGHTWEIGHT DIRECTED GRAPH
# ============================================================

class DiGraph:
    """Minimal directed weighted graph."""

    def __init__(self):
        self.nodes = {}         # node_key -> {attr dict}
        self.adj = defaultdict(dict)   # src -> {tgt -> {edge attrs}}
        self.pred = defaultdict(dict)  # tgt -> {src -> {edge attrs}}

    def add_node(self, key: str, **attrs):
        self.nodes[key] = attrs

    def add_edge(self, src: str, tgt: str, **attrs):
        self.adj[src][tgt] = attrs
        self.pred[tgt][src] = attrs

    def out_edges(self, node: str):
        """Yield (src, tgt, edge_data) for outgoing edges."""
        for tgt, data in self.adj.get(node, {}).items():
            yield node, tgt, data

    def has_edge(self, src: str, tgt: str) -> bool:
        return tgt in self.adj.get(src, {})

    def edge_data(self, src: str, tgt: str) -> dict:
        return self.adj.get(src, {}).get(tgt, {})

    def number_of_nodes(self) -> int:
        return len(self.nodes)

    def number_of_edges(self) -> int:
        return sum(len(targets) for targets in self.adj.values())

    def all_nodes(self):
        return list(self.nodes.keys())

    def neighbors_undirected(self, node: str) -> set:
        """All neighbors (ignoring direction)."""
        result = set(self.adj.get(node, {}).keys())
        result.update(self.pred.get(node, {}).keys())
        return result

    def undirected_edge_weight(self, u: str, v: str) -> float:
        """Get weight of edge between u and v in either direction."""
        if v in self.adj.get(u, {}):
            return self.adj[u][v].get("weight", 1.0)
        if u in self.adj.get(v, {}):
            return self.adj[v][u].get("weight", 1.0)
        return float("inf")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _node_key(entity_type: str, entity_id: int) -> str:
    return f"{entity_type}_{entity_id}"


def _months_since(last_interaction: Optional[str], reference_date: Optional[date] = None) -> float:
    if not last_interaction:
        return 12.0
    ref = reference_date or date.today()
    try:
        last = datetime.strptime(last_interaction, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return 12.0
    delta = ref - last
    return max(delta.days / 30.44, 0.0)


# Edge type weights — different relationship types carry different strategic value
EDGE_TYPE_WEIGHTS = {
    "client": 1.5,
    "deal_counterpart": 1.4,
    "investor": 1.3,
    "board": 1.3,
    "colleague": 1.0,
    "former_colleague": 0.9,
    "introduced_by": 1.2,
    "alumni": 0.8,
    "friend": 0.7,
    "tenant": 1.4,
    "broker": 1.3,
    "landlord": 1.2,
}

# Relationship layer classification
EDGE_LAYERS = {
    "professional": {"client", "deal_counterpart", "colleague", "former_colleague", "board", "tenant", "broker", "landlord"},
    "capital": {"investor"},
    "social": {"alumni", "friend", "introduced_by"},
}


def _compute_edge_weight(base_weight: float, strength: int, confidence: float,
                         last_interaction: Optional[str],
                         relationship_type: str = None) -> float:
    """
    Edge weight = Base × TypeMultiplier × Strength × Decay × Confidence
    Decay = e^(-0.1 × months_since_last_interaction)
    """
    months = _months_since(last_interaction)
    decay = math.exp(-0.1 * months)
    type_mult = EDGE_TYPE_WEIGHTS.get(relationship_type, 1.0) if relationship_type else 1.0
    return base_weight * type_mult * strength * decay * confidence


def _get_edge_layer(relationship_type: str) -> str:
    """Classify an edge into its layer."""
    for layer, types in EDGE_LAYERS.items():
        if relationship_type in types:
            return layer
    return "professional"


# ============================================================
# GRAPH CONSTRUCTION
# ============================================================

def _categorize_company(row) -> tuple:
    """
    Determine entity category and maturity flag.
    Returns (category: str, mature: bool).

    Logic:
    - If hedge_fund industry or institutional flag → 'institutional'
    - If revenue > threshold or SF > threshold → mature + 'institutional'
    - If has funding events and below thresholds → 'high_growth'
    - Otherwise → None (uncategorized)
    """
    from thresholds import REVENUE_THRESHOLD, SF_THRESHOLD

    industry = (row["industry"] or "").lower()
    status = (row["status"] or "").lower()
    revenue = row["revenue_est"] or 0
    sf = row["office_sf"] or 0

    is_mature = revenue > REVENUE_THRESHOLD or sf > SF_THRESHOLD

    if industry in ("hedge_fund", "private_equity", "asset_management", "investment_banking"):
        return "institutional", is_mature
    if is_mature:
        return "institutional", True
    if status in ("high_growth_target",):
        return "high_growth", False

    return None, is_mature


def build_graph(db_path: str = DB_PATH) -> DiGraph:
    """Read all relationships from SQLite and build a directed weighted graph."""
    conn = _get_conn(db_path)
    cur = conn.cursor()
    G = DiGraph()

    cur.execute("SELECT id, first_name, last_name, title, company_id, status FROM contacts")
    for row in cur.fetchall():
        key = _node_key("contact", row["id"])
        G.add_node(key, entity_type="contact", entity_id=row["id"],
                   name=f"{row['first_name']} {row['last_name']}",
                   title=row["title"], company_id=row["company_id"],
                   status=row["status"])

    cur.execute("""SELECT id, name, sector, status,
                          COALESCE(category, '') as category
                   FROM companies""")
    for row in cur.fetchall():
        key = _node_key("company", row["id"])
        category = row["category"] or "other"
        G.add_node(key, entity_type="company", entity_id=row["id"],
                   name=row["name"], sector=row["sector"],
                   status=row["status"],
                   category=category)

    cur.execute("""
        SELECT contact_id_a, contact_id_b, relationship_type, strength
        FROM relationships
    """)
    for row in cur.fetchall():
        src = _node_key("contact", row["contact_id_a"])
        tgt = _node_key("contact", row["contact_id_b"])
        rel_type = row["relationship_type"]
        weight = row["strength"] or 1
        G.add_edge(src, tgt, weight=weight,
                   relationship_type=rel_type,
                   strength=row["strength"] or 1)

    conn.close()
    return G


# ============================================================
# CENTRALITY
# ============================================================

def weighted_out_degree_centrality(G: DiGraph) -> dict:
    """Weighted out-degree centrality for all nodes."""
    n = G.number_of_nodes()
    if n <= 1:
        return {node: 0.0 for node in G.all_nodes()}
    centrality = {}
    for node in G.all_nodes():
        total_weight = sum(d.get("weight", 1.0) for _, _, d in G.out_edges(node))
        centrality[node] = total_weight / (n - 1)
    return centrality


def influence_propagation(G: DiGraph, damping: float = 0.85,
                          max_iter: int = 100, tol: float = 1e-6) -> dict:
    """
    PageRank-style influence propagation on the directed weighted graph.

    Unlike simple out-degree centrality, this captures transitive influence:
    a node connected to highly-influential nodes scores higher than one
    connected to peripheral nodes, even if raw edge counts are the same.

    Returns dict: node_key -> influence_score (sums to 1.0).
    """
    nodes = G.all_nodes()
    n = len(nodes)
    if n == 0:
        return {}

    # Initialize uniform
    scores = {node: 1.0 / n for node in nodes}

    # Precompute outgoing weight totals for each node
    out_weight = {}
    for node in nodes:
        total = sum(d.get("weight", 1.0) for _, _, d in G.out_edges(node))
        out_weight[node] = total if total > 0 else 1.0  # avoid div/0

    for _ in range(max_iter):
        new_scores = {}
        # Sink mass = damping × sum of scores of nodes with no outgoing edges
        sink = 0.0
        for node in nodes:
            if out_weight.get(node, 0) == 0:
                sink += scores[node]

        for node in nodes:
            # Incoming weighted contribution
            incoming = 0.0
            # Check all predecessors (nodes with edges pointing TO this node)
            for pred, edge_data in G.pred.get(node, {}).items():
                w = edge_data.get("weight", 1.0)
                incoming += scores[pred] * w / out_weight[pred]

            new_scores[node] = (1 - damping) / n + damping * (incoming + sink / n)

        # Check convergence
        diff = sum(abs(new_scores[node] - scores[node]) for node in nodes)
        scores = new_scores
        if diff < tol:
            break

    return scores


# ============================================================
# 2-HOP LEVERAGE
# ============================================================

def two_hop_leverage(G: DiGraph) -> dict:
    """Sum of weights to all nodes reachable within 2 hops."""
    leverage = {}
    for node in G.all_nodes():
        score = 0.0
        one_hop = set()
        for _, neighbor, d in G.out_edges(node):
            score += d.get("weight", 1.0)
            one_hop.add(neighbor)
        for hop1 in one_hop:
            for _, hop2, d in G.out_edges(hop1):
                if hop2 != node:
                    score += d.get("weight", 1.0)
        leverage[node] = score
    return leverage


# ============================================================
# STRATEGIC ADJACENCY INDEX
# ============================================================

def strategic_adjacency_index(G: DiGraph, db_path: str = DB_PATH) -> dict:
    """
    For each node, compute a weighted score based on how many high-value
    nodes (decision makers, C-suite, high-growth companies) are within 2 hops.

    A contact who is 2 hops from 5 C-suite execs at target companies is
    strategically more valuable than one connected to 5 junior employees.
    """
    # Classify node value from DB attributes stored on graph
    def _node_value(node: str) -> float:
        data = G.nodes.get(node, {})
        etype = data.get("entity_type")
        if etype == "company":
            status = data.get("status", "")
            return {"high_growth_target": 3.0, "prospect": 2.0, "active_client": 2.5,
                    "former_client": 1.0}.get(status, 0.5)
        elif etype == "contact":
            title = (data.get("title") or "").lower()
            if any(t in title for t in ["ceo", "cfo", "coo", "cio", "president", "partner", "managing director"]):
                return 3.0
            elif any(t in title for t in ["vp", "vice president", "director", "head of", "svp"]):
                return 2.0
            elif any(t in title for t in ["manager", "associate director"]):
                return 1.0
        return 0.5

    index = {}
    for node in G.all_nodes():
        score = 0.0
        seen = {node}

        # 1-hop neighbors
        hop1_set = set()
        for neighbor in G.neighbors_undirected(node):
            w = G.undirected_edge_weight(node, neighbor)
            edge_quality = min(w, 5.0) / 5.0  # normalize to 0-1
            score += _node_value(neighbor) * edge_quality * 1.0  # full weight at hop 1
            hop1_set.add(neighbor)
            seen.add(neighbor)

        # 2-hop neighbors
        for hop1 in hop1_set:
            for hop2 in G.neighbors_undirected(hop1):
                if hop2 in seen:
                    continue
                seen.add(hop2)
                w = G.undirected_edge_weight(hop1, hop2)
                edge_quality = min(w, 5.0) / 5.0
                score += _node_value(hop2) * edge_quality * 0.5  # half weight at hop 2

        index[node] = round(score, 4)
    return index


# ============================================================
# BROKER COVERAGE OVERLAP
# ============================================================

def broker_coverage_overlap(G: DiGraph) -> dict:
    """
    Identify contacts that bridge to the same target companies.

    Returns dict keyed by company node, where each value is a list of
    contacts that provide paths to that company, with overlap counts.
    Useful for spotting redundant intro paths vs. single-threaded risk.
    """
    # For each company node, find all contacts within 2 hops
    company_nodes = [n for n in G.all_nodes() if G.nodes[n].get("entity_type") == "company"]
    contact_nodes = [n for n in G.all_nodes() if G.nodes[n].get("entity_type") == "contact"]

    coverage = {}  # company_node -> [contact_node, ...]
    for company in company_nodes:
        company_neighbors = G.neighbors_undirected(company)
        # Direct contacts (hop 1)
        direct = set()
        for c in company_neighbors:
            if G.nodes.get(c, {}).get("entity_type") == "contact":
                direct.add(c)

        # Hop-2 contacts (contact -> X -> company)
        indirect = set()
        for neighbor in company_neighbors:
            for hop2 in G.neighbors_undirected(neighbor):
                if hop2 != company and G.nodes.get(hop2, {}).get("entity_type") == "contact":
                    indirect.add(hop2)

        all_bridges = direct | indirect
        if all_bridges:
            coverage[company] = {
                "direct": list(direct),
                "indirect": list(indirect - direct),
                "total_bridges": len(all_bridges),
                "single_threaded": len(all_bridges) == 1
            }

    return coverage


# ============================================================
# CLUSTER DETECTION (LOUVAIN-STYLE GREEDY MODULARITY)
# ============================================================

def detect_clusters(G: DiGraph) -> dict:
    """
    Greedy modularity-based community detection (Louvain-inspired).
    Operates on undirected version of the graph.
    """
    nodes = G.all_nodes()
    if not nodes:
        return {}

    # Build undirected adjacency with weights
    adj = defaultdict(dict)
    for src in nodes:
        for _, tgt, d in G.out_edges(src):
            w = d.get("weight", 1.0)
            adj[src][tgt] = adj[src].get(tgt, 0) + w
            adj[tgt][src] = adj[tgt].get(src, 0) + w

    # Total weight
    m = sum(sum(targets.values()) for targets in adj.values()) / 2.0
    if m == 0:
        return {n: 0 for n in nodes}

    # Node strengths (sum of weights)
    k = {n: sum(adj[n].values()) for n in nodes}

    # Initial: each node in its own community
    community = {n: i for i, n in enumerate(nodes)}

    improved = True
    while improved:
        improved = False
        for node in nodes:
            current_comm = community[node]

            # Calculate neighbor communities and delta-Q for each
            neighbor_comms = defaultdict(float)
            for neighbor, w in adj[node].items():
                neighbor_comms[community[neighbor]] += w

            best_comm = current_comm
            best_delta = 0.0

            # Sum of weights in current community (excluding node)
            sigma_current = sum(
                k[n] for n in nodes if community[n] == current_comm and n != node
            )
            ki_in_current = neighbor_comms.get(current_comm, 0.0)

            for comm, ki_in in neighbor_comms.items():
                if comm == current_comm:
                    continue
                sigma_comm = sum(k[n] for n in nodes if community[n] == comm)

                # Modularity gain of moving node to comm
                delta = (ki_in - ki_in_current) / m - k[node] * (sigma_comm - sigma_current) / (2 * m * m)

                if delta > best_delta:
                    best_delta = delta
                    best_comm = comm

            if best_comm != current_comm:
                community[node] = best_comm
                improved = True

    # Renumber clusters to 0, 1, 2, ...
    unique_comms = sorted(set(community.values()))
    remap = {c: i for i, c in enumerate(unique_comms)}
    return {n: remap[c] for n, c in community.items()}


def cluster_sector_dominance(G: DiGraph, clusters: dict) -> dict:
    """
    For each cluster, compute sector composition and identify the dominant sector.

    Returns dict: cluster_id -> {
        "dominant_sector": str,
        "sector_shares": {sector: fraction},
        "size": int,
        "layer_mix": {"professional": n, "capital": n, "social": n}
    }
    """
    from collections import Counter

    # Group nodes by cluster
    cluster_nodes = defaultdict(list)
    for node, cid in clusters.items():
        cluster_nodes[cid].append(node)

    result = {}
    for cid, nodes in cluster_nodes.items():
        sectors = []
        layers = Counter()

        for node in nodes:
            data = G.nodes.get(node, {})
            # Companies have industry; contacts inherit from company_id
            industry = data.get("industry")
            if industry:
                sectors.append(industry)

            # Count edge layers within this cluster
            for _, tgt, edata in G.out_edges(node):
                if clusters.get(tgt) == cid:
                    layers[edata.get("layer", "professional")] += 1

        sector_counts = Counter(sectors)
        total_sectors = len(sectors) if sectors else 1
        sector_shares = {s: round(c / total_sectors, 3) for s, c in sector_counts.most_common()}

        dominant = sector_counts.most_common(1)[0][0] if sector_counts else "mixed"

        result[cid] = {
            "dominant_sector": dominant,
            "sector_shares": sector_shares,
            "size": len(nodes),
            "layer_mix": dict(layers)
        }

    return result


# ============================================================
# SHORTEST PATH (DIJKSTRA)
# ============================================================

def shortest_weighted_path(G: DiGraph, source_type: str, source_id: int,
                           target_type: str, target_id: int) -> dict:
    """Find shortest weighted path between two entities (undirected)."""
    src = _node_key(source_type, source_id)
    tgt = _node_key(target_type, target_id)

    if src not in G.nodes or tgt not in G.nodes:
        return {"error": "One or both nodes not found in graph", "source": src, "target": tgt}

    # Dijkstra on undirected graph
    dist = {src: 0.0}
    prev = {src: None}
    visited = set()
    heap = [(0.0, src)]

    while heap:
        d, u = heapq.heappop(heap)
        if u in visited:
            continue
        visited.add(u)
        if u == tgt:
            break
        for neighbor in G.neighbors_undirected(u):
            if neighbor in visited:
                continue
            w = G.undirected_edge_weight(u, neighbor)
            new_dist = d + w
            if new_dist < dist.get(neighbor, float("inf")):
                dist[neighbor] = new_dist
                prev[neighbor] = u
                heapq.heappush(heap, (new_dist, neighbor))

    if tgt not in prev:
        return {"error": "No path exists between these entities", "source": src, "target": tgt}

    # Reconstruct path
    path = []
    current = tgt
    while current is not None:
        path.append(current)
        current = prev[current]
    path.reverse()

    path_details = []
    for i, node in enumerate(path):
        node_data = G.nodes.get(node, {})
        entry = {"node": node, "name": node_data.get("name", node),
                 "type": node_data.get("entity_type", "unknown")}
        if i < len(path) - 1:
            next_node = path[i + 1]
            if G.has_edge(node, next_node):
                edge_data = G.edge_data(node, next_node)
            elif G.has_edge(next_node, node):
                edge_data = G.edge_data(next_node, node)
            else:
                edge_data = {}
            entry["edge_to_next"] = {
                "weight": edge_data.get("weight", 0),
                "relationship_type": edge_data.get("relationship_type", "unknown")
            }
        path_details.append(entry)

    return {
        "path": path,
        "path_details": path_details,
        "total_weight": dist[tgt],
        "hops": len(path) - 1
    }


# ============================================================
# SAVE SCORES TO DB
# ============================================================

def save_categories_to_db(G: DiGraph, db_path: str = DB_PATH) -> int:
    """Persist computed category and mature flag back to companies table."""
    conn = _get_conn(db_path)
    cur = conn.cursor()
    updated = 0
    for node in G.all_nodes():
        data = G.nodes[node]
        if data.get("entity_type") != "company":
            continue
        cat = data.get("category")
        mature = 1 if data.get("mature") else 0
        cur.execute(
            "UPDATE companies SET category = ?, mature = ? WHERE id = ?",
            (cat, mature, data["entity_id"])
        )
        updated += cur.rowcount
    conn.commit()
    conn.close()
    return updated


def save_scores_to_db(centrality: dict, leverage: dict, clusters: dict,
                      influence: dict = None, adjacency: dict = None,
                      db_path: str = DB_PATH) -> int:
    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Ensure new columns exist
    for table in ("contacts", "companies"):
        for col in ("influence_score", "adjacency_index"):
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL DEFAULT 0")
            except Exception:
                pass

    updated = 0
    for node, cent_score in centrality.items():
        lev_score = leverage.get(node, 0.0)
        cluster_id = clusters.get(node, 0)
        inf_score = (influence or {}).get(node, 0.0)
        adj_score = (adjacency or {}).get(node, 0.0)
        parts = node.split("_", 1)
        entity_type = parts[0]
        entity_id = int(parts[1])

        if entity_type == "contact":
            cur.execute("""
                UPDATE contacts SET centrality_score = ?, leverage_score = ?, cluster_id = ?,
                    influence_score = ?, adjacency_index = ?,
                    updated_at = CURRENT_TIMESTAMP WHERE id = ?
            """, (cent_score, lev_score, cluster_id, inf_score, adj_score, entity_id))
        elif entity_type == "company":
            cur.execute("""
                UPDATE companies SET centrality_score = ?, leverage_score = ?, cluster_id = ?,
                    influence_score = ?, adjacency_index = ?,
                    updated_at = CURRENT_TIMESTAMP WHERE id = ?
            """, (cent_score, lev_score, cluster_id, inf_score, adj_score, entity_id))
        updated += cur.rowcount

    conn.commit()
    conn.close()
    return updated


# ============================================================
# COMPUTE ALL
# ============================================================

def compute_all(db_path: str = DB_PATH) -> dict:
    """Full computation pipeline — graph metrics + influence + adjacency + clusters."""
    G = build_graph(db_path)
    centrality = weighted_out_degree_centrality(G)
    leverage = two_hop_leverage(G)
    influence = influence_propagation(G)
    adjacency = strategic_adjacency_index(G, db_path)
    clusters = detect_clusters(G)
    sector_dom = cluster_sector_dominance(G, clusters)
    coverage = broker_coverage_overlap(G)

    save_categories_to_db(G, db_path)
    updated = save_scores_to_db(centrality, leverage, clusters,
                                influence=influence, adjacency=adjacency,
                                db_path=db_path)

    def ranked(scores: dict, top_n: int = 20):
        ranked_list = []
        for node, score in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]:
            node_data = G.nodes.get(node, {})
            ranked_list.append({
                "node": node,
                "name": node_data.get("name", node),
                "type": node_data.get("entity_type", "unknown"),
                "score": round(score, 4)
            })
        return ranked_list

    cluster_groups = {}
    for node, cid in clusters.items():
        if cid not in cluster_groups:
            cluster_groups[cid] = []
        node_data = G.nodes.get(node, {})
        cluster_groups[cid].append({
            "node": node,
            "name": node_data.get("name", node),
            "type": node_data.get("entity_type", "unknown")
        })

    # Count single-threaded companies
    single_threaded = [c for c, info in coverage.items() if info.get("single_threaded")]

    return {
        "graph_stats": {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "clusters": len(cluster_groups)
        },
        "top_centrality": ranked(centrality),
        "top_leverage": ranked(leverage),
        "top_influence": ranked(influence),
        "top_adjacency": ranked(adjacency),
        "clusters": {str(k): v for k, v in cluster_groups.items()},
        "cluster_sectors": sector_dom,
        "single_threaded_companies": len(single_threaded),
        "records_updated": updated
    }


# ============================================================
# COMPATIBILITY API
# Functions expected by other modules (action_dashboard, path_finder,
# opportunity_scoring, morning_briefing, email_parser, nightly_recompute, etc.)
# ============================================================

def get_db_path() -> str:
    """Return the database path. Used by all sibling modules."""
    return DB_PATH


def find_shortest_path(G: DiGraph, source_node: str, target_node: str):
    """
    Find shortest weighted path between two node keys.
    Returns (path_list, total_weight) tuple, or (None, None) if no path.

    This is the tuple-returning API expected by opportunity_scoring.py,
    path_finder.py, and action_dashboard.py.
    """
    if source_node not in G.nodes or target_node not in G.nodes:
        return None, None

    # Dijkstra on undirected graph
    dist = {source_node: 0.0}
    prev = {source_node: None}
    visited = set()
    heap = [(0.0, source_node)]

    while heap:
        d, u = heapq.heappop(heap)
        if u in visited:
            continue
        visited.add(u)
        if u == target_node:
            break
        for neighbor in G.neighbors_undirected(u):
            if neighbor in visited:
                continue
            w = G.undirected_edge_weight(u, neighbor)
            new_dist = d + w
            if new_dist < dist.get(neighbor, float("inf")):
                dist[neighbor] = new_dist
                prev[neighbor] = u
                heapq.heappush(heap, (new_dist, neighbor))

    if target_node not in visited:
        return None, None

    # Reconstruct path
    path = []
    current = target_node
    while current is not None:
        path.append(current)
        current = prev[current]
    path.reverse()

    return path, dist[target_node]


if __name__ == "__main__":
    import json
    results = compute_all()
    print(json.dumps(results, indent=2))
