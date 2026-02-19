"""
Persistent Memory for OpenClaw.

Stores key-value memories with categories, confidence, expiry, access tracking.
Used by the agent to remember user preferences, facts, and context.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


def _get_conn(db_path=None):
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# CORE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────

def remember(key, value, category='general', source='user', confidence=1.0,
             expires_at=None, db_path=None):
    """Store or update a memory. Upserts on key+category."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Check if exists
    cur.execute("""
        SELECT id FROM openclaw_memory
        WHERE key = ? AND category = ?
    """, (key, category))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
            UPDATE openclaw_memory
            SET value = ?, source = ?, confidence = ?, expires_at = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (value, source, confidence, expires_at, existing['id']))
        mem_id = existing['id']
    else:
        cur.execute("""
            INSERT INTO openclaw_memory
            (key, value, category, source, confidence, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (key, value, category, source, confidence, expires_at))
        mem_id = cur.lastrowid

    conn.commit()
    conn.close()
    return mem_id


def recall(key=None, category=None, limit=20, include_expired=False,
           db_path=None):
    """Retrieve memories by key and/or category. Updates access tracking."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    conditions = []
    params = []

    if key:
        conditions.append("(key LIKE ? OR value LIKE ?)")
        params.extend([f"%{key}%", f"%{key}%"])
    if category:
        conditions.append("category = ?")
        params.append(category)
    if not include_expired:
        conditions.append("(expires_at IS NULL OR expires_at > datetime('now'))")

    where = " AND ".join(conditions) if conditions else "1=1"

    cur.execute(f"""
        SELECT * FROM openclaw_memory
        WHERE {where}
        ORDER BY confidence DESC, updated_at DESC
        LIMIT ?
    """, params + [limit])

    rows = [dict(r) for r in cur.fetchall()]

    # Update access tracking
    if rows:
        ids = [r['id'] for r in rows]
        placeholders = ','.join('?' * len(ids))
        cur.execute(f"""
            UPDATE openclaw_memory
            SET access_count = access_count + 1,
                last_accessed = datetime('now')
            WHERE id IN ({placeholders})
        """, ids)
        conn.commit()

    conn.close()
    return rows


def forget(key=None, category=None, memory_id=None, db_path=None):
    """Delete memories by key, category, or specific ID."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    if memory_id:
        cur.execute("DELETE FROM openclaw_memory WHERE id = ?", (memory_id,))
    elif key and category:
        cur.execute("DELETE FROM openclaw_memory WHERE key = ? AND category = ?",
                    (key, category))
    elif key:
        cur.execute("DELETE FROM openclaw_memory WHERE key LIKE ?", (f"%{key}%",))
    elif category:
        cur.execute("DELETE FROM openclaw_memory WHERE category = ?", (category,))
    else:
        conn.close()
        return 0

    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted


def get_relevant_context(topic, max_items=10, db_path=None):
    """Get memories relevant to a topic. Searches key, value, category."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    words = topic.lower().split()
    if not words:
        conn.close()
        return []

    # Build OR conditions for each word
    conditions = []
    params = []
    for w in words[:5]:  # Cap at 5 words
        conditions.append("(LOWER(key) LIKE ? OR LOWER(value) LIKE ? OR LOWER(category) LIKE ?)")
        params.extend([f"%{w}%", f"%{w}%", f"%{w}%"])

    where = " OR ".join(conditions)
    cur.execute(f"""
        SELECT *, (
            CASE WHEN expires_at IS NULL OR expires_at > datetime('now') THEN 1 ELSE 0 END
        ) as active
        FROM openclaw_memory
        WHERE ({where})
        AND (expires_at IS NULL OR expires_at > datetime('now'))
        ORDER BY confidence DESC, access_count DESC, updated_at DESC
        LIMIT ?
    """, params + [max_items])

    rows = [dict(r) for r in cur.fetchall()]

    # Update access
    if rows:
        ids = [r['id'] for r in rows]
        placeholders = ','.join('?' * len(ids))
        cur.execute(f"""
            UPDATE openclaw_memory
            SET access_count = access_count + 1,
                last_accessed = datetime('now')
            WHERE id IN ({placeholders})
        """, ids)
        conn.commit()

    conn.close()
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# STATS & MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def get_memory_stats(db_path=None):
    """Return summary stats about stored memories."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM openclaw_memory")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT category, COUNT(*) as cnt
        FROM openclaw_memory
        GROUP BY category
        ORDER BY cnt DESC
    """)
    by_category = {r['category']: r['cnt'] for r in cur.fetchall()}

    cur.execute("""
        SELECT COUNT(*) FROM openclaw_memory
        WHERE expires_at IS NOT NULL AND expires_at <= datetime('now')
    """)
    expired = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM openclaw_memory
        WHERE source = 'user'
    """)
    user_memories = cur.fetchone()[0]

    conn.close()
    return {
        'total': total,
        'by_category': by_category,
        'expired': expired,
        'user_memories': user_memories,
    }


def cleanup_expired(db_path=None):
    """Remove expired memories."""
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM openclaw_memory
        WHERE expires_at IS NOT NULL AND expires_at <= datetime('now')
    """)
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='OpenClaw Memory Manager')
    sub = parser.add_subparsers(dest='cmd')

    # remember
    p_rem = sub.add_parser('remember', help='Store a memory')
    p_rem.add_argument('key')
    p_rem.add_argument('value')
    p_rem.add_argument('--category', default='general')
    p_rem.add_argument('--source', default='user')

    # recall
    p_rec = sub.add_parser('recall', help='Retrieve memories')
    p_rec.add_argument('query', nargs='?', default=None)
    p_rec.add_argument('--category', default=None)

    # forget
    p_for = sub.add_parser('forget', help='Delete memories')
    p_for.add_argument('key', nargs='?', default=None)
    p_for.add_argument('--category', default=None)
    p_for.add_argument('--id', type=int, default=None)

    # context
    p_ctx = sub.add_parser('context', help='Get relevant context')
    p_ctx.add_argument('topic')

    # stats
    sub.add_parser('stats', help='Memory stats')

    # cleanup
    sub.add_parser('cleanup', help='Remove expired memories')

    args = parser.parse_args()

    if args.cmd == 'remember':
        mid = remember(args.key, args.value, args.category, args.source)
        print(f"Stored memory #{mid}: {args.key} = {args.value}")

    elif args.cmd == 'recall':
        rows = recall(key=args.query, category=args.category)
        if rows:
            for r in rows:
                print(f"[{r['category']}] {r['key']} = {r['value']} "
                      f"(confidence: {r['confidence']}, accessed: {r['access_count']}x)")
        else:
            print("No memories found.")

    elif args.cmd == 'forget':
        deleted = forget(key=args.key, category=args.category, memory_id=args.id)
        print(f"Deleted {deleted} memories.")

    elif args.cmd == 'context':
        rows = get_relevant_context(args.topic)
        if rows:
            for r in rows:
                print(f"[{r['category']}] {r['key']} = {r['value']}")
        else:
            print("No relevant context found.")

    elif args.cmd == 'stats':
        s = get_memory_stats()
        print(f"Total memories: {s['total']}")
        print(f"User memories: {s['user_memories']}")
        print(f"Expired: {s['expired']}")
        for cat, cnt in s['by_category'].items():
            print(f"  {cat}: {cnt}")

    elif args.cmd == 'cleanup':
        deleted = cleanup_expired()
        print(f"Cleaned up {deleted} expired memories.")

    else:
        parser.print_help()
