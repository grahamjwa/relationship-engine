"""
API Cost Tracker for OpenClaw.

Logs API usage (tokens, cost) and provides spend summaries.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path

# Pricing per 1K tokens (approximate, update as needed)
MODEL_PRICING = {
    'claude-3-haiku': {'input': 0.00025, 'output': 0.00125},
    'claude-3-sonnet': {'input': 0.003, 'output': 0.015},
    'claude-3-opus': {'input': 0.015, 'output': 0.075},
    'claude-3.5-sonnet': {'input': 0.003, 'output': 0.015},
    'claude-3.5-haiku': {'input': 0.001, 'output': 0.005},
    'gpt-4o-mini': {'input': 0.00015, 'output': 0.0006},
}


def _get_conn(db_path=None):
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def log_usage(service, endpoint=None, model=None, input_tokens=0, output_tokens=0,
              cost_usd=None, agent='openclaw', subagent_run_id=None, db_path=None):
    """Log an API call. Auto-calculates cost if model is known and cost not provided."""
    if cost_usd is None and model:
        # Try to calculate
        for model_key, pricing in MODEL_PRICING.items():
            if model_key in (model or '').lower():
                cost_usd = (input_tokens / 1000 * pricing['input'] +
                           output_tokens / 1000 * pricing['output'])
                break
        if cost_usd is None:
            cost_usd = 0

    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO api_usage
        (service, endpoint, model, input_tokens, output_tokens, cost_usd,
         agent, subagent_run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (service, endpoint, model, input_tokens, output_tokens, cost_usd or 0,
          agent, subagent_run_id))
    usage_id = cur.lastrowid
    conn.commit()
    conn.close()
    return usage_id


def get_spend_summary(days=30, db_path=None):
    """Get spend summary for the last N days."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(SUM(cost_usd), 0) as total_cost,
               COALESCE(SUM(input_tokens), 0) as total_input,
               COALESCE(SUM(output_tokens), 0) as total_output,
               COUNT(*) as total_calls
        FROM api_usage
        WHERE created_at >= datetime('now', ? || ' days')
    """, (f"-{days}",))
    row = dict(cur.fetchone())

    # Today's spend
    cur.execute("""
        SELECT COALESCE(SUM(cost_usd), 0) as today_cost,
               COUNT(*) as today_calls
        FROM api_usage
        WHERE created_at >= datetime('now', 'start of day')
    """)
    today = dict(cur.fetchone())

    # By service
    cur.execute("""
        SELECT service, COALESCE(SUM(cost_usd), 0) as cost,
               COUNT(*) as calls
        FROM api_usage
        WHERE created_at >= datetime('now', ? || ' days')
        GROUP BY service
        ORDER BY cost DESC
    """, (f"-{days}",))
    by_service = {r['service']: {'cost': r['cost'], 'calls': r['calls']}
                  for r in cur.fetchall()}

    # By agent
    cur.execute("""
        SELECT agent, COALESCE(SUM(cost_usd), 0) as cost,
               COUNT(*) as calls
        FROM api_usage
        WHERE created_at >= datetime('now', ? || ' days')
        GROUP BY agent
        ORDER BY cost DESC
    """, (f"-{days}",))
    by_agent = {r['agent']: {'cost': r['cost'], 'calls': r['calls']}
                for r in cur.fetchall()}

    # Daily trend (last 7 days)
    cur.execute("""
        SELECT date(created_at) as day,
               COALESCE(SUM(cost_usd), 0) as cost,
               COUNT(*) as calls
        FROM api_usage
        WHERE created_at >= datetime('now', '-7 days')
        GROUP BY date(created_at)
        ORDER BY day ASC
    """)
    daily = [dict(r) for r in cur.fetchall()]

    conn.close()

    return {
        'total_cost': row['total_cost'],
        'total_input_tokens': row['total_input'],
        'total_output_tokens': row['total_output'],
        'total_calls': row['total_calls'],
        'today_cost': today['today_cost'],
        'today_calls': today['today_calls'],
        'by_service': by_service,
        'by_agent': by_agent,
        'daily_trend': daily,
        'period_days': days,
    }


def get_sidebar_widget_data(db_path=None):
    """Compact data for dashboard sidebar display."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(SUM(cost_usd), 0) FROM api_usage
        WHERE created_at >= datetime('now', 'start of day')
    """)
    today = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(cost_usd), 0) FROM api_usage
        WHERE created_at >= datetime('now', '-7 days')
    """)
    week = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(cost_usd), 0) FROM api_usage
        WHERE created_at >= datetime('now', '-30 days')
    """)
    month = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM api_usage
        WHERE created_at >= datetime('now', 'start of day')
    """)
    calls_today = cur.fetchone()[0]

    conn.close()
    return {
        'today': today,
        'week': week,
        'month': month,
        'calls_today': calls_today,
    }
