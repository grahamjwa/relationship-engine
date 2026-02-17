"""
Configurable Alert Rules Engine for Relationship Engine.

Evaluates custom SQL-based rules against the database, tracks triggered alerts,
and provides reporting and Discord integration.
"""

import sys
import os
import sqlite3
import logging
import argparse
import json
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

# Add repo root to path for imports
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.graph_engine import get_db_path

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def _get_conn(db_path: str) -> sqlite3.Connection:
    """Get database connection with Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_alert_tables(db_path: str = None) -> None:
    """Initialize alert_rules and alert_history tables if they don't exist."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Alert rules table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            rule_type TEXT NOT NULL,
            condition_sql TEXT NOT NULL,
            threshold REAL,
            enabled INTEGER DEFAULT 1,
            last_triggered DATETIME,
            trigger_count INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Alert history table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER,
            triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            matches_found INTEGER,
            details TEXT,
            FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Alert tables initialized.")


def seed_default_rules(db_path: str = None) -> None:
    """Seed built-in default rules on first run."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Check if rules already exist
    cur.execute("SELECT COUNT(*) as cnt FROM alert_rules")
    if cur.fetchone()["cnt"] > 0:
        logger.info("Default rules already seeded. Skipping.")
        conn.close()
        return

    default_rules = [
        {
            "name": "Client No Contact 45d",
            "rule_type": "no_contact",
            "condition_sql": """
                SELECT c.id, c.name, COALESCE(r.last_interaction, '1970-01-01') as last_contact
                FROM companies c
                LEFT JOIN relationships r ON c.id = r.target_id AND r.target_type = 'company'
                WHERE c.status = 'active_client'
                  AND DATE(COALESCE(r.last_interaction, '1970-01-01')) < DATE('now', '-45 days')
                GROUP BY c.id
            """,
            "threshold": 0
        },
        {
            "name": "Prospect Large Raise",
            "rule_type": "funding_alert",
            "condition_sql": """
                SELECT id, name, cash_reserves
                FROM companies
                WHERE status = 'prospect'
                  AND cash_reserves > 100000000
                  AND cash_updated_at > DATE('now', '-30 days')
            """,
            "threshold": 0
        },
        {
            "name": "Competitor Repeat Win",
            "rule_type": "competitor_tracking",
            "condition_sql": """
                SELECT competitor_broker, COUNT(*) as loss_count
                FROM (
                    SELECT DISTINCT deal_id, competitor_broker
                    FROM deals
                    WHERE status = 'lost'
                      AND competitor_broker IS NOT NULL
                    GROUP BY deal_id, competitor_broker
                    HAVING COUNT(*) >= 2
                )
                GROUP BY competitor_broker
                HAVING loss_count >= 2
            """,
            "threshold": 1
        },
        {
            "name": "Lease Expiry 12m",
            "rule_type": "lease_alert",
            "condition_sql": """
                SELECT id, name, lease_expiry_date
                FROM companies
                WHERE status = 'active_client'
                  AND lease_expiry_date IS NOT NULL
                  AND DATE(lease_expiry_date) BETWEEN DATE('now') AND DATE('now', '+12 months')
                ORDER BY lease_expiry_date ASC
            """,
            "threshold": 0
        },
        {
            "name": "High Score No Outreach",
            "rule_type": "engagement_alert",
            "condition_sql": """
                SELECT o.id, o.company_id, o.opportunity_score,
                       MAX(COALESCE(a.activity_date, '1970-01-01')) as last_outreach
                FROM opportunities o
                LEFT JOIN activities a ON o.id = a.opportunity_id
                WHERE o.opportunity_score > 40
                  AND DATE(COALESCE(a.activity_date, '1970-01-01')) < DATE('now', '-30 days')
                GROUP BY o.id
            """,
            "threshold": 0
        }
    ]

    for rule in default_rules:
        try:
            cur.execute("""
                INSERT INTO alert_rules (name, rule_type, condition_sql, threshold, enabled)
                VALUES (?, ?, ?, ?, 1)
            """, (rule["name"], rule["rule_type"], rule["condition_sql"], rule["threshold"]))
        except Exception as e:
            logger.warning(f"Could not seed rule '{rule['name']}': {e}")

    conn.commit()
    conn.close()
    logger.info(f"Seeded {len(default_rules)} default rules.")


# ============================================================
# RULE EVALUATION
# ============================================================

def evaluate_rule(rule_id: int, db_path: str = None) -> Dict:
    """
    Evaluate a single rule by its ID.

    Returns:
        {
            'rule_id': int,
            'name': str,
            'triggered': bool,
            'matches_found': int,
            'matches': list,
            'error': str (if any)
        }
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Fetch rule
    cur.execute("SELECT * FROM alert_rules WHERE id = ?", (rule_id,))
    rule_row = cur.fetchone()

    if not rule_row:
        conn.close()
        return {
            'rule_id': rule_id,
            'triggered': False,
            'error': f'Rule {rule_id} not found'
        }

    rule = dict(rule_row)
    result = {
        'rule_id': rule_id,
        'name': rule['name'],
        'rule_type': rule['rule_type'],
        'triggered': False,
        'matches_found': 0,
        'matches': [],
        'error': None
    }

    if not rule['enabled']:
        result['error'] = 'Rule is disabled'
        conn.close()
        return result

    try:
        # Execute the condition SQL
        cur.execute(rule['condition_sql'])
        matches = [dict(row) for row in cur.fetchall()]
        matches_count = len(matches)

        # Check threshold (default: trigger if any matches)
        threshold = rule['threshold'] if rule['threshold'] is not None else 0
        triggered = matches_count > threshold

        result['matches_found'] = matches_count
        result['triggered'] = triggered
        result['matches'] = matches[:10]  # Limit to first 10 for reporting

        # Log to alert_history if triggered
        if triggered:
            details_json = json.dumps({
                'rule_type': rule['rule_type'],
                'match_count': matches_count,
                'sample_matches': matches[:5]
            })
            cur.execute("""
                INSERT INTO alert_history (rule_id, matches_found, details)
                VALUES (?, ?, ?)
            """, (rule_id, matches_count, details_json))

            # Update rule metadata
            cur.execute("""
                UPDATE alert_rules
                SET last_triggered = CURRENT_TIMESTAMP, trigger_count = trigger_count + 1
                WHERE id = ?
            """, (rule_id,))

            conn.commit()
            logger.info(f"Rule '{rule['name']}' triggered with {matches_count} matches.")

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error evaluating rule {rule_id}: {e}")

    conn.close()
    return result


def evaluate_all(db_path: str = None) -> List[Dict]:
    """
    Evaluate all enabled rules and return results.

    Returns:
        List of evaluation results (triggered or not)
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id FROM alert_rules WHERE enabled = 1")
    rule_ids = [row['id'] for row in cur.fetchall()]
    conn.close()

    results = []
    for rule_id in rule_ids:
        result = evaluate_rule(rule_id, db_path)
        results.append(result)

    logger.info(f"Evaluated {len(rule_ids)} rules.")
    return results


# ============================================================
# RULE MANAGEMENT
# ============================================================

def add_rule(name: str, rule_type: str, condition_sql: str,
             threshold: float = None, db_path: str = None) -> Tuple[bool, str]:
    """
    Add a custom rule to the database.

    Returns:
        (success: bool, message: str)
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    try:
        # Validate SQL by attempting a dry run
        cur.execute(condition_sql)
        cur.fetchone()  # Try to fetch at least one row

        # Insert rule
        cur.execute("""
            INSERT INTO alert_rules (name, rule_type, condition_sql, threshold, enabled)
            VALUES (?, ?, ?, ?, 1)
        """, (name, rule_type, condition_sql, threshold))

        conn.commit()
        rule_id = cur.lastrowid
        conn.close()

        logger.info(f"Added rule '{name}' (ID: {rule_id})")
        return True, f"Rule '{name}' added successfully (ID: {rule_id})"

    except Exception as e:
        conn.close()
        logger.error(f"Failed to add rule '{name}': {e}")
        return False, f"Error adding rule: {e}"


def list_rules(db_path: str = None) -> List[Dict]:
    """
    List all rules with their current status.

    Returns:
        List of rule dictionaries
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, rule_type, enabled, last_triggered, trigger_count, created_at
        FROM alert_rules
        ORDER BY created_at DESC
    """)

    rules = [dict(row) for row in cur.fetchall()]
    conn.close()

    return rules


def disable_rule(rule_id: int, db_path: str = None) -> Tuple[bool, str]:
    """Disable a rule."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    try:
        cur.execute("UPDATE alert_rules SET enabled = 0 WHERE id = ?", (rule_id,))
        conn.commit()
        conn.close()
        return True, f"Rule {rule_id} disabled."
    except Exception as e:
        conn.close()
        return False, f"Error disabling rule: {e}"


def enable_rule(rule_id: int, db_path: str = None) -> Tuple[bool, str]:
    """Enable a rule."""
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    try:
        cur.execute("UPDATE alert_rules SET enabled = 1 WHERE id = ?", (rule_id,))
        conn.commit()
        conn.close()
        return True, f"Rule {rule_id} enabled."
    except Exception as e:
        conn.close()
        return False, f"Error enabling rule: {e}"


# ============================================================
# REPORTING
# ============================================================

def generate_alert_report(db_path: str = None) -> str:
    """
    Generate a Markdown summary of all triggered alerts.

    Returns:
        Markdown formatted report
    """
    if db_path is None:
        db_path = get_db_path()

    # Evaluate all rules
    results = evaluate_all(db_path)
    triggered = [r for r in results if r.get('triggered')]

    # Build report
    report = []
    report.append("# Alert Rules Report\n")
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report.append(f"**Total Rules:** {len(results)}")
    report.append(f"**Triggered:** {len(triggered)}\n")

    if triggered:
        report.append("## Triggered Alerts\n")
        for alert in triggered:
            report.append(f"### {alert['name']}")
            report.append(f"- **Type:** {alert.get('rule_type', 'N/A')}")
            report.append(f"- **Matches Found:** {alert['matches_found']}")

            if alert.get('matches'):
                report.append("- **Sample Matches:**")
                for i, match in enumerate(alert['matches'][:3], 1):
                    # Convert match dict to readable format
                    match_str = " | ".join([f"{k}: {v}" for k, v in match.items()])
                    report.append(f"  {i}. {match_str}")
            report.append("")
    else:
        report.append("## No Alerts Triggered\n")
        report.append("All rules evaluated successfully with no matches.\n")

    # Rule summary
    report.append("## All Rules Status\n")
    report.append("| Rule | Type | Enabled | Last Triggered | Count |")
    report.append("|------|------|---------|-----------------|-------|")

    all_rules = list_rules(db_path)
    for rule in all_rules:
        enabled_str = "✓" if rule['enabled'] else "✗"
        last_trigger = rule['last_triggered'] or "Never"
        report.append(f"| {rule['name']} | {rule['rule_type']} | {enabled_str} | {last_trigger} | {rule['trigger_count']} |")

    report.append("")

    return "\n".join(report)


# ============================================================
# DISCORD INTEGRATION
# ============================================================

def send_alerts_discord(alerts: List[Dict], webhook_url: str = None) -> Tuple[bool, str]:
    """
    Post triggered alerts to Discord.

    Args:
        alerts: List of alert dicts (from evaluate_all or evaluate_rule)
        webhook_url: Discord webhook URL (env var: DISCORD_WEBHOOK_URL if not provided)

    Returns:
        (success: bool, message: str)
    """
    if not webhook_url:
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        return False, "No webhook URL provided and DISCORD_WEBHOOK_URL env var not set"

    try:
        import requests
    except ImportError:
        return False, "requests library not installed. Install with: pip install requests"

    triggered = [a for a in alerts if a.get('triggered')]

    if not triggered:
        return True, "No triggered alerts to send."

    # Build Discord message
    embeds = []
    for alert in triggered:
        embed = {
            "title": alert['name'],
            "description": f"**Type:** {alert.get('rule_type', 'N/A')}\n**Matches:** {alert['matches_found']}",
            "color": 16711680,  # Red
            "timestamp": datetime.now().isoformat()
        }

        # Add sample matches as fields
        if alert.get('matches'):
            matches_text = "\n".join([
                " | ".join([f"{k}: {v}" for k, v in match.items()])
                for match in alert['matches'][:3]
            ])
            embed["fields"] = [{
                "name": "Sample Matches",
                "value": f"```\n{matches_text}\n```",
                "inline": False
            }]

        embeds.append(embed)

    payload = {
        "content": f":warning: **{len(triggered)} Alert(s) Triggered**",
        "embeds": embeds
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Sent {len(triggered)} alert(s) to Discord.")
        return True, f"Sent {len(triggered)} alert(s) to Discord"
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")
        return False, f"Discord error: {e}"


# ============================================================
# CLI
# ============================================================

def main():
    """Command-line interface for alert rules."""
    parser = argparse.ArgumentParser(description="Alert Rules Engine")
    parser.add_argument("--evaluate", action="store_true", help="Evaluate all enabled rules")
    parser.add_argument("--list", action="store_true", help="List all rules")
    parser.add_argument("--report", action="store_true", help="Generate alert report")
    parser.add_argument("--add-rule", type=str, help="Add a new rule (requires --name, --type, --sql)")
    parser.add_argument("--name", type=str, help="Rule name")
    parser.add_argument("--type", type=str, help="Rule type")
    parser.add_argument("--sql", type=str, help="Condition SQL")
    parser.add_argument("--threshold", type=float, help="Rule threshold")
    parser.add_argument("--enable-rule", type=int, help="Enable a rule by ID")
    parser.add_argument("--disable-rule", type=int, help="Disable a rule by ID")
    parser.add_argument("--discord", action="store_true", help="Send triggered alerts to Discord")
    parser.add_argument("--init", action="store_true", help="Initialize tables and seed default rules")
    parser.add_argument("--db", type=str, help="Database path (overrides default)")

    args = parser.parse_args()

    db_path = args.db or get_db_path()

    # Initialize if requested
    if args.init:
        init_alert_tables(db_path)
        seed_default_rules(db_path)
        print("Initialized alert tables and seeded default rules.")
        return

    # List rules
    if args.list:
        rules = list_rules(db_path)
        print("\nAlert Rules:")
        print("-" * 80)
        for rule in rules:
            status = "ENABLED" if rule['enabled'] else "DISABLED"
            print(f"ID {rule['id']}: {rule['name']} [{status}]")
            print(f"  Type: {rule['rule_type']}")
            print(f"  Triggered: {rule['trigger_count']} times")
            print(f"  Last Triggered: {rule['last_triggered'] or 'Never'}")
            print()
        return

    # Add rule
    if args.add_rule:
        if not args.name or not args.type or not args.sql:
            print("Error: --add-rule requires --name, --type, and --sql")
            return
        success, message = add_rule(args.name, args.type, args.sql, args.threshold, db_path)
        print(message)
        return

    # Enable/disable rule
    if args.enable_rule:
        success, message = enable_rule(args.enable_rule, db_path)
        print(message)
        return

    if args.disable_rule:
        success, message = disable_rule(args.disable_rule, db_path)
        print(message)
        return

    # Evaluate rules
    if args.evaluate:
        results = evaluate_all(db_path)
        triggered = [r for r in results if r.get('triggered')]
        print(f"\nEvaluated {len(results)} rules.")
        print(f"Triggered: {len(triggered)}")
        print()
        for result in results:
            status = "✓ TRIGGERED" if result.get('triggered') else "○ No match"
            error_msg = f" [ERROR: {result['error']}]" if result.get('error') else ""
            rule_name = result.get('name', f'Rule {result["rule_id"]}')
            print(f"{status} {rule_name} ({result['matches_found']} matches){error_msg}")
        return

    # Generate report
    if args.report:
        report = generate_alert_report(db_path)
        print(report)
        return

    # Discord integration
    if args.discord:
        results = evaluate_all(db_path)
        success, message = send_alerts_discord(results)
        print(message)
        return

    # Default: show help
    parser.print_help()


if __name__ == "__main__":
    main()
