"""
Conversational Entry Module for Relationship Engine
Parses natural language input into structured database operations
using the Anthropic API.
"""

import os
import sys
import json
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

try:
    from dotenv import load_dotenv
    load_dotenv()  # config.py already loaded .env
except ImportError:
    pass

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


PARSE_PROMPT = """You are a natural language parser for a real estate relationship intelligence database.

The database has these tables:
- companies: id, name, industry, sector, hq_city, hq_state, website, employee_count, revenue_estimate, status (active|inactive|prospect|client)
- contacts: id, first_name, last_name, email, phone, title, company_id, linkedin_url, status
- buildings: id, name, address, city, state, zip, property_type, square_footage, floors, year_built, owner_company_id
- leases: id, building_id, tenant_company_id, floor, square_footage, lease_start, lease_end, annual_rent, rent_psf, lease_type, status
- deals: id, name, deal_type, status, company_id, building_id, contact_id, estimated_value, commission_rate, close_date
- relationships: id, source_type, source_id, target_type, target_id, relationship_type, strength, confidence, base_weight, last_interaction
- outreach_log: id, contact_id, company_id, channel, direction, subject, body, outcome, outreach_date, follow_up_date
- funding_events: id, company_id, round_type, amount, lead_investor, event_date
- hiring_signals: id, company_id, signal_type, title, description, signal_date

Relationship types: works_at, knows, referred_by, tenant_of, advisor_to, investor_in, partner_with, competes_with
Deal types: acquisition, disposition, lease, development, financing
Deal statuses: prospecting, active, under_contract, closed, dead

Parse the user's natural language input and return a JSON object describing the proposed database operations:
{
  "operations": [
    {
      "action": "insert|update|delete",
      "table": "companies|contacts|buildings|leases|deals|relationships|outreach_log|funding_events|hiring_signals",
      "data": { ... fields and values ... },
      "lookup": { ... optional fields to find existing records ... },
      "confidence": 0.0-1.0,
      "reasoning": "brief explanation"
    }
  ],
  "ambiguities": ["list of anything unclear that should be confirmed"],
  "summary": "one-line summary of what will be done"
}

Rules:
- Set confidence based on how certain you are about the parsed intent
- Flag any ambiguities rather than guessing
- For relationships, infer strength (1-10) and relationship_type from context
- Default status for new companies/contacts is "prospect" unless stated otherwise
- Return ONLY valid JSON, no markdown formatting

USER INPUT:
"""


def parse_input(text: str, api_key: Optional[str] = None) -> dict:
    """
    Parse natural language input into structured database operations.

    Args:
        text: Natural language input (e.g., "Add Citadel as a prospect, Ken Griffin is CEO")
        api_key: Anthropic API key (falls back to ANTHROPIC_API_KEY env var)

    Returns:
        Structured JSON of proposed database operations (NOT auto-inserted)
    """
    if not HAS_ANTHROPIC:
        raise ImportError("anthropic package required. Install with: pip install anthropic")

    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("No Anthropic API key provided. Set ANTHROPIC_API_KEY in .env or pass api_key parameter.")

    if not text or not text.strip():
        return {"error": "Empty input provided", "operations": []}

    client = anthropic.Anthropic(api_key=key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[
            {"role": "user", "content": PARSE_PROMPT + text}
        ]
    )

    response_text = message.content[0].text.strip()

    # Handle potential markdown code blocks
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        response_text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        return {
            "error": "Failed to parse API response as JSON",
            "raw_response": response_text,
            "operations": []
        }

    parsed["_metadata"] = {
        "original_input": text,
        "status": "proposed",
        "note": "These operations are PROPOSED only. Review before executing."
    }

    return parsed


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python conversational_entry.py "Add Citadel as a prospect, Ken Griffin is CEO"')
        sys.exit(1)
    result = parse_input(" ".join(sys.argv[1:]))
    print(json.dumps(result, indent=2))
