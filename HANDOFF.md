# Relationship Engine — Claude Code Handoff

## Project Overview

A personal relationship intelligence system for commercial real estate brokerage. Tracks companies, contacts, relationships, deals, funding events, and hiring signals. Computes network centrality, opportunity scores, and surfaces actionable intelligence.

**Owner:** Graham Walter, CBRE (Bob Alexander's team)
**Location:** ~/relationship_engine/ on Mac Mini
**Database:** SQLite at ~/relationship_engine/data/relationship_engine.db
**GitHub:** https://github.com/grahamjwa/relationship-engine (private)

---

## Current System Status

### Working Components

| Component | File | Status | Notes |
|-----------|------|--------|-------|
| Graph Engine | graph_engine.py | ✅ Working | Computes centrality, leverage, clusters |
| Opportunity Scoring | opportunity_scoring.py | ✅ Working | Scores companies/contacts |
| Nightly Recompute | nightly_recompute.py | ✅ Working | Runs at 2 AM ET |
| Signal Pipeline | scrapers/signal_pipeline.py | ✅ Working | Funding + hiring signals |
| Search Client | scrapers/search_client.py | ✅ Working | SerpApi wrapper |
| Signal Classifier | scrapers/signal_classifier.py | ✅ Working | Claude classification |
| Document Parser | document_parser.py | ✅ Working | Extracts entities from docs |
| Conversational Entry | conversational_entry.py | ✅ Working | NL to DB operations |
| Dashboard | dashboard.py | ✅ Working | Streamlit UI |
| Data Entry CLI | data_entry.py | ✅ Working | Manual data entry |
| Scheduler | scheduler.py | ✅ Working | APScheduler daemon |
| Discord Bot | OpenClaw gateway | ✅ Working | Responds to DMs/@mentions |
| Discord Webhook | .env | ✅ Working | Posts alerts to #general |

### Scheduled Jobs

| Job | Time (ET) | Function |
|-----|-----------|----------|
| Nightly Recompute | 2:00 AM | Graph + opportunity scores + Discord report |
| Morning Signal Scan | 6:00 AM | Funding + hiring for 10 companies |
| Afternoon Signal Scan | 5:00 PM | Funding only for 5 companies |

### Known Issues to Fix

1. **High Priority duplicates** — `identify_high_priority()` in opportunity_scoring.py returns duplicates when a company has multiple funding events
2. **Funding event duplicates** — Same funding round inserted multiple times from different sources (dedup by company + amount + date range)
3. **Flex/Ramp status** — Currently `high_growth_target` but should be `active_client`
4. **At Risk false positives** — Shows clients as "at risk" even if contacted recently (outreach_log may be incomplete)

---

## Database Schema

### Core Tables

```sql
companies (id, name, type, status, sector, hq_city, hq_state, website, employee_count, 
           founded_year, notes, centrality_score, leverage_score, cluster_id, opportunity_score)

contacts (id, first_name, last_name, company_id, title, role_level, email, phone, 
          linkedin_url, alma_mater, previous_companies, notes, centrality_score, 
          leverage_score, cluster_id, priority_score)

relationships (id, contact_id_a, contact_id_b, relationship_type, strength, direction, 
               context, notes)

buildings (id, name, address, city, state, submarket, building_class, total_sf, 
           owner_company_id, managing_agent, we_rep, notes)

leases (id, company_id, building_id, floor, square_feet, lease_start, lease_expiry, 
        rent_psf, lease_type, source, confidence, notes)

deals (id, company_id, building_id, deal_type, status, square_feet, deal_value, 
       our_role, lead_broker_id, intro_path, started_date, closed_date, notes)

outreach_log (id, target_company_id, target_contact_id, outreach_date, outreach_type, 
              intro_path_used, angle, outcome, follow_up_date, follow_up_done, deal_id, notes)

funding_events (id, company_id, event_date, round_type, amount, lead_investor, 
                all_investors, post_valuation, source_url, notes)

hiring_signals (id, company_id, signal_date, signal_type, role_title, location, 
                details, source_url, relevance, notes)
```

### Key Constraints

- `companies.type`: tenant, landlord, investor, lender, developer, advisory, other
- `companies.status`: active_client, former_client, high_growth_target, prospect, network_portfolio, team_affiliated, watching
- `contacts.role_level`: c_suite, decision_maker, influencer, team, external_partner
- `relationships.relationship_type`: colleague, former_colleague, alumni, investor, client, friend, board, deal_counterpart, introduced_by, other
- `relationships.strength`: 1-5 (5=weekly contact, 1=aware of)
- `hiring_signals.signal_type`: job_posting, headcount_growth, new_office, leadership_hire, press_announcement
- `hiring_signals.relevance`: high, medium, low

---

## Key Functions to Audit

### graph_engine.py
- `build_graph()` — Builds NetworkX DiGraph from database
- `compute_centrality()` — Weighted out-degree
- `compute_two_hop_leverage()` — 2-hop reach calculation
- `detect_clusters()` — Louvain community detection
- `find_shortest_path()` — Path between two nodes
- `compute_all()` — Full pipeline, saves to DB

### opportunity_scoring.py
- `score_company_funding()` — 0-100 based on recency/amount
- `score_company_hiring()` — 0-100 based on signals
- `score_company_lease_expiry()` — 0-100 based on upcoming expiries
- `score_company_relationship_proximity()` — 0-100 based on team paths
- `compute_company_opportunity_score()` — Weighted total
- `identify_high_priority()` — **NEEDS DEDUP FIX**
- `identify_undercovered()` — No outreach in 90 days
- `identify_relationships_at_risk()` — Active clients, no outreach 60 days

### scrapers/signal_pipeline.py
- `insert_funding_event()` — **NEEDS BETTER DEDUP** (same company + similar amount + within 7 days)
- `insert_hiring_signal()` — Deduped by source_url
- `scan_company()` — Searches + classifies + inserts
- `run_signal_scan()` — Batch scan for target companies

---

## Environment Setup

### Required Packages
```bash
pip3 install streamlit networkx python-louvain plotly PyPDF2 apscheduler 
             anthropic requests python-dotenv schedule matplotlib pandas
```

### Environment Variables (.env)
```
ANTHROPIC_API_KEY=sk-ant-...
DISCORD_BOT_TOKEN=MTQ3...
DISCORD_SERVER_ID=...
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
SERPAPI_KEY=...
```

### Services
- **Scheduler:** `nohup python3 scheduler.py > logs/scheduler.log 2>&1 &`
- **OpenClaw Gateway:** Running as LaunchAgent (auto-starts)
- **Dashboard:** `python3 -m streamlit run dashboard.py`

---

## Testing Commands

```bash
# Graph engine
python3 -c "from graph_engine import compute_all; compute_all()"

# Opportunity scoring
python3 opportunity_scoring.py

# Nightly recompute (sends Discord report)
python3 nightly_recompute.py

# Signal pipeline (uses API quota)
cd scrapers && python3 signal_pipeline.py

# Document parser
python3 -c "from document_parser import parse_document; print(parse_document('test_note.txt'))"

# Conversational entry
python3 -c "from conversational_entry import parse_input; print(parse_input('Add Blackstone as prospect'))"

# Dashboard
python3 -m streamlit run dashboard.py
```

---

## Remaining Tasks

### High Priority
1. [ ] Fix `identify_high_priority()` to deduplicate by company
2. [ ] Fix `insert_funding_event()` to deduplicate by company + amount + date window
3. [ ] Update Flex and Ramp status to `active_client`
4. [ ] Populate outreach_log with real client contact history

### Medium Priority
5. [ ] Create `monitored_clients.json` or table for current client tracking
6. [ ] Add smarter hiring signal relevance (detect "real estate" in title)
7. [ ] Improve decay calculations (use `updated_at` not just `created_at`)
8. [ ] Add weekly digest Discord message (separate from daily)

### Low Priority
9. [ ] Dashboard UI polish (better charts, filters)
10. [ ] Add email integration (send reports, read forwarded emails)
11. [ ] Add meeting notes parser
12. [ ] Build competitive threat detection

---

## File Structure

```
~/relationship_engine/
├── .env                      # API keys (not in git)
├── .gitignore
├── ARCHITECTURE.md           # Scoring philosophy & entity model
├── HANDOFF.md               # This file
├── README.md
├── requirements.txt
│
├── graph_engine.py          # Core graph computations
├── opportunity_scoring.py   # Opportunity & priority scoring
├── nightly_recompute.py     # Nightly job + Discord report
├── dashboard.py             # Streamlit dashboard
├── data_entry.py            # CLI data entry
├── document_parser.py       # PDF/DOCX entity extraction
├── conversational_entry.py  # NL to DB operations
├── scheduler.py             # APScheduler daemon
│
├── scrapers/
│   ├── __init__.py
│   ├── search_client.py     # SerpApi wrapper
│   ├── signal_classifier.py # Claude classification
│   ├── signal_pipeline.py   # Orchestration
│   ├── funding_scraper.py   # (legacy, replaced by pipeline)
│   └── hiring_scraper.py    # (legacy, replaced by pipeline)
│
├── data/
│   └── relationship_engine.db
│
├── private_data/            # Sensitive imports (not in git)
│
├── logs/
│   └── scheduler.log
│
└── venv/
```

---

## Quick Fixes for Claude Code

### Fix 1: Deduplicate High Priority

In `opportunity_scoring.py`, update `identify_high_priority()`:

```python
def identify_high_priority(conn: sqlite3.Connection) -> List[Dict]:
    """Identify high-priority opportunities (deduplicated by company)."""
    cur = conn.cursor()
    results = []
    seen_companies = set()
    
    # ... existing queries ...
    
    # After each query, dedupe:
    for row in cur.fetchall():
        if row[0] not in seen_companies:
            seen_companies.add(row[0])
            results.append({...})
    
    return results
```

### Fix 2: Better Funding Deduplication

In `scrapers/signal_pipeline.py`, update `insert_funding_event()`:

```python
# Check for duplicate (same company, similar amount, within 7 days)
cur.execute("""
    SELECT id FROM funding_events 
    WHERE company_id = ? 
    AND (source_url = ? OR (
        ABS(amount - ?) < ? * 0.1  -- within 10% of amount
        AND event_date BETWEEN date(?, '-7 days') AND date(?, '+7 days')
    ))
""", (company_id, source_url, amount, amount, event_date, event_date))
```

### Fix 3: Update Company Status

```bash
sqlite3 ~/relationship_engine/data/relationship_engine.db "
UPDATE companies SET status = 'active_client' WHERE name IN ('Flex', 'Ramp');
"
```

---

## Contact

For questions about this system, context is in:
- This HANDOFF.md file
- ARCHITECTURE.md (scoring philosophy)
- Chat history with Claude (search "relationship engine" or "openclaw")
