# Relationship Engine

CRE intelligence platform. Tracks companies, contacts, funding signals, hiring activity, leases, and outreach — surfaces ranked opportunities with recommended actions.

## Quick Start

```bash
# Install dependencies
pip install streamlit networkx python-dotenv

# Run the main multi-page app
streamlit run app.py

# Or run the unified action dashboard (tabbed)
streamlit run action_dashboard.py
```

The Streamlit app starts at `http://localhost:8501`. Pages:

- **Opportunities** — signal-ranked targets with inline outreach logging
- **Pipeline** — company pipeline by status
- **Signal Feed** — funding, hiring, and executive signals
- **Company Deep Dive** — single-company analysis with graph paths
- **Recent News** — CRE-filtered news with "why it matters"
- **Market Notes** — freeform intel entries
- **Import Data** — CSV upload with validation

## CSV Import

Place CSV files in `data/imports/` and run:

```bash
python3 import_all.py
```

Or use the Import Data page in Streamlit to upload directly.

Templates are in `data/imports/TEMPLATES/`:

| File | Required Columns |
|------|-----------------|
| `clients.csv` | company_name, status, last_deal_date |
| `contacts.csv` | first_name, last_name, company |
| `relationships.csv` | person_a_name, person_b_name, relationship_type, strength |
| `linkedin_connections.csv` | First Name, Last Name, Company |
| `buildings.csv` | company_name, building_address, square_feet |

Individual import scripts:

```bash
python3 import_clients.py data/imports/clients.csv
python3 import_buildings.py data/imports/buildings.csv
python3 linkedin_import.py data/imports/linkedin_connections.csv
python3 import_contacts.py data/imports/contacts.csv
python3 import_relationships.py data/imports/relationships.csv
```

Validate before importing:

```bash
python3 -m importers.validate_csv data/imports/clients.csv clients
```

## Scoring

Two pipelines:

- **Emerging companies** — Lean v1: 4 buckets × 25 pts (funding recency, funding size, hiring growth, geo qualification). Max 100.
- **Enterprise companies** — relationship adjacency only. Direct connection = 100, 2 hops = 75, 3 hops = 50.

Recompute all scores:

```bash
python3 jobs/lean_recompute.py
```

## Outreach Tracking

Log outreach from the Opportunities page or programmatically:

```python
from core.outreach_manager import log_outreach, get_due_followups
log_outreach(company_id=1, outreach_type="email", outcome="sent", follow_up_days=7)
print(get_due_followups())  # shows overdue follow-ups
```

## Discord Morning Briefing

Set `DISCORD_WEBHOOK_URL` in `private_data/.env`, then:

```bash
python3 -m integrations.morning_briefing
```

Includes: follow-ups due, hot funded targets, hiring spikes, outreach gaps, warm intros, weekly stats.

## Scheduled Jobs

Run the scheduler for automated nightly recomputation:

```bash
python3 jobs/scheduler.py
```

Jobs: nightly graph + lean scoring recompute, weekly digest, morning briefing at 7 AM ET.

## Data Quality

```bash
python3 run_data_quality.py          # print to console
python3 run_data_quality.py --save   # save to reports/data_quality.md
```

## Backup

```bash
bash backup.sh
```

Creates timestamped backup of the database in `backups/`.

## Reports

```bash
python3 -m reports.weekly_output             # weekly top 25 + top 10
python3 -m reports.consultant_mode Ramp      # 6-section company deep dive
python3 -m reports.company_profile Ramp      # company profile report
```

## Local LLM (Optional)

Install [Ollama](https://ollama.ai), pull a model, and the engine will use it for signal classification:

```bash
ollama pull llama3.2
python3 -c "from core.local_llm import is_ollama_available; print(is_ollama_available())"
```

## Project Structure

```
relationship_engine/
├── app.py                    # Main Streamlit app
├── action_dashboard.py       # Unified tabbed dashboard
├── config.py                 # Path setup
├── core/                     # Engine modules
│   ├── graph_engine.py       # PageRank graph
│   ├── signals.py            # 14 signal types
│   ├── opportunity_engine.py # Opportunity ranking
│   ├── outreach_manager.py   # Follow-up tracking
│   ├── scoring_v1.py         # Lean v1 scoring
│   ├── local_llm.py          # Ollama integration
│   └── ...
├── pages/                    # Streamlit pages
├── importers/                # CSV validation + wrappers
├── jobs/                     # Scheduled jobs
├── reports/                  # Report generators
├── scrapers/                 # Signal scrapers
├── integrations/             # Discord, calendar, email
├── analytics/                # Conversion, timing, sector
├── data/imports/TEMPLATES/   # CSV templates
└── private_data/             # DB + .env (gitignored)
```
