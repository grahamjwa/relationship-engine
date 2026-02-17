# Relationship Engine - Current System State

Last Updated: 2026-02-16

## Executive Summary

The Relationship Engine is a sophisticated commercial real estate intelligence platform built on graph analysis, predictive scoring, and signal detection. It tracks 57 companies, 19 contacts, 7 buildings, and 38 relationships to identify high-value business opportunities through funding events, hiring activity, lease expirations, and executive movements.

---

## Database Schema

### Core Tables

#### buildings (7 rows)

Columns: id, name, address, city, state, zip, property_type, square_footage, floors, year_built, owner_company_id, notes, created_at, updated_at, submarket, building_class, total_sf, managing_agent, we_rep

Primary real estate assets tracked in the system. Links to owner companies and manages agent relationships.

#### companies (57 rows)

Columns: id, name, industry, sector, hq_city, hq_state, website, employee_count, revenue_estimate, status, notes, centrality_score, leverage_score, cluster_id, created_at, updated_at, type, opportunity_score, spoc_covered, founded_year, influence_score, adjacency_index, opp_funding, opp_hiring, opp_lease, opp_relationship, opp_hiring_velocity, opp_funding_accel, opp_rel_depth, opp_coverage, category, mature, revenue_est, office_sf, cash_reserves, cash_updated_at, chain_lease_prob, chain_score

Central entity in the system. Includes:
- **Graph scores**: centrality, leverage, influence, adjacency_index, cluster_id
- **Opportunity scores**: composite score + 8 component sub-scores (funding, hiring, lease, relationship, hiring_velocity, funding_accel, rel_depth, coverage)
- **Categorization**: type (prospect/client), category (high_growth/institutional/mature), chain_lease_prob, chain_score
- **Financial**: revenue_estimate, cash_reserves, office_sf, employee_count

#### contacts (19 rows)

Columns: id, first_name, last_name, email, phone, title, company_id, linkedin_url, status, notes, centrality_score, leverage_score, cluster_id, created_at, updated_at, role_level, previous_companies, alma_mater, priority_score, influence_score, adjacency_index

Key decision-makers and influencers. Includes:
- **Graph scores**: centrality, leverage, influence, adjacency_index, cluster_id
- **Metadata**: title, role_level, previous_companies, alma_mater, linkedin_url
- **Scoring**: priority_score

#### deals (0 rows)

Columns: id, name, deal_type, status, company_id, building_id, contact_id, estimated_value, commission_rate, close_date, notes, created_at, updated_at, square_feet, deal_value, our_role, lead_broker_id, intro_path, started_date, closed_date

Pipeline tracking for lease, sale, and tenant representation opportunities. Currently empty pending deal entry.

#### funding_events (15 rows)

Signal data capturing capital raises by companies. Used in opportunity scoring and trend analysis.

#### hiring_signals (12 rows)

Signal data capturing office expansion indicators. Used in growth velocity and opportunity scoring.

#### leases (7 rows)

Active and expired lease records with tenant/property relationships. Feeds into lease opportunity scoring and renewal tracking.

#### market_notes (4 rows)

Qualitative market observations and trend notes. User-maintained, searchable context for analysis.

#### monitored_clients (0 rows)

Designated high-value accounts receiving enhanced monitoring and reporting.

#### outreach_log (0 rows)

Audit trail of contact outreach. Currently not populated; intended for tracking engagement history.

#### recompute_log (9 rows)

Nightly job execution history. Tracks recompute timing, durations, and score changes.

#### relationships (38 rows)

Graph edges connecting companies and contacts. Typed edges:
- Company → Company (partnership, competitor, adjacency)
- Contact → Company (employment, advisory)
- Company → Building (tenant, owner, management)

Central to pathfinding and influence propagation algorithms.

#### scan_log (0 rows)

Signal scanning execution history. Track calls to search APIs and ingestion rates.

---

## Database Views (7 total)

### v_contact_company
Joins contacts with company info for enriched contact records.

```sql
Columns: contact_id, first_name, last_name, email, company_name, industry, sector
```

### v_active_leases
Active leases with building and tenant names for portfolio view.

```sql
Columns: lease_id, tenant_name, building_name, address, city, state, sf, expiry_date
```

### v_deal_pipeline
Deals with company/contact/building names and estimated commission amounts.

```sql
Columns: deal_id, deal_name, company_name, contact_name, building_name, estimated_value, commission_est, status, close_date
```

### v_outreach_summary
Outreach grouped by contact/company with counts and last activity.

```sql
Columns: contact_id, contact_name, company_id, company_name, outreach_count, last_outreach, next_touch_date
```

### v_relationship_graph
Human-readable relationship edges for visualization and analysis.

```sql
Columns: source_id, source_name, source_type, target_id, target_name, target_type, relationship_type, strength
```

### v_funding_timeline
Funding events ordered by date with company context.

```sql
Columns: event_id, company_name, amount, round_type, announced_date, investor_list
```

### v_hiring_activity
Hiring signals ordered by date with company context.

```sql
Columns: signal_id, company_name, title_openings, experience_level, posted_date, location
```

---

## Scheduled Jobs

| Job | Schedule (ET) | Function | Input | Output |
|-----|---------------|----------|-------|--------|
| **Nightly Recompute** | 2:00 AM | Graph recompute + Scoring 2.1 + predictive chain | All companies, relationships | Updated centrality/leverage/opportunity/chain scores, Discord summary |
| **Morning Signal Scan** | 6:00 AM | Funding + hiring for up to 30 companies | 30 high-priority companies | Funding/hiring signals ingested; alerts fired |
| **Morning Briefing** | 7:00 AM | Discord summary | Recent signals, recompute results | Formatted Discord post |
| **Afternoon Signal Scan** | 5:00 PM | Funding + hiring for up to 30 companies | 30 companies | Funding/hiring signals ingested; alerts fired |
| **Weekly Executive Scan** | Sunday 3:00 AM | Executive movement tracker | All contacts | Executive transitions detected; alerts fired |

---

## OpenClaw Bot Capabilities

### Interaction Modes

**Discord Integration:**
- Responds to direct messages (DMs)
- Responds to @mentions in channels
- Posts scheduled summaries (nightly recompute, weekly digest)
- Sends real-time alerts

### Natural Language Queries

Bot understands conversational questions such as:
- "Who knows someone at Citadel?"
- "What's Blackstone's opportunity score?"
- "Show me companies hiring in Manhattan"
- "Which buildings has Citadel leased?"

### Data Entry

Conversational company/contact/relationship addition:
- "Add Citadel as a prospect in finance"
- "Add John Doe, VP Real Estate at BlackRock"
- "Connect Citadel and Apollo as competitors"

### Alert System

Automated alerts trigger for:
- Funding rounds > $50M
- High-relevance hiring (executive movement, office expansion)
- Executive moves between tracked companies
- Lease expirations < 90 days

### Reporting

- **Nightly Recompute Summary**: Graph stats, top scoring changes, new opportunities
- **Weekly Executive Digest**: Sunday briefing with exec movements section
- **Ad-hoc Reports**: Excel export, company deep dives

---

## System Architecture

### Application Layer

#### app.py
Streamlit entry point for multipage dashboard. Routes to 5 main pages.

#### config.py
Path configuration and database resolution. Handles environment-based path setup (local vs. prod).

### Core Engines

#### graph_engine.py
**Purpose:** Build and compute graph structure and node importance

**Key Functions:**
- `build_graph()`: Constructs directed graph from companies, contacts, relationships
- `compute_all()`: Runs full suite of graph algorithms
- `compute_centrality()`: Betweenness and closeness centrality
- `compute_leverage()`: Betweenness centrality on company subgraph
- `compute_clusters()`: Community detection (NetworkX Louvain)
- `compute_influence()`: PageRank-style influence propagation
- `compute_strategic_adjacency()`: Cross-cluster strategic positioning
- `compute_coverage_overlap()`: Broker coverage competitive analysis
- `compute_entity_categories()`: Classification into high_growth/institutional/mature

**Output:** Scores persisted to companies/contacts tables

#### opportunity_scoring.py
**Purpose:** Scoring 2.1 engine — multi-factor opportunity assessment

**Architecture:**
- Entity categorization (high_growth, institutional, mature)
- Category-aware weight profiles
- 8 component sub-scores:
  - `opp_funding`: Funding event recency and magnitude
  - `opp_hiring`: Hiring signal recency and role level
  - `opp_lease`: Lease expiry proximity and asset SF
  - `opp_relationship`: Relationship depth and contact centrality
  - `opp_hiring_velocity`: Hiring velocity delta (acceleration)
  - `opp_funding_accel`: Funding acceleration (frequency trend)
  - `opp_rel_depth`: Relationship depth from pathfinding
  - `opp_coverage`: Competitive coverage overlap

**Score Calculation:**
```
opportunity_score = weighted_sum(sub_scores, category_weights)
```

**Output:** Composite score + 8 sub-scores persisted to companies table

#### thresholds.py
**Configuration for algorithm thresholds and weights:**

```python
REVENUE_THRESHOLD = 50_000_000  # Min revenue for institutional
SQUARE_FEET_THRESHOLD = 30_000  # Min SF for portfolio relevance
CASH_THRESHOLD = 100_000_000    # Min cash reserves for institutional

WEIGHTS_HIGH_GROWTH = {
    'opp_funding': 0.25,
    'opp_hiring': 0.25,
    'opp_lease': 0.15,
    'opp_relationship': 0.15,
    'opp_hiring_velocity': 0.10,
    'opp_funding_accel': 0.10,
}

WEIGHTS_INSTITUTIONAL = {
    'opp_funding': 0.20,
    'opp_hiring': 0.15,
    'opp_lease': 0.25,
    'opp_relationship': 0.20,
    'opp_coverage': 0.20,
}

CHAIN_PARAMS = {
    'capital_threshold': 50_000_000,
    'expansion_sf_threshold': 50_000,
}
```

All thresholds overridable via environment variables for easy tuning.

#### predictive_chain.py
**Purpose:** Predict company expansion trajectory (Capital → Expansion → Lease)

**Process:**
1. Detect capital event (funding > threshold)
2. Flag expected expansion (hiring growth + funding event)
3. Predict lease need (expansion signal + building portfolio match)
4. Score confidence via sigmoid transform

**Output:** chain_lease_prob, chain_score (likelihood of lease need within 6 months)

#### data_ingestion.py
**Purpose:** CSV loader with entity resolution and deduplication

**Features:**
- Bulk CSV import for companies, contacts, relationships, leases
- Duplicate detection on name/email/domain
- Conflict resolution (keep vs. merge)
- Transaction rollback on validation failure

#### path_finder.py
**Purpose:** Shortest path and intro path generation

**Key Functions:**
- `dijkstra_shortest_path()`: Minimum-weight path between entities
- `generate_intro_path()`: Human-readable introduction sequence
- `find_mutual_connections()`: Common contacts between two companies

#### market_notes.py
**Purpose:** Store, retrieve, and search market context

**Features:**
- Free-form note creation with date/author
- Full-text search
- Tagging by sector, region, topic
- Integration with opportunity scoring context

### Job Scheduling and Processing

#### scheduler.py
**Purpose:** APScheduler daemon managing 5 scheduled jobs

**Jobs:**
1. Nightly Recompute (2:00 AM): `nightly_recompute.py`
2. Morning Signal Scan (6:00 AM): `bulk_hedge_fund_scan.py`
3. Morning Briefing (7:00 AM): `morning_briefing.py`
4. Afternoon Signal Scan (5:00 PM): `bulk_hedge_fund_scan.py`
5. Weekly Executive Scan (Sunday 3:00 AM): `executive_tracker.py`

**Status:** Runs as persistent background daemon

#### nightly_recompute.py
**Purpose:** 3-stage nightly pipeline

**Stage 1: Graph Compute**
- Rebuild graph from companies/contacts/relationships
- Compute all centrality, leverage, cluster, influence scores
- Update companies/contacts tables

**Stage 2: Scoring 2.1**
- Fetch recent funding/hiring/lease signals
- Compute opportunity sub-scores
- Apply category-aware weights
- Update opportunity_score in companies table

**Stage 3: Predictive Chain**
- Run predictive capital → expansion → lease model
- Update chain_lease_prob, chain_score

**Output:**
- Updated scores in DB
- Discord summary post with top changes
- Log entry in recompute_log

#### bulk_hedge_fund_scan.py
**Purpose:** Bulk signal scanning for up to 30 companies per run

**Process:**
1. Select 30 highest-priority companies
2. Dry-run mode: estimate API quota usage
3. Execute searches:
   - Funding event search (SerpApi)
   - Hiring signal search (SerpApi)
4. Classify signals (Claude-based)
5. Insert new signals into funding_events/hiring_signals
6. Fire alerts for significant signals

**Quota Management:**
- Tracks SerpApi monthly usage
- Respects rate limits
- Dry-run estimates prevent overages

#### excel_report.py
**Purpose:** Weekly Excel report generation (6 sheets)

**Sheet 1: Executive Summary**
- Top 10 by opportunity score
- Recent funding events (top 5)
- Active hiring (top 5)

**Sheet 2: Company Pipeline**
- All 57 companies with key metrics
- Sortable by opportunity_score, centrality, leverage

**Sheet 3: Contact Network**
- All 19 contacts with company/title/priority

**Sheet 4: Relationship Graph**
- All 38 relationships with types and strengths

**Sheet 5: Lease Portfolio**
- Active leases with renewal dates

**Sheet 6: Signals Feed**
- Recent funding/hiring/lease signals (30 days)

#### weekly_digest.py
**Purpose:** Sunday morning Discord digest with exec movement section

**Content:**
- Weekly top opportunities (by score)
- Recent funding/hiring summary
- Executive movements (new section)
- Week-ahead events (lease renewals, milestone dates)

### Signal Detection and Classification

#### search_client.py
**Purpose:** SerpApi wrapper for web signal detection

**Methods:**
- `search_funding()`: Find funding announcements via Google News + domain searches
- `search_hiring()`: Detect job postings and hiring activity
- `search_leases()`: Track commercial real estate transactions
- `search_general()`: Ad-hoc web searches

**Features:**
- Query templating (company name, location, keywords)
- Rate limit awareness
- Result parsing and deduplication
- Metadata extraction (date, source, snippet)

#### signal_classifier.py
**Purpose:** Claude-based signal relevance and category classification

**Classification Rules:**
- **Funding**: Identifies round size, type (seed/Series/late-stage), investor list
- **Hiring**: Extracts title, experience level, location, growth signal strength
- **Lease**: Parses tenant/landlord, SF, location, lease type
- **Relevance**: Scores 0-1 relevance to user's investment thesis

**Output:** Structured signal object ready for insertion

#### signal_pipeline.py
**Purpose:** Orchestrates search → classify → insert workflow

**Process:**
1. Search for signals (SerpApi)
2. Parse and extract entities
3. Classify with Claude
4. Check for duplicates in DB
5. Insert new signals with relevance score
6. Fire alerts if relevance > threshold

**Monitoring:** Logs all API calls and ingestion metrics

#### executive_tracker.py
**Purpose:** Detect executive movements between tracked companies

**Process:**
1. Monitor LinkedIn activity of tracked contacts
2. Detect company changes
3. Cross-reference with relationship graph
4. Alert on high-impact moves (executives changing between cluster competitors, etc.)

### Data Integration and Parsing

#### email_parser.py
**Purpose:** Extract structured entities from email content

**Capabilities:**
- Company name detection (regex + lookup)
- Contact identification (email addresses, phone numbers)
- Dollar amount extraction (funding, deal sizes)
- Location parsing (city, state, zip)
- Deal type inference (lease, sale, tenant rep)

**Output:** Structured extraction for manual review and ingestion

#### document_parser.py
**Purpose:** PDF and DOCX entity extraction for market research and deal docs

**Features:**
- PDF text extraction (PyPDF2)
- DOCX parsing (python-docx)
- Table extraction and parsing
- Financial data identification
- Company/contact mentions

**Output:** Annotated document with highlighted entities

#### conversational_entry.py
**Purpose:** Natural language to database operations via Discord/Slack

**Capabilities:**
- Add company: "Add Citadel as a prospect in finance"
- Add contact: "Add John Doe, VP Real Estate at BlackRock"
- Create relationship: "Connect Citadel and Apollo as competitors"
- Update scores: "Rescore all companies"

**Processing:**
1. Parse natural language input
2. Extract entities and intent
3. Validate against DB
4. Confirm with user
5. Execute database transaction

#### morning_briefing.py
**Purpose:** Generate and post daily Discord briefing

**Content:**
- Top 5 opportunities (by overnight score change)
- Recent signals (funding/hiring/leases) from last 24 hours
- Upcoming lease expirations (< 30 days)
- Recompute summary statistics

**Delivery:** 7:00 AM ET via Discord webhook

### Dashboard Pages (Streamlit)

#### 1_Pipeline.py
**Deal Pipeline View:**
- 0 deals currently in pipeline (template ready)
- Sortable by status, estimated value, close date
- Quick entry form for new deals
- Commission calculator

#### 2_Signal_Feed.py
**Funding + Hiring Signal Feed:**
- 15 funding events + 12 hiring signals
- Filter by company, date range, relevance score
- Signal details with source links
- Mark as actioned/archived

#### 3_Company_Deep_Dive.py
**Single Company Analysis:**
- Company profile card (revenue, employees, sector, etc.)
- Opportunity score breakdown (8 sub-scores)
- Relationship subgraph (direct and 2-hop connections)
- Recent signals (funding/hiring/leases)
- Notes and historical interactions
- Market context

#### 4_Recent_News.py
**Recent Signals and News:**
- 30-day signal feed
- Real-time alerts log
- Trigger history (which conditions fired alerts)
- Sorted by relevance and recency

#### 5_Market_Notes.py
**Market Notes Browser:**
- 4 existing notes with full-text search
- Create new notes with tagging
- Tag-based filtering
- Note timeline view

### Database and Configuration

#### init_schema.sql
**Database initialization with:**
- Full schema (13 tables, 7 views)
- Foreign key constraints
- Index definitions for performance
- Seed data (sample companies, contacts, relationships, signals)

#### relationship_engine.db
**SQLite database:**
- Size: 196 KB
- Location: `/sessions/sharp-admiring-curie/relationship_engine/private_data/`
- Backup: Regular snapshots recommended

### Data

#### simulated_signals.csv
**Test dataset (35 rows):**
- 10 high-growth companies
- 10 institutional targets
- 5 mature tenants
- Mixed funding, hiring, and lease signals
- Used for development and demo purposes

### Documentation

#### HANDOFF.md
**System handoff documentation:**
- Architecture overview
- Deployment instructions
- Database migration guide
- Monitoring and maintenance
- Troubleshooting guide

#### system.md
**OpenClaw bot system prompt:**
- Interaction guidelines
- Tone and style guide
- Response templates
- Error handling procedures

---

## Scoring System Details

### Graph Scores (Computed Nightly)

**Centrality Score (0-1)**
- Betweenness centrality on full graph
- Measures: How often on shortest paths between other entities
- High value: Key connectors, bridge builders

**Leverage Score (0-1)**
- Betweenness centrality on company-only subgraph
- Measures: Company's influence over capital flow and deal structure
- High value: Market leaders, deal gatekeepers

**Influence Score (0-1)**
- PageRank-style propagation on directed relationship graph
- Measures: How much a company's decisions impact network
- High value: Strategic thought leaders, upstream decision-makers

**Adjacency Index (0-1)**
- Strategic positioning relative to cluster peers
- Measures: Distance to high-growth companies in cluster
- High value: Close to expansion-stage entities

**Cluster ID**
- Community detection result (Louvain algorithm)
- Groups companies by connection density
- Enables sector and strategic analysis

### Opportunity Scoring 2.1 (Computed Nightly)

**opportunity_score (0-100)**
Weighted composite of 8 sub-scores, weights determined by entity category.

**opp_funding (0-20)**
- Input: Recent funding events (weight by recency and size)
- Logic: Larger rounds and recent closures = higher scores
- Threshold: Signals > $10M weighted 3x

**opp_hiring (0-20)**
- Input: Recent hiring signals (weight by title level and growth)
- Logic: Executive hiring and volume > threshold = higher scores
- Threshold: Director+ hires weighted 2x

**opp_lease (0-20)**
- Input: Lease expiration proximity + portfolio SF
- Logic: Leases expiring 3-12 months = highest score
- Threshold: Leases with SF > $30K weighted 2x

**opp_relationship (0-20)**
- Input: Contact centrality in network, relationship depth
- Logic: High-centrality contacts = higher scores
- Threshold: Contacts with centrality > 0.7 base score

**opp_hiring_velocity (0-10)**
- Input: Hiring signal count acceleration (month-over-month)
- Logic: Positive delta > threshold = score boost
- Threshold: 3+ signals in recent month vs. average

**opp_funding_accel (0-10)**
- Input: Funding event frequency acceleration
- Logic: Increasing pace of rounds = growth signal
- Threshold: 2+ events in recent 3 months

**opp_rel_depth (0-10)**
- Input: Shortest path distance to contact or partner
- Logic: Closer connections = higher scores
- Threshold: Direct = 10, 2-hop = 5, 3+ hop = 0

**opp_coverage (0-10)**
- Input: Competitive coverage overlap (other brokers on company)
- Logic: Low coverage = higher opportunity score
- Threshold: Exclusive coverage = 10, shared = 5, heavy coverage = 0

---

## Alert System

### Alert Thresholds

| Signal Type | Threshold | Alert Level |
|-------------|-----------|------------|
| Funding round | > $50M | High |
| Series A+ round | Any | High |
| Director+ hire | Any | Medium |
| Lease expiry | < 90 days | Medium |
| Exec move (competitor) | Any | High |
| Funding acceleration | 2+ events in 3 months | Medium |
| Hiring velocity spike | 3+ openings in month | Medium |

### Alert Channels

- **Discord**: Real-time alerts in #signals channel
- **Email**: Daily digest (optional)
- **Dashboard**: Alerts dashboard in Streamlit
- **Log**: Audit trail in alert_log table

---

## Environment Variables

```bash
# Database
DB_PATH=/path/to/relationship_engine.db

# API Keys
SERPAPI_KEY=your_key
CLAUDE_API_KEY=your_key
DISCORD_WEBHOOK_URL=your_webhook

# Thresholds
REVENUE_THRESHOLD=50000000
SQUARE_FEET_THRESHOLD=30000
CASH_THRESHOLD=100000000

# Weights (JSON override)
WEIGHTS_OVERRIDE='{"opp_funding": 0.25}'

# Scheduling
SCHEDULER_ENABLED=true
NIGHTLY_RECOMPUTE_TIME=02:00
```

---

## Key Metrics and KPIs

| Metric | Current | Target | Trend |
|--------|---------|--------|-------|
| Total companies | 57 | 100+ | Growing |
| Total contacts | 19 | 50+ | Growing |
| Relationship edges | 38 | 150+ | Growing |
| Funding events (30d) | 15 | 20+ | Growing |
| Hiring signals (30d) | 12 | 15+ | Stable |
| Active leases | 7 | 30+ | Growing |
| Avg opportunity score | ~45 | 50 | Improving |
| Graph recompute time | < 5s | < 10s | Good |
| API quota used (monthly) | 60% | < 80% | Good |

---

## Data Quality and Maintenance

### Data Validation

- Email format validation on contact entry
- Revenue/SF range checks (warnings for outliers)
- Duplicate detection on company name and domain
- Circular relationship detection
- Orphaned relationship cleanup (daily)

### Regular Maintenance Tasks

| Task | Frequency | Owner |
|------|-----------|-------|
| Database backup | Daily | Scheduler |
| Data quality audit | Weekly | Team |
| Opportunity score validation | Nightly | Recompute job |
| API quota monitoring | Daily | Alerts |
| Stale data cleanup (90+ days) | Monthly | Manual |

### Backup and Recovery

- **Automatic**: Daily 2:00 AM (alongside nightly recompute)
- **Location**: `/private_data/backups/relationship_engine_YYYY-MM-DD.db`
- **Retention**: 30 days rolling
- **Recovery**: Replace primary DB and restart scheduler

---

## Performance Notes

### Graph Computation
- 57 companies + 19 contacts + 38 relationships
- Nightly recompute: ~2-4 seconds
- Centrality/leverage: O(V²) = acceptable for current scale
- Cluster detection: O(E log V) = ~500ms

### Opportunity Scoring
- Per-company: ~100ms
- Batch (57 companies): ~5-6 seconds
- Database updates: ~2 seconds
- Total scoring 2.1: ~8 seconds

### Signal Processing
- Search API call: ~2-3 seconds
- Classification (Claude): ~3-5 seconds per signal
- Insertion: ~100ms per signal
- Bulk scan (30 companies): ~10 minutes

### Dashboard Loading
- Pipeline view: ~1 second (0 deals)
- Company deep dive: ~2 seconds
- Signal feed: ~1 second (27 total signals)
- Relationship graph render: ~3 seconds (50 edge limit)

---

## Known Limitations and Future Work

### Current Limitations

1. **Deal Pipeline**: Empty (0 rows) — ready for deployment, requires deal data entry
2. **Outreach Logging**: Not populated — would enhance engagement tracking
3. **Executive Tracking**: Depends on LinkedIn data access (limited by scraping restrictions)
4. **Market Notes**: Manual entry only (4 notes) — could be automated from market research feeds
5. **Relationship Weighting**: Currently binary; opportunity to add strength/quality metrics

### Planned Enhancements

- [ ] Integrate CRM for deal pipeline population
- [ ] Add outreach tracking from email/calendar
- [ ] Expand signal sources (proprietary feeds, API integrations)
- [ ] Real-time (vs. nightly) recompute for time-sensitive signals
- [ ] Machine learning for opportunity score predictions
- [ ] Geographic heat maps for market expansion
- [ ] Scenario modeling (what-if analysis)
- [ ] Integration with accounting systems (for cash/revenue validation)

---

## System Dependencies

### Python Libraries
- `streamlit`: Dashboard framework
- `networkx`: Graph algorithms
- `sqlite3`: Database driver
- `apscheduler`: Job scheduling
- `requests`: HTTP client
- `anthropic`: Claude API
- `pandas`: Data manipulation
- `python-docx`, `PyPDF2`: Document parsing

### External Services
- **SerpApi**: Web search (funding, hiring, lease signals)
- **Claude API**: Signal classification and NLP
- **Discord Webhook**: Alert delivery and briefing

### Infrastructure
- SQLite database (196 KB, local)
- Streamlit server (local or cloud deployment)
- APScheduler daemon (background job runner)
- Discord server/workspace

---

## Contact and Support

**System Owner**: (To be assigned)
**Last Updated**: 2026-02-16
**Version**: 1.0 (Production Ready)

For questions on system architecture, see HANDOFF.md for full deployment guide.
