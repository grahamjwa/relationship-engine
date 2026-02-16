# Relationship Engine — Architecture & Scoring Philosophy

## 1. Canonical Entity Model

### Core Entities

| Entity | Purpose | Unique ID Logic |
|--------|---------|-----------------|
| **Company** | Tenant, landlord, investor, or other market participant | `name` (unique, case-normalized) |
| **Contact** | Individual person with relationships | `first_name + last_name + company_id` |
| **Building** | Physical property | `address + city + state` |
| **Lease** | Occupancy record | `company_id + building_id + floor` |
| **Deal** | Active/historical transaction | `company_id + building_id + deal_type + started_date` |
| **Relationship** | Edge between two contacts | `contact_id_a + contact_id_b + relationship_type` |

### Signal Entities (Time-Decaying)

| Entity | Purpose | Decay Rate |
|--------|---------|------------|
| **Funding Event** | Capital raise signal | 6-month half-life |
| **Hiring Signal** | Growth/expansion indicator | 3-month half-life |
| **Outreach Log** | Engagement history | 1-month half-life for scoring |

---

## 2. Field Classifications

### Required vs Optional

**Companies — Required:**
- `name`, `type`, `status`

**Companies — Optional but High-Value:**
- `sector` (enables sector-based targeting)
- `hq_city`, `hq_state` (enables geo filtering)
- `employee_count` (growth signal baseline)

**Contacts — Required:**
- `first_name`, `last_name`, `role_level`

**Contacts — Optional but High-Value:**
- `company_id` (enables company rollup)
- `title` (enables decision-maker identification)
- `alma_mater` (enables alumni network matching)

**Relationships — Required:**
- `contact_id_a`, `contact_id_b`, `relationship_type`, `strength`

---

## 3. Update Rules

### Company Status Transitions
```
high_growth_target → prospect → active_client → former_client
                  ↘ watching (if disqualified)
```

### Contact Role Level Mapping
| Title Contains | Role Level |
|---------------|------------|
| CEO, CFO, COO, CRO, CTO, President, Founder | c_suite |
| VP, Head of, Director, SVP | decision_maker |
| Manager, Associate, Analyst | influencer |
| (CBRE team) | team |
| (External collaborators) | external_partner |

### Relationship Strength Decay
- **Strength 5** (talk weekly): No decay for 30 days, then -1 per 90 days of no interaction
- **Strength 4** (quarterly): No decay for 90 days, then -1 per 180 days
- **Strength 3** (annual): No decay for 365 days, then -1 per 365 days
- **Strength 2** (met once): Decays to 1 after 2 years
- **Strength 1** (aware of): No decay (floor)

---

## 4. Scoring Philosophy

### What Makes a Company Rise?

**Opportunity Score** = weighted sum of:

| Factor | Weight | Rationale |
|--------|--------|-----------|
| **Funding Recency** | 25% | Recent capital = expansion likely |
| **Hiring Velocity** | 20% | Growing headcount = space needs |
| **Lease Expiry Proximity** | 20% | Upcoming expiry = decision timeline |
| **Relationship Proximity** | 20% | Closer to team = higher conversion |
| **Market Momentum** | 15% | Sector/company growth signals |

### What Makes a Contact Rise?

**Contact Priority Score** = weighted sum of:

| Factor | Weight | Rationale |
|--------|--------|-----------|
| **Role Level** | 30% | c_suite > decision_maker > influencer |
| **Company Opportunity Score** | 25% | High-value company = high-value contact |
| **Relationship Strength to Team** | 25% | Stronger path = higher conversion |
| **Engagement Recency** | 20% | Recent interaction = warm relationship |

---

## 5. Decay Functions

### Time Decay Formula
```
decay_factor = e^(-λ * days_since_event)
```

| Signal Type | λ (decay rate) | Half-life |
|-------------|----------------|-----------|
| Funding Event | 0.00385 | 180 days |
| Hiring Signal | 0.0077 | 90 days |
| Outreach | 0.0231 | 30 days |
| Relationship (no interaction) | 0.00095 | 730 days |

### Decay Application
- Funding: `funding_score = amount * decay_factor`
- Hiring: `hiring_score = relevance_weight * decay_factor`
- Relationship: `edge_weight = strength * decay_factor`

---

## 6. Opportunity Categories

### High Priority
Criteria (ANY of):
- Funding event in last 90 days AND company is high_growth_target
- Lease expiry in next 12 months AND company is prospect or better
- High-relevance hiring signal in last 30 days
- Direct relationship (strength ≥ 4) to decision_maker at target

### Undercovered Opportunity
Criteria:
- Company status = high_growth_target OR prospect
- No outreach in last 90 days
- Has at least one intro path through team network

### Relationship at Risk
Criteria:
- Company status = active_client
- No outreach in last 60 days
- Last outreach outcome = no_response OR declined

### Capital Adjacency Expansion
Criteria:
- Company received funding in last 180 days
- Lead investor is in our network (relationship exists)
- We have no direct relationship to the funded company

### Competitive Threat
Criteria:
- Company is active_client
- Hiring signal for "Head of Real Estate" or similar (leadership_hire)
- Could indicate they're bringing RE in-house

---

## 7. Proactive Insight Categories

### Daily Alerts (Push to Discord)
1. **New Funding** — Any funding event for watched companies
2. **High-Value Hire** — RE/workplace leadership hires
3. **Expiring Soon** — Leases expiring in next 6 months
4. **Overdue Follow-up** — Outreach with follow_up_date passed

### Weekly Digest
1. **Top 10 Opportunities** — Ranked by opportunity score
2. **Untouched Targets** — High-value companies with no recent outreach
3. **Network Expansion** — New relationships added to graph
4. **Performance Attribution** — Deals closed, intro paths used

### On-Demand Analysis
1. **Path to X** — Shortest relationship path to any contact/company
2. **Cluster Analysis** — Who is connected to whom
3. **Sector Heatmap** — Which sectors have most activity

---

## 8. Deduplication Rules

### Companies
- Normalize: lowercase, strip "Inc.", "LLC", "Corp.", leading "The"
- Match: Levenshtein distance < 3 = likely duplicate
- Action: Flag for manual review, don't auto-merge

### Contacts
- Normalize: lowercase first/last name
- Match: Same name + same company = duplicate
- Action: Merge, keep most recent data

### Funding Events
- Match: Same company + same amount + event_date within 7 days
- Action: Keep first, discard subsequent

### Hiring Signals
- Match: Same company + same signal_type + same source_url
- Action: Keep first, discard subsequent

---

## 9. Schema Lock Checklist

Before mass data import:

- [ ] All CHECK constraints validated
- [ ] All indexes created for query patterns
- [ ] All views tested with sample data
- [ ] Decay functions implemented and tested
- [ ] Opportunity scoring implemented and tested
- [ ] Deduplication rules implemented
- [ ] Audit log table created for changes

---

## 10. Future Extensions (Do Not Build Yet)

- **Capital Stack Table** — Track debt/equity structure
- **Market Comps Table** — Comparable transactions
- **News Mentions Table** — Press coverage tracking
- **Meeting Notes Table** — Structured meeting intelligence
- **Competitor Tracking** — Other brokers on deals

These wait until core system is validated.
