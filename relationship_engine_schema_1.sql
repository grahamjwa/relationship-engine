-- ============================================================================
-- RELATIONSHIP ENGINE — SQLite Schema v1.0
-- CBRE NYC Commercial Leasing Intelligence System
-- ============================================================================
-- Design principles:
--   1. Contacts are the center of gravity (people make deals)
--   2. Relationships are graph edges (person → person, with type + strength)
--   3. Every outreach attempt is logged with the intro path used
--   4. Opportunity surfacing is built into the schema via views
-- ============================================================================

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- COMPANIES
-- Every organization we track: clients, targets, investors, landlords
CREATE TABLE companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL CHECK (type IN (
                        'tenant', 'landlord', 'investor', 'lender',
                        'developer', 'advisory', 'other'
                    )),
    status          TEXT NOT NULL CHECK (status IN (
                        'active_client', 'former_client', 'high_growth_target',
                        'prospect', 'network_portfolio', 'team_affiliated',
                        'watching'
                    )),
    sector          TEXT,                -- e.g. 'financial_services', 'tech', 'media', 'vc_pe'
    hq_city         TEXT,
    hq_state        TEXT,
    website         TEXT,
    employee_count  INTEGER,
    founded_year    INTEGER,
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- CONTACTS
-- Every person we know or want to know
CREATE TABLE contacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    company_id      INTEGER REFERENCES companies(id),
    title           TEXT,
    role_level      TEXT NOT NULL CHECK (role_level IN (
                        'c_suite',           -- CEO, CFO, COO, CRO, CTO
                        'decision_maker',    -- VP Real Estate, Head of Facilities, SVP Ops
                        'influencer',        -- connected to decision maker, can make intro
                        'team',              -- our team at CBRE
                        'external_partner'   -- outside collaborators (e.g. Danny Green)
                    )),
    email           TEXT,
    phone           TEXT,
    linkedin_url    TEXT,
    alma_mater      TEXT,                -- for alumni network matching
    previous_companies TEXT,             -- comma-separated, for relationship mapping
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- BUILDINGS
-- Properties we rep or that are relevant context
CREATE TABLE buildings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT,                -- e.g. 'One Vanderbilt'
    address         TEXT NOT NULL,
    city            TEXT NOT NULL DEFAULT 'New York',
    state           TEXT NOT NULL DEFAULT 'NY',
    submarket       TEXT,                -- e.g. 'Midtown', 'Hudson Yards', 'FiDi'
    building_class  TEXT CHECK (building_class IN ('A', 'B', 'C', 'Trophy')),
    total_sf        INTEGER,
    owner_company_id INTEGER REFERENCES companies(id),
    managing_agent  TEXT,
    we_rep          BOOLEAN DEFAULT 0,   -- 1 = landlord representation assignment
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- LEASES
-- Tracks where companies are, when leases expire = opportunity triggers
CREATE TABLE leases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    building_id     INTEGER NOT NULL REFERENCES buildings(id),
    floor           TEXT,
    square_feet     INTEGER,
    lease_start     DATE,
    lease_expiry    DATE,                -- KEY FIELD: drives opportunity alerts
    rent_psf        REAL,                -- per square foot, if known
    lease_type      TEXT CHECK (lease_type IN (
                        'direct', 'sublease', 'renewal', 'expansion'
                    )),
    source          TEXT,                -- where we learned this (CoStar, press, etc.)
    confidence      TEXT CHECK (confidence IN ('confirmed', 'estimated', 'rumored')),
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- DEALS
-- Completed or in-progress transactions we're involved in
CREATE TABLE deals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    building_id     INTEGER REFERENCES buildings(id),
    deal_type       TEXT NOT NULL CHECK (deal_type IN (
                        'new_lease', 'renewal', 'expansion', 'sublease',
                        'purchase', 'sale', 'consulting', 'other'
                    )),
    status          TEXT NOT NULL CHECK (status IN (
                        'prospecting', 'pitched', 'touring', 'negotiating',
                        'signed', 'closed', 'lost', 'dead'
                    )),
    square_feet     INTEGER,
    deal_value      REAL,                -- estimated or actual
    our_role        TEXT CHECK (our_role IN (
                        'tenant_rep', 'landlord_rep', 'buyer_rep',
                        'seller_rep', 'consultant'
                    )),
    lead_broker_id  INTEGER REFERENCES contacts(id),  -- who on our team leads
    intro_path      TEXT,                -- how we got the deal (relationship chain)
    started_date    DATE,
    closed_date     DATE,
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- RELATIONSHIPS (graph edges)
-- The core intelligence layer: who knows who, and how
CREATE TABLE relationships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id_a    INTEGER NOT NULL REFERENCES contacts(id),
    contact_id_b    INTEGER NOT NULL REFERENCES contacts(id),
    relationship_type TEXT NOT NULL CHECK (relationship_type IN (
                        'colleague',         -- work at same company
                        'former_colleague',  -- worked together previously
                        'alumni',            -- same school
                        'investor',          -- A invested in B's company
                        'client',            -- A is client of B
                        'friend',            -- personal connection
                        'board',             -- serve on same board
                        'deal_counterpart',  -- did a deal together
                        'introduced_by',     -- A introduced B
                        'other'
                    )),
    strength        INTEGER CHECK (strength BETWEEN 1 AND 5),
                    -- 5 = talk weekly, 4 = quarterly, 3 = annual,
                    -- 2 = met once, 1 = aware of / no direct contact
    direction       TEXT DEFAULT 'bidirectional' CHECK (direction IN (
                        'bidirectional', 'a_to_b', 'b_to_a'
                    )),
    context         TEXT,                -- e.g. 'Harvard 2018', 'BofA deal 2022'
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    CHECK (contact_id_a != contact_id_b)
);

-- OUTREACH LOG
-- Every time we reach out, what happened, and how we got there
CREATE TABLE outreach_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_company_id INTEGER NOT NULL REFERENCES companies(id),
    target_contact_id INTEGER REFERENCES contacts(id),
    outreach_date   DATE NOT NULL,
    outreach_type   TEXT NOT NULL CHECK (outreach_type IN (
                        'email', 'call', 'linkedin', 'text',
                        'in_person', 'event', 'intro_request', 'other'
                    )),
    intro_path_used TEXT,                -- e.g. 'Graham → Ryan → Danny Green → target'
    angle           TEXT,                -- what hook did we use
    outcome         TEXT CHECK (outcome IN (
                        'no_response', 'responded_positive', 'responded_negative',
                        'meeting_booked', 'meeting_held', 'deal_started',
                        'referred', 'declined', 'pending'
                    )),
    follow_up_date  DATE,
    follow_up_done  BOOLEAN DEFAULT 0,
    deal_id         INTEGER REFERENCES deals(id),  -- if this outreach led to a deal
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- FUNDING EVENTS
-- Tracks VC rounds, PE investments = growth signals = opportunity triggers
CREATE TABLE funding_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    event_date      DATE,
    round_type      TEXT,                -- e.g. 'Series B', 'Growth Equity', 'IPO'
    amount          REAL,                -- in USD
    lead_investor   TEXT,
    all_investors   TEXT,                -- comma-separated
    post_valuation  REAL,
    source_url      TEXT,
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- HIRING SIGNALS
-- Job postings that indicate growth = space needs
CREATE TABLE hiring_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    signal_date     DATE NOT NULL,
    signal_type     TEXT NOT NULL CHECK (signal_type IN (
                        'job_posting', 'headcount_growth', 'new_office',
                        'leadership_hire', 'press_announcement'
                    )),
    role_title      TEXT,                -- e.g. 'VP Real Estate', 'Head of Workplace'
    location        TEXT,                -- e.g. 'New York, NY'
    details         TEXT,
    source_url      TEXT,
    relevance       TEXT CHECK (relevance IN ('high', 'medium', 'low')),
                    -- high = RE decision maker hire or explicit NYC expansion
                    -- medium = significant NYC headcount growth
                    -- low = general growth signal
    notes           TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- JUNCTION TABLES
-- ============================================================================

-- A contact can be associated with multiple companies (board seats, advisors)
CREATE TABLE contact_companies (
    contact_id      INTEGER NOT NULL REFERENCES contacts(id),
    company_id      INTEGER NOT NULL REFERENCES companies(id),
    role            TEXT,                -- their role at this company
    is_primary      BOOLEAN DEFAULT 0,   -- 1 = this is their main gig
    start_date      DATE,
    end_date        DATE,                -- NULL = current
    PRIMARY KEY (contact_id, company_id)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Companies
CREATE INDEX idx_companies_status ON companies(status);
CREATE INDEX idx_companies_sector ON companies(sector);
CREATE INDEX idx_companies_type ON companies(type);

-- Contacts
CREATE INDEX idx_contacts_company ON contacts(company_id);
CREATE INDEX idx_contacts_role_level ON contacts(role_level);
CREATE INDEX idx_contacts_name ON contacts(last_name, first_name);
CREATE INDEX idx_contacts_alma_mater ON contacts(alma_mater);

-- Leases — expiry is the money query
CREATE INDEX idx_leases_expiry ON leases(lease_expiry);
CREATE INDEX idx_leases_company ON leases(company_id);
CREATE INDEX idx_leases_building ON leases(building_id);

-- Deals
CREATE INDEX idx_deals_status ON deals(status);
CREATE INDEX idx_deals_company ON deals(company_id);

-- Relationships — graph traversal
CREATE INDEX idx_rel_contact_a ON relationships(contact_id_a);
CREATE INDEX idx_rel_contact_b ON relationships(contact_id_b);
CREATE INDEX idx_rel_type ON relationships(relationship_type);

-- Outreach
CREATE INDEX idx_outreach_company ON outreach_log(target_company_id);
CREATE INDEX idx_outreach_date ON outreach_log(outreach_date);
CREATE INDEX idx_outreach_followup ON outreach_log(follow_up_date)
    WHERE follow_up_done = 0;

-- Funding
CREATE INDEX idx_funding_company ON funding_events(company_id);
CREATE INDEX idx_funding_date ON funding_events(event_date);

-- Hiring
CREATE INDEX idx_hiring_company ON hiring_signals(company_id);
CREATE INDEX idx_hiring_date ON hiring_signals(signal_date);
CREATE INDEX idx_hiring_relevance ON hiring_signals(relevance);

-- ============================================================================
-- VIEWS — Opportunity Surfacing
-- ============================================================================

-- UPCOMING LEASE EXPIRATIONS (next 24 months)
-- "Who's coming up for renewal and might need to move?"
CREATE VIEW v_upcoming_expirations AS
SELECT
    c.name AS company_name,
    c.status AS company_status,
    b.name AS building_name,
    b.address,
    l.square_feet,
    l.lease_expiry,
    CAST((julianday(l.lease_expiry) - julianday('now')) / 30.44 AS INTEGER) AS months_until_expiry,
    l.confidence
FROM leases l
JOIN companies c ON l.company_id = c.id
JOIN buildings b ON l.building_id = b.id
WHERE l.lease_expiry BETWEEN date('now') AND date('now', '+24 months')
ORDER BY l.lease_expiry ASC;

-- RECENT FUNDING = GROWTH SIGNAL
-- "Who just raised money and might need more space?"
CREATE VIEW v_recent_funding AS
SELECT
    c.name AS company_name,
    c.status AS company_status,
    f.round_type,
    f.amount,
    f.lead_investor,
    f.event_date,
    CAST((julianday('now') - julianday(f.event_date)) AS INTEGER) AS days_since_funding
FROM funding_events f
JOIN companies c ON f.company_id = c.id
WHERE f.event_date >= date('now', '-6 months')
ORDER BY f.event_date DESC;

-- INTRO PATH FINDER
-- "How do I get to decision makers at target companies?"
-- Returns: our team member → who they know → who that person knows at the target
CREATE VIEW v_intro_paths AS
SELECT
    team.first_name || ' ' || team.last_name AS team_member,
    connector.first_name || ' ' || connector.last_name AS connector,
    r1.relationship_type AS team_to_connector,
    r1.strength AS connection_strength,
    target.first_name || ' ' || target.last_name AS target_person,
    target.title AS target_title,
    target.role_level AS target_role,
    r2.relationship_type AS connector_to_target,
    tc.name AS target_company
FROM contacts team
-- Team member knows connector
JOIN relationships r1 ON (
    (r1.contact_id_a = team.id OR r1.contact_id_b = team.id)
)
JOIN contacts connector ON (
    connector.id = CASE
        WHEN r1.contact_id_a = team.id THEN r1.contact_id_b
        ELSE r1.contact_id_a
    END
)
-- Connector knows target
JOIN relationships r2 ON (
    (r2.contact_id_a = connector.id OR r2.contact_id_b = connector.id)
)
JOIN contacts target ON (
    target.id = CASE
        WHEN r2.contact_id_a = connector.id THEN r2.contact_id_b
        ELSE r2.contact_id_a
    END
    AND target.id != team.id  -- don't loop back to team
)
JOIN companies tc ON target.company_id = tc.id
WHERE team.role_level = 'team'
  AND target.role_level IN ('c_suite', 'decision_maker')
ORDER BY r1.strength DESC, r2.strength DESC;

-- OUTREACH EFFECTIVENESS
-- "What outreach methods and intro paths actually work?"
CREATE VIEW v_outreach_effectiveness AS
SELECT
    outreach_type,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN outcome IN ('responded_positive', 'meeting_booked',
        'meeting_held', 'deal_started') THEN 1 ELSE 0 END) AS positive_outcomes,
    ROUND(100.0 * SUM(CASE WHEN outcome IN ('responded_positive', 'meeting_booked',
        'meeting_held', 'deal_started') THEN 1 ELSE 0 END) / COUNT(*), 1)
        AS success_rate_pct
FROM outreach_log
WHERE outcome IS NOT NULL AND outcome != 'pending'
GROUP BY outreach_type
ORDER BY success_rate_pct DESC;

-- UNTOUCHED TARGETS
-- "Which high-value companies have we never reached out to?"
CREATE VIEW v_untouched_targets AS
SELECT
    c.id,
    c.name,
    c.status,
    c.sector,
    c.employee_count
FROM companies c
LEFT JOIN outreach_log o ON c.id = o.target_company_id
WHERE c.status IN ('high_growth_target', 'prospect', 'watching')
  AND o.id IS NULL
ORDER BY c.status, c.name;

-- OVERDUE FOLLOW-UPS
-- "Who did I promise to follow up with and haven't?"
CREATE VIEW v_overdue_followups AS
SELECT
    o.follow_up_date,
    c.name AS company_name,
    ct.first_name || ' ' || ct.last_name AS contact_name,
    o.outreach_type,
    o.angle,
    o.outcome AS last_outcome,
    CAST((julianday('now') - julianday(o.follow_up_date)) AS INTEGER) AS days_overdue
FROM outreach_log o
JOIN companies c ON o.target_company_id = c.id
LEFT JOIN contacts ct ON o.target_contact_id = ct.id
WHERE o.follow_up_done = 0
  AND o.follow_up_date <= date('now')
ORDER BY o.follow_up_date ASC;

-- HIGH-VALUE HIRING SIGNALS
-- "Who's hiring RE decision makers or expanding in NYC?"
CREATE VIEW v_high_value_hiring AS
SELECT
    c.name AS company_name,
    c.status AS company_status,
    h.signal_type,
    h.role_title,
    h.location,
    h.signal_date,
    h.details,
    h.source_url
FROM hiring_signals h
JOIN companies c ON h.company_id = c.id
WHERE h.relevance = 'high'
  AND h.signal_date >= date('now', '-3 months')
ORDER BY h.signal_date DESC;
