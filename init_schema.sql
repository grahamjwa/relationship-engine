-- Relationship Engine Database Schema
-- 9 Tables + 7 Views

-- ============================================================
-- TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    industry TEXT,
    sector TEXT,
    hq_city TEXT,
    hq_state TEXT,
    website TEXT,
    employee_count INTEGER,
    revenue_estimate REAL,
    status TEXT DEFAULT 'active',  -- active, inactive, prospect, client
    notes TEXT,
    centrality_score REAL DEFAULT 0.0,
    leverage_score REAL DEFAULT 0.0,
    cluster_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    title TEXT,
    company_id INTEGER REFERENCES companies(id),
    linkedin_url TEXT,
    status TEXT DEFAULT 'active',  -- active, inactive, prospect, client
    notes TEXT,
    centrality_score REAL DEFAULT 0.0,
    leverage_score REAL DEFAULT 0.0,
    cluster_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS buildings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    address TEXT NOT NULL,
    city TEXT,
    state TEXT,
    zip TEXT,
    property_type TEXT,  -- office, retail, industrial, multifamily, mixed-use
    square_footage REAL,
    floors INTEGER,
    year_built INTEGER,
    owner_company_id INTEGER REFERENCES companies(id),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER REFERENCES buildings(id),
    tenant_company_id INTEGER REFERENCES companies(id),
    floor TEXT,
    square_footage REAL,
    lease_start DATE,
    lease_end DATE,
    annual_rent REAL,
    rent_psf REAL,
    lease_type TEXT,  -- gross, net, triple-net, modified-gross
    status TEXT DEFAULT 'active',  -- active, expired, pending
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    deal_type TEXT,  -- acquisition, disposition, lease, development, financing
    status TEXT DEFAULT 'prospecting',  -- prospecting, active, under_contract, closed, dead
    company_id INTEGER REFERENCES companies(id),
    building_id INTEGER REFERENCES buildings(id),
    contact_id INTEGER REFERENCES contacts(id),
    estimated_value REAL,
    commission_rate REAL,
    close_date DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,  -- contact, company
    source_id INTEGER NOT NULL,
    target_type TEXT NOT NULL,  -- contact, company
    target_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,  -- works_at, knows, referred_by, tenant_of, advisor_to, investor_in, partner_with, competes_with
    strength INTEGER DEFAULT 5 CHECK(strength BETWEEN 1 AND 10),
    confidence REAL DEFAULT 1.0 CHECK(confidence BETWEEN 0.0 AND 1.0),
    base_weight REAL DEFAULT 1.0,
    last_interaction DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outreach_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER REFERENCES contacts(id),
    company_id INTEGER REFERENCES companies(id),
    channel TEXT,  -- email, phone, linkedin, in_person, text
    direction TEXT,  -- inbound, outbound
    subject TEXT,
    body TEXT,
    outcome TEXT,  -- connected, voicemail, no_answer, bounced, meeting_set, replied
    outreach_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    follow_up_date DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS funding_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER REFERENCES companies(id),
    round_type TEXT,  -- seed, series_a, series_b, series_c, ipo, debt, secondary
    amount REAL,
    lead_investor TEXT,
    event_date DATE,
    source_url TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hiring_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER REFERENCES companies(id),
    signal_type TEXT,  -- job_posting, headcount_growth, exec_hire, layoff, office_expansion
    title TEXT,
    description TEXT,
    source_url TEXT,
    signal_date DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- VIEWS
-- ============================================================

CREATE VIEW IF NOT EXISTS v_contact_company AS
SELECT
    c.id AS contact_id,
    c.first_name || ' ' || c.last_name AS contact_name,
    c.title,
    c.email,
    c.phone,
    c.status AS contact_status,
    c.centrality_score AS contact_centrality,
    c.leverage_score AS contact_leverage,
    co.id AS company_id,
    co.name AS company_name,
    co.industry,
    co.status AS company_status
FROM contacts c
LEFT JOIN companies co ON c.company_id = co.id;

CREATE VIEW IF NOT EXISTS v_active_leases AS
SELECT
    l.id AS lease_id,
    b.name AS building_name,
    b.address,
    b.city,
    b.state,
    co.name AS tenant_name,
    l.square_footage,
    l.annual_rent,
    l.rent_psf,
    l.lease_start,
    l.lease_end,
    l.lease_type
FROM leases l
JOIN buildings b ON l.building_id = b.id
JOIN companies co ON l.tenant_company_id = co.id
WHERE l.status = 'active';

CREATE VIEW IF NOT EXISTS v_deal_pipeline AS
SELECT
    d.id AS deal_id,
    d.name AS deal_name,
    d.deal_type,
    d.status,
    d.estimated_value,
    d.commission_rate,
    d.estimated_value * d.commission_rate AS estimated_commission,
    d.close_date,
    co.name AS company_name,
    ct.first_name || ' ' || ct.last_name AS contact_name,
    b.address AS building_address
FROM deals d
LEFT JOIN companies co ON d.company_id = co.id
LEFT JOIN contacts ct ON d.contact_id = ct.id
LEFT JOIN buildings b ON d.building_id = b.id;

CREATE VIEW IF NOT EXISTS v_outreach_summary AS
SELECT
    ct.first_name || ' ' || ct.last_name AS contact_name,
    co.name AS company_name,
    COUNT(*) AS total_outreach,
    SUM(CASE WHEN o.direction = 'outbound' THEN 1 ELSE 0 END) AS outbound_count,
    SUM(CASE WHEN o.direction = 'inbound' THEN 1 ELSE 0 END) AS inbound_count,
    MAX(o.outreach_date) AS last_outreach,
    SUM(CASE WHEN o.outcome = 'meeting_set' THEN 1 ELSE 0 END) AS meetings_set
FROM outreach_log o
LEFT JOIN contacts ct ON o.contact_id = ct.id
LEFT JOIN companies co ON o.company_id = co.id
GROUP BY o.contact_id, o.company_id;

CREATE VIEW IF NOT EXISTS v_relationship_graph AS
SELECT
    r.id AS relationship_id,
    r.source_type,
    r.source_id,
    CASE
        WHEN r.source_type = 'contact' THEN (SELECT first_name || ' ' || last_name FROM contacts WHERE id = r.source_id)
        WHEN r.source_type = 'company' THEN (SELECT name FROM companies WHERE id = r.source_id)
    END AS source_name,
    r.target_type,
    r.target_id,
    CASE
        WHEN r.target_type = 'contact' THEN (SELECT first_name || ' ' || last_name FROM contacts WHERE id = r.target_id)
        WHEN r.target_type = 'company' THEN (SELECT name FROM companies WHERE id = r.target_id)
    END AS target_name,
    r.relationship_type,
    r.strength,
    r.confidence,
    r.base_weight,
    r.last_interaction
FROM relationships r;

CREATE VIEW IF NOT EXISTS v_funding_timeline AS
SELECT
    co.name AS company_name,
    co.industry,
    f.round_type,
    f.amount,
    f.lead_investor,
    f.event_date,
    f.source_url
FROM funding_events f
JOIN companies co ON f.company_id = co.id
ORDER BY f.event_date DESC;

CREATE VIEW IF NOT EXISTS v_hiring_activity AS
SELECT
    co.name AS company_name,
    co.industry,
    h.signal_type,
    h.title,
    h.description,
    h.signal_date,
    h.source_url
FROM hiring_signals h
JOIN companies co ON h.company_id = co.id
ORDER BY h.signal_date DESC;

-- ============================================================
-- RECOMPUTE LOG TABLE (for nightly_recompute.py)
-- ============================================================

CREATE TABLE IF NOT EXISTS recompute_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nodes_computed INTEGER,
    edges_computed INTEGER,
    clusters_found INTEGER,
    top_centrality_node TEXT,
    top_leverage_node TEXT,
    duration_seconds REAL,
    status TEXT DEFAULT 'success',  -- success, failed
    error_message TEXT,
    notes TEXT
);

-- ============================================================
-- SEED DATA (sample relationships for testing)
-- ============================================================

INSERT OR IGNORE INTO companies (name, industry, sector, hq_city, hq_state, status) VALUES
    ('Citadel LLC', 'Finance', 'Hedge Fund', 'Chicago', 'IL', 'prospect'),
    ('Brookfield Asset Management', 'Real Estate', 'Investment Management', 'Toronto', 'ON', 'prospect'),
    ('CBRE Group', 'Real Estate', 'Brokerage', 'Dallas', 'TX', 'active'),
    ('Blackstone', 'Finance', 'Private Equity', 'New York', 'NY', 'prospect'),
    ('Prologis', 'Real Estate', 'Industrial REIT', 'San Francisco', 'CA', 'active'),
    ('JLL', 'Real Estate', 'Brokerage', 'Chicago', 'IL', 'active'),
    ('Hines', 'Real Estate', 'Development', 'Houston', 'TX', 'prospect'),
    ('Starwood Capital', 'Real Estate', 'Investment', 'Miami', 'FL', 'prospect'),
    ('WeWork', 'Real Estate', 'Flex Space', 'New York', 'NY', 'active'),
    ('Cushman & Wakefield', 'Real Estate', 'Brokerage', 'Chicago', 'IL', 'active');

INSERT OR IGNORE INTO contacts (first_name, last_name, title, company_id, status) VALUES
    ('Ken', 'Griffin', 'CEO', 1, 'active'),
    ('Bruce', 'Flatt', 'CEO', 2, 'active'),
    ('Bob', 'Sulentic', 'CEO', 3, 'active'),
    ('Steve', 'Schwarzman', 'Chairman', 4, 'active'),
    ('Hamid', 'Moghadam', 'CEO', 5, 'active'),
    ('Christian', 'Ulbrich', 'CEO', 6, 'active'),
    ('Jeff', 'Hines', 'Co-Chairman', 7, 'active'),
    ('Barry', 'Sternlicht', 'Chairman', 8, 'active'),
    ('John', 'Santora', 'CEO', 9, 'active'),
    ('Michelle', 'MacKay', 'CEO', 10, 'active'),
    ('Sarah', 'Thompson', 'VP Acquisitions', 4, 'active'),
    ('David', 'Chen', 'Managing Director', 2, 'active'),
    ('Lisa', 'Park', 'Senior Broker', 3, 'active'),
    ('Michael', 'Rivera', 'Head of Leasing', 7, 'active'),
    ('Amanda', 'Foster', 'Investment Analyst', 8, 'active');

INSERT OR IGNORE INTO relationships (source_type, source_id, target_type, target_id, relationship_type, strength, confidence, base_weight, last_interaction) VALUES
    ('contact', 1, 'company', 1, 'works_at', 10, 1.0, 1.0, '2026-01-15'),
    ('contact', 2, 'company', 2, 'works_at', 10, 1.0, 1.0, '2026-01-10'),
    ('contact', 3, 'company', 3, 'works_at', 10, 1.0, 1.0, '2026-02-01'),
    ('contact', 4, 'company', 4, 'works_at', 10, 1.0, 1.0, '2025-12-20'),
    ('contact', 5, 'company', 5, 'works_at', 10, 1.0, 1.0, '2026-01-25'),
    ('contact', 1, 'contact', 4, 'knows', 7, 0.9, 1.5, '2025-11-15'),
    ('contact', 4, 'contact', 2, 'knows', 6, 0.8, 1.2, '2025-10-01'),
    ('contact', 2, 'contact', 5, 'knows', 5, 0.7, 1.0, '2025-09-15'),
    ('contact', 3, 'contact', 6, 'knows', 8, 0.95, 1.3, '2026-01-20'),
    ('contact', 6, 'contact', 10, 'knows', 7, 0.85, 1.1, '2026-01-05'),
    ('company', 4, 'company', 2, 'partner_with', 6, 0.8, 2.0, '2025-12-01'),
    ('company', 1, 'company', 4, 'investor_in', 5, 0.7, 1.8, '2025-08-15'),
    ('contact', 7, 'company', 7, 'works_at', 10, 1.0, 1.0, '2026-02-10'),
    ('contact', 8, 'company', 8, 'works_at', 10, 1.0, 1.0, '2026-01-30'),
    ('contact', 11, 'company', 4, 'works_at', 9, 1.0, 1.0, '2026-02-05'),
    ('contact', 12, 'company', 2, 'works_at', 9, 1.0, 1.0, '2026-01-28'),
    ('contact', 13, 'company', 3, 'works_at', 9, 1.0, 1.0, '2026-02-12'),
    ('contact', 14, 'company', 7, 'works_at', 9, 1.0, 1.0, '2026-01-18'),
    ('contact', 15, 'company', 8, 'works_at', 9, 1.0, 1.0, '2026-02-08'),
    ('contact', 11, 'contact', 12, 'knows', 6, 0.75, 1.2, '2025-12-15'),
    ('contact', 13, 'contact', 14, 'referred_by', 7, 0.9, 1.5, '2026-01-10'),
    ('contact', 8, 'contact', 4, 'knows', 8, 0.85, 1.4, '2025-11-20'),
    ('contact', 7, 'contact', 5, 'knows', 5, 0.6, 1.0, '2025-10-10'),
    ('company', 3, 'company', 6, 'competes_with', 4, 0.9, 0.8, '2026-01-01'),
    ('company', 5, 'company', 7, 'partner_with', 6, 0.75, 1.5, '2025-11-01'),
    ('contact', 1, 'contact', 8, 'knows', 6, 0.7, 1.3, '2025-12-10'),
    ('contact', 5, 'contact', 7, 'knows', 4, 0.6, 1.0, '2025-09-01');

INSERT OR IGNORE INTO buildings (name, address, city, state, property_type, square_footage, owner_company_id) VALUES
    ('131 S Dearborn', '131 S Dearborn St', 'Chicago', 'IL', 'office', 1500000, 2),
    ('One Manhattan West', '401 9th Ave', 'New York', 'NY', 'office', 2100000, 4);

INSERT OR IGNORE INTO leases (building_id, tenant_company_id, floor, square_footage, lease_start, lease_end, annual_rent, rent_psf, lease_type, status) VALUES
    (1, 1, '30-35', 120000, '2023-01-01', '2033-12-31', 7200000, 60.0, 'net', 'active'),
    (2, 9, '10-12', 85000, '2022-06-01', '2032-05-31', 6800000, 80.0, 'gross', 'active');

INSERT OR IGNORE INTO funding_events (company_id, round_type, amount, lead_investor, event_date) VALUES
    (9, 'debt', 500000000, 'SoftBank', '2025-06-15'),
    (5, 'secondary', 2000000000, 'Public Markets', '2025-09-01');

INSERT OR IGNORE INTO hiring_signals (company_id, signal_type, title, description, signal_date) VALUES
    (4, 'exec_hire', 'New Head of RE Acquisitions', 'Blackstone hires new head of real estate acquisitions from Goldman', '2025-12-01'),
    (1, 'office_expansion', 'Citadel Miami Expansion', 'Citadel expanding Miami HQ by 200k sqft', '2026-01-10');
