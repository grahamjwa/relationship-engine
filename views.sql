-- ============================================================
-- Relationship Engine: SQL Views
-- ============================================================
-- This file contains all SQL views for easy recreation.
-- 7 existing views + 3 new views for analysis and monitoring.
-- Run: sqlite3 relationship_engine.db < views.sql
-- ============================================================

-- ============================================================
-- EXISTING VIEWS (7 views from schema)
-- ============================================================

-- View 1: Contact-Company relationships
-- Joins contacts with their associated companies
-- Shows all contact details alongside company information
DROP VIEW IF EXISTS v_contact_company;
CREATE VIEW v_contact_company AS
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

-- View 2: Active Leases
-- Lists all active lease agreements with building and tenant details
-- Useful for understanding real estate holdings and tenant relationships
DROP VIEW IF EXISTS v_active_leases;
CREATE VIEW v_active_leases AS
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

-- View 3: Deal Pipeline
-- Shows all deals with related company, contact, and building information
-- Displays estimated commission for each deal
DROP VIEW IF EXISTS v_deal_pipeline;
CREATE VIEW v_deal_pipeline AS
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

-- View 4: Outreach Summary
-- Aggregates all outreach activity by contact and company
-- Tracks total interactions, direction (inbound/outbound), and outcomes
DROP VIEW IF EXISTS v_outreach_summary;
CREATE VIEW v_outreach_summary AS
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

-- View 5: Relationship Graph
-- Displays all relationships with source and target entities
-- Shows relationship strength, confidence, and base weight
DROP VIEW IF EXISTS v_relationship_graph;
CREATE VIEW v_relationship_graph AS
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

-- View 6: Funding Timeline
-- Lists all funding events chronologically with company and investor info
-- Tracks round type, amount, and lead investor
DROP VIEW IF EXISTS v_funding_timeline;
CREATE VIEW v_funding_timeline AS
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

-- View 7: Hiring Activity
-- Lists hiring signals and executive changes
-- Useful for identifying growth signals and leadership changes
DROP VIEW IF EXISTS v_hiring_activity;
CREATE VIEW v_hiring_activity AS
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
-- NEW VIEWS (3 new analytical views)
-- ============================================================

-- View 8: Target Ranking
-- Ranks companies by opportunity_score with detailed scoring breakdown
-- Combines company status, centrality, leverage, and relationship data
-- Calculates weighted scores for strategic targeting and prioritization
DROP VIEW IF EXISTS v_target_ranking;
CREATE VIEW v_target_ranking AS
SELECT
    co.id AS company_id,
    co.name AS company_name,
    co.status AS category,
    co.industry,
    co.sector,
    co.centrality_score,
    co.leverage_score,
    -- Calculate opportunity score from multiple factors
    (
        (co.centrality_score * 0.3) +
        (co.leverage_score * 0.2) +
        (COALESCE((SELECT COUNT(*) FROM relationships WHERE target_id = co.id AND target_type = 'company'), 0) * 0.1) +
        (COALESCE((SELECT COUNT(*) FROM contacts WHERE company_id = co.id), 0) * 0.15) +
        (CASE WHEN co.status = 'client' THEN 10 WHEN co.status = 'active' THEN 5 WHEN co.status = 'prospect' THEN 2 ELSE 0 END * 0.25)
    ) AS opportunity_score,
    -- Chain lease probability (estimate based on property involvement)
    CASE
        WHEN EXISTS (SELECT 1 FROM leases l WHERE l.tenant_company_id = co.id AND l.status = 'active') THEN 0.9
        WHEN EXISTS (SELECT 1 FROM buildings b WHERE b.owner_company_id = co.id) THEN 0.7
        WHEN co.status IN ('active', 'client') THEN 0.4
        ELSE 0.1
    END AS chain_lease_prob,
    COALESCE((SELECT MAX(outreach_date) FROM outreach_log WHERE company_id = co.id), '1900-01-01') AS last_contact_date,
    COALESCE((SELECT COUNT(*) FROM deals WHERE company_id = co.id AND status IN ('active', 'under_contract')), 0) AS active_deals
FROM companies co
ORDER BY opportunity_score DESC;

-- View 9: Funded Need Outreach
-- Identifies companies that received recent funding but lack recent outreach
-- Flags high-priority targets: funded in last 30 days + no outreach in last 60 days
-- Useful for proactive business development and relationship building
DROP VIEW IF EXISTS v_funded_need_outreach;
CREATE VIEW v_funded_need_outreach AS
SELECT
    co.id AS company_id,
    co.name AS company_name,
    co.industry,
    co.status,
    f.round_type AS latest_funding_round,
    f.amount AS latest_funding_amount,
    f.event_date AS funding_date,
    f.lead_investor,
    COALESCE(MAX(o.outreach_date), '1900-01-01') AS last_outreach_date,
    CAST((julianday('now') - julianday(f.event_date)) AS INTEGER) AS days_since_funding,
    CAST((julianday('now') - COALESCE(julianday(MAX(o.outreach_date)), julianday('1900-01-01'))) AS INTEGER) AS days_since_outreach,
    CASE
        WHEN MAX(o.outreach_date) IS NULL THEN 'No prior outreach'
        ELSE 'Existing relationship'
    END AS relationship_status
FROM funding_events f
JOIN companies co ON f.company_id = co.id
LEFT JOIN outreach_log o ON co.id = o.company_id
WHERE f.event_date >= date('now', '-30 days')
GROUP BY co.id, co.name, co.industry, co.status, f.id, f.round_type, f.amount, f.event_date, f.lead_investor
HAVING MAX(o.outreach_date) IS NULL OR MAX(o.outreach_date) < date('now', '-60 days')
ORDER BY f.event_date DESC, days_since_outreach DESC;

-- View 10: Client Health
-- Monitors relationship health of active client companies
-- Tracks last contact, days elapsed, and flags at-risk accounts
-- At-risk threshold: no contact in 45+ days (indicator of relationship decay)
DROP VIEW IF EXISTS v_client_health;
CREATE VIEW v_client_health AS
SELECT
    co.id AS company_id,
    co.name AS company_name,
    co.industry,
    co.status,
    COALESCE((SELECT COUNT(*) FROM contacts WHERE company_id = co.id), 0) AS contact_count,
    COALESCE((SELECT COUNT(*) FROM leases WHERE tenant_company_id = co.id AND status = 'active'), 0) AS active_leases,
    COALESCE((SELECT COUNT(*) FROM deals WHERE company_id = co.id AND status IN ('active', 'under_contract')), 0) AS active_deals,
    COALESCE(MAX(o.outreach_date), '1900-01-01') AS last_outreach_date,
    CAST((julianday('now') - COALESCE(julianday(MAX(o.outreach_date)), julianday('1900-01-01'))) AS INTEGER) AS days_since_last_contact,
    CASE
        WHEN CAST((julianday('now') - COALESCE(julianday(MAX(o.outreach_date)), julianday('1900-01-01'))) AS INTEGER) > 45 THEN 'at_risk'
        WHEN CAST((julianday('now') - COALESCE(julianday(MAX(o.outreach_date)), julianday('1900-01-01'))) AS INTEGER) > 30 THEN 'warning'
        ELSE 'healthy'
    END AS health_status,
    COALESCE((SELECT SUM(annual_rent) FROM leases WHERE tenant_company_id = co.id AND status = 'active'), 0) AS total_annual_rent
FROM companies co
LEFT JOIN outreach_log o ON co.id = o.company_id
WHERE co.status = 'active' OR co.status = 'client'
GROUP BY co.id, co.name, co.industry, co.status
ORDER BY days_since_last_contact DESC;
