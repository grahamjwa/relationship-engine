"""
Comprehensive unit tests for opportunity_scoring.py module.

Uses in-memory SQLite databases to test all scoring functions without
requiring a real database. Tests cover all major functions with various
data scenarios (high/low values, edge cases, empty data).
"""

import unittest
import sqlite3
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import scoring functions
from core.opportunity_scoring import (
    days_since,
    decay_factor,
    score_company_funding,
    score_company_hiring,
    score_company_lease_expiry,
    score_company_relationship_proximity,
    hiring_velocity_delta,
    funding_acceleration_index,
    relationship_depth_multiplier,
    competitive_coverage_factor,
    score_cash_adjacency,
    compute_company_opportunity_score,
    _get_company_category,
)
from core.graph_engine import DiGraph, _node_key, find_shortest_path


class TestHelperFunctions(unittest.TestCase):
    """Test utility functions used by scoring functions."""

    def test_days_since_with_valid_date_string(self):
        """Test days_since with a valid date string."""
        today = datetime.now().strftime("%Y-%m-%d")
        days = days_since(today)
        self.assertLessEqual(days, 1)  # Should be 0 or 1 depending on timing

    def test_days_since_with_old_date(self):
        """Test days_since with a very old date."""
        old_date = "2020-01-01"
        days = days_since(old_date)
        self.assertGreater(days, 1000)

    def test_days_since_with_none(self):
        """Test days_since with None returns very large value."""
        days = days_since(None)
        self.assertEqual(days, 9999)

    def test_days_since_with_datetime_string_format(self):
        """Test days_since with datetime string (YYYY-MM-DD HH:MM:SS)."""
        today = datetime.now().strftime("%Y-%m-%d 10:30:45")
        days = days_since(today)
        self.assertLessEqual(days, 1)

    def test_decay_factor_today(self):
        """Test decay_factor for day 0 returns 1.0."""
        factor = decay_factor(0, 180)
        self.assertEqual(factor, 1.0)

    def test_decay_factor_at_half_life(self):
        """Test decay_factor at half-life returns approximately 0.5."""
        factor = decay_factor(180, 180)
        self.assertAlmostEqual(factor, 0.5, places=2)

    def test_decay_factor_future_days(self):
        """Test decay_factor with negative days (future) returns 1.0."""
        factor = decay_factor(-10, 180)
        self.assertEqual(factor, 1.0)

    def test_decay_factor_long_time(self):
        """Test decay_factor decays toward 0 over long time."""
        factor = decay_factor(1000, 180)
        self.assertLess(factor, 0.1)


class TestScoreCompanyFunding(unittest.TestCase):
    """Test score_company_funding function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE funding_events (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                event_date TEXT,
                amount REAL,
                round_type TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_funding_events(self):
        """Test scoring with no funding events returns 0."""
        score = score_company_funding(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_recent_funding_high_amount(self):
        """Test scoring with recent, high-amount funding."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 1000000000, 'Series C')
        """, (today,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        self.assertGreater(score, 50.0)
        self.assertLessEqual(score, 100.0)

    def test_old_funding_events(self):
        """Test scoring with old funding events (decayed)."""
        old_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 1000000000, 'Series A')
        """, (old_date,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        # Should be significantly less due to decay over 1 year
        self.assertLess(score, 50.0)
        self.assertGreater(score, 0.0)

    def test_multiple_funding_events(self):
        """Test scoring with multiple funding events."""
        today = datetime.now().strftime("%Y-%m-%d")
        old_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 500000000, 'Series B')
        """, (today,))
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 250000000, 'Series A')
        """, (old_date,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        # Multiple events should accumulate
        self.assertGreater(score, 50.0)
        self.assertLessEqual(score, 100.0)

    def test_funding_with_unknown_amount(self):
        """Test scoring with NULL amount (unknown)."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, NULL, 'Series A')
        """, (today,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        # Should still have some score (0.3 factor for unknown amount)
        self.assertGreater(score, 0.0)
        self.assertLess(score, 50.0)


class TestScoreCompanyHiring(unittest.TestCase):
    """Test score_company_hiring function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE hiring_signals (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                signal_date TEXT,
                relevance TEXT,
                signal_type TEXT,
                details TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_hiring_signals(self):
        """Test scoring with no hiring signals returns 0."""
        score = score_company_hiring(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_high_relevance_leadership_hire(self):
        """Test scoring with high-relevance leadership hire."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'high', 'leadership_hire', 'New CTO hired')
        """, (today,))
        self.conn.commit()

        score = score_company_hiring(1, self.conn)
        self.assertGreater(score, 20.0)
        self.assertLessEqual(score, 100.0)

    def test_medium_relevance_job_posting(self):
        """Test scoring with medium-relevance job posting."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'medium', 'job_posting', 'Engineering roles posted')
        """, (today,))
        self.conn.commit()

        score = score_company_hiring(1, self.conn)
        self.assertGreater(score, 5.0)
        self.assertLess(score, 50.0)

    def test_low_relevance_press_announcement(self):
        """Test scoring with low-relevance press announcement."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'low', 'press_announcement', 'General hiring announcement')
        """, (today,))
        self.conn.commit()

        score = score_company_hiring(1, self.conn)
        self.assertGreater(score, 0.0)
        self.assertLess(score, 30.0)

    def test_multiple_hiring_signals(self):
        """Test scoring with multiple hiring signals accumulates."""
        today = datetime.now().strftime("%Y-%m-%d")
        recent = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'high', 'leadership_hire', 'New CEO')
        """, (today,))
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'medium', 'headcount_growth', 'Expanding team')
        """, (recent,))
        self.conn.commit()

        score = score_company_hiring(1, self.conn)
        self.assertGreater(score, 30.0)
        self.assertLessEqual(score, 100.0)

    def test_old_hiring_signals_decay(self):
        """Test that old signals decay appropriately."""
        old_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'high', 'leadership_hire', 'Old hire')
        """, (old_date,))
        self.conn.commit()

        score = score_company_hiring(1, self.conn)
        # Should decay significantly over 180 days (half-life of hiring)
        self.assertLess(score, 15.0)


class TestScoreCompanyLeaseExpiry(unittest.TestCase):
    """Test score_company_lease_expiry function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE leases (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                lease_expiry TEXT,
                square_feet REAL,
                building_id INTEGER
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_leases(self):
        """Test scoring with no leases returns 0."""
        score = score_company_lease_expiry(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_lease_expiring_soon_within_year(self):
        """Test scoring with lease expiring within 1 year."""
        expiry = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 50000, 1)
        """, (expiry,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        self.assertGreater(score, 20.0)
        self.assertLessEqual(score, 100.0)

    def test_lease_expiring_1_to_2_years(self):
        """Test scoring with lease expiring between 1-2 years."""
        expiry = (datetime.now() + timedelta(days=550)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 50000, 1)
        """, (expiry,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        self.assertGreater(score, 10.0)
        self.assertLess(score, 50.0)

    def test_lease_expiring_beyond_2_years(self):
        """Test scoring with lease expiring beyond 2 years."""
        expiry = (datetime.now() + timedelta(days=900)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 50000, 1)
        """, (expiry,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        self.assertGreater(score, 0.0)
        self.assertLess(score, 30.0)

    def test_large_lease_size_impact(self):
        """Test that larger lease size increases score."""
        expiry = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 200000, 1)
        """, (expiry,))
        self.conn.commit()

        score_large = score_company_lease_expiry(1, self.conn)

        # Clear and insert smaller lease
        self.conn.execute("DELETE FROM leases")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 10000, 1)
        """, (expiry,))
        self.conn.commit()

        score_small = score_company_lease_expiry(1, self.conn)
        self.assertGreater(score_large, score_small)

    def test_expired_lease_skipped(self):
        """Test that already-expired leases are skipped."""
        past = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 50000, 1)
        """, (past,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_unknown_square_feet(self):
        """Test scoring with unknown square footage."""
        expiry = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, NULL, 1)
        """, (expiry,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        # Should still have some score (0.3 factor for unknown SF)
        self.assertGreater(score, 5.0)
        self.assertLess(score, 50.0)


class TestHiringVelocityDelta(unittest.TestCase):
    """Test hiring_velocity_delta function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE hiring_signals (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                signal_date TEXT,
                relevance TEXT,
                signal_type TEXT,
                details TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_signals_returns_zero(self):
        """Test velocity with no signals returns 0."""
        velocity = hiring_velocity_delta(1, self.conn)
        self.assertEqual(velocity, 0.0)

    def test_positive_velocity_acceleration(self):
        """Test positive velocity when hiring accelerates."""
        # Insert 3 signals in prior 90 days
        for i in range(3):
            date = (datetime.now() - timedelta(days=120 - i * 20)).strftime("%Y-%m-%d")
            self.conn.execute("""
                INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
                VALUES (1, ?, 'high', 'leadership_hire', 'Hire')
            """, (date,))

        # Insert 5 signals in recent 90 days
        for i in range(5):
            date = (datetime.now() - timedelta(days=45 - i * 15)).strftime("%Y-%m-%d")
            self.conn.execute("""
                INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
                VALUES (1, ?, 'high', 'leadership_hire', 'Hire')
            """, (date,))
        self.conn.commit()

        velocity = hiring_velocity_delta(1, self.conn)
        self.assertGreater(velocity, 0.0)

    def test_negative_velocity_deceleration(self):
        """Test negative velocity when hiring decelerates."""
        # Insert 5 signals in prior 90 days
        for i in range(5):
            date = (datetime.now() - timedelta(days=120 - i * 15)).strftime("%Y-%m-%d")
            self.conn.execute("""
                INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
                VALUES (1, ?, 'high', 'leadership_hire', 'Hire')
            """, (date,))

        # Insert 1 signal in recent 90 days
        date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'high', 'leadership_hire', 'Hire')
        """, (date,))
        self.conn.commit()

        velocity = hiring_velocity_delta(1, self.conn)
        # Negative or zero velocity is expected (deceleration or no change)
        self.assertLessEqual(velocity, 0.0)

    def test_new_activity_from_zero(self):
        """Test strong positive signal when no prior activity, then recent activity."""
        # Insert signals only in recent 90 days
        for i in range(3):
            date = (datetime.now() - timedelta(days=30 - i * 10)).strftime("%Y-%m-%d")
            self.conn.execute("""
                INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
                VALUES (1, ?, 'high', 'leadership_hire', 'Hire')
            """, (date,))
        self.conn.commit()

        velocity = hiring_velocity_delta(1, self.conn)
        # New activity from zero should be strong positive
        self.assertGreater(velocity, 30.0)


class TestFundingAccelerationIndex(unittest.TestCase):
    """Test funding_acceleration_index function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE funding_events (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                event_date TEXT,
                amount REAL,
                round_type TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_funding_events(self):
        """Test acceleration with no funding returns 0."""
        accel = funding_acceleration_index(1, self.conn)
        self.assertEqual(accel, 0.0)

    def test_single_funding_event(self):
        """Test acceleration with only one event returns 0."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 100000000, 'Series A')
        """, (today,))
        self.conn.commit()

        accel = funding_acceleration_index(1, self.conn)
        self.assertEqual(accel, 0.0)

    def test_accelerating_funding_amounts(self):
        """Test positive acceleration with increasing round sizes."""
        dates = [
            (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
            (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ]
        amounts = [100000000, 250000000, 500000000]  # Increasing amounts

        for date, amount in zip(dates, amounts):
            self.conn.execute("""
                INSERT INTO funding_events (company_id, event_date, amount, round_type)
                VALUES (1, ?, ?, 'Series')
            """, (date, amount))
        self.conn.commit()

        accel = funding_acceleration_index(1, self.conn)
        self.assertGreater(accel, 0.0)

    def test_decelerating_funding_amounts(self):
        """Test low acceleration with decreasing round sizes."""
        dates = [
            (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
            (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ]
        amounts = [500000000, 250000000, 100000000]  # Decreasing amounts

        for date, amount in zip(dates, amounts):
            self.conn.execute("""
                INSERT INTO funding_events (company_id, event_date, amount, round_type)
                VALUES (1, ?, ?, 'Series')
            """, (date, amount))
        self.conn.commit()

        accel = funding_acceleration_index(1, self.conn)
        # Decreasing should give low score (closer to 0)
        self.assertGreaterEqual(accel, 0.0)
        self.assertLess(accel, 50.0)

    def test_frequent_funding_acceleration(self):
        """Test positive frequency acceleration (rounds getting closer)."""
        dates = [
            (datetime.now() - timedelta(days=360)).strftime("%Y-%m-%d"),
            (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ]
        amounts = [100000000, 150000000, 200000000]

        for date, amount in zip(dates, amounts):
            self.conn.execute("""
                INSERT INTO funding_events (company_id, event_date, amount, round_type)
                VALUES (1, ?, ?, 'Series')
            """, (date, amount))
        self.conn.commit()

        accel = funding_acceleration_index(1, self.conn)
        self.assertGreater(accel, 0.0)


class TestScoreCashAdjacency(unittest.TestCase):
    """Test score_cash_adjacency function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                cash_reserves REAL,
                cash_updated_at TEXT,
                category TEXT,
                mature INTEGER
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_company(self):
        """Test cash scoring with non-existent company returns 0."""
        score = score_cash_adjacency(999, self.conn)
        self.assertEqual(score, 0.0)

    def test_no_cash_data(self):
        """Test cash scoring with NULL cash returns 0."""
        self.conn.execute("""
            INSERT INTO companies (id, name, cash_reserves, cash_updated_at, category, mature)
            VALUES (1, 'Test Corp', NULL, NULL, 'institutional', 1)
        """)
        self.conn.commit()

        score = score_cash_adjacency(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_high_cash_reserves_recent(self):
        """Test cash scoring with high recent cash reserves."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO companies (id, name, cash_reserves, cash_updated_at, category, mature)
            VALUES (1, 'Rich Corp', 200000000, ?, 'institutional', 1)
        """, (today,))
        self.conn.commit()

        score = score_cash_adjacency(1, self.conn)
        self.assertGreater(score, 80.0)
        self.assertLessEqual(score, 100.0)

    def test_low_cash_reserves(self):
        """Test cash scoring with low cash reserves."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO companies (id, name, cash_reserves, cash_updated_at, category, mature)
            VALUES (1, 'Poor Corp', 10000000, ?, 'institutional', 1)
        """, (today,))
        self.conn.commit()

        score = score_cash_adjacency(1, self.conn)
        self.assertGreater(score, 0.0)
        self.assertLess(score, 20.0)

    def test_cash_data_decay(self):
        """Test that old cash data decays in value."""
        old_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO companies (id, name, cash_reserves, cash_updated_at, category, mature)
            VALUES (1, 'Old Data Corp', 200000000, ?, 'institutional', 1)
        """, (old_date,))
        self.conn.commit()

        score_old = score_cash_adjacency(1, self.conn)

        # Compare to recent data
        self.conn.execute("DELETE FROM companies")
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO companies (id, name, cash_reserves, cash_updated_at, category, mature)
            VALUES (1, 'Recent Data Corp', 200000000, ?, 'institutional', 1)
        """, (today,))
        self.conn.commit()

        score_recent = score_cash_adjacency(1, self.conn)
        self.assertLess(score_old, score_recent)


class TestGetCompanyCategory(unittest.TestCase):
    """Test _get_company_category function."""

    def setUp(self):
        """Set up in-memory SQLite database with test schema."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_high_growth_category(self):
        """Test retrieving high_growth category."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature)
            VALUES (1, 'Startup', 'high_growth', 0)
        """)
        self.conn.commit()

        category = _get_company_category(1, self.conn)
        self.assertEqual(category, 'high_growth')

    def test_institutional_category(self):
        """Test retrieving institutional category."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature)
            VALUES (1, 'BigCorp', 'institutional', 1)
        """)
        self.conn.commit()

        category = _get_company_category(1, self.conn)
        self.assertEqual(category, 'institutional')

    def test_default_category(self):
        """Test retrieving NULL category returns None."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature)
            VALUES (1, 'Unknown', NULL, 0)
        """)
        self.conn.commit()

        category = _get_company_category(1, self.conn)
        self.assertIsNone(category)

    def test_nonexistent_company(self):
        """Test with non-existent company returns None."""
        category = _get_company_category(999, self.conn)
        self.assertIsNone(category)

    def test_category_from_graph(self):
        """Test retrieving category from graph object if provided."""
        graph = DiGraph()
        company_node = _node_key("company", 1)
        graph.add_node(company_node, entity_type="company", category="high_growth")

        category = _get_company_category(1, self.conn, graph)
        self.assertEqual(category, 'high_growth')


class TestScoreCompanyRelationshipProximity(unittest.TestCase):
    """Test score_company_relationship_proximity function."""

    def setUp(self):
        """Set up in-memory SQLite database and graph with test data."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                title TEXT,
                company_id INTEGER,
                role_level TEXT,
                status TEXT,
                centrality_score REAL,
                leverage_score REAL
            )
        """)
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER,
                industry TEXT,
                status TEXT,
                revenue_est REAL,
                office_sf REAL,
                cash_reserves REAL,
                cash_updated_at TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_team_contacts(self):
        """Test with no team contacts returns 0."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Test Corp', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.commit()

        score = score_company_relationship_proximity(1, self.conn)
        self.assertEqual(score, 0.0)

    def test_direct_team_contact_at_company(self):
        """Test direct relationship (team member at company)."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Target Corp', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'John', 'Doe', 'VP Sales', 1, 'team', 'active')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (2, 'Jane', 'Smith', 'CEO', 2, 'team', 'active')
        """)
        self.conn.commit()

        # Mock graph with simple connections
        graph = DiGraph()
        graph.add_node("contact_2", entity_type="contact")
        graph.add_node("company_1", entity_type="company")
        graph.add_node("contact_1", entity_type="contact")

        score = score_company_relationship_proximity(1, self.conn, graph)
        # Team member at company should give 100
        self.assertEqual(score, 100.0)

    def test_one_hop_path(self):
        """Test one-hop path between team and company contact."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Target Corp', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Alice', 'Brown', 'VP', 1, 'external_partner', 'active')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (2, 'Bob', 'Jones', 'CTO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("contact_2", entity_type="contact")
        graph.add_node("company_1", entity_type="company")
        graph.add_node("contact_1", entity_type="contact")
        # Direct connection: team -> company contact
        graph.add_edge("contact_2", "contact_1", weight=1.0, relationship_type="colleague")

        score = score_company_relationship_proximity(1, self.conn, graph)
        self.assertEqual(score, 100.0)

    def test_no_path_to_company(self):
        """Test when no path exists to company."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Isolated Corp', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CTO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("contact_1", entity_type="contact")
        graph.add_node("company_1", entity_type="company")

        score = score_company_relationship_proximity(1, self.conn, graph)
        self.assertEqual(score, 0.0)


class TestRelationshipDepthMultiplier(unittest.TestCase):
    """Test relationship_depth_multiplier function."""

    def setUp(self):
        """Set up in-memory SQLite database and graph."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                title TEXT,
                company_id INTEGER,
                role_level TEXT,
                status TEXT,
                centrality_score REAL,
                leverage_score REAL
            )
        """)
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER,
                industry TEXT,
                status TEXT,
                revenue_est REAL,
                office_sf REAL,
                cash_reserves REAL,
                cash_updated_at TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_no_targets(self):
        """Test with no company targets returns 0."""
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CTO', 1, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("contact_1", entity_type="contact")

        depth = relationship_depth_multiplier(999, self.conn, graph)
        self.assertEqual(depth, 0.0)

    def test_direct_connection_to_company(self):
        """Test direct connection to company node."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Target', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CTO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("contact_1", entity_type="contact")
        graph.add_node("company_1", entity_type="company")
        graph.add_edge("contact_1", "company_1", weight=1.5, relationship_type="client")

        depth = relationship_depth_multiplier(1, self.conn, graph)
        # Direct connection should give good score
        self.assertGreater(depth, 50.0)


class TestCompetitiveCoverageFactor(unittest.TestCase):
    """Test competitive_coverage_factor function."""

    def setUp(self):
        """Set up in-memory SQLite database."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER,
                industry TEXT,
                status TEXT,
                revenue_est REAL,
                office_sf REAL,
                cash_reserves REAL,
                cash_updated_at TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_company_not_in_graph(self):
        """Test with company not in graph returns 50 (moderate opportunity)."""
        graph = DiGraph()
        coverage = competitive_coverage_factor(999, self.conn, graph)
        self.assertEqual(coverage, 50.0)

    def test_no_coverage(self):
        """Test with no broker coverage returns 100 (max opportunity)."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Target', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company")

        # Mock broker_coverage_overlap to return empty
        with patch('core.opportunity_scoring.broker_coverage_overlap') as mock_coverage:
            mock_coverage.return_value = {}
            score = competitive_coverage_factor(1, self.conn, graph)
            self.assertEqual(score, 100.0)

    def test_single_threaded_coverage(self):
        """Test with single-threaded coverage returns 80."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Target', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company")

        with patch('core.opportunity_scoring.broker_coverage_overlap') as mock_coverage:
            mock_coverage.return_value = {
                "company_1": {"total_bridges": 1}
            }
            score = competitive_coverage_factor(1, self.conn, graph)
            self.assertEqual(score, 80.0)

    def test_well_covered_company(self):
        """Test with well-covered company returns lower score."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Well Covered', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company")

        with patch('core.opportunity_scoring.broker_coverage_overlap') as mock_coverage:
            mock_coverage.return_value = {
                "company_1": {"total_bridges": 6}
            }
            score = competitive_coverage_factor(1, self.conn, graph)
            self.assertLess(score, 50.0)


class TestComputeCompanyOpportunityScore(unittest.TestCase):
    """Integration test for compute_company_opportunity_score function."""

    def setUp(self):
        """Set up comprehensive test database."""
        self.conn = sqlite3.connect(":memory:")

        # Create all required tables
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER,
                industry TEXT,
                status TEXT,
                revenue_est REAL,
                office_sf REAL,
                cash_reserves REAL,
                cash_updated_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                title TEXT,
                company_id INTEGER,
                role_level TEXT,
                status TEXT,
                centrality_score REAL,
                leverage_score REAL
            )
        """)
        self.conn.execute("""
            CREATE TABLE funding_events (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                event_date TEXT,
                amount REAL,
                round_type TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE hiring_signals (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                signal_date TEXT,
                relevance TEXT,
                signal_type TEXT,
                details TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE leases (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                lease_expiry TEXT,
                square_feet REAL,
                building_id INTEGER
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database connection."""
        self.conn.close()

    def test_basic_score_computation(self):
        """Test basic opportunity score computation with minimal data."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Test Corp', NULL, 0, 'Tech', 'prospect')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CEO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company", category=None)
        graph.add_node("contact_1", entity_type="contact")

        scores = compute_company_opportunity_score(1, self.conn, graph)

        # Should have all required keys
        self.assertIn('total', scores)
        self.assertIn('funding', scores)
        self.assertIn('hiring', scores)
        self.assertIn('lease_expiry', scores)
        self.assertIn('relationship', scores)
        self.assertIn('company_id', scores)
        self.assertIn('category', scores)

        # All scores should be non-negative and <= 100
        for key in ['funding', 'hiring', 'lease_expiry', 'relationship']:
            self.assertGreaterEqual(scores[key], 0.0)
            self.assertLessEqual(scores[key], 100.0)

    def test_high_growth_category_scoring(self):
        """Test scoring with high_growth category uses appropriate weights."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (1, 'Startup', 'high_growth', 0, 'Tech', 'high_growth_target')
        """)
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 500000000, 'Series B')
        """, (today,))
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CEO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company", category="high_growth")
        graph.add_node("contact_1", entity_type="contact")

        scores = compute_company_opportunity_score(1, self.conn, graph)

        # High-growth should weight funding heavily
        self.assertGreater(scores['funding'], 30.0)
        self.assertEqual(scores['category'], 'high_growth')

    def test_institutional_category_scoring(self):
        """Test scoring with institutional category uses appropriate weights."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status, cash_reserves, cash_updated_at)
            VALUES (1, 'BigCorp', 'institutional', 1, 'Finance', 'active_client', 500000000, ?)
        """, (today,))
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status)
            VALUES (2, 'OurTeam', 'institutional', 1, 'Finance', 'active_client')
        """)
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CEO', 2, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company", category="institutional")
        graph.add_node("contact_1", entity_type="contact")

        scores = compute_company_opportunity_score(1, self.conn, graph)

        # Institutional should have cash_adjacency included
        self.assertIn('cash_adjacency', scores)
        self.assertEqual(scores['category'], 'institutional')

    def test_complete_company_profile(self):
        """Test scoring with comprehensive company data."""
        self.conn.execute("""
            INSERT INTO companies (id, name, category, mature, industry, status, cash_reserves, cash_updated_at)
            VALUES (1, 'Full Profile Corp', 'high_growth', 0, 'Tech', 'prospect', 150000000, ?)
        """, (datetime.now().strftime("%Y-%m-%d"),))

        today = datetime.now().strftime("%Y-%m-%d")

        # Add funding
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 500000000, 'Series C')
        """, (today,))

        # Add hiring signals
        self.conn.execute("""
            INSERT INTO hiring_signals (company_id, signal_date, relevance, signal_type, details)
            VALUES (1, ?, 'high', 'leadership_hire', 'New CTO')
        """, (today,))

        # Add lease
        lease_expiry = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 100000, 1)
        """, (lease_expiry,))

        # Add team contact
        self.conn.execute("""
            INSERT INTO contacts (id, first_name, last_name, title, company_id, role_level, status)
            VALUES (1, 'Team', 'Member', 'CEO', 999, 'team', 'active')
        """)
        self.conn.commit()

        graph = DiGraph()
        graph.add_node("company_1", entity_type="company", category="high_growth")
        graph.add_node("contact_1", entity_type="contact")

        scores = compute_company_opportunity_score(1, self.conn, graph)

        # Overall score should be reasonably high with multiple signals
        self.assertGreater(scores['total'], 10.0)
        self.assertEqual(scores['company_id'], 1)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        """Set up test database."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                mature INTEGER,
                industry TEXT,
                status TEXT,
                revenue_est REAL,
                office_sf REAL,
                cash_reserves REAL,
                cash_updated_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE funding_events (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                event_date TEXT,
                amount REAL,
                round_type TEXT
            )
        """)
        self.conn.commit()

    def tearDown(self):
        """Close database."""
        self.conn.close()

    def test_very_large_funding_amount(self):
        """Test with extremely large funding amounts."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 10000000000, 'IPO')
        """, (today,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        # Should cap at 100
        self.assertLessEqual(score, 100.0)
        self.assertGreater(score, 50.0)

    def test_zero_funding_amount(self):
        """Test with zero funding amount."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type)
            VALUES (1, ?, 0, 'Seed')
        """, (today,))
        self.conn.commit()

        score = score_company_funding(1, self.conn)
        # Zero amount should be treated like unknown
        self.assertGreater(score, 0.0)
        self.assertLessEqual(score, 30.0)

    def test_future_lease_expiry(self):
        """Test with lease expiring very far in future."""
        expiry = (datetime.now() + timedelta(days=3650)).strftime("%Y-%m-%d")

        self.conn.execute("""
            CREATE TABLE leases (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                lease_expiry TEXT,
                square_feet REAL,
                building_id INTEGER
            )
        """)
        self.conn.execute("""
            INSERT INTO leases (company_id, lease_expiry, square_feet, building_id)
            VALUES (1, ?, 50000, 1)
        """, (expiry,))
        self.conn.commit()

        score = score_company_lease_expiry(1, self.conn)
        # Very distant lease should have minimal score
        self.assertLess(score, 20.0)


if __name__ == '__main__':
    unittest.main()
