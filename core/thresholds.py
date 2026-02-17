"""
Configurable thresholds for entity categorization and scoring.
All thresholds are defined here as module-level constants.
Override via environment variables (RE_THRESHOLD_*) for quick tuning.

To tune: change values here or set env vars in private_data/.env
e.g. RE_THRESHOLD_REVENUE=75000000
"""

import os

def _env_float(key: str, default: float) -> float:
    val = os.environ.get(key)
    if val:
        try:
            return float(val)
        except ValueError:
            pass
    return default


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    if val:
        try:
            return int(val)
        except ValueError:
            pass
    return default


# =============================================================================
# ENTITY CATEGORIZATION THRESHOLDS
# =============================================================================

# Companies above either threshold are "mature" / institutional
REVENUE_THRESHOLD = _env_float("RE_THRESHOLD_REVENUE", 50_000_000)       # $50M
SF_THRESHOLD = _env_int("RE_THRESHOLD_SF", 30_000)                       # 30,000 SF
CASH_BONUS_THRESHOLD = _env_float("RE_THRESHOLD_CASH", 100_000_000)      # $100M

# =============================================================================
# SCORING WEIGHT PROFILES
# Weights sum to 1.0 within each profile.
# =============================================================================

# Default (balanced) — used when category is unknown or mixed
WEIGHTS_DEFAULT = {
    'funding': 0.15,
    'hiring': 0.12,
    'lease_expiry': 0.15,
    'relationship': 0.10,
    'hiring_velocity': 0.10,
    'funding_accel': 0.08,
    'rel_depth': 0.12,
    'coverage': 0.10,
    'momentum': 0.08,
}

# High-growth: funding signals dominate, connections matter less
WEIGHTS_HIGH_GROWTH = {
    'funding': 0.20,
    'hiring': 0.15,
    'lease_expiry': 0.10,
    'relationship': 0.05,
    'hiring_velocity': 0.15,
    'funding_accel': 0.15,
    'rel_depth': 0.05,
    'coverage': 0.10,
    'momentum': 0.05,
}

# Institutional / mature: connections dominant, funding downweighted
WEIGHTS_INSTITUTIONAL = {
    'funding': 0.05,
    'hiring': 0.08,
    'lease_expiry': 0.15,
    'relationship': 0.18,
    'hiring_velocity': 0.05,
    'funding_accel': 0.03,
    'rel_depth': 0.18,
    'coverage': 0.12,
    'momentum': 0.06,
    'cash_adjacency': 0.10,   # Extra dimension for institutional
}

# =============================================================================
# PREDICTIVE CHAIN PARAMETERS
# =============================================================================

# Capital → Expansion → Lease lag expectations (months)
CHAIN_LAG_MIN_MONTHS = 6
CHAIN_LAG_MAX_MONTHS = 18

# Probability thresholds for chain surfacing
CHAIN_SURFACE_THRESHOLD = 0.5    # Surface predictions above this prob
CHAIN_HIGH_CONFIDENCE = 0.75     # Flag as high-confidence above this

# Funding amount thresholds for chain scoring
CHAIN_LARGE_RAISE = 500_000_000  # $500M — triggers higher expansion prob
CHAIN_MEDIUM_RAISE = 100_000_000 # $100M

# =============================================================================
# DECAY HALF-LIVES (days)
# =============================================================================

HALF_LIFE_FUNDING = 180       # 6 months
HALF_LIFE_HIRING = 90         # 3 months
HALF_LIFE_OUTREACH = 30       # 1 month
HALF_LIFE_RELATIONSHIP = 730  # 2 years
HALF_LIFE_CASH = 365          # 1 year — cash reserves freshness
