"""
News Filter — Strict CRE-Relevant Event Filtering
====================================================
Filters news/events to only real-estate-relevant items.
Rejects generic business news (product launches, earnings, stock moves).
"""

import re
from typing import Dict, List, Optional, Tuple


# =============================================================================
# CATEGORY DEFINITIONS
# =============================================================================

ALLOWED_CATEGORIES = [
    'hiring',
    'layoffs',
    'funding',
    'leadership_change',
    'office_lease',
    'expansion',
    'relocation',
    'sublease',
    'acquisition',
    'headcount_growth',
    'new_office',
    'lease_expiry',
    'client_risk',
]

# Keywords that indicate CRE-relevant content (case-insensitive)
CRE_KEYWORDS = [
    # Space actions
    'lease', 'sublease', 'office', 'relocat', 'expansion', 'expand',
    'headquart', 'hq', 'move', 'vacate', 'downsize', 'consolidat',
    'coworking', 'flex space', 'square feet', 'sq ft', 'sf ',
    # Hiring / workforce (space driver)
    'hiring', 'headcount', 'layoff', 'rif', 'workforce reduction',
    'job posting', 'new hire', 'growth', 'recruit',
    # Funding (space driver)
    'funding', 'series a', 'series b', 'series c', 'series d',
    'raised', 'round', 'valuation', 'capital',
    # Leadership (strategy signal)
    'ceo', 'cfo', 'coo', 'cto', 'chief', 'vp ', 'head of',
    'facilities', 'workplace', 'real estate',
    # Market signals
    'tenant', 'landlord', 'broker', 'deal', 'transaction',
    'construction', 'build-out', 'fit-out',
]

# Keywords that indicate NON-relevant generic business news
BLOCKED_KEYWORDS = [
    'product launch', 'product update', 'feature release',
    'earnings', 'quarterly results', 'q1 ', 'q2 ', 'q3 ', 'q4 ',
    'stock price', 'share price', 'market cap',
    'analyst', 'rating', 'upgrade', 'downgrade',
    'opinion', 'editorial', 'podcast', 'webinar',
    'ipo', 'spac', 'ticker',
    'customer win', 'partnership', 'integration',
    'blog post', 'press release', 'sponsored',
    'award', 'ranking', 'best place to work',
    'conference', 'summit', 'keynote',
    'patent', 'lawsuit', 'litigation',
    'cryptocurrency', 'blockchain', 'nft',
    'ai model', 'algorithm', 'open source',
]

# Why-it-matters templates by category
WHY_IT_MATTERS = {
    'funding': 'Companies that raise capital typically expand headcount and space within 6-12 months.',
    'hiring': 'Rapid hiring drives demand for additional office space, typically 120-150 SF per new employee.',
    'layoffs': 'Workforce reductions often lead to sublease activity within 3-6 months.',
    'leadership_change': 'New C-suite executives typically review all real estate commitments within their first quarter.',
    'office_lease': 'Active lease activity signals imminent space decisions.',
    'expansion': 'Company has confirmed plans to grow its physical footprint.',
    'relocation': 'Company is evaluating new locations — prime opportunity for representation.',
    'sublease': 'Company listing sublease space — either downsizing or moving.',
    'acquisition': 'Acquisitions frequently trigger office consolidation or expansion.',
    'headcount_growth': 'Sustained headcount growth will require additional office capacity.',
    'new_office': 'Company opening a new office location — active real estate activity.',
    'lease_expiry': 'Upcoming lease expiration creates a decision window: renew, relocate, or expand.',
    'client_risk': 'Long gap since last contact — relationship maintenance needed.',
}


# =============================================================================
# FILTER FUNCTIONS
# =============================================================================

def classify_news_item(title: str, snippet: str = "",
                       category_hint: str = "") -> Tuple[Optional[str], str]:
    """
    Classify a news item into a CRE category or reject it.

    Args:
        title: headline or title text
        snippet: body text or description
        category_hint: optional pre-classified category

    Returns:
        (category, why_it_matters) if relevant, (None, '') if blocked
    """
    # If already categorized and valid, accept
    if category_hint and category_hint.lower() in ALLOWED_CATEGORIES:
        cat = category_hint.lower()
        return cat, WHY_IT_MATTERS.get(cat, '')

    combined = f"{title} {snippet}".lower()

    # Block check first — if blocked keyword found, reject
    for blocked in BLOCKED_KEYWORDS:
        if blocked in combined:
            return None, ''

    # Classify by CRE keyword matching
    best_category = None
    best_score = 0

    # Check keyword groups
    category_keywords = {
        'funding': ['funding', 'raised', 'series ', 'round', 'capital', 'valuation'],
        'hiring': ['hiring', 'headcount', 'job posting', 'recruit', 'new hire'],
        'layoffs': ['layoff', 'rif', 'workforce reduction', 'downsize', 'cut '],
        'leadership_change': ['ceo', 'cfo', 'coo', 'chief', 'appoint', 'named'],
        'office_lease': ['lease', 'sublease', 'sq ft', 'square feet', 'sf '],
        'expansion': ['expansion', 'expand', 'new office', 'opening', 'headquart'],
        'relocation': ['relocat', 'move', 'moving', 'new headquart'],
        'sublease': ['sublease', 'vacate', 'downsize'],
        'headcount_growth': ['growth', 'growing', 'team size'],
    }

    for cat, keywords in category_keywords.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_category = cat

    # Require at least one CRE keyword match
    has_cre_keyword = any(kw in combined for kw in CRE_KEYWORDS)

    if best_category and (best_score >= 1 or has_cre_keyword):
        return best_category, WHY_IT_MATTERS.get(best_category, '')

    if has_cre_keyword:
        return 'office_lease', WHY_IT_MATTERS.get('office_lease', '')

    return None, ''


def filter_news(items: List[Dict],
                title_key: str = "headline",
                snippet_key: str = "body",
                category_key: str = "category") -> List[Dict]:
    """
    Filter a list of news items to only CRE-relevant events.
    Adds 'why_it_matters' field to each passing item.

    Args:
        items: list of news item dicts
        title_key: key in dict for headline
        snippet_key: key in dict for body/snippet
        category_key: key for pre-classified category

    Returns:
        Filtered list with 'why_it_matters' added
    """
    filtered = []

    for item in items:
        title = item.get(title_key, '')
        snippet = item.get(snippet_key, '')
        hint = item.get(category_key, '')

        category, why = classify_news_item(title, snippet, hint)

        if category:
            enriched = dict(item)
            enriched['cre_category'] = category
            enriched['why_it_matters'] = why
            filtered.append(enriched)

    return filtered


def get_why_it_matters(category: str) -> str:
    """Get the 'why it matters' explanation for a category."""
    return WHY_IT_MATTERS.get(category, '')


if __name__ == "__main__":
    # Quick test
    test_items = [
        {"headline": "Ramp raises $150M in Series D", "body": "Led by Founders Fund", "category": "Funding"},
        {"headline": "Company XYZ launches new AI product", "body": "Now available on App Store", "category": ""},
        {"headline": "Citadel expanding NYC headquarters", "body": "Adding 50,000 SF at 425 Park", "category": ""},
        {"headline": "Tech firm reduces workforce by 20%", "body": "Layoffs across engineering", "category": ""},
        {"headline": "Q3 earnings beat estimates", "body": "Revenue up 15%", "category": ""},
    ]

    results = filter_news(test_items)
    print(f"Filtered {len(test_items)} items to {len(results)} CRE-relevant:")
    for r in results:
        print(f"  [{r['cre_category']}] {r['headline']}")
        print(f"    Why: {r['why_it_matters']}")
