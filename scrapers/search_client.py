"""
Search Client for Relationship Engine
Wraps SerpApi for structured web searches.
"""

import os
import sys
from typing import Optional, List, Dict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


SERPAPI_BASE_URL = "https://serpapi.com/search"


def get_api_key() -> str:
    """Get SerpApi key from environment."""
    return os.environ.get("SERPAPI_KEY", "")


def search_funding(company_name: str, api_key: Optional[str] = None) -> List[Dict]:
    """
    Search for funding news about a company.
    
    Args:
        company_name: Name of the company
        api_key: SerpApi key (falls back to env var)
    
    Returns:
        List of search results with title, link, snippet, date
    """
    if not api_key:
        api_key = get_api_key()
    
    if not api_key:
        raise ValueError("SERPAPI_KEY not set")
    
    query = f'"{company_name}" funding round OR series OR raised'
    return _execute_search(query, api_key)


def search_hiring(company_name: str, api_key: Optional[str] = None) -> List[Dict]:
    """
    Search for hiring signals about a company.
    
    Args:
        company_name: Name of the company
        api_key: SerpApi key (falls back to env var)
    
    Returns:
        List of search results with title, link, snippet, date
    """
    if not api_key:
        api_key = get_api_key()
    
    if not api_key:
        raise ValueError("SERPAPI_KEY not set")
    
    query = f'"{company_name}" hiring OR "head of real estate" OR "VP real estate" OR "office expansion" OR "new office"'
    return _execute_search(query, api_key)


def search_lease(company_name: str, api_key: Optional[str] = None) -> List[Dict]:
    """
    Search for lease/real estate news about a company.
    
    Args:
        company_name: Name of the company
        api_key: SerpApi key (falls back to env var)
    
    Returns:
        List of search results with title, link, snippet, date
    """
    if not api_key:
        api_key = get_api_key()
    
    if not api_key:
        raise ValueError("SERPAPI_KEY not set")
    
    query = f'"{company_name}" lease OR "signed lease" OR "office space" OR "square feet" NYC OR Manhattan OR "New York"'
    return _execute_search(query, api_key)


def search_general(query: str, api_key: Optional[str] = None) -> List[Dict]:
    """
    Execute a general search query.
    
    Args:
        query: Search query string
        api_key: SerpApi key (falls back to env var)
    
    Returns:
        List of search results
    """
    if not api_key:
        api_key = get_api_key()
    
    if not api_key:
        raise ValueError("SERPAPI_KEY not set")
    
    return _execute_search(query, api_key)


def _execute_search(query: str, api_key: str, num_results: int = 10) -> List[Dict]:
    """
    Execute a search via SerpApi.
    
    Args:
        query: Search query
        api_key: SerpApi key
        num_results: Number of results to return
    
    Returns:
        List of cleaned search results
    """
    if not HAS_REQUESTS:
        raise ImportError("requests package required")
    
    params = {
        "q": query,
        "api_key": api_key,
        "engine": "google",
        "num": num_results,
        "gl": "us",
        "hl": "en"
    }
    
    try:
        response = requests.get(SERPAPI_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Search failed: {e}")
        return []
    
    # Extract organic results
    results = []
    for item in data.get("organic_results", []):
        results.append({
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "snippet": item.get("snippet", ""),
            "date": item.get("date", ""),
            "source": item.get("source", ""),
            "position": item.get("position", 0),
            "query": query,
            "searched_at": datetime.now().isoformat()
        })
    
    return results


def get_remaining_searches(api_key: Optional[str] = None) -> Dict:
    """
    Check remaining search quota on SerpApi account.
    
    Returns:
        Dict with account info and remaining searches
    """
    if not api_key:
        api_key = get_api_key()
    
    if not api_key:
        return {"error": "SERPAPI_KEY not set"}
    
    try:
        response = requests.get(
            "https://serpapi.com/account",
            params={"api_key": api_key},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # Test search
    print("Testing SerpApi connection...")
    account = get_remaining_searches()
    if "error" in account:
        print(f"Error: {account['error']}")
    else:
        print(f"Account: {account.get('email', 'N/A')}")
        print(f"Searches this month: {account.get('this_month_usage', 'N/A')}")
        print(f"Plan limit: {account.get('plan_searches_left', 'N/A')}")
