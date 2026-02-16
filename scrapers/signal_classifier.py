"""
Signal Classifier for Relationship Engine
Uses Claude to classify search results into actionable signals.
"""

import os
import sys
import json
from typing import Optional, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


CLASSIFICATION_PROMPT = """You are a signal classifier for a commercial real estate intelligence system.

Analyze the following search result and classify it:

Company being searched: {company_name}
Title: {title}
Snippet: {snippet}
Source: {source}
Date: {date}

Classify this result into ONE of these categories:
- "funding" — company raised money, closed a funding round, received investment
- "hiring" — company is hiring, especially real estate/facilities roles, or expanding headcount
- "lease" — company signed a lease, is looking for space, or has real estate news
- "expansion" — company is growing, opening new offices, expanding footprint
- "irrelevant" — not actionable for commercial real estate intelligence

Return a JSON object with:
{{
  "category": "funding|hiring|lease|expansion|irrelevant",
  "confidence": 0.0 to 1.0,
  "summary": "One sentence summary of the signal",
  "actionable": true or false,
  "details": {{
    "amount": "funding amount if mentioned",
    "investor": "lead investor if mentioned", 
    "location": "city/location if mentioned",
    "square_feet": "SF if mentioned",
    "role": "job title if hiring signal"
  }}
}}

Only return the JSON object, no other text."""


def classify_result(
    company_name: str,
    title: str,
    snippet: str,
    source: str = "",
    date: str = "",
    api_key: Optional[str] = None
) -> Dict:
    """
    Classify a single search result using Claude.
    
    Args:
        company_name: Company being searched
        title: Result title
        snippet: Result snippet
        source: Source website
        date: Date if available
        api_key: Anthropic API key (falls back to env var)
    
    Returns:
        Classification dict with category, confidence, summary, etc.
    """
    if not HAS_ANTHROPIC:
        raise ImportError("anthropic package required. Install with: pip install anthropic")
    
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    
    prompt = CLASSIFICATION_PROMPT.format(
        company_name=company_name,
        title=title,
        snippet=snippet,
        source=source,
        date=date
    )
    
    client = anthropic.Anthropic(api_key=api_key)
    
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.content[0].text.strip()
        
        # Parse JSON response
        # Handle potential markdown code blocks
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        
        result = json.loads(content)
        result["_raw_response"] = content
        result["_tokens_used"] = response.usage.input_tokens + response.usage.output_tokens
        
        return result
        
    except json.JSONDecodeError as e:
        return {
            "category": "irrelevant",
            "confidence": 0.0,
            "summary": "Failed to parse classification",
            "actionable": False,
            "error": f"JSON parse error: {e}",
            "_raw_response": content if 'content' in locals() else ""
        }
    except Exception as e:
        return {
            "category": "irrelevant",
            "confidence": 0.0,
            "summary": "Classification failed",
            "actionable": False,
            "error": str(e)
        }


def classify_batch(
    company_name: str,
    results: List[Dict],
    api_key: Optional[str] = None,
    skip_irrelevant: bool = True
) -> List[Dict]:
    """
    Classify a batch of search results.
    
    Args:
        company_name: Company being searched
        results: List of search results from search_client
        api_key: Anthropic API key
        skip_irrelevant: If True, filter out irrelevant results
    
    Returns:
        List of classified results with original data + classification
    """
    classified = []
    
    for result in results:
        classification = classify_result(
            company_name=company_name,
            title=result.get("title", ""),
            snippet=result.get("snippet", ""),
            source=result.get("source", ""),
            date=result.get("date", ""),
            api_key=api_key
        )
        
        # Merge original result with classification
        combined = {**result, "classification": classification}
        
        if skip_irrelevant and classification.get("category") == "irrelevant":
            continue
        
        classified.append(combined)
    
    return classified


if __name__ == "__main__":
    # Test classification
    test_result = classify_result(
        company_name="Citadel",
        title="Citadel raises $2 billion for new fund",
        snippet="Ken Griffin's Citadel has raised $2 billion for a new hedge fund, according to sources familiar with the matter.",
        source="Bloomberg",
        date="2025-02-01"
    )
    print(json.dumps(test_result, indent=2))
