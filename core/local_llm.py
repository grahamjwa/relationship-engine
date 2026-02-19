"""
Local LLM Integration (Ollama)
===============================
Prep module for using a local LLM to classify signals at scale
without paid API costs.

Requires: Ollama running locally (ollama.ai)
Default model: llama3.2 (or mistral)

This module is optional — all core functionality works without it.
When Ollama is available, it accelerates:
  - News headline classification (CRE relevance)
  - Signal summarization
  - Outreach angle generation
"""

import json
import os
import sys
from typing import Optional, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OLLAMA_BASE_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")


def is_ollama_available() -> bool:
    """Check if Ollama is running and reachable."""
    try:
        import urllib.request
        req = urllib.request.Request(f"{OLLAMA_BASE_URL}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False


def _query_ollama(prompt: str, model: str = DEFAULT_MODEL,
                  temperature: float = 0.1) -> Optional[str]:
    """
    Send a prompt to Ollama and return the response text.
    Returns None if Ollama is unavailable or errors.
    """
    try:
        import urllib.request
        payload = json.dumps({
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature}
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{OLLAMA_BASE_URL}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("response", "").strip()
    except Exception:
        return None


def classify_news_with_llm(headline: str, snippet: str = "",
                           model: str = DEFAULT_MODEL) -> Optional[Dict]:
    """
    Use local LLM to classify a news headline for CRE relevance.

    Returns dict with:
        - is_relevant: bool
        - category: str (expansion, contraction, funding, hiring, lease, leadership, other)
        - why_it_matters: str
        - confidence: float (0-1)

    Returns None if Ollama is unavailable.
    """
    prompt = f"""You are a commercial real estate analyst. Classify this news headline.

Headline: {headline}
{f'Snippet: {snippet}' if snippet else ''}

Respond in JSON only (no markdown):
{{
  "is_relevant": true/false,
  "category": "expansion|contraction|funding|hiring|lease|leadership|other",
  "why_it_matters": "1 sentence explaining CRE impact",
  "confidence": 0.0 to 1.0
}}"""

    response = _query_ollama(prompt, model=model, temperature=0.1)
    if not response:
        return None

    try:
        # Try to parse JSON from response
        # Handle cases where LLM wraps in markdown code blocks
        cleaned = response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned
            cleaned = cleaned.rsplit("```", 1)[0].strip()
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return None


def generate_outreach_angle(company_name: str, signals: List[str],
                            status: str = "prospect",
                            model: str = DEFAULT_MODEL) -> Optional[str]:
    """
    Use local LLM to generate a personalized outreach angle.

    Returns a 2-3 sentence outreach suggestion, or None if unavailable.
    """
    signals_str = "; ".join(signals[:5])
    prompt = f"""You are a commercial real estate broker. Suggest a brief (2-3 sentence)
outreach angle for contacting this company.

Company: {company_name}
Status: {status}
Recent signals: {signals_str}

Be specific, professional, and reference their signals. No generic pitches."""

    return _query_ollama(prompt, model=model, temperature=0.3)


def summarize_company_signals(company_name: str, signals: List[Dict],
                              model: str = DEFAULT_MODEL) -> Optional[str]:
    """
    Use local LLM to create a 1-paragraph executive summary of company signals.
    """
    signal_lines = []
    for s in signals[:8]:
        signal_lines.append(f"- [{s.get('signal_type', '?')}] {s.get('detail', '')}")
    signals_text = "\n".join(signal_lines)

    prompt = f"""Summarize these commercial real estate signals for {company_name} in one paragraph.
Focus on what this means for their office space needs.

Signals:
{signals_text}

Write 3-4 sentences. Be factual, no speculation."""

    return _query_ollama(prompt, model=model, temperature=0.2)


def filter_news_batch_local(headlines: List[Dict],
                            model: str = DEFAULT_MODEL) -> List[Dict]:
    """
    Batch-classify news headlines for CRE relevance using local LLM.
    Falls back gracefully if Ollama is unavailable.

    Each item in headlines should have 'title' and optionally 'snippet'.
    Returns items with added 'llm_relevant', 'llm_category', 'llm_why' fields.
    """
    if not is_ollama_available():
        return headlines  # Return unmodified if no LLM

    for item in headlines:
        result = classify_news_with_llm(
            item.get('title', ''),
            item.get('snippet', ''),
            model=model
        )
        if result:
            item['llm_relevant'] = result.get('is_relevant', False)
            item['llm_category'] = result.get('category', 'other')
            item['llm_why'] = result.get('why_it_matters', '')
            item['llm_confidence'] = result.get('confidence', 0.0)
        else:
            item['llm_relevant'] = None  # Unknown — LLM failed

    return headlines


if __name__ == "__main__":
    available = is_ollama_available()
    print(f"Ollama available: {available}")
    if available:
        print(f"URL: {OLLAMA_BASE_URL}")
        print(f"Model: {DEFAULT_MODEL}")
        # Quick test
        test = classify_news_with_llm("Ramp raises $150M Series D, plans NYC office expansion")
        if test:
            print(f"Test classification: {json.dumps(test, indent=2)}")
    else:
        print("Ollama not running. Install from https://ollama.ai")
        print("Then: ollama pull llama3.2")
