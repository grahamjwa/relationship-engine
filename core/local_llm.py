"""
Local LLM Integration (Ollama) + Claude API Routing
=====================================================
Cheap/fast tasks → Ollama (local, free)
Quality tasks → Claude API (paid)

Requires: Ollama running locally (ollama.ai)
Default model: llama3.2 (or mistral)

This module is optional — all core functionality works without it.
When Ollama is available, it accelerates classification/filtering tasks.
"""

import json
import os
import sys
from typing import Optional, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OLLAMA_BASE_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")
CLAUDE_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# ROUTING
# ─────────────────────────────────────────────────────────────────────────────

OLLAMA_TASKS = ['classify', 'filter', 'extract', 'score', 'categorize', 'yes_no']
CLAUDE_TASKS = ['report', 'summarize', 'answer', 'draft', 'analyze', 'generate']


def route_to_model(task_type, prompt, temperature=0.1):
    """Route task to Ollama (cheap) or Claude (quality) based on task type."""
    if any(t in task_type.lower() for t in OLLAMA_TASKS):
        result = call_ollama(prompt, temperature=temperature)
        if result is not None:
            return result
        # Fall back to Claude if Ollama unavailable
        return call_claude(prompt, temperature=temperature)
    return call_claude(prompt, temperature=temperature)


def call_ollama(prompt, model=DEFAULT_MODEL, temperature=0.1):
    """Send prompt to local Ollama. Returns None if unavailable."""
    return _query_ollama(prompt, model=model, temperature=temperature)


def call_claude(prompt, model="claude-3-5-haiku-20241022", temperature=0.1):
    """Send prompt to Claude API. Returns None if no API key or error."""
    if not CLAUDE_API_KEY:
        return None
    try:
        import urllib.request
        payload = json.dumps({
            "model": model,
            "max_tokens": 1024,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}]
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01"
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("content", [{}])[0].get("text", "").strip()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# OLLAMA CORE
# ─────────────────────────────────────────────────────────────────────────────

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
    """Send a prompt to Ollama and return the response text."""
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


def _parse_json_response(response):
    """Parse JSON from LLM response, handling markdown code blocks."""
    if not response:
        return None
    cleaned = response.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned
        cleaned = cleaned.rsplit("```", 1)[0].strip()
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# OLLAMA TASKS (cheap, fast)
# ─────────────────────────────────────────────────────────────────────────────

def classify_title(title):
    """Classify an executive title into priority tier. Ollama task."""
    prompt = f"""Classify this job title into exactly one priority level:
- high (CEO, CFO, COO, CRO, Head of Real Estate, Managing Partner)
- medium (VP, Director, SVP, General Counsel, Head of Facilities)
- low (Manager, Associate, Analyst, Coordinator)

Title: {title}

Reply with only: high, medium, or low"""
    result = route_to_model('classify', prompt)
    if result and result.strip().lower() in ('high', 'medium', 'low'):
        return result.strip().lower()
    return 'medium'


def filter_news_relevance(headline, snippet=""):
    """Filter headline for CRE relevance. Returns True/False. Ollama task."""
    prompt = f"""Is this news headline relevant to commercial real estate brokerage?
Relevant = office leases, expansions, relocations, funding (implies growth), executive hires, space needs.
Not relevant = product launches, earnings, purely tech news with no space implications.

Headline: {headline}
{f'Snippet: {snippet}' if snippet else ''}

Reply with only: true or false"""
    result = route_to_model('filter', prompt)
    if result:
        return result.strip().lower().startswith('true')
    return False


def categorize_input(text):
    """Categorize a Discord/user input message. Ollama task."""
    return categorize_discord_input(text)


def categorize_discord_input(text):
    """Use Ollama to sort Discord messages into categories."""
    prompt = f"""Categorize this message into exactly one category:
- funding (company raised money)
- rumor (company in market for space)
- outreach (meeting, call, email logged)
- agency (update on building we represent)
- subagent (request for new automation)
- exec (executive change)
- other

Message: {text}

Reply with only the category name."""
    result = route_to_model('categorize', prompt)
    if result:
        cat = result.strip().lower().split()[0]
        if cat in ('funding', 'rumor', 'outreach', 'agency', 'subagent', 'exec', 'other'):
            return cat
    return 'other'


def extract_company_name(text):
    """Extract the primary company name from a text message. Ollama task."""
    prompt = f"""Extract the primary company name from this message.
If no company is mentioned, reply "none".
Reply with only the company name.

Message: {text}"""
    result = route_to_model('extract', prompt)
    if result and result.strip().lower() != 'none':
        return result.strip()
    return None


def score_signal_importance(signal_type, detail, company_status="prospect"):
    """Score a signal's importance 1-10 for CRE brokerage. Ollama task."""
    prompt = f"""Score this signal's importance for a commercial real estate broker (1-10).
10 = company definitely needs office space soon
1 = irrelevant noise

Signal type: {signal_type}
Detail: {detail}
Company status: {company_status}

Reply with only a number 1-10."""
    result = route_to_model('score', prompt)
    if result:
        try:
            score = int(result.strip().split()[0])
            return max(1, min(10, score))
        except (ValueError, IndexError):
            pass
    return 5


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE TASKS (quality)
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(data, report_type="weekly"):
    """Generate a formatted report. Claude task."""
    prompt = f"""Generate a concise {report_type} report for a CRE broker.
Data: {json.dumps(data, default=str)[:3000]}

Format: Markdown with sections. Be factual, concise. No fluff."""
    return route_to_model('report', prompt, temperature=0.3)


def summarize_company(company_name, signals, context=""):
    """Summarize a company's status. Claude task."""
    prompt = f"""Summarize {company_name}'s current status for a CRE broker in 3-4 sentences.
Focus on: space needs, growth trajectory, decision timeline.

Signals: {json.dumps(signals, default=str)[:2000]}
{f'Context: {context}' if context else ''}

Be factual. No speculation."""
    return route_to_model('summarize', prompt, temperature=0.2)


def answer_complex_query(question, context_data=None):
    """Answer a complex analytical question. Claude task."""
    ctx = f"\nContext data: {json.dumps(context_data, default=str)[:3000]}" if context_data else ""
    prompt = f"""You are a CRE intelligence assistant. Answer this question concisely.
{ctx}

Question: {question}

Be direct. Facts first. 1-2 actionable next steps if applicable."""
    return route_to_model('answer', prompt, temperature=0.2)


def draft_outreach_email(company_name, contact_name, signals, context=""):
    """Draft an outreach email. Claude task."""
    prompt = f"""Draft a short (3-4 sentence) professional outreach email for a CRE broker.

To: {contact_name} at {company_name}
Recent signals: {json.dumps(signals, default=str)[:1500]}
{f'Context: {context}' if context else ''}

Tone: Professional but warm. Reference specific signals. Include a clear ask (meeting/call).
No generic pitches. Keep it under 100 words."""
    return route_to_model('draft', prompt, temperature=0.3)


# ─────────────────────────────────────────────────────────────────────────────
# LEGACY FUNCTIONS (preserved for backwards compatibility)
# ─────────────────────────────────────────────────────────────────────────────

def classify_news_with_llm(headline, snippet="", model=DEFAULT_MODEL):
    """Classify a news headline for CRE relevance. Returns dict or None."""
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
    return _parse_json_response(response)


def generate_outreach_angle(company_name, signals, status="prospect",
                            model=DEFAULT_MODEL):
    """Generate a personalized outreach angle. Returns string or None."""
    signals_str = "; ".join(signals[:5])
    prompt = f"""You are a commercial real estate broker. Suggest a brief (2-3 sentence)
outreach angle for contacting this company.

Company: {company_name}
Status: {status}
Recent signals: {signals_str}

Be specific, professional, and reference their signals. No generic pitches."""
    return _query_ollama(prompt, model=model, temperature=0.3)


def summarize_company_signals(company_name, signals, model=DEFAULT_MODEL):
    """Create a 1-paragraph executive summary of company signals."""
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


def filter_news_batch_local(headlines, model=DEFAULT_MODEL):
    """Batch-classify news headlines for CRE relevance using local LLM."""
    if not is_ollama_available():
        return headlines

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
            item['llm_relevant'] = None

    return headlines


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='LLM Router')
    parser.add_argument('--test', action='store_true', help='Run connectivity test')
    parser.add_argument('--classify', help='Classify a title')
    parser.add_argument('--categorize', help='Categorize a message')
    parser.add_argument('--extract', help='Extract company name')
    parser.add_argument('--score', help='Score a signal')
    parser.add_argument('--relevance', help='Check news relevance')
    args = parser.parse_args()

    if args.test:
        available = is_ollama_available()
        print(f"Ollama available: {available}")
        if available:
            print(f"URL: {OLLAMA_BASE_URL}")
            print(f"Model: {DEFAULT_MODEL}")
            test = classify_news_with_llm("Ramp raises $150M Series D, plans NYC office expansion")
            if test:
                print(f"Test: {json.dumps(test, indent=2)}")
        print(f"Claude API key set: {bool(CLAUDE_API_KEY)}")

    elif args.classify:
        print(f"Priority: {classify_title(args.classify)}")

    elif args.categorize:
        print(f"Category: {categorize_discord_input(args.categorize)}")

    elif args.extract:
        print(f"Company: {extract_company_name(args.extract)}")

    elif args.score:
        print(f"Score: {score_signal_importance('funding', args.score)}")

    elif args.relevance:
        print(f"Relevant: {filter_news_relevance(args.relevance)}")

    else:
        parser.print_help()
