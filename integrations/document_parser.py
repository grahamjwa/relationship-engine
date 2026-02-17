"""
Document Parser for Relationship Engine
Extracts entities and relationships from PDF, DOCX, or TXT files
using the Anthropic API.
"""

import os
import sys
import json
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

try:
    from dotenv import load_dotenv
    load_dotenv()  # config.py already loaded .env
except ImportError:
    pass

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    import docx
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False


EXTRACTION_PROMPT = """You are an entity extraction assistant for a real estate relationship intelligence system.

Analyze the following text and extract ALL of the following entities and relationships:

1. **Company names** — with any context about industry, sector, or role
2. **Contact names** — with titles, roles, and company affiliations
3. **Relationships** — who knows whom, who works where, partnerships, investments
4. **Funding mentions** — any fundraising rounds, investment amounts, investors
5. **Hiring mentions** — job postings, executive hires, headcount changes, office expansions

Return a JSON object with this exact structure:
{
  "companies": [
    {"name": "...", "industry": "...", "sector": "...", "hq_city": "...", "hq_state": "...", "status": "prospect"}
  ],
  "contacts": [
    {"first_name": "...", "last_name": "...", "title": "...", "company_name": "...", "status": "active"}
  ],
  "relationships": [
    {"source_type": "contact|company", "source_name": "...", "target_type": "contact|company", "target_name": "...", "relationship_type": "works_at|knows|referred_by|tenant_of|advisor_to|investor_in|partner_with|competes_with", "strength": 5, "confidence": 0.8}
  ],
  "funding_events": [
    {"company_name": "...", "round_type": "...", "amount": null, "lead_investor": "...", "event_date": null}
  ],
  "hiring_signals": [
    {"company_name": "...", "signal_type": "job_posting|headcount_growth|exec_hire|layoff|office_expansion", "title": "...", "description": "..."}
  ]
}

Only include entities you are confident about. Set confidence scores accordingly.
If a field is unknown, use null.
Return ONLY valid JSON, no markdown formatting.

TEXT TO ANALYZE:
"""


def extract_text_from_pdf(file_path: str) -> str:
    """Extract text from a PDF file."""
    if not HAS_PYPDF:
        raise ImportError("pypdf is required for PDF parsing. Install with: pip install pypdf")
    text = ""
    reader = PdfReader(file_path)
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"
    return text.strip()


def extract_text_from_docx(file_path: str) -> str:
    """Extract text from a DOCX file."""
    if not HAS_DOCX:
        raise ImportError("python-docx is required for DOCX parsing. Install with: pip install python-docx")
    doc = docx.Document(file_path)
    return "\n".join(para.text for para in doc.paragraphs if para.text.strip())


def extract_text_from_txt(file_path: str) -> str:
    """Extract text from a plain text file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read().strip()


def extract_text(file_path: str) -> str:
    """Extract text from a file based on its extension."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return extract_text_from_pdf(file_path)
    elif ext == ".docx":
        return extract_text_from_docx(file_path)
    elif ext in (".txt", ".text", ".md"):
        return extract_text_from_txt(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Supported: .pdf, .docx, .txt")


def parse_document(file_path: str, api_key: Optional[str] = None) -> dict:
    """
    Parse a document and extract entities using Anthropic API.

    Args:
        file_path: Path to PDF, DOCX, or TXT file
        api_key: Anthropic API key (falls back to ANTHROPIC_API_KEY env var)

    Returns:
        Structured JSON of extracted entities (proposed, not auto-inserted)
    """
    if not HAS_ANTHROPIC:
        raise ImportError("anthropic package required. Install with: pip install anthropic")

    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("No Anthropic API key provided. Set ANTHROPIC_API_KEY in .env or pass api_key parameter.")

    # Extract text
    text = extract_text(file_path)
    if not text:
        return {"error": "No text extracted from document", "file_path": file_path}

    # Truncate if too long (API limit considerations)
    max_chars = 80000
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[TRUNCATED — document exceeded extraction limit]"

    # Call Anthropic API
    client = anthropic.Anthropic(api_key=key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[
            {"role": "user", "content": EXTRACTION_PROMPT + text}
        ]
    )

    # Parse response
    response_text = message.content[0].text.strip()

    # Handle potential markdown code blocks in response
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        response_text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    try:
        extracted = json.loads(response_text)
    except json.JSONDecodeError:
        return {
            "error": "Failed to parse API response as JSON",
            "raw_response": response_text,
            "file_path": file_path
        }

    extracted["_metadata"] = {
        "source_file": file_path,
        "text_length": len(text),
        "status": "proposed",
        "note": "These entries are PROPOSED only. Review before inserting into database."
    }

    return extracted


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python document_parser.py <file_path>")
        sys.exit(1)
    result = parse_document(sys.argv[1])
    print(json.dumps(result, indent=2))
