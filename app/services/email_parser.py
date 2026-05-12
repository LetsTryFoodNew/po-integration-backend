"""
Email PO Parser — uses Claude AI to extract Purchase Order data from email text.

Supports:
  - Structured emails (regex fast-path)
  - Unstructured free-text emails (Claude Haiku AI parse)
  - HTML emails (strips tags before parsing)

Returns a dict with keys: po_number, partner_code, partner_name, order_date,
delivery_date, items, notes, confidence, error (if failed).
"""

import os
import re
import json
import logging
from typing import Optional

logger = logging.getLogger("edi.email_parser")

# System prompt — cached with prompt caching to save tokens on repeated calls
_SYSTEM_PROMPT = """You are a Purchase Order data extractor for Let's Try Foods, an FMCG snack brand.
Emails may come from retail partners (Blinkit, Zepto, Swiggy, BigBasket, JioMart) or direct B2B buyers.

Extract PO information and return ONLY a valid JSON object — no markdown, no explanation.

JSON schema:
{
  "po_number": "string or null",
  "partner_code": "BLINKIT|ZEPTO|SWIGGY|BIGBASKET|JIOMART|EMAIL",
  "partner_name": "string or null",
  "order_date": "YYYY-MM-DD or null",
  "delivery_date": "YYYY-MM-DD or null",
  "items": [
    {
      "product_name": "string",
      "sku": "string or null",
      "quantity": integer,
      "unit_price": float or 0
    }
  ],
  "notes": "string or null",
  "confidence": "HIGH|MEDIUM|LOW"
}

Rules:
- partner_code: detect from domain/name — zeptonow→ZEPTO, blinkit→BLINKIT, swiggy→SWIGGY, etc. Default: EMAIL
- If no PO items found, return {"error": "No PO line items found", "confidence": "LOW"}
- Quantities must be integers > 0
- Dates in YYYY-MM-DD format only
"""


def _strip_html(html: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _detect_partner_from_email(sender: str) -> str:
    sender = (sender or "").lower()
    if "blinkit" in sender or "grofers" in sender:
        return "BLINKIT"
    if "zepto" in sender or "zeptonow" in sender:
        return "ZEPTO"
    if "swiggy" in sender:
        return "SWIGGY"
    if "bigbasket" in sender:
        return "BIGBASKET"
    if "jiomart" in sender or "reliance" in sender:
        return "JIOMART"
    return "EMAIL"


def _try_regex_parse(subject: str, body: str) -> Optional[dict]:
    """Fast-path: try to extract a PO number from well-known email formats."""
    # Look for PO number patterns
    po_match = re.search(
        r'(?:PO|Purchase Order|Order)[#:\s-]*([A-Z0-9\-]{4,30})',
        subject + " " + body,
        re.IGNORECASE
    )
    if not po_match:
        return None

    po_number = po_match.group(1).strip()
    # Only return if we also find at least one quantity
    if not re.search(r'\b\d+\s*(?:units?|pcs?|cases?|boxes?|kgs?|qty)\b', body, re.IGNORECASE):
        return None

    return {"po_number": po_number, "_regex_hint": True}


def parse_email_for_po(
    sender_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
) -> dict:
    """
    Parse an email and return extracted PO data.
    Uses Claude Haiku with prompt caching for cost efficiency.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — email parsing unavailable")
        return {"error": "ANTHROPIC_API_KEY not configured", "confidence": "LOW"}

    # Prefer plain text; fall back to stripping HTML
    body = body_text or ""
    if not body and body_html:
        body = _strip_html(body_html)

    if not body.strip():
        return {"error": "Empty email body", "confidence": "LOW"}

    # Detect partner early so we can pass it as a hint
    partner_hint = _detect_partner_from_email(sender_email)

    # Build the user message
    user_content = (
        f"From: {sender_email}\n"
        f"Subject: {subject}\n"
        f"Detected partner hint: {partner_hint}\n\n"
        f"Email body:\n{body[:5000]}"
    )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=[{
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_content}],
        )

        raw = response.content[0].text.strip()
        logger.info("Claude email parse response (first 200 chars): %s", raw[:200])

        # Extract JSON — Claude should return bare JSON but be defensive
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not json_match:
            return {"error": f"Claude returned non-JSON: {raw[:200]}", "confidence": "LOW"}

        parsed = json.loads(json_match.group())

        # Attach partner hint if Claude left it as EMAIL and we detected better
        if parsed.get("partner_code") == "EMAIL" and partner_hint != "EMAIL":
            parsed["partner_code"] = partner_hint

        logger.info(
            "Email parsed: po_number=%s partner=%s confidence=%s items=%d",
            parsed.get("po_number"), parsed.get("partner_code"),
            parsed.get("confidence"), len(parsed.get("items", [])),
        )
        return parsed

    except json.JSONDecodeError as exc:
        return {"error": f"JSON parse error: {exc}", "confidence": "LOW"}
    except Exception as exc:
        logger.error("Email parser error: %s", exc)
        return {"error": str(exc), "confidence": "LOW"}
