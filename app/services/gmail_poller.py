"""
Gmail IMAP Poller — reads Purchase Order emails from Gmail labels.

Connects to imap.gmail.com using GMAIL_ADDRESS + GMAIL_APP_PASSWORD.
Polls each configured label, deduplicates by Gmail Message-ID,
then passes new emails through Claude AI → PO creation pipeline.

Credentials (add to backend/.env):
  GMAIL_ADDRESS=ecom@earthcrust.co.in
  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx   (spaces are stripped automatically)
"""

import imaplib
import email as email_lib
import email.header as email_header_lib
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("edi.gmail")

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# Gmail label name (exact, case-sensitive) → internal partner code
# All labels confirmed live from ecom@earthcrust.co.in inbox
# NOTE: BLINKIT_PO excluded — Blinkit has its own webhook system (/api/webhook/inbound/blinkit/po)
#       Zepto_PO / ZEPTO ADMIN PO excluded — Zepto has its own Silk Route API (/api/zepto/po-events)
LABEL_PARTNER_MAP: dict[str, str] = {
    "SWIGGY_PO":                 "SWIGGY",
    "FLIPKART":                  "FLIPKART",
    "Big_Basket_PO":             "BIGBASKET",
    "DAALCHINI_PO":              "DAALCHINI",
    "DMART_PO":                  "DMART",
    "FIRST_CLUB PO":             "FIRSTCLUB",
    "Reliance_POs":              "RELIANCE",
    "Amazon_POs":                "AMAZON",
    "REVISED_PO":                "EMAIL",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _decode_header(value: str) -> str:
    parts = email_header_lib.decode_header(value or "")
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return "".join(decoded)


def _extract_body(msg) -> tuple[str, str]:
    """Return (plain_text, html_text) from a MIME email message."""
    plain, html = "", ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition  = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition:
                continue
            try:
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                charset = part.get_content_charset() or "utf-8"
                text    = payload.decode(charset, errors="replace")
                if content_type == "text/plain":
                    plain += text
                elif content_type == "text/html":
                    html += text
            except Exception:
                continue
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                text    = payload.decode(charset, errors="replace")
                if msg.get_content_type() == "text/html":
                    html = text
                else:
                    plain = text
        except Exception:
            pass

    return plain, html


def _imap_label(label: str) -> str:
    """Format a Gmail label name for IMAP SELECT (quote if it contains spaces)."""
    return f'"{label}"' if " " in label else label


# ── Connection ─────────────────────────────────────────────────────────────────

def _connect() -> imaplib.IMAP4_SSL:
    address  = os.getenv("GMAIL_ADDRESS", "").strip()
    password = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "")
    if not address or not password:
        raise ValueError("GMAIL_ADDRESS and GMAIL_APP_PASSWORD must be set in .env")
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    mail.login(address, password)
    return mail


def test_connection() -> dict:
    """Test Gmail IMAP connectivity and return available labels."""
    try:
        mail = _connect()
        status, folder_list = mail.list()
        labels = []
        if status == "OK":
            for entry in folder_list or []:
                if isinstance(entry, bytes):
                    decoded = entry.decode("utf-8", errors="replace")
                    # IMAP LIST format: (\HasNoChildren) "/" "LABEL_NAME"
                    parts = decoded.rsplit(' "/" ', 1)
                    if len(parts) == 2:
                        label = parts[1].strip().strip('"')
                        labels.append(label)
        mail.logout()
        return {
            "connected": True,
            "address":   os.getenv("GMAIL_ADDRESS", ""),
            "labels":    sorted(labels),
        }
    except Exception as exc:
        return {
            "connected": False,
            "address":   os.getenv("GMAIL_ADDRESS", ""),
            "error":     str(exc),
        }


# ── Deduplication ──────────────────────────────────────────────────────────────

def _already_imported(db, gmail_message_id: str) -> bool:
    """Return True if we already have this Gmail message in email_po_logs."""
    from app.models import EmailPOLog

    if not gmail_message_id:
        return False
    # raw_payload is JSON; query the gmail_message_id key inside it
    existing = (
        db.query(EmailPOLog)
        .filter(
            EmailPOLog.raw_payload["gmail_message_id"].as_string() == gmail_message_id
        )
        .first()
    )
    return existing is not None


# ── Per-label poll ─────────────────────────────────────────────────────────────

def _poll_label(
    db,
    mail:         imaplib.IMAP4_SSL,
    label:        str,
    partner_code: str,
    days_back:    int,
    max_emails:   int,
) -> dict:
    """
    Read up to `max_emails` new emails from one Gmail label.
    Returns {"imported": n, "skipped": n, "errors": n}.
    """
    from app.models import EmailPOLog, EmailParseStatus
    from app.services.po_processor import process_email_log

    result = {"imported": 0, "skipped": 0, "errors": 0}

    # SELECT the label
    try:
        status, _ = mail.select(_imap_label(label), readonly=True)
        if status != "OK":
            logger.warning("Gmail: cannot SELECT label '%s' — it may not exist", label)
            result["errors"] += 1
            return result
    except Exception as exc:
        logger.error("Gmail SELECT error label='%s': %s", label, exc)
        result["errors"] += 1
        return result

    # SEARCH emails from the past N days
    since = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    try:
        status, data = mail.search(None, f"SINCE {since}")
        if status != "OK" or not data or not data[0]:
            return result
    except Exception as exc:
        logger.error("Gmail SEARCH error label='%s': %s", label, exc)
        result["errors"] += 1
        return result

    uid_list = data[0].split()
    # Most-recent first, up to max_emails
    uid_list = uid_list[-max_emails:][::-1]
    logger.info("Gmail label='%s': %d emails since %s", label, len(uid_list), since)

    for uid in uid_list:
        try:
            # Fetch headers first for fast deduplication
            status, hdr_data = mail.fetch(uid, "(RFC822.HEADER)")
            if status != "OK" or not hdr_data or not hdr_data[0]:
                continue

            raw_hdr = hdr_data[0][1]
            hdr_msg = email_lib.message_from_bytes(raw_hdr)
            gmail_msg_id = hdr_msg.get("Message-ID", "").strip()

            if gmail_msg_id and _already_imported(db, gmail_msg_id):
                result["skipped"] += 1
                continue

            # Fetch full email
            status, full_data = mail.fetch(uid, "(RFC822)")
            if status != "OK" or not full_data or not full_data[0]:
                continue

            msg        = email_lib.message_from_bytes(full_data[0][1])
            subject    = _decode_header(msg.get("Subject", ""))
            from_addr  = _decode_header(msg.get("From", ""))
            body_text, body_html = _extract_body(msg)

            logger.info(
                "Gmail importing: label='%s' from='%s' subject='%s'",
                label, from_addr[:50], subject[:60],
            )

            log = EmailPOLog(
                sender_email = from_addr[:200] or None,
                subject      = subject[:500] or None,
                body_text    = body_text or None,
                body_html    = body_html or None,
                parse_status = EmailParseStatus.PENDING,
                partner_code = partner_code,
                raw_payload  = {
                    "gmail_message_id": gmail_msg_id,
                    "gmail_label":      label,
                    "partner_code":     partner_code,
                    "source":           "gmail_poll",
                },
            )
            db.add(log)
            db.commit()
            db.refresh(log)

            process_email_log(db, log.id)
            result["imported"] += 1

        except Exception as exc:
            logger.error("Gmail error processing uid=%s label='%s': %s", uid, label, exc)
            result["errors"] += 1
            continue

    return result


# ── Main entry point ───────────────────────────────────────────────────────────

def poll_all_labels(
    db,
    days_back:     int = 30,
    max_per_label: int = 50,
    labels:        Optional[list[str]] = None,
) -> dict:
    """
    Poll all (or a subset of) configured Gmail labels and import new PO emails.

    Args:
        db:            SQLAlchemy session
        days_back:     How many days back to fetch (default 30)
        max_per_label: Max emails to import per label per run (default 50)
        labels:        Filter to specific label names; None = all configured labels

    Returns a summary dict with totals + per-label breakdown.
    """
    target = {
        k: v for k, v in LABEL_PARTNER_MAP.items()
        if labels is None or k in labels
    }

    summary = {
        "imported":       0,
        "skipped":        0,
        "errors":         0,
        "labels_checked": [],
    }

    try:
        mail = _connect()
        logger.info(
            "Gmail poll started: %d labels, days_back=%d, max_per_label=%d",
            len(target), days_back, max_per_label,
        )
    except Exception as exc:
        logger.error("Gmail connection failed: %s", exc)
        return {**summary, "error": str(exc)}

    try:
        for label, partner_code in target.items():
            stats = _poll_label(db, mail, label, partner_code, days_back, max_per_label)
            summary["imported"] += stats["imported"]
            summary["skipped"]  += stats["skipped"]
            summary["errors"]   += stats["errors"]
            summary["labels_checked"].append({
                "label":    label,
                "partner":  partner_code,
                "imported": stats["imported"],
                "skipped":  stats["skipped"],
                "errors":   stats["errors"],
            })
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    logger.info(
        "Gmail poll complete: imported=%d skipped=%d errors=%d",
        summary["imported"], summary["skipped"], summary["errors"],
    )
    return summary
