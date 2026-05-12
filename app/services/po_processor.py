"""
PO Processor — shared logic for turning a parsed email into a PurchaseOrder.

Used by both the HTTP email routes and the Gmail poller so neither
has to import from the other (avoids circular imports).
"""

import logging
import random
import string

logger = logging.getLogger("edi.po_processor")


def process_email_log(db, log_id: int):
    """
    Parse an EmailPOLog entry with Claude AI, then create a PurchaseOrder
    if the parse succeeds and at least one product can be resolved.

    Returns (purchase_order, error_string).
    On success error_string is None; on failure purchase_order is None.
    """
    from app.models import (
        EmailPOLog, EmailParseStatus,
        PurchaseOrder, POStatus, POItem,
        Company, Product,
    )
    from app.services.email_parser import parse_email_for_po

    log = db.query(EmailPOLog).filter(EmailPOLog.id == log_id).first()
    if not log:
        return None, "Email log not found"

    parsed = parse_email_for_po(
        sender_email=log.sender_email or "",
        subject=log.subject or "",
        body_text=log.body_text or "",
        body_html=log.body_html,
    )

    if "error" in parsed or not parsed.get("items"):
        log.parse_status  = EmailParseStatus.FAILED
        log.error_message = parsed.get("error", "No items extracted from email")
        log.parsed_data   = parsed
        db.commit()
        return None, log.error_message

    log.parsed_data  = parsed
    log.partner_code = log.partner_code or parsed.get("partner_code", "EMAIL")
    log.po_number    = (
        parsed.get("po_number")
        or f"EMAIL-{''.join(random.choices(string.digits, k=8))}"
    )

    # Match partner to a Company row — try by code, then by name
    partner_code = log.partner_code
    company = (
        db.query(Company).filter(Company.code == partner_code).first()
        or db.query(Company).filter(Company.name.ilike(f"%{partner_code}%")).first()
        or db.query(Company).first()
    )
    if not company:
        log.parse_status  = EmailParseStatus.FAILED
        log.error_message = "No matching company found in DB for partner: " + partner_code
        db.commit()
        return None, log.error_message

    # Resolve each line item to an internal Product
    items_payload = []
    for item in parsed.get("items", []):
        sku  = (item.get("sku") or "").strip()
        name = (item.get("product_name") or "").strip()
        qty  = int(item.get("quantity") or 0)
        if qty <= 0:
            continue

        product = None
        if sku:
            product = db.query(Product).filter(Product.sku == sku).first()
        if not product and name:
            product = (
                db.query(Product)
                .filter(Product.name.ilike(f"%{name[:30]}%"))
                .first()
            )
        if product:
            items_payload.append((product, qty, float(item.get("unit_price") or product.price_per_unit)))

    if not items_payload:
        log.parse_status  = EmailParseStatus.FAILED
        log.error_message = (
            "Email items could not be matched to any internal products. "
            "Add/update your Product Inventory so names or SKUs match."
        )
        db.commit()
        return None, log.error_message

    # Create PurchaseOrder
    po = PurchaseOrder(
        po_number  = log.po_number,
        company_id = company.id,
        status     = POStatus.PENDING,
        source     = "EMAIL",
        notes      = parsed.get("notes") or f"Auto-parsed from email: {log.subject}",
        raw_payload= parsed,
    )
    db.add(po)
    db.flush()

    total = 0.0
    for product, qty, unit_price in items_payload:
        subtotal = qty * unit_price
        total   += subtotal
        db.add(POItem(
            po_id         = po.id,
            product_id    = product.id,
            requested_qty = qty,
            fulfilled_qty = 0,
            unit_price    = unit_price,
            subtotal      = subtotal,
        ))

    po.total_amount  = total
    log.parse_status = EmailParseStatus.PARSED
    log.po_id        = po.id
    db.commit()
    db.refresh(po)

    logger.info(
        "Email PO created: po_number=%s company=%s items=%d total=%.2f",
        po.po_number, company.name, len(items_payload), total,
    )
    return po, None
