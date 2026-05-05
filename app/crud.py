from sqlalchemy.orm import Session
from app import models, schemas
from datetime import datetime, timedelta
import random, string, httpx, base64, logging

logger = logging.getLogger("edi_integration")

def generate_po_number(company_code: str) -> str:
    suffix = ''.join(random.choices(string.digits, k=6))
    return f"PO-{company_code.upper()}-{suffix}"

def generate_sap_order_id() -> str:
    return f"SAP-{random.randint(1000000, 9999999)}"

def generate_asn_number() -> str:
    return f"ASN-{random.randint(100000, 999999)}"

def generate_universal_po_number() -> str:
    suffix = ''.join(random.choices(string.digits, k=6))
    return f"UPO-{datetime.now().year}-{suffix}"

# SAP Customer Master: partner_code → SAP Sold-To / Ship-To party codes
SAP_CUSTOMER_MAP = {
    "BLK":      {"sold_to": "C-10001", "ship_to": "S-10001", "name": "Blinkit"},
    "BLINKIT":  {"sold_to": "C-10001", "ship_to": "S-10001", "name": "Blinkit"},
    "ZPT":      {"sold_to": "C-10002", "ship_to": "S-10002", "name": "Zepto"},
    "ZEPTO":    {"sold_to": "C-10002", "ship_to": "S-10002", "name": "Zepto"},
    "SWG":      {"sold_to": "C-10003", "ship_to": "S-10003", "name": "Swiggy Instamart"},
    "SWIGGY":   {"sold_to": "C-10003", "ship_to": "S-10003", "name": "Swiggy Instamart"},
    "BBK":      {"sold_to": "C-10004", "ship_to": "S-10004", "name": "BigBasket"},
    "BIGBASKET":{"sold_to": "C-10004", "ship_to": "S-10004", "name": "BigBasket"},
}

# ---- Product Mapping Engine ----
def resolve_product(db: Session, partner_code: str, partner_sku: str, partner_name: str = None):
    """
    Resolve partner SKU → internal Product + SAP material code.
    Step 1: Exact mapping lookup (product_mappings table)
    Step 2: Fallback — match by internal SKU directly
    Step 3: Fuzzy name match → auto-creates low-confidence mapping
    Returns: (product, mapping, confidence_score, log_message)
    """
    pc = partner_code.upper()

    # Step 1 — Exact mapping in product_mappings table
    mapping = db.query(models.ProductMapping).filter(
        models.ProductMapping.partner_code == pc,
        models.ProductMapping.partner_sku  == partner_sku,
        models.ProductMapping.is_active    == True
    ).first()
    if mapping:
        return mapping.product, mapping, mapping.confidence_score, \
            f"✅ MAPPED [{pc}] '{partner_sku}' → '{mapping.product.name}' (SAP: {mapping.sap_material_code}) [confidence: {mapping.confidence_score:.0%}]"

    # Step 2 — Direct SKU match in products table
    product = db.query(models.Product).filter(models.Product.sku == partner_sku).first()
    if product:
        return product, None, 1.0, \
            f"✅ DIRECT SKU match: '{partner_sku}' → '{product.name}'"

    # Step 3 — Fuzzy name match using RapidFuzz
    if partner_name:
        try:
            from rapidfuzz import fuzz, process as rfprocess
            all_products = db.query(models.Product).all()
            if all_products:
                choices = {p.id: p.name for p in all_products}
                result = rfprocess.extractOne(
                    partner_name,
                    choices,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=65
                )
                if result:
                    matched_name, score, matched_id = result
                    confidence = round(score / 100, 2)
                    product = next(p for p in all_products if p.id == matched_id)
                    # Auto-create mapping for human review
                    try:
                        auto_map = models.ProductMapping(
                            partner_code=pc,
                            partner_sku=partner_sku,
                            partner_product_name=partner_name,
                            product_id=product.id,
                            sap_material_code=product.sap_material_code,
                            confidence_score=confidence,
                            mapped_by="AUTO",
                            notes=f"RapidFuzz match score: {score:.1f}% — NEEDS VERIFICATION"
                        )
                        db.add(auto_map); db.flush()
                    except Exception:
                        db.rollback()
                        auto_map = None
                    return product, auto_map, confidence, \
                        f"⚠️ FUZZY-MAPPED [{pc}] '{partner_name}' → '{product.name}' [RapidFuzz score: {score:.1f}% — needs verification]"
        except ImportError:
            # Fallback to simple substring match
            normalized = partner_name.lower().replace(" ", "").replace("-", "").replace("'", "")
            all_products = db.query(models.Product).all()
            for p in all_products:
                p_norm = p.name.lower().replace(" ", "").replace("-", "").replace("'", "")
                if p_norm in normalized or normalized in p_norm:
                    try:
                        auto_map = models.ProductMapping(
                            partner_code=pc, partner_sku=partner_sku,
                            partner_product_name=partner_name, product_id=p.id,
                            sap_material_code=p.sap_material_code, confidence_score=0.7,
                            mapped_by="AUTO", notes="Substring match — NEEDS VERIFICATION"
                        )
                        db.add(auto_map); db.flush()
                    except Exception:
                        db.rollback(); auto_map = None
                    return p, auto_map, 0.7, \
                        f"⚠️ AUTO-MAPPED [{pc}] '{partner_name}' → '{p.name}' [confidence: 70% — needs verification]"

    return None, None, 0.0, \
        f"❌ UNMAPPED SKU: [{pc}] '{partner_sku}' ('{partner_name}') — add to product_mappings table"

def get_product_mappings(db: Session, partner_code: str = None):
    q = db.query(models.ProductMapping)
    if partner_code:
        q = q.filter(models.ProductMapping.partner_code == partner_code.upper())
    return q.order_by(models.ProductMapping.partner_code).all()

def create_product_mapping(db: Session, data: schemas.ProductMappingCreate):
    obj = models.ProductMapping(
        partner_code=data.partner_code.upper(),
        partner_sku=data.partner_sku,
        partner_product_name=data.partner_product_name,
        product_id=data.product_id,
        sap_material_code=data.sap_material_code,
        notes=data.notes,
        confidence_score=1.0,
        mapped_by="MANUAL"
    )
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

def delete_product_mapping(db: Session, mapping_id: int):
    obj = db.query(models.ProductMapping).filter(models.ProductMapping.id == mapping_id).first()
    if obj:
        db.delete(obj); db.commit()
    return obj

def get_companies(db: Session):
    return db.query(models.Company).all()

def create_company(db: Session, data: schemas.CompanyCreate):
    obj = models.Company(**data.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

def update_company_integration(db: Session, company_id: int, data: schemas.CompanyIntegrationUpdate):
    obj = db.query(models.Company).filter(models.Company.id == company_id).first()
    if not obj:
        return None
    for k, v in data.model_dump(exclude_none=True).items():
        setattr(obj, k, v)
    db.commit(); db.refresh(obj)
    return obj

# ---- Product CRUD ----
def get_products(db: Session):
    return db.query(models.Product).all()

def get_product(db: Session, product_id: int):
    return db.query(models.Product).filter(models.Product.id == product_id).first()

def create_product(db: Session, data: schemas.ProductCreate):
    obj = models.Product(**data.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

def update_product_stock(db: Session, product_id: int, data: schemas.ProductUpdate):
    obj = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not obj:
        return None
    for k, v in data.model_dump(exclude_none=True).items():
        setattr(obj, k, v)
    db.commit(); db.refresh(obj)
    return obj

# ---- PO CRUD ----
def get_purchase_orders(db: Session, company_id: int = None, status: str = None):
    q = db.query(models.PurchaseOrder)
    if company_id:
        q = q.filter(models.PurchaseOrder.company_id == company_id)
    if status:
        q = q.filter(models.PurchaseOrder.status == status)
    return q.order_by(models.PurchaseOrder.created_at.desc()).all()

def get_purchase_order(db: Session, po_id: int):
    return db.query(models.PurchaseOrder).filter(models.PurchaseOrder.id == po_id).first()

def _process_po_items(db, po, items_data):
    """Shared logic to create PO items and check stock"""
    total = 0.0
    all_available = True
    any_available = False
    for item_data in items_data:
        product = db.query(models.Product).filter(models.Product.id == item_data.product_id).first()
        if not product:
            db.rollback()
            return None, f"Product ID {item_data.product_id} not found"
        fulfilled = min(product.stock_quantity, item_data.requested_qty)
        subtotal = fulfilled * product.price_per_unit
        if fulfilled < item_data.requested_qty:
            all_available = False
        if fulfilled > 0:
            any_available = True
        po_item = models.POItem(
            po_id=po.id, product_id=product.id,
            requested_qty=item_data.requested_qty,
            fulfilled_qty=fulfilled,
            unit_price=product.price_per_unit,
            subtotal=subtotal
        )
        db.add(po_item)
        product.stock_quantity -= fulfilled
        total += subtotal
    return (total, all_available, any_available), None

def create_purchase_order(db: Session, data: schemas.POCreate):
    company = db.query(models.Company).filter(models.Company.id == data.company_id).first()
    if not company:
        return None, "Company not found"
    po = models.PurchaseOrder(
        po_number=generate_po_number(company.code),
        company_id=data.company_id,
        notes=data.notes,
        status=models.POStatus.PENDING,
        source="MANUAL"
    )
    db.add(po); db.flush()
    result, err = _process_po_items(db, po, data.items)
    if err:
        return None, err
    total, all_avail, any_avail = result
    if all_avail and any_avail:
        po.status = models.POStatus.STOCK_AVAILABLE
    elif any_avail:
        po.status = models.POStatus.STOCK_PARTIAL
    else:
        po.status = models.POStatus.OUT_OF_STOCK
    po.total_amount = total
    po.sap_order_id = generate_sap_order_id()
    db.commit(); db.refresh(po)
    # Auto-create SAP Sales Order record
    try:
        create_sap_sales_order(db, po)
    except Exception as sap_err:
        logger.error(f"SAP SO creation failed for manual PO {po.po_number}: {sap_err}")
    return po, None

def update_po_status(db: Session, po_id: int, status: models.POStatus):
    po = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.id == po_id).first()
    if not po:
        return None
    po.status = status
    po.updated_at = datetime.utcnow()
    db.commit(); db.refresh(po)
    return po

# ---- Inbound Webhook: receive PO from partner (Blinkit / Zepto / Swiggy) ----
def process_inbound_po_webhook(db: Session, payload: schemas.InboundPOPayload, source_ip: str = None):
    """
    Core EDI handler: partner sends us a PO via HTTP POST (Basic Auth).
    We validate stock, create PO, log the webhook, and return ACK.
    Per Blinkit API contract.
    """
    log = models.WebhookLog(
        event_type="PO_CREATED",
        source_ip=source_ip,
        payload=payload.model_dump(),
        po_number=payload.po_number,
        status=models.WebhookStatus.PENDING,
    )

    # Find company by partner_code
    company = db.query(models.Company).filter(models.Company.code == payload.partner_code).first()
    if not company:
        log.status = models.WebhookStatus.FAILED
        log.error_message = f"Unknown partner_code: {payload.partner_code}"
        log.response_status = 404
        db.add(log); db.commit()
        return None, f"Unknown partner_code: {payload.partner_code}"

    log.company_id = company.id

    # Check for duplicate PO
    existing = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.po_number == payload.po_number).first()
    if existing:
        log.status = models.WebhookStatus.FAILED
        log.error_message = "Duplicate PO number"
        log.response_status = 409
        db.add(log); db.commit()
        return None, f"Duplicate PO: {payload.po_number}"

    sap_id = generate_sap_order_id()
    sap_customer = SAP_CUSTOMER_MAP.get(payload.partner_code.upper(), {"sold_to": "C-UNKNOWN", "ship_to": "S-UNKNOWN"})
    transformation_log = []

    po = models.PurchaseOrder(
        po_number=payload.po_number,
        company_id=company.id,
        notes=payload.notes,
        status=models.POStatus.PENDING,
        source="WEBHOOK",
        sap_order_id=sap_id,
        raw_payload=payload.model_dump()
    )
    db.add(po); db.flush()

    total = 0.0
    all_available = True
    any_available = False

    for item_data in payload.items:
        product, mapping, confidence, map_log = resolve_product(
            db, payload.partner_code, item_data.sku, item_data.product_name
        )
        transformation_log.append(map_log)

        if not product:
            # Flag unmapped SKU for human review
            flag_unmapped_sku(db, payload.partner_code, item_data.sku, item_data.product_name, payload.po_number)
            # Auto-create with zero stock so PO can still be recorded
            product = models.Product(
                sku=item_data.sku,
                name=item_data.product_name,
                price_per_unit=item_data.unit_price,
                stock_quantity=0,
                reorder_level=50,
            )
            db.add(product); db.flush()
            transformation_log.append(f"🆕 AUTO-CREATED product: '{item_data.product_name}' — add SAP mapping manually")

        fulfilled = min(product.stock_quantity, item_data.quantity)
        subtotal = fulfilled * product.price_per_unit
        if fulfilled < item_data.quantity:
            all_available = False
        if fulfilled > 0:
            any_available = True

        po_item = models.POItem(
            po_id=po.id, product_id=product.id,
            requested_qty=item_data.quantity,
            fulfilled_qty=fulfilled,
            unit_price=product.price_per_unit,
            subtotal=subtotal
        )
        db.add(po_item)
        product.stock_quantity -= fulfilled
        total += subtotal

    if all_available and any_available:
        po.status = models.POStatus.STOCK_AVAILABLE
    elif any_available:
        po.status = models.POStatus.STOCK_PARTIAL
    else:
        po.status = models.POStatus.OUT_OF_STOCK

    po.total_amount = total
    # Store transformation log + SAP customer mapping in raw_payload
    po.raw_payload = {
        **payload.model_dump(),
        "sap_sold_to_party": sap_customer["sold_to"],
        "sap_ship_to_party": sap_customer["ship_to"],
        "transformation_log": transformation_log,
        "universal_po_number": generate_universal_po_number(),
    }
    log.status = models.WebhookStatus.SUCCESS
    log.response_status = 200
    db.commit(); db.refresh(po)

    # Auto-create SAP Sales Order record after PO is confirmed
    try:
        db.refresh(po)  # reload relationships
        create_sap_sales_order(db, po)
    except Exception as sap_err:
        logger.error(f"SAP SO creation failed for PO {po.po_number}: {sap_err}")

    return po, None

# ---- Webhook Logs ----
def get_webhook_logs(db: Session, company_id: int = None, limit: int = 100):
    q = db.query(models.WebhookLog)
    if company_id:
        q = q.filter(models.WebhookLog.company_id == company_id)
    return q.order_by(models.WebhookLog.created_at.desc()).limit(limit).all()

# ---- ASN ----
def create_asn(db: Session, data: schemas.ASNCreate):
    po = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.id == data.po_id).first()
    if not po:
        return None, "PO not found"

    items_payload = [
        {
            "sku": item.product.sku,
            "product_name": item.product.name,
            "shipped_qty": item.fulfilled_qty,
            "unit": item.product.unit,
        }
        for item in po.items
    ]

    asn = models.ASNRecord(
        asn_number=generate_asn_number(),
        po_id=po.id,
        company_id=po.company_id,
        status=models.ASNStatus.CREATED,
        shipment_date=data.shipment_date,
        expected_delivery=data.expected_delivery,
        carrier=data.carrier,
        tracking_number=data.tracking_number,
        items_payload=items_payload,
    )
    db.add(asn); db.commit(); db.refresh(asn)
    return asn, None

def sync_asn_to_partner(db: Session, asn_id: int):
    """
    Push ASN to partner's webhook endpoint (Basic Auth, per Blinkit contract).
    In real setup: uses company.webhook_endpoint + Basic Auth credentials.
    """
    asn = db.query(models.ASNRecord).filter(models.ASNRecord.id == asn_id).first()
    if not asn:
        return None, "ASN not found"

    company = db.query(models.Company).filter(models.Company.id == asn.company_id).first()
    asn.sync_attempts += 1

    if not company.webhook_endpoint or not company.integration_active:
        asn.status = models.ASNStatus.FAILED
        asn.sync_response = "No webhook endpoint configured or integration inactive"
        db.commit(); db.refresh(asn)
        return asn, "Integration not configured"

    payload = {
        "asn_number": asn.asn_number,
        "po_number": asn.purchase_order.po_number if asn.purchase_order else None,
        "sap_order_id": asn.purchase_order.sap_order_id if asn.purchase_order else None,
        "shipment_date": asn.shipment_date.isoformat() if asn.shipment_date else None,
        "expected_delivery": asn.expected_delivery.isoformat() if asn.expected_delivery else None,
        "carrier": asn.carrier,
        "tracking_number": asn.tracking_number,
        "items": asn.items_payload,
    }

    try:
        auth = base64.b64encode(f"{company.webhook_username}:{company.webhook_password}".encode()).decode()
        response = httpx.post(
            company.webhook_endpoint,
            json=payload,
            headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
            timeout=10.0
        )
        asn.sync_response = response.text
        if response.status_code in (200, 201, 202):
            asn.status = models.ASNStatus.SYNCED
        else:
            asn.status = models.ASNStatus.FAILED
    except Exception as e:
        asn.status = models.ASNStatus.FAILED
        asn.sync_response = str(e)

    asn.updated_at = datetime.utcnow()
    db.commit(); db.refresh(asn)
    return asn, None

def get_asn_records(db: Session, company_id: int = None):
    q = db.query(models.ASNRecord)
    if company_id:
        q = q.filter(models.ASNRecord.company_id == company_id)
    return q.order_by(models.ASNRecord.created_at.desc()).all()


# ---- SAP Sales Order Creation ------------------------------------------------
def create_sap_sales_order(db: Session, po: models.PurchaseOrder) -> models.SAPSalesOrder:
    """
    Simulate (or call real SAP RFC/OData) to create a Sales Order in SAP.
    In production: replace the simulation block with actual SAP API call.
    """
    sap_customer = SAP_CUSTOMER_MAP.get(po.company.code.upper(), {
        "sold_to": "C-UNKNOWN", "ship_to": "S-UNKNOWN"
    })

    line_items = []
    for item in po.items:
        line_items.append({
            "item_number": f"{(po.items.index(item) + 1) * 10:06d}",
            "material_code": item.product.sap_material_code or item.product.sku,
            "material_description": item.product.name,
            "order_quantity": item.requested_qty,
            "fulfilled_quantity": item.fulfilled_qty,
            "sales_unit": item.product.unit,
            "unit_price": item.unit_price,
            "net_value": item.subtotal,
            "plant": "1000",
            "storage_location": "0001",
        })

    # ── Simulation: generate a realistic SAP SO response ──
    # In production replace with:
    #   response = call_sap_create_sales_order(payload)
    simulated_response = {
        "T_RETURN": [{"TYPE": "S", "ID": "V1", "NUMBER": "311", "MESSAGE": "Standard Order created"}],
        "SALESDOCUMENT": po.sap_order_id,
        "simulation": True,
        "timestamp": datetime.utcnow().isoformat(),
    }

    sap_so = models.SAPSalesOrder(
        sap_order_id=po.sap_order_id,
        po_id=po.id,
        company_id=po.company_id,
        sold_to_party=sap_customer["sold_to"],
        ship_to_party=sap_customer["ship_to"],
        order_type="ZOR",
        sales_org="1000",
        distribution_ch="10",
        division="00",
        status="CREATED",
        total_value=po.total_amount,
        currency="INR",
        line_items=line_items,
        raw_response=simulated_response,
    )
    db.add(sap_so)
    db.commit()
    db.refresh(sap_so)
    logger.info(f"SAP SO created: {sap_so.sap_order_id} for PO {po.po_number}")
    return sap_so


def get_sap_sales_orders(db: Session, company_id: int = None, po_id: int = None):
    q = db.query(models.SAPSalesOrder)
    if company_id:
        q = q.filter(models.SAPSalesOrder.company_id == company_id)
    if po_id:
        q = q.filter(models.SAPSalesOrder.po_id == po_id)
    return q.order_by(models.SAPSalesOrder.created_at.desc()).all()


def get_sap_sales_order(db: Session, sap_order_id: str):
    return db.query(models.SAPSalesOrder).filter(
        models.SAPSalesOrder.sap_order_id == sap_order_id
    ).first()


# ---- Unmapped SKU Alert Management ------------------------------------------
def flag_unmapped_sku(db: Session, partner_code: str, partner_sku: str,
                      partner_name: str = None, po_number: str = None):
    """Create or increment an UnmappedSKUAlert for manual review."""
    existing = db.query(models.UnmappedSKUAlert).filter(
        models.UnmappedSKUAlert.partner_code == partner_code,
        models.UnmappedSKUAlert.partner_sku == partner_sku,
        models.UnmappedSKUAlert.resolved == False
    ).first()
    if existing:
        existing.occurrences += 1
        existing.updated_at = datetime.utcnow()
        if po_number:
            existing.po_number = po_number
        db.commit()
        return existing
    alert = models.UnmappedSKUAlert(
        partner_code=partner_code,
        partner_sku=partner_sku,
        partner_product_name=partner_name,
        po_number=po_number,
    )
    db.add(alert); db.commit(); db.refresh(alert)
    logger.warning(f"Unmapped SKU flagged: [{partner_code}] '{partner_sku}' ({partner_name})")
    return alert


def get_unmapped_sku_alerts(db: Session, resolved: bool = None):
    q = db.query(models.UnmappedSKUAlert)
    if resolved is not None:
        q = q.filter(models.UnmappedSKUAlert.resolved == resolved)
    return q.order_by(models.UnmappedSKUAlert.updated_at.desc()).all()


def resolve_unmapped_sku(db: Session, alert_id: int, product_id: int, notes: str = None):
    """Human resolves unmapped SKU: create mapping + close alert."""
    alert = db.query(models.UnmappedSKUAlert).filter(
        models.UnmappedSKUAlert.id == alert_id
    ).first()
    if not alert:
        return None, "Alert not found"
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not product:
        return None, "Product not found"
    # Create verified mapping
    try:
        mapping = models.ProductMapping(
            partner_code=alert.partner_code,
            partner_sku=alert.partner_sku,
            partner_product_name=alert.partner_product_name,
            product_id=product.id,
            sap_material_code=product.sap_material_code,
            confidence_score=1.0,
            mapped_by="MANUAL",
            notes=notes or f"Resolved from alert #{alert_id}",
        )
        db.add(mapping); db.flush()
    except Exception as e:
        db.rollback()
        return None, str(e)
    alert.resolved = True
    alert.resolution_notes = notes
    alert.updated_at = datetime.utcnow()
    db.commit()
    return alert, None


# ---- Dashboard ----
def get_dashboard_stats(db: Session):
    from sqlalchemy import func
    total_pos = db.query(models.PurchaseOrder).count()
    pending = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.status == models.POStatus.PENDING).count()
    confirmed = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.status == models.POStatus.CONFIRMED).count()
    dispatched = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.status == models.POStatus.DISPATCHED).count()
    oos = db.query(models.PurchaseOrder).filter(models.PurchaseOrder.status == models.POStatus.OUT_OF_STOCK).count()
    revenue = db.query(func.sum(models.PurchaseOrder.total_amount)).scalar() or 0.0
    low_stock = db.query(models.Product).filter(models.Product.stock_quantity <= models.Product.reorder_level).count()
    total_webhooks = db.query(models.WebhookLog).count()
    failed_webhooks = db.query(models.WebhookLog).filter(models.WebhookLog.status == models.WebhookStatus.FAILED).count()
    total_asn = db.query(models.ASNRecord).count()
    total_sap_orders = db.query(models.SAPSalesOrder).count()
    unmapped_skus = db.query(models.UnmappedSKUAlert).filter(models.UnmappedSKUAlert.resolved == False).count()
    return {
        "total_pos": total_pos, "pending_pos": pending, "confirmed_pos": confirmed,
        "dispatched_pos": dispatched, "out_of_stock_pos": oos, "total_revenue": revenue,
        "low_stock_products": low_stock, "total_webhooks": total_webhooks,
        "failed_webhooks": failed_webhooks, "total_asn": total_asn,
        "total_sap_orders": total_sap_orders, "unmapped_skus": unmapped_skus,
    }
