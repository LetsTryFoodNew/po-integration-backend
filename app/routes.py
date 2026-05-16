from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Request, Header, Response
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from app.database import get_db
from app import crud, schemas
from app.models import ZeptoASNAllocation
from app.services.blinkit import blinkit_service
from app.services.zepto import zepto_service
import base64
import httpx
import os

router = APIRouter(prefix="/api", tags=["EDI Integration"])

# ── Dashboard ──────────────────────────────────────────────────────────────────
@router.get("/dashboard", response_model=schemas.DashboardStats)
def dashboard(db: Session = Depends(get_db)):
    return crud.get_dashboard_stats(db)

# ── Companies ──────────────────────────────────────────────────────────────────
@router.get("/companies", response_model=list[schemas.CompanyOut])
def list_companies(db: Session = Depends(get_db)):
    return crud.get_companies(db)

@router.post("/companies", response_model=schemas.CompanyOut)
def add_company(data: schemas.CompanyCreate, db: Session = Depends(get_db)):
    return crud.create_company(db, data)

@router.patch("/companies/{company_id}/integration", response_model=schemas.CompanyOut)
def update_integration(company_id: int, data: schemas.CompanyIntegrationUpdate, db: Session = Depends(get_db)):
    obj = crud.update_company_integration(db, company_id, data)
    if not obj:
        raise HTTPException(404, "Company not found")
    return obj

# ── Products ───────────────────────────────────────────────────────────────────
@router.get("/products", response_model=list[schemas.ProductOut])
def list_products(db: Session = Depends(get_db)):
    return crud.get_products(db)

@router.post("/products", response_model=schemas.ProductOut)
def add_product(data: schemas.ProductCreate, db: Session = Depends(get_db)):
    return crud.create_product(db, data)

@router.patch("/products/{product_id}", response_model=schemas.ProductOut)
def update_product(product_id: int, data: schemas.ProductUpdate, db: Session = Depends(get_db)):
    obj = crud.update_product_stock(db, product_id, data)
    if not obj:
        raise HTTPException(404, "Product not found")
    return obj

# ── Purchase Orders ────────────────────────────────────────────────────────────
@router.get("/purchase-orders", response_model=list[schemas.POOut])
def list_pos(company_id: int = None, status: str = None, db: Session = Depends(get_db)):
    return crud.get_purchase_orders(db, company_id, status)

@router.get("/purchase-orders/{po_id}", response_model=schemas.POOut)
def get_po(po_id: int, db: Session = Depends(get_db)):
    po = crud.get_purchase_order(db, po_id)
    if not po:
        raise HTTPException(404, "PO not found")
    return po

@router.post("/purchase-orders", response_model=schemas.POOut)
def create_po(data: schemas.POCreate, db: Session = Depends(get_db)):
    po, err = crud.create_purchase_order(db, data)
    if err:
        raise HTTPException(400, err)
    return po

@router.patch("/purchase-orders/{po_id}/status", response_model=schemas.POOut)
def update_status(po_id: int, data: schemas.POStatusUpdate, db: Session = Depends(get_db)):
    po = crud.update_po_status(db, po_id, data.status)
    if not po:
        raise HTTPException(404, "PO not found")
    return po

# ── Inbound Webhook — Partners POST Purchase Orders to this endpoint ───────────
# This is the endpoint URL you share with Blinkit / Zepto / Swiggy tech team (Step 1)
# They call: POST /api/webhook/inbound/po  with Basic Auth
@router.post("/webhook/inbound/po", response_model=schemas.WebhookAcknowledgement)
async def inbound_po_webhook(
    payload: schemas.InboundPOPayload,
    request: Request,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    """
    Inbound EDI endpoint — Blinkit/Zepto/Swiggy posts POs here.
    Secured via HTTP Basic Auth (username = partner_code, password = shared secret).
    Returns an acknowledgement per the API contract.
    """
    from datetime import datetime as dt

    # Basic Auth validation (in production use per-partner credentials from DB)
    if authorization:
        try:
            scheme, creds = authorization.split(" ", 1)
            decoded = base64.b64decode(creds).decode("utf-8")
            username, password = decoded.split(":", 1)
        except Exception:
            raise HTTPException(401, "Invalid Authorization header")
    # For demo: allow any auth; in prod validate against company.webhook_username/password

    source_ip = request.client.host if request.client else "unknown"
    po, err = crud.process_inbound_po_webhook(db, payload, source_ip)

    if err:
        return schemas.WebhookAcknowledgement(
            status="REJECTED",
            po_number=payload.po_number,
            sap_order_id=None,
            message=err,
            timestamp=dt.utcnow().isoformat()
        )

    return schemas.WebhookAcknowledgement(
        status="ACCEPTED",
        po_number=po.po_number,
        sap_order_id=po.sap_order_id,
        message=f"PO accepted. Stock status: {po.status.value}",
        timestamp=dt.utcnow().isoformat()
    )

# Simulate inbound webhook (for testing without real partner)
@router.post("/webhook/simulate/{partner_code}")
def simulate_inbound_po(partner_code: str, db: Session = Depends(get_db)):
    """Simulate a partner sending a PO webhook — for testing/demo"""
    import random, string
    from app import models as m
    from datetime import datetime as dt

    po_number = f"{partner_code.upper()}-{''.join(random.choices(string.digits, k=8))}"
    products = db.query(m.Product).limit(3).all()
    if not products:
        raise HTTPException(400, "No products in DB. Please run seed first.")

    simulated = schemas.InboundPOPayload(
        po_number=po_number,
        partner_code=partner_code.upper(),
        order_date=dt.utcnow().isoformat(),
        items=[
            schemas.InboundPOItem(
                sku=p.sku,
                product_name=p.name,
                quantity=random.randint(10, 200),
                unit_price=p.price_per_unit
            )
            for p in products
        ],
        notes=f"Simulated PO from {partner_code.upper()} integration test"
    )

    po, err = crud.process_inbound_po_webhook(db, simulated, "127.0.0.1")
    if err:
        raise HTTPException(400, err)
    return {"message": "Simulated PO created", "po_number": po.po_number, "sap_order_id": po.sap_order_id}

# ── Webhook Logs ───────────────────────────────────────────────────────────────
@router.get("/webhook/logs", response_model=list[schemas.WebhookLogOut])
def get_webhook_logs(company_id: int = None, db: Session = Depends(get_db)):
    return crud.get_webhook_logs(db, company_id)

# ── ASN — Advance Shipment Notification ───────────────────────────────────────
@router.get("/asn", response_model=list[schemas.ASNOut])
def list_asn(company_id: int = None, db: Session = Depends(get_db)):
    return crud.get_asn_records(db, company_id)

@router.post("/asn", response_model=schemas.ASNOut)
def create_asn(data: schemas.ASNCreate, db: Session = Depends(get_db)):
    asn, err = crud.create_asn(db, data)
    if err:
        raise HTTPException(400, err)
    return asn

@router.post("/asn/{asn_id}/sync", response_model=schemas.ASNOut)
def sync_asn(asn_id: int, db: Session = Depends(get_db)):
    """Push ASN to partner's HTTP endpoint (Basic Auth) per Blinkit API contract"""
    asn, err = crud.sync_asn_to_partner(db, asn_id)
    if not asn:
        raise HTTPException(404, "ASN not found")
    return asn

# ── Product Mappings — partner SKU ↔ SAP material code ───────────────────────
@router.get("/product-mappings", response_model=list[schemas.ProductMappingOut])
def list_mappings(partner_code: str = None, db: Session = Depends(get_db)):
    """List all partner SKU → SAP product mappings"""
    return crud.get_product_mappings(db, partner_code)

@router.post("/product-mappings", response_model=schemas.ProductMappingOut)
def create_mapping(data: schemas.ProductMappingCreate, db: Session = Depends(get_db)):
    """Add a new partner SKU → internal product mapping"""
    return crud.create_product_mapping(db, data)

@router.delete("/product-mappings/{mapping_id}")
def delete_mapping(mapping_id: int, db: Session = Depends(get_db)):
    obj = crud.delete_product_mapping(db, mapping_id)
    if not obj:
        raise HTTPException(404, "Mapping not found")
    return {"message": "Deleted successfully"}

@router.get("/product-mappings/resolve")
def resolve_sku(partner_code: str, partner_sku: str, partner_name: str = None, db: Session = Depends(get_db)):
    """Test: resolve a partner SKU → internal SAP product"""
    product, mapping, confidence, log = crud.resolve_product(db, partner_code, partner_sku, partner_name)
    if not product:
        raise HTTPException(404, detail=log)
    return {
        "resolved": True,
        "confidence": confidence,
        "sap_material_code": product.sap_material_code,
        "internal_product": product.name,
        "internal_sku": product.sku,
        "log": log
    }


# ── SAP Sales Orders ───────────────────────────────────────────────────────────
@router.get("/sap-orders", response_model=list[schemas.SAPSalesOrderOut])
def list_sap_orders(company_id: int = None, po_id: int = None, db: Session = Depends(get_db)):
    """List all SAP Sales Orders created from inbound POs"""
    return crud.get_sap_sales_orders(db, company_id, po_id)

@router.get("/sap-orders/{sap_order_id}", response_model=schemas.SAPSalesOrderOut)
def get_sap_order(sap_order_id: str, db: Session = Depends(get_db)):
    so = crud.get_sap_sales_order(db, sap_order_id)
    if not so:
        raise HTTPException(404, "SAP Sales Order not found")
    return so

@router.post("/sap-orders/create-from-po/{po_id}", response_model=schemas.SAPSalesOrderOut)
def create_sap_order_from_po(po_id: int, db: Session = Depends(get_db)):
    """Manually trigger SAP Sales Order creation for an existing PO"""
    from app import models as m
    po = db.query(m.PurchaseOrder).filter(m.PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(404, "PO not found")
    # Check if already exists
    existing = crud.get_sap_sales_order(db, po.sap_order_id)
    if existing:
        return existing
    so = crud.create_sap_sales_order(db, po)
    return so


# ── Unmapped SKU Alerts ────────────────────────────────────────────────────────
@router.get("/unmapped-skus", response_model=list[schemas.UnmappedSKUAlertOut])
def list_unmapped_skus(resolved: bool = None, db: Session = Depends(get_db)):
    """List all unmapped SKU alerts (unresolved by default)"""
    return crud.get_unmapped_sku_alerts(db, resolved)

@router.post("/unmapped-skus/{alert_id}/resolve")
def resolve_unmapped_sku_alert(alert_id: int, data: schemas.UnmappedSKUResolve, db: Session = Depends(get_db)):
    """Resolve an unmapped SKU alert by mapping it to an internal product"""
    alert, err = crud.resolve_unmapped_sku(db, alert_id, data.product_id, data.resolution_notes)
    if err:
        raise HTTPException(400, err)
    return {"message": "Alert resolved and mapping created", "alert_id": alert_id}


# ── Blinkit DB debug (temporary) ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# ── BLINKIT PROXY — Local Mac calls this → Render (static IP) → Blinkit ──────
# ══════════════════════════════════════════════════════════════════════════════

@router.api_route(
    "/proxy/blinkit/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    tags=["Blinkit Proxy"],
    summary="Proxy all Blinkit API calls through this server (static IP)"
)
async def blinkit_proxy(path: str, request: Request):
    """
    Proxy endpoint — forwards any request to Blinkit Vendor API.

    LOCAL  : Your Mac → this Render server (static IP whitelisted by Blinkit) → Blinkit ✅
    PROD   : This route is still available but BlinkitService calls Blinkit directly.

    Credentials are read from Render env vars first; if not set, the calling service
    should pass them in request headers (api-key / x-vendor-id) — same pattern as
    the Zepto proxy. This means the proxy works even before Render env vars are set.

    Usage (called automatically by BlinkitService in local mode):
      GET  /api/proxy/blinkit/v1/purchase-orders?vendor_id=18309
      POST /api/proxy/blinkit/v1/asn
      POST /api/proxy/blinkit/v1/asn/{id}/cancel
    """
    # Credentials: prefer Render env vars; fall back to headers sent by the caller
    # (BlinkitService in local mode sends api-key + x-vendor-id in the request headers)
    api_key   = os.getenv("BLINKIT_API_KEY", "")   or request.headers.get("api-key", "")
    vendor_id = os.getenv("BLINKIT_VENDOR_ID", "18309") or request.headers.get("x-vendor-id", "18309")
    # Testing: dev.partnersbiz.com  |  Prod: api.partnersbiz.com
    blinkit_base = os.getenv("BLINKIT_BASE_URL", "https://dev.partnersbiz.com").rstrip("/")
    target_url   = f"{blinkit_base}/{path.lstrip('/')}"

    # Forward query params
    if request.query_params:
        target_url += f"?{request.query_params}"

    # Read body
    try:
        body = await request.json()
    except Exception:
        body = None

    # Blinkit uses api-key + x-vendor-id (not Bearer token)
    forward_headers = {
        "Content-Type":   "application/json",
        "api-key":        api_key,
        "x-vendor-id":    vendor_id,
        "X-Forwarded-By": "EDI-Integration-Proxy",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method=request.method,
                url=target_url,
                json=body,
                headers=forward_headers,
            )
        return {
            "proxied": True,
            "target_url": target_url,
            "status_code": response.status_code,
            "data": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
        }
    except httpx.ConnectError:
        raise HTTPException(502, f"Cannot reach Blinkit API at {target_url}")
    except Exception as e:
        raise HTTPException(500, f"Proxy error: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# ── ZEPTO PROXY — Local Mac calls this → Render (static IP) → Zepto API ───────
# ══════════════════════════════════════════════════════════════════════════════

@router.api_route(
    "/proxy/zepto/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    tags=["Zepto Proxy"],
    summary="Proxy all Zepto API calls through this server (static IP)"
)
async def zepto_proxy(path: str, request: Request):
    """
    Proxy endpoint — forwards any request to the Zepto Silk Route API.

    LOCAL  : Your Mac → this Render server (static IP whitelisted by Zepto) → Zepto ✅
    PROD   : This route is still available but ZeptoService calls Zepto directly.

    Render outbound IPs to whitelist with Zepto:
      74.220.48.0/24
      74.220.56.0/24

    Usage (called automatically by ZeptoService in local mode):
      GET  /api/proxy/zepto/api/v1/external/po/events?days=7
      POST /api/proxy/zepto/api/v1/external/asn
      PUT  /api/proxy/zepto/api/v1/external/po/{po_number}/amendment
    """
    # Credentials: prefer Render env vars; fall back to headers sent by caller
    # (ZeptoService in local mode sends them in the request headers)
    client_id     = os.getenv("ZEPTO_CLIENT_ID", "") or request.headers.get("X-Client-Id", "")
    client_secret = os.getenv("ZEPTO_CLIENT_SECRET", "") or request.headers.get("X-Client-Secret", "")
    zepto_base    = "https://silkroute.zeptonow.dev"   # proxy always hits QA host
    target_url    = f"{zepto_base}/{path.lstrip('/')}"

    if request.query_params:
        target_url += f"?{request.query_params}"

    try:
        body = await request.json()
    except Exception:
        body = None

    forward_headers = {
        "Content-Type":    "application/json",
        "X-Client-Id":     client_id,
        "X-Client-Secret": client_secret,
        "X-Forwarded-By":  "EDI-Integration-Proxy",
    }
    idempotency_key = request.headers.get("X-Idempotency-Key")
    if idempotency_key:
        forward_headers["X-Idempotency-Key"] = idempotency_key

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method=request.method,
                url=target_url,
                json=body,
                headers=forward_headers,
            )
        # Return Zepto's raw response transparently so ZeptoService.response.json()
        # gets the real Zepto body — keeps the data depth consistent in local + prod.
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type", "application/json"),
        )
    except httpx.ConnectError:
        raise HTTPException(502, f"Cannot reach Zepto API at {target_url}")
    except Exception as e:
        raise HTTPException(500, f"Zepto proxy error: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# ── BLINKIT INBOUND WEBHOOK — Blinkit pushes PO events here ───────────────────
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/webhook/inbound/blinkit/po", tags=["Blinkit Webhook"])
async def blinkit_po_inbound(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Blinkit pushes PO creation/update events to this endpoint.
    Webhook URL to share with Blinkit team:
      https://po-integration-backend.onrender.com/api/webhook/inbound/blinkit/po

    The response body IS the PO acknowledgement — Blinkit reads po_status from it.
    We immediately return "processing"; a final ACK can be sent later via /blinkit/po-ack.
    """
    import logging
    from app.models import WebhookLog, WebhookStatus
    from datetime import datetime as dt

    logger = logging.getLogger("edi.blinkit.webhook")

    # ── 1. Parse body (never crash on bad input) ──────────────────────────────
    try:
        body = await request.json()
    except Exception as exc:
        logger.error("Blinkit webhook: JSON parse error: %s", exc)
        body = {}

    po_number  = (body.get("po_number") or "")[:50] or None
    event_type = (body.get("type") or "PO_CREATION")[:40]

    logger.info("Blinkit inbound webhook: po_number=%s type=%s", po_number, event_type)

    # ── 2. Store in DB (never let a DB error cause a 500 to Blinkit) ──────────
    try:
        log = WebhookLog(
            event_type=f"BLINKIT_{event_type}"[:50],
            source_ip=(request.client.host if request.client else None),
            payload=body,
            po_number=po_number,
            status=WebhookStatus.PENDING,   # PENDING is safe — always in the DB enum
        )
        db.add(log)
        db.commit()
        logger.info("Blinkit PO %s stored in webhook_logs (id=%s)", po_number, log.id)
    except Exception as exc:
        logger.error("Blinkit webhook: DB write failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        # Continue — don't let a DB failure block the ACK response to Blinkit

    # ── 3. Optional: send final ACK asynchronously after we respond ───────────
    async def _send_ack():
        try:
            await blinkit_service.acknowledge_po(po_number or "unknown", "accepted")
        except Exception as exc:
            logger.warning("Blinkit PO ack background send failed: %s", exc)

    background_tasks.add_task(_send_ack)

    # ── 4. Return ACK in Blinkit's expected response format ───────────────────
    return {
        "success":   True,
        "message":   "PO received",
        "timestamp": dt.utcnow().isoformat() + "Z",
        "data": {
            "po_status": "processing",
            "po_number": po_number,
            "errors":    [],
            "warnings":  [],
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── BLINKIT API OPERATIONS — Blinkit Vendor Supply API ────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/blinkit/health", tags=["Blinkit API"])
async def blinkit_health(request: Request):
    """Test connectivity to Blinkit's partnersbiz.com (testing: dev.partnersbiz.com)."""
    result = await blinkit_service.health_check()
    env = os.getenv("ENVIRONMENT", "local")
    server_url = str(request.base_url).rstrip("/")
    return {
        "environment":  env,
        "vendor_id":    os.getenv("BLINKIT_VENDOR_ID", "18309"),
        "base_url":     blinkit_service.base_url,
        "api_key_set":  bool(os.getenv("BLINKIT_API_KEY")),
        "routing_mode": "via_render_proxy" if env == "local" else "direct",
        "connectivity": result,
        "webhook_url":  f"{server_url}/api/webhook/inbound/blinkit/po",
    }


@router.get("/blinkit/pos", tags=["Blinkit API"])
def get_blinkit_pos(request: Request, db: Session = Depends(get_db)):
    """
    Return Blinkit POs received via inbound webhook (stored in our DB).
    Blinkit PUSHES POs to us — there is no pull/list API.
    Share webhook URL with Blinkit:
      https://po-integration-backend.onrender.com/api/webhook/inbound/blinkit/po
    """
    from app.models import WebhookLog

    logs = (
        db.query(WebhookLog)
        .filter(
            WebhookLog.event_type.like("BLINKIT_%"),
            WebhookLog.event_type != "BLINKIT_TEST",
            WebhookLog.po_number.isnot(None),
            ~WebhookLog.po_number.like("TEST%"),
            ~WebhookLog.po_number.like("DEBUG%"),
        )
        .order_by(WebhookLog.created_at.desc())
        .limit(500)
        .all()
    )

    # Deduplicate by po_number — keep only the latest event per PO
    seen: dict = {}
    for log in logs:
        pn = log.po_number or ""
        if pn not in seen:
            seen[pn] = log

    def _build_po_item(item: dict) -> dict:
        tax = item.get("tax_details", {}) or {}
        igst = tax.get("igst_percentage")
        cgst = tax.get("cgst_percentage", 0) or 0
        sgst = tax.get("sgst_percentage", 0) or 0
        return {
            "productId":    str(item.get("item_id", "")),
            "skuCode":      item.get("sku_code", ""),
            "upc":          item.get("upc", ""),
            "productName":  item.get("name", ""),
            "requestedQty": item.get("units_ordered", 0),
            "rate":         item.get("basic_price", 0),
            "mrp":          item.get("mrp", 0),
            "hsnCode":      item.get("hsn_code"),        # top-level field, not in tax_details
            "gstRate":      igst if igst is not None else round(cgst + sgst, 2),
            "cgstRate":     cgst,
            "sgstRate":     sgst,
            "igstRate":     igst or 0,
        }

    from datetime import datetime as _dt
    _now = _dt.utcnow().isoformat()

    pos = []
    for log in seen.values():
        payload = log.payload or {}
        details = payload.get("details", {})
        buyer   = details.get("buyer_details", {}) or {}
        dest    = buyer.get("destination_address", {}) or {}
        items   = [_build_po_item(i) for i in details.get("item_data", [])]

        event_type = payload.get("type", "PO_CREATION")
        expiry_date = details.get("expiry_date") or ""
        # Derive PO status: CANCELLED if event says so, EXPIRED if past expiry, else RELEASED
        if "CANCEL" in event_type.upper():
            po_status = "CANCELLED"
        elif expiry_date and expiry_date[:19] < _now[:19]:
            po_status = "EXPIRED"
        else:
            po_status = "RELEASED"

        pos.append({
            "purchaseOrderId": payload.get("po_number") or log.po_number,
            "poCode":          payload.get("po_number") or log.po_number,
            "status":          po_status,
            "eventType":       event_type,
            "deliveryDate":    details.get("delivery_date"),
            "expiryDate":      details.get("expiry_date"),
            "issueDate":       details.get("issue_date"),
            "totalQty":        details.get("total_qty", 0),
            "totalAmount":     details.get("total_amount", 0),
            "warehouseName":   buyer.get("name"),
            "warehouseCode":   str(details.get("outlet_id", "")),
            "warehouseAddress": ", ".join(filter(None, [
                dest.get("line1"), dest.get("line2"),
                dest.get("city"), dest.get("state"), dest.get("postal_code"),
            ])),
            "cityName":        dest.get("city"),
            "buyerGstin":      buyer.get("gstin", ""),
            "items":           items,
            "receivedAt":      log.created_at.isoformat() if log.created_at else None,
        })

    server_url = str(request.base_url).rstrip("/")
    return {
        "success": True,
        "data": {
            "purchaseOrders": pos,
            "hasNext":        False,
            "source":         "inbound_webhook",
            "total":          len(pos),
            "webhook_url":    f"{server_url}/api/webhook/inbound/blinkit/po",
        },
    }


@router.get("/blinkit/pos/{po_number}", tags=["Blinkit API"])
def get_blinkit_po(po_number: str, db: Session = Depends(get_db)):
    """Return a specific Blinkit PO from our local webhook log."""
    from app.models import WebhookLog

    log = (
        db.query(WebhookLog)
        .filter(
            WebhookLog.event_type.like("BLINKIT_%"),
            WebhookLog.po_number == po_number,
        )
        .order_by(WebhookLog.created_at.desc())
        .first()
    )
    if not log:
        raise HTTPException(404, f"PO {po_number} not found — has Blinkit pushed it yet?")

    payload = log.payload or {}
    details = payload.get("details", {})
    buyer = details.get("buyer_details", {}) or {}
    dest  = buyer.get("destination_address", {}) or {}
    items = [
        {
            "productId":    str(item.get("item_id", "")),
            "skuCode":      item.get("sku_code", ""),
            "upc":          item.get("upc", ""),
            "productName":  item.get("name", ""),
            "requestedQty": item.get("units_ordered", 0),
            "rate":         item.get("basic_price", 0),
            "mrp":          item.get("mrp", 0),
            "hsnCode":      item.get("hsn_code"),
            "cgstRate":     (item.get("tax_details", {}) or {}).get("cgst_percentage", 0),
            "sgstRate":     (item.get("tax_details", {}) or {}).get("sgst_percentage", 0),
            "igstRate":     (item.get("tax_details", {}) or {}).get("igst_percentage", 0),
        }
        for item in details.get("item_data", [])
    ]
    return {
        "success": True,
        "data": {
            "purchaseOrderId": po_number,
            "buyerGstin":      buyer.get("gstin", ""),
            "warehouseName":   buyer.get("name"),
            "warehouseAddress": ", ".join(filter(None, [
                dest.get("line1"), dest.get("city"), dest.get("state"), dest.get("postal_code"),
            ])),
            "items": items,
        },
    }


@router.post("/blinkit/asn", tags=["Blinkit API"])
async def create_blinkit_asn(payload: dict, db: Session = Depends(get_db), idempotency_key: str = None):
    """
    Submit an ASN/invoice to Blinkit and persist per-item allocations locally.
    Blinkit has no List-ASNs API so we track qty ourselves to compute remaining.
    """
    result = await blinkit_service.create_asn(payload, idempotency_key)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Blinkit ASN creation failed"))

    from app.models import BlinkitASNAllocation
    asn_id         = result.get("asn_id") or result.get("data", {}).get("asn_id")
    po_number      = payload.get("po_number")
    invoice_number = payload.get("invoice_number")
    if asn_id and po_number:
        for item in payload.get("items", []):
            item_id = str(item.get("item_id", ""))
            qty     = int(item.get("quantity", 0))
            if item_id and qty > 0:
                try:
                    db.add(BlinkitASNAllocation(
                        asn_id=asn_id,
                        po_number=po_number,
                        item_id=item_id,
                        sku_code=item.get("sku_code", ""),
                        invoice_number=invoice_number,
                        invoiced_qty=qty,
                    ))
                except Exception:
                    pass
        try:
            db.commit()
        except Exception:
            db.rollback()

    return result


@router.get("/blinkit/asn", tags=["Blinkit API"])
def list_blinkit_asns(po_number: str = None, db: Session = Depends(get_db)):
    """Return locally-tracked Blinkit ASNs. Blinkit has no List-ASNs API."""
    from app.models import BlinkitASNAllocation
    q = db.query(BlinkitASNAllocation).filter(BlinkitASNAllocation.cancelled == False)
    if po_number:
        q = q.filter(BlinkitASNAllocation.po_number == po_number)
    rows = q.order_by(BlinkitASNAllocation.created_at.desc()).all()

    asns: dict = {}
    for row in rows:
        if row.asn_id not in asns:
            asns[row.asn_id] = {
                "asn_id":         row.asn_id,
                "po_number":      row.po_number,
                "invoice_number": row.invoice_number,
                "created_at":     row.created_at.isoformat() if row.created_at else None,
                "total_qty":      0,
                "items":          [],
            }
        asns[row.asn_id]["items"].append({
            "item_id":     row.item_id,
            "sku_code":    row.sku_code,
            "invoiced_qty": row.invoiced_qty,
        })
        asns[row.asn_id]["total_qty"] += row.invoiced_qty

    return {"success": True, "data": {"asns": list(asns.values()), "hasNext": False}}


@router.delete("/blinkit/asn/{asn_id}", tags=["Blinkit API"])
def cancel_blinkit_asn_local(asn_id: str, db: Session = Depends(get_db)):
    """
    Mark a locally-tracked Blinkit ASN as cancelled (local DB only).
    Blinkit has NO Cancel ASN API — this only frees allocated qty in our
    tracking table so you can re-submit against the same PO.
    Always returns 200. If the ASN wasn't tracked locally, success=False with a note.
    """
    from app.models import BlinkitASNAllocation
    rows = (
        db.query(BlinkitASNAllocation)
        .filter(BlinkitASNAllocation.asn_id == asn_id, BlinkitASNAllocation.cancelled == False)
        .all()
    )
    if not rows:
        return {
            "success":        False,
            "asn_id":         asn_id,
            "rows_cancelled": 0,
            "message":        (
                f"ASN '{asn_id}' is not in local tracking — it was submitted before "
                "local tracking was enabled, or is already cancelled. "
                "Contact Blinkit directly if the physical shipment needs to be recalled."
            ),
        }
    for row in rows:
        row.cancelled = True
    db.commit()
    return {
        "success":        True,
        "asn_id":         asn_id,
        "rows_cancelled": len(rows),
        "message":        (
            f"ASN {asn_id} cancelled in local DB — {len(rows)} item(s) released. "
            "Blinkit has no cancel API; contact them if shipment is already in transit."
        ),
    }


@router.get("/blinkit/po/{po_number}/sku-allocations", tags=["Blinkit API"])
def get_blinkit_sku_allocations(po_number: str, db: Session = Depends(get_db)):
    """
    Per-item_id map of already-invoiced quantities for a Blinkit PO.
    Sourced from our own DB — not from Blinkit (they have no such API).
    Response: { "po_number": "...", "allocations": { "item_id": qty } }
    """
    from app.models import BlinkitASNAllocation
    rows = db.query(BlinkitASNAllocation).filter(
        BlinkitASNAllocation.po_number == po_number,
        BlinkitASNAllocation.cancelled == False,
    ).all()
    allocations: dict[str, int] = {}
    for row in rows:
        allocations[row.item_id] = allocations.get(row.item_id, 0) + row.invoiced_qty
    return {"po_number": po_number, "allocations": allocations}


@router.post("/blinkit/po/{po_number}/amendment", tags=["Blinkit API"])
async def blinkit_po_amendment(po_number: str, payload: dict = Body(...)):
    """
    Request a PO amendment for items on a specific Blinkit PO.
    Corrects MRP, UPC, or UOM values that were wrong in the original PO.
    Endpoint: POST /webhook/public/v1/po/amendment

    Body: { request_data: [{ item_id, variants: [{ upc, mrp, uom, po_numbers }] }] }
    """
    request_data = payload.get("request_data", [])
    if not request_data:
        raise HTTPException(400, "request_data must be a non-empty list")

    # Ensure every variant includes this PO number
    for item in request_data:
        for variant in item.get("variants", []):
            if po_number not in (variant.get("po_numbers") or []):
                variant.setdefault("po_numbers", []).append(po_number)

    result = await blinkit_service.request_amendment(request_data)
    if not result["success"]:
        status = result.get("status_code", 502)
        error  = result.get("error", "Blinkit PO amendment request failed")
        if status == 404:
            error = (
                "Blinkit amendment API returned 404 — the endpoint "
                "(POST /webhook/public/v1/po/amendment) is not enabled for Vendor 18309 "
                "in the test environment. Ask Blinkit to activate it for dev.partnersbiz.com."
            )
        raise HTTPException(status, error)
    return result


@router.post("/blinkit/po-ack", tags=["Blinkit API"])
async def blinkit_po_ack(po_number: str, status: str = "accepted", idempotency_key: str = None):
    """
    Manually send a PO acknowledgement to Blinkit.
    status: processing | accepted | partially_accepted | rejected
    """
    result = await blinkit_service.acknowledge_po(po_number, status, idempotency_key=idempotency_key)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Blinkit PO ack failed"))
    return result


@router.get("/blinkit/connection-info", tags=["Blinkit API"])
def blinkit_connection_info():
    """Show current Blinkit routing and credential configuration."""
    env = os.getenv("ENVIRONMENT", "local")
    render_url = os.getenv("RENDER_URL", "NOT SET")
    return {
        "environment":        env,
        "vendor_id":          os.getenv("BLINKIT_VENDOR_ID", "18309"),
        "base_url":           blinkit_service.base_url,
        "api_key_configured": bool(os.getenv("BLINKIT_API_KEY")),
        "routing":            "Local Mac → Render Proxy → Blinkit" if env == "local" else "Render → Blinkit Direct",
        "render_url":         render_url,
        "webhook_url":        f"{render_url}/api/webhook/inbound/blinkit/po",
        "asn_endpoint":       f"{blinkit_service.base_url}/{blinkit_service.path_asn}",
        "po_ack_endpoint":    f"{blinkit_service.base_url}/{blinkit_service.path_po_ack}",
        "architecture":       "POs are INBOUND (Blinkit pushes to webhook). ASN + PO-ack are OUTBOUND.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── ZEPTO API OPERATIONS — Zepto Silk Route integration ───────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/zepto/health", tags=["Zepto API"])
async def zepto_health_check():
    """Test Zepto API connectivity (QA in local, Prod in production)."""
    result = await zepto_service.health_check()
    return {
        "environment":     os.getenv("ENVIRONMENT", "local"),
        "base_url":        zepto_service.base_url,
        "client_id_set":   bool(zepto_service.client_id),
        "client_secret_set": bool(zepto_service.client_secret),
        "connectivity":    result,
    }


@router.get("/zepto/connection-info", tags=["Zepto API"])
def zepto_connection_info():
    """Show current Zepto routing configuration."""
    env = os.getenv("ENVIRONMENT", "local")
    return {
        "environment":        env,
        "base_url":           zepto_service.base_url,
        "client_id_set":      bool(zepto_service.client_id),
        "client_secret_set":  bool(zepto_service.client_secret),
        "note": "IP whitelisting required — share your server's outbound IP with Zepto before calls will succeed",
    }


@router.get("/zepto/po-events", tags=["Zepto API"])
async def get_zepto_po_events(
    days: int = 7,
    vendor_codes: str = None,
    po_codes: str = None,
    include_all_po_events: bool = False,
    include_line_item_details: bool = False,
    page_size: int = 10,
    page_number: int = 1,
):
    """
    Fetch PO events from Zepto for the past `days` days.
    vendor_codes and po_codes are comma-separated strings (max 10 each).
    """
    result = await zepto_service.list_po_events(
        days=days,
        vendor_codes=vendor_codes.split(",") if vendor_codes else None,
        po_codes=po_codes.split(",") if po_codes else None,
        include_all_po_events=include_all_po_events,
        include_line_item_details=include_line_item_details,
        page_size=page_size,
        page_number=page_number,
    )
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto API error"))
    return result


@router.post("/zepto/asn", tags=["Zepto API"])
async def create_zepto_asn(payload: dict, db: Session = Depends(get_db), idempotency_key: str = None):
    """
    Submit an ASN / invoice to Zepto against a PO.
    Returns the Zepto-issued asnNumber — store it for potential cancellation.
    Also persists per-SKU allocations in zepto_asn_allocations so the frontend
    can compute exact remaining qty per SKU (Zepto's list_asns API never returns
    item-level breakdowns, so we track this ourselves).
    """
    result = await zepto_service.create_asn(payload, idempotency_key)
    if not result["success"]:
        status = result.get("status_code", 502)
        raise HTTPException(status, result.get("error", "Zepto ASN creation failed"))

    asn_number = result.get("asn_number")
    po_code    = payload.get("purchaseOrderDetails", {}).get("purchaseOrderNumber")
    if asn_number and po_code:
        for item in payload.get("itemDetails", []):
            sku_code = (
                item.get("productIdentifier", {})
                    .get("buyerProductIdentifier", {})
                    .get("skuCode")
            )
            qty = int(
                item.get("quantity", {})
                    .get("invoicedQuantity", {})
                    .get("amount", 0)
            )
            if sku_code and qty > 0:
                db.add(ZeptoASNAllocation(
                    asn_number=asn_number,
                    po_code=po_code,
                    sku_code=sku_code,
                    invoiced_qty=qty,
                ))
        db.commit()

    return result


@router.delete("/zepto/asn/{asn_number}", tags=["Zepto API"])
async def cancel_zepto_asn(asn_number: str, db: Session = Depends(get_db), idempotency_key: str = None):
    """
    Cancel an existing Zepto ASN.
    Marks our local per-SKU allocation rows as cancelled so the remaining-qty
    computation stays accurate after cancellation.
    To update an ASN: cancel it here, then POST /zepto/asn with a new invoiceNumber.
    """
    result = await zepto_service.cancel_asn(asn_number, idempotency_key)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto ASN cancellation failed"))

    db.query(ZeptoASNAllocation).filter(
        ZeptoASNAllocation.asn_number == asn_number
    ).update({"cancelled": True})
    db.commit()

    return result


@router.get("/zepto/asn", tags=["Zepto API"])
async def list_zepto_asns(po_code: str, page_size: int = 10, page_number: int = 1):
    """Fetch all ASNs created against a given Zepto PO code."""
    result = await zepto_service.list_asns(po_code, page_size, page_number)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto list ASNs failed"))
    return result


@router.get("/zepto/po/{po_number}/sku-allocations", tags=["Zepto API"])
def get_zepto_sku_allocations(po_number: str, db: Session = Depends(get_db)):
    """
    Returns a per-SKU map of how many pieces have already been invoiced in active
    (non-cancelled) ASNs for this PO — sourced from our own DB, not from Zepto.

    Zepto's List-ASNs API returns only total ASN qty with no per-SKU breakdown.
    We record the per-SKU quantities ourselves each time POST /zepto/asn succeeds,
    and clear them on cancellation, so this endpoint is always accurate.

    Response: { "po_code": "P365999", "allocations": { "SKU123": 5, "SKU456": 3 } }
    """
    rows = (
        db.query(ZeptoASNAllocation)
        .filter(
            ZeptoASNAllocation.po_code   == po_number,
            ZeptoASNAllocation.cancelled == False,
        )
        .all()
    )
    allocations: dict[str, int] = {}
    for row in rows:
        allocations[row.sku_code] = allocations.get(row.sku_code, 0) + row.invoiced_qty
    return {"po_code": po_number, "allocations": allocations}


@router.get("/zepto/po/{po_number}/pdf", tags=["Zepto API"])
async def get_zepto_po_pdf(po_number: str):
    """
    Fetch a fresh pre-signed PDF URL for a Zepto PO and redirect the browser to it.

    Pre-signed S3 URLs embed an AWS STS token that expires independently of the
    URL's own X-Amz-Expires field — sometimes within hours.  Calling this endpoint
    on every click guarantees a fresh URL, so the browser never hits ExpiredToken.
    """
    result = await zepto_service.list_po_events(
        days=45,
        po_codes=[po_number],
        include_line_item_details=False,
        page_size=1,
        page_number=1,
    )
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto API error"))

    orders = result.get("data", {}).get("data", {}).get("purchaseOrders", [])
    if not orders:
        raise HTTPException(404, f"PO {po_number} not found in Zepto (searched last 45 days)")

    po = orders[0]
    pdf_url = po.get("expiringUrlForPoPDF") or po.get("expiringPoPdfLink")
    if not pdf_url:
        raise HTTPException(404, f"No PDF available for PO {po_number}")

    return RedirectResponse(url=pdf_url, status_code=302)


@router.post("/zepto/po/{po_number}/amendment", tags=["Zepto API"])
async def request_zepto_po_amendment(po_number: str, payload: dict, idempotency_key: str = None):
    """
    Submit a PO amendment request to Zepto.
    Supported attributeNames: MRP, BASE_PRICE, EAN, CASE_SIZE, EXPIRY_DATE.
    """
    result = await zepto_service.request_po_amendment(po_number, payload, idempotency_key)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto amendment request failed"))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ── EMAIL PO LAYER — ingest Purchase Orders that arrive via email ──────────────
# ══════════════════════════════════════════════════════════════════════════════

# Shared processor — also used by the Gmail poller
from app.services.po_processor import process_email_log as _process_email_log


@router.post("/email/inbound", tags=["Email PO"])
async def email_inbound(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Receive an inbound email from SendGrid Inbound Parse, Mailgun, or Postmark.

    All three providers POST to a webhook URL — this endpoint accepts any of:
      • SendGrid: multipart/form-data  (fields: from, subject, text, html)
      • Mailgun:  application/json     (fields: sender, subject, body-plain, body-html)
      • Postmark: application/json     (fields: From, Subject, TextBody, HtmlBody)
      • Manual:   application/json     (fields: sender_email, subject, body_text, body_html)

    Configure your email provider's inbound route to POST to:
      https://po-integration-backend.onrender.com/api/email/inbound

    For Gmail / custom mail: forward the email to a SendGrid inbound address
    that webhooks here, or use a service like Zapier.
    """
    import logging as lg
    from app.models import EmailPOLog, EmailParseStatus

    logger_e = lg.getLogger("edi.email.inbound")

    content_type = request.headers.get("content-type", "")
    sender = subject = body_text = body_html = ""

    try:
        if "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
            form = await request.form()
            sender     = str(form.get("from") or form.get("sender") or "")
            subject    = str(form.get("subject") or "")
            body_text  = str(form.get("text") or form.get("body-plain") or "")
            body_html  = str(form.get("html") or form.get("body-html") or "")
        else:
            body = await request.json()
            # Support SendGrid JSON, Mailgun, Postmark, and our own test schema
            sender    = (body.get("from") or body.get("sender") or body.get("From") or body.get("sender_email") or "")
            subject   = (body.get("subject") or body.get("Subject") or "")
            body_text = (body.get("text") or body.get("body-plain") or body.get("TextBody") or body.get("body_text") or "")
            body_html = (body.get("html") or body.get("body-html") or body.get("HtmlBody") or body.get("body_html") or "")
    except Exception as exc:
        logger_e.error("Email inbound parse error: %s", exc)
        return {"status": "error", "message": "Could not parse request body"}

    logger_e.info("Email inbound: from=%s subject=%s", sender[:60], subject[:80])

    log = EmailPOLog(
        sender_email = sender[:200] if sender else None,
        subject      = subject[:500] if subject else None,
        body_text    = body_text or None,
        body_html    = body_html or None,
        parse_status = EmailParseStatus.PENDING,
        raw_payload  = {"content_type": content_type},
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    # Process in background so we respond to the email provider immediately
    def _bg_process(log_id: int):
        from app.database import SessionLocal
        from app.services.po_processor import process_email_log
        bg_db = SessionLocal()
        try:
            process_email_log(bg_db, log_id)
        finally:
            bg_db.close()

    background_tasks.add_task(_bg_process, log.id)

    return {
        "status":   "received",
        "message":  "Email queued for PO parsing",
        "log_id":   log.id,
        "subject":  subject[:80],
    }


@router.post("/email/test", tags=["Email PO"], response_model=schemas.EmailPOLogOut)
async def test_email_po(payload: schemas.EmailPOTest, db: Session = Depends(get_db)):
    """
    Simulate an inbound email PO for testing — parses synchronously and returns result.
    Useful for testing Claude parsing without a real email provider.
    """
    from app.models import EmailPOLog, EmailParseStatus

    log = EmailPOLog(
        sender_email = payload.sender_email,
        subject      = payload.subject,
        body_text    = payload.body_text,
        body_html    = payload.body_html,
        parse_status = EmailParseStatus.PENDING,
        raw_payload  = {"source": "test_endpoint"},
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    _process_email_log(db, log.id)
    db.refresh(log)
    return log


@router.get("/email/pos", tags=["Email PO"], response_model=list[schemas.EmailPOLogOut])
def list_email_pos(
    parse_status: str = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """List all emails received with PO data — filtered by parse_status if provided."""
    from app.models import EmailPOLog

    q = db.query(EmailPOLog)
    if parse_status:
        q = q.filter(EmailPOLog.parse_status == parse_status.upper())
    return q.order_by(EmailPOLog.created_at.desc()).limit(limit).all()


@router.post("/email/pos/{log_id}/reprocess", tags=["Email PO"], response_model=schemas.EmailPOLogOut)
def reprocess_email_po(log_id: int, db: Session = Depends(get_db)):
    """
    Manually re-trigger Claude parsing for a PENDING or FAILED email PO.
    Useful when the ANTHROPIC_API_KEY was not yet configured at ingest time.
    """
    from app.models import EmailPOLog

    log = db.query(EmailPOLog).filter(EmailPOLog.id == log_id).first()
    if not log:
        raise HTTPException(404, "Email log not found")
    if log.parse_status == "PARSED" and log.po_id:
        raise HTTPException(400, "Email already parsed successfully — PO already created")

    _process_email_log(db, log_id)
    db.refresh(log)
    return log


@router.get("/email/pos/{log_id}", tags=["Email PO"], response_model=schemas.EmailPOLogOut)
def get_email_po(log_id: int, db: Session = Depends(get_db)):
    """Get a single email PO log entry."""
    from app.models import EmailPOLog

    log = db.query(EmailPOLog).filter(EmailPOLog.id == log_id).first()
    if not log:
        raise HTTPException(404, "Email log not found")
    return log


# ══════════════════════════════════════════════════════════════════════════════
# ── GMAIL POLLER — pull POs directly from Gmail labels ────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/email/gmail-status", tags=["Email PO"])
def gmail_status():
    """
    Test Gmail IMAP connectivity and return the list of labels visible in the inbox.
    Uses GMAIL_ADDRESS + GMAIL_APP_PASSWORD from .env.
    """
    from app.services.gmail_poller import test_connection
    return test_connection()


@router.post("/email/poll-gmail", tags=["Email PO"])
def poll_gmail(
    days_back:     int = 30,
    max_per_label: int = 50,
    db: Session = Depends(get_db),
):
    """
    Poll all configured Gmail labels and import new PO emails.

    - days_back:     How many calendar days back to search (default 30)
    - max_per_label: Max emails to import per label per run (default 50)

    Already-imported emails are skipped automatically (deduplication by Gmail Message-ID).
    Each new email is passed through Claude AI to extract PO details,
    then a PurchaseOrder is created in the system with source='EMAIL'.
    """
    from app.services.gmail_poller import poll_all_labels
    result = poll_all_labels(db, days_back=days_back, max_per_label=max_per_label)
    return result
