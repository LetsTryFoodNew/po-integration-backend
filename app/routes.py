from fastapi import APIRouter, Depends, HTTPException, Request, Header
from sqlalchemy.orm import Session
from app.database import get_db
from app import crud, schemas
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
    Proxy endpoint — forwards any request to Blinkit API.

    LOCAL  : Your Mac → this server (Render static IP) → Blinkit ✅
    PROD   : This route still works but direct calls are used instead.

    Usage:
      POST /api/proxy/blinkit/vendor/asn       → Blinkit ASN endpoint
      POST /api/proxy/blinkit/vendor/po/ack    → Blinkit PO acknowledgement
      GET  /api/proxy/blinkit/vendor/po/123    → Get PO details
    """
    blinkit_base = os.getenv("BLINKIT_BASE_URL", "https://api.blinkit.com").rstrip("/")
    api_key      = os.getenv("BLINKIT_API_KEY", "")
    target_url   = f"{blinkit_base}/{path.lstrip('/')}"

    # Forward query params
    if request.query_params:
        target_url += f"?{request.query_params}"

    # Read body
    try:
        body = await request.json()
    except Exception:
        body = None

    # Forward all headers except host, add auth
    forward_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
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
        return {
            "proxied":     True,
            "target_url":  target_url,
            "status_code": response.status_code,
            "data": (
                response.json()
                if response.headers.get("content-type", "").startswith("application/json")
                else response.text
            ),
        }
    except httpx.ConnectError:
        raise HTTPException(502, f"Cannot reach Zepto API at {target_url}")
    except Exception as e:
        raise HTTPException(500, f"Zepto proxy error: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# ── BLINKIT API OPERATIONS — Used by your application logic ───────────────────
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/blinkit/health", tags=["Blinkit API"])
async def blinkit_health_check():
    """
    Test if Blinkit API is reachable.
    Shows whether routing via Render proxy (local) or directly (production).
    """
    result = await blinkit_service.health_check()
    return {
        "environment": os.getenv("ENVIRONMENT", "local"),
        "routing_mode": "via_render_proxy" if os.getenv("ENVIRONMENT", "local") == "local" else "direct_to_blinkit",
        "render_url": os.getenv("RENDER_URL", "not_set"),
        "blinkit_base": os.getenv("BLINKIT_BASE_URL", "not_set"),
        "api_key_set": bool(os.getenv("BLINKIT_API_KEY")),
        "connectivity": result,
    }


@router.post("/blinkit/send-asn/{asn_id}", tags=["Blinkit API"])
async def send_asn_to_blinkit(asn_id: int, db: Session = Depends(get_db)):
    """
    Send an ASN to Blinkit using BlinkitService (smart routing).
    Works from local Mac AND production server.
    """
    from app import models as m
    asn = db.query(m.ASNRecord).filter(m.ASNRecord.id == asn_id).first()
    if not asn:
        raise HTTPException(404, "ASN not found")

    po = db.query(m.PurchaseOrder).filter(m.PurchaseOrder.id == asn.po_id).first()

    payload = {
        "asn_number": asn.asn_number,
        "po_number": po.po_number if po else None,
        "sap_order_id": po.sap_order_id if po else None,
        "shipment_date": asn.shipment_date.isoformat() if asn.shipment_date else None,
        "expected_delivery": asn.expected_delivery.isoformat() if asn.expected_delivery else None,
        "carrier": asn.carrier,
        "tracking_number": asn.tracking_number,
        "items": asn.items_payload or [],
    }

    result = await blinkit_service.send_asn(payload)
    return {
        "asn_number": asn.asn_number,
        "blinkit_response": result,
        "routing": "via_render_proxy" if os.getenv("ENVIRONMENT", "local") == "local" else "direct",
    }


@router.post("/blinkit/acknowledge-po/{po_id}", tags=["Blinkit API"])
async def acknowledge_po_to_blinkit(po_id: int, db: Session = Depends(get_db)):
    """
    Send PO acknowledgement back to Blinkit.
    Automatically called after inbound webhook PO is processed.
    """
    from app import models as m
    po = db.query(m.PurchaseOrder).filter(m.PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(404, "PO not found")

    # Map internal status to Blinkit status
    status_map = {
        "STOCK_AVAILABLE": "ACCEPTED",
        "STOCK_PARTIAL":   "PARTIAL",
        "OUT_OF_STOCK":    "REJECTED",
        "CONFIRMED":       "ACCEPTED",
    }
    blinkit_status = status_map.get(po.status.value, "ACCEPTED")

    result = await blinkit_service.send_po_acknowledgement(
        po_number=po.po_number,
        sap_order_id=po.sap_order_id or "",
        status=blinkit_status,
    )
    return {
        "po_number": po.po_number,
        "status_sent": blinkit_status,
        "blinkit_response": result,
    }


@router.get("/blinkit/pending-pos", tags=["Blinkit API"])
async def get_blinkit_pending_pos():
    """
    Pull pending POs from Blinkit (poll mode).
    Use this if Blinkit sends POs by pull instead of webhook push.
    """
    result = await blinkit_service.get_pending_pos()
    return result


@router.get("/blinkit/connection-info", tags=["Blinkit API"])
def blinkit_connection_info():
    """
    Show current Blinkit routing configuration.
    Use this to verify your setup before sharing with Blinkit team.
    """
    env = os.getenv("ENVIRONMENT", "local")
    render_url = os.getenv("RENDER_URL", "NOT SET — deploy to Render first")
    api_key = os.getenv("BLINKIT_API_KEY", "")

    return {
        "environment": env,
        "routing_mode": "Local Mac → Render Proxy → Blinkit" if env == "local" else "Render Server → Blinkit (Direct)",
        "your_static_ip_source": "Render.com outbound IP" if env == "production" else "Will use Render's IP via proxy",
        "render_server_url": render_url,
        "blinkit_api_key_configured": bool(api_key),
        "inbound_webhook_url": f"{render_url}/api/webhook/inbound/po",
        "what_to_send_blinkit_team": {
            "webhook_endpoint": f"{render_url}/api/webhook/inbound/po",
            "auth_type": "Basic Auth",
            "username": "edi_vendor",
            "note": "Get static IP from Render dashboard → Settings → Outbound IP"
        }
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
async def create_zepto_asn(payload: dict, idempotency_key: str = None):
    """
    Submit an ASN / invoice to Zepto against a PO.
    Returns the Zepto-issued asnNumber — store it for potential cancellation.
    Payload must match the Zepto ASN creation contract (see API Externalisation Contracts v12).
    On 5XX, retry with the same idempotency_key.
    """
    result = await zepto_service.create_asn(payload, idempotency_key)
    if not result["success"]:
        status = result.get("status_code", 502)
        raise HTTPException(status, result.get("error", "Zepto ASN creation failed"))
    return result


@router.delete("/zepto/asn/{asn_number}", tags=["Zepto API"])
async def cancel_zepto_asn(asn_number: str, idempotency_key: str = None):
    """
    Cancel an existing Zepto ASN.
    To update an ASN: cancel it here, then POST /zepto/asn with a new invoiceNumber.
    """
    result = await zepto_service.cancel_asn(asn_number, idempotency_key)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto ASN cancellation failed"))
    return result


@router.get("/zepto/asn", tags=["Zepto API"])
async def list_zepto_asns(po_code: str, page_size: int = 10, page_number: int = 1):
    """Fetch all ASNs created against a given Zepto PO code."""
    result = await zepto_service.list_asns(po_code, page_size, page_number)
    if not result["success"]:
        raise HTTPException(result.get("status_code", 502), result.get("error", "Zepto list ASNs failed"))
    return result


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
