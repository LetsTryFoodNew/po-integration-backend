from sqlalchemy import Column, Integer, String, Float, DateTime, Enum, ForeignKey, Text, JSON, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base
import enum

class POStatus(str, enum.Enum):
    PENDING = "PENDING"
    STOCK_AVAILABLE = "STOCK_AVAILABLE"
    STOCK_PARTIAL = "STOCK_PARTIAL"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    CONFIRMED = "CONFIRMED"
    DISPATCHED = "DISPATCHED"

class WebhookStatus(str, enum.Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"

class ASNStatus(str, enum.Enum):
    CREATED = "CREATED"
    SYNCED = "SYNCED"
    FAILED = "FAILED"

class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    code = Column(String(20), unique=True, nullable=False)
    logo_color = Column(String(20), default="#6366f1")
    contact_email = Column(String(150))
    # Real integration fields (Blinkit 4-step setup)
    webhook_endpoint = Column(String(300), nullable=True)   # Their HTTP endpoint to receive ASN/ACK
    webhook_username = Column(String(100), nullable=True)   # Basic Auth username
    webhook_password = Column(String(200), nullable=True)   # Basic Auth password
    integration_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    purchase_orders = relationship("PurchaseOrder", back_populates="company")
    webhook_logs = relationship("WebhookLog", back_populates="company")
    asn_records = relationship("ASNRecord", back_populates="company")

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(50), unique=True, nullable=False)
    name = Column(String(200), nullable=False)
    category = Column(String(100))
    unit = Column(String(20), default="units")
    price_per_unit = Column(Float, nullable=False)
    stock_quantity = Column(Integer, default=0)
    reorder_level = Column(Integer, default=50)
    sap_material_code = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    po_items = relationship("POItem", back_populates="product")
    mappings = relationship("ProductMapping", back_populates="product")

class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"
    id = Column(Integer, primary_key=True, index=True)
    po_number = Column(String(50), unique=True, nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"))
    status = Column(Enum(POStatus), default=POStatus.PENDING)
    total_amount = Column(Float, default=0.0)
    notes = Column(Text, nullable=True)
    sap_order_id = Column(String(50), nullable=True)
    # Track if this PO came via inbound webhook (real EDI)
    source = Column(String(20), default="MANUAL")  # MANUAL | WEBHOOK
    raw_payload = Column(JSON, nullable=True)       # original partner payload
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    company = relationship("Company", back_populates="purchase_orders")
    items = relationship("POItem", back_populates="purchase_order")
    asn_records = relationship("ASNRecord", back_populates="purchase_order")

class POItem(Base):
    __tablename__ = "po_items"
    id = Column(Integer, primary_key=True, index=True)
    po_id = Column(Integer, ForeignKey("purchase_orders.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    requested_qty = Column(Integer, nullable=False)
    fulfilled_qty = Column(Integer, default=0)
    unit_price = Column(Float, nullable=False)
    subtotal = Column(Float, default=0.0)
    purchase_order = relationship("PurchaseOrder", back_populates="items")
    product = relationship("Product", back_populates="po_items")

# ── Webhook Log: every inbound PO hit from Zepto / Blinkit / Swiggy ──
class WebhookLog(Base):
    __tablename__ = "webhook_logs"
    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True)
    event_type = Column(String(50))          # PO_CREATED, PO_CANCELLED, etc.
    source_ip = Column(String(50), nullable=True)
    headers = Column(JSON, nullable=True)
    payload = Column(JSON, nullable=True)
    response_status = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    status = Column(Enum(WebhookStatus), default=WebhookStatus.PENDING)
    po_number = Column(String(50), nullable=True)    # extracted po number
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    company = relationship("Company", back_populates="webhook_logs")

# ── ASN Record: outbound Advance Shipment Notification sent to partners ──
class ASNRecord(Base):
    __tablename__ = "asn_records"
    id = Column(Integer, primary_key=True, index=True)
    asn_number = Column(String(50), unique=True, nullable=False)
    po_id = Column(Integer, ForeignKey("purchase_orders.id"))
    company_id = Column(Integer, ForeignKey("companies.id"))
    status = Column(Enum(ASNStatus), default=ASNStatus.CREATED)
    shipment_date = Column(DateTime, nullable=True)
    expected_delivery = Column(DateTime, nullable=True)
    carrier = Column(String(100), nullable=True)
    tracking_number = Column(String(100), nullable=True)
    items_payload = Column(JSON, nullable=True)      # line items being shipped
    sync_response = Column(Text, nullable=True)      # response from partner webhook
    sync_attempts = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    purchase_order = relationship("PurchaseOrder", back_populates="asn_records")
    company = relationship("Company", back_populates="asn_records")


# ── Product Mapping: maps each partner's SKU → your internal SAP product ──
class ProductMapping(Base):
    """
    Master mapping table.
    Example:
      Blinkit "BLK-NK-001"  "Let's Try Namkin"   → Product id=5, SAP: MAT-10045
      Zepto   "ZPT-NK-9821" "Let's Try Namkeen"  → Product id=5, SAP: MAT-10045
      Swiggy  "SWG-SNCK-442" "LT Namkeen 200g"   → Product id=5, SAP: MAT-10045
    """
    __tablename__ = "product_mappings"

    id                   = Column(Integer, primary_key=True, index=True)
    partner_code         = Column(String(50), nullable=False)   # "BLINKIT" | "ZEPTO" | "SWIGGY"
    partner_sku          = Column(String(100), nullable=False)  # Partner's own item ID/SKU
    partner_product_name = Column(String(255), nullable=True)   # Partner's product name (reference)
    product_id           = Column(Integer, ForeignKey("products.id"), nullable=False)
    sap_material_code    = Column(String(50), nullable=True)    # SAP material code e.g. MAT-10045
    is_active            = Column(Boolean, default=True)
    confidence_score     = Column(Float, default=1.0)           # 1.0=manual verified, <1.0=auto
    mapped_by            = Column(String(20), default="MANUAL") # MANUAL | AUTO
    notes                = Column(Text, nullable=True)
    created_at           = Column(DateTime, default=datetime.utcnow)
    updated_at           = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("Product", back_populates="mappings")

    __table_args__ = (
        UniqueConstraint("partner_code", "partner_sku", name="uq_partner_sku"),
    )


# ── SAP Sales Order: tracks every SO created/simulated in SAP ──────────────────
class SAPSalesOrder(Base):
    """
    Full audit record for every SAP Sales Order created from an inbound PO.
    In a real integration this would call SAP RFC / OData API.
    """
    __tablename__ = "sap_sales_orders"

    id               = Column(Integer, primary_key=True, index=True)
    sap_order_id     = Column(String(50), unique=True, nullable=False)   # e.g. SAP-1234567
    po_id            = Column(Integer, ForeignKey("purchase_orders.id"), nullable=True)
    company_id       = Column(Integer, ForeignKey("companies.id"), nullable=True)
    sold_to_party    = Column(String(50), nullable=True)   # SAP Customer (e.g. C-10001)
    ship_to_party    = Column(String(50), nullable=True)
    order_type       = Column(String(20), default="ZOR")   # SAP order type
    sales_org        = Column(String(20), default="1000")
    distribution_ch  = Column(String(20), default="10")
    division         = Column(String(20), default="00")
    status           = Column(String(30), default="CREATED")  # CREATED | SENT | CONFIRMED | ERROR
    total_value      = Column(Float, default=0.0)
    currency         = Column(String(10), default="INR")
    line_items       = Column(JSON, nullable=True)           # full SO line items w/ SAP mat codes
    raw_response     = Column(JSON, nullable=True)           # SAP API response (or simulation)
    error_message    = Column(Text, nullable=True)
    created_at       = Column(DateTime, default=datetime.utcnow)
    updated_at       = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    purchase_order   = relationship("PurchaseOrder", foreign_keys=[po_id])
    company          = relationship("Company", foreign_keys=[company_id])


# ── Blinkit ASN Allocation: per-item qty tracker (Blinkit has no List ASNs API) ──────────────
class BlinkitASNAllocation(Base):
    """
    Tracks how many units of each item were invoiced in every Blinkit ASN
    created via this system. Blinkit has no List-ASNs API, so without this
    table we cannot compute remaining qty and would allow over-invoicing.

    Lifecycle:
      - Row inserted when POST /blinkit/asn succeeds.
      - cancelled=True if ASN is voided (future: if Blinkit adds cancel API).
      - GET /blinkit/po/{po_number}/sku-allocations sums non-cancelled rows.
    """
    __tablename__ = "blinkit_asn_allocations"

    id           = Column(Integer, primary_key=True, index=True)
    asn_id       = Column(String(100), nullable=False, index=True)
    po_number    = Column(String(50),  nullable=False, index=True)
    item_id      = Column(String(100), nullable=False)
    sku_code     = Column(String(100), nullable=True)
    invoice_number = Column(String(100), nullable=True)
    invoiced_qty = Column(Integer,     nullable=False)
    cancelled    = Column(Boolean,     default=False)
    created_at   = Column(DateTime,    default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("asn_id", "item_id", name="uq_blinkit_asn_item"),
    )


# ── Zepto ASN Allocation: per-SKU qty tracker (Zepto's list_asns never returns itemDetails) ──
class ZeptoASNAllocation(Base):
    """
    Tracks exactly how many pieces of each SKU were invoiced in every Zepto ASN
    created via this system.  Zepto's List-ASNs API returns only the total ASN
    qty — no per-SKU breakdown — so without this table we can't compute
    remaining qty per SKU and would let users enter amounts that Zepto rejects.

    Lifecycle:
      - Row inserted when POST /zepto/asn succeeds.
      - cancelled=True when DELETE /zepto/asn/{asn_number} succeeds.
      - GET /zepto/po/{po_code}/sku-allocations sums non-cancelled rows per SKU.
    """
    __tablename__ = "zepto_asn_allocations"

    id           = Column(Integer, primary_key=True, index=True)
    asn_number   = Column(String(100), nullable=False, index=True)
    po_code      = Column(String(50),  nullable=False, index=True)
    sku_code     = Column(String(100), nullable=False)
    invoiced_qty = Column(Integer,     nullable=False)
    cancelled    = Column(Boolean,     default=False)
    created_at   = Column(DateTime,    default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("asn_number", "sku_code", name="uq_zepto_asn_sku"),
    )


# ── Unmapped SKU Alert: flags unknown partner SKUs for human review ─────────────
class UnmappedSKUAlert(Base):
    __tablename__ = "unmapped_sku_alerts"

    id                   = Column(Integer, primary_key=True, index=True)
    partner_code         = Column(String(50), nullable=False)
    partner_sku          = Column(String(100), nullable=False)
    partner_product_name = Column(String(255), nullable=True)
    po_number            = Column(String(50), nullable=True)
    occurrences          = Column(Integer, default=1)
    resolved             = Column(Boolean, default=False)
    resolution_notes     = Column(Text, nullable=True)
    created_at           = Column(DateTime, default=datetime.utcnow)
    updated_at           = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ── Email PO Log: every PO that arrives via email ──────────────────────────────
class EmailParseStatus(str, enum.Enum):
    PENDING = "PENDING"
    PARSED  = "PARSED"
    FAILED  = "FAILED"

class EmailPOLog(Base):
    """
    Stores every inbound email that contains (or might contain) a Purchase Order.
    Claude AI parses the email body to extract PO details.
    On success, a PurchaseOrder row is created with source='EMAIL'.
    """
    __tablename__ = "email_po_logs"

    id             = Column(Integer, primary_key=True, index=True)
    sender_email   = Column(String(200), nullable=True)
    subject        = Column(String(500), nullable=True)
    body_text      = Column(Text, nullable=True)
    body_html      = Column(Text, nullable=True)
    parse_status   = Column(Enum(EmailParseStatus), default=EmailParseStatus.PENDING)
    po_number      = Column(String(50), nullable=True, index=True)
    partner_code   = Column(String(50), nullable=True)
    parsed_data    = Column(JSON, nullable=True)   # Claude's extracted PO structure
    po_id          = Column(Integer, ForeignKey("purchase_orders.id"), nullable=True)
    error_message  = Column(Text, nullable=True)
    raw_payload    = Column(JSON, nullable=True)   # raw webhook body from email provider
    created_at     = Column(DateTime, default=datetime.utcnow)

    purchase_order = relationship("PurchaseOrder", foreign_keys=[po_id])
