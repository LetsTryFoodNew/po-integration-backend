from pydantic import BaseModel
from typing import List, Optional, Any
from datetime import datetime
from app.models import POStatus, WebhookStatus, ASNStatus

# --- Company ---
class CompanyBase(BaseModel):
    name: str
    code: str
    logo_color: Optional[str] = "#6366f1"
    contact_email: Optional[str] = None

class CompanyCreate(CompanyBase):
    pass

class CompanyIntegrationUpdate(BaseModel):
    webhook_endpoint: Optional[str] = None
    webhook_username: Optional[str] = None
    webhook_password: Optional[str] = None
    integration_active: Optional[bool] = None

class CompanyOut(CompanyBase):
    id: int
    webhook_endpoint: Optional[str] = None
    webhook_username: Optional[str] = None
    integration_active: bool
    created_at: datetime
    class Config:
        from_attributes = True

# --- Product ---
class ProductBase(BaseModel):
    sku: str
    name: str
    category: Optional[str] = None
    unit: Optional[str] = "units"
    price_per_unit: float
    stock_quantity: int
    reorder_level: Optional[int] = 50
    sap_material_code: Optional[str] = None

class ProductCreate(ProductBase):
    pass

class ProductUpdate(BaseModel):
    stock_quantity: Optional[int] = None
    price_per_unit: Optional[float] = None
    reorder_level: Optional[int] = None

class ProductOut(ProductBase):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

# --- PO Item ---
class POItemCreate(BaseModel):
    product_id: int
    requested_qty: int

class POItemOut(BaseModel):
    id: int
    product_id: int
    product: ProductOut
    requested_qty: int
    fulfilled_qty: int
    unit_price: float
    subtotal: float
    class Config:
        from_attributes = True

# --- Purchase Order ---
class POCreate(BaseModel):
    company_id: int
    notes: Optional[str] = None
    items: List[POItemCreate]

class POStatusUpdate(BaseModel):
    status: POStatus

class POOut(BaseModel):
    id: int
    po_number: str
    company_id: int
    company: CompanyOut
    status: POStatus
    total_amount: float
    notes: Optional[str]
    sap_order_id: Optional[str]
    source: str
    created_at: datetime
    updated_at: datetime
    items: List[POItemOut]
    class Config:
        from_attributes = True

# --- Inbound Webhook (Blinkit / Zepto / Swiggy style EDI) ---
class InboundPOItem(BaseModel):
    sku: str
    product_name: str
    quantity: int
    unit_price: float

class InboundPOPayload(BaseModel):
    """Real Blinkit/Zepto style inbound PO webhook payload"""
    po_number: str
    partner_code: str           # "BLK" | "ZPT" | "SWG" | "BBK"
    order_date: str
    items: List[InboundPOItem]
    notes: Optional[str] = None
    delivery_address: Optional[str] = None

class WebhookAcknowledgement(BaseModel):
    """ACK sent back to partner after receiving PO (per API contract)"""
    status: str                 # "ACCEPTED" | "REJECTED"
    po_number: str
    sap_order_id: Optional[str]
    message: str
    timestamp: str

# --- Webhook Log ---
class WebhookLogOut(BaseModel):
    id: int
    company_id: Optional[int]
    event_type: str
    source_ip: Optional[str]
    payload: Optional[Any]
    response_status: Optional[int]
    status: WebhookStatus
    po_number: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    company: Optional[CompanyOut] = None
    class Config:
        from_attributes = True

# --- ASN (Advance Shipment Notification) ---
class ASNCreate(BaseModel):
    po_id: int
    shipment_date: datetime
    expected_delivery: datetime
    carrier: str
    tracking_number: str

class ASNOut(BaseModel):
    id: int
    asn_number: str
    po_id: int
    company_id: int
    status: ASNStatus
    shipment_date: Optional[datetime]
    expected_delivery: Optional[datetime]
    carrier: Optional[str]
    tracking_number: Optional[str]
    sync_attempts: int
    sync_response: Optional[str]
    created_at: datetime
    updated_at: datetime
    company: Optional[CompanyOut] = None
    class Config:
        from_attributes = True

# --- Dashboard Stats ---
class DashboardStats(BaseModel):
    total_pos: int
    pending_pos: int
    confirmed_pos: int
    dispatched_pos: int
    out_of_stock_pos: int
    total_revenue: float
    low_stock_products: int
    total_webhooks: int
    failed_webhooks: int
    total_asn: int
    total_sap_orders: int = 0
    unmapped_skus: int = 0


# --- Product Mapping ---
class ProductMappingCreate(BaseModel):
    partner_code: str
    partner_sku: str
    partner_product_name: Optional[str] = None
    product_id: int
    sap_material_code: Optional[str] = None
    notes: Optional[str] = None

class ProductMappingOut(BaseModel):
    id: int
    partner_code: str
    partner_sku: str
    partner_product_name: Optional[str]
    product_id: int
    sap_material_code: Optional[str]
    is_active: bool
    confidence_score: float
    mapped_by: str
    notes: Optional[str]
    created_at: datetime
    product: Optional[ProductOut] = None
    class Config:
        from_attributes = True

# --- Universal PO Item (after mapping) ---
class UniversalPOItem(BaseModel):
    internal_product_id:    int
    sap_material_code:      str
    internal_product_name:  str
    partner_sku:            str
    partner_product_name:   str
    ordered_qty:            float
    unit_price:             float
    total_price:            float
    available_qty:          float
    fulfillment_status:     str     # FULL | PARTIAL | NONE
    mapping_confidence:     float
    mapped_by:              str

# --- SAP Sales Order ---
class SAPSalesOrderOut(BaseModel):
    id: int
    sap_order_id: str
    po_id: Optional[int]
    company_id: Optional[int]
    sold_to_party: Optional[str]
    ship_to_party: Optional[str]
    order_type: str
    sales_org: str
    status: str
    total_value: float
    currency: str
    line_items: Optional[Any]
    raw_response: Optional[Any]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime
    company: Optional[CompanyOut] = None
    class Config:
        from_attributes = True

# --- Unmapped SKU Alert ---
class UnmappedSKUAlertOut(BaseModel):
    id: int
    partner_code: str
    partner_sku: str
    partner_product_name: Optional[str]
    po_number: Optional[str]
    occurrences: int
    resolved: bool
    resolution_notes: Optional[str]
    created_at: datetime
    class Config:
        from_attributes = True

class UnmappedSKUResolve(BaseModel):
    product_id: int
    resolution_notes: Optional[str] = None

# --- Universal PO (standardized internal format) ---
class UniversalPOOut(BaseModel):
    universal_po_number:    str
    source_po_number:       str
    partner_code:           str
    partner_name:           str
    sap_sold_to_party:      str
    sap_ship_to_party:      str
    po_date:                datetime
    items:                  List[UniversalPOItem]
    total_ordered_value:    float
    total_fulfilled_value:  float
    fulfillment_status:     str
    transformation_log:     List[str]
