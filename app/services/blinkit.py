"""
BlinkitService — Smart routing for Blinkit Vendor Supply API calls.

LOCAL  : Your Mac → Render Server (static IP) → Blinkit API ✅
PROD   : Render Server (static IP) → Blinkit API directly ✅

Auth:  api-key + x-vendor-id headers (no Bearer token)
Hosts: UAT  → https://api-uat.blinkit.com  (set BLINKIT_BASE_URL)
       Prod → update BLINKIT_BASE_URL to production endpoint

Vendor ID: 18309 (BLINKIT_VENDOR_ID env var)

Key rules from Blinkit API contract:
- All requests require api-key and x-vendor-id headers
- Quantities must be in pieces (PC / units)
- ASN invoice number must be unique per PO
- Cancel by POST to /v1/asn/{asnId}/cancel (no direct update API)
- Idempotency key supported via x-idempotency-key header
"""

import httpx
import json
import os
import logging
from typing import Optional

logger = logging.getLogger("edi.blinkit")

UAT_BASE_URL  = "https://api-uat.blinkit.com"
PROD_BASE_URL = "https://api.blinkit.com"


class BlinkitService:
    def __init__(self):
        self.env        = os.getenv("ENVIRONMENT", "local")
        self.api_key    = os.getenv("BLINKIT_API_KEY", "")
        self.vendor_id  = os.getenv("BLINKIT_VENDOR_ID", "18309")
        self.render_url = os.getenv("RENDER_URL", "").rstrip("/")
        default_url     = UAT_BASE_URL
        self.base_url   = os.getenv("BLINKIT_BASE_URL", default_url).rstrip("/")

        if self.env == "local":
            logger.info("BlinkitService: LOCAL mode — routing via Render proxy (%s)", self.render_url)
        else:
            logger.info("BlinkitService: PRODUCTION mode — calling Blinkit directly (%s)", self.base_url)

    def _url(self, path: str) -> str:
        """
        LOCAL      → https://po-integration-backend.onrender.com/api/proxy/blinkit/v1/...
        PRODUCTION → https://api-uat.blinkit.com/v1/...
        """
        path = path.lstrip("/")
        if self.env == "local":
            url = f"{self.render_url}/api/proxy/blinkit/{path}"
            logger.debug("BlinkitService [LOCAL] routing via Render: %s", url)
        else:
            url = f"{self.base_url}/{path}"
            logger.debug("BlinkitService [PROD] calling Blinkit directly: %s", url)
        return url

    def _headers(self, idempotency_key: Optional[str] = None) -> dict:
        h = {
            "Content-Type": "application/json",
            "api-key":      self.api_key,
            "x-vendor-id":  str(self.vendor_id),
        }
        if idempotency_key:
            h["x-idempotency-key"] = idempotency_key
        return h

    @staticmethod
    def _blinkit_message(body) -> str:
        """Extract a human-readable error message from a Blinkit error body."""
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                return body
        if isinstance(body, dict):
            for key in ("message", "error", "detail", "errorMessage", "description"):
                if body.get(key) and isinstance(body[key], str):
                    return body[key]
            errors = body.get("errors")
            if isinstance(errors, list) and errors:
                msgs = [e.get("message") or e.get("error", "") for e in errors if isinstance(e, dict)]
                msgs = [m for m in msgs if m]
                if msgs:
                    return "; ".join(msgs)
            return json.dumps(body)
        return str(body)

    # ── 1. List Purchase Orders ──────────────────────────────────────────────────
    async def list_purchase_orders(
        self,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        days: int = 30,
    ) -> dict:
        """
        Fetch POs from Blinkit for this vendor.
        status: OPEN | CLOSED | CANCELLED | DRAFT | EXPIRED (omit for all)
        days: how far back to look (max 90)
        """
        url = self._url("v1/purchase-orders")
        params: dict = {
            "vendor_id": self.vendor_id,
            "page":      page,
            "page_size": min(page_size, 50),
            "days":      min(days, 90),
        }
        if status:
            params["status"] = status
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params, headers=self._headers())
                resp.raise_for_status()
                logger.info("Blinkit list_purchase_orders: HTTP %s, days=%s", resp.status_code, days)
                return {"success": True, "status_code": resp.status_code, "data": resp.json()}
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit list_purchase_orders HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._blinkit_message(e.response.text)}
        except Exception as e:
            logger.error("Blinkit list_purchase_orders failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 2. Get PO Details ────────────────────────────────────────────────────────
    async def get_po_details(self, po_number: str) -> dict:
        """Fetch full PO details including line items for a specific PO number/ID."""
        url = self._url(f"v1/purchase-orders/{po_number}")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    url,
                    params={"vendor_id": self.vendor_id},
                    headers=self._headers(),
                )
                resp.raise_for_status()
                return {"success": True, "status_code": resp.status_code, "data": resp.json()}
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit get_po_details HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._blinkit_message(e.response.text)}
        except Exception as e:
            logger.error("Blinkit get_po_details failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 3. Create ASN / Invoice ──────────────────────────────────────────────────
    async def create_asn(self, payload: dict, idempotency_key: Optional[str] = None) -> dict:
        """
        Submit an ASN/invoice to Blinkit against a PO.
        Required fields in payload:
          purchaseOrderId, vendorId, invoiceNumber, invoiceDate, items[]
        Each item: productId, invoicedQty, rate, mrp (batchNumber + expiryDate optional).
        Returns asnId on success — store for cancellation reference.
        """
        url = self._url("v1/asn")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(url, json=payload, headers=self._headers(idempotency_key))
                resp.raise_for_status()
                data   = resp.json()
                asn_id = (
                    data.get("data", {}).get("asnId")
                    or data.get("asnId")
                    or data.get("data", {}).get("id")
                )
                po_id = payload.get("purchaseOrderId")
                logger.info("Blinkit ASN created: %s for PO %s", asn_id, po_id)
                return {
                    "success":     True,
                    "status_code": resp.status_code,
                    "data":        data,
                    "asn_id":      asn_id,
                }
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit create_asn HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._blinkit_message(e.response.text)}
        except Exception as e:
            logger.error("Blinkit create_asn failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 4. List ASNs ─────────────────────────────────────────────────────────────
    async def list_asns(
        self,
        po_number: str,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        """Fetch all ASNs submitted against a given Blinkit PO number/ID."""
        url = self._url("v1/asn")
        params = {
            "vendor_id":         self.vendor_id,
            "purchase_order_id": po_number,
            "page":              page,
            "page_size":         page_size,
        }
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params, headers=self._headers())
                resp.raise_for_status()
                return {"success": True, "status_code": resp.status_code, "data": resp.json()}
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit list_asns HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._blinkit_message(e.response.text)}
        except Exception as e:
            logger.error("Blinkit list_asns failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 5. Cancel ASN ────────────────────────────────────────────────────────────
    async def cancel_asn(
        self,
        asn_id: str,
        reason: str = "VENDOR_REQUEST",
    ) -> dict:
        """
        Cancel an existing Blinkit ASN by its asnId.
        Blinkit has no update API — to amend, cancel and re-submit with a new invoiceNumber.
        """
        url = self._url(f"v1/asn/{asn_id}/cancel")
        payload = {"vendorId": int(self.vendor_id), "reason": reason}
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(url, json=payload, headers=self._headers())
                resp.raise_for_status()
                logger.info("Blinkit ASN cancelled: %s (reason: %s)", asn_id, reason)
                return {"success": True, "status_code": resp.status_code, "data": resp.json()}
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit cancel_asn HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._blinkit_message(e.response.text)}
        except Exception as e:
            logger.error("Blinkit cancel_asn failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── Health Check ─────────────────────────────────────────────────────────────
    async def health_check(self) -> dict:
        """Test Blinkit API connectivity using a minimal PO list call."""
        url = self._url("v1/purchase-orders")
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    url,
                    params={"vendor_id": self.vendor_id, "page_size": 1, "page": 1},
                    headers=self._headers(),
                )
                return {
                    "reachable":   True,
                    "status_code": resp.status_code,
                    "endpoint":    self.base_url,
                    "environment": self.env,
                    "vendor_id":   self.vendor_id,
                }
        except Exception as e:
            return {
                "reachable":   False,
                "error":       str(e),
                "endpoint":    self.base_url,
                "environment": self.env,
                "vendor_id":   self.vendor_id,
            }


# ── Single instance — import this everywhere ─────────────────────────────────
blinkit_service = BlinkitService()
