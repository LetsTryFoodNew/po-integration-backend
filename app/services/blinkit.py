"""
BlinkitService — Outbound API calls to Blinkit's partnersbiz.com partner API.

Architecture (from Blinkit API contracts):
  - PO flow is INBOUND:  Blinkit POSTs PO creation events to our webhook
    → we expose POST /api/webhook/inbound/blinkit/po
    → we ACK back to dev.partnersbiz.com/webhook/public/v1/po/acknowledgement
  - ASN flow is OUTBOUND: we POST ASNs to Blinkit
    → dev.partnersbiz.com/webhook/public/v1/asn

Auth:   api-key header + IP whitelisting (Render's static IP must be whitelisted)
Hosts:  Testing/Pre-prod → https://dev.partnersbiz.com
        Production       → https://api.partnersbiz.com

Vendor ID: 18309 (BLINKIT_VENDOR_ID env var)
"""

import httpx
import os
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("edi.blinkit")

PREPROD_BASE_URL = "https://dev.partnersbiz.com"
PROD_BASE_URL    = "https://api.partnersbiz.com"


class BlinkitService:
    def __init__(self):
        self.env        = os.getenv("ENVIRONMENT", "local")
        self.api_key    = os.getenv("BLINKIT_API_KEY", "")
        self.vendor_id  = os.getenv("BLINKIT_VENDOR_ID", "18309")
        self.render_url = os.getenv("RENDER_URL", "").rstrip("/")

        # Base URL: testing = dev.partnersbiz.com, prod = api.partnersbiz.com
        default_base    = PREPROD_BASE_URL if self.env != "production" else PROD_BASE_URL
        self.base_url   = os.getenv("BLINKIT_BASE_URL", default_base).rstrip("/")

        # Outbound endpoint paths (confirmed from Blinkit API contract docs)
        self.path_asn    = os.getenv("BLINKIT_PATH_ASN",    "webhook/public/v1/asn")
        self.path_po_ack = os.getenv("BLINKIT_PATH_PO_ACK", "webhook/public/v1/po/acknowledgement")

        if self.env == "local":
            logger.info("BlinkitService: LOCAL — outbound calls via Render proxy (%s)", self.render_url)
        else:
            logger.info("BlinkitService: PROD — calling Blinkit directly (%s)", self.base_url)

    def _url(self, path: str) -> str:
        """
        LOCAL      → https://po-integration-backend.onrender.com/api/proxy/blinkit/webhook/public/v1/...
        PRODUCTION → https://dev.partnersbiz.com/webhook/public/v1/...
        """
        path = path.lstrip("/")
        if self.env == "local":
            url = f"{self.render_url}/api/proxy/blinkit/{path}"
            logger.debug("BlinkitService [LOCAL] via Render: %s", url)
        else:
            url = f"{self.base_url}/{path}"
            logger.debug("BlinkitService [PROD] direct: %s", url)
        return url

    def _headers(self, idempotency_key: Optional[str] = None) -> dict:
        h = {
            "Content-Type": "application/json",
            "api-key":      self.api_key,
            "x-vendor-id":  str(self.vendor_id),
        }
        if idempotency_key:
            h["X-Idempotency-Key"] = idempotency_key
        return h

    @staticmethod
    def _parse_error(body) -> str:
        import json
        if isinstance(body, (bytes, str)):
            try:
                body = json.loads(body)
            except Exception:
                return str(body)
        if isinstance(body, dict):
            for key in ("message", "error", "detail", "description"):
                if body.get(key):
                    return str(body[key])
            return json.dumps(body)
        return str(body)

    # ── 1. Create ASN ─────────────────────────────────────────────────────────
    async def create_asn(self, payload: dict, idempotency_key: Optional[str] = None) -> dict:
        """
        POST an ASN/invoice to Blinkit against a PO.
        Endpoint: POST /webhook/public/v1/asn

        Required fields in payload:
          po_number, invoice_number, invoice_date, delivery_date,
          supplier_details (name, gstin, supplier_address),
          buyer_details (gstin),
          shipment_details (delivery_type),
          items[] (item_id, sku_code, batch_number, sku_description, upc,
                   quantity, mrp, unit_basic_price, unit_landing_price,
                   expiry_date, uom, tax_distribution)

        Response contains asn_id — store it for future reference.
        """
        url = self._url(self.path_asn)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(url, json=payload, headers=self._headers(idempotency_key))
                resp.raise_for_status()
                data   = resp.json()
                asn_id = (
                    data.get("asn_id")
                    or data.get("data", {}).get("asn_id")
                    or data.get("id")
                )
                logger.info("Blinkit ASN created: %s for PO %s", asn_id, payload.get("po_number"))
                return {
                    "success":     True,
                    "status_code": resp.status_code,
                    "data":        data,
                    "asn_id":      asn_id,
                }
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit create_asn HTTP %s: %s", e.response.status_code, e.response.text)
            return {
                "success":     False,
                "status_code": e.response.status_code,
                "error":       self._parse_error(e.response.text),
            }
        except Exception as e:
            logger.error("Blinkit create_asn failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 2. Acknowledge PO ─────────────────────────────────────────────────────
    async def acknowledge_po(
        self,
        po_number: str,
        status: str = "processing",
        errors: Optional[list] = None,
        warnings: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """
        POST a PO acknowledgement to Blinkit after receiving their inbound PO push.
        Endpoint: POST /webhook/public/v1/po/acknowledgement

        status: processing | accepted | partially_accepted | rejected
        Send "processing" immediately; send final status when done.
        """
        url = self._url(self.path_po_ack)
        payload = {
            "success":   status != "rejected",
            "message":   f"PO {po_number} acknowledged — {status}",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": {
                "po_status": status.upper(),
                "po_number": po_number,
                "errors":    errors or [],
                "warnings":  warnings or [],
            },
        }
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload, headers=self._headers(idempotency_key))
                resp.raise_for_status()
                logger.info("Blinkit PO ack sent: %s → %s", po_number, status)
                return {"success": True, "status_code": resp.status_code, "data": resp.json()}
        except httpx.HTTPStatusError as e:
            logger.error("Blinkit acknowledge_po HTTP %s: %s", e.response.status_code, e.response.text)
            return {
                "success":     False,
                "status_code": e.response.status_code,
                "error":       self._parse_error(e.response.text),
            }
        except Exception as e:
            logger.error("Blinkit acknowledge_po failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── Health Check ──────────────────────────────────────────────────────────
    async def health_check(self) -> dict:
        """
        Test connectivity to Blinkit's partnersbiz.com.
        HTTP 403 means reachable but our IP is not yet whitelisted — that's OK from
        our Mac (local). When called through Render proxy (whitelisted IP) we expect
        405/400/200 depending on the verb.
        """
        url = self._url(self.path_asn)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, headers=self._headers())
            reachable      = resp.status_code not in (502, 503, 504, 0)
            ip_whitelisted = resp.status_code != 403
            return {
                "reachable":       reachable,
                "ip_whitelisted":  ip_whitelisted,
                "status_code":     resp.status_code,
                "endpoint":        url,
                "base_url":        self.base_url,
                "environment":     self.env,
                "vendor_id":       self.vendor_id,
                "note": (
                    "IP not whitelisted by Blinkit — calls route via Render proxy"
                    if resp.status_code == 403 else None
                ),
            }
        except Exception as e:
            return {
                "reachable":      False,
                "ip_whitelisted": False,
                "error":          str(e),
                "endpoint":       url,
                "base_url":       self.base_url,
                "environment":    self.env,
                "vendor_id":      self.vendor_id,
            }


blinkit_service = BlinkitService()
