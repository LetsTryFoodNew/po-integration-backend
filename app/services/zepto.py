"""
ZeptoService — Smart routing for Zepto Silk Route API calls.

LOCAL  : Your Mac → Render Server (static IP) → Zepto API ✅
PROD   : Render Server (static IP) → Zepto API directly ✅

This solves the IP whitelisting problem: your local Mac has a dynamic IP
that Zepto will block, but Render has static IPs (74.220.48.0/24 and
74.220.56.0/24) that are already whitelisted with Zepto.

Auth:  X-Client-Id + X-Client-Secret headers (not Bearer token)
Hosts: QA   → silkroute.zeptonow.dev
       Prod → silkroute.zepto.co.in

Key rules from API contract v12:
- All write APIs require X-Idempotency-Key header
- Rate limit: 60 RPM per clientId per API
- Quantities must be in pieces (PC), not case sizes
- No ASN update API — cancel + recreate with a new invoiceNumber
- Use eventId as idempotency key when polling PO events
- PO PDF links expire in ~7 days — download promptly
- All timestamps are UTC
"""

import httpx
import json
import os
import uuid
import logging
from typing import Optional

logger = logging.getLogger("edi.zepto")

QA_BASE_URL   = "https://silkroute.zeptonow.dev"
PROD_BASE_URL = "https://silkroute.zepto.co.in"


class ZeptoService:
    def __init__(self):
        self.env           = os.getenv("ENVIRONMENT", "local")
        self.client_id     = os.getenv("ZEPTO_CLIENT_ID", "")
        self.client_secret = os.getenv("ZEPTO_CLIENT_SECRET", "")
        self.render_url    = os.getenv("RENDER_URL", "").rstrip("/")
        default_url        = QA_BASE_URL if self.env == "local" else PROD_BASE_URL
        self.base_url      = os.getenv("ZEPTO_BASE_URL", default_url).rstrip("/")

        if self.env == "local":
            logger.info("ZeptoService: LOCAL mode — routing via Render proxy (%s)", self.render_url)
        else:
            logger.info("ZeptoService: PRODUCTION mode — calling Zepto directly (%s)", self.base_url)

    def _unwrap(self, raw) -> dict:
        """Strip the Render proxy wrapper {proxied: True, data: <zepto>} if present."""
        if isinstance(raw, dict) and raw.get("proxied"):
            inner = raw.get("data")
            return inner if isinstance(inner, dict) else {}
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _zepto_message(body) -> str:
        """
        Extract a clean, human-readable message from a Zepto error body.

        Zepto error shapes seen in the wild:
          {"errors": [{"code": 400, "error": "Invalid record. ..."}], "data": null}
          {"message": "...", "statusCode": 400}
          "plain string"
        """
        if isinstance(body, str):
            # Try to parse JSON string first
            try:
                body = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                return body
        if isinstance(body, dict):
            errors = body.get("errors")
            if isinstance(errors, list) and errors:
                msgs = [e.get("error") or e.get("message", "") for e in errors if isinstance(e, dict)]
                msgs = [m for m in msgs if m]
                if msgs:
                    return "; ".join(msgs)
            for key in ("message", "error", "detail"):
                if body.get(key) and isinstance(body[key], str):
                    return body[key]
            return json.dumps(body)
        return str(body)

    def _proxy_error(self, raw) -> Optional[dict]:
        """
        Old Render proxy returns HTTP 200 even when Zepto returns 4xx, packing
        the real status in {proxied: True, status_code: <zepto>, data: <body>}.
        Detect that here so every caller can surface the real Zepto error.
        """
        if isinstance(raw, dict) and raw.get("proxied"):
            status = raw.get("status_code", 200)
            if status >= 400:
                return {
                    "success":     False,
                    "status_code": status,
                    "error":       self._zepto_message(raw.get("data", "")),
                }
        return None

    def _url(self, path: str) -> str:
        """
        LOCAL      → https://po-integration-backend.onrender.com/api/proxy/zepto/api/v1/external/...
        PRODUCTION → https://silkroute.zepto.co.in/api/v1/external/...

        In local mode every request leaves from Render's static IP, so Zepto's
        IP whitelist check passes even though your Mac's IP keeps changing.
        """
        path = path.lstrip("/")
        if self.env == "local":
            url = f"{self.render_url}/api/proxy/zepto/{path}"
            logger.debug("ZeptoService [LOCAL] routing via Render: %s", url)
        else:
            url = f"{self.base_url}/{path}"
            logger.debug("ZeptoService [PROD] calling Zepto directly: %s", url)
        return url

    def _headers(self, idempotency_key: Optional[str] = None) -> dict:
        h = {
            "Content-Type":    "application/json",
            "X-Client-Id":     self.client_id,
            "X-Client-Secret": self.client_secret,
        }
        if idempotency_key:
            h["X-Idempotency-Key"] = idempotency_key
        return h

    # ── 1. List PO Events ────────────────────────────────────────────────────────
    async def list_po_events(
        self,
        days: int,
        vendor_codes: Optional[list] = None,
        po_codes: Optional[list] = None,
        include_all_po_events: bool = False,
        include_line_item_details: bool = False,
        page_size: int = 10,
        page_number: int = 1,
    ) -> dict:
        """
        Retrieve latest PO snapshots for POs created in the past `days` days.
        Max days=45, max pageSize=20, max 10 vendor/po codes per request.
        Use the returned eventId as an idempotency key to avoid re-processing.
        """
        url = self._url("/api/v1/external/po/events")
        params: dict = {
            "days":                    min(days, 45),
            "pageSize":                min(page_size, 20),
            "pageNumber":              page_number,
            "includeAllPoEvents":      str(include_all_po_events).lower(),
            "includeLineItemDetails":  str(include_line_item_details).lower(),
        }
        if vendor_codes:
            params["vendorCodes"] = ",".join(vendor_codes[:10])
        if po_codes:
            params["poCodes"] = ",".join(po_codes[:10])

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, params=params, headers=self._headers())
                raw = response.json()
                err = self._proxy_error(raw)
                if err:
                    logger.error("Zepto list_po_events (via proxy) HTTP %s: %s", err["status_code"], err["error"])
                    return err
                response.raise_for_status()
                logger.info("Zepto list_po_events: HTTP %s, days=%s", response.status_code, days)
                return {"success": True, "status_code": response.status_code, "data": self._unwrap(raw)}
        except httpx.HTTPStatusError as e:
            logger.error("Zepto list_po_events HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._zepto_message(e.response.text)}
        except Exception as e:
            logger.error("Zepto list_po_events failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 2a. Create ASN ───────────────────────────────────────────────────────────
    async def create_asn(self, payload: dict, idempotency_key: Optional[str] = None) -> dict:
        """
        Submit an ASN / invoice against a Zepto PO.
        Returns asnNumber — store it, you need it for cancellation.
        invoiceNumber must be unique per request; there is no update API.
        Quantities must be in pieces (PC), not case sizes.
        On 5XX errors, retrying with the same idempotency key is safe.
        """
        url = self._url("/api/v1/external/asn")
        key = idempotency_key or str(uuid.uuid4())
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=payload, headers=self._headers(key))
                raw = response.json()
                err = self._proxy_error(raw)
                if err:
                    logger.error("Zepto create_asn (via proxy) HTTP %s: %s", err["status_code"], err["error"])
                    return err
                response.raise_for_status()
                data       = self._unwrap(raw)
                asn_number = data.get("data", {}).get("asnNumber")
                po_number  = payload.get("purchaseOrderDetails", {}).get("purchaseOrderNumber")
                logger.info("Zepto ASN created: %s for PO %s", asn_number, po_number)
                return {
                    "success":     True,
                    "status_code": response.status_code,
                    "data":        data,
                    "asn_number":  asn_number,
                }
        except httpx.HTTPStatusError as e:
            logger.error("Zepto create_asn HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._zepto_message(e.response.text)}
        except Exception as e:
            logger.error("Zepto create_asn failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 2b. Cancel ASN ───────────────────────────────────────────────────────────
    async def cancel_asn(self, asn_number: str, idempotency_key: Optional[str] = None) -> dict:
        """
        Cancel an existing ASN by its Zepto-issued asnNumber.
        To update an ASN: cancel it here, then call create_asn with a new invoiceNumber.
        """
        url = self._url("/api/v1/external/asn")
        key = idempotency_key or str(uuid.uuid4())
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.delete(
                    url,
                    params={"asnNumber": asn_number},
                    headers=self._headers(key),
                )
                raw = response.json()
                err = self._proxy_error(raw)
                if err:
                    logger.error("Zepto cancel_asn (via proxy) HTTP %s: %s", err["status_code"], err["error"])
                    return err
                response.raise_for_status()
                logger.info("Zepto ASN cancelled: %s", asn_number)
                return {"success": True, "status_code": response.status_code, "data": self._unwrap(raw)}
        except httpx.HTTPStatusError as e:
            logger.error("Zepto cancel_asn HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._zepto_message(e.response.text)}
        except Exception as e:
            logger.error("Zepto cancel_asn failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 2c. List ASNs ────────────────────────────────────────────────────────────
    async def list_asns(
        self,
        po_code: str,
        page_size: int = 10,
        page_number: int = 1,
    ) -> dict:
        """Fetch all ASNs (and their statuses) created against a given PO code."""
        url = self._url("/api/v1/external/asn")
        params = {
            "poCode":     po_code,
            "pageSize":   page_size,
            "pageNumber": page_number,
        }
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, params=params, headers=self._headers())
                raw = response.json()
                err = self._proxy_error(raw)
                if err:
                    logger.error("Zepto list_asns (via proxy) HTTP %s: %s", err["status_code"], err["error"])
                    return err
                response.raise_for_status()
                return {"success": True, "status_code": response.status_code, "data": self._unwrap(raw)}
        except httpx.HTTPStatusError as e:
            logger.error("Zepto list_asns HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._zepto_message(e.response.text)}
        except Exception as e:
            logger.error("Zepto list_asns failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── 3. Request PO Amendment ──────────────────────────────────────────────────
    async def request_po_amendment(
        self,
        po_number: str,
        payload: dict,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """
        Request a PO-level or line-item amendment from Zepto.
        Supported attributeNames: MRP, BASE_PRICE, EAN, CASE_SIZE, EXPIRY_DATE.
        payload must include purchaseOrderAmendment.purchaseOrderNumber.
        """
        url = self._url(f"/api/v1/external/po/{po_number}/amendment")
        key = idempotency_key or str(uuid.uuid4())
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=payload, headers=self._headers(key))
                raw = response.json()
                err = self._proxy_error(raw)
                if err:
                    logger.error("Zepto request_po_amendment (via proxy) HTTP %s: %s", err["status_code"], err["error"])
                    return err
                response.raise_for_status()
                logger.info("Zepto PO amendment submitted: %s", po_number)
                return {"success": True, "status_code": response.status_code, "data": self._unwrap(raw)}
        except httpx.HTTPStatusError as e:
            logger.error("Zepto request_po_amendment HTTP %s: %s", e.response.status_code, e.response.text)
            return {"success": False, "status_code": e.response.status_code, "error": self._zepto_message(e.response.text)}
        except Exception as e:
            logger.error("Zepto request_po_amendment failed: %s", e)
            return {"success": False, "error": str(e)}

    # ── Health Check ─────────────────────────────────────────────────────────────
    async def health_check(self) -> dict:
        """Test Zepto API connectivity using a minimal PO events call."""
        url = self._url("/api/v1/external/po/events")
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(
                    url,
                    params={"days": 1, "pageSize": 1, "pageNumber": 1},
                    headers=self._headers(),
                )
                return {
                    "reachable":   True,
                    "status_code": response.status_code,
                    "endpoint":    self.base_url,
                    "environment": self.env,
                }
        except Exception as e:
            return {
                "reachable":   False,
                "error":       str(e),
                "endpoint":    self.base_url,
                "environment": self.env,
            }


# ── Single instance — import this everywhere ─────────────────────────────────
zepto_service = ZeptoService()
