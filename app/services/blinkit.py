"""
BlinkitService — Smart routing for Blinkit API calls.

LOCAL  : Your Mac → Render Server (static IP) → Blinkit API ✅
PROD   : Render Server (static IP) → Blinkit API directly ✅

This means Blinkit API works from your local Mac too,
even though your Mac has a dynamic IP.
"""

import httpx
import os
import logging
from datetime import datetime

logger = logging.getLogger("edi.blinkit")


class BlinkitService:
    def __init__(self):
        self.env          = os.getenv("ENVIRONMENT", "local")
        self.api_key      = os.getenv("BLINKIT_API_KEY", "")
        self.render_url   = os.getenv("RENDER_URL", "").rstrip("/")
        self.blinkit_url  = os.getenv("BLINKIT_BASE_URL", "https://api.blinkit.com").rstrip("/")

        if self.env == "local":
            logger.info("🔄 BlinkitService: LOCAL mode — routing via Render proxy")
        else:
            logger.info("🚀 BlinkitService: PRODUCTION mode — calling Blinkit directly")

    # ── Internal: build the correct URL based on environment ────────────────
    def _url(self, path: str) -> str:
        """
        LOCAL      → https://po-integration-backend.onrender.com/api/proxy/blinkit/vendor/asn
        PRODUCTION → https://api.blinkit.com/vendor/asn
        """
        path = path.lstrip("/")
        if self.env == "local":
            url = f"{self.render_url}/api/proxy/blinkit/{path}"
            logger.debug(f"🔄 [LOCAL] routing via Render: {url}")
        else:
            url = f"{self.blinkit_url}/{path}"
            logger.debug(f"🚀 [PROD] calling Blinkit directly: {url}")
        return url

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Source": "EDI-Integration-System",
        }

    # ── Send ASN (Advance Shipment Notification) to Blinkit ─────────────────
    async def send_asn(self, asn_payload: dict) -> dict:
        """
        Push ASN to Blinkit after stock is dispatched.
        Blinkit expects: asn_number, po_number, items[], shipment_date, carrier, tracking
        """
        url = self._url("vendor/asn")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=asn_payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"✅ ASN sent to Blinkit: HTTP {response.status_code}")
                return {"success": True, "status_code": response.status_code, "data": response.json()}
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Blinkit ASN HTTP error: {e.response.status_code} — {e.response.text}")
            return {"success": False, "status_code": e.response.status_code, "error": e.response.text}
        except Exception as e:
            logger.error(f"❌ Blinkit ASN failed: {e}")
            return {"success": False, "error": str(e)}

    # ── Send PO Acknowledgement back to Blinkit ──────────────────────────────
    async def send_po_acknowledgement(self, po_number: str, sap_order_id: str, status: str) -> dict:
        """
        Acknowledge a received PO back to Blinkit.
        status: "ACCEPTED" | "REJECTED" | "PARTIAL"
        """
        url = self._url("vendor/po/acknowledge")
        payload = {
            "po_number": po_number,
            "sap_order_id": sap_order_id,
            "status": status,
            "acknowledged_at": datetime.utcnow().isoformat(),
        }
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"✅ PO ACK sent to Blinkit: {po_number} → {status}")
                return {"success": True, "status_code": response.status_code, "data": response.json()}
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Blinkit PO ACK error: {e.response.status_code} — {e.response.text}")
            return {"success": False, "status_code": e.response.status_code, "error": e.response.text}
        except Exception as e:
            logger.error(f"❌ Blinkit PO ACK failed: {e}")
            return {"success": False, "error": str(e)}

    # ── Get PO details from Blinkit ──────────────────────────────────────────
    async def get_po_details(self, po_number: str) -> dict:
        """Fetch PO details from Blinkit (for reconciliation)"""
        url = self._url(f"vendor/po/{po_number}")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, headers=self._headers())
                response.raise_for_status()
                return {"success": True, "data": response.json()}
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Blinkit GET PO error: {e.response.status_code}")
            return {"success": False, "status_code": e.response.status_code, "error": e.response.text}
        except Exception as e:
            logger.error(f"❌ Blinkit GET PO failed: {e}")
            return {"success": False, "error": str(e)}

    # ── Get pending POs from Blinkit ─────────────────────────────────────────
    async def get_pending_pos(self) -> dict:
        """Poll Blinkit for new/pending POs (pull mode)"""
        url = self._url("vendor/po/pending")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, headers=self._headers())
                response.raise_for_status()
                return {"success": True, "data": response.json()}
        except Exception as e:
            logger.error(f"❌ Blinkit pending POs failed: {e}")
            return {"success": False, "error": str(e)}

    # ── Check Blinkit API connectivity ───────────────────────────────────────
    async def health_check(self) -> dict:
        """Test if Blinkit API is reachable with current routing"""
        url = self._url("vendor/health")
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url, headers=self._headers())
                return {
                    "reachable": True,
                    "status_code": response.status_code,
                    "routing": "via_render_proxy" if self.env == "local" else "direct",
                    "endpoint": url,
                }
        except Exception as e:
            return {
                "reachable": False,
                "error": str(e),
                "routing": "via_render_proxy" if self.env == "local" else "direct",
                "endpoint": url,
            }


# ── Single instance — import this everywhere ─────────────────────────────────
blinkit_service = BlinkitService()
