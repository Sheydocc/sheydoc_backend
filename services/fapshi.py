"""
services/fapshi.py
──────────────────
Fapshi API client for SheydocApp.

Responsibilities:
  - direct_pay       — initiates a USSD MoMo/OM push to the payer's phone
  - payment_status   — fetches current status of a transaction by transId

Environment variables required:
  FAPSHI_API_USER = your apiuser from the Fapshi dashboard
  FAPSHI_API_KEY  = your apikey from the Fapshi dashboard
  FAPSHI_ENV      = "live" (default) | "sandbox" (for testing)

Fapshi API reference:
  Base URL (live):    https://live.fapshi.com
  Base URL (sandbox): https://sandbox.fapshi.com

Auth: every request carries { apiuser, apikey } in HTTP headers.
No token lifecycle — credentials are static per service.

Phone number format for Fapshi direct-pay:
  9 digits, no country code e.g.  "674123456"  NOT "237674123456"
  The caller (payment.py) is responsible for normalizing the phone
  before passing it here.

Fapshi transaction statuses:
  CREATED    — payment link created, user not yet interacted
  PENDING    — user has started the USSD flow
  SUCCESSFUL — payment completed
  FAILED     — payment failed (operator-side)
  EXPIRED    — link expired (only possible with initiate-pay, not direct-pay)

Note on direct-pay vs initiate-pay:
  We use direct-pay exclusively. Direct-pay transactions can ONLY end
  in SUCCESSFUL or FAILED — they never expire, which simplifies our
  polling logic considerably.
"""

import os
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

_ENV      = os.getenv("FAPSHI_ENV", "live")
_API_USER = os.getenv("FAPSHI_API_USER", "")
_API_KEY  = os.getenv("FAPSHI_API_KEY", "")

_BASE_URLS = {
    "live":    "https://live.fapshi.com",
    "sandbox": "https://sandbox.fapshi.com",
}
BASE_URL = _BASE_URLS.get(_ENV, _BASE_URLS["live"])


# ── Auth headers ────────────────────────────────────────────────────────────────

def _headers() -> dict:
    """Return the auth headers required by every Fapshi request."""
    return {
        "apiuser":       _API_USER,
        "apikey":        _API_KEY,
        "Content-Type":  "application/json",
    }


# ── API calls ──────────────────────────────────────────────────────────────────

async def direct_pay(
    *,
    amount:      int,        # XAF, minimum 100
    phone:       str,        # 9-digit Cameroonian number e.g. "674123456"
    external_id: str,        # YOUR unique ref for reconciliation (max 100 chars)
    name:        Optional[str] = None,
    email:       Optional[str] = None,
    user_id:     Optional[str] = None,
    medium:      Optional[str] = None,   # "mobile money" | "orange money" | None (auto)
    message:     str = "SheydocApp appointment payment",
) -> dict:
    """
    POST /direct-pay

    Sends a USSD push to the payer's phone.

    Returns the Fapshi response dict:
      {
        "message": "...",
        "transId":       "ll7J2fl4",   ← use this to poll status
        "dateInitiated": "2025-01-15"
      }

    Raises RuntimeError on any Fapshi-level error (4XX) or network failure.
    """
    payload: dict = {
        "amount":     amount,
        "phone":      phone,
        "externalId": external_id,
        "message":    message,
    }
    if name:    payload["name"]   = name
    if email:   payload["email"]  = email
    if user_id: payload["userId"] = user_id
    if medium:  payload["medium"] = medium

    logger.info(
        "Fapshi direct-pay: externalId=%s amount=%s XAF phone=%s env=%s",
        external_id, amount, phone, _ENV,
    )

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            f"{BASE_URL}/direct-pay",
            json=payload,
            headers=_headers(),
        )

    body = resp.json()

    if resp.status_code != 200:
        err = body.get("message", "Unknown Fapshi error")
        logger.error("❌ Fapshi direct-pay failed [%s]: %s", resp.status_code, err)
        raise RuntimeError(err)

    logger.info(
        "✅ Fapshi direct-pay accepted: transId=%s externalId=%s",
        body.get("transId"), external_id,
    )
    return body


async def payment_status(trans_id: str) -> dict:
    """
    GET /payment-status/{transId}

    Returns the full Fapshi transaction object, e.g.:
      {
        "transId":  "ll7J2fl4",
        "status":   "SUCCESSFUL",   # CREATED | PENDING | SUCCESSFUL | FAILED | EXPIRED
        "amount":   100,
        "medium":   "mobile money",
        ...
      }

    Raises RuntimeError on 4XX / network failure.
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{BASE_URL}/payment-status/{trans_id}",
            headers=_headers(),
        )

    body = resp.json()

    if resp.status_code != 200:
        err = body.get("message", "Failed to fetch payment status")
        logger.error(
            "❌ Fapshi payment-status failed [%s] transId=%s: %s",
            resp.status_code, trans_id, err,
        )
        raise RuntimeError(err)

    logger.debug(
        "🔍 Fapshi payment-status: transId=%s status=%s",
        trans_id, body.get("status"),
    )
    return body