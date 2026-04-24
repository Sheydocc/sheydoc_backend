"""
services/tranzak.py
────────────────────
Tranzak API client for SheydocApp.

Responsibilities:
  - Token lifecycle: fetch + in-process cache with 5-min early-refresh buffer
  - create_mobile_wallet_charge — initiates a direct MoMo charge
  - get_request_details         — fetches current state of a payment request
  - refresh_transaction_status  — asks Tranzak to re-query the operator

Environment variables required:
  TRANZAK_ENV     = "production"           (or "sandbox" for testing)
  TRANZAK_APP_ID  = your appId from portal
  TRANZAK_API_KEY = PROD_xxxxx  (or SAND_xxxxx for testing)

FIXES APPLIED:
  1. Added `X-App-ID` header to ALL requests (auth + API calls).
     The Tranzak docs list this as a required common header.
     Its absence causes the "payments API temporarily restricted" error.
  2. Added env/key consistency check at startup — warns loudly if you
     mix a SAND_ key with production env or vice versa.
  3. Improved error logging to surface the full Tranzak error body.
"""

import os
import time
import logging
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

_ENV     = os.getenv("TRANZAK_ENV", "production")
_APP_ID  = os.getenv("TRANZAK_APP_ID", "")
_APP_KEY = os.getenv("TRANZAK_API_KEY", "")

_BASE_URLS = {
    "production": "https://dsapi.tranzak.me",
    "sandbox":    "https://sandbox.dsapi.tranzak.me",
}
BASE_URL = _BASE_URLS.get(_ENV, _BASE_URLS["production"])

# ── Startup sanity check ───────────────────────────────────────────────────────
# Mixing a sandbox key with the production URL (or vice versa) is the #1 cause
# of "Access to the payments API has been temporarily restricted."

def _check_env_key_consistency() -> None:
    if not _APP_ID:
        logger.error("❌ TRANZAK_APP_ID is not set!")
    if not _APP_KEY:
        logger.error("❌ TRANZAK_API_KEY is not set!")
        return

    key_is_sandbox = _APP_KEY.startswith("SAND_")
    key_is_prod    = _APP_KEY.startswith("PROD_")

    if _ENV == "production" and key_is_sandbox:
        logger.error(
            "🚨 MISMATCH: TRANZAK_ENV=production but TRANZAK_API_KEY starts "
            "with SAND_. This WILL cause 'payments API restricted' errors. "
            "Either set TRANZAK_ENV=sandbox or use a PROD_ key."
        )
    elif _ENV == "sandbox" and key_is_prod:
        logger.error(
            "🚨 MISMATCH: TRANZAK_ENV=sandbox but TRANZAK_API_KEY starts "
            "with PROD_. Either set TRANZAK_ENV=production or use a SAND_ key."
        )
    elif not key_is_sandbox and not key_is_prod:
        logger.warning(
            "⚠️  TRANZAK_API_KEY doesn't start with SAND_ or PROD_. "
            "Double-check you copied the full key from the developer portal."
        )
    else:
        logger.info(
            "✅ Tranzak config OK — env=%s appId=%s key_prefix=%s base_url=%s",
            _ENV, _APP_ID[:8], _APP_KEY[:5], BASE_URL,
        )

_check_env_key_consistency()


# ── Token cache ────────────────────────────────────────────────────────────────
# Single-process cache. Works fine for a single-worker Render deployment.
# For multi-worker, swap this for Redis or Firestore.

_cached_token:     Optional[str] = None
_token_expires_at: float         = 0.0   # Unix timestamp


async def _fetch_fresh_token() -> Tuple[str, int]:
    """
    POST /auth/token → (token, expiresIn).

    FIX: X-App-ID is included here as well — the docs require it on
    every request, including the auth endpoint.
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{BASE_URL}/auth/token",
            # FIX: X-App-ID is a required header on ALL endpoints per the docs.
            headers={"X-App-ID": _APP_ID},
            json={"appId": _APP_ID, "appKey": _APP_KEY},
        )

    # Log the raw response body when auth fails so you can diagnose quickly.
    if resp.status_code != 200:
        logger.error(
            "❌ Tranzak auth HTTP %s: %s", resp.status_code, resp.text[:400]
        )
        resp.raise_for_status()

    body = resp.json()
    if not body.get("success"):
        err = body.get("errorMsg", "unknown")
        logger.error("❌ Tranzak auth success=false: %s | full body: %s", err, body)
        raise RuntimeError(f"Tranzak auth failed: {err}")

    data = body["data"]
    return data["token"], data["expiresIn"]


async def get_token() -> str:
    """Return a valid bearer token, fetching a new one if near expiry."""
    global _cached_token, _token_expires_at

    # Refresh when less than 5 minutes remain
    if _cached_token and time.time() < _token_expires_at - 300:
        return _cached_token

    logger.info("🔑 Fetching Tranzak token (env=%s appId=%s)", _ENV, _APP_ID[:8])
    token, expires_in = await _fetch_fresh_token()
    _cached_token      = token
    _token_expires_at  = time.time() + expires_in
    logger.info("✅ Tranzak token acquired, valid for %ds", expires_in)
    return _cached_token


async def _auth_headers() -> dict:
    """
    Returns headers required on every authenticated Tranzak API call.

    FIX: Added `X-App-ID` — the Tranzak docs list it as a required
    common header for ALL requests. Omitting it causes the
    "Access to the payments API has been temporarily restricted" error.
    """
    token = await get_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        # ↓ THIS WAS THE MISSING HEADER
        "X-App-ID":      _APP_ID,
    }


# ── API calls ──────────────────────────────────────────────────────────────────

async def create_mobile_wallet_charge(
    *,
    amount:               float,
    currency_code:        str,
    description:          str,
    mch_transaction_ref:  str,    # YOUR unique ref, max 64 chars
    mobile_wallet_number: str,    # e.g. "237674123456"
    callback_url:         str,
    return_url:           str = "",
) -> dict:
    """
    POST /xp021/v1/request/create-mobile-wallet-charge

    Sends a USSD push to the user's MoMo number.
    Returns the Tranzak `data` dict (contains requestId, status, links…).
    Raises RuntimeError on Tranzak-level failure.

    NOTE: mobileWalletNumber must be prefixed with country code e.g. 237674123456
    NOTE: mchTransactionRef must be unique per request for the last 30 days
          and is REQUIRED for direct mobile wallet charges (per Tranzak docs).
    """
    payload = {
        "amount":             amount,
        "currencyCode":       currency_code,
        "description":        description,
        "mchTransactionRef":  mch_transaction_ref,
        "mobileWalletNumber": mobile_wallet_number,
        "callbackUrl":        callback_url,
        "returnUrl":          return_url,
    }
    logger.info(
        "💳 Tranzak charge: ref=%s amount=%s%s phone=%s",
        mch_transaction_ref, amount, currency_code, mobile_wallet_number,
    )

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            f"{BASE_URL}/xp021/v1/request/create-mobile-wallet-charge",
            json=payload,
            headers=await _auth_headers(),
        )

    # Log raw response on non-200 so you always have the full picture
    if resp.status_code != 200:
        logger.error(
            "❌ Tranzak charge HTTP %s: %s", resp.status_code, resp.text[:600]
        )
        resp.raise_for_status()

    body = resp.json()
    if not body.get("success"):
        err      = body.get("errorMsg", "Unknown Tranzak error")
        err_code = body.get("errorCode", "N/A")
        logger.error(
            "❌ Tranzak charge failed: errorMsg=%s errorCode=%s | full body: %s",
            err, err_code, body,
        )
        raise RuntimeError(err)

    logger.info(
        "✅ Tranzak charge created: requestId=%s", body["data"].get("requestId")
    )
    return body["data"]


async def get_request_details(request_id: str) -> dict:
    """
    GET /xp021/v1/request/details?requestId={id}

    Returns the full request dict (status, payer info, transaction info).
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{BASE_URL}/xp021/v1/request/details",
            params={"requestId": request_id},
            headers=await _auth_headers(),
        )

    if resp.status_code != 200:
        logger.error(
            "❌ get_request_details HTTP %s: %s", resp.status_code, resp.text[:400]
        )
        resp.raise_for_status()

    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(body.get("errorMsg", "Failed to fetch request details"))
    return body["data"]


async def refresh_transaction_status(request_id: str) -> dict:
    """
    POST /xp021/v1/request/refresh-transaction-status

    Tells Tranzak to re-query the operator for the latest status.
    Use this when the webhook is delayed (slow operator network).
    Returns the updated request dict.
    """
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            f"{BASE_URL}/xp021/v1/request/refresh-transaction-status",
            json={"requestId": request_id},
            headers=await _auth_headers(),
        )

    if resp.status_code != 200:
        logger.error(
            "❌ refresh_transaction_status HTTP %s: %s",
            resp.status_code, resp.text[:400],
        )
        resp.raise_for_status()

    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(body.get("errorMsg", "Refresh failed"))
    return body["data"]







# """
# services/tranzak.py
# ────────────────────
# Tranzak API client for SheydocApp.

# Responsibilities:
#   - Token lifecycle: fetch + in-process cache with 5-min early-refresh buffer
#   - create_mobile_wallet_charge — initiates a direct MoMo charge
#   - get_request_details         — fetches current state of a payment request
#   - refresh_transaction_status  — asks Tranzak to re-query the operator

# Environment variables required:
#   TRANZAK_ENV     = "production"           (or "sandbox" for testing)
#   TRANZAK_APP_ID  = your appId from portal
#   TRANZAK_API_KEY = PROD_xxxxx  (or SAND_xxxxx for testing)
# """

# import os
# import time
# import logging
# from typing import Optional, Tuple

# import httpx

# logger = logging.getLogger(__name__)

# # ── Config ─────────────────────────────────────────────────────────────────────

# _ENV     = os.getenv("TRANZAK_ENV", "production")
# _APP_ID  = os.getenv("TRANZAK_APP_ID", "")
# _APP_KEY = os.getenv("TRANZAK_API_KEY", "")

# _BASE_URLS = {
#     "production": "https://dsapi.tranzak.me",
#     "sandbox":    "https://sandbox.dsapi.tranzak.me",
# }
# BASE_URL = _BASE_URLS.get(_ENV, _BASE_URLS["production"])

# # ── Token cache ────────────────────────────────────────────────────────────────
# # Single-process cache. Works fine for a single-worker Render deployment.
# # For multi-worker, swap this for Redis or Firestore.

# _cached_token:    Optional[str] = None
# _token_expires_at: float        = 0.0   # Unix timestamp


# async def _fetch_fresh_token() -> Tuple[str, int]:
#     """POST /auth/token → (token, expiresIn)."""
#     async with httpx.AsyncClient(timeout=15) as client:
#         resp = await client.post(
#             f"{BASE_URL}/auth/token",
#             json={"appId": _APP_ID, "appKey": _APP_KEY},
#         )
#     resp.raise_for_status()
#     body = resp.json()
#     if not body.get("success"):
#         raise RuntimeError(f"Tranzak auth failed: {body.get('errorMsg', 'unknown')}")
#     data = body["data"]
#     return data["token"], data["expiresIn"]


# async def get_token() -> str:
#     """Return a valid bearer token, fetching a new one if near expiry."""
#     global _cached_token, _token_expires_at

#     # Refresh when less than 5 minutes remain
#     if _cached_token and time.time() < _token_expires_at - 300:
#         return _cached_token

#     logger.info("🔑 Fetching Tranzak token (env=%s appId=%s)", _ENV, _APP_ID[:6])
#     token, expires_in = await _fetch_fresh_token()
#     _cached_token      = token
#     _token_expires_at  = time.time() + expires_in
#     logger.info("✅ Tranzak token acquired, valid for %ds", expires_in)
#     return _cached_token


# async def _auth_headers() -> dict:
#     token = await get_token()
#     return {
#         "Authorization": f"Bearer {token}",
#         "Content-Type":  "application/json",
#     }


# # ── API calls ──────────────────────────────────────────────────────────────────

# async def create_mobile_wallet_charge(
#     *,
#     amount:               float,
#     currency_code:        str,
#     description:          str,
#     mch_transaction_ref:  str,    # YOUR unique ref, max 64 chars
#     mobile_wallet_number: str,    # e.g. "237674123456"
#     callback_url:         str,
#     return_url:           str = "",
# ) -> dict:
#     """
#     POST /xp021/v1/request/create-mobile-wallet-charge

#     Sends a USSD push to the user's MoMo number.
#     Returns the Tranzak `data` dict (contains requestId, status, links…).
#     Raises RuntimeError on Tranzak-level failure.
#     """
#     payload = {
#         "amount":             amount,
#         "currencyCode":       currency_code,
#         "description":        description,
#         "mchTransactionRef":  mch_transaction_ref,
#         "mobileWalletNumber": mobile_wallet_number,
#         "callbackUrl":        callback_url,
#         "returnUrl":          return_url,
#     }
#     logger.info(
#         "💳 Tranzak charge: ref=%s amount=%s%s phone=%s",
#         mch_transaction_ref, amount, currency_code, mobile_wallet_number,
#     )
#     async with httpx.AsyncClient(timeout=20) as client:
#         resp = await client.post(
#             f"{BASE_URL}/xp021/v1/request/create-mobile-wallet-charge",
#             json=payload,
#             headers=await _auth_headers(),
#         )

#     body = resp.json()
#     if not body.get("success"):
#         err = body.get("errorMsg", "Unknown Tranzak error")
#         logger.error("❌ Tranzak charge failed: %s", err)
#         raise RuntimeError(err)

#     logger.info("✅ Tranzak charge created: requestId=%s", body["data"].get("requestId"))
#     return body["data"]


# async def get_request_details(request_id: str) -> dict:
#     """
#     GET /xp021/v1/request/details?requestId={id}

#     Returns the full request dict (status, payer info, transaction info).
#     """
#     async with httpx.AsyncClient(timeout=15) as client:
#         resp = await client.get(
#             f"{BASE_URL}/xp021/v1/request/details",
#             params={"requestId": request_id},
#             headers=await _auth_headers(),
#         )
#     body = resp.json()
#     if not body.get("success"):
#         raise RuntimeError(body.get("errorMsg", "Failed to fetch request details"))
#     return body["data"]


# async def refresh_transaction_status(request_id: str) -> dict:
#     """
#     POST /xp021/v1/request/refresh-transaction-status

#     Tells Tranzak to re-query the operator for the latest status.
#     Use this when the webhook is delayed (slow operator network).
#     Returns the updated request dict.
#     """
#     async with httpx.AsyncClient(timeout=20) as client:
#         resp = await client.post(
#             f"{BASE_URL}/xp021/v1/request/refresh-transaction-status",
#             json={"requestId": request_id},
#             headers=await _auth_headers(),
#         )
#     body = resp.json()
#     if not body.get("success"):
#         raise RuntimeError(body.get("errorMsg", "Refresh failed"))
#     return body["data"]