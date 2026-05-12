

"""
services/payment_store.py
──────────────────────────
Firestore helpers for the `payments` collection.

Document ID = payment_ref  (our own UUID, sent to Fapshi as externalId).
This is the single source of truth for payment state in SheydocApp.

Document schema
───────────────
  payment_ref           str    our unique ref — also the Firestore doc ID
  fapshi_trans_id       str    Fapshi's transId (returned by /direct-pay)
  status                str    pending | successful | failed
  amount                int    XAF
  mobile_number         str    normalized 9-digit e.g. "674123456"
  patient_id            str
  doctor_id             str
  appointment_payload   dict   written to /appointments on success
  fapshi_external_id    str    same as payment_ref, kept for clarity
  error_message         str|None
  created_at            Timestamp
  updated_at            Timestamp
"""

import logging
from typing import Any, Dict, Optional

from firebase_admin import firestore

logger = logging.getLogger(__name__)

_COLLECTION = "payments"


def _db():
    return firestore.client()


# ── Write ──────────────────────────────────────────────────────────────────────

def create_payment_record(
    *,
    payment_ref:         str,
    fapshi_trans_id:     str,
    patient_id:          str,
    doctor_id:           str,
    amount:              int,
    mobile_number:       str,
    appointment_payload: Dict[str, Any],
) -> None:
    """Create a new PENDING payment document."""
    _db().collection(_COLLECTION).document(payment_ref).set({
        "payment_ref":         payment_ref,
        "fapshi_trans_id":     fapshi_trans_id,
        "fapshi_external_id":  payment_ref,   # redundant but useful for Firestore queries
        "status":              "pending",
        "amount":              amount,
        "mobile_number":       mobile_number,
        "patient_id":          patient_id,
        "doctor_id":           doctor_id,
        "appointment_payload": appointment_payload,
        "error_message":       None,
        "created_at":          firestore.SERVER_TIMESTAMP,
        "updated_at":          firestore.SERVER_TIMESTAMP,
    })
    logger.info(
        "Payment record created: ref=%s fapshi_transId=%s",
        payment_ref, fapshi_trans_id,
    )


def mark_payment_successful(payment_ref: str) -> None:
    _db().collection(_COLLECTION).document(payment_ref).update({
        "status":     "successful",
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    logger.info("Payment successful: ref=%s", payment_ref)


def mark_payment_failed(payment_ref: str, reason: str) -> None:
    _db().collection(_COLLECTION).document(payment_ref).update({
        "status":        "failed",
        "error_message": reason,
        "updated_at":    firestore.SERVER_TIMESTAMP,
    })
    logger.warning("❌ Payment failed: ref=%s reason=%s", payment_ref, reason)


# ── Read ───────────────────────────────────────────────────────────────────────

def get_payment_record(payment_ref: str) -> Optional[Dict[str, Any]]:
    doc = _db().collection(_COLLECTION).document(payment_ref).get()
    return doc.to_dict() if doc.exists else None


def get_payment_by_fapshi_trans_id(
    fapshi_trans_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Secondary lookup — used in the webhook when externalId is absent.
    Normally unnecessary because we always set externalId = payment_ref.
    """
    results = (
        _db().collection(_COLLECTION)
             .where("fapshi_trans_id", "==", fapshi_trans_id)
             .limit(1)
             .stream()
    )
    for doc in results:
        return doc.to_dict()
    return None


def is_already_processed(payment_ref: str) -> bool:
    """
    Idempotency guard.
    Returns True if this payment is already marked successful.
    Prevents double appointment creation when Fapshi retries the webhook.
    """
    rec = get_payment_record(payment_ref)
    return rec is not None and rec.get("status") == "successful"



















# """
# services/payment_store.py
# ──────────────────────────
# Firestore helpers for the `payments` collection.

# Document ID = payment_ref (our mchTransactionRef sent to Tranzak).
# This is the single source of truth for payment state in SheydocApp.

# Document schema:
#   payment_ref           str    our unique ref, also the Firestore doc ID
#   tranzak_request_id    str    Tranzak's requestId (returned on charge creation)
#   status                str    pending | successful | failed
#   amount                float
#   currency_code         str
#   mobile_number         str    normalized e.g. "237674123456"
#   patient_id            str
#   doctor_id             str
#   appointment_payload   dict   full appointment data — written to /appointments on success
#   tranzak_transaction_id str|None  set on success
#   error_message         str|None  set on failure
#   created_at            Timestamp
#   updated_at            Timestamp
# """

# import logging
# from typing import Any, Dict, Optional

# from firebase_admin import firestore

# logger = logging.getLogger(__name__)

# _COLLECTION = "payments"


# def _db():
#     return firestore.client()


# # ── Write ──────────────────────────────────────────────────────────────────────

# def create_payment_record(
#     *,
#     payment_ref:         str,
#     tranzak_request_id:  str,
#     patient_id:          str,
#     doctor_id:           str,
#     amount:              float,
#     currency_code:       str,
#     mobile_number:       str,
#     appointment_payload: Dict[str, Any],
# ) -> None:
#     """Create a new PENDING payment document."""
#     _db().collection(_COLLECTION).document(payment_ref).set({
#         "payment_ref":            payment_ref,
#         "tranzak_request_id":     tranzak_request_id,
#         "status":                 "pending",
#         "amount":                 amount,
#         "currency_code":          currency_code,
#         "mobile_number":          mobile_number,
#         "patient_id":             patient_id,
#         "doctor_id":              doctor_id,
#         "appointment_payload":    appointment_payload,
#         "tranzak_transaction_id": None,
#         "error_message":          None,
#         "created_at":             firestore.SERVER_TIMESTAMP,
#         "updated_at":             firestore.SERVER_TIMESTAMP,
#     })
#     logger.info("📝 Payment record created: ref=%s tranzak_req=%s",
#                 payment_ref, tranzak_request_id)


# def mark_payment_successful(
#     payment_ref:           str,
#     tranzak_transaction_id: str,
# ) -> None:
#     _db().collection(_COLLECTION).document(payment_ref).update({
#         "status":                  "successful",
#         "tranzak_transaction_id":  tranzak_transaction_id,
#         "updated_at":              firestore.SERVER_TIMESTAMP,
#     })
#     logger.info("✅ Payment successful: ref=%s txn=%s",
#                 payment_ref, tranzak_transaction_id)


# def mark_payment_failed(payment_ref: str, reason: str) -> None:
#     _db().collection(_COLLECTION).document(payment_ref).update({
#         "status":        "failed",
#         "error_message": reason,
#         "updated_at":    firestore.SERVER_TIMESTAMP,
#     })
#     logger.warning("❌ Payment failed: ref=%s reason=%s", payment_ref, reason)


# # ── Read ───────────────────────────────────────────────────────────────────────

# def get_payment_record(payment_ref: str) -> Optional[Dict[str, Any]]:
#     doc = _db().collection(_COLLECTION).document(payment_ref).get()
#     return doc.to_dict() if doc.exists else None


# def get_payment_by_tranzak_request_id(
#     tranzak_request_id: str,
# ) -> Optional[Dict[str, Any]]:
#     """
#     Secondary lookup used in the webhook when mchTransactionRef is absent.
#     Normally you won't need this — we always set mchTransactionRef.
#     """
#     results = (
#         _db().collection(_COLLECTION)
#              .where("tranzak_request_id", "==", tranzak_request_id)
#              .limit(1)
#              .stream()
#     )
#     for doc in results:
#         return doc.to_dict()
#     return None


# def is_already_processed(payment_ref: str) -> bool:
#     """
#     Idempotency guard.
#     Returns True if this payment is already marked successful.
#     Prevents double appointment creation when Tranzak retries the webhook.
#     """
#     rec = get_payment_record(payment_ref)
#     return rec is not None and rec.get("status") == "successful"