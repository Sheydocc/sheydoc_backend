"""
services/payment_store.py
──────────────────────────
Firestore helpers for the `payments` collection.

Document ID = payment_ref  (our own UUID, sent to Fapshi as externalId).
This is the single source of truth for payment state in SheydocApp.

Document schema
───────────────
  payment_ref              str    our unique ref also the Firestore doc ID
  fapshi_trans_id          str    Fapshi's transId (returned by /direct-pay)
  status                   str    pending | successful | failed
                                  (Fapshi transaction status — OWNED BY THIS
                                  MODULE. Never written to by session
                                  lifecycle code.)
  lifecycle_status         str    successful | under_review | refund_pending
                                  | refunded
                                  (NEW — post-success consultation/session
                                  money-safety status. OWNED BY the session
                                  lifecycle endpoints in main.py, via
                                  set_lifecycle_status() below. Absent on
                                  records created before this field existed
                                  or on records that never reached
                                  'successful'.)
  current_appointment_id   str    (NEW) the appointment this payment is
                                  CURRENTLY funding. Set to the original
                                  appointment on success; repointed here
                                  (never duplicated) if the patient is
                                  reassigned to a different doctor.
  amount                   int    XAF
  mobile_number             str    normalized 9-digit e.g. "674123456"
  patient_id                str
  doctor_id                 str
  appointment_payload        dict   written to /appointments on success
  fapshi_external_id         str    same as payment_ref, kept for clarity
  error_message               str|None
  created_at                  Timestamp
  updated_at                  Timestamp
  lifecycle_status_updated_at Timestamp (NEW)
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


def seed_lifecycle_fields(payment_ref: str, appointment_id: str) -> None:
    """
    NEW (v7.0). Called once, right after the appointment document has been
    written for a successful payment. Initialises the lifecycle-tracking
    fields on the SAME payments/{payment_ref} document — no new document,
    no new collection, no ID scheme mismatch with the rest of this module.

    Uses set(..., merge=True) rather than update() because it's safe to
    call even if, in some edge case, the base 'pending' record write raced
    with this (merge just fills in these two fields either way).
    """
    _db().collection(_COLLECTION).document(payment_ref).set({
        "lifecycle_status":           "successful",
        "current_appointment_id":     appointment_id,
        "lifecycle_status_updated_at": firestore.SERVER_TIMESTAMP,
    }, merge=True)
    logger.info(
        "Lifecycle fields seeded: ref=%s appointment_id=%s",
        payment_ref, appointment_id,
    )


def set_lifecycle_status(
    payment_ref: str,
    lifecycle_status: str,
    current_appointment_id: Optional[str] = None,
) -> None:
    """
    NEW (v7.0). Called by the session-lifecycle endpoints in main.py
    (_mark_payment_status) to record what happened to an already-successful
    payment: 'under_review' (interrupted call), 'refund_pending' (failed /
    doctor unavailable with no alternative / cancelled), or 'refunded'
    (settled manually).

    Never touches `status` — that field remains Fapshi's own
    pending/successful/failed and is owned exclusively by the functions
    above.

    When current_appointment_id is provided (reassignment case), it is
    updated too, so the SAME payment record always points at whichever
    appointment it is currently funding. This is what guarantees a
    reassigned patient is never charged a second time — nothing here ever
    creates a new payments/ document.
    """
    update: Dict[str, Any] = {
        "lifecycle_status":            lifecycle_status,
        "lifecycle_status_updated_at": firestore.SERVER_TIMESTAMP,
    }
    if current_appointment_id is not None:
        update["current_appointment_id"] = current_appointment_id

    _db().collection(_COLLECTION).document(payment_ref).set(update, merge=True)
    logger.info(
        "Lifecycle status updated: ref=%s lifecycle_status=%s current_appointment_id=%s",
        payment_ref, lifecycle_status, current_appointment_id,
    )


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

# Document ID = payment_ref  (our own UUID, sent to Fapshi as externalId).
# This is the single source of truth for payment state in SheydocApp.

# Document schema
# ───────────────
#   payment_ref           str    our unique ref also the Firestore doc ID
#   fapshi_trans_id       str    Fapshi's transId (returned by /direct-pay)
#   status                str    pending | successful | failed
#   amount                int    XAF
#   mobile_number         str    normalized 9-digit e.g. "674123456"
#   patient_id            str
#   doctor_id             str
#   appointment_payload   dict   written to /appointments on success
#   fapshi_external_id    str    same as payment_ref, kept for clarity
#   error_message         str|None
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
#     fapshi_trans_id:     str,
#     patient_id:          str,
#     doctor_id:           str,
#     amount:              int,
#     mobile_number:       str,
#     appointment_payload: Dict[str, Any],
# ) -> None:
#     """Create a new PENDING payment document."""
#     _db().collection(_COLLECTION).document(payment_ref).set({
#         "payment_ref":         payment_ref,
#         "fapshi_trans_id":     fapshi_trans_id,
#         "fapshi_external_id":  payment_ref,   # redundant but useful for Firestore queries
#         "status":              "pending",
#         "amount":              amount,
#         "mobile_number":       mobile_number,
#         "patient_id":          patient_id,
#         "doctor_id":           doctor_id,
#         "appointment_payload": appointment_payload,
#         "error_message":       None,
#         "created_at":          firestore.SERVER_TIMESTAMP,
#         "updated_at":          firestore.SERVER_TIMESTAMP,
#     })
#     logger.info(
#         "Payment record created: ref=%s fapshi_transId=%s",
#         payment_ref, fapshi_trans_id,
#     )


# def mark_payment_successful(payment_ref: str) -> None:
#     _db().collection(_COLLECTION).document(payment_ref).update({
#         "status":     "successful",
#         "updated_at": firestore.SERVER_TIMESTAMP,
#     })
#     logger.info("Payment successful: ref=%s", payment_ref)


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


# def get_payment_by_fapshi_trans_id(
#     fapshi_trans_id: str,
# ) -> Optional[Dict[str, Any]]:
#     """
#     Secondary lookup — used in the webhook when externalId is absent.
#     Normally unnecessary because we always set externalId = payment_ref.
#     """
#     results = (
#         _db().collection(_COLLECTION)
#              .where("fapshi_trans_id", "==", fapshi_trans_id)
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
#     Prevents double appointment creation when Fapshi retries the webhook.
#     """
#     rec = get_payment_record(payment_ref)
#     return rec is not None and rec.get("status") == "successful"
















