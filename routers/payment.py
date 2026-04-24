"""
routers/payment.py
──────────────────
Payment endpoints for SheydocApp using the Tranzak Collection API.

Endpoints
─────────
POST /payment/initiate
    Called by Flutter immediately after the patient taps "Pay now".
    Sends a USSD push to the patient's MoMo number via Tranzak.
    Writes a PENDING payment record to Firestore.
    Returns payment_ref for Flutter to use when polling.

POST /payment/webhook
    Called by Tranzak servers when the payment completes or fails.
    Idempotent — safe if called multiple times for the same payment.
    On SUCCESSFUL: writes the appointment to Firestore + sends notifications.
    On FAILED/CANCELLED: marks the payment record as failed.

GET /payment/status/{payment_ref}
    Polled by Flutter every ~4 seconds while the user is on the waiting screen.
    Triggers a Tranzak refresh if the record is still pending,
    so we catch cases where the webhook was delayed.

Flow
────
Flutter → POST /payment/initiate
            ↓
        Tranzak sends USSD to patient's phone
            ↓
        Patient enters PIN
            ↓
        Tranzak → POST /payment/webhook  (async, usually within seconds)
            ↓
        Backend writes Firestore appointment + sends FCM + email
            ↓
Flutter polls GET /payment/status → sees "successful" → shows success screen
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel
from firebase_admin import firestore

from services import tranzak, payment_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/payment", tags=["Payment"])

# ── Config ─────────────────────────────────────────────────────────────────────

BACKEND_BASE_URL     = os.getenv("BACKEND_BASE_URL", "https://sheydoc-backend.onrender.com")
APPOINTMENT_FEE_XAF  = float(os.getenv("APPOINTMENT_FEE_XAF", "3000"))
CURRENCY             = "XAF"
TRANZAK_WEBHOOK_AUTH_KEY = os.getenv("TRANZAK_WEBHOOK_AUTH_KEY", "")


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _db():
    return firestore.client()


async def _get_user(uid: str) -> Optional[dict]:
    doc = _db().collection("users").document(uid).get()
    return doc.to_dict() if doc.exists else None


def _generate_payment_ref(patient_id: str) -> str:
    """
    Unique reference we send to Tranzak as mchTransactionRef.
    Format: SHD-<8 chars of patient_id>-<10 random hex chars>
    Max 64 chars (Tranzak limit). Stays recognisable in the Tranzak dashboard.
    """
    suffix = uuid.uuid4().hex[:10].upper()
    return f"SHD-{patient_id[:8].upper()}-{suffix}"


def _display_name(user: dict, fallback: str) -> str:
    return (
        user.get("name") or
        user.get("displayName") or
        user.get("firstName") or
        fallback
    )


def _appointment_exists(payment_ref: str) -> bool:
    """Check whether an appointment was already created for this payment."""
    docs = (
        _db().collection("appointments")
             .where("paymentRef", "==", payment_ref)
             .limit(1)
             .stream()
    )
    return any(True for _ in docs)


# ── Pydantic models ─────────────────────────────────────────────────────────────

class InitiatePaymentRequest(BaseModel):
    patient_id:             str
    doctor_id:              str
    mobile_wallet_number:   str       # normalized e.g. "237674123456"
    appointment_datetime:   str       # ISO-8601 UTC e.g. "2025-01-15T09:30:00+00:00"
    appointment_time:       str       # display string e.g. "9:30 AM"
    appointment_type:       str       # "video" | "audio"
    duration_minutes:       int
    reason_for_consultation: str = ""
    symptoms:               list[str] = []


class PaymentStatusResponse(BaseModel):
    payment_ref:         str
    status:              str          # pending | successful | failed
    tranzak_request_id:  Optional[str] = None
    error_message:       Optional[str] = None


# ── POST /payment/initiate ──────────────────────────────────────────────────────

@router.post("/initiate")
async def initiate_payment(req: InitiatePaymentRequest):
    """
    1. Verify patient and doctor exist in Firestore.
    2. Generate a unique payment_ref (our mchTransactionRef).
    3. Call Tranzak create-mobile-wallet-charge → sends USSD to patient's phone.
    4. Write PENDING payment record to Firestore.
    5. Return payment_ref to Flutter for polling.
    """

    # ── 1. Fetch users ──────────────────────────────────────────────────────
    patient = await _get_user(req.patient_id)
    doctor  = await _get_user(req.doctor_id)

    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")
    if not doctor:
        raise HTTPException(status_code=404, detail="Doctor not found")

    patient_name = _display_name(patient, "Patient")
    doctor_name  = _display_name(doctor,  "Doctor")

    # ── 2. Generate ref ─────────────────────────────────────────────────────
    payment_ref = _generate_payment_ref(req.patient_id)

    # ── 3. Build appointment payload ────────────────────────────────────────
    # Stored in the payment record so the webhook can write the appointment
    # without a second network call from Flutter.
    appointment_payload = {
        "doctorId":              req.doctor_id,
        "doctorName":            doctor_name,
        "patientId":             req.patient_id,
        "patientName":           patient_name,
        "patientEmail":          patient.get("email", ""),
        "appointmentDateTime":   req.appointment_datetime,
        "appointmentTime":       req.appointment_time,
        "appointmentType":       req.appointment_type,
        "durationMinutes":       req.duration_minutes,
        "reasonForConsultation": req.reason_for_consultation,
        "symptoms":              req.symptoms,
        "fee":                   APPOINTMENT_FEE_XAF,
        "status":                "confirmed",
        "timeConfirmed":         True,
        "hasRecord":             False,
        "lastReminderSent":      None,
        "location":              patient.get("location", ""),
        "paymentRef":            payment_ref,
        "paymentStatus":         "paid",
    }

    # ── 4. Hit Tranzak ──────────────────────────────────────────────────────
    webhook_url = f"{BACKEND_BASE_URL}/payment/webhook"
    try:
        tranzak_data = await tranzak.create_mobile_wallet_charge(
            amount               = APPOINTMENT_FEE_XAF,
            currency_code        = CURRENCY,
            description          = f"SheydocApp – Appointment with Dr. {doctor_name}",
            mch_transaction_ref  = payment_ref,
            mobile_wallet_number = req.mobile_wallet_number,
            callback_url         = webhook_url,
        )
    except RuntimeError as exc:
        logger.error("Tranzak error during initiation: %s", exc)
        raise HTTPException(status_code=502, detail=f"Payment gateway error: {exc}")

    tranzak_request_id = tranzak_data.get("requestId", "")

    # ── 5. Persist pending record ───────────────────────────────────────────
    payment_store.create_payment_record(
        payment_ref          = payment_ref,
        tranzak_request_id   = tranzak_request_id,
        patient_id           = req.patient_id,
        doctor_id            = req.doctor_id,
        amount               = APPOINTMENT_FEE_XAF,
        currency_code        = CURRENCY,
        mobile_number        = req.mobile_wallet_number,
        appointment_payload  = appointment_payload,
    )

    logger.info(
        "✅ Payment initiated: ref=%s tranzak_req=%s patient=%s",
        payment_ref, tranzak_request_id, req.patient_id,
    )

    return {
        "success":            True,
        "payment_ref":        payment_ref,
        "tranzak_request_id": tranzak_request_id,
        "amount":             APPOINTMENT_FEE_XAF,
        "currency":           CURRENCY,
        "message":            "USSD prompt sent. Please enter your MoMo PIN.",
    }


# ── POST /payment/webhook ───────────────────────────────────────────────────────

@router.post("/webhook")
async def payment_webhook(request: Request, bg: BackgroundTasks):
    """
    Tranzak calls this endpoint when a payment completes or fails.

    Security  : If TRANZAK_WEBHOOK_AUTH_KEY is set in .env, we reject any
                webhook whose authKey field doesn't match.
    Idempotency: We check is_already_processed() before doing any work.
                 Safe if Tranzak retries the webhook.
    """
    body = await request.json()
    logger.info("📩 Tranzak webhook received: eventType=%s", body.get("eventType"))

    # ── Optional authKey check ──────────────────────────────────────────────
    if TRANZAK_WEBHOOK_AUTH_KEY:
        if body.get("authKey") != TRANZAK_WEBHOOK_AUTH_KEY:
            logger.warning("⚠️  Webhook authKey mismatch — rejecting request")
            raise HTTPException(status_code=401, detail="Invalid webhook auth key")

    resource        = body.get("resource", {})
    tranzak_status  = resource.get("transactionStatus") or resource.get("status", "")
    tranzak_req_id  = resource.get("requestId", "")
    tranzak_txn_id  = resource.get("transactionId", "")
    mch_ref         = resource.get("mchTransactionRef", "")

    # Our payment_ref is the mchTransactionRef we sent to Tranzak
    payment_ref = mch_ref or tranzak_req_id

    if not payment_ref:
        logger.warning("Webhook missing payment reference — ignoring")
        return {"received": True}

    # ── Idempotency check ───────────────────────────────────────────────────
    if payment_store.is_already_processed(payment_ref):
        logger.info("↩️  Duplicate webhook for ref=%s — skipping", payment_ref)
        return {"received": True}

    # ── Route on Tranzak status ─────────────────────────────────────────────
    if tranzak_status == "SUCCESSFUL":
        # Hand off to background task — webhook must return quickly
        bg.add_task(_on_payment_success, payment_ref, tranzak_txn_id)

    elif tranzak_status in ("FAILED", "CANCELLED", "CANCELLED_BY_PAYER"):
        reason = resource.get("errorMessage") or tranzak_status
        payment_store.mark_payment_failed(payment_ref, reason)
        logger.info("💔 Payment %s: ref=%s reason=%s",
                    tranzak_status, payment_ref, reason)

    else:
        # PENDING / PAYMENT_IN_PROGRESS — nothing to do yet
        logger.info("ℹ️  Webhook: status=%s ref=%s — waiting", tranzak_status, payment_ref)

    # Always return 200 so Tranzak doesn't retry unnecessarily
    return {"received": True}


async def _on_payment_success(payment_ref: str, tranzak_txn_id: str) -> None:
    """
    Background task triggered by the webhook on SUCCESSFUL payment.

    Steps:
      1. Mark payment successful in Firestore.
      2. Write the appointment document to Firestore.
      3. Send FCM push notifications to patient and doctor.
      4. Send confirmation emails.
    """
    # Import from main to reuse existing helpers (avoids duplication)
    from main import (
        db, send_fcm, send_email,
        _get_fcm_token, _booking_email, fmt_dt,
    )

    # ── 1. Fetch and validate record ────────────────────────────────────────
    record = payment_store.get_payment_record(payment_ref)
    if not record:
        logger.error("_on_payment_success: no record for ref=%s", payment_ref)
        return

    # Mark successful first — prevents duplicate processing on retry
    payment_store.mark_payment_successful(payment_ref, tranzak_txn_id)

    payload = record["appointment_payload"]

    # ── 2. Write appointment ────────────────────────────────────────────────
    # Parse appointmentDateTime to produce a proper Timestamp for Firestore
    try:
        apt_dt_obj = datetime.fromisoformat(
            payload["appointmentDateTime"].replace("Z", "+00:00")
        )
        apt_date_ts = apt_dt_obj.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    except Exception:
        apt_date_ts = datetime.now(timezone.utc)

    firestore_doc = {
        **payload,
        "appointmentDate":     apt_date_ts,          # Firestore Timestamp for date queries
        "createdAt":           firestore.SERVER_TIMESTAMP,
        "tranzakTransactionId": tranzak_txn_id,
    }

    apt_ref = db.collection("appointments").document()
    apt_ref.set(firestore_doc)
    appointment_id = apt_ref.id
    logger.info(
        "📅 Appointment written: id=%s patient=%s doctor=%s",
        appointment_id, payload["patientId"], payload["doctorId"],
    )

    # ── 3 & 4. Notifications ────────────────────────────────────────────────
    try:
        patient = (db.collection("users")
                     .document(payload["patientId"])
                     .get().to_dict() or {})
        doctor  = (db.collection("users")
                     .document(payload["doctorId"])
                     .get().to_dict() or {})

        pname  = _display_name(patient, payload.get("patientName", "Patient"))
        dname  = _display_name(doctor,  payload.get("doctorName",  "Doctor"))
        atime  = fmt_dt(payload["appointmentDateTime"])
        reason = payload.get("reasonForConsultation", "")
        notif_data = {
            "type":           "booking_confirmed",
            "appointment_id": appointment_id,
        }

        # FCM — patient
        if fcm := _get_fcm_token(patient, payload["patientId"], "patient"):
            await send_fcm(
                fcm,
                "Appointment Confirmed ✅",
                f"Payment received! Your appointment with Dr. {dname} on {atime} is confirmed.",
                notif_data,
            )
        # FCM — doctor
        if fcm := _get_fcm_token(doctor, payload["doctorId"], "doctor"):
            await send_fcm(
                fcm,
                "New Appointment 📅",
                f"Payment confirmed. {pname} booked for {atime}.",
                notif_data,
            )
        # Email — patient
        if email := patient.get("email"):
            await send_email(
                email, pname,
                "Appointment Confirmed — Payment Received",
                _booking_email(pname, dname, atime, reason),
            )
        # Email — doctor
        if email := doctor.get("email"):
            await send_email(
                email, dname,
                "New Paid Appointment",
                _booking_email(dname, pname, atime, reason),
            )
    except Exception as exc:
        # Notification failure must not roll back the appointment
        logger.error("Notification error after payment: %s", exc)


def _display_name(user: dict, fallback: str) -> str:
    return (
        user.get("name") or
        user.get("displayName") or
        user.get("firstName") or
        fallback
    )


# ── GET /payment/status/{payment_ref} ───────────────────────────────────────────

@router.get("/status/{payment_ref}", response_model=PaymentStatusResponse)
async def get_payment_status(payment_ref: str):
    """
    Flutter polls this while the patient is on the waiting screen.

    If the record is still pending, we call Tranzak's refresh endpoint to
    get the latest operator status. This handles slow operator networks where
    the webhook hasn't arrived yet.
    """
    record = payment_store.get_payment_record(payment_ref)
    if not record:
        raise HTTPException(status_code=404, detail="Payment reference not found")

    current_status = record.get("status", "pending")

    # If still pending, ask Tranzak for the latest status
    if current_status == "pending":
        tranzak_req_id = record.get("tranzak_request_id", "")
        if tranzak_req_id:
            try:
                fresh = await tranzak.refresh_transaction_status(tranzak_req_id)
                fresh_status = fresh.get("status", "")

                if fresh_status == "SUCCESSFUL":
                    txn_id = fresh.get("transactionId", "")
                    payment_store.mark_payment_successful(payment_ref, txn_id)
                    current_status = "successful"
                    # Create appointment if the webhook hasn't fired yet
                    if not _appointment_exists(payment_ref):
                        asyncio.create_task(_on_payment_success(payment_ref, txn_id))

                elif fresh_status in ("FAILED", "CANCELLED", "CANCELLED_BY_PAYER"):
                    reason = fresh.get("errorMessage") or fresh_status
                    payment_store.mark_payment_failed(payment_ref, reason)
                    current_status = "failed"

            except Exception as exc:
                # Refresh errors are non-fatal — just return the current status
                logger.warning("Tranzak refresh failed for %s: %s", payment_ref, exc)

    return PaymentStatusResponse(
        payment_ref        = payment_ref,
        status             = current_status,
        tranzak_request_id = record.get("tranzak_request_id"),
        error_message      = record.get("error_message"),
    )