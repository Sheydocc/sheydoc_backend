"""
TeleMed FastAPI Backend v6.9
Changes from v6.8:
  - FIX critical bug: cron-job.org was configured to ping the bare root URL
    ("/") every 15 minutes, but "/" was a pure health-check endpoint that
    never invoked the reminder logic. This meant _check_and_send_reminders()
    was NEVER being called in production, even though the 5m/30m/1h/24h
    window logic, debounce, and priority ordering were all correct.
    Root cause: reminder logic only lived behind /heartbeat and
    /check-reminders, and the scheduler was calling neither.
  - FIX: root() now also runs the debounced reminder check
    (via _run_reminders_if_due), so ANY external pinger hitting "/" keeps
    reminders flowing — not just /heartbeat. This makes the system resilient
    to cron misconfiguration instead of silently doing nothing.
  - No other behavior changed from v6.8.
"""

import asyncio
import os
import mimetypes
import smtplib
import tempfile
import json
import base64
import hmac
import hashlib
import time as time_module
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import firebase_admin
from firebase_admin import credentials, firestore, messaging

from appwrite.client import Client
from appwrite.services.storage import Storage
from appwrite.input_file import InputFile
from appwrite.id import ID

load_dotenv()

# ============================================================================
# CONFIG
# ============================================================================

SMTP_HOST         = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER         = os.getenv("SMTP_USER")
SMTP_PASSWORD     = os.getenv("SMTP_PASSWORD")
FROM_NAME         = os.getenv("FROM_NAME", "SheydocApp")

APPWRITE_ENDPOINT       = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
APPWRITE_PROJECT_ID     = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY        = os.getenv("APPWRITE_API_KEY")
APPWRITE_BUCKET_ID      = os.getenv("APPWRITE_BUCKET_ID")
APPWRITE_CHAT_BUCKET_ID = os.getenv("APPWRITE_CHAT_BUCKET_ID", APPWRITE_BUCKET_ID)

STREAM_API_KEY    = os.getenv("STREAM_API_KEY")
STREAM_API_SECRET = os.getenv("STREAM_API_SECRET")

appwrite_client = Client()
appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
appwrite_client.set_project(APPWRITE_PROJECT_ID)
appwrite_client.set_key(APPWRITE_API_KEY)
appwrite_storage = Storage(appwrite_client)

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="SheydocApp Backend",
    description="Notifications, email, file uploads, Stream Video tokens, Medical Records, Slot Validation",
    version="6.9.0",
)

from routers.payment import router as payment_router
app.include_router(payment_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
firebase_admin.initialize_app(cred)
db = firestore.client()


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class BookingConfirmedRequest(BaseModel):
    appointment_id: str
    patient_id: str
    doctor_id: str
    appointment_datetime: str
    duration_minutes: int
    reason_for_consultation: Optional[str] = ""
    time_confirmed: Optional[bool] = True


class AppointmentCanceledRequest(BaseModel):
    appointment_id: str
    patient_id: str
    doctor_id: str
    canceled_by: str
    appointment_datetime: str


class StreamTokenRequest(BaseModel):
    user_id: str
    appointment_id: str


class NotifyMessageRequest(BaseModel):
    sender_id: str
    recipient_id: str
    chat_id: str
    message_preview: str


class NotifyCallStartedRequest(BaseModel):
    caller_id: str
    callee_id: str
    appointment_id: str
    call_type: str
    caller_is_doctor: bool
    invitation_id: Optional[str] = None


class CreateCallInvitationRequest(BaseModel):
    caller_id: str
    callee_id: str
    caller_name: str
    callee_name: str
    appointment_id: str
    call_type: str
    caller_is_doctor: bool


class NotifyCallJoinedRequest(BaseModel):
    joiner_id: str
    other_user_id: str
    appointment_id: str


class FileUploadResponse(BaseModel):
    success: bool
    url: str
    file_id: str
    message: str


class SaveMedicalRecordRequest(BaseModel):
    appointment_id: str
    patient_id: str
    doctor_id: str
    patient_name: str
    complaints: Optional[str] = ""
    diagnosis: Optional[str] = ""
    prescription: Optional[str] = ""
    notes: Optional[str] = ""
    follow_up: Optional[str] = ""
    status: str = "finalized"


class ValidateSlotRequest(BaseModel):
    doctor_id: str
    appointment_datetime: str
    duration_minutes: int
    appointment_id: Optional[str] = None


class PresenceRequest(BaseModel):
    user_id: str
    is_online: bool


# ============================================================================
# HELPERS — FCM TOKEN EXTRACTION
# ============================================================================

_FCM_TOKEN_FIELDS = [
    "fcmToken", "FCMToken", "fcm_token",
    "deviceToken", "pushToken", "token",
]

def _get_fcm_token(user_data: Dict[str, Any], uid: str, role: str = "user") -> Optional[str]:
    for field in _FCM_TOKEN_FIELDS:
        value = user_data.get(field)
        if value and isinstance(value, str) and value.strip():
            print(f"✅ FCM token found for {role} {uid} under '{field}': {value[:20]}...")
            return value.strip()
    print(f"⚠️  No FCM token for {role} {uid}. Keys: {list(user_data.keys())}")
    return None


# ============================================================================
# HELPERS — STREAM TOKEN
# ============================================================================

def _generate_stream_token(user_id: str) -> str:
    now = int(time_module.time())

    header = {"alg": "HS256", "typ": "JWT"}

    payload = {
        "iss":     f"stream-video-golang@{STREAM_API_KEY}",
        "sub":     f"user/{user_id}",
        "user_id": user_id,
        "iat":     now,
        "exp":     now + (7 * 24 * 3600),
    }

    def _b64url(data: dict) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(data, separators=(",", ":")).encode("utf-8")
        ).rstrip(b"=").decode("ascii")

    signing_input = f"{_b64url(header)}.{_b64url(payload)}"

    signature = hmac.new(
        STREAM_API_SECRET.encode("utf-8"),
        signing_input.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    signature_enc = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{signing_input}.{signature_enc}"


# ============================================================================
# HELPERS — APPWRITE UPLOAD
# ============================================================================

ALLOWED_DOC_TYPES  = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}
ALLOWED_CHAT_TYPES = {
    "image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf",
    "video/mp4", "video/quicktime", "video/x-matroska",
    "audio/mp4", "audio/aac", "audio/mpeg", "audio/ogg", "audio/webm",
}
EXT_MAP = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/webp": ".webp", "application/pdf": ".pdf", "video/mp4": ".mp4",
    "video/quicktime": ".mov", "video/x-matroska": ".mkv",
}

def _resolve_mime(file: UploadFile, fallback: str = "image/jpeg") -> str:
    if file.content_type and file.content_type != "application/octet-stream":
        return file.content_type
    if file.filename:
        guessed, _ = mimetypes.guess_type(file.filename)
        if guessed:
            return guessed
    return fallback

def _appwrite_view_url(file_id: str, bucket_id: str) -> str:
    return (f"{APPWRITE_ENDPOINT}/storage/buckets/{bucket_id}"
            f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}")

async def _upload_to_appwrite(file, bucket_id, content_type, prefix="file"):
    contents = await file.read()
    ext = EXT_MAP.get(content_type, ".bin")
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name
    try:
        result = appwrite_storage.create_file(
            bucket_id=bucket_id, file_id=ID.unique(),
            file=InputFile.from_path(tmp_path))
        file_id = result["$id"]
        return {"success": True, "file_id": file_id,
                "url": _appwrite_view_url(file_id, bucket_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ============================================================================
# HELPERS — FIREBASE / EMAIL / FCM
# ============================================================================

async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
    for attempt in range(3):
        try:
            doc = db.collection("users").document(uid).get()
            if doc.exists:
                return doc.to_dict()
            print(f"⚠️  User document not found: {uid}")
            return None
        except Exception as e:
            if attempt < 2:
                print(f"⚠️  get_user_data attempt {attempt + 1} failed for {uid}: {e} — retrying")
                await asyncio.sleep(0.5)
            else:
                print(f"❌ get_user_data all retries failed for {uid}: {e}")
                return None
    return None


async def send_fcm(token, title, body, data=None):
    if not token:
        return
    try:
        msg = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data or {},
            token=token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default", channel_id="sheydoc_default")),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(aps=messaging.Aps(sound="default"))),
        )
        messaging.send(msg)
        print(f"✅ FCM sent → {token[:20]}...")
    except Exception as e:
        print(f"❌ FCM failed: {e}")


async def send_email(to_email, to_name, subject, html):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{FROM_NAME} <{SMTP_USER}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
        print(f"✅ Email → {to_email}")
    except Exception as e:
        print(f"❌ Email failed: {e}")


def fmt_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%B %d, %Y at %I:%M %p")
    except Exception:
        return iso

def fmt_date_only(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%B %d, %Y")
    except Exception:
        return iso

def _ts_to_iso(val) -> Optional[str]:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)

def _parse_dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ============================================================================
# EMAIL TEMPLATES
# ============================================================================

def _booking_email(patient, doctor, time_str, reason="", date_only=False):
    time_label = (
        f"<strong>{time_str}</strong>"
        if not date_only
        else f"<strong>{time_str}</strong> (exact time to be confirmed by the doctor)"
    )
    reason_row = f"<p><strong>Reason:</strong> {reason}</p>" if reason else ""
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
    <div style="max-width:600px;margin:auto;padding:20px">
      <div style="background:#4A90E2;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
        <h2>Appointment Confirmed</h2></div>
      <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
        <p>Hi {patient},</p>
        <p>Your appointment with <strong>Dr. {doctor}</strong> is confirmed for {time_label}.</p>
        {reason_row}
        <p>Open the SheydocApp and join from your sessions screen when it's time.</p>
      </div></div></body></html>"""

def _cancel_email(name, doctor, time, by):
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
    <div style="max-width:600px;margin:auto;padding:20px">
      <div style="background:#E74C3C;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
        <h2>Appointment Cancelled</h2></div>
      <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
        <p>Hi {name},</p>
        <p>Your appointment with <strong>Dr. {doctor}</strong> on <strong>{time}</strong>
           was cancelled by the {by}.</p>
        <p>You can rebook anytime via the app.</p>
      </div></div></body></html>"""

def _medical_record_email(patient_name, doctor_name, date_str, diagnosis, prescription):
    diag_row  = f"<tr><td style='padding:8px;font-weight:bold'>Diagnosis</td><td style='padding:8px'>{diagnosis}</td></tr>" if diagnosis else ""
    presc_row = f"<tr><td style='padding:8px;font-weight:bold'>Prescription</td><td style='padding:8px'>{prescription}</td></tr>" if prescription else ""
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
    <div style="max-width:600px;margin:auto;padding:20px">
      <div style="background:#4A90E2;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
        <h2>Medical Record Available</h2></div>
      <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
        <p>Hi {patient_name},</p>
        <p>Dr. <strong>{doctor_name}</strong> has added a medical record from your consultation on <strong>{date_str}</strong>.</p>
        <table style="width:100%;border-collapse:collapse;margin-top:16px;background:white;border-radius:8px">
          {diag_row}{presc_row}
        </table>
        <p style="margin-top:20px">Open the SheydocApp to view your full record.</p>
      </div></div></body></html>"""

def _reminder_email(recipient_name, other_name, time_str, label: str, is_doctor: bool) -> str:
    """FIX: Dedicated reminder email template with clear time-remaining info."""
    role_note = "your patient" if is_doctor else f"Dr. {other_name}"
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
    <div style="max-width:600px;margin:auto;padding:20px">
      <div style="background:#F39C12;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
        <h2>⏰ Appointment Reminder</h2></div>
      <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
        <p>Hi {recipient_name},</p>
        <p>Your appointment with <strong>{role_note}</strong> starts <strong>in {label}</strong>.</p>
        <p>Scheduled time: <strong>{time_str}</strong></p>
        <p>Open the SheydocApp and join from your sessions screen when it's time.</p>
        <p style="color:#888;font-size:13px">Make sure you have a stable internet connection before joining.</p>
      </div></div></body></html>"""


# ============================================================================
# SLOT VALIDATION HELPER
# ============================================================================

def _slots_overlap(start_a: datetime, dur_a: int, start_b: datetime, dur_b: int) -> bool:
    end_a = start_a + timedelta(minutes=dur_a)
    end_b = start_b + timedelta(minutes=dur_b)
    return start_a < end_b and start_b < end_a


async def _validate_slot(
    doctor_id: str,
    apt_dt: datetime,
    duration_minutes: int,
    exclude_id: Optional[str] = None,
    time_confirmed: bool = True,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    if not time_confirmed:
        doctor = await get_user_data(doctor_id)
        if not doctor:
            return {"valid": False, "reason": "Doctor not found."}
        if apt_dt.date() < now.date():
            return {"valid": False, "reason": "Appointment date must be today or in the future."}
        return {"valid": True, "reason": None}

    if apt_dt <= now - timedelta(minutes=5):
        return {"valid": False, "reason": "Appointment must be in the future."}

    doctor = await get_user_data(doctor_id)
    if not doctor:
        return {"valid": False, "reason": "Doctor not found."}

    availability: List[Dict] = doctor.get("availability", [])
    if availability:
        apt_local_dow = apt_dt.weekday()
        apt_start_min = apt_dt.hour * 60 + apt_dt.minute
        apt_end_min   = apt_start_min + duration_minutes

        in_window = False
        for window in availability:
            if window.get("day") != apt_local_dow:
                continue
            win_start = window.get("startHour", 0) * 60 + window.get("startMinute", 0)
            win_end   = window.get("endHour", 23) * 60 + window.get("endMinute", 59)
            if apt_start_min >= win_start and apt_end_min <= win_end:
                in_window = True
                break

        if not in_window:
            return {"valid": False, "reason": "Slot is outside the doctor's available hours."}

    day_start = apt_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = day_start + timedelta(days=1)

    try:
        existing_stream = (
            db.collection("appointments")
              .where("doctorId", "==", doctor_id)
              .where("status", "in", ["confirmed", "pending"])
              .where("appointmentDate", ">=", day_start)
              .where("appointmentDate", "<", day_end)
              .stream()
        )
        for doc in existing_stream:
            if exclude_id and doc.id == exclude_id:
                continue

            data = doc.to_dict()
            if not data.get("timeConfirmed", True):
                continue

            apt_dt_str = data.get("appointmentDateTime", "")
            if not apt_dt_str:
                continue

            try:
                existing_dt  = _parse_dt(apt_dt_str)
                existing_dur = int(data.get("durationMinutes", 30))
            except Exception:
                continue

            if _slots_overlap(apt_dt, duration_minutes, existing_dt, existing_dur):
                return {
                    "valid": False,
                    "reason": f"Doctor already has an appointment at {existing_dt.strftime('%H:%M')}."
                }
    except Exception as e:
        print(f"⚠️  Slot check DB error: {e}")

    return {"valid": True, "reason": None}


# ============================================================================
# CALL FCM HELPER
# ============================================================================

def _send_call_fcm(
    fcm_token: str,
    full_caller_name: str,
    call_type: str,
    appointment_id: str,
    caller_id: str,
    invitation_id: Optional[str],
) -> None:
    """
    FIX: previously sent TWO messages: a data-only message AND a
    'display' message containing a `notification` block. Sending both
    caused Android to auto-render the display message using the
    DEFAULT system channel/sound (bypassing the custom per-ringtone
    channel entirely), while the data-only message is what actually
    triggered the app's own full-screen call UI. The result was either
    a duplicate notification, the wrong sound, or a plain banner instead
    of a real full-screen incoming-call alert.

    Sending ONLY a data-only message is the correct pattern for VoIP-
    style call alerts on Android: it reliably reaches the app's
    background/foreground handlers, giving the app full control to
    render the correctly-channeled, correctly-ringtoned, full-screen
    call UI itself, with nothing else competing with it.
    """
    fcm_data: Dict[str, str] = {
        "type":           "incoming_call",
        "appointment_id": appointment_id,
        "caller_id":      caller_id,
        "caller_name":    full_caller_name,
        "call_type":      call_type,
        "click_action":   "FLUTTER_NOTIFICATION_CLICK",
    }
    if invitation_id:
        fcm_data["invitation_id"] = invitation_id

    data_only = messaging.Message(
        data=fcm_data,
        token=fcm_token,
        android=messaging.AndroidConfig(
            priority="high",
            ttl=60,
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(content_available=True)
            ),
        ),
    )

    try:
        messaging.send(data_only)
        print(f"✅ Call FCM [data-only] sent → {fcm_token[:20]}...")
    except Exception as e:
        print(f"⚠️  Call FCM [data-only] failed (non-fatal): {e}")

# ============================================================================
# REMINDERS
# Windows use tighter bounds to prevent overlap. Each window: (key, lo, hi).
# A reminder fires when diff_min is within [lo, hi).
#
# Reminder schedule:
#   "24h"  → fires when 23h 45m – 24h 15m remain  (window centre: 24h)
#   "1h"   → fires when 55m – 65m remain           (window centre: 1h)
#   "30m"  → fires when 27m – 33m remain           (window centre: 30m)
#   "5m"   → fires when 3m – 8m remain             (window centre: 5m)
# ============================================================================

_REMINDER_WINDOWS = [
    # (key,   lo_min,  hi_min)   — fires when diff_min in [lo, hi)
    ("5m",     3,       8),
    ("30m",   27,      33),
    ("1h",    55,      65),
    ("24h",  1425,    1455),    # 23h45m – 24h15m
]

# Human-readable labels for notifications
_REMINDER_LABELS = {
    "5m":  "5 minutes",
    "30m": "30 minutes",
    "1h":  "1 hour",
    "24h": "24 hours",
}

# Priority ordering — once "5m" is sent we don't send higher windows
_REMINDER_PRIORITY = ["24h", "1h", "30m", "5m"]


async def _check_and_send_reminders(bg: BackgroundTasks) -> int:
    now    = datetime.now(timezone.utc)
    # Look ahead 25 hours to catch the 24h window
    in_25h = now + timedelta(hours=25)

    upcoming = (
        db.collection("appointments")
          .where("status", "==", "confirmed")
          .where("appointmentDateTime", ">=", now.isoformat())
          .where("appointmentDateTime", "<=", in_25h.isoformat())
          .stream()
    )

    sent = 0
    for doc in upcoming:
        appt = doc.to_dict()

        if not appt.get("timeConfirmed", True):
            continue

        try:
            apt_dt = datetime.fromisoformat(
                appt.get("appointmentDateTime", "").replace("Z", "+00:00"))
        except Exception:
            continue

        diff_min = (apt_dt - now).total_seconds() / 60
        last_key = appt.get("lastReminderSent", "")

        # Find which window we're currently in
        target_key = None
        for key, lo, hi in _REMINDER_WINDOWS:
            if lo <= diff_min < hi:
                target_key = key
                break

        if target_key is None:
            continue

        # Don't send a lower-priority reminder if a higher one was already sent.
        # Priority order: 24h < 1h < 30m < 5m (5m is highest priority / last)
        if last_key == target_key:
            # Already sent this window
            continue

        last_priority = _REMINDER_PRIORITY.index(last_key) if last_key in _REMINDER_PRIORITY else -1
        target_priority = _REMINDER_PRIORITY.index(target_key)
        if last_priority >= target_priority:
            # Already sent an equal or higher priority reminder
            continue

        appt_ref = db.collection("appointments").document(doc.id)
        try:
            @firestore.transactional
            def _claim_reminder(transaction, ref, key, current):
                snap = ref.get(transaction=transaction)
                if snap.get("lastReminderSent") != current:
                    return False
                transaction.update(ref, {"lastReminderSent": key})
                return True

            tx      = db.transaction()
            claimed = _claim_reminder(tx, appt_ref, target_key, last_key)
            if not claimed:
                continue
        except Exception as e:
            print(f"⚠️  Reminder claim error for {doc.id}: {e}")
            continue

        label = _REMINDER_LABELS.get(target_key, "soon")
        await _send_reminder_notifications(appt, doc.id, label, bg)
        sent += 1

    print(f"✅ Reminders sent: {sent}")
    return sent


async def _send_reminder_notifications(appt, appt_id, label: str, bg: BackgroundTasks):
    patient = await get_user_data(appt.get("patientId"))
    doctor  = await get_user_data(appt.get("doctorId"))
    if not patient or not doctor:
        return

    pname = patient.get("displayName") or patient.get("name") or patient.get("firstName", "Patient")
    dname = doctor.get("displayName")  or doctor.get("name")  or doctor.get("firstName",  "Doctor")
    atime = fmt_dt(appt.get("appointmentDateTime", ""))

    # Use appointment_id in data so Flutter can deep-link to the session
    fcm_data = {
        "type":           "reminder",
        "appointment_id": appt_id,
        "click_action":   "FLUTTER_NOTIFICATION_CLICK",
    }

    # Notify patient
    if patient_fcm := _get_fcm_token(patient, appt.get("patientId", ""), "patient"):
        bg.add_task(
            send_fcm,
            patient_fcm,
            f"Appointment Reminder ⏰",
            f"Your appointment with Dr. {dname} starts in {label}",
            fcm_data,
        )
    if patient_email := patient.get("email"):
        bg.add_task(
            send_email,
            patient_email,
            pname,
            f"Appointment in {label} — Dr. {dname}",
            _reminder_email(pname, dname, atime, label, is_doctor=False),
        )

    # Notify doctor
    if doctor_fcm := _get_fcm_token(doctor, appt.get("doctorId", ""), "doctor"):
        bg.add_task(
            send_fcm,
            doctor_fcm,
            f"Appointment Reminder ⏰",
            f"You have an appointment with {pname} in {label}",
            fcm_data,
        )
    if doctor_email := doctor.get("email"):
        bg.add_task(
            send_email,
            doctor_email,
            dname,
            f"Appointment in {label} — {pname}",
            _reminder_email(dname, pname, atime, label, is_doctor=True),
        )


# ============================================================================
# HEARTBEAT
# ============================================================================

HEARTBEAT_DOC = "scheduler/heartbeat"

async def _run_reminders_if_due(bg: BackgroundTasks):
    ref = db.document(HEARTBEAT_DOC)

    @firestore.transactional
    def _claim(transaction, ref):
        snap = ref.get(transaction=transaction)
        now  = datetime.now(timezone.utc)
        if snap.exists:
            last = snap.get("lastRun")
            if last and hasattr(last, "replace"):
                last_dt = last.replace(tzinfo=timezone.utc) if last.tzinfo is None else last
                if (now - last_dt).total_seconds() < 240:
                    return False
        transaction.set(ref, {"lastRun": now, "updatedAt": firestore.SERVER_TIMESTAMP})
        return True

    try:
        transaction = db.transaction()
        should_run  = _claim(transaction, ref)
    except Exception as e:
        print(f"⚠️  Heartbeat transaction error: {e}")
        should_run = False

    if should_run:
        print("⏰ Heartbeat: running reminder check")
        await _check_and_send_reminders(bg)
    else:
        print("⏭️  Heartbeat: debounced, skipping")


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.api_route("/", methods=["GET", "HEAD"])
async def root(bg: BackgroundTasks):
    """
    FIX v6.9: This endpoint is what cron-job.org's "Sheydoc Reminders" job
    actually pings every 15 minutes (see job config — URL is the bare root,
    not /heartbeat). Previously this only returned a static health payload
    and never triggered reminder logic, so reminders silently never fired
    in production despite correct window/debounce code.

    Now it also runs the debounced reminder check. _run_reminders_if_due()
    self-throttles via the Firestore heartbeat doc (skips if run < 4 min
    ago), so this is safe to call on every ping regardless of pinger
    frequency, and /heartbeat and /check-reminders keep working exactly
    as before for anyone using those directly.
    """
    await _run_reminders_if_due(bg)
    return {
        "status":    "healthy",
        "service":   "SheydocApp Backend",
        "version":   "6.9.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/heartbeat")
async def heartbeat(bg: BackgroundTasks):
    await _run_reminders_if_due(bg)
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


# ── Stream token ──────────────────────────────────────────────────────────────

@app.post("/stream-token")
async def get_stream_token(req: StreamTokenRequest):
    if not STREAM_API_KEY or not STREAM_API_SECRET:
        raise HTTPException(500, "Stream credentials not configured")

    user_data = await get_user_data(req.user_id)
    if not user_data:
        print(f"⚠️  /stream-token: user doc not found for {req.user_id} — "
              f"issuing token anyway (user may be mid-registration)")

    token = _generate_stream_token(req.user_id)

    print(f"✅ /stream-token issued for user={req.user_id} appointment={req.appointment_id}")
    return {
        "success":    True,
        "token":      token,
        "api_key":    STREAM_API_KEY,
        "call_id":    req.appointment_id,
        "user_id":    req.user_id,
    }


# ── Validate appointment slot ─────────────────────────────────────────────────

@app.post("/validate-slot")
async def validate_slot(req: ValidateSlotRequest):
    try:
        apt_dt = _parse_dt(req.appointment_datetime)
    except Exception:
        raise HTTPException(400, "Invalid appointment_datetime format. Use ISO-8601 UTC.")
    result = await _validate_slot(
        req.doctor_id, apt_dt, req.duration_minutes,
        exclude_id=req.appointment_id,
        time_confirmed=True,
    )
    return result


# ── Available slots ───────────────────────────────────────────────────────────

@app.get("/available-slots/{doctor_id}")
async def get_available_slots(doctor_id: str, date: str, duration: int = 30):
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(400, "date must be YYYY-MM-DD")

    doctor = await get_user_data(doctor_id)
    if not doctor:
        raise HTTPException(404, "Doctor not found")

    availability: List[Dict] = doctor.get("availability", [])
    dow = target_date.weekday()
    windows = [w for w in availability if w.get("day") == dow]
    if not windows:
        return {"success": True, "slots": [], "reason": "Doctor not available on this day"}

    day_end = target_date + timedelta(days=1)
    try:
        booked_stream = (
            db.collection("appointments")
              .where("doctorId", "==", doctor_id)
              .where("status", "in", ["confirmed", "pending"])
              .where("appointmentDate", ">=", target_date)
              .where("appointmentDate", "<", day_end)
              .stream()
        )
        booked = []
        for doc in booked_stream:
            d = doc.to_dict()
            if not d.get("timeConfirmed", True):
                continue
            try:
                booked.append((_parse_dt(d["appointmentDateTime"]), int(d.get("durationMinutes", 30))))
            except Exception:
                pass
    except Exception as e:
        print(f"⚠️  Error fetching booked appointments: {e}")
        booked = []

    now = datetime.now(timezone.utc)
    slots = []

    for window in windows:
        cursor_min = window.get("startHour", 8) * 60 + window.get("startMinute", 0)
        end_min    = window.get("endHour", 17) * 60 + window.get("endMinute", 0)

        while cursor_min + duration <= end_min:
            slot_dt = target_date.replace(
                hour=cursor_min // 60, minute=cursor_min % 60,
                second=0, microsecond=0)
            if slot_dt <= now:
                cursor_min += duration
                continue
            overlaps = any(
                _slots_overlap(slot_dt, duration, b_dt, b_dur)
                for b_dt, b_dur in booked
            )
            if not overlaps:
                slots.append(slot_dt.isoformat())
            cursor_min += duration

    return {"success": True, "slots": slots}


# ── Booking confirmed ─────────────────────────────────────────────────────────

@app.post("/booking-confirmed")
async def booking_confirmed(req: BookingConfirmedRequest, bg: BackgroundTasks):
    try:
        apt_dt = _parse_dt(req.appointment_datetime)
    except Exception:
        raise HTTPException(400, "Invalid appointment_datetime")

    time_confirmed = req.time_confirmed if req.time_confirmed is not None else True

    print(f"📅 booking-confirmed: appt={req.appointment_id} doctor={req.doctor_id} "
          f"patient={req.patient_id} dt={req.appointment_datetime} "
          f"time_confirmed={time_confirmed}")

    slot_check = await _validate_slot(
        req.doctor_id, apt_dt, req.duration_minutes,
        exclude_id=req.appointment_id,
        time_confirmed=time_confirmed,
    )
    if not slot_check["valid"]:
        print(f"  ❌ Slot invalid: {slot_check['reason']}")
        raise HTTPException(409, slot_check["reason"])

    patient = await get_user_data(req.patient_id)
    doctor  = await get_user_data(req.doctor_id)
    if not patient or not doctor:
        raise HTTPException(404, "User not found")

    pname  = patient.get("name") or patient.get("displayName") or patient.get("firstName", "Patient")
    dname  = doctor.get("name")  or doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
    reason = req.reason_for_consultation or ""
    data   = {"type": "booking_confirmed", "appointment_id": req.appointment_id}
    atime  = fmt_dt(req.appointment_datetime) if time_confirmed else fmt_date_only(req.appointment_datetime)

    if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
        bg.add_task(send_fcm, fcm, "Appointment Confirmed ✅",
                    f"Your appointment with Dr. {dname} on {atime} is confirmed!", data)

    if fcm := _get_fcm_token(doctor, req.doctor_id, "doctor"):
        body = f"New appointment from {pname} for {atime}"
        if reason:
            body += f" — {reason[:60]}"
        bg.add_task(send_fcm, fcm, "New Appointment Request 📅", body, data)

    if email := patient.get("email"):
        bg.add_task(send_email, email, pname, "Appointment Confirmed",
                    _booking_email(pname, dname, atime, reason, date_only=not time_confirmed))
    if email := doctor.get("email"):
        bg.add_task(send_email, email, dname, "New Appointment Request",
                    _booking_email(dname, pname, atime, reason, date_only=not time_confirmed))

    print(f"✅ booking-confirmed success: patient={req.patient_id} doctor={req.doctor_id}")
    return {"success": True}


# ── Appointment cancelled ─────────────────────────────────────────────────────

@app.post("/appointment-canceled")
async def appointment_canceled(req: AppointmentCanceledRequest, bg: BackgroundTasks):
    patient = await get_user_data(req.patient_id)
    doctor  = await get_user_data(req.doctor_id)
    if not patient or not doctor:
        raise HTTPException(404, "User not found")

    pname = patient.get("name") or patient.get("displayName") or patient.get("firstName", "Patient")
    dname = doctor.get("name")  or doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
    atime = fmt_dt(req.appointment_datetime)
    data  = {"type": "appointment_canceled", "appointment_id": req.appointment_id}

    if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
        bg.add_task(send_fcm, fcm, "Appointment Cancelled",
                    f"Your appointment with Dr. {dname} was cancelled", data)
    if fcm := _get_fcm_token(doctor, req.doctor_id, "doctor"):
        bg.add_task(send_fcm, fcm, "Appointment Cancelled",
                    f"Appointment with {pname} was cancelled", data)
    if email := patient.get("email"):
        bg.add_task(send_email, email, pname, "Appointment Cancelled",
                    _cancel_email(pname, dname, atime, req.canceled_by))
    if email := doctor.get("email"):
        bg.add_task(send_email, email, dname, "Appointment Cancelled",
                    _cancel_email(dname, pname, atime, req.canceled_by))

    return {"success": True}


# ── Notify message ────────────────────────────────────────────────────────────

@app.post("/notify-message")
async def notify_message(req: NotifyMessageRequest, bg: BackgroundTasks):
    print(f"📨 notify-message: sender={req.sender_id} -> recipient={req.recipient_id} "
          f"preview='{req.message_preview[:40]}'")

    sender    = await get_user_data(req.sender_id)
    recipient = await get_user_data(req.recipient_id)

    if not sender:
        return {"success": True, "note": "sender data missing, skipped"}
    if not recipient:
        return {"success": True, "note": "recipient data missing, skipped"}

    sender_name = (
        sender.get("name") or sender.get("displayName") or
        sender.get("firstName") or "Someone"
    )
    fcm_data = {
        "type":         "new_message",
        "chat_id":      req.chat_id,
        "sender_id":    req.sender_id,
        "sender_name":  sender_name,
        "click_action": "FLUTTER_NOTIFICATION_CLICK",
    }
    if fcm := _get_fcm_token(recipient, req.recipient_id, "recipient"):
        bg.add_task(send_fcm, fcm, sender_name, req.message_preview, fcm_data)
    else:
        print(f"⚠️  Push skipped for recipient {req.recipient_id}.")

    return {"success": True}


# ── Notify call started ───────────────────────────────────────────────────────

@app.post("/notify-call-started")
async def notify_call_started(req: NotifyCallStartedRequest, bg: BackgroundTasks):
    caller = await get_user_data(req.caller_id)
    callee = await get_user_data(req.callee_id)
    if not caller or not callee:
        return {"success": True, "note": "user data missing, skipped"}

    caller_name = (
        caller.get("name") or caller.get("displayName") or
        caller.get("firstName", "Someone")
    )
    prefix    = "Dr. " if req.caller_is_doctor else ""
    full_name = f"{prefix}{caller_name}"

    fcm_token = _get_fcm_token(callee, req.callee_id, "callee")
    if not fcm_token:
        return {"success": True, "note": "no FCM token for callee"}

    bg.add_task(
        _send_call_fcm,
        fcm_token,
        full_name,
        req.call_type,
        req.appointment_id,
        req.caller_id,
        req.invitation_id,
    )

    return {"success": True}


# ── Create call invitation ────────────────────────────────────────────────────

@app.post("/create-call-invitation")
async def create_call_invitation(req: CreateCallInvitationRequest, bg: BackgroundTasks):
    inv_ref = db.collection("call_invitations").document()
    inv_ref.set({
        "callerId":      req.caller_id,
        "callerName":    req.caller_name,
        "receiverId":    req.callee_id,
        "receiverName":  req.callee_name,
        "callType":      req.call_type,
        "appointmentId": req.appointment_id,
        "status":        "pending",
        "createdAt":     firestore.SERVER_TIMESTAMP,
    })
    invitation_id = inv_ref.id
    print(f"✅ call_invitation created: {invitation_id}")

    caller = await get_user_data(req.caller_id)
    callee = await get_user_data(req.callee_id)
    if not caller or not callee:
        return {"success": True, "invitation_id": invitation_id,
                "note": "user data missing — FCM skipped"}

    caller_name = (
        caller.get("name") or caller.get("displayName") or
        caller.get("firstName", req.caller_name)
    )
    prefix    = "Dr. " if req.caller_is_doctor else ""
    full_name = f"{prefix}{caller_name}"

    fcm_token = _get_fcm_token(callee, req.callee_id, "callee")
    if fcm_token:
        bg.add_task(
            _send_call_fcm,
            fcm_token,
            full_name,
            req.call_type,
            req.appointment_id,
            req.caller_id,
            invitation_id,
        )

    return {"success": True, "invitation_id": invitation_id}


# ── Notify call joined ────────────────────────────────────────────────────────

@app.post("/notify-call-joined")
async def notify_call_joined(req: NotifyCallJoinedRequest, bg: BackgroundTasks):
    joiner     = await get_user_data(req.joiner_id)
    other_user = await get_user_data(req.other_user_id)
    if not joiner or not other_user:
        return {"success": True, "note": "user data missing, skipped"}

    joiner_name = (
        joiner.get("name") or joiner.get("displayName") or
        joiner.get("firstName", "Someone")
    )
    fcm_data = {
        "type":           "call_joined",
        "appointment_id": req.appointment_id,
        "joiner_id":      req.joiner_id,
        "click_action":   "FLUTTER_NOTIFICATION_CLICK",
    }
    if fcm := _get_fcm_token(other_user, req.other_user_id, "other_user"):
        bg.add_task(send_fcm, fcm, "Patient Joined",
                    f"{joiner_name} has joined the call", fcm_data)

    return {"success": True}


# ── Online presence ───────────────────────────────────────────────────────────

@app.post("/presence")
async def update_presence(req: PresenceRequest):
    try:
        db.collection("users").document(req.user_id).update({
            "isOnline": req.is_online,
            "lastSeen": firestore.SERVER_TIMESTAMP,
        })
        return {"success": True}
    except Exception as e:
        print(f"❌ Presence update error: {e}")
        raise HTTPException(500, f"Presence update failed: {e}")


@app.get("/presence/{user_id}")
async def get_presence(user_id: str):
    user = await get_user_data(user_id)
    if not user:
        raise HTTPException(404, "User not found")
    return {
        "success":  True,
        "isOnline": user.get("isOnline", False),
        "lastSeen": _ts_to_iso(user.get("lastSeen")),
    }


# ── Upload document ───────────────────────────────────────────────────────────

@app.post("/upload-document", response_model=FileUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form(...),
):
    content_type = _resolve_mime(file)
    if content_type not in ALLOWED_DOC_TYPES:
        raise HTTPException(400, f"File type '{content_type}' not allowed.")
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    if size > 10 * 1024 * 1024:
        raise HTTPException(400, "File too large. Max 10MB.")
    result = await _upload_to_appwrite(file, APPWRITE_BUCKET_ID, content_type, "doc")
    camel = file_type.replace("_", " ").title().replace(" ", "")
    key = f"{camel[0].lower()}{camel[1:]}Url"
    db.collection("users").document(user_id).set({key: result["url"]}, merge=True)
    return FileUploadResponse(success=True, url=result["url"],
                              file_id=result["file_id"], message="Uploaded successfully")


# ── Upload chat media ─────────────────────────────────────────────────────────

@app.post("/upload-chat-media", response_model=FileUploadResponse)
async def upload_chat_media(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    media_type: str = Form(...),
):
    content_type = _resolve_mime(
        file, fallback="video/mp4" if media_type == "video" else "image/jpeg")
    if content_type not in ALLOWED_CHAT_TYPES:
        raise HTTPException(400, f"Unsupported type: {content_type}")
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    max_mb = 50 if media_type == "video" else 25 if media_type == "audio" else 10
    if size > max_mb * 1024 * 1024:
        raise HTTPException(400, f"File too large. Max {max_mb}MB.")
    result = await _upload_to_appwrite(
        file, APPWRITE_CHAT_BUCKET_ID, content_type, f"chat_{media_type}")
    return FileUploadResponse(success=True, url=result["url"],
                              file_id=result["file_id"],
                              message=f"Chat {media_type} uploaded")


# ── Delete doctor files ───────────────────────────────────────────────────────

@app.delete("/delete-doctor-files/{doctor_id}")
async def delete_doctor_files(doctor_id: str):
    FILE_ID_FIELDS = ["educationCertificateFileId", "authorizationFileFileId",
                      "affiliateHospitalFileFileId", "idCardFileFileId"]
    URL_FIELDS     = ["educationCertificateUrl", "authorizationFileUrl",
                      "affiliateHospitalFileUrl", "idCardFileUrl"]
    doc_ref  = db.collection("users").document(doctor_id)
    doc_snap = doc_ref.get()
    if not doc_snap.exists:
        raise HTTPException(404, "Doctor not found")
    data = doc_snap.to_dict()
    deleted, failed = [], []
    for field in FILE_ID_FIELDS:
        fid = data.get(field)
        if not fid:
            continue
        try:
            appwrite_storage.delete_file(APPWRITE_BUCKET_ID, fid)
            deleted.append(fid)
        except Exception:
            failed.append(fid)
    clear = {f: firestore.DELETE_FIELD for f in FILE_ID_FIELDS + URL_FIELDS}
    doc_ref.update(clear)
    return {"success": True, "deleted": deleted, "failed": failed}


# ============================================================================
# MEDICAL RECORDS
# ============================================================================

@app.post("/save-medical-record")
async def save_medical_record(req: SaveMedicalRecordRequest, bg: BackgroundTasks):
    record_data = {
        "appointmentId":  req.appointment_id,
        "patientId":      req.patient_id,
        "doctorId":       req.doctor_id,
        "patientName":    req.patient_name,
        "complaints":     req.complaints or "",
        "diagnosis":      req.diagnosis or "",
        "prescription":   req.prescription or "",
        "notes":          req.notes or "",
        "followUp":       req.follow_up or "",
        "status":         req.status,
        "updatedAt":      firestore.SERVER_TIMESTAMP,
    }

    existing_stream = (
        db.collection("medical_records")
          .where("appointmentId", "==", req.appointment_id)
          .limit(1).stream()
    )
    existing_docs = list(existing_stream)

    if existing_docs:
        existing_docs[0].reference.update(record_data)
        record_id = existing_docs[0].id
    else:
        record_data["createdAt"] = firestore.SERVER_TIMESTAMP
        record_ref = db.collection("medical_records").document()
        record_ref.set(record_data)
        record_id = record_ref.id

    db.collection("appointments").document(req.appointment_id).update({
        "hasRecord": True, "recordId": record_id,
    })

    if req.status == "finalized":
        patient = await get_user_data(req.patient_id)
        doctor  = await get_user_data(req.doctor_id)
        if patient and doctor:
            dname    = doctor.get("name") or doctor.get("displayName") or doctor.get("firstName") or "Your doctor"
            pname    = patient.get("name") or patient.get("displayName") or patient.get("firstName") or "Patient"
            date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
            notif_data = {
                "type":           "medical_record",
                "record_id":      record_id,
                "appointment_id": req.appointment_id,
                "click_action":   "FLUTTER_NOTIFICATION_CLICK",
            }
            if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
                bg.add_task(send_fcm, fcm, "Medical Record Available",
                            f"Dr. {dname} has added notes from your consultation.",
                            notif_data)
            if email := patient.get("email"):
                bg.add_task(send_email, email, pname,
                            f"Medical Record from Dr. {dname}",
                            _medical_record_email(pname, dname, date_str,
                                                  req.diagnosis or "",
                                                  req.prescription or ""))
            db.collection("notifications").add({
                "userId":        req.patient_id,
                "title":         "Medical Record Available",
                "body":          f"Dr. {dname} added notes from your consultation.",
                "type":          "medical_record",
                "recordId":      record_id,
                "appointmentId": req.appointment_id,
                "createdAt":     firestore.SERVER_TIMESTAMP,
                "read":          False,
            })

    return {"success": True, "record_id": record_id}


@app.get("/medical-records/{patient_id}")
async def get_patient_records(patient_id: str):
    records_stream = (
        db.collection("medical_records")
          .where("patientId", "==", patient_id)
          .where("status", "==", "finalized")
          .order_by("createdAt", direction=firestore.Query.DESCENDING)
          .stream()
    )
    result = []
    for doc in records_stream:
        d = doc.to_dict()
        d["id"] = doc.id
        d["createdAt"] = _ts_to_iso(d.get("createdAt"))
        d["updatedAt"] = _ts_to_iso(d.get("updatedAt"))
        result.append(d)
    return {"success": True, "records": result, "count": len(result)}


@app.get("/medical-records/appointment/{appointment_id}")
async def get_appointment_record(appointment_id: str):
    records_stream = (
        db.collection("medical_records")
          .where("appointmentId", "==", appointment_id)
          .limit(1).stream()
    )
    docs = list(records_stream)
    if not docs:
        return {"success": True, "record": None}
    d = docs[0].to_dict()
    d["id"] = docs[0].id
    d["createdAt"] = _ts_to_iso(d.get("createdAt"))
    d["updatedAt"] = _ts_to_iso(d.get("updatedAt"))
    return {"success": True, "record": d}


# ── Reminders ─────────────────────────────────────────────────────────────────

@app.get("/check-reminders")
async def check_reminders(bg: BackgroundTasks):
    sent = await _check_and_send_reminders(bg)
    return {"success": True, "reminders_sent": sent}


# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


