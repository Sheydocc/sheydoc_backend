"""
TeleMed FastAPI Backend v6.6
Changes from v6.5:
  - /notify-call-started now sends TWO FCM messages:
      1. Data-only (wakes killed Android app via background handler)
      2. Display notification (banner when app is backgrounded)
  - /notify-call-started accepts optional invitation_id and includes it
    in the FCM payload so Flutter can accept/decline via Firestore.
  - New POST /create-call-invitation:
      Creates the call_invitations Firestore doc AND fires FCM in one call.
  - All previous v6.5 fixes retained.
"""

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
    version="6.6.0",
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
    call_type: str              # 'video' or 'audio'
    caller_is_doctor: bool
    invitation_id: Optional[str] = None  # Firestore call_invitations doc ID


class CreateCallInvitationRequest(BaseModel):
    caller_id: str
    callee_id: str
    caller_name: str
    callee_name: str
    appointment_id: str
    call_type: str              # 'video' or 'audio'
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
    header  = {"alg": "HS256", "typ": "JWT"}
    now     = int(time_module.time())
    payload = {"user_id": user_id, "iat": now, "exp": now + (7 * 24 * 3600)}

    def b64url(d):
        return base64.urlsafe_b64encode(
            json.dumps(d, separators=(",", ":")).encode()
        ).rstrip(b"=").decode()

    si  = f"{b64url(header)}.{b64url(payload)}"
    sig = hmac.new(STREAM_API_SECRET.encode(), si.encode(), hashlib.sha256).digest()
    return f"{si}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"


# ============================================================================
# HELPERS — APPWRITE UPLOAD
# ============================================================================

ALLOWED_DOC_TYPES  = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}
ALLOWED_CHAT_TYPES = {
    "image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf",
    "video/mp4", "video/quicktime", "video/x-matroska",
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
    try:
        doc = db.collection("users").document(uid).get()
        if doc.exists:
            return doc.to_dict()
        print(f"⚠️  User document not found: {uid}")
        return None
    except Exception as e:
        print(f"❌ Error fetching user {uid}: {e}")
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

    # Future check (allow 5 min buffer for network delays)
    if apt_dt <= now - timedelta(minutes=5):
        return {"valid": False, "reason": "Appointment must be in the future."}

    doctor = await get_user_data(doctor_id)
    if not doctor:
        return {"valid": False, "reason": "Doctor not found."}

    # Availability window check
    availability: List[Dict] = doctor.get("availability", [])
    if availability:
        apt_local_dow = apt_dt.weekday()  # Mon=0…Sun=6
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

    # Double-booking check
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
            # Always skip the appointment being confirmed (v6.5 fix)
            if exclude_id and doc.id == exclude_id:
                print(f"  ↳ Skipping self-conflict for appointment {doc.id}")
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
                print(f"  ⚠️  Conflict: {apt_dt} overlaps {existing_dt} (appt {doc.id})")
                return {
                    "valid": False,
                    "reason": f"Doctor already has an appointment at {existing_dt.strftime('%H:%M')}."
                }
    except Exception as e:
        print(f"⚠️  Slot check DB error: {e}")

    return {"valid": True, "reason": None}


# ============================================================================
# CALL FCM HELPER — sends data-only + display message pair
# ============================================================================

def _send_call_fcm(fcm_token: str, full_caller_name: str, call_type: str,
                   appointment_id: str, caller_id: str,
                   invitation_id: Optional[str]) -> None:
    """
    Sends two FCM messages for an incoming call:
      1. Data-only (priority=high) — wakes a killed Android app via the
         background handler so it can show its own local notification.
      2. Display notification — shown as a banner when the app is backgrounded.

    Both messages carry the full call payload so the receiver has everything
    it needs regardless of which message it processes first.
    """
    call_label = "Video" if call_type == "video" else "Audio"

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

    # ── Message 1: Data-only (wakes killed app) ───────────────────────────────
    data_only = messaging.Message(
        data=fcm_data,
        token=fcm_token,
        android=messaging.AndroidConfig(
            priority="high",
            ttl=60,          # expire after 60 s — stale call is useless
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(content_available=True)
            ),
        ),
    )

    # ── Message 2: Display notification (banner when app is backgrounded) ─────
    display = messaging.Message(
        notification=messaging.Notification(
            title=f"Incoming {call_label} Call",
            body=f"{full_caller_name} is calling you",
        ),
        data=fcm_data,
        token=fcm_token,
        android=messaging.AndroidConfig(
            priority="high",
            ttl=60,
            notification=messaging.AndroidNotification(
                sound="default",
                channel_id="sheydoc_calls",
                # full_screen_intent opens the ringing UI on a locked screen.
                # Requires android.permission.USE_FULL_SCREEN_INTENT in manifest.
            ),
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(sound="default")
            ),
        ),
    )

    for label, msg in [("data-only", data_only), ("display", display)]:
        try:
            messaging.send(msg)
            print(f"✅ Call FCM [{label}] sent → {fcm_token[:20]}...")
        except Exception as e:
            print(f"⚠️  Call FCM [{label}] failed (non-fatal): {e}")


# ============================================================================
# REMINDERS
# ============================================================================

_REMINDER_WINDOWS = [
    ("5m",  0,        7),
    ("1h",  55,       70),
    ("24h", 23 * 60,  25 * 60),
]

async def _check_and_send_reminders(bg: BackgroundTasks) -> int:
    now    = datetime.now(timezone.utc)
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

        target_key = None
        for key, lo, hi in _REMINDER_WINDOWS:
            if lo <= diff_min <= hi:
                target_key = key
                break

        if target_key is None:
            continue
        if last_key == target_key:
            continue
        if last_key == "5m":
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

        label = {"5m": "5 minutes", "1h": "1 hour", "24h": "24 hours"}.get(target_key, "soon")
        await _send_reminder_notifications(appt, doc.id, label, bg)
        sent += 1

    print(f"✅ Reminders sent: {sent}")
    return sent


async def _send_reminder_notifications(appt, appt_id, label: str, bg: BackgroundTasks):
    patient = await get_user_data(appt.get("patientId"))
    doctor  = await get_user_data(appt.get("doctorId"))
    if not patient or not doctor:
        return
    pname = patient.get("displayName") or patient.get("firstName", "Patient")
    dname = doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
    atime = fmt_dt(appt.get("appointmentDateTime", ""))
    data  = {"type": "reminder", "appointment_id": appt_id}

    for user, uid, other_name, role in [
        (patient, appt.get("patientId"), f"Dr. {dname}", "patient"),
        (doctor,  appt.get("doctorId"),  pname,          "doctor"),
    ]:
        if fcm := _get_fcm_token(user, uid or "", role):
            bg.add_task(
                send_fcm, fcm,
                f"Appointment in {label}",
                f"Your appointment with {other_name} is at {atime}",
                data,
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
async def root():
    return {
        "status":    "healthy",
        "service":   "SheydocApp Backend",
        "version":   "6.6.0",
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
        raise HTTPException(404, "User not found")
    token = _generate_stream_token(req.user_id)
    return {"success": True, "token": token, "api_key": STREAM_API_KEY,
            "call_id": req.appointment_id, "user_id": req.user_id}


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
        exclude_id=req.appointment_id,   # always exclude self (v6.5 fix)
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
    prefix     = "Dr. " if req.caller_is_doctor else ""
    full_name  = f"{prefix}{caller_name}"

    fcm_token = _get_fcm_token(callee, req.callee_id, "callee")
    if not fcm_token:
        return {"success": True, "note": "no FCM token for callee"}

    # Send data-only + display FCM pair in background
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


# ── Create call invitation (Firestore doc + FCM in one call) ──────────────────

@app.post("/create-call-invitation")
async def create_call_invitation(req: CreateCallInvitationRequest, bg: BackgroundTasks):
    """
    Creates the call_invitations Firestore document and fires FCM to the callee.
    The Flutter doctor side calls this instead of separately creating the
    Firestore doc client-side and then calling /notify-call-started.

    Returns the invitation_id so the doctor-side Flutter code can listen to
    the document for status changes (accepted/declined/cancelled).
    """
    # 1. Write Firestore document
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

    # 2. Get caller info for the display name
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
    max_mb = 50 if media_type == "video" else 10
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














# """
# TeleMed FastAPI Backend v6.5
# Fix from v6.4:
#   - /booking-confirmed was returning 409 even for valid new bookings because:
#     1. The Flutter app writes the appointment to Firestore BEFORE calling
#        /booking-confirmed, so the overlap check found the appointment itself
#        as a conflict. Fix: always exclude the current appointment_id from the
#        double-booking check in _validate_slot().
#     2. The Firestore query used "appointmentDateTime" (ISO string) range
#        comparisons which are unreliable. Now also checks appointmentDate
#        (Timestamp) for the day range to catch all same-day appointments.
#   - Improved logging so 409 conflicts print WHY they conflicted.
# """

# import os
# import mimetypes
# import smtplib
# import tempfile
# import json
# import base64
# import hmac
# import hashlib
# import time as time_module
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from datetime import datetime, timedelta, timezone
# from typing import Optional, Dict, Any, List
# from dotenv import load_dotenv

# from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel

# import firebase_admin
# from firebase_admin import credentials, firestore, messaging

# from appwrite.client import Client
# from appwrite.services.storage import Storage
# from appwrite.input_file import InputFile
# from appwrite.id import ID




# load_dotenv()

# # ============================================================================
# # CONFIG
# # ============================================================================

# SMTP_HOST         = os.getenv("SMTP_HOST", "smtp.gmail.com")
# SMTP_PORT         = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER         = os.getenv("SMTP_USER")
# SMTP_PASSWORD     = os.getenv("SMTP_PASSWORD")
# FROM_NAME         = os.getenv("FROM_NAME", "SheydocApp")

# APPWRITE_ENDPOINT       = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
# APPWRITE_PROJECT_ID     = os.getenv("APPWRITE_PROJECT_ID")
# APPWRITE_API_KEY        = os.getenv("APPWRITE_API_KEY")
# APPWRITE_BUCKET_ID      = os.getenv("APPWRITE_BUCKET_ID")
# APPWRITE_CHAT_BUCKET_ID = os.getenv("APPWRITE_CHAT_BUCKET_ID", APPWRITE_BUCKET_ID)

# STREAM_API_KEY    = os.getenv("STREAM_API_KEY")
# STREAM_API_SECRET = os.getenv("STREAM_API_SECRET")

# appwrite_client = Client()
# appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
# appwrite_client.set_project(APPWRITE_PROJECT_ID)
# appwrite_client.set_key(APPWRITE_API_KEY)
# appwrite_storage = Storage(appwrite_client)

# # ============================================================================
# # FASTAPI APP
# # ============================================================================

# app = FastAPI(
#     title="SheydocApp Backend",
#     description="Notifications, email, file uploads, Stream Video tokens, Medical Records, Slot Validation",
#     version="6.5.0",
# )
# from routers.payment import router as payment_router
# app.include_router(payment_router)
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# firebase_admin.initialize_app(cred)
# db = firestore.client()


# # ============================================================================
# # PYDANTIC MODELS
# # ============================================================================

# class BookingConfirmedRequest(BaseModel):
#     appointment_id: str
#     patient_id: str
#     doctor_id: str
#     appointment_datetime: str
#     duration_minutes: int
#     reason_for_consultation: Optional[str] = ""
#     time_confirmed: Optional[bool] = True  # default True — we always confirm time now


# class AppointmentCanceledRequest(BaseModel):
#     appointment_id: str
#     patient_id: str
#     doctor_id: str
#     canceled_by: str
#     appointment_datetime: str

# class StreamTokenRequest(BaseModel):
#     user_id: str
#     appointment_id: str

# class NotifyMessageRequest(BaseModel):
#     sender_id: str
#     recipient_id: str
#     chat_id: str
#     message_preview: str

# class NotifyCallStartedRequest(BaseModel):
#     caller_id: str
#     callee_id: str
#     appointment_id: str
#     call_type: str
#     caller_is_doctor: bool

# class NotifyCallJoinedRequest(BaseModel):
#     joiner_id: str
#     other_user_id: str
#     appointment_id: str

# class FileUploadResponse(BaseModel):
#     success: bool
#     url: str
#     file_id: str
#     message: str

# class SaveMedicalRecordRequest(BaseModel):
#     appointment_id: str
#     patient_id: str
#     doctor_id: str
#     patient_name: str
#     complaints: Optional[str] = ""
#     diagnosis: Optional[str] = ""
#     prescription: Optional[str] = ""
#     notes: Optional[str] = ""
#     follow_up: Optional[str] = ""
#     status: str = "finalized"

# class ValidateSlotRequest(BaseModel):
#     doctor_id: str
#     appointment_datetime: str
#     duration_minutes: int
#     appointment_id: Optional[str] = None

# class PresenceRequest(BaseModel):
#     user_id: str
#     is_online: bool


# # ============================================================================
# # HELPERS — FCM TOKEN EXTRACTION
# # ============================================================================

# _FCM_TOKEN_FIELDS = [
#     "fcmToken", "FCMToken", "fcm_token",
#     "deviceToken", "pushToken", "token",
# ]

# def _get_fcm_token(user_data: Dict[str, Any], uid: str, role: str = "user") -> Optional[str]:
#     for field in _FCM_TOKEN_FIELDS:
#         value = user_data.get(field)
#         if value and isinstance(value, str) and value.strip():
#             print(f"✅ FCM token found for {role} {uid} under field '{field}': {value[:20]}...")
#             return value.strip()
#     all_keys = list(user_data.keys())
#     print(f"⚠️  No FCM token found for {role} {uid}. Tried: {_FCM_TOKEN_FIELDS}. Keys: {all_keys}")
#     return None


# # ============================================================================
# # HELPERS — STREAM TOKEN
# # ============================================================================

# def _generate_stream_token(user_id: str) -> str:
#     header  = {"alg": "HS256", "typ": "JWT"}
#     now     = int(time_module.time())
#     payload = {"user_id": user_id, "iat": now, "exp": now + (7 * 24 * 3600)}

#     def b64url(d):
#         return base64.urlsafe_b64encode(
#             json.dumps(d, separators=(",", ":")).encode()
#         ).rstrip(b"=").decode()

#     si  = f"{b64url(header)}.{b64url(payload)}"
#     sig = hmac.new(STREAM_API_SECRET.encode(), si.encode(), hashlib.sha256).digest()
#     return f"{si}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"


# # ============================================================================
# # HELPERS — APPWRITE UPLOAD
# # ============================================================================

# ALLOWED_DOC_TYPES  = {"image/jpeg","image/jpg","image/png","image/webp","application/pdf"}
# ALLOWED_CHAT_TYPES = {
#     "image/jpeg","image/jpg","image/png","image/webp","application/pdf",
#     "video/mp4","video/quicktime","video/x-matroska",
# }
# EXT_MAP = {
#     "image/jpeg":".jpg","image/jpg":".jpg","image/png":".png","image/webp":".webp",
#     "application/pdf":".pdf","video/mp4":".mp4","video/quicktime":".mov",
#     "video/x-matroska":".mkv",
# }

# def _resolve_mime(file: UploadFile, fallback: str = "image/jpeg") -> str:
#     if file.content_type and file.content_type != "application/octet-stream":
#         return file.content_type
#     if file.filename:
#         guessed, _ = mimetypes.guess_type(file.filename)
#         if guessed:
#             return guessed
#     return fallback

# def _appwrite_view_url(file_id: str, bucket_id: str) -> str:
#     return (f"{APPWRITE_ENDPOINT}/storage/buckets/{bucket_id}"
#             f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}")

# async def _upload_to_appwrite(file, bucket_id, content_type, prefix="file"):
#     contents = await file.read()
#     ext = EXT_MAP.get(content_type, ".bin")
#     with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
#         tmp.write(contents)
#         tmp_path = tmp.name
#     try:
#         result = appwrite_storage.create_file(
#             bucket_id=bucket_id, file_id=ID.unique(),
#             file=InputFile.from_path(tmp_path))
#         file_id = result["$id"]
#         return {"success": True, "file_id": file_id,
#                 "url": _appwrite_view_url(file_id, bucket_id)}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
#     finally:
#         if os.path.exists(tmp_path):
#             os.remove(tmp_path)


# # ============================================================================
# # HELPERS — FIREBASE / EMAIL / FCM
# # ============================================================================

# async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
#     try:
#         doc = db.collection("users").document(uid).get()
#         if doc.exists:
#             return doc.to_dict()
#         print(f"⚠️  User document not found for uid: {uid}")
#         return None
#     except Exception as e:
#         print(f"❌ Error fetching user {uid}: {e}")
#         return None

# async def send_fcm(token, title, body, data=None):
#     if not token:
#         return
#     try:
#         msg = messaging.Message(
#             notification=messaging.Notification(title=title, body=body),
#             data=data or {},
#             token=token,
#             android=messaging.AndroidConfig(
#                 priority="high",
#                 notification=messaging.AndroidNotification(
#                     sound="default", channel_id="sheydoc_default")),
#             apns=messaging.APNSConfig(
#                 payload=messaging.APNSPayload(aps=messaging.Aps(sound="default"))),
#         )
#         messaging.send(msg)
#         print(f"✅ FCM sent → {token[:20]}...")
#     except Exception as e:
#         print(f"❌ FCM failed: {e}")

# async def send_email(to_email, to_name, subject, html):
#     try:
#         msg = MIMEMultipart("alternative")
#         msg["Subject"] = subject
#         msg["From"]    = f"{FROM_NAME} <{SMTP_USER}>"
#         msg["To"]      = to_email
#         msg.attach(MIMEText(html, "html"))
#         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
#             s.starttls()
#             s.login(SMTP_USER, SMTP_PASSWORD)
#             s.send_message(msg)
#         print(f"✅ Email → {to_email}")
#     except Exception as e:
#         print(f"❌ Email failed: {e}")

# def fmt_dt(iso: str) -> str:
#     try:
#         dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
#         return dt.strftime("%B %d, %Y at %I:%M %p")
#     except Exception:
#         return iso

# def fmt_date_only(iso: str) -> str:
#     try:
#         dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
#         return dt.strftime("%B %d, %Y")
#     except Exception:
#         return iso

# def _ts_to_iso(val) -> Optional[str]:
#     if val is None:
#         return None
#     if hasattr(val, "isoformat"):
#         return val.isoformat()
#     return str(val)

# def _parse_dt(iso: str) -> datetime:
#     return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# # ============================================================================
# # EMAIL TEMPLATES
# # ============================================================================

# def _booking_email(patient, doctor, time_str, reason="", date_only=False):
#     time_label = (
#         f"<strong>{time_str}</strong>"
#         if not date_only
#         else f"<strong>{time_str}</strong> (exact time to be confirmed by the doctor)"
#     )
#     reason_row = f"<p><strong>Reason:</strong> {reason}</p>" if reason else ""
#     return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
#     <div style="max-width:600px;margin:auto;padding:20px">
#       <div style="background:#4A90E2;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
#         <h2>Appointment Confirmed</h2></div>
#       <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
#         <p>Hi {patient},</p>
#         <p>Your appointment with <strong>Dr. {doctor}</strong> is confirmed for {time_label}.</p>
#         {reason_row}
#         <p>Open the SheydocApp and join from your sessions screen when it's time.</p>
#       </div></div></body></html>"""

# def _cancel_email(name, doctor, time, by):
#     return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
#     <div style="max-width:600px;margin:auto;padding:20px">
#       <div style="background:#E74C3C;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
#         <h2>Appointment Cancelled</h2></div>
#       <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
#         <p>Hi {name},</p>
#         <p>Your appointment with <strong>Dr. {doctor}</strong> on <strong>{time}</strong>
#            was cancelled by the {by}.</p>
#         <p>You can rebook anytime via the app.</p>
#       </div></div></body></html>"""

# def _medical_record_email(patient_name, doctor_name, date_str, diagnosis, prescription):
#     diag_row  = f"<tr><td style='padding:8px;font-weight:bold'>Diagnosis</td><td style='padding:8px'>{diagnosis}</td></tr>" if diagnosis else ""
#     presc_row = f"<tr><td style='padding:8px;font-weight:bold'>Prescription</td><td style='padding:8px'>{prescription}</td></tr>" if prescription else ""
#     return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
#     <div style="max-width:600px;margin:auto;padding:20px">
#       <div style="background:#4A90E2;padding:20px;border-radius:8px 8px 0 0;color:white;text-align:center">
#         <h2>Medical Record Available</h2></div>
#       <div style="background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px">
#         <p>Hi {patient_name},</p>
#         <p>Dr. <strong>{doctor_name}</strong> has added a medical record from your consultation on <strong>{date_str}</strong>.</p>
#         <table style="width:100%;border-collapse:collapse;margin-top:16px;background:white;border-radius:8px">
#           {diag_row}{presc_row}
#         </table>
#         <p style="margin-top:20px">Open the SheydocApp to view your full record.</p>
#       </div></div></body></html>"""


# # ============================================================================
# # SLOT VALIDATION HELPER
# # ============================================================================

# def _slots_overlap(start_a: datetime, dur_a: int, start_b: datetime, dur_b: int) -> bool:
#     end_a = start_a + timedelta(minutes=dur_a)
#     end_b = start_b + timedelta(minutes=dur_b)
#     return start_a < end_b and start_b < end_a

# async def _validate_slot(
#     doctor_id: str,
#     apt_dt: datetime,
#     duration_minutes: int,
#     exclude_id: Optional[str] = None,   # ← ALWAYS pass appointment_id here
#     time_confirmed: bool = True,
# ) -> Dict[str, Any]:
#     """
#     FIX v6.5:
#     - When the Flutter app calls /booking-confirmed, it has ALREADY written the
#       appointment to Firestore. So the overlap query WILL find this very
#       appointment and return 409. The fix is to ALWAYS exclude exclude_id
#       (the current appointment_id) from the overlap check.
#     - For date-only bookings (time_confirmed=False) we skip overlap entirely.
#     """
#     now = datetime.now(timezone.utc)

#     if not time_confirmed:
#         doctor = await get_user_data(doctor_id)
#         if not doctor:
#             return {"valid": False, "reason": "Doctor not found."}
#         apt_date = apt_dt.date()
#         today    = now.date()
#         if apt_date < today:
#             return {"valid": False, "reason": "Appointment date must be today or in the future."}
#         return {"valid": True, "reason": None}

#     # ── Exact-time booking — full validation ─────────────────────────────────

#     # 1. Future check (allow up to 5 min in the past to handle network delays)
#     if apt_dt <= now - timedelta(minutes=5):
#         return {"valid": False, "reason": "Appointment must be in the future."}

#     # 2. Doctor exists check
#     doctor = await get_user_data(doctor_id)
#     if not doctor:
#         return {"valid": False, "reason": "Doctor not found."}

#     # 3. Availability window check
#     availability: List[Dict] = doctor.get("availability", [])
#     if availability:
#         # apt_dt weekday: Mon=0…Sun=6 (Python's weekday())
#         apt_local_dow = apt_dt.weekday()
#         apt_start_min = apt_dt.hour * 60 + apt_dt.minute
#         apt_end_min   = apt_start_min + duration_minutes

#         in_window = False
#         for window in availability:
#             if window.get("day") != apt_local_dow:
#                 continue
#             win_start = window.get("startHour", 0) * 60 + window.get("startMinute", 0)
#             win_end   = window.get("endHour", 23) * 60 + window.get("endMinute", 59)
#             if apt_start_min >= win_start and apt_end_min <= win_end:
#                 in_window = True
#                 break

#         if not in_window:
#             return {"valid": False, "reason": "Slot is outside the doctor's available hours."}

#     # 4. Double-booking check — query by doctorId + date range
#     #    We use appointmentDate (Timestamp) for the day boundary since
#     #    appointmentDateTime (ISO string) range queries can be unreliable.
#     day_start = apt_dt.replace(hour=0, minute=0, second=0, microsecond=0)
#     day_end   = day_start + timedelta(days=1)

#     try:
#         existing_stream = (
#             db.collection("appointments")
#               .where("doctorId", "==", doctor_id)
#               .where("status", "in", ["confirmed", "pending"])
#               .where("appointmentDate", ">=", day_start)
#               .where("appointmentDate", "<", day_end)
#               .stream()
#         )
#         for doc in existing_stream:
#             # ── FIX: always skip the appointment being confirmed ──────────
#             if exclude_id and doc.id == exclude_id:
#                 print(f"  ↳ Skipping self-conflict for appointment {doc.id}")
#                 continue

#             data = doc.to_dict()

#             # Skip other date-only bookings
#             if not data.get("timeConfirmed", True):
#                 continue

#             apt_dt_str = data.get("appointmentDateTime", "")
#             if not apt_dt_str:
#                 continue

#             try:
#                 existing_dt  = _parse_dt(apt_dt_str)
#                 existing_dur = int(data.get("durationMinutes", 30))
#             except Exception:
#                 continue

#             if _slots_overlap(apt_dt, duration_minutes, existing_dt, existing_dur):
#                 print(f"  ⚠️  Conflict: new slot {apt_dt} overlaps existing {existing_dt} (appt {doc.id})")
#                 return {
#                     "valid": False,
#                     "reason": f"Doctor already has an appointment at {existing_dt.strftime('%H:%M')}."
#                 }
#     except Exception as e:
#         print(f"⚠️  Slot check DB error: {e}")
#         # Don't block booking on DB query errors — log and continue
#         pass

#     return {"valid": True, "reason": None}


# # ============================================================================
# # REMINDERS — 24h / 1h / 5min windows
# # ============================================================================

# _REMINDER_WINDOWS = [
#     ("5m",  0,        7),
#     ("1h",  55,       70),
#     ("24h", 23 * 60,  25 * 60),
# ]

# async def _check_and_send_reminders(bg: BackgroundTasks) -> int:
#     now    = datetime.now(timezone.utc)
#     in_25h = now + timedelta(hours=25)

#     upcoming = (
#         db.collection("appointments")
#           .where("status", "==", "confirmed")
#           .where("appointmentDateTime", ">=", now.isoformat())
#           .where("appointmentDateTime", "<=", in_25h.isoformat())
#           .stream()
#     )

#     sent = 0
#     for doc in upcoming:
#         appt = doc.to_dict()

#         if not appt.get("timeConfirmed", True):
#             continue

#         try:
#             apt_dt = datetime.fromisoformat(
#                 appt.get("appointmentDateTime", "").replace("Z", "+00:00"))
#         except Exception:
#             continue

#         diff_min = (apt_dt - now).total_seconds() / 60
#         last_key = appt.get("lastReminderSent", "")

#         target_key = None
#         for key, lo, hi in _REMINDER_WINDOWS:
#             if lo <= diff_min <= hi:
#                 target_key = key
#                 break

#         if target_key is None:
#             continue
#         if last_key == target_key:
#             continue
#         if last_key == "5m":
#             continue

#         appt_ref = db.collection("appointments").document(doc.id)
#         try:
#             @firestore.transactional
#             def _claim_reminder(transaction, ref, key, current):
#                 snap = ref.get(transaction=transaction)
#                 if snap.get("lastReminderSent") != current:
#                     return False
#                 transaction.update(ref, {"lastReminderSent": key})
#                 return True

#             tx      = db.transaction()
#             claimed = _claim_reminder(tx, appt_ref, target_key, last_key)
#             if not claimed:
#                 continue
#         except Exception as e:
#             print(f"⚠️  Reminder claim error for {doc.id}: {e}")
#             continue

#         label = {"5m": "5 minutes", "1h": "1 hour", "24h": "24 hours"}.get(target_key, "soon")
#         await _send_reminder_notifications(appt, doc.id, label, bg)
#         sent += 1

#     print(f"✅ Reminders sent: {sent}")
#     return sent


# async def _send_reminder_notifications(appt, appt_id, label: str, bg: BackgroundTasks):
#     patient = await get_user_data(appt.get("patientId"))
#     doctor  = await get_user_data(appt.get("doctorId"))
#     if not patient or not doctor:
#         return
#     pname = patient.get("displayName") or patient.get("firstName", "Patient")
#     dname = doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
#     atime = fmt_dt(appt.get("appointmentDateTime", ""))
#     data  = {"type": "reminder", "appointment_id": appt_id}

#     for user, uid, other_name, role in [
#         (patient, appt.get("patientId"), f"Dr. {dname}", "patient"),
#         (doctor,  appt.get("doctorId"),  pname,          "doctor"),
#     ]:
#         if fcm := _get_fcm_token(user, uid or "", role):
#             bg.add_task(
#                 send_fcm, fcm,
#                 f"Appointment in {label}",
#                 f"Your appointment with {other_name} is at {atime}",
#                 data,
#             )


# # ============================================================================
# # HEARTBEAT
# # ============================================================================

# HEARTBEAT_DOC = "scheduler/heartbeat"

# async def _run_reminders_if_due(bg: BackgroundTasks):
#     ref = db.document(HEARTBEAT_DOC)

#     @firestore.transactional
#     def _claim(transaction, ref):
#         snap = ref.get(transaction=transaction)
#         now  = datetime.now(timezone.utc)
#         if snap.exists:
#             last = snap.get("lastRun")
#             if last and hasattr(last, "replace"):
#                 last_dt = last.replace(tzinfo=timezone.utc) if last.tzinfo is None else last
#                 if (now - last_dt).total_seconds() < 240:
#                     return False
#         transaction.set(ref, {"lastRun": now, "updatedAt": firestore.SERVER_TIMESTAMP})
#         return True

#     try:
#         transaction = db.transaction()
#         should_run  = _claim(transaction, ref)
#     except Exception as e:
#         print(f"⚠️  Heartbeat transaction error: {e}")
#         should_run = False

#     if should_run:
#         print("⏰ Heartbeat: running reminder check")
#         await _check_and_send_reminders(bg)
#     else:
#         print("⏭️  Heartbeat: debounced, skipping this run")


# # ============================================================================
# # ENDPOINTS
# # ============================================================================

# @app.api_route("/", methods=["GET", "HEAD"])
# async def root():
#     return {
#         "status":    "healthy",
#         "service":   "SheydocApp Backend",
#         "version":   "6.5.0",
#         "timestamp": datetime.now(timezone.utc).isoformat(),
#     }

# @app.get("/heartbeat")
# async def heartbeat(bg: BackgroundTasks):
#     await _run_reminders_if_due(bg)
#     return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}

# # ── Stream token ──────────────────────────────────────────────────────────────

# @app.post("/stream-token")
# async def get_stream_token(req: StreamTokenRequest):
#     if not STREAM_API_KEY or not STREAM_API_SECRET:
#         raise HTTPException(500, "Stream credentials not configured")
#     user_data = await get_user_data(req.user_id)
#     if not user_data:
#         raise HTTPException(404, "User not found")
#     token = _generate_stream_token(req.user_id)
#     return {"success": True, "token": token, "api_key": STREAM_API_KEY,
#             "call_id": req.appointment_id, "user_id": req.user_id}

# # ── Validate appointment slot ─────────────────────────────────────────────────

# @app.post("/validate-slot")
# async def validate_slot(req: ValidateSlotRequest):
#     try:
#         apt_dt = _parse_dt(req.appointment_datetime)
#     except Exception:
#         raise HTTPException(400, "Invalid appointment_datetime format. Use ISO-8601 UTC.")
#     result = await _validate_slot(
#         req.doctor_id, apt_dt, req.duration_minutes,
#         exclude_id=req.appointment_id,
#         time_confirmed=True,
#     )
#     return result

# # ── Get doctor available slots for a date ─────────────────────────────────────

# @app.get("/available-slots/{doctor_id}")
# async def get_available_slots(doctor_id: str, date: str, duration: int = 30):
#     try:
#         target_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
#     except ValueError:
#         raise HTTPException(400, "date must be YYYY-MM-DD")

#     doctor = await get_user_data(doctor_id)
#     if not doctor:
#         raise HTTPException(404, "Doctor not found")

#     availability: List[Dict] = doctor.get("availability", [])
#     dow = target_date.weekday()

#     windows = [w for w in availability if w.get("day") == dow]
#     if not windows:
#         return {"success": True, "slots": [], "reason": "Doctor not available on this day"}

#     day_end = target_date + timedelta(days=1)
#     try:
#         booked_stream = (
#             db.collection("appointments")
#               .where("doctorId", "==", doctor_id)
#               .where("status", "in", ["confirmed", "pending"])
#               .where("appointmentDate", ">=", target_date)
#               .where("appointmentDate", "<", day_end)
#               .stream()
#         )
#         booked = []
#         for doc in booked_stream:
#             d = doc.to_dict()
#             if not d.get("timeConfirmed", True):
#                 continue
#             try:
#                 booked.append((_parse_dt(d["appointmentDateTime"]), int(d.get("durationMinutes", 30))))
#             except Exception:
#                 pass
#     except Exception as e:
#         print(f"⚠️  Error fetching booked appointments: {e}")
#         booked = []

#     now = datetime.now(timezone.utc)
#     slots = []

#     for window in windows:
#         cursor_min = window.get("startHour", 8) * 60 + window.get("startMinute", 0)
#         end_min    = window.get("endHour", 17) * 60 + window.get("endMinute", 0)

#         while cursor_min + duration <= end_min:
#             slot_dt = target_date.replace(
#                 hour=cursor_min // 60, minute=cursor_min % 60,
#                 second=0, microsecond=0)
#             if slot_dt <= now:
#                 cursor_min += duration
#                 continue
#             overlaps = any(_slots_overlap(slot_dt, duration, b_dt, b_dur) for b_dt, b_dur in booked)
#             if not overlaps:
#                 slots.append(slot_dt.isoformat())
#             cursor_min += duration

#     return {"success": True, "slots": slots}

# # ── Booking confirmed ─────────────────────────────────────────────────────────

# @app.post("/booking-confirmed")
# async def booking_confirmed(req: BookingConfirmedRequest, bg: BackgroundTasks):
#     """
#     FIX v6.5:
#     - Always pass req.appointment_id as exclude_id to _validate_slot() so the
#       newly-written appointment doesn't trigger a self-conflict 409.
#     - Default time_confirmed=True (patients now always pick a slot).
#     """
#     try:
#         apt_dt = _parse_dt(req.appointment_datetime)
#     except Exception:
#         raise HTTPException(400, "Invalid appointment_datetime")

#     time_confirmed = req.time_confirmed if req.time_confirmed is not None else True

#     print(f"📅 booking-confirmed: appt={req.appointment_id} doctor={req.doctor_id} "
#           f"patient={req.patient_id} dt={req.appointment_datetime} "
#           f"time_confirmed={time_confirmed}")

#     slot_check = await _validate_slot(
#         req.doctor_id, apt_dt, req.duration_minutes,
#         exclude_id=req.appointment_id,   # ← KEY FIX: always exclude self
#         time_confirmed=time_confirmed,
#     )
#     if not slot_check["valid"]:
#         print(f"  ❌ Slot invalid: {slot_check['reason']}")
#         raise HTTPException(409, slot_check["reason"])

#     patient = await get_user_data(req.patient_id)
#     doctor  = await get_user_data(req.doctor_id)
#     if not patient or not doctor:
#         raise HTTPException(404, "User not found")

#     pname  = patient.get("name") or patient.get("displayName") or patient.get("firstName", "Patient")
#     dname  = doctor.get("name")  or doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
#     reason = req.reason_for_consultation or ""
#     data   = {"type": "booking_confirmed", "appointment_id": req.appointment_id}

#     if time_confirmed:
#         atime = fmt_dt(req.appointment_datetime)
#     else:
#         atime = fmt_date_only(req.appointment_datetime)

#     # ── Patient FCM ───────────────────────────────────────────────────────────
#     if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
#         patient_body = f"Your appointment with Dr. {dname} on {atime} is confirmed!"
#         bg.add_task(send_fcm, fcm, "Appointment Confirmed ✅", patient_body, data)

#     # ── Doctor FCM ────────────────────────────────────────────────────────────
#     if fcm := _get_fcm_token(doctor, req.doctor_id, "doctor"):
#         body = f"New appointment from {pname} for {atime}"
#         if reason:
#             body += f" — {reason[:60]}"
#         bg.add_task(send_fcm, fcm, "New Appointment Request 📅", body, data)

#     # ── Emails ────────────────────────────────────────────────────────────────
#     if email := patient.get("email"):
#         bg.add_task(send_email, email, pname, "Appointment Confirmed",
#                     _booking_email(pname, dname, atime, reason, date_only=not time_confirmed))
#     if email := doctor.get("email"):
#         bg.add_task(send_email, email, dname, "New Appointment Request",
#                     _booking_email(dname, pname, atime, reason, date_only=not time_confirmed))

#     print(f"✅ booking-confirmed success: patient={req.patient_id} doctor={req.doctor_id}")
#     return {"success": True}

# # ── Appointment cancelled ─────────────────────────────────────────────────────

# @app.post("/appointment-canceled")
# async def appointment_canceled(req: AppointmentCanceledRequest, bg: BackgroundTasks):
#     patient = await get_user_data(req.patient_id)
#     doctor  = await get_user_data(req.doctor_id)
#     if not patient or not doctor:
#         raise HTTPException(404, "User not found")
#     pname = patient.get("name") or patient.get("displayName") or patient.get("firstName", "Patient")
#     dname = doctor.get("name")  or doctor.get("displayName")  or doctor.get("firstName",  "Doctor")
#     atime = fmt_dt(req.appointment_datetime)
#     data  = {"type": "appointment_canceled", "appointment_id": req.appointment_id}

#     if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
#         bg.add_task(send_fcm, fcm, "Appointment Cancelled",
#                     f"Your appointment with Dr. {dname} was cancelled", data)
#     if fcm := _get_fcm_token(doctor, req.doctor_id, "doctor"):
#         bg.add_task(send_fcm, fcm, "Appointment Cancelled",
#                     f"Appointment with {pname} was cancelled", data)
#     if email := patient.get("email"):
#         bg.add_task(send_email, email, pname, "Appointment Cancelled",
#                     _cancel_email(pname, dname, atime, req.canceled_by))
#     if email := doctor.get("email"):
#         bg.add_task(send_email, email, dname, "Appointment Cancelled",
#                     _cancel_email(dname, pname, atime, req.canceled_by))
#     return {"success": True}

# # ── Notify message ────────────────────────────────────────────────────────────

# @app.post("/notify-message")
# async def notify_message(req: NotifyMessageRequest, bg: BackgroundTasks):
#     print(f"📨 notify-message: sender={req.sender_id} -> recipient={req.recipient_id} "
#           f"preview='{req.message_preview[:40]}'")

#     sender    = await get_user_data(req.sender_id)
#     recipient = await get_user_data(req.recipient_id)

#     if not sender:
#         return {"success": True, "note": "sender data missing, skipped"}
#     if not recipient:
#         return {"success": True, "note": "recipient data missing, skipped"}

#     sender_name = (
#         sender.get("name") or sender.get("displayName") or
#         sender.get("firstName") or "Someone"
#     )
#     fcm_data = {
#         "type":         "new_message",
#         "chat_id":      req.chat_id,
#         "sender_id":    req.sender_id,
#         "sender_name":  sender_name,
#         "click_action": "FLUTTER_NOTIFICATION_CLICK",
#     }
#     fcm_token = _get_fcm_token(recipient, req.recipient_id, "recipient")
#     if fcm_token:
#         bg.add_task(send_fcm, fcm_token, sender_name, req.message_preview, fcm_data)
#     else:
#         print(f"⚠️  Push skipped for recipient {req.recipient_id}.")
#     return {"success": True}

# # ── Notify call started ───────────────────────────────────────────────────────

# @app.post("/notify-call-started")
# async def notify_call_started(req: NotifyCallStartedRequest, bg: BackgroundTasks):
#     caller = await get_user_data(req.caller_id)
#     callee = await get_user_data(req.callee_id)
#     if not caller or not callee:
#         return {"success": True, "note": "user data missing, skipped"}
#     caller_name = (
#         caller.get("name") or caller.get("displayName") or
#         caller.get("firstName", "Someone")
#     )
#     prefix     = "Dr. " if req.caller_is_doctor else ""
#     call_label = "Video" if req.call_type == "video" else "Audio"
#     fcm_data = {
#         "type":           "incoming_call",
#         "appointment_id": req.appointment_id,
#         "caller_id":      req.caller_id,
#         "caller_name":    f"{prefix}{caller_name}",
#         "call_type":      req.call_type,
#         "click_action":   "FLUTTER_NOTIFICATION_CLICK",
#     }
#     if fcm := _get_fcm_token(callee, req.callee_id, "callee"):
#         bg.add_task(send_fcm, fcm, f"Incoming {call_label} Call",
#                     f"{prefix}{caller_name} is calling you", fcm_data)
#     return {"success": True}

# # ── Notify call joined ────────────────────────────────────────────────────────

# @app.post("/notify-call-joined")
# async def notify_call_joined(req: NotifyCallJoinedRequest, bg: BackgroundTasks):
#     joiner     = await get_user_data(req.joiner_id)
#     other_user = await get_user_data(req.other_user_id)
#     if not joiner or not other_user:
#         return {"success": True, "note": "user data missing, skipped"}
#     joiner_name = (
#         joiner.get("name") or joiner.get("displayName") or
#         joiner.get("firstName", "Someone")
#     )
#     fcm_data = {
#         "type": "call_joined",
#         "appointment_id": req.appointment_id,
#         "joiner_id": req.joiner_id,
#         "click_action": "FLUTTER_NOTIFICATION_CLICK",
#     }
#     if fcm := _get_fcm_token(other_user, req.other_user_id, "other_user"):
#         bg.add_task(send_fcm, fcm, "Patient Joined",
#                     f"{joiner_name} has joined the call", fcm_data)
#     return {"success": True}

# # ── Online presence ───────────────────────────────────────────────────────────

# @app.post("/presence")
# async def update_presence(req: PresenceRequest):
#     try:
#         db.collection("users").document(req.user_id).update({
#             "isOnline": req.is_online,
#             "lastSeen": firestore.SERVER_TIMESTAMP,
#         })
#         return {"success": True}
#     except Exception as e:
#         print(f"❌ Presence update error: {e}")
#         raise HTTPException(500, f"Presence update failed: {e}")

# @app.get("/presence/{user_id}")
# async def get_presence(user_id: str):
#     user = await get_user_data(user_id)
#     if not user:
#         raise HTTPException(404, "User not found")
#     return {
#         "success":  True,
#         "isOnline": user.get("isOnline", False),
#         "lastSeen": _ts_to_iso(user.get("lastSeen")),
#     }

# # ── Upload document ───────────────────────────────────────────────────────────

# @app.post("/upload-document", response_model=FileUploadResponse)
# async def upload_document(
#     file: UploadFile = File(...),
#     user_id: str = Form(...),
#     file_type: str = Form(...),
# ):
#     content_type = _resolve_mime(file)
#     if content_type not in ALLOWED_DOC_TYPES:
#         raise HTTPException(400, f"File type '{content_type}' not allowed.")
#     file.file.seek(0, 2); size = file.file.tell(); file.file.seek(0)
#     if size > 10 * 1024 * 1024:
#         raise HTTPException(400, "File too large. Max 10MB.")
#     result = await _upload_to_appwrite(file, APPWRITE_BUCKET_ID, content_type, "doc")
#     camel = file_type.replace("_", " ").title().replace(" ", "")
#     key = f"{camel[0].lower()}{camel[1:]}Url"
#     db.collection("users").document(user_id).set({key: result["url"]}, merge=True)
#     return FileUploadResponse(success=True, url=result["url"],
#                               file_id=result["file_id"], message="Uploaded successfully")

# # ── Upload chat media ─────────────────────────────────────────────────────────

# @app.post("/upload-chat-media", response_model=FileUploadResponse)
# async def upload_chat_media(
#     file: UploadFile = File(...),
#     user_id: str = Form(...),
#     media_type: str = Form(...),
# ):
#     content_type = _resolve_mime(file, fallback="video/mp4" if media_type == "video" else "image/jpeg")
#     if content_type not in ALLOWED_CHAT_TYPES:
#         raise HTTPException(400, f"Unsupported type: {content_type}")
#     file.file.seek(0, 2); size = file.file.tell(); file.file.seek(0)
#     max_mb = 50 if media_type == "video" else 10
#     if size > max_mb * 1024 * 1024:
#         raise HTTPException(400, f"File too large. Max {max_mb}MB.")
#     result = await _upload_to_appwrite(file, APPWRITE_CHAT_BUCKET_ID, content_type, f"chat_{media_type}")
#     return FileUploadResponse(success=True, url=result["url"],
#                               file_id=result["file_id"], message=f"Chat {media_type} uploaded")

# # ── Delete doctor files ───────────────────────────────────────────────────────

# @app.delete("/delete-doctor-files/{doctor_id}")
# async def delete_doctor_files(doctor_id: str):
#     FILE_ID_FIELDS = ["educationCertificateFileId","authorizationFileFileId",
#                       "affiliateHospitalFileFileId","idCardFileFileId"]
#     URL_FIELDS     = ["educationCertificateUrl","authorizationFileUrl",
#                       "affiliateHospitalFileUrl","idCardFileUrl"]
#     doc_ref  = db.collection("users").document(doctor_id)
#     doc_snap = doc_ref.get()
#     if not doc_snap.exists:
#         raise HTTPException(404, "Doctor not found")
#     data = doc_snap.to_dict()
#     deleted, failed = [], []
#     for field in FILE_ID_FIELDS:
#         fid = data.get(field)
#         if not fid: continue
#         try:
#             appwrite_storage.delete_file(APPWRITE_BUCKET_ID, fid)
#             deleted.append(fid)
#         except Exception:
#             failed.append(fid)
#     clear = {f: firestore.DELETE_FIELD for f in FILE_ID_FIELDS + URL_FIELDS}
#     doc_ref.update(clear)
#     return {"success": True, "deleted": deleted, "failed": failed}

# # ============================================================================
# # MEDICAL RECORDS
# # ============================================================================

# @app.post("/save-medical-record")
# async def save_medical_record(req: SaveMedicalRecordRequest, bg: BackgroundTasks):
#     record_data = {
#         "appointmentId":  req.appointment_id,
#         "patientId":      req.patient_id,
#         "doctorId":       req.doctor_id,
#         "patientName":    req.patient_name,
#         "complaints":     req.complaints or "",
#         "diagnosis":      req.diagnosis or "",
#         "prescription":   req.prescription or "",
#         "notes":          req.notes or "",
#         "followUp":       req.follow_up or "",
#         "status":         req.status,
#         "updatedAt":      firestore.SERVER_TIMESTAMP,
#     }

#     existing_stream = (
#         db.collection("medical_records")
#           .where("appointmentId", "==", req.appointment_id)
#           .limit(1).stream()
#     )
#     existing_docs = list(existing_stream)

#     if existing_docs:
#         existing_docs[0].reference.update(record_data)
#         record_id = existing_docs[0].id
#     else:
#         record_data["createdAt"] = firestore.SERVER_TIMESTAMP
#         record_ref = db.collection("medical_records").document()
#         record_ref.set(record_data)
#         record_id = record_ref.id

#     db.collection("appointments").document(req.appointment_id).update({
#         "hasRecord": True, "recordId": record_id,
#     })

#     if req.status == "finalized":
#         patient = await get_user_data(req.patient_id)
#         doctor  = await get_user_data(req.doctor_id)
#         if patient and doctor:
#             dname    = doctor.get("name") or doctor.get("displayName") or doctor.get("firstName") or "Your doctor"
#             pname    = patient.get("name") or patient.get("displayName") or patient.get("firstName") or "Patient"
#             date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
#             if fcm := _get_fcm_token(patient, req.patient_id, "patient"):
#                 bg.add_task(send_fcm, fcm, "Medical Record Available",
#                             f"Dr. {dname} has added notes from your consultation.",
#                             {"type": "medical_record", "record_id": record_id,
#                              "appointment_id": req.appointment_id,
#                              "click_action": "FLUTTER_NOTIFICATION_CLICK"})
#             if email := patient.get("email"):
#                 bg.add_task(send_email, email, pname,
#                             f"Medical Record from Dr. {dname}",
#                             _medical_record_email(pname, dname, date_str,
#                                                   req.diagnosis or "", req.prescription or ""))
#             db.collection("notifications").add({
#                 "userId":        req.patient_id,
#                 "title":         "Medical Record Available",
#                 "body":          f"Dr. {dname} added notes from your consultation.",
#                 "type":          "medical_record",
#                 "recordId":      record_id,
#                 "appointmentId": req.appointment_id,
#                 "createdAt":     firestore.SERVER_TIMESTAMP,
#                 "read":          False,
#             })

#     return {"success": True, "record_id": record_id}


# @app.get("/medical-records/{patient_id}")
# async def get_patient_records(patient_id: str):
#     records_stream = (
#         db.collection("medical_records")
#           .where("patientId", "==", patient_id)
#           .where("status", "==", "finalized")
#           .order_by("createdAt", direction=firestore.Query.DESCENDING)
#           .stream()
#     )
#     result = []
#     for doc in records_stream:
#         d = doc.to_dict()
#         d["id"] = doc.id
#         d["createdAt"] = _ts_to_iso(d.get("createdAt"))
#         d["updatedAt"] = _ts_to_iso(d.get("updatedAt"))
#         result.append(d)
#     return {"success": True, "records": result, "count": len(result)}


# @app.get("/medical-records/appointment/{appointment_id}")
# async def get_appointment_record(appointment_id: str):
#     records_stream = (
#         db.collection("medical_records")
#           .where("appointmentId", "==", appointment_id)
#           .limit(1).stream()
#     )
#     docs = list(records_stream)
#     if not docs:
#         return {"success": True, "record": None}
#     d = docs[0].to_dict()
#     d["id"] = docs[0].id
#     d["createdAt"] = _ts_to_iso(d.get("createdAt"))
#     d["updatedAt"] = _ts_to_iso(d.get("updatedAt"))
#     return {"success": True, "record": d}

# # ── Reminders ─────────────────────────────────────────────────────────────────

# @app.get("/check-reminders")
# async def check_reminders(bg: BackgroundTasks):
#     sent = await _check_and_send_reminders(bg)
#     return {"success": True, "reminders_sent": sent}


# # ============================================================================

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)



