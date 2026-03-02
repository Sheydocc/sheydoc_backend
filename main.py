"""
TeleMed FastAPI Backend
Handles notifications, emails, scheduled reminders, file uploads via Appwrite,
and Stream Video token generation.
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
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import firebase_admin
from firebase_admin import credentials, firestore, messaging

# Appwrite SDK
from appwrite.client import Client
from appwrite.services.storage import Storage
from appwrite.input_file import InputFile
from appwrite.id import ID

load_dotenv()

# ============================================================================
# CONFIG
# ============================================================================

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_NAME = os.getenv("FROM_NAME", "SheydocApp")

# Appwrite config
APPWRITE_ENDPOINT   = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY    = os.getenv("APPWRITE_API_KEY")
APPWRITE_BUCKET_ID  = os.getenv("APPWRITE_BUCKET_ID")

# Stream Video config — secret NEVER leaves this server
STREAM_API_KEY    = os.getenv("STREAM_API_KEY")     # nvmcympwmahx
STREAM_API_SECRET = os.getenv("STREAM_API_SECRET")  # your secret

# Build Appwrite client
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
    description="Notification, email, file upload, and Stream Video token service",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Firebase
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


class AppointmentCanceledRequest(BaseModel):
    appointment_id: str
    patient_id: str
    doctor_id: str
    canceled_by: str
    appointment_datetime: str


class StreamTokenRequest(BaseModel):
    user_id: str
    appointment_id: str


class FileUploadResponse(BaseModel):
    success: bool
    url: str
    file_id: str
    message: str


# ============================================================================
# STREAM VIDEO TOKEN GENERATION
# Pure Python JWT — no extra pip dependency needed.
# Stream tokens are standard HS256 JWTs signed with your API secret.
# ============================================================================

def _generate_stream_token(user_id: str) -> str:
    """
    Generate a Stream Video user token (JWT HS256).
    Manually constructed to avoid adding a dependency.
    """
    header = {"alg": "HS256", "typ": "JWT"}

    now = int(time_module.time())
    payload = {
        "user_id": user_id,
        "iat": now,
        "exp": now + (7 * 24 * 60 * 60),  # 7 days validity
    }

    def b64url_encode(data: dict) -> str:
        json_str = json.dumps(data, separators=(",", ":"))
        return base64.urlsafe_b64encode(json_str.encode()).rstrip(b"=").decode()

    header_enc = b64url_encode(header)
    payload_enc = b64url_encode(payload)
    signing_input = f"{header_enc}.{payload_enc}"

    signature = hmac.new(
        STREAM_API_SECRET.encode(),
        signing_input.encode(),
        hashlib.sha256,
    ).digest()

    sig_enc = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    return f"{signing_input}.{sig_enc}"


# ============================================================================
# FILE UPLOAD — APPWRITE
# ============================================================================

ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}


def resolve_content_type(file: UploadFile) -> str:
    if file.content_type and file.content_type != "application/octet-stream":
        return file.content_type
    if file.filename:
        guessed, _ = mimetypes.guess_type(file.filename)
        if guessed:
            print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
            return guessed
    print("⚠️ Defaulting MIME to image/jpeg")
    return "image/jpeg"


def build_appwrite_view_url(file_id: str) -> str:
    return (
        f"{APPWRITE_ENDPOINT}/storage/buckets/{APPWRITE_BUCKET_ID}"
        f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}"
    )


async def upload_to_appwrite(
    file: UploadFile,
    user_id: str,
    file_type: str,
    content_type: str,
) -> Dict[str, Any]:
    contents = await file.read()

    ext_map = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "application/pdf": ".pdf",
    }
    ext = ext_map.get(content_type, ".jpg")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{user_id}_{file_type}_{timestamp}{ext}"

        result = appwrite_storage.create_file(
            bucket_id=APPWRITE_BUCKET_ID,
            file_id=ID.unique(),
            file=InputFile.from_path(tmp_path),
        )

        file_id = result['$id']
        url = build_appwrite_view_url(file_id)

        print(f"✅ Appwrite upload OK — file_id: {file_id}")
        return {"success": True, "file_id": file_id, "url": url}

    except Exception as e:
        print(f"❌ Appwrite upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ============================================================================
# FIREBASE / EMAIL HELPERS
# ============================================================================

async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
    try:
        doc = db.collection("users").document(uid).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        print(f"❌ Error fetching user {uid}: {e}")
        return None


async def send_fcm_notification(
    fcm_token: str,
    title: str,
    body: str,
    data: Optional[Dict[str, str]] = None
):
    if not fcm_token:
        return
    try:
        msg = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data or {},
            token=fcm_token,
        )
        messaging.send(msg)
        print(f"✅ FCM sent")
    except Exception as e:
        print(f"❌ FCM failed: {e}")


async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
        msg['To'] = to_email
        msg.attach(MIMEText(html_content, 'html'))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"✅ Email sent to {to_email}")
    except Exception as e:
        print(f"❌ Email failed: {e}")


def format_datetime(iso_string: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.strftime("%B %d, %Y at %I:%M %p")
    except:
        return iso_string


# ============================================================================
# EMAIL TEMPLATES
# ============================================================================

def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
    return f"""<!DOCTYPE html><html><head><style>
        body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
        .container{{max-width:600px;margin:0 auto;padding:20px}}
        .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
        .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
        .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
        .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
    </style></head><body><div class="container">
        <div class="header"><h1>✅ Appointment Confirmed</h1></div>
        <div class="content">
            <p>Hi {patient_name},</p>
            <p>Your telemedicine appointment has been confirmed.</p>
            <div class="info-box">
                <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
                <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
            </div>
            <p>Please be ready a few minutes before the scheduled time.</p>
        </div>
        <div class="footer"><p>Sheydoc - Your Health, Our Priority</p></div>
    </div></body></html>"""


def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
    return f"""<!DOCTYPE html><html><head><style>
        body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
        .container{{max-width:600px;margin:0 auto;padding:20px}}
        .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
        .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
        .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
        .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
    </style></head><body><div class="container">
        <div class="header"><h1>❌ Appointment Canceled</h1></div>
        <div class="content">
            <p>Hi {name},</p>
            <p>Your appointment was canceled by the {canceled_by}.</p>
            <div class="info-box">
                <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
                <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
            </div>
            <p>You can rebook anytime through the app.</p>
        </div>
        <div class="footer"><p>Sheydoc - Your Health, Our Priority</p></div>
    </div></body></html>"""


def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
    return f"""<!DOCTYPE html><html><head><style>
        body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
        .container{{max-width:600px;margin:0 auto;padding:20px}}
        .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
        .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
        .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
        .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
        .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
    </style></head><body><div class="container">
        <div class="header"><h1>⏰ Appointment Reminder</h1></div>
        <div class="content">
            <p>Hi {name},</p>
            <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
            <div class="info-box">
                <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
                <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
            </div>
        </div>
        <div class="footer"><p>Sheydoc - Your Health, Our Priority</p></div>
    </div></body></html>"""


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "status": "healthy",
        "service": "SheydocApp Backend",
        "version": "4.0.0",
        "file_storage": "appwrite",
        "video": "stream",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ============================================================================
# STREAM VIDEO TOKEN ENDPOINT
# ============================================================================

@app.post("/stream-token")
async def get_stream_token(request: StreamTokenRequest):
    """
    Generate a Stream Video token for a user joining a call.
    Flutter calls this before joining — the call_id is always the appointment_id.
    Both doctor and patient use the same appointment_id to join the same call.
    """
    try:
        if not STREAM_API_KEY or not STREAM_API_SECRET:
            raise HTTPException(
                status_code=500,
                detail="Stream credentials not configured on server"
            )

        # Validate the user exists in Firestore
        user_data = await get_user_data(request.user_id)
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")

        token = _generate_stream_token(request.user_id)

        print(f"✅ Stream token generated for user: {request.user_id}")
        print(f"   Call ID (appointment): {request.appointment_id}")

        return {
            "success": True,
            "token": token,
            "api_key": STREAM_API_KEY,
            "call_id": request.appointment_id,
            "user_id": request.user_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Stream token error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FILE UPLOAD ENDPOINT
# ============================================================================

@app.post("/upload-document", response_model=FileUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form(...),
):
    try:
        content_type = resolve_content_type(file)
        print(f"📎 Content type: {content_type} (raw: {file.content_type})")

        if content_type not in ALLOWED_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
            )

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        if file_size > 10 * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
            )

        print(f"📤 Uploading {file_type} for {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

        result = await upload_to_appwrite(file, user_id, file_type, content_type)

        camel_type = (
            file_type.replace("_", " ").title().replace(" ", "")
        )
        firestore_update = {
            f"{camel_type[0].lower()}{camel_type[1:]}Url": result["url"],
            f"{camel_type[0].lower()}{camel_type[1:]}FileId": result["file_id"],
        }
        db.collection("users").document(user_id).set(
            firestore_update, merge=True
        )
        print(f"✅ Saved to Firestore: {firestore_update}")

        return FileUploadResponse(
            success=True,
            url=result['url'],
            file_id=result['file_id'],
            message="File uploaded successfully"
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete-doctor-files/{doctor_id}")
async def delete_doctor_files(doctor_id: str):
    file_id_fields = [
        "educationCertificateFileId",
        "authorizationFileFileId",
        "affiliateHospitalFileFileId",
        "idCardFileFileId",
    ]

    url_fields = [
        "educationCertificateUrl",
        "authorizationFileUrl",
        "affiliateHospitalFileUrl",
        "idCardFileUrl",
    ]

    try:
        doctor_ref = db.collection("users").document(doctor_id)
        doctor_doc = doctor_ref.get()

        if not doctor_doc.exists:
            raise HTTPException(status_code=404, detail="Doctor not found")

        doctor_data = doctor_doc.to_dict()
        deleted_files = []
        failed_files = []

        for field in file_id_fields:
            file_id = doctor_data.get(field)
            if not file_id:
                continue
            try:
                appwrite_storage.delete_file(
                    bucket_id=APPWRITE_BUCKET_ID,
                    file_id=file_id,
                )
                deleted_files.append(file_id)
                print(f"✅ Deleted Appwrite file: {file_id} ({field})")
            except Exception as e:
                failed_files.append(file_id)
                print(f"⚠️ Could not delete {file_id}: {e}")

        fields_to_clear = {field: firestore.DELETE_FIELD for field in file_id_fields + url_fields}
        doctor_ref.update(fields_to_clear)

        return {
            "success": True,
            "doctor_id": doctor_id,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
            "message": f"Deleted {len(deleted_files)} verification file(s). Profile photo preserved.",
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ delete_doctor_files error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/booking-confirmed")
async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
    try:
        patient_data = await get_user_data(request.patient_id)
        doctor_data = await get_user_data(request.doctor_id)
        if not patient_data or not doctor_data:
            raise HTTPException(status_code=404, detail="User not found")

        patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
        doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
        apt_time = format_datetime(request.appointment_datetime)

        if fcm := patient_data.get("fcmToken"):
            background_tasks.add_task(send_fcm_notification, fcm,
                "Appointment Confirmed ✅",
                f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
                {"type": "booking_confirmed", "appointment_id": request.appointment_id})

        if fcm := doctor_data.get("fcmToken"):
            background_tasks.add_task(send_fcm_notification, fcm,
                "New Appointment 📅",
                f"New appointment with {patient_name} for {apt_time}",
                {"type": "booking_confirmed", "appointment_id": request.appointment_id})

        if email := patient_data.get("email"):
            background_tasks.add_task(send_email, email, patient_name,
                "Appointment Confirmed",
                booking_confirmed_email(patient_name, doctor_name, apt_time))

        if email := doctor_data.get("email"):
            background_tasks.add_task(send_email, email, doctor_name,
                "New Appointment Scheduled",
                booking_confirmed_email(doctor_name, patient_name, apt_time))

        return {"success": True, "message": "Notifications sent"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/appointment-canceled")
async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
    try:
        patient_data = await get_user_data(request.patient_id)
        doctor_data = await get_user_data(request.doctor_id)
        if not patient_data or not doctor_data:
            raise HTTPException(status_code=404, detail="User not found")

        patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
        doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
        apt_time = format_datetime(request.appointment_datetime)

        if fcm := patient_data.get("fcmToken"):
            background_tasks.add_task(send_fcm_notification, fcm,
                "Appointment Canceled ❌",
                f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
                {"type": "appointment_canceled", "appointment_id": request.appointment_id})

        if fcm := doctor_data.get("fcmToken"):
            background_tasks.add_task(send_fcm_notification, fcm,
                "Appointment Canceled ❌",
                f"Appointment with {patient_name} on {apt_time} was canceled",
                {"type": "appointment_canceled", "appointment_id": request.appointment_id})

        if email := patient_data.get("email"):
            background_tasks.add_task(send_email, email, patient_name,
                "Appointment Canceled",
                appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

        if email := doctor_data.get("email"):
            background_tasks.add_task(send_email, email, doctor_name,
                "Appointment Canceled",
                appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

        return {"success": True, "message": "Cancellation notifications sent"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/check-reminders")
async def check_reminders(background_tasks: BackgroundTasks):
    try:
        now = datetime.now(timezone.utc)
        in_24h = now + timedelta(hours=24)
        in_1h = now + timedelta(hours=1)

        upcoming = (
            db.collection("appointments")
            .where("status", "==", "confirmed")
            .where("appointmentDateTime", ">=", now.isoformat())
            .where("appointmentDateTime", "<=", in_24h.isoformat())
            .stream()
        )

        reminders_sent = 0
        for doc in upcoming:
            appointment = doc.to_dict()
            try:
                apt_time = datetime.fromisoformat(
                    appointment.get("appointmentDateTime").replace('Z', '+00:00'))
            except Exception:
                continue

            last = appointment.get("lastReminderSent")
            if now <= apt_time <= in_24h and apt_time > in_1h and not last:
                await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
                reminders_sent += 1
            if now <= apt_time <= in_1h and last != "1h":
                await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
                reminders_sent += 1

        return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
    patient_data = await get_user_data(appointment.get("patientId"))
    doctor_data = await get_user_data(appointment.get("doctorId"))
    if not patient_data or not doctor_data:
        return

    patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
    doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
    apt_time = format_datetime(appointment.get("appointmentDateTime"))
    title = f"⏰ Appointment in {hours_until}h"

    if fcm := patient_data.get("fcmToken"):
        background_tasks.add_task(send_fcm_notification, fcm, title,
            f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
            {"type": "reminder", "appointment_id": appointment_id})
    if email := patient_data.get("email"):
        background_tasks.add_task(send_email, email, patient_name,
            f"Appointment Reminder - {hours_until}h",
            reminder_email(patient_name, doctor_name, apt_time, hours_until))
    if fcm := doctor_data.get("fcmToken"):
        background_tasks.add_task(send_fcm_notification, fcm, title,
            f"Reminder: Appointment with {patient_name} at {apt_time}",
            {"type": "reminder", "appointment_id": appointment_id})
    if email := doctor_data.get("email"):
        background_tasks.add_task(send_email, email, doctor_name,
            f"Appointment Reminder - {hours_until}h",
            reminder_email(doctor_name, patient_name, apt_time, hours_until))

    reminder_key = "1h" if hours_until == 1 else "24h"
    db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
    print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)













# """
# TeleMed FastAPI Backend
# Handles notifications, emails, scheduled reminders, AND file uploads via Appwrite
# """

# import os
# import mimetypes
# import smtplib
# import tempfile
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from datetime import datetime, timedelta, timezone
# from typing import Optional, Dict, Any
# from dotenv import load_dotenv

# from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel

# import firebase_admin
# from firebase_admin import credentials, firestore, messaging

# # Appwrite SDK
# from appwrite.client import Client
# from appwrite.services.storage import Storage
# from appwrite.input_file import InputFile
# from appwrite.id import ID

# load_dotenv()

# # ============================================================================
# # CONFIG
# # ============================================================================

# SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER = os.getenv("SMTP_USER")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # Appwrite config — set these in Render environment variables
# APPWRITE_ENDPOINT   = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
# APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")   # from Appwrite console
# APPWRITE_API_KEY    = os.getenv("APPWRITE_API_KEY")       # Server API key
# APPWRITE_BUCKET_ID  = os.getenv("APPWRITE_BUCKET_ID")     # Storage bucket ID

# # Build Appwrite client
# appwrite_client = Client()
# appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
# appwrite_client.set_project(APPWRITE_PROJECT_ID)
# appwrite_client.set_key(APPWRITE_API_KEY)

# appwrite_storage = Storage(appwrite_client)

# # ============================================================================
# # FASTAPI APP
# # ============================================================================

# app = FastAPI(
#     title="TeleMed Backend",
#     description="Notification, email, and file upload service for TeleMed app",
#     version="3.0.0"
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Firebase
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


# class AppointmentCanceledRequest(BaseModel):
#     appointment_id: str
#     patient_id: str
#     doctor_id: str
#     canceled_by: str
#     appointment_datetime: str


# class FileUploadResponse(BaseModel):
#     success: bool
#     url: str
#     file_id: str
#     message: str


# # ============================================================================
# # FILE UPLOAD — APPWRITE
# # No signatures, no credentials in requests, just works.
# # ============================================================================

# ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}


# def resolve_content_type(file: UploadFile) -> str:
#     """Detect real MIME type — Flutter sends octet-stream by default."""
#     if file.content_type and file.content_type != "application/octet-stream":
#         return file.content_type
#     if file.filename:
#         guessed, _ = mimetypes.guess_type(file.filename)
#         if guessed:
#             print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
#             return guessed
#     print("⚠️ Defaulting MIME to image/jpeg")
#     return "image/jpeg"


# def build_appwrite_view_url(file_id: str) -> str:
#     """
#     Build a direct public view URL for the uploaded file.
#     Requires the bucket to have 'File Security' disabled OR the file to be public.
#     """
#     return (
#         f"{APPWRITE_ENDPOINT}/storage/buckets/{APPWRITE_BUCKET_ID}"
#         f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}"
#     )


# async def upload_to_appwrite(
#     file: UploadFile,
#     user_id: str,
#     file_type: str,
#     content_type: str,
# ) -> Dict[str, Any]:
#     """
#     Upload file to Appwrite Storage.
#     Appwrite requires a real file path (not a stream) so we write to a temp file first.
#     """
#     contents = await file.read()

#     # Determine extension from content type
#     ext_map = {
#         "image/jpeg": ".jpg",
#         "image/jpg": ".jpg",
#         "image/png": ".png",
#         "image/webp": ".webp",
#         "application/pdf": ".pdf",
#     }
#     ext = ext_map.get(content_type, ".jpg")

#     # Write to temp file — Appwrite SDK needs a file path
#     with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
#         tmp.write(contents)
#         tmp_path = tmp.name

#     try:
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         # Use a meaningful filename so it's identifiable in Appwrite console
#         filename = f"{user_id}_{file_type}_{timestamp}{ext}"

#         # Upload — ID.unique() generates a unique Appwrite file ID automatically
#         result = appwrite_storage.create_file(
#             bucket_id=APPWRITE_BUCKET_ID,
#             file_id=ID.unique(),
#             file=InputFile.from_path(tmp_path),
#         )

#         file_id = result['$id']
#         url = build_appwrite_view_url(file_id)

#         print(f"✅ Appwrite upload OK — file_id: {file_id}")
#         print(f"   URL: {url}")

#         return {
#             "success": True,
#             "file_id": file_id,
#             "url": url,
#         }

#     except Exception as e:
#         print(f"❌ Appwrite upload failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

#     finally:
#         # Always clean up temp file
#         if os.path.exists(tmp_path):
#             os.remove(tmp_path)


# # ============================================================================
# # FIREBASE / EMAIL HELPERS
# # ============================================================================

# async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
#     try:
#         doc = db.collection("users").document(uid).get()
#         return doc.to_dict() if doc.exists else None
#     except Exception as e:
#         print(f"❌ Error fetching user {uid}: {e}")
#         return None


# async def send_fcm_notification(
#     fcm_token: str,
#     title: str,
#     body: str,
#     data: Optional[Dict[str, str]] = None
# ):
#     if not fcm_token:
#         return
#     try:
#         msg = messaging.Message(
#             notification=messaging.Notification(title=title, body=body),
#             data=data or {},
#             token=fcm_token,
#         )
#         messaging.send(msg)
#         print(f"✅ FCM sent")
#     except Exception as e:
#         print(f"❌ FCM failed: {e}")


# async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
#     try:
#         msg = MIMEMultipart('alternative')
#         msg['Subject'] = subject
#         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
#         msg['To'] = to_email
#         msg.attach(MIMEText(html_content, 'html'))
#         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
#             server.starttls()
#             server.login(SMTP_USER, SMTP_PASSWORD)
#             server.send_message(msg)
#         print(f"✅ Email sent to {to_email}")
#     except Exception as e:
#         print(f"❌ Email failed: {e}")


# def format_datetime(iso_string: str) -> str:
#     try:
#         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
#         return dt.strftime("%B %d, %Y at %I:%M %p")
#     except:
#         return iso_string


# # ============================================================================
# # EMAIL TEMPLATES
# # ============================================================================

# def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
#         <div class="content">
#             <p>Hi {patient_name},</p>
#             <p>Your telemedicine appointment has been confirmed.</p>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#             <p>Please be ready a few minutes before the scheduled time.</p>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>❌ Appointment Canceled</h1></div>
#         <div class="content">
#             <p>Hi {name},</p>
#             <p>Your appointment was canceled by the {canceled_by}.</p>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#             <p>You can rebook anytime through the app.</p>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
#         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
#         <div class="content">
#             <p>Hi {name},</p>
#             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# # ============================================================================
# # ENDPOINTS
# # ============================================================================

# @app.api_route("/", methods=["GET", "HEAD"])
# async def root():
#     return {
#         "status": "healthy",
#         "service": "TeleMed Backend",
#         "version": "3.0.0",
#         "file_storage": "appwrite",
#         "timestamp": datetime.now(timezone.utc).isoformat()
#     }


# @app.post("/upload-document", response_model=FileUploadResponse)
# async def upload_document(
#     file: UploadFile = File(...),
#     user_id: str = Form(...),
#     file_type: str = Form(...),
# ):
#     """Upload a document or image to Appwrite Storage."""
#     try:
#         content_type = resolve_content_type(file)
#         print(f"📎 Content type: {content_type} (raw: {file.content_type})")

#         if content_type not in ALLOWED_TYPES:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
#             )

#         # Check file size (max 10MB)
#         file.file.seek(0, 2)
#         file_size = file.file.tell()
#         file.file.seek(0)

#         if file_size > 10 * 1024 * 1024:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
#             )

#         print(f"📤 Uploading {file_type} for {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

#         result = await upload_to_appwrite(file, user_id, file_type, content_type)

#         # ✅ Persist both the view URL and the Appwrite file_id to Firestore
#         # so the delete endpoint can find and remove files after approval.
#         # Field naming convention:  <file_type>Url  and  <file_type>FileId
#         # e.g. educationCertificateUrl / educationCertificateFileId
#         camel_type = (
#             file_type.replace("_", " ").title().replace(" ", "")
#         )  # "education_certificate" → "EducationCertificate"
#         firestore_update = {
#             f"{camel_type[0].lower()}{camel_type[1:]}Url": result["url"],        # educationCertificateUrl
#             f"{camel_type[0].lower()}{camel_type[1:]}FileId": result["file_id"], # educationCertificateFileId
#         }
#         db.collection("users").document(user_id).set(
#             firestore_update, merge=True
#         )
#         print(f"✅ Saved to Firestore: {firestore_update}")

#         return FileUploadResponse(
#             success=True,
#             url=result['url'],
#             file_id=result['file_id'],
#             message="File uploaded successfully"
#         )

#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"❌ Upload error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# @app.delete("/delete-doctor-files/{doctor_id}")
# async def delete_doctor_files(doctor_id: str):
#     """
#     Called after admin approves a doctor.
#     Deletes VERIFICATION files from Appwrite and clears their Firestore fields.
    
#     ✅ KEEPS: Profile photo (still needed for app display)
#     ❌ DELETES: Education cert, authorization, hospital docs, ID card
    
#     The doctor's approval status is handled on the Flutter side before calling this.
#     """
#     # ✅ Only verification document file IDs (NOT profile photo)
#     file_id_fields = [
#         "educationCertificateFileId",
#         "authorizationFileFileId",
#         "affiliateHospitalFileFileId",
#         "idCardFileFileId",
#         # ❌ photoFileId is NOT included - profile photo stays!
#     ]

#     # ✅ Only verification document URLs (NOT profile photo)
#     url_fields = [
#         "educationCertificateUrl",
#         "authorizationFileUrl",
#         "affiliateHospitalFileUrl",
#         "idCardFileUrl",
#         # ❌ photoUrl is NOT included - profile photo stays!
#     ]

#     try:
#         # Fetch the doctor's Firestore doc to get file IDs
#         doctor_ref = db.collection("users").document(doctor_id)
#         doctor_doc = doctor_ref.get()

#         if not doctor_doc.exists:
#             raise HTTPException(status_code=404, detail="Doctor not found")

#         doctor_data = doctor_doc.to_dict()
#         deleted_files = []
#         failed_files = []

#         # Delete each verification file from Appwrite
#         for field in file_id_fields:
#             file_id = doctor_data.get(field)
#             if not file_id:
#                 continue  # File wasn't uploaded (optional docs)
#             try:
#                 appwrite_storage.delete_file(
#                     bucket_id=APPWRITE_BUCKET_ID,
#                     file_id=file_id,
#                 )
#                 deleted_files.append(file_id)
#                 print(f"✅ Deleted Appwrite file: {file_id} ({field})")
#             except Exception as e:
#                 # Don't abort — try to delete the rest
#                 failed_files.append(file_id)
#                 print(f"⚠️ Could not delete {file_id}: {e}")

#         # Clear all verification URL and file ID fields from Firestore
#         # Use DELETE_FIELD sentinel so the keys are removed entirely
#         fields_to_clear = {field: firestore.DELETE_FIELD for field in file_id_fields + url_fields}
#         doctor_ref.update(fields_to_clear)

#         print(f"✅ Cleared verification document fields for doctor {doctor_id}")
#         print(f"✅ Profile photo PRESERVED (photoUrl field not touched)")

#         return {
#             "success": True,
#             "doctor_id": doctor_id,
#             "deleted_files": deleted_files,
#             "failed_files": failed_files,
#             "message": f"Deleted {len(deleted_files)} verification file(s). Profile photo preserved.",
#         }

#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"❌ delete_doctor_files error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# @app.post("/booking-confirmed")
# async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
#     try:
#         patient_data = await get_user_data(request.patient_id)
#         doctor_data = await get_user_data(request.doctor_id)
#         if not patient_data or not doctor_data:
#             raise HTTPException(status_code=404, detail="User not found")

#         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#         apt_time = format_datetime(request.appointment_datetime)

#         if fcm := patient_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Confirmed ✅",
#                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
#                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

#         if fcm := doctor_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "New Appointment 📅",
#                 f"New appointment with {patient_name} for {apt_time}",
#                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

#         if email := patient_data.get("email"):
#             background_tasks.add_task(send_email, email, patient_name,
#                 "Appointment Confirmed",
#                 booking_confirmed_email(patient_name, doctor_name, apt_time))

#         if email := doctor_data.get("email"):
#             background_tasks.add_task(send_email, email, doctor_name,
#                 "New Appointment Scheduled",
#                 booking_confirmed_email(doctor_name, patient_name, apt_time))

#         return {"success": True, "message": "Notifications sent"}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.post("/appointment-canceled")#sw1
# async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
#     try:
#         patient_data = await get_user_data(request.patient_id)
#         doctor_data = await get_user_data(request.doctor_id)
#         if not patient_data or not doctor_data:
#             raise HTTPException(status_code=404, detail="User not found")

#         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#         apt_time = format_datetime(request.appointment_datetime)

#         if fcm := patient_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Canceled ❌",
#                 f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
#                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

#         if fcm := doctor_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Canceled ❌",
#                 f"Appointment with {patient_name} on {apt_time} was canceled",
#                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

#         if email := patient_data.get("email"):
#             background_tasks.add_task(send_email, email, patient_name,
#                 "Appointment Canceled",
#                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

#         if email := doctor_data.get("email"):
#             background_tasks.add_task(send_email, email, doctor_name,
#                 "Appointment Canceled",
#                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

#         return {"success": True, "message": "Cancellation notifications sent"}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get("/check-reminders")
# async def check_reminders(background_tasks: BackgroundTasks):
#     try:
#         now = datetime.now(timezone.utc)
#         in_24h = now + timedelta(hours=24)
#         in_1h = now + timedelta(hours=1)

#         upcoming = (
#             db.collection("appointments")
#             .where("status", "==", "confirmed")
#             .where("appointmentDateTime", ">=", now.isoformat())
#             .where("appointmentDateTime", "<=", in_24h.isoformat())
#             .stream()
#         )

#         reminders_sent = 0
#         for doc in upcoming:
#             appointment = doc.to_dict()
#             try:
#                 apt_time = datetime.fromisoformat(
#                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
#             except Exception:
#                 continue

#             last = appointment.get("lastReminderSent")
#             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
#                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
#                 reminders_sent += 1
#             if now <= apt_time <= in_1h and last != "1h":
#                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
#                 reminders_sent += 1

#         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
#     patient_data = await get_user_data(appointment.get("patientId"))
#     doctor_data = await get_user_data(appointment.get("doctorId"))
#     if not patient_data or not doctor_data:
#         return

#     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#     apt_time = format_datetime(appointment.get("appointmentDateTime"))
#     title = f"⏰ Appointment in {hours_until}h"

#     if fcm := patient_data.get("fcmToken"):
#         background_tasks.add_task(send_fcm_notification, fcm, title,
#             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
#             {"type": "reminder", "appointment_id": appointment_id})
#     if email := patient_data.get("email"):
#         background_tasks.add_task(send_email, email, patient_name,
#             f"Appointment Reminder - {hours_until}h",
#             reminder_email(patient_name, doctor_name, apt_time, hours_until))
#     if fcm := doctor_data.get("fcmToken"):
#         background_tasks.add_task(send_fcm_notification, fcm, title,
#             f"Reminder: Appointment with {patient_name} at {apt_time}",
#             {"type": "reminder", "appointment_id": appointment_id})
#     if email := doctor_data.get("email"):
#         background_tasks.add_task(send_email, email, doctor_name,
#             f"Appointment Reminder - {hours_until}h",
#             reminder_email(doctor_name, patient_name, apt_time, hours_until))

#     reminder_key = "1h" if hours_until == 1 else "24h"
#     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
#     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)





# """
# TeleMed FastAPI Backend
# Handles notifications, emails, scheduled reminders, AND file uploads via Appwrite
# """

# import os
# import mimetypes
# import smtplib
# import tempfile
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from datetime import datetime, timedelta, timezone
# from typing import Optional, Dict, Any
# from dotenv import load_dotenv

# from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel

# import firebase_admin
# from firebase_admin import credentials, firestore, messaging

# # Appwrite SDK
# from appwrite.client import Client
# from appwrite.services.storage import Storage
# from appwrite.input_file import InputFile
# from appwrite.id import ID

# load_dotenv()

# # ============================================================================
# # CONFIG
# # ============================================================================

# SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER = os.getenv("SMTP_USER")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # Appwrite config — set these in Render environment variables
# APPWRITE_ENDPOINT   = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
# APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")   # from Appwrite console
# APPWRITE_API_KEY    = os.getenv("APPWRITE_API_KEY")       # Server API key
# APPWRITE_BUCKET_ID  = os.getenv("APPWRITE_BUCKET_ID")     # Storage bucket ID

# # Build Appwrite client
# appwrite_client = Client()
# appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
# appwrite_client.set_project(APPWRITE_PROJECT_ID)
# appwrite_client.set_key(APPWRITE_API_KEY)

# appwrite_storage = Storage(appwrite_client)

# # ============================================================================
# # FASTAPI APP
# # ============================================================================

# app = FastAPI(
#     title="TeleMed Backend",
#     description="Notification, email, and file upload service for TeleMed app",
#     version="3.0.0"
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Firebase
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


# class AppointmentCanceledRequest(BaseModel):
#     appointment_id: str
#     patient_id: str
#     doctor_id: str
#     canceled_by: str
#     appointment_datetime: str


# class FileUploadResponse(BaseModel):
#     success: bool
#     url: str
#     file_id: str
#     message: str


# # ============================================================================
# # FILE UPLOAD — APPWRITE
# # No signatures, no credentials in requests, just works.
# # ============================================================================

# ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}


# def resolve_content_type(file: UploadFile) -> str:
#     """Detect real MIME type — Flutter sends octet-stream by default."""
#     if file.content_type and file.content_type != "application/octet-stream":
#         return file.content_type
#     if file.filename:
#         guessed, _ = mimetypes.guess_type(file.filename)
#         if guessed:
#             print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
#             return guessed
#     print("⚠️ Defaulting MIME to image/jpeg")
#     return "image/jpeg"


# def build_appwrite_view_url(file_id: str) -> str:
#     """
#     Build a direct public view URL for the uploaded file.
#     Requires the bucket to have 'File Security' disabled OR the file to be public.
#     """
#     return (
#         f"{APPWRITE_ENDPOINT}/storage/buckets/{APPWRITE_BUCKET_ID}"
#         f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}"
#     )


# async def upload_to_appwrite(
#     file: UploadFile,
#     user_id: str,
#     file_type: str,
#     content_type: str,
# ) -> Dict[str, Any]:
#     """
#     Upload file to Appwrite Storage.
#     Appwrite requires a real file path (not a stream) so we write to a temp file first.
#     """
#     contents = await file.read()

#     # Determine extension from content type
#     ext_map = {
#         "image/jpeg": ".jpg",
#         "image/jpg": ".jpg",
#         "image/png": ".png",
#         "image/webp": ".webp",
#         "application/pdf": ".pdf",
#     }
#     ext = ext_map.get(content_type, ".jpg")

#     # Write to temp file — Appwrite SDK needs a file path
#     with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
#         tmp.write(contents)
#         tmp_path = tmp.name

#     try:
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         # Use a meaningful filename so it's identifiable in Appwrite console
#         filename = f"{user_id}_{file_type}_{timestamp}{ext}"

#         # Upload — ID.unique() generates a unique Appwrite file ID automatically
#         result = appwrite_storage.create_file(
#             bucket_id=APPWRITE_BUCKET_ID,
#             file_id=ID.unique(),
#             file=InputFile.from_path(tmp_path),
#         )

#         file_id = result['$id']
#         url = build_appwrite_view_url(file_id)

#         print(f"✅ Appwrite upload OK — file_id: {file_id}")
#         print(f"   URL: {url}")

#         return {
#             "success": True,
#             "file_id": file_id,
#             "url": url,
#         }

#     except Exception as e:
#         print(f"❌ Appwrite upload failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

#     finally:
#         # Always clean up temp file
#         if os.path.exists(tmp_path):
#             os.remove(tmp_path)


# # ============================================================================
# # FIREBASE / EMAIL HELPERS
# # ============================================================================

# async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
#     try:
#         doc = db.collection("users").document(uid).get()
#         return doc.to_dict() if doc.exists else None
#     except Exception as e:
#         print(f"❌ Error fetching user {uid}: {e}")
#         return None


# async def send_fcm_notification(
#     fcm_token: str,
#     title: str,
#     body: str,
#     data: Optional[Dict[str, str]] = None
# ):
#     if not fcm_token:
#         return
#     try:
#         msg = messaging.Message(
#             notification=messaging.Notification(title=title, body=body),
#             data=data or {},
#             token=fcm_token,
#         )
#         messaging.send(msg)
#         print(f"✅ FCM sent")
#     except Exception as e:
#         print(f"❌ FCM failed: {e}")


# async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
#     try:
#         msg = MIMEMultipart('alternative')
#         msg['Subject'] = subject
#         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
#         msg['To'] = to_email
#         msg.attach(MIMEText(html_content, 'html'))
#         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
#             server.starttls()
#             server.login(SMTP_USER, SMTP_PASSWORD)
#             server.send_message(msg)
#         print(f"✅ Email sent to {to_email}")
#     except Exception as e:
#         print(f"❌ Email failed: {e}")


# def format_datetime(iso_string: str) -> str:
#     try:
#         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
#         return dt.strftime("%B %d, %Y at %I:%M %p")
#     except:
#         return iso_string


# # ============================================================================
# # EMAIL TEMPLATES
# # ============================================================================

# def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
#         <div class="content">
#             <p>Hi {patient_name},</p>
#             <p>Your telemedicine appointment has been confirmed.</p>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#             <p>Please be ready a few minutes before the scheduled time.</p>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>❌ Appointment Canceled</h1></div>
#         <div class="content">
#             <p>Hi {name},</p>
#             <p>Your appointment was canceled by the {canceled_by}.</p>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#             <p>You can rebook anytime through the app.</p>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
#     return f"""<!DOCTYPE html><html><head><style>
#         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
#         .container{{max-width:600px;margin:0 auto;padding:20px}}
#         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
#         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
#         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
#         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
#         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
#     </style></head><body><div class="container">
#         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
#         <div class="content">
#             <p>Hi {name},</p>
#             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
#             <div class="info-box">
#                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
#                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
#             </div>
#         </div>
#         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
#     </div></body></html>"""


# # ============================================================================
# # ENDPOINTS
# # ============================================================================

# @app.api_route("/", methods=["GET", "HEAD"])
# async def root():
#     return {
#         "status": "healthy",
#         "service": "TeleMed Backend",
#         "version": "3.0.0",
#         "file_storage": "appwrite",
#         "timestamp": datetime.now(timezone.utc).isoformat()
#     }


# @app.post("/upload-document", response_model=FileUploadResponse)
# async def upload_document(
#     file: UploadFile = File(...),
#     user_id: str = Form(...),
#     file_type: str = Form(...),
# ):
#     """Upload a document or image to Appwrite Storage."""
#     try:
#         content_type = resolve_content_type(file)
#         print(f"📎 Content type: {content_type} (raw: {file.content_type})")

#         if content_type not in ALLOWED_TYPES:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
#             )

#         # Check file size (max 10MB)
#         file.file.seek(0, 2)
#         file_size = file.file.tell()
#         file.file.seek(0)

#         if file_size > 10 * 1024 * 1024:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
#             )

#         print(f"📤 Uploading {file_type} for {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

#         result = await upload_to_appwrite(file, user_id, file_type, content_type)

#         # ✅ Persist both the view URL and the Appwrite file_id to Firestore
#         # so the delete endpoint can find and remove files after approval.
#         # Field naming convention:  <file_type>Url  and  <file_type>FileId
#         # e.g. educationCertificateUrl / educationCertificateFileId
#         camel_type = (
#             file_type.replace("_", " ").title().replace(" ", "")
#         )  # "education_certificate" → "EducationCertificate"
#         firestore_update = {
#             f"{camel_type[0].lower()}{camel_type[1:]}Url": result["url"],        # educationCertificateUrl
#             f"{camel_type[0].lower()}{camel_type[1:]}FileId": result["file_id"], # educationCertificateFileId
#         }
#         db.collection("users").document(user_id).set(
#             firestore_update, merge=True
#         )
#         print(f"✅ Saved to Firestore: {firestore_update}")

#         return FileUploadResponse(
#             success=True,
#             url=result['url'],
#             file_id=result['file_id'],
#             message="File uploaded successfully"
#         )

#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"❌ Upload error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# class DeleteDocumentsRequest(BaseModel):
#     file_ids: list[str]  # list of Appwrite file IDs to delete


# @app.post("/delete-doctor-documents")
# async def delete_doctor_documents(request: DeleteDocumentsRequest):
#     """
#     Called by admin panel after approving a doctor.
#     Deletes verification documents from Appwrite — they're no longer needed
#     once the doctor is approved. URLs in Firestore are cleared by the Flutter
#     admin client directly after calling this endpoint.
#     """
#     deleted = []
#     failed = []

#     for file_id in request.file_ids:
#         if not file_id or file_id.strip() == "":
#             continue
#         try:
#             appwrite_storage.delete_file(
#                 bucket_id=APPWRITE_BUCKET_ID,
#                 file_id=file_id,
#             )
#             deleted.append(file_id)
#             print(f"🗑️ Deleted Appwrite file: {file_id}")
#         except Exception as e:
#             print(f"⚠️ Could not delete file {file_id}: {e}")
#             failed.append(file_id)

#     return {
#         "success": True,
#         "deleted": deleted,
#         "failed": failed,
#         "message": f"Deleted {len(deleted)} file(s), {len(failed)} failed.",
#     }


# @app.delete("/delete-doctor-files/{doctor_id}")
# async def delete_doctor_files(doctor_id: str):
#     """
#     Called after admin approves a doctor.
#     Deletes all uploaded verification files from Appwrite and
#     clears the URL + file ID fields from Firestore.
#     The doctor's approval status and profile data are NOT touched here —
#     that's handled on the Flutter side before calling this endpoint.
#     """
#     # The Firestore fields that hold file IDs for each document type
#     file_id_fields = [
#         "educationCertificateFileId",
#         "authorizationFileFileId",
#         "affiliateHospitalFileFileId",
#         "idCardFileFileId",
#     ]

#     # The Firestore fields that hold the Appwrite view URLs
#     url_fields = [
#         "educationCertificateUrl",
#         "authorizationFileUrl",
#         "affiliateHospitalFileUrl",
#         "idCardFileUrl",
#     ]

#     try:
#         # Fetch the doctor's Firestore doc to get file IDs
#         doctor_ref = db.collection("users").document(doctor_id)
#         doctor_doc = doctor_ref.get()

#         if not doctor_doc.exists:
#             raise HTTPException(status_code=404, detail="Doctor not found")

#         doctor_data = doctor_doc.to_dict()
#         deleted_files = []
#         failed_files = []

#         # Delete each file from Appwrite
#         for field in file_id_fields:
#             file_id = doctor_data.get(field)
#             if not file_id:
#                 continue  # File wasn't uploaded (optional docs)
#             try:
#                 appwrite_storage.delete_file(
#                     bucket_id=APPWRITE_BUCKET_ID,
#                     file_id=file_id,
#                 )
#                 deleted_files.append(file_id)
#                 print(f"✅ Deleted Appwrite file: {file_id} ({field})")
#             except Exception as e:
#                 # Don't abort — try to delete the rest
#                 failed_files.append(file_id)
#                 print(f"⚠️ Could not delete {file_id}: {e}")

#         # Clear all URL and file ID fields from Firestore
#         # Use DELETE_FIELD sentinel so the keys are removed entirely
#         fields_to_clear = {field: firestore.DELETE_FIELD for field in file_id_fields + url_fields}
#         doctor_ref.update(fields_to_clear)

#         print(f"✅ Cleared document fields for doctor {doctor_id}")

#         return {
#             "success": True,
#             "doctor_id": doctor_id,
#             "deleted_files": deleted_files,
#             "failed_files": failed_files,
#             "message": f"Deleted {len(deleted_files)} file(s) from Appwrite and cleared Firestore fields.",
#         }

#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"❌ delete_doctor_files error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# @app.post("/booking-confirmed")
# async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
#     try:
#         patient_data = await get_user_data(request.patient_id)
#         doctor_data = await get_user_data(request.doctor_id)
#         if not patient_data or not doctor_data:
#             raise HTTPException(status_code=404, detail="User not found")

#         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#         apt_time = format_datetime(request.appointment_datetime)

#         if fcm := patient_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Confirmed ✅",
#                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
#                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

#         if fcm := doctor_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "New Appointment 📅",
#                 f"New appointment with {patient_name} for {apt_time}",
#                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

#         if email := patient_data.get("email"):
#             background_tasks.add_task(send_email, email, patient_name,
#                 "Appointment Confirmed",
#                 booking_confirmed_email(patient_name, doctor_name, apt_time))

#         if email := doctor_data.get("email"):
#             background_tasks.add_task(send_email, email, doctor_name,
#                 "New Appointment Scheduled",
#                 booking_confirmed_email(doctor_name, patient_name, apt_time))

#         return {"success": True, "message": "Notifications sent"}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.post("/appointment-canceled")
# async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
#     try:
#         patient_data = await get_user_data(request.patient_id)
#         doctor_data = await get_user_data(request.doctor_id)
#         if not patient_data or not doctor_data:
#             raise HTTPException(status_code=404, detail="User not found")

#         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#         apt_time = format_datetime(request.appointment_datetime)

#         if fcm := patient_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Canceled ❌",
#                 f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
#                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

#         if fcm := doctor_data.get("fcmToken"):
#             background_tasks.add_task(send_fcm_notification, fcm,
#                 "Appointment Canceled ❌",
#                 f"Appointment with {patient_name} on {apt_time} was canceled",
#                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

#         if email := patient_data.get("email"):
#             background_tasks.add_task(send_email, email, patient_name,
#                 "Appointment Canceled",
#                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

#         if email := doctor_data.get("email"):
#             background_tasks.add_task(send_email, email, doctor_name,
#                 "Appointment Canceled",
#                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

#         return {"success": True, "message": "Cancellation notifications sent"}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get("/check-reminders")
# async def check_reminders(background_tasks: BackgroundTasks):
#     try:
#         now = datetime.now(timezone.utc)
#         in_24h = now + timedelta(hours=24)
#         in_1h = now + timedelta(hours=1)

#         upcoming = (
#             db.collection("appointments")
#             .where("status", "==", "confirmed")
#             .where("appointmentDateTime", ">=", now.isoformat())
#             .where("appointmentDateTime", "<=", in_24h.isoformat())
#             .stream()
#         )

#         reminders_sent = 0
#         for doc in upcoming:
#             appointment = doc.to_dict()
#             try:
#                 apt_time = datetime.fromisoformat(
#                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
#             except Exception:
#                 continue

#             last = appointment.get("lastReminderSent")
#             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
#                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
#                 reminders_sent += 1
#             if now <= apt_time <= in_1h and last != "1h":
#                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
#                 reminders_sent += 1

#         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
#     patient_data = await get_user_data(appointment.get("patientId"))
#     doctor_data = await get_user_data(appointment.get("doctorId"))
#     if not patient_data or not doctor_data:
#         return

#     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
#     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
#     apt_time = format_datetime(appointment.get("appointmentDateTime"))
#     title = f"⏰ Appointment in {hours_until}h"

#     if fcm := patient_data.get("fcmToken"):
#         background_tasks.add_task(send_fcm_notification, fcm, title,
#             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
#             {"type": "reminder", "appointment_id": appointment_id})
#     if email := patient_data.get("email"):
#         background_tasks.add_task(send_email, email, patient_name,
#             f"Appointment Reminder - {hours_until}h",
#             reminder_email(patient_name, doctor_name, apt_time, hours_until))
#     if fcm := doctor_data.get("fcmToken"):
#         background_tasks.add_task(send_fcm_notification, fcm, title,
#             f"Reminder: Appointment with {patient_name} at {apt_time}",
#             {"type": "reminder", "appointment_id": appointment_id})
#     if email := doctor_data.get("email"):
#         background_tasks.add_task(send_email, email, doctor_name,
#             f"Appointment Reminder - {hours_until}h",
#             reminder_email(doctor_name, patient_name, apt_time, hours_until))

#     reminder_key = "1h" if hours_until == 1 else "24h"
#     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
#     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)





# # """
# # TeleMed FastAPI Backend
# # Handles notifications, emails, scheduled reminders, AND file uploads via Appwrite
# # """

# # import os
# # import mimetypes
# # import smtplib
# # import tempfile
# # from email.mime.text import MIMEText
# # from email.mime.multipart import MIMEMultipart
# # from datetime import datetime, timedelta, timezone
# # from typing import Optional, Dict, Any
# # from dotenv import load_dotenv

# # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # from fastapi.middleware.cors import CORSMiddleware
# # from pydantic import BaseModel

# # import firebase_admin
# # from firebase_admin import credentials, firestore, messaging

# # # Appwrite SDK
# # from appwrite.client import Client
# # from appwrite.services.storage import Storage
# # from appwrite.input_file import InputFile
# # from appwrite.id import ID

# # load_dotenv()

# # # ============================================================================
# # # CONFIG
# # # ============================================================================

# # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # SMTP_USER = os.getenv("SMTP_USER")
# # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # Appwrite config — set these in Render environment variables
# # APPWRITE_ENDPOINT   = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
# # APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")   # from Appwrite console
# # APPWRITE_API_KEY    = os.getenv("APPWRITE_API_KEY")       # Server API key
# # APPWRITE_BUCKET_ID  = os.getenv("APPWRITE_BUCKET_ID")     # Storage bucket ID

# # # Build Appwrite client
# # appwrite_client = Client()
# # appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
# # appwrite_client.set_project(APPWRITE_PROJECT_ID)
# # appwrite_client.set_key(APPWRITE_API_KEY)

# # appwrite_storage = Storage(appwrite_client)

# # # ============================================================================
# # # FASTAPI APP
# # # ============================================================================

# # app = FastAPI(
# #     title="TeleMed Backend",
# #     description="Notification, email, and file upload service for TeleMed app",
# #     version="3.0.0"
# # )

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # # Firebase
# # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # firebase_admin.initialize_app(cred)
# # db = firestore.client()


# # # ============================================================================
# # # PYDANTIC MODELS
# # # ============================================================================

# # class BookingConfirmedRequest(BaseModel):
# #     appointment_id: str
# #     patient_id: str
# #     doctor_id: str
# #     appointment_datetime: str
# #     duration_minutes: int


# # class AppointmentCanceledRequest(BaseModel):
# #     appointment_id: str
# #     patient_id: str
# #     doctor_id: str
# #     canceled_by: str
# #     appointment_datetime: str


# # class FileUploadResponse(BaseModel):
# #     success: bool
# #     url: str
# #     file_id: str
# #     message: str


# # # ============================================================================
# # # FILE UPLOAD — APPWRITE
# # # No signatures, no credentials in requests, just works.
# # # ============================================================================

# # ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}


# # def resolve_content_type(file: UploadFile) -> str:
# #     """Detect real MIME type — Flutter sends octet-stream by default."""
# #     if file.content_type and file.content_type != "application/octet-stream":
# #         return file.content_type
# #     if file.filename:
# #         guessed, _ = mimetypes.guess_type(file.filename)
# #         if guessed:
# #             print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
# #             return guessed
# #     print("⚠️ Defaulting MIME to image/jpeg")
# #     return "image/jpeg"


# # def build_appwrite_view_url(file_id: str) -> str:
# #     """
# #     Build a direct public view URL for the uploaded file.
# #     Requires the bucket to have 'File Security' disabled OR the file to be public.
# #     """
# #     return (
# #         f"{APPWRITE_ENDPOINT}/storage/buckets/{APPWRITE_BUCKET_ID}"
# #         f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}"
# #     )


# # async def upload_to_appwrite(
# #     file: UploadFile,
# #     user_id: str,
# #     file_type: str,
# #     content_type: str,
# # ) -> Dict[str, Any]:
# #     """
# #     Upload file to Appwrite Storage.
# #     Appwrite requires a real file path (not a stream) so we write to a temp file first.
# #     """
# #     contents = await file.read()

# #     # Determine extension from content type
# #     ext_map = {
# #         "image/jpeg": ".jpg",
# #         "image/jpg": ".jpg",
# #         "image/png": ".png",
# #         "image/webp": ".webp",
# #         "application/pdf": ".pdf",
# #     }
# #     ext = ext_map.get(content_type, ".jpg")

# #     # Write to temp file — Appwrite SDK needs a file path
# #     with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
# #         tmp.write(contents)
# #         tmp_path = tmp.name

# #     try:
# #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# #         # Use a meaningful filename so it's identifiable in Appwrite console
# #         filename = f"{user_id}_{file_type}_{timestamp}{ext}"

# #         # Upload — ID.unique() generates a unique Appwrite file ID automatically
# #         result = appwrite_storage.create_file(
# #             bucket_id=APPWRITE_BUCKET_ID,
# #             file_id=ID.unique(),
# #             file=InputFile.from_path(tmp_path),
# #         )

# #         file_id = result['$id']
# #         url = build_appwrite_view_url(file_id)

# #         print(f"✅ Appwrite upload OK — file_id: {file_id}")
# #         print(f"   URL: {url}")

# #         return {
# #             "success": True,
# #             "file_id": file_id,
# #             "url": url,
# #         }

# #     except Exception as e:
# #         print(f"❌ Appwrite upload failed: {e}")
# #         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

# #     finally:
# #         # Always clean up temp file
# #         if os.path.exists(tmp_path):
# #             os.remove(tmp_path)


# # # ============================================================================
# # # FIREBASE / EMAIL HELPERS
# # # ============================================================================

# # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# #     try:
# #         doc = db.collection("users").document(uid).get()
# #         return doc.to_dict() if doc.exists else None
# #     except Exception as e:
# #         print(f"❌ Error fetching user {uid}: {e}")
# #         return None


# # async def send_fcm_notification(
# #     fcm_token: str,
# #     title: str,
# #     body: str,
# #     data: Optional[Dict[str, str]] = None
# # ):
# #     if not fcm_token:
# #         return
# #     try:
# #         msg = messaging.Message(
# #             notification=messaging.Notification(title=title, body=body),
# #             data=data or {},
# #             token=fcm_token,
# #         )
# #         messaging.send(msg)
# #         print(f"✅ FCM sent")
# #     except Exception as e:
# #         print(f"❌ FCM failed: {e}")


# # async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
# #     try:
# #         msg = MIMEMultipart('alternative')
# #         msg['Subject'] = subject
# #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# #         msg['To'] = to_email
# #         msg.attach(MIMEText(html_content, 'html'))
# #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# #             server.starttls()
# #             server.login(SMTP_USER, SMTP_PASSWORD)
# #             server.send_message(msg)
# #         print(f"✅ Email sent to {to_email}")
# #     except Exception as e:
# #         print(f"❌ Email failed: {e}")


# # def format_datetime(iso_string: str) -> str:
# #     try:
# #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# #         return dt.strftime("%B %d, %Y at %I:%M %p")
# #     except:
# #         return iso_string


# # # ============================================================================
# # # EMAIL TEMPLATES
# # # ============================================================================

# # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# #     return f"""<!DOCTYPE html><html><head><style>
# #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# #         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
# #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# #     </style></head><body><div class="container">
# #         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
# #         <div class="content">
# #             <p>Hi {patient_name},</p>
# #             <p>Your telemedicine appointment has been confirmed.</p>
# #             <div class="info-box">
# #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# #             </div>
# #             <p>Please be ready a few minutes before the scheduled time.</p>
# #         </div>
# #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# #     </div></body></html>"""


# # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# #     return f"""<!DOCTYPE html><html><head><style>
# #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# #         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
# #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# #     </style></head><body><div class="container">
# #         <div class="header"><h1>❌ Appointment Canceled</h1></div>
# #         <div class="content">
# #             <p>Hi {name},</p>
# #             <p>Your appointment was canceled by the {canceled_by}.</p>
# #             <div class="info-box">
# #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# #                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
# #             </div>
# #             <p>You can rebook anytime through the app.</p>
# #         </div>
# #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# #     </div></body></html>"""


# # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# #     return f"""<!DOCTYPE html><html><head><style>
# #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# #         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
# #         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
# #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# #     </style></head><body><div class="container">
# #         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
# #         <div class="content">
# #             <p>Hi {name},</p>
# #             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
# #             <div class="info-box">
# #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# #             </div>
# #         </div>
# #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# #     </div></body></html>"""


# # # ============================================================================
# # # ENDPOINTS
# # # ============================================================================

# # @app.api_route("/", methods=["GET", "HEAD"])
# # async def root():
# #     return {
# #         "status": "healthy",
# #         "service": "TeleMed Backend",
# #         "version": "3.0.0",
# #         "file_storage": "appwrite",
# #         "timestamp": datetime.now(timezone.utc).isoformat()
# #     }


# # @app.post("/upload-document", response_model=FileUploadResponse)
# # async def upload_document(
# #     file: UploadFile = File(...),
# #     user_id: str = Form(...),
# #     file_type: str = Form(...),
# # ):
# #     """Upload a document or image to Appwrite Storage."""
# #     try:
# #         content_type = resolve_content_type(file)
# #         print(f"📎 Content type: {content_type} (raw: {file.content_type})")

# #         if content_type not in ALLOWED_TYPES:
# #             raise HTTPException(
# #                 status_code=400,
# #                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
# #             )

# #         # Check file size to be (max 10MB)
# #         file.file.seek(0, 2)
# #         file_size = file.file.tell()
# #         file.file.seek(0)

# #         if file_size > 10 * 1024 * 1024:
# #             raise HTTPException(
# #                 status_code=400,
# #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
# #             )

# #         print(f"📤 Uploading {file_type} for {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

# #         result = await upload_to_appwrite(file, user_id, file_type, content_type)

# #         return FileUploadResponse(
# #             success=True,
# #             url=result['url'],
# #             file_id=result['file_id'],
# #             message="File uploaded successfully"
# #         )

# #     except HTTPException:
# #         raise
# #     except Exception as e:
# #         print(f"❌ Upload error: {e}")
# #         raise HTTPException(status_code=500, detail=str(e))


# # @app.post("/booking-confirmed")
# # async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
# #     try:
# #         patient_data = await get_user_data(request.patient_id)
# #         doctor_data = await get_user_data(request.doctor_id)
# #         if not patient_data or not doctor_data:
# #             raise HTTPException(status_code=404, detail="User not found")

# #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# #         apt_time = format_datetime(request.appointment_datetime)

# #         if fcm := patient_data.get("fcmToken"):
# #             background_tasks.add_task(send_fcm_notification, fcm,
# #                 "Appointment Confirmed ✅",
# #                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
# #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# #         if fcm := doctor_data.get("fcmToken"):
# #             background_tasks.add_task(send_fcm_notification, fcm,
# #                 "New Appointment 📅",
# #                 f"New appointment with {patient_name} for {apt_time}",
# #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# #         if email := patient_data.get("email"):
# #             background_tasks.add_task(send_email, email, patient_name,
# #                 "Appointment Confirmed",
# #                 booking_confirmed_email(patient_name, doctor_name, apt_time))

# #         if email := doctor_data.get("email"):
# #             background_tasks.add_task(send_email, email, doctor_name,
# #                 "New Appointment Scheduled",
# #                 booking_confirmed_email(doctor_name, patient_name, apt_time))

# #         return {"success": True, "message": "Notifications sent"}

# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=str(e))


# # @app.post("/appointment-canceled")
# # async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
# #     try:
# #         patient_data = await get_user_data(request.patient_id)
# #         doctor_data = await get_user_data(request.doctor_id)
# #         if not patient_data or not doctor_data:
# #             raise HTTPException(status_code=404, detail="User not found")

# #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# #         apt_time = format_datetime(request.appointment_datetime)

# #         if fcm := patient_data.get("fcmToken"):
# #             background_tasks.add_task(send_fcm_notification, fcm,
# #                 "Appointment Canceled ❌",
# #                 f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
# #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# #         if fcm := doctor_data.get("fcmToken"):
# #             background_tasks.add_task(send_fcm_notification, fcm,
# #                 "Appointment Canceled ❌",
# #                 f"Appointment with {patient_name} on {apt_time} was canceled",
# #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# #         if email := patient_data.get("email"):
# #             background_tasks.add_task(send_email, email, patient_name,
# #                 "Appointment Canceled",
# #                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

# #         if email := doctor_data.get("email"):
# #             background_tasks.add_task(send_email, email, doctor_name,
# #                 "Appointment Canceled",
# #                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

# #         return {"success": True, "message": "Cancellation notifications sent"}

# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=str(e))


# # @app.get("/check-reminders")
# # async def check_reminders(background_tasks: BackgroundTasks):
# #     try:
# #         now = datetime.now(timezone.utc)
# #         in_24h = now + timedelta(hours=24)
# #         in_1h = now + timedelta(hours=1)

# #         upcoming = (
# #             db.collection("appointments")
# #             .where("status", "==", "confirmed")
# #             .where("appointmentDateTime", ">=", now.isoformat())
# #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# #             .stream()
# #         )

# #         reminders_sent = 0
# #         for doc in upcoming:
# #             appointment = doc.to_dict()
# #             try:
# #                 apt_time = datetime.fromisoformat(
# #                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
# #             except Exception:
# #                 continue

# #             last = appointment.get("lastReminderSent")
# #             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
# #                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
# #                 reminders_sent += 1
# #             if now <= apt_time <= in_1h and last != "1h":
# #                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
# #                 reminders_sent += 1

# #         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=str(e))


# # async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
# #     patient_data = await get_user_data(appointment.get("patientId"))
# #     doctor_data = await get_user_data(appointment.get("doctorId"))
# #     if not patient_data or not doctor_data:
# #         return

# #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# #     apt_time = format_datetime(appointment.get("appointmentDateTime"))
# #     title = f"⏰ Appointment in {hours_until}h"

# #     if fcm := patient_data.get("fcmToken"):
# #         background_tasks.add_task(send_fcm_notification, fcm, title,
# #             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
# #             {"type": "reminder", "appointment_id": appointment_id})
# #     if email := patient_data.get("email"):
# #         background_tasks.add_task(send_email, email, patient_name,
# #             f"Appointment Reminder - {hours_until}h",
# #             reminder_email(patient_name, doctor_name, apt_time, hours_until))
# #     if fcm := doctor_data.get("fcmToken"):
# #         background_tasks.add_task(send_fcm_notification, fcm, title,
# #             f"Reminder: Appointment with {patient_name} at {apt_time}",
# #             {"type": "reminder", "appointment_id": appointment_id})
# #     if email := doctor_data.get("email"):
# #         background_tasks.add_task(send_email, email, doctor_name,
# #             f"Appointment Reminder - {hours_until}h",
# #             reminder_email(doctor_name, patient_name, apt_time, hours_until))

# #     reminder_key = "1h" if hours_until == 1 else "24h"
# #     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
# #     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=8000)






# # # """
# # # TeleMed FastAPI Backend
# # # Handles notifications, emails, scheduled reminders, AND file uploads via Appwrite
# # # """

# # # import os
# # # import mimetypes
# # # import smtplib
# # # import tempfile
# # # from email.mime.text import MIMEText
# # # from email.mime.multipart import MIMEMultipart
# # # from datetime import datetime, timedelta, timezone
# # # from typing import Optional, Dict, Any
# # # from dotenv import load_dotenv

# # # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # # from fastapi.middleware.cors import CORSMiddleware
# # # from pydantic import BaseModel

# # # import firebase_admin
# # # from firebase_admin import credentials, firestore, messaging

# # # # Appwrite SDK
# # # from appwrite.client import Client
# # # from appwrite.services.storage import Storage
# # # from appwrite.input_file import InputFile
# # # from appwrite.id import ID

# # # load_dotenv()

# # # # ============================================================================
# # # # CONFIG
# # # # ============================================================================

# # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # SMTP_USER = os.getenv("SMTP_USER")
# # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # Appwrite config — set these in Render environment variables
# # # APPWRITE_ENDPOINT   = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
# # # APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")   # from Appwrite console
# # # APPWRITE_API_KEY    = os.getenv("APPWRITE_API_KEY")       # Server API key
# # # APPWRITE_BUCKET_ID  = os.getenv("APPWRITE_BUCKET_ID")     # Storage bucket ID

# # # # Build Appwrite client
# # # appwrite_client = Client()
# # # appwrite_client.set_endpoint(APPWRITE_ENDPOINT)
# # # appwrite_client.set_project(APPWRITE_PROJECT_ID)
# # # appwrite_client.set_key(APPWRITE_API_KEY)

# # # appwrite_storage = Storage(appwrite_client)

# # # # ============================================================================
# # # # FASTAPI APP
# # # # ============================================================================

# # # app = FastAPI(
# # #     title="TeleMed Backend",
# # #     description="Notification, email, and file upload service for TeleMed app",
# # #     version="3.0.0"
# # # )

# # # app.add_middleware(
# # #     CORSMiddleware,
# # #     allow_origins=["*"],
# # #     allow_credentials=True,
# # #     allow_methods=["*"],
# # #     allow_headers=["*"],
# # # )

# # # # Firebase
# # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # firebase_admin.initialize_app(cred)
# # # db = firestore.client()


# # # # ============================================================================
# # # # PYDANTIC MODELS
# # # # ============================================================================

# # # class BookingConfirmedRequest(BaseModel):
# # #     appointment_id: str
# # #     patient_id: str
# # #     doctor_id: str
# # #     appointment_datetime: str
# # #     duration_minutes: int


# # # class AppointmentCanceledRequest(BaseModel):
# # #     appointment_id: str
# # #     patient_id: str
# # #     doctor_id: str
# # #     canceled_by: str
# # #     appointment_datetime: str


# # # class FileUploadResponse(BaseModel):
# # #     success: bool
# # #     url: str
# # #     file_id: str
# # #     message: str


# # # # ============================================================================
# # # # FILE UPLOAD — APPWRITE
# # # # No signatures, no credentials in requests, just works.
# # # # ============================================================================

# # # ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}


# # # def resolve_content_type(file: UploadFile) -> str:
# # #     """Detect real MIME type — Flutter sends octet-stream by default."""
# # #     if file.content_type and file.content_type != "application/octet-stream":
# # #         return file.content_type
# # #     if file.filename:
# # #         guessed, _ = mimetypes.guess_type(file.filename)
# # #         if guessed:
# # #             print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
# # #             return guessed
# # #     print("⚠️ Defaulting MIME to image/jpeg")
# # #     return "image/jpeg"


# # # def build_appwrite_view_url(file_id: str) -> str:
# # #     """
# # #     Build a direct public view URL for the uploaded file.
# # #     Requires the bucket to have 'File Security' disabled OR the file to be public.
# # #     """
# # #     return (
# # #         f"{APPWRITE_ENDPOINT}/storage/buckets/{APPWRITE_BUCKET_ID}"
# # #         f"/files/{file_id}/view?project={APPWRITE_PROJECT_ID}"
# # #     )


# # # async def upload_to_appwrite(
# # #     file: UploadFile,
# # #     user_id: str,
# # #     file_type: str,
# # #     content_type: str,
# # # ) -> Dict[str, Any]:
# # #     """
# # #     Upload file to Appwrite Storage.
# # #     Appwrite requires a real file path (not a stream) so we write to a temp file first.
# # #     """
# # #     contents = await file.read()

# # #     # Determine extension from content type
# # #     ext_map = {
# # #         "image/jpeg": ".jpg",
# # #         "image/jpg": ".jpg",
# # #         "image/png": ".png",
# # #         "image/webp": ".webp",
# # #         "application/pdf": ".pdf",
# # #     }
# # #     ext = ext_map.get(content_type, ".jpg")

# # #     # Write to temp file — Appwrite SDK needs a file path
# # #     with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
# # #         tmp.write(contents)
# # #         tmp_path = tmp.name

# # #     try:
# # #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# # #         # Use a meaningful filename so it's identifiable in Appwrite console
# # #         filename = f"{user_id}_{file_type}_{timestamp}{ext}"

# # #         # Upload — ID.unique() generates a unique Appwrite file ID automatically
# # #         result = appwrite_storage.create_file(
# # #             bucket_id=APPWRITE_BUCKET_ID,
# # #             file_id=ID.unique(),
# # #             file=InputFile.from_path(tmp_path, filename=filename),
# # #         )

# # #         file_id = result['$id']
# # #         url = build_appwrite_view_url(file_id)

# # #         print(f"✅ Appwrite upload OK — file_id: {file_id}")
# # #         print(f"   URL: {url}")

# # #         return {
# # #             "success": True,
# # #             "file_id": file_id,
# # #             "url": url,
# # #         }

# # #     except Exception as e:
# # #         print(f"❌ Appwrite upload failed: {e}")
# # #         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

# # #     finally:
# # #         # Always clean up temp file
# # #         if os.path.exists(tmp_path):
# # #             os.remove(tmp_path)


# # # # ============================================================================
# # # # FIREBASE / EMAIL HELPERS
# # # # ============================================================================

# # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # #     try:
# # #         doc = db.collection("users").document(uid).get()
# # #         return doc.to_dict() if doc.exists else None
# # #     except Exception as e:
# # #         print(f"❌ Error fetching user {uid}: {e}")
# # #         return None


# # # async def send_fcm_notification(
# # #     fcm_token: str,
# # #     title: str,
# # #     body: str,
# # #     data: Optional[Dict[str, str]] = None
# # # ):
# # #     if not fcm_token:
# # #         return
# # #     try:
# # #         msg = messaging.Message(
# # #             notification=messaging.Notification(title=title, body=body),
# # #             data=data or {},
# # #             token=fcm_token,
# # #         )
# # #         messaging.send(msg)
# # #         print(f"✅ FCM sent")
# # #     except Exception as e:
# # #         print(f"❌ FCM failed: {e}")


# # # async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
# # #     try:
# # #         msg = MIMEMultipart('alternative')
# # #         msg['Subject'] = subject
# # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # #         msg['To'] = to_email
# # #         msg.attach(MIMEText(html_content, 'html'))
# # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # #             server.starttls()
# # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # #             server.send_message(msg)
# # #         print(f"✅ Email sent to {to_email}")
# # #     except Exception as e:
# # #         print(f"❌ Email failed: {e}")


# # # def format_datetime(iso_string: str) -> str:
# # #     try:
# # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # #     except:
# # #         return iso_string


# # # # ============================================================================
# # # # EMAIL TEMPLATES
# # # # ============================================================================

# # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # #     return f"""<!DOCTYPE html><html><head><style>
# # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # #         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
# # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # #     </style></head><body><div class="container">
# # #         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
# # #         <div class="content">
# # #             <p>Hi {patient_name},</p>
# # #             <p>Your telemedicine appointment has been confirmed.</p>
# # #             <div class="info-box">
# # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # #             </div>
# # #             <p>Please be ready a few minutes before the scheduled time.</p>
# # #         </div>
# # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # #     </div></body></html>"""


# # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # #     return f"""<!DOCTYPE html><html><head><style>
# # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # #         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
# # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # #     </style></head><body><div class="container">
# # #         <div class="header"><h1>❌ Appointment Canceled</h1></div>
# # #         <div class="content">
# # #             <p>Hi {name},</p>
# # #             <p>Your appointment was canceled by the {canceled_by}.</p>
# # #             <div class="info-box">
# # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # #                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
# # #             </div>
# # #             <p>You can rebook anytime through the app.</p>
# # #         </div>
# # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # #     </div></body></html>"""


# # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # #     return f"""<!DOCTYPE html><html><head><style>
# # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # #         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
# # #         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
# # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # #     </style></head><body><div class="container">
# # #         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
# # #         <div class="content">
# # #             <p>Hi {name},</p>
# # #             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
# # #             <div class="info-box">
# # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # #             </div>
# # #         </div>
# # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # #     </div></body></html>"""


# # # # ============================================================================
# # # # ENDPOINTS
# # # # ============================================================================

# # # @app.api_route("/", methods=["GET", "HEAD"])
# # # async def root():
# # #     return {
# # #         "status": "healthy",
# # #         "service": "TeleMed Backend",
# # #         "version": "3.0.0",
# # #         "file_storage": "appwrite",
# # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # #     }


# # # @app.post("/upload-document", response_model=FileUploadResponse)
# # # async def upload_document(
# # #     file: UploadFile = File(...),
# # #     user_id: str = Form(...),
# # #     file_type: str = Form(...),
# # # ):
# # #     """Upload a document or image to Appwrite Storage."""
# # #     try:
# # #         content_type = resolve_content_type(file)
# # #         print(f"📎 Content type: {content_type} (raw: {file.content_type})")

# # #         if content_type not in ALLOWED_TYPES:
# # #             raise HTTPException(
# # #                 status_code=400,
# # #                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
# # #             )

# # #         # Check file size (max 10MB)
# # #         file.file.seek(0, 2)
# # #         file_size = file.file.tell()
# # #         file.file.seek(0)

# # #         if file_size > 10 * 1024 * 1024:
# # #             raise HTTPException(
# # #                 status_code=400,
# # #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
# # #             )

# # #         print(f"📤 Uploading {file_type} for {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

# # #         result = await upload_to_appwrite(file, user_id, file_type, content_type)

# # #         return FileUploadResponse(
# # #             success=True,
# # #             url=result['url'],
# # #             file_id=result['file_id'],
# # #             message="File uploaded successfully"
# # #         )

# # #     except HTTPException:
# # #         raise
# # #     except Exception as e:
# # #         print(f"❌ Upload error: {e}")
# # #         raise HTTPException(status_code=500, detail=str(e))


# # # @app.post("/booking-confirmed")
# # # async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
# # #     try:
# # #         patient_data = await get_user_data(request.patient_id)
# # #         doctor_data = await get_user_data(request.doctor_id)
# # #         if not patient_data or not doctor_data:
# # #             raise HTTPException(status_code=404, detail="User not found")

# # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # #         apt_time = format_datetime(request.appointment_datetime)

# # #         if fcm := patient_data.get("fcmToken"):
# # #             background_tasks.add_task(send_fcm_notification, fcm,
# # #                 "Appointment Confirmed ✅",
# # #                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
# # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # #         if fcm := doctor_data.get("fcmToken"):
# # #             background_tasks.add_task(send_fcm_notification, fcm,
# # #                 "New Appointment 📅",
# # #                 f"New appointment with {patient_name} for {apt_time}",
# # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # #         if email := patient_data.get("email"):
# # #             background_tasks.add_task(send_email, email, patient_name,
# # #                 "Appointment Confirmed",
# # #                 booking_confirmed_email(patient_name, doctor_name, apt_time))

# # #         if email := doctor_data.get("email"):
# # #             background_tasks.add_task(send_email, email, doctor_name,
# # #                 "New Appointment Scheduled",
# # #                 booking_confirmed_email(doctor_name, patient_name, apt_time))

# # #         return {"success": True, "message": "Notifications sent"}

# # #     except Exception as e:
# # #         raise HTTPException(status_code=500, detail=str(e))


# # # @app.post("/appointment-canceled")
# # # async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
# # #     try:
# # #         patient_data = await get_user_data(request.patient_id)
# # #         doctor_data = await get_user_data(request.doctor_id)
# # #         if not patient_data or not doctor_data:
# # #             raise HTTPException(status_code=404, detail="User not found")

# # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # #         apt_time = format_datetime(request.appointment_datetime)

# # #         if fcm := patient_data.get("fcmToken"):
# # #             background_tasks.add_task(send_fcm_notification, fcm,
# # #                 "Appointment Canceled ❌",
# # #                 f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
# # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # #         if fcm := doctor_data.get("fcmToken"):
# # #             background_tasks.add_task(send_fcm_notification, fcm,
# # #                 "Appointment Canceled ❌",
# # #                 f"Appointment with {patient_name} on {apt_time} was canceled",
# # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # #         if email := patient_data.get("email"):
# # #             background_tasks.add_task(send_email, email, patient_name,
# # #                 "Appointment Canceled",
# # #                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

# # #         if email := doctor_data.get("email"):
# # #             background_tasks.add_task(send_email, email, doctor_name,
# # #                 "Appointment Canceled",
# # #                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

# # #         return {"success": True, "message": "Cancellation notifications sent"}

# # #     except Exception as e:
# # #         raise HTTPException(status_code=500, detail=str(e))


# # # @app.get("/check-reminders")
# # # async def check_reminders(background_tasks: BackgroundTasks):
# # #     try:
# # #         now = datetime.now(timezone.utc)
# # #         in_24h = now + timedelta(hours=24)
# # #         in_1h = now + timedelta(hours=1)

# # #         upcoming = (
# # #             db.collection("appointments")
# # #             .where("status", "==", "confirmed")
# # #             .where("appointmentDateTime", ">=", now.isoformat())
# # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # #             .stream()
# # #         )

# # #         reminders_sent = 0
# # #         for doc in upcoming:
# # #             appointment = doc.to_dict()
# # #             try:
# # #                 apt_time = datetime.fromisoformat(
# # #                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
# # #             except Exception:
# # #                 continue

# # #             last = appointment.get("lastReminderSent")
# # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
# # #                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
# # #                 reminders_sent += 1
# # #             if now <= apt_time <= in_1h and last != "1h":
# # #                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
# # #                 reminders_sent += 1

# # #         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

# # #     except Exception as e:
# # #         raise HTTPException(status_code=500, detail=str(e))


# # # async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
# # #     patient_data = await get_user_data(appointment.get("patientId"))
# # #     doctor_data = await get_user_data(appointment.get("doctorId"))
# # #     if not patient_data or not doctor_data:
# # #         return

# # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # #     apt_time = format_datetime(appointment.get("appointmentDateTime"))
# # #     title = f"⏰ Appointment in {hours_until}h"

# # #     if fcm := patient_data.get("fcmToken"):
# # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # #             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
# # #             {"type": "reminder", "appointment_id": appointment_id})
# # #     if email := patient_data.get("email"):
# # #         background_tasks.add_task(send_email, email, patient_name,
# # #             f"Appointment Reminder - {hours_until}h",
# # #             reminder_email(patient_name, doctor_name, apt_time, hours_until))
# # #     if fcm := doctor_data.get("fcmToken"):
# # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # #             f"Reminder: Appointment with {patient_name} at {apt_time}",
# # #             {"type": "reminder", "appointment_id": appointment_id})
# # #     if email := doctor_data.get("email"):
# # #         background_tasks.add_task(send_email, email, doctor_name,
# # #             f"Appointment Reminder - {hours_until}h",
# # #             reminder_email(doctor_name, patient_name, apt_time, hours_until))

# # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # #     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
# # #     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# # # if __name__ == "__main__":
# # #     import uvicorn
# # #     uvicorn.run(app, host="0.0.0.0", port=8000)
# # # # """
# # # # TeleMed FastAPI Backend
# # # # File storage: Backblaze B2 (S3-compatible, no signature headaches)
# # # # """

# # # # import os
# # # # import mimetypes
# # # # import smtplib
# # # # import uuid
# # # # from email.mime.text import MIMEText
# # # # from email.mime.multipart import MIMEMultipart
# # # # from datetime import datetime, timedelta, timezone
# # # # from typing import Optional, Dict, Any
# # # # from dotenv import load_dotenv

# # # # import boto3
# # # # from botocore.client import Config

# # # # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # # # from fastapi.middleware.cors import CORSMiddleware
# # # # from pydantic import BaseModel

# # # # import firebase_admin
# # # # from firebase_admin import credentials, firestore, messaging

# # # # load_dotenv()

# # # # # ============================================================================
# # # # # CONFIG
# # # # # ============================================================================

# # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # Backblaze B2 credentials — add these to your Render environment variables:
# # # # # B2_KEY_ID        → your Backblaze Application Key ID
# # # # # B2_APPLICATION_KEY → your Backblaze Application Key
# # # # # B2_BUCKET_NAME   → your bucket name (e.g. "sheydoc-documents")
# # # # # B2_BUCKET_REGION → your bucket region (e.g. "us-west-004")
# # # # #
# # # # # Backblaze endpoint format: https://s3.<region>.backblazeb2.com
# # # # B2_KEY_ID = os.getenv("B2_KEY_ID")
# # # # B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
# # # # B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
# # # # B2_BUCKET_REGION = os.getenv("B2_BUCKET_REGION", "us-west-004")
# # # # B2_ENDPOINT = f"https://s3.{B2_BUCKET_REGION}.backblazeb2.com"

# # # # # Build the boto3 S3 client pointed at Backblaze
# # # # s3 = boto3.client(
# # # #     service_name="s3",
# # # #     endpoint_url=B2_ENDPOINT,
# # # #     aws_access_key_id=B2_KEY_ID,
# # # #     aws_secret_access_key=B2_APPLICATION_KEY,
# # # #     config=Config(signature_version="s3v4"),
# # # # )

# # # # # ============================================================================
# # # # # APP SETUP
# # # # # ============================================================================

# # # # app = FastAPI(
# # # #     title="TeleMed Backend",
# # # #     description="Notification, email, and file upload service for TeleMed app",
# # # #     version="3.0.0"
# # # # )

# # # # app.add_middleware(
# # # #     CORSMiddleware,
# # # #     allow_origins=["*"],
# # # #     allow_credentials=True,
# # # #     allow_methods=["*"],
# # # #     allow_headers=["*"],
# # # # )

# # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # firebase_admin.initialize_app(cred)
# # # # db = firestore.client()


# # # # # ============================================================================
# # # # # PYDANTIC MODELS
# # # # # ============================================================================

# # # # class BookingConfirmedRequest(BaseModel):
# # # #     appointment_id: str
# # # #     patient_id: str
# # # #     doctor_id: str
# # # #     appointment_datetime: str
# # # #     duration_minutes: int


# # # # class AppointmentCanceledRequest(BaseModel):
# # # #     appointment_id: str
# # # #     patient_id: str
# # # #     doctor_id: str
# # # #     canceled_by: str
# # # #     appointment_datetime: str


# # # # class FileUploadResponse(BaseModel):
# # # #     success: bool
# # # #     url: str
# # # #     file_key: str
# # # #     size_bytes: int
# # # #     message: str


# # # # # ============================================================================
# # # # # FILE UPLOAD — BACKBLAZE B2
# # # # # ============================================================================

# # # # ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}

# # # # FOLDER_MAP = {
# # # #     "education_certificate": "doctors/certificates",
# # # #     "authorization_file":    "doctors/authorizations",
# # # #     "affiliate_hospital":    "doctors/hospitals",
# # # #     "id_card":               "doctors/ids",
# # # #     "profile_photo":         "doctors/photos",
# # # # }


# # # # def resolve_content_type(file: UploadFile) -> str:
# # # #     """Determine real MIME type — handles Flutter's octet-stream default."""
# # # #     if file.content_type and file.content_type != "application/octet-stream":
# # # #         return file.content_type
# # # #     if file.filename:
# # # #         guessed, _ = mimetypes.guess_type(file.filename)
# # # #         if guessed:
# # # #             print(f"🔍 Guessed MIME from '{file.filename}': {guessed}")
# # # #             return guessed
# # # #     return "image/jpeg"


# # # # async def upload_to_b2(
# # # #     file: UploadFile,
# # # #     user_id: str,
# # # #     file_type: str,
# # # #     content_type: str,
# # # # ) -> Dict[str, Any]:
# # # #     """
# # # #     Upload file to Backblaze B2 using boto3 S3-compatible API.
# # # #     No manual signature generation — boto3 handles everything.
# # # #     """
# # # #     contents = await file.read()
# # # #     file_size = len(contents)

# # # #     folder = FOLDER_MAP.get(file_type, "doctors/documents")
# # # #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# # # #     unique_id = uuid.uuid4().hex[:8]

# # # #     # Determine file extension from content type
# # # #     ext_map = {
# # # #         "image/jpeg": ".jpg",
# # # #         "image/jpg":  ".jpg",
# # # #         "image/png":  ".png",
# # # #         "image/webp": ".webp",
# # # #         "application/pdf": ".pdf",
# # # #     }
# # # #     ext = ext_map.get(content_type, ".jpg")

# # # #     # Final key (path) in the bucket
# # # #     file_key = f"{folder}/{user_id}_{file_type}_{timestamp}_{unique_id}{ext}"

# # # #     print(f"📤 Uploading to B2: {file_key} ({file_size / 1024:.1f}KB) [{content_type}]")

# # # #     # Simple put_object — boto3 handles signing automatically
# # # #     s3.put_object(
# # # #         Bucket=B2_BUCKET_NAME,
# # # #         Key=file_key,
# # # #         Body=contents,
# # # #         ContentType=content_type,
# # # #         # Make file publicly readable
# # # #         ACL="public-read",
# # # #     )

# # # #     # Public URL format for Backblaze B2
# # # #     public_url = f"{B2_ENDPOINT}/{B2_BUCKET_NAME}/{file_key}"

# # # #     print(f"✅ B2 upload OK: {public_url}")

# # # #     return {
# # # #         "success": True,
# # # #         "url": public_url,
# # # #         "file_key": file_key,
# # # #         "size_bytes": file_size,
# # # #     }


# # # # # ============================================================================
# # # # # FIREBASE HELPERS
# # # # # ============================================================================

# # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # #     try:
# # # #         doc = db.collection("users").document(uid).get()
# # # #         return doc.to_dict() if doc.exists else None
# # # #     except Exception as e:
# # # #         print(f"❌ Error fetching user {uid}: {e}")
# # # #         return None


# # # # async def send_fcm_notification(
# # # #     fcm_token: str, title: str, body: str,
# # # #     data: Optional[Dict[str, str]] = None
# # # # ):
# # # #     if not fcm_token:
# # # #         return
# # # #     try:
# # # #         msg = messaging.Message(
# # # #             notification=messaging.Notification(title=title, body=body),
# # # #             data=data or {},
# # # #             token=fcm_token,
# # # #         )
# # # #         messaging.send(msg)
# # # #         print(f"✅ FCM sent")
# # # #     except Exception as e:
# # # #         print(f"❌ FCM failed: {e}")


# # # # async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
# # # #     try:
# # # #         msg = MIMEMultipart('alternative')
# # # #         msg['Subject'] = subject
# # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # #         msg['To'] = to_email
# # # #         msg.attach(MIMEText(html_content, 'html'))
# # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # #             server.starttls()
# # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # #             server.send_message(msg)
# # # #         print(f"✅ Email sent to {to_email}")
# # # #     except Exception as e:
# # # #         print(f"❌ Email failed: {e}")


# # # # def format_datetime(iso_string: str) -> str:
# # # #     try:
# # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # #     except:
# # # #         return iso_string


# # # # # ============================================================================
# # # # # EMAIL TEMPLATES
# # # # # ============================================================================

# # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # #     return f"""<!DOCTYPE html><html><head><style>
# # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # #         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
# # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # #     </style></head><body><div class="container">
# # # #         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
# # # #         <div class="content">
# # # #             <p>Hi {patient_name},</p>
# # # #             <p>Your telemedicine appointment has been confirmed.</p>
# # # #             <div class="info-box">
# # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # # #             </div>
# # # #             <p>Please be ready a few minutes before the scheduled time.</p>
# # # #         </div>
# # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # #     </div></body></html>"""


# # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # #     return f"""<!DOCTYPE html><html><head><style>
# # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # #         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
# # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # #     </style></head><body><div class="container">
# # # #         <div class="header"><h1>❌ Appointment Canceled</h1></div>
# # # #         <div class="content">
# # # #             <p>Hi {name},</p>
# # # #             <p>Your appointment was canceled by the {canceled_by}.</p>
# # # #             <div class="info-box">
# # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # #                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
# # # #             </div>
# # # #             <p>You can rebook anytime through the app.</p>
# # # #         </div>
# # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # #     </div></body></html>"""


# # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # #     return f"""<!DOCTYPE html><html><head><style>
# # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # #         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
# # # #         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
# # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # #     </style></head><body><div class="container">
# # # #         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
# # # #         <div class="content">
# # # #             <p>Hi {name},</p>
# # # #             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
# # # #             <div class="info-box">
# # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # # #             </div>
# # # #         </div>
# # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # #     </div></body></html>"""


# # # # # ============================================================================
# # # # # ENDPOINTS
# # # # # ============================================================================

# # # # @app.api_route("/", methods=["GET", "HEAD"])
# # # # async def root():
# # # #     return {
# # # #         "status": "healthy",
# # # #         "service": "TeleMed Backend",
# # # #         "version": "3.0.0",
# # # #         "file_storage": "backblaze_b2",
# # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # #     }


# # # # @app.post("/upload-document", response_model=FileUploadResponse)
# # # # async def upload_document(
# # # #     file: UploadFile = File(...),
# # # #     user_id: str = Form(...),
# # # #     file_type: str = Form(...),
# # # # ):
# # # #     try:
# # # #         content_type = resolve_content_type(file)
# # # #         print(f"📎 Content type: {content_type} (raw: {file.content_type})")

# # # #         if content_type not in ALLOWED_TYPES:
# # # #             raise HTTPException(
# # # #                 status_code=400,
# # # #                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
# # # #             )

# # # #         # Check size before reading full file
# # # #         file.file.seek(0, 2)
# # # #         file_size = file.file.tell()
# # # #         file.file.seek(0)

# # # #         if file_size > 10 * 1024 * 1024:
# # # #             raise HTTPException(
# # # #                 status_code=400,
# # # #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Max is 10MB."
# # # #             )

# # # #         result = await upload_to_b2(file, user_id, file_type, content_type)

# # # #         return FileUploadResponse(
# # # #             success=True,
# # # #             url=result["url"],
# # # #             file_key=result["file_key"],
# # # #             size_bytes=result["size_bytes"],
# # # #             message="File uploaded successfully"
# # # #         )

# # # #     except HTTPException:
# # # #         raise
# # # #     except Exception as e:
# # # #         print(f"❌ Upload error: {e}")
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.post("/booking-confirmed")
# # # # async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
# # # #     try:
# # # #         patient_data = await get_user_data(request.patient_id)
# # # #         doctor_data = await get_user_data(request.doctor_id)
# # # #         if not patient_data or not doctor_data:
# # # #             raise HTTPException(status_code=404, detail="User not found")

# # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #         apt_time = format_datetime(request.appointment_datetime)

# # # #         if fcm := patient_data.get("fcmToken"):
# # # #             background_tasks.add_task(send_fcm_notification, fcm,
# # # #                 "Appointment Confirmed ✅",
# # # #                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
# # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # # #         if fcm := doctor_data.get("fcmToken"):
# # # #             background_tasks.add_task(send_fcm_notification, fcm,
# # # #                 "New Appointment 📅",
# # # #                 f"New appointment with {patient_name} for {apt_time}",
# # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # # #         if email := patient_data.get("email"):
# # # #             background_tasks.add_task(send_email, email, patient_name,
# # # #                 "Appointment Confirmed",
# # # #                 booking_confirmed_email(patient_name, doctor_name, apt_time))

# # # #         if email := doctor_data.get("email"):
# # # #             background_tasks.add_task(send_email, email, doctor_name,
# # # #                 "New Appointment Scheduled",
# # # #                 booking_confirmed_email(doctor_name, patient_name, apt_time))

# # # #         return {"success": True, "message": "Notifications sent"}

# # # #     except Exception as e:
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.post("/appointment-canceled")
# # # # async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
# # # #     try:
# # # #         patient_data = await get_user_data(request.patient_id)
# # # #         doctor_data = await get_user_data(request.doctor_id)
# # # #         if not patient_data or not doctor_data:
# # # #             raise HTTPException(status_code=404, detail="User not found")

# # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #         apt_time = format_datetime(request.appointment_datetime)

# # # #         if fcm := patient_data.get("fcmToken"):
# # # #             background_tasks.add_task(send_fcm_notification, fcm,
# # # #                 "Appointment Canceled ❌",
# # # #                 f"Appointment with Dr. {doctor_name} on {apt_time} was canceled",
# # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # # #         if fcm := doctor_data.get("fcmToken"):
# # # #             background_tasks.add_task(send_fcm_notification, fcm,
# # # #                 "Appointment Canceled ❌",
# # # #                 f"Appointment with {patient_name} on {apt_time} was canceled",
# # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # # #         if email := patient_data.get("email"):
# # # #             background_tasks.add_task(send_email, email, patient_name,
# # # #                 "Appointment Canceled",
# # # #                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

# # # #         if email := doctor_data.get("email"):
# # # #             background_tasks.add_task(send_email, email, doctor_name,
# # # #                 "Appointment Canceled",
# # # #                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

# # # #         return {"success": True, "message": "Cancellation notifications sent"}

# # # #     except Exception as e:
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.get("/check-reminders")
# # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # #     try:
# # # #         now = datetime.now(timezone.utc)
# # # #         in_24h = now + timedelta(hours=24)
# # # #         in_1h = now + timedelta(hours=1)

# # # #         upcoming = (
# # # #             db.collection("appointments")
# # # #             .where("status", "==", "confirmed")
# # # #             .where("appointmentDateTime", ">=", now.isoformat())
# # # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # # #             .stream()
# # # #         )

# # # #         reminders_sent = 0
# # # #         for doc in upcoming:
# # # #             appointment = doc.to_dict()
# # # #             try:
# # # #                 apt_time = datetime.fromisoformat(
# # # #                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
# # # #             except Exception:
# # # #                 continue

# # # #             last = appointment.get("lastReminderSent")
# # # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
# # # #                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
# # # #                 reminders_sent += 1
# # # #             if now <= apt_time <= in_1h and last != "1h":
# # # #                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
# # # #                 reminders_sent += 1

# # # #         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

# # # #     except Exception as e:
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
# # # #     patient_data = await get_user_data(appointment.get("patientId"))
# # # #     doctor_data = await get_user_data(appointment.get("doctorId"))
# # # #     if not patient_data or not doctor_data:
# # # #         return

# # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #     apt_time = format_datetime(appointment.get("appointmentDateTime"))
# # # #     title = f"⏰ Appointment in {hours_until}h"

# # # #     if fcm := patient_data.get("fcmToken"):
# # # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # # #             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
# # # #             {"type": "reminder", "appointment_id": appointment_id})
# # # #     if email := patient_data.get("email"):
# # # #         background_tasks.add_task(send_email, email, patient_name,
# # # #             f"Appointment Reminder - {hours_until}h",
# # # #             reminder_email(patient_name, doctor_name, apt_time, hours_until))
# # # #     if fcm := doctor_data.get("fcmToken"):
# # # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # # #             f"Reminder: Appointment with {patient_name} at {apt_time}",
# # # #             {"type": "reminder", "appointment_id": appointment_id})
# # # #     if email := doctor_data.get("email"):
# # # #         background_tasks.add_task(send_email, email, doctor_name,
# # # #             f"Appointment Reminder - {hours_until}h",
# # # #             reminder_email(doctor_name, patient_name, apt_time, hours_until))

# # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # #     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
# # # #     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# # # # if __name__ == "__main__":
# # # #     import uvicorn
# # # #     uvicorn.run(app, host="0.0.0.0", port=8000)







# # # # """
# # # # TeleMed FastAPI Backend
# # # # Handles notifications, emails, scheduled reminders, AND file uploads
# # # # """

# # # # import os
# # # # import smtplib
# # # # from email.mime.text import MIMEText
# # # # from email.mime.multipart import MIMEMultipart
# # # # from datetime import datetime, timedelta, timezone
# # # # from typing import Optional, List, Dict, Any
# # # # from dotenv import load_dotenv

# # # # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # # # from fastapi.middleware.cors import CORSMiddleware
# # # # from pydantic import BaseModel, EmailStr

# # # # import firebase_admin
# # # # from firebase_admin import credentials, firestore, messaging

# # # # # Cloudinary for file uploads
# # # # import cloudinary
# # # # import cloudinary.uploader
# # # # import cloudinary.api

# # # # # Load environment variables FIRST
# # # # load_dotenv()

# # # # # Email configuration
# # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # Cloudinary configuration
# # # # cloudinary.config(
# # # #     cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
# # # #     api_key=os.getenv("CLOUDINARY_API_KEY"),
# # # #     api_secret=os.getenv("CLOUDINARY_API_SECRET"),
# # # #     secure=True
# # # # )

# # # # # Initialize FastAPI
# # # # app = FastAPI(
# # # #     title="TeleMed Backend",
# # # #     description="Notification, email, and file upload service for TeleMed app",
# # # #     version="2.0.0"
# # # # )

# # # # # CORS - Allow your Flutter app to call this API
# # # # app.add_middleware(
# # # #     CORSMiddleware,
# # # #     allow_origins=["*"],  # In production, replace with your actual domain
# # # #     allow_credentials=True,
# # # #     allow_methods=["*"],
# # # #     allow_headers=["*"],
# # # # )

# # # # # Initialize Firebase Admin SDK
# # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # firebase_admin.initialize_app(cred)
# # # # db = firestore.client()


# # # # # ============================================================================
# # # # # PYDANTIC MODELS
# # # # # ============================================================================

# # # # class BookingConfirmedRequest(BaseModel):
# # # #     appointment_id: str
# # # #     patient_id: str
# # # #     doctor_id: str
# # # #     appointment_datetime: str  # ISO format
# # # #     duration_minutes: int


# # # # class AppointmentCanceledRequest(BaseModel):
# # # #     appointment_id: str
# # # #     patient_id: str
# # # #     doctor_id: str
# # # #     canceled_by: str  # "patient" or "doctor"
# # # #     appointment_datetime: str


# # # # class FileUploadResponse(BaseModel):
# # # #     success: bool
# # # #     url: str
# # # #     public_id: str
# # # #     format: str
# # # #     size_bytes: int
# # # #     message: str


# # # # # ============================================================================
# # # # # HELPER FUNCTIONS
# # # # # ============================================================================

# # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # #     """Fetch user data from Firestore"""
# # # #     try:
# # # #         user_ref = db.collection("users").document(uid)
# # # #         user_doc = user_ref.get()
# # # #         return user_doc.to_dict() if user_doc.exists else None
# # # #     except Exception as e:
# # # #         print(f"❌ Error fetching user data for {uid}: {e}")
# # # #         return None


# # # # async def send_fcm_notification(
# # # #     fcm_token: str,
# # # #     title: str,
# # # #     body: str,
# # # #     data: Optional[Dict[str, str]] = None
# # # # ):
# # # #     """Send FCM push notification"""
# # # #     if not fcm_token:
# # # #         print("⚠️ No FCM token provided")
# # # #         return
    
# # # #     try:
# # # #         message = messaging.Message(
# # # #             notification=messaging.Notification(title=title, body=body),
# # # #             data=data or {},
# # # #             token=fcm_token,
# # # #         )
# # # #         response = messaging.send(message)
# # # #         print(f"✅ FCM sent: {response}")
# # # #     except Exception as e:
# # # #         print(f"❌ FCM failed: {e}")


# # # # async def send_email(
# # # #     to_email: str,
# # # #     to_name: str,
# # # #     subject: str,
# # # #     html_content: str
# # # # ):
# # # #     """Send email via Gmail SMTP"""
# # # #     try:
# # # #         msg = MIMEMultipart('alternative')
# # # #         msg['Subject'] = subject
# # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # #         msg['To'] = to_email
        
# # # #         html_part = MIMEText(html_content, 'html')
# # # #         msg.attach(html_part)
        
# # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # #             server.starttls()
# # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # #             server.send_message(msg)
        
# # # #         print(f"✅ Email sent via Gmail to {to_email}")
# # # #     except Exception as e:
# # # #         print(f"❌ Email failed for {to_email}: {e}")


# # # # def format_datetime(iso_string: str) -> str:
# # # #     """Format ISO datetime to readable format"""
# # # #     try:
# # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # #     except:
# # # #         return iso_string


# # # # # ============================================================================
# # # # # FILE UPLOAD FUNCTIONS
# # # # # ============================================================================

# # # # def get_file_category(file_type: str) -> str:
# # # #     """Determine Cloudinary folder based on file type"""
# # # #     categories = {
# # # #         "education_certificate": "doctors/certificates",
# # # #         "authorization_file": "doctors/authorizations",
# # # #         "affiliate_hospital": "doctors/hospitals",
# # # #         "id_card": "doctors/ids",
# # # #         "profile_photo": "doctors/photos",
# # # #     }
# # # #     return categories.get(file_type, "doctors/documents")


# # # # async def upload_to_cloudinary(
# # # #     file: UploadFile,
# # # #     user_id: str,
# # # #     file_type: str
# # # # ) -> Dict[str, Any]:
# # # #     """
# # # #     Upload file to Cloudinary
# # # #     Returns URL and metadata
# # # #     """
# # # #     try:
# # # #         # Read file contents
# # # #         contents = await file.read()
        
# # # #         # Determine folder
# # # #         folder = get_file_category(file_type)
        
# # # #         # Generate unique public_id
# # # #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# # # #         public_id = f"{folder}/{user_id}_{file_type}_{timestamp}"
        
# # # #         # Upload to Cloudinary
# # # #         # For images: auto-optimize
# # # #         # For PDFs: store as-is
# # # #         resource_type = "image" if file.content_type.startswith("image") else "raw"
        
# # # #         upload_result = cloudinary.uploader.upload(
# # # #             contents,
# # # #             public_id=public_id,
# # # #             resource_type=resource_type,
# # # #             folder=folder,
# # # #             # Optimization for images
# # # #             quality="auto" if resource_type == "image" else None,
# # # #             fetch_format="auto" if resource_type == "image" else None,
# # # #         )
        
# # # #         print(f"✅ Uploaded to Cloudinary: {upload_result['secure_url']}")
        
# # # #         return {
# # # #             "success": True,
# # # #             "url": upload_result['secure_url'],
# # # #             "public_id": upload_result['public_id'],
# # # #             "format": upload_result['format'],
# # # #             "size_bytes": upload_result['bytes'],
# # # #             "resource_type": resource_type,
# # # #         }
        
# # # #     except Exception as e:
# # # #         print(f"❌ Cloudinary upload failed: {e}")
# # # #         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# # # # # ============================================================================
# # # # # EMAIL TEMPLATES
# # # # # ============================================================================

# # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # #     return f"""
# # # #     <!DOCTYPE html>
# # # #     <html>
# # # #     <head>
# # # #         <style>
# # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # #             .header {{ background: #4A90E2; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #4A90E2; }}
# # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # #         </style>
# # # #     </head>
# # # #     <body>
# # # #         <div class="container">
# # # #             <div class="header">
# # # #                 <h1>✅ Appointment Confirmed</h1>
# # # #             </div>
# # # #             <div class="content">
# # # #                 <p>Hi {patient_name},</p>
# # # #                 <p>Great news! Your telemedicine appointment has been confirmed.</p>
                
# # # #                 <div class="info-box">
# # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # #                 </div>
                
# # # #                 <p>You will receive reminder notifications before your appointment.</p>
# # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # #             </div>
# # # #             <div class="footer">
# # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # #             </div>
# # # #         </div>
# # # #     </body>
# # # #     </html>
# # # #     """


# # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # #     return f"""
# # # #     <!DOCTYPE html>
# # # #     <html>
# # # #     <head>
# # # #         <style>
# # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # #             .header {{ background: #E74C3C; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #E74C3C; }}
# # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # #         </style>
# # # #     </head>
# # # #     <body>
# # # #         <div class="container">
# # # #             <div class="header">
# # # #                 <h1>❌ Appointment Canceled</h1>
# # # #             </div>
# # # #             <div class="content">
# # # #                 <p>Hi {name},</p>
# # # #                 <p>We're writing to inform you that the following appointment has been canceled by the {canceled_by}.</p>
                
# # # #                 <div class="info-box">
# # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # #                     <p><strong>📅 Original Date &amp; Time:</strong> {appointment_time}</p>
# # # #                 </div>
                
# # # #                 <p>You can book a new appointment anytime through the app.</p>
# # # #             </div>
# # # #             <div class="footer">
# # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # #             </div>
# # # #         </div>
# # # #     </body>
# # # #     </html>
# # # #     """


# # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # #     return f"""
# # # #     <!DOCTYPE html>
# # # #     <html>
# # # #     <head>
# # # #         <style>
# # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # #             .header {{ background: #F39C12; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #F39C12; }}
# # # #             .reminder-badge {{ background: #F39C12; color: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 20px 0; font-weight: bold; }}
# # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # #         </style>
# # # #     </head>
# # # #     <body>
# # # #         <div class="container">
# # # #             <div class="header">
# # # #                 <h1>⏰ Appointment Reminder</h1>
# # # #             </div>
# # # #             <div class="content">
# # # #                 <p>Hi {name},</p>
# # # #                 <p>This is a friendly reminder about your upcoming appointment.</p>
                
# # # #                 <div style="text-align: center;">
# # # #                     <span class="reminder-badge">In {hours_until} hour(s)</span>
# # # #                 </div>
                
# # # #                 <div class="info-box">
# # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # #                 </div>
                
# # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # #             </div>
# # # #             <div class="footer">
# # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # #             </div>
# # # #         </div>
# # # #     </body>
# # # #     </html>
# # # #     """


# # # # # ============================================================================
# # # # # ENDPOINTS
# # # # # ============================================================================

# # # # @app.api_route("/", methods=["GET", "HEAD"])
# # # # async def root():
# # # #     """Health check endpoint"""
# # # #     return {
# # # #         "status": "healthy",
# # # #         "service": "TeleMed Backend",
# # # #         "version": "2.0.0",
# # # #         "email_provider": "gmail_smtp",
# # # #         "file_storage": "cloudinary",
# # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # #     }


# # # # @app.post("/upload-document", response_model=FileUploadResponse)
# # # # async def upload_document(
# # # #     file: UploadFile = File(...),
# # # #     user_id: str = Form(...),
# # # #     file_type: str = Form(...),  # e.g., "education_certificate", "id_card"
# # # # ):
# # # #     """
# # # #     Upload a document/image to Cloudinary
    
# # # #     Parameters:
# # # #     - file: The file to upload (image or PDF)
# # # #     - user_id: Firebase user ID
# # # #     - file_type: Type of document (education_certificate, id_card, etc.)
    
# # # #     Returns:
# # # #     - Secure URL to access the file
# # # #     - Public ID for potential deletion
# # # #     - File metadata
# # # #     """
# # # #     try:
# # # #         # Validate file type
# # # #         allowed_types = [
# # # #             "image/jpeg", "image/png", "image/jpg", "image/webp",
# # # #             "application/pdf"
# # # #         ]
        
# # # #         if file.content_type not in allowed_types:
# # # #             raise HTTPException(
# # # #                 status_code=400,
# # # #                 detail=f"File type {file.content_type} not allowed. Use JPG, PNG, WEBP, or PDF."
# # # #             )
        
# # # #         # Check file size (max 10MB)
# # # #         file.file.seek(0, 2)  # Seek to end
# # # #         file_size = file.file.tell()  # Get position (size)
# # # #         file.file.seek(0)  # Reset to start
        
# # # #         max_size = 10 * 1024 * 1024  # 10MB
# # # #         if file_size > max_size:
# # # #             raise HTTPException(
# # # #                 status_code=400,
# # # #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Maximum is 10MB."
# # # #             )
        
# # # #         print(f"📤 Uploading {file_type} for user {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")
        
# # # #         # Upload to Cloudinary
# # # #         result = await upload_to_cloudinary(file, user_id, file_type)
        
# # # #         return FileUploadResponse(
# # # #             success=result['success'],
# # # #             url=result['url'],
# # # #             public_id=result['public_id'],
# # # #             format=result['format'],
# # # #             size_bytes=result['size_bytes'],
# # # #             message="File uploaded successfully"
# # # #         )
        
# # # #     except HTTPException:
# # # #         raise
# # # #     except Exception as e:
# # # #         print(f"❌ Upload endpoint error: {e}")
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.post("/booking-confirmed")
# # # # async def booking_confirmed(
# # # #     request: BookingConfirmedRequest,
# # # #     background_tasks: BackgroundTasks
# # # # ):
# # # #     """
# # # #     Called when a new appointment is booked
# # # #     Sends notifications to both patient and doctor
# # # #     """
# # # #     try:
# # # #         # Fetch patient and doctor data
# # # #         patient_data = await get_user_data(request.patient_id)
# # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # #         if not patient_data or not doctor_data:
# # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # #         # Prepare notification content
# # # #         patient_title = "Appointment Confirmed ✅"
# # # #         patient_body = f"Your appointment with Dr. {doctor_name} is confirmed for {appointment_time}"
        
# # # #         doctor_title = "New Appointment 📅"
# # # #         doctor_body = f"New appointment with {patient_name} scheduled for {appointment_time}"
        
# # # #         # Send FCM notifications in background
# # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # #             background_tasks.add_task(
# # # #                 send_fcm_notification,
# # # #                 patient_fcm,
# # # #                 patient_title,
# # # #                 patient_body,
# # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # #             )
        
# # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # #             background_tasks.add_task(
# # # #                 send_fcm_notification,
# # # #                 doctor_fcm,
# # # #                 doctor_title,
# # # #                 doctor_body,
# # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # #             )
        
# # # #         # Send emails in background
# # # #         if patient_email := patient_data.get("email"):
# # # #             background_tasks.add_task(
# # # #                 send_email,
# # # #                 patient_email,
# # # #                 patient_name,
# # # #                 "Appointment Confirmed",
# # # #                 booking_confirmed_email(patient_name, doctor_name, appointment_time)
# # # #             )
        
# # # #         if doctor_email := doctor_data.get("email"):
# # # #             background_tasks.add_task(
# # # #                 send_email,
# # # #                 doctor_email,
# # # #                 doctor_name,
# # # #                 "New Appointment Scheduled",
# # # #                 booking_confirmed_email(doctor_name, patient_name, appointment_time)
# # # #             )
        
# # # #         return {
# # # #             "success": True,
# # # #             "message": "Notifications sent successfully",
# # # #             "patient": patient_name,
# # # #             "doctor": doctor_name
# # # #         }
    
# # # #     except Exception as e:
# # # #         print(f"❌ Error in booking_confirmed: {e}")
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.post("/appointment-canceled")
# # # # async def appointment_canceled(
# # # #     request: AppointmentCanceledRequest,
# # # #     background_tasks: BackgroundTasks
# # # # ):
# # # #     """
# # # #     Called when an appointment is canceled
# # # #     Sends notifications to both patient and doctor
# # # #     """
# # # #     try:
# # # #         # Fetch patient and doctor data
# # # #         patient_data = await get_user_data(request.patient_id)
# # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # #         if not patient_data or not doctor_data:
# # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # #         # Prepare notification content
# # # #         title = "Appointment Canceled ❌"
# # # #         patient_body = f"Your appointment with Dr. {doctor_name} on {appointment_time} has been canceled"
# # # #         doctor_body = f"Appointment with {patient_name} on {appointment_time} has been canceled"
        
# # # #         # Send FCM notifications
# # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # #             background_tasks.add_task(
# # # #                 send_fcm_notification,
# # # #                 patient_fcm,
# # # #                 title,
# # # #                 patient_body,
# # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # #             )
        
# # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # #             background_tasks.add_task(
# # # #                 send_fcm_notification,
# # # #                 doctor_fcm,
# # # #                 title,
# # # #                 doctor_body,
# # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # #             )
        
# # # #         # Send emails
# # # #         if patient_email := patient_data.get("email"):
# # # #             background_tasks.add_task(
# # # #                 send_email,
# # # #                 patient_email,
# # # #                 patient_name,
# # # #                 "Appointment Canceled",
# # # #                 appointment_canceled_email(patient_name, doctor_name, appointment_time, request.canceled_by)
# # # #             )
        
# # # #         if doctor_email := doctor_data.get("email"):
# # # #             background_tasks.add_task(
# # # #                 send_email,
# # # #                 doctor_email,
# # # #                 doctor_name,
# # # #                 "Appointment Canceled",
# # # #                 appointment_canceled_email(doctor_name, patient_name, appointment_time, request.canceled_by)
# # # #             )
        
# # # #         return {
# # # #             "success": True,
# # # #             "message": "Cancellation notifications sent",
# # # #             "canceled_by": request.canceled_by
# # # #         }
    
# # # #     except Exception as e:
# # # #         print(f"❌ Error in appointment_canceled: {e}")
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # @app.get("/check-reminders")
# # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # #     """
# # # #     Called by cron job every hour.
# # # #     Checks for appointments in next 24h and 1h windows.
# # # #     Sends reminder notifications.
# # # #     """
# # # #     try:
# # # #         now = datetime.now(timezone.utc)
# # # #         in_24h = now + timedelta(hours=24)
# # # #         in_1h = now + timedelta(hours=1)

# # # #         appointments_ref = db.collection("appointments")
# # # #         upcoming = (
# # # #             appointments_ref
# # # #             .where("status", "==", "confirmed")
# # # #             .where("appointmentDateTime", ">=", now.isoformat())
# # # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # # #             .stream()
# # # #         )

# # # #         reminders_sent = 0

# # # #         for doc in upcoming:
# # # #             appointment = doc.to_dict()

# # # #             try:
# # # #                 apt_time_str = appointment.get("appointmentDateTime")
# # # #                 apt_time = datetime.fromisoformat(apt_time_str.replace('Z', '+00:00'))
# # # #             except Exception:
# # # #                 continue

# # # #             last_reminder = appointment.get("lastReminderSent")

# # # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last_reminder:
# # # #                 await send_appointment_reminder(
# # # #                     appointment,
# # # #                     doc.id,
# # # #                     hours_until=24,
# # # #                     background_tasks=background_tasks
# # # #                 )
# # # #                 reminders_sent += 1

# # # #             if now <= apt_time <= in_1h and last_reminder != "1h":
# # # #                 await send_appointment_reminder(
# # # #                     appointment,
# # # #                     doc.id,
# # # #                     hours_until=1,
# # # #                     background_tasks=background_tasks
# # # #                 )
# # # #                 reminders_sent += 1

# # # #         return {
# # # #             "success": True,
# # # #             "reminders_sent": reminders_sent,
# # # #             "checked_at": now.isoformat()
# # # #         }

# # # #     except Exception as e:
# # # #         print(f"❌ Error in check_reminders: {e}")
# # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # async def send_appointment_reminder(
# # # #     appointment: Dict[str, Any],
# # # #     appointment_id: str,
# # # #     hours_until: int,
# # # #     background_tasks: BackgroundTasks
# # # # ):
# # # #     """Helper function to send appointment reminders"""
# # # #     patient_id = appointment.get("patientId")
# # # #     doctor_id = appointment.get("doctorId")
# # # #     appointment_time_str = appointment.get("appointmentDateTime")

# # # #     patient_data = await get_user_data(patient_id)
# # # #     doctor_data = await get_user_data(doctor_id)

# # # #     if not patient_data or not doctor_data:
# # # #         return

# # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # #     appointment_time = format_datetime(appointment_time_str)

# # # #     title = f"⏰ Appointment in {hours_until}h"
# # # #     patient_body = f"Reminder: Appointment with Dr. {doctor_name} at {appointment_time}"
# # # #     doctor_body = f"Reminder: Appointment with {patient_name} at {appointment_time}"

# # # #     if patient_fcm := patient_data.get("fcmToken"):
# # # #         background_tasks.add_task(
# # # #             send_fcm_notification,
# # # #             patient_fcm,
# # # #             title,
# # # #             patient_body,
# # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # #         )

# # # #     if patient_email := patient_data.get("email"):
# # # #         background_tasks.add_task(
# # # #             send_email,
# # # #             patient_email,
# # # #             patient_name,
# # # #             f"Appointment Reminder - {hours_until}h",
# # # #             reminder_email(patient_name, doctor_name, appointment_time, hours_until)
# # # #         )

# # # #     if doctor_fcm := doctor_data.get("fcmToken"):
# # # #         background_tasks.add_task(
# # # #             send_fcm_notification,
# # # #             doctor_fcm,
# # # #             title,
# # # #             doctor_body,
# # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # #         )

# # # #     if doctor_email := doctor_data.get("email"):
# # # #         background_tasks.add_task(
# # # #             send_email,
# # # #             doctor_email,
# # # #             doctor_name,
# # # #             f"Appointment Reminder - {hours_until}h",
# # # #             reminder_email(doctor_name, patient_name, appointment_time, hours_until)
# # # #         )

# # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # #     db.collection("appointments").document(appointment_id).update({
# # # #         "lastReminderSent": reminder_key
# # # #     })

# # # #     print(f"✅ Reminder sent for appointment {appointment_id} ({hours_until}h)")


# # # # if __name__ == "__main__":
# # # #     import uvicorn
# # # #     uvicorn.run(app, host="0.0.0.0", port=8000)




# # # # # """
# # # # # TeleMed FastAPI Backend
# # # # # Handles notifications, emails, scheduled reminders, AND file uploads
# # # # # """

# # # # # import os
# # # # # import mimetypes
# # # # # import smtplib
# # # # # from email.mime.text import MIMEText
# # # # # from email.mime.multipart import MIMEMultipart
# # # # # from datetime import datetime, timedelta, timezone
# # # # # from typing import Optional, Dict, Any
# # # # # from dotenv import load_dotenv

# # # # # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # # # # from fastapi.middleware.cors import CORSMiddleware
# # # # # from pydantic import BaseModel

# # # # # import firebase_admin
# # # # # from firebase_admin import credentials, firestore, messaging

# # # # # import cloudinary
# # # # # import cloudinary.uploader
# # # # # import cloudinary.api

# # # # # load_dotenv()

# # # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # cloudinary.config(
# # # # #     cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
# # # # #     api_key=os.getenv("CLOUDINARY_API_KEY"),
# # # # #     api_secret=os.getenv("CLOUDINARY_API_SECRET"),
# # # # #     secure=True
# # # # # )

# # # # # app = FastAPI(
# # # # #     title="TeleMed Backend",
# # # # #     description="Notification, email, and file upload service for TeleMed app",
# # # # #     version="2.0.0"
# # # # # )

# # # # # app.add_middleware(
# # # # #     CORSMiddleware,
# # # # #     allow_origins=["*"],
# # # # #     allow_credentials=True,
# # # # #     allow_methods=["*"],
# # # # #     allow_headers=["*"],
# # # # # )

# # # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # # firebase_admin.initialize_app(cred)
# # # # # db = firestore.client()


# # # # # # ============================================================================
# # # # # # PYDANTIC MODELS
# # # # # # ============================================================================

# # # # # class BookingConfirmedRequest(BaseModel):
# # # # #     appointment_id: str
# # # # #     patient_id: str
# # # # #     doctor_id: str
# # # # #     appointment_datetime: str
# # # # #     duration_minutes: int


# # # # # class AppointmentCanceledRequest(BaseModel):
# # # # #     appointment_id: str
# # # # #     patient_id: str
# # # # #     doctor_id: str
# # # # #     canceled_by: str
# # # # #     appointment_datetime: str


# # # # # class FileUploadResponse(BaseModel):
# # # # #     success: bool
# # # # #     url: str
# # # # #     public_id: str
# # # # #     format: str
# # # # #     size_bytes: int
# # # # #     message: str


# # # # # # ============================================================================
# # # # # # HELPERS
# # # # # # ============================================================================

# # # # # def resolve_content_type(file: UploadFile) -> str:
# # # # #     if file.content_type and file.content_type != "application/octet-stream":
# # # # #         return file.content_type
# # # # #     if file.filename:
# # # # #         guessed, _ = mimetypes.guess_type(file.filename)
# # # # #         if guessed:
# # # # #             print(f"🔍 Guessed MIME from filename '{file.filename}': {guessed}")
# # # # #             return guessed
# # # # #     print("⚠️ Defaulting MIME type to image/jpeg")
# # # # #     return "image/jpeg"


# # # # # ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "application/pdf"}

# # # # # FOLDER_MAP = {
# # # # #     "education_certificate": "doctors/certificates",
# # # # #     "authorization_file": "doctors/authorizations",
# # # # #     "affiliate_hospital": "doctors/hospitals",
# # # # #     "id_card": "doctors/ids",
# # # # #     "profile_photo": "doctors/photos",
# # # # # }


# # # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # # #     try:
# # # # #         doc = db.collection("users").document(uid).get()
# # # # #         return doc.to_dict() if doc.exists else None
# # # # #     except Exception as e:
# # # # #         print(f"❌ Error fetching user {uid}: {e}")
# # # # #         return None


# # # # # async def send_fcm_notification(fcm_token: str, title: str, body: str, data: Optional[Dict[str, str]] = None):
# # # # #     if not fcm_token:
# # # # #         return
# # # # #     try:
# # # # #         msg = messaging.Message(
# # # # #             notification=messaging.Notification(title=title, body=body),
# # # # #             data=data or {},
# # # # #             token=fcm_token,
# # # # #         )
# # # # #         messaging.send(msg)
# # # # #         print(f"✅ FCM sent to {fcm_token[:20]}...")
# # # # #     except Exception as e:
# # # # #         print(f"❌ FCM failed: {e}")


# # # # # async def send_email(to_email: str, to_name: str, subject: str, html_content: str):
# # # # #     try:
# # # # #         msg = MIMEMultipart('alternative')
# # # # #         msg['Subject'] = subject
# # # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # # #         msg['To'] = to_email
# # # # #         msg.attach(MIMEText(html_content, 'html'))
# # # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # # #             server.starttls()
# # # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # # #             server.send_message(msg)
# # # # #         print(f"✅ Email sent to {to_email}")
# # # # #     except Exception as e:
# # # # #         print(f"❌ Email failed: {e}")


# # # # # def format_datetime(iso_string: str) -> str:
# # # # #     try:
# # # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # # #     except:
# # # # #         return iso_string


# # # # # # ============================================================================
# # # # # # CLOUDINARY UPLOAD
# # # # # # The root cause of "Invalid Signature" was passing `folder` as a separate
# # # # # # parameter alongside `public_id`. Cloudinary signs `folder` and `public_id`
# # # # # # independently, but the SDK generates a signature that doesn't always match
# # # # # # this split. The fix: embed the folder directly into `public_id` as a path
# # # # # # prefix and do NOT pass `folder` at all. Cloudinary will parse the slashes
# # # # # # in public_id as the folder structure automatically.
# # # # # # ============================================================================

# # # # # async def upload_to_cloudinary(
# # # # #     file: UploadFile,
# # # # #     user_id: str,
# # # # #     file_type: str,
# # # # #     resolved_content_type: str
# # # # # ) -> Dict[str, Any]:
# # # # #     try:
# # # # #         contents = await file.read()
# # # # #         folder = FOLDER_MAP.get(file_type, "doctors/documents")
# # # # #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# # # # #         # ✅ Embed folder directly into public_id — do NOT pass folder separately.
# # # # #         # When folder is a separate param, Cloudinary signs it independently and
# # # # #         # the signature never matches. With the full path in public_id only,
# # # # #         # the signed string is simply: public_id=<full_path>&timestamp=<ts>
# # # # #         full_public_id = f"{folder}/{user_id}_{file_type}_{timestamp}"

# # # # #         resource_type = "image" if resolved_content_type.startswith("image") else "raw"

# # # # #         upload_result = cloudinary.uploader.upload(
# # # # #             contents,
# # # # #             public_id=full_public_id,   # e.g. "doctors/certificates/uid_education_certificate_20260227"
# # # # #             resource_type=resource_type,
# # # # #             # NO folder param — folder is encoded in public_id above
# # # # #             # NO quality/fetch_format — those cause transformation signature issues
# # # # #         )

# # # # #         print(f"✅ Cloudinary upload OK: {upload_result['secure_url']}")

# # # # #         return {
# # # # #             "success": True,
# # # # #             "url": upload_result['secure_url'],
# # # # #             "public_id": upload_result['public_id'],
# # # # #             "format": upload_result.get('format', ''),
# # # # #             "size_bytes": upload_result['bytes'],
# # # # #         }

# # # # #     except Exception as e:
# # # # #         print(f"❌ Cloudinary upload failed: {e}")
# # # # #         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# # # # # # ============================================================================
# # # # # # EMAIL TEMPLATES
# # # # # # ============================================================================

# # # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # # #     return f"""<!DOCTYPE html><html><head><style>
# # # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # # #         .header{{background:#4A90E2;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #4A90E2}}
# # # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # # #     </style></head><body><div class="container">
# # # # #         <div class="header"><h1>✅ Appointment Confirmed</h1></div>
# # # # #         <div class="content">
# # # # #             <p>Hi {patient_name},</p>
# # # # #             <p>Your telemedicine appointment has been confirmed.</p>
# # # # #             <div class="info-box">
# # # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # # # #             </div>
# # # # #             <p>Please be ready a few minutes before the scheduled time.</p>
# # # # #         </div>
# # # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # # #     </div></body></html>"""


# # # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # # #     return f"""<!DOCTYPE html><html><head><style>
# # # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # # #         .header{{background:#E74C3C;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #E74C3C}}
# # # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # # #     </style></head><body><div class="container">
# # # # #         <div class="header"><h1>❌ Appointment Canceled</h1></div>
# # # # #         <div class="content">
# # # # #             <p>Hi {name},</p>
# # # # #             <p>Your appointment was canceled by the {canceled_by}.</p>
# # # # #             <div class="info-box">
# # # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # # #                 <p><strong>Original Date &amp; Time:</strong> {appointment_time}</p>
# # # # #             </div>
# # # # #             <p>You can rebook anytime through the app.</p>
# # # # #         </div>
# # # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # # #     </div></body></html>"""


# # # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # # #     return f"""<!DOCTYPE html><html><head><style>
# # # # #         body{{font-family:Arial,sans-serif;line-height:1.6;color:#333}}
# # # # #         .container{{max-width:600px;margin:0 auto;padding:20px}}
# # # # #         .header{{background:#F39C12;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0}}
# # # # #         .content{{background:#f9f9f9;padding:30px;border-radius:0 0 8px 8px}}
# # # # #         .info-box{{background:white;padding:15px;margin:20px 0;border-left:4px solid #F39C12}}
# # # # #         .badge{{background:#F39C12;color:white;padding:10px 20px;border-radius:20px;display:inline-block;margin:20px 0;font-weight:bold}}
# # # # #         .footer{{text-align:center;padding:20px;color:#666;font-size:12px}}
# # # # #     </style></head><body><div class="container">
# # # # #         <div class="header"><h1>⏰ Appointment Reminder</h1></div>
# # # # #         <div class="content">
# # # # #             <p>Hi {name},</p>
# # # # #             <div style="text-align:center"><span class="badge">In {hours_until} hour(s)</span></div>
# # # # #             <div class="info-box">
# # # # #                 <p><strong>Doctor:</strong> Dr. {doctor_name}</p>
# # # # #                 <p><strong>Date &amp; Time:</strong> {appointment_time}</p>
# # # # #             </div>
# # # # #         </div>
# # # # #         <div class="footer"><p>TeleMed - Your Health, Our Priority</p></div>
# # # # #     </div></body></html>"""


# # # # # # ============================================================================
# # # # # # ENDPOINTS
# # # # # # ============================================================================

# # # # # @app.api_route("/", methods=["GET", "HEAD"])
# # # # # async def root():
# # # # #     return {
# # # # #         "status": "healthy",
# # # # #         "service": "TeleMed Backend",
# # # # #         "version": "2.0.0",
# # # # #         "file_storage": "cloudinary",
# # # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # # #     }


# # # # # @app.post("/upload-document", response_model=FileUploadResponse)
# # # # # async def upload_document(
# # # # #     file: UploadFile = File(...),
# # # # #     user_id: str = Form(...),
# # # # #     file_type: str = Form(...),
# # # # # ):
# # # # #     try:
# # # # #         content_type = resolve_content_type(file)
# # # # #         print(f"📎 Resolved content type: {content_type} (original: {file.content_type})")

# # # # #         if content_type not in ALLOWED_TYPES:
# # # # #             raise HTTPException(
# # # # #                 status_code=400,
# # # # #                 detail=f"File type '{content_type}' not allowed. Use JPG, PNG, WEBP, or PDF."
# # # # #             )

# # # # #         file.file.seek(0, 2)
# # # # #         file_size = file.file.tell()
# # # # #         file.file.seek(0)

# # # # #         if file_size > 10 * 1024 * 1024:
# # # # #             raise HTTPException(
# # # # #                 status_code=400,
# # # # #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Maximum is 10MB."
# # # # #             )

# # # # #         print(f"📤 Uploading {file_type} for user {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")

# # # # #         result = await upload_to_cloudinary(file, user_id, file_type, content_type)

# # # # #         return FileUploadResponse(
# # # # #             success=result['success'],
# # # # #             url=result['url'],
# # # # #             public_id=result['public_id'],
# # # # #             format=result['format'],
# # # # #             size_bytes=result['size_bytes'],
# # # # #             message="File uploaded successfully"
# # # # #         )

# # # # #     except HTTPException:
# # # # #         raise
# # # # #     except Exception as e:
# # # # #         print(f"❌ Upload endpoint error: {e}")
# # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # @app.post("/booking-confirmed")
# # # # # async def booking_confirmed(request: BookingConfirmedRequest, background_tasks: BackgroundTasks):
# # # # #     try:
# # # # #         patient_data = await get_user_data(request.patient_id)
# # # # #         doctor_data = await get_user_data(request.doctor_id)
# # # # #         if not patient_data or not doctor_data:
# # # # #             raise HTTPException(status_code=404, detail="User not found")

# # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # #         apt_time = format_datetime(request.appointment_datetime)

# # # # #         if fcm := patient_data.get("fcmToken"):
# # # # #             background_tasks.add_task(send_fcm_notification, fcm, "Appointment Confirmed ✅",
# # # # #                 f"Your appointment with Dr. {doctor_name} is confirmed for {apt_time}",
# # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # # # #         if fcm := doctor_data.get("fcmToken"):
# # # # #             background_tasks.add_task(send_fcm_notification, fcm, "New Appointment 📅",
# # # # #                 f"New appointment with {patient_name} for {apt_time}",
# # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id})

# # # # #         if email := patient_data.get("email"):
# # # # #             background_tasks.add_task(send_email, email, patient_name, "Appointment Confirmed",
# # # # #                 booking_confirmed_email(patient_name, doctor_name, apt_time))

# # # # #         if email := doctor_data.get("email"):
# # # # #             background_tasks.add_task(send_email, email, doctor_name, "New Appointment Scheduled",
# # # # #                 booking_confirmed_email(doctor_name, patient_name, apt_time))

# # # # #         return {"success": True, "message": "Notifications sent"}

# # # # #     except Exception as e:
# # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # @app.post("/appointment-canceled")
# # # # # async def appointment_canceled(request: AppointmentCanceledRequest, background_tasks: BackgroundTasks):
# # # # #     try:
# # # # #         patient_data = await get_user_data(request.patient_id)
# # # # #         doctor_data = await get_user_data(request.doctor_id)
# # # # #         if not patient_data or not doctor_data:
# # # # #             raise HTTPException(status_code=404, detail="User not found")

# # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # #         apt_time = format_datetime(request.appointment_datetime)

# # # # #         if fcm := patient_data.get("fcmToken"):
# # # # #             background_tasks.add_task(send_fcm_notification, fcm, "Appointment Canceled ❌",
# # # # #                 f"Your appointment with Dr. {doctor_name} on {apt_time} was canceled",
# # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # # # #         if fcm := doctor_data.get("fcmToken"):
# # # # #             background_tasks.add_task(send_fcm_notification, fcm, "Appointment Canceled ❌",
# # # # #                 f"Appointment with {patient_name} on {apt_time} was canceled",
# # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id})

# # # # #         if email := patient_data.get("email"):
# # # # #             background_tasks.add_task(send_email, email, patient_name, "Appointment Canceled",
# # # # #                 appointment_canceled_email(patient_name, doctor_name, apt_time, request.canceled_by))

# # # # #         if email := doctor_data.get("email"):
# # # # #             background_tasks.add_task(send_email, email, doctor_name, "Appointment Canceled",
# # # # #                 appointment_canceled_email(doctor_name, patient_name, apt_time, request.canceled_by))

# # # # #         return {"success": True, "message": "Cancellation notifications sent"}

# # # # #     except Exception as e:
# # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # @app.get("/check-reminders")
# # # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # # #     try:
# # # # #         now = datetime.now(timezone.utc)
# # # # #         in_24h = now + timedelta(hours=24)
# # # # #         in_1h = now + timedelta(hours=1)

# # # # #         upcoming = (
# # # # #             db.collection("appointments")
# # # # #             .where("status", "==", "confirmed")
# # # # #             .where("appointmentDateTime", ">=", now.isoformat())
# # # # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # # # #             .stream()
# # # # #         )

# # # # #         reminders_sent = 0
# # # # #         for doc in upcoming:
# # # # #             appointment = doc.to_dict()
# # # # #             try:
# # # # #                 apt_time = datetime.fromisoformat(
# # # # #                     appointment.get("appointmentDateTime").replace('Z', '+00:00'))
# # # # #             except Exception:
# # # # #                 continue

# # # # #             last = appointment.get("lastReminderSent")
# # # # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last:
# # # # #                 await send_appointment_reminder(appointment, doc.id, 24, background_tasks)
# # # # #                 reminders_sent += 1
# # # # #             if now <= apt_time <= in_1h and last != "1h":
# # # # #                 await send_appointment_reminder(appointment, doc.id, 1, background_tasks)
# # # # #                 reminders_sent += 1

# # # # #         return {"success": True, "reminders_sent": reminders_sent, "checked_at": now.isoformat()}

# # # # #     except Exception as e:
# # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # async def send_appointment_reminder(appointment, appointment_id, hours_until, background_tasks):
# # # # #     patient_data = await get_user_data(appointment.get("patientId"))
# # # # #     doctor_data = await get_user_data(appointment.get("doctorId"))
# # # # #     if not patient_data or not doctor_data:
# # # # #         return

# # # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # #     apt_time = format_datetime(appointment.get("appointmentDateTime"))
# # # # #     title = f"⏰ Appointment in {hours_until}h"

# # # # #     if fcm := patient_data.get("fcmToken"):
# # # # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # # # #             f"Reminder: Appointment with Dr. {doctor_name} at {apt_time}",
# # # # #             {"type": "reminder", "appointment_id": appointment_id})
# # # # #     if email := patient_data.get("email"):
# # # # #         background_tasks.add_task(send_email, email, patient_name,
# # # # #             f"Appointment Reminder - {hours_until}h",
# # # # #             reminder_email(patient_name, doctor_name, apt_time, hours_until))
# # # # #     if fcm := doctor_data.get("fcmToken"):
# # # # #         background_tasks.add_task(send_fcm_notification, fcm, title,
# # # # #             f"Reminder: Appointment with {patient_name} at {apt_time}",
# # # # #             {"type": "reminder", "appointment_id": appointment_id})
# # # # #     if email := doctor_data.get("email"):
# # # # #         background_tasks.add_task(send_email, email, doctor_name,
# # # # #             f"Appointment Reminder - {hours_until}h",
# # # # #             reminder_email(doctor_name, patient_name, apt_time, hours_until))

# # # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # # #     db.collection("appointments").document(appointment_id).update({"lastReminderSent": reminder_key})
# # # # #     print(f"✅ Reminder sent for {appointment_id} ({hours_until}h)")


# # # # # if __name__ == "__main__":
# # # # #     import uvicorn
# # # # #     uvicorn.run(app, host="0.0.0.0", port=8000)







# # # # # # """
# # # # # # TeleMed FastAPI Backend
# # # # # # Handles notifications, emails, scheduled reminders, AND file uploads
# # # # # # """

# # # # # # import os
# # # # # # import smtplib
# # # # # # from email.mime.text import MIMEText
# # # # # # from email.mime.multipart import MIMEMultipart
# # # # # # from datetime import datetime, timedelta, timezone
# # # # # # from typing import Optional, List, Dict, Any
# # # # # # from dotenv import load_dotenv

# # # # # # from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form
# # # # # # from fastapi.middleware.cors import CORSMiddleware
# # # # # # from pydantic import BaseModel, EmailStr

# # # # # # import firebase_admin
# # # # # # from firebase_admin import credentials, firestore, messaging

# # # # # # # Cloudinary for file uploads
# # # # # # import cloudinary
# # # # # # import cloudinary.uploader
# # # # # # import cloudinary.api

# # # # # # # Load environment variables FIRST
# # # # # # load_dotenv()

# # # # # # # Email configuration
# # # # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # # # Cloudinary configuration
# # # # # # cloudinary.config(
# # # # # #     cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
# # # # # #     api_key=os.getenv("CLOUDINARY_API_KEY"),
# # # # # #     api_secret=os.getenv("CLOUDINARY_API_SECRET"),
# # # # # #     secure=True
# # # # # # )

# # # # # # # Initialize FastAPI
# # # # # # app = FastAPI(
# # # # # #     title="TeleMed Backend",
# # # # # #     description="Notification, email, and file upload service for TeleMed app",
# # # # # #     version="2.0.0"
# # # # # # )

# # # # # # # CORS - Allow your Flutter app to call this API
# # # # # # app.add_middleware(
# # # # # #     CORSMiddleware,
# # # # # #     allow_origins=["*"],  # In production, replace with your actual domain
# # # # # #     allow_credentials=True,
# # # # # #     allow_methods=["*"],
# # # # # #     allow_headers=["*"],
# # # # # # )

# # # # # # # Initialize Firebase Admin SDK
# # # # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # # # firebase_admin.initialize_app(cred)
# # # # # # db = firestore.client()


# # # # # # # ============================================================================
# # # # # # # PYDANTIC MODELS
# # # # # # # ============================================================================

# # # # # # class BookingConfirmedRequest(BaseModel):
# # # # # #     appointment_id: str
# # # # # #     patient_id: str
# # # # # #     doctor_id: str
# # # # # #     appointment_datetime: str  # ISO format
# # # # # #     duration_minutes: int


# # # # # # class AppointmentCanceledRequest(BaseModel):
# # # # # #     appointment_id: str
# # # # # #     patient_id: str
# # # # # #     doctor_id: str
# # # # # #     canceled_by: str  # "patient" or "doctor"
# # # # # #     appointment_datetime: str


# # # # # # class FileUploadResponse(BaseModel):
# # # # # #     success: bool
# # # # # #     url: str
# # # # # #     public_id: str
# # # # # #     format: str
# # # # # #     size_bytes: int
# # # # # #     message: str


# # # # # # # ============================================================================
# # # # # # # HELPER FUNCTIONS
# # # # # # # ============================================================================

# # # # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # # # #     """Fetch user data from Firestore"""
# # # # # #     try:
# # # # # #         user_ref = db.collection("users").document(uid)
# # # # # #         user_doc = user_ref.get()
# # # # # #         return user_doc.to_dict() if user_doc.exists else None
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Error fetching user data for {uid}: {e}")
# # # # # #         return None


# # # # # # async def send_fcm_notification(
# # # # # #     fcm_token: str,
# # # # # #     title: str,
# # # # # #     body: str,
# # # # # #     data: Optional[Dict[str, str]] = None
# # # # # # ):
# # # # # #     """Send FCM push notification"""
# # # # # #     if not fcm_token:
# # # # # #         print("⚠️ No FCM token provided")
# # # # # #         return
    
# # # # # #     try:
# # # # # #         message = messaging.Message(
# # # # # #             notification=messaging.Notification(title=title, body=body),
# # # # # #             data=data or {},
# # # # # #             token=fcm_token,
# # # # # #         )
# # # # # #         response = messaging.send(message)
# # # # # #         print(f"✅ FCM sent: {response}")
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ FCM failed: {e}")


# # # # # # async def send_email(
# # # # # #     to_email: str,
# # # # # #     to_name: str,
# # # # # #     subject: str,
# # # # # #     html_content: str
# # # # # # ):
# # # # # #     """Send email via Gmail SMTP"""
# # # # # #     try:
# # # # # #         msg = MIMEMultipart('alternative')
# # # # # #         msg['Subject'] = subject
# # # # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # # # #         msg['To'] = to_email
        
# # # # # #         html_part = MIMEText(html_content, 'html')
# # # # # #         msg.attach(html_part)
        
# # # # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # # # #             server.starttls()
# # # # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # # # #             server.send_message(msg)
        
# # # # # #         print(f"✅ Email sent via Gmail to {to_email}")
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Email failed for {to_email}: {e}")


# # # # # # def format_datetime(iso_string: str) -> str:
# # # # # #     """Format ISO datetime to readable format"""
# # # # # #     try:
# # # # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # # # #     except:
# # # # # #         return iso_string


# # # # # # # ============================================================================
# # # # # # # FILE UPLOAD FUNCTIONS
# # # # # # # ============================================================================

# # # # # # def get_file_category(file_type: str) -> str:
# # # # # #     """Determine Cloudinary folder based on file type"""
# # # # # #     categories = {
# # # # # #         "education_certificate": "doctors/certificates",
# # # # # #         "authorization_file": "doctors/authorizations",
# # # # # #         "affiliate_hospital": "doctors/hospitals",
# # # # # #         "id_card": "doctors/ids",
# # # # # #         "profile_photo": "doctors/photos",
# # # # # #     }
# # # # # #     return categories.get(file_type, "doctors/documents")


# # # # # # async def upload_to_cloudinary(
# # # # # #     file: UploadFile,
# # # # # #     user_id: str,
# # # # # #     file_type: str
# # # # # # ) -> Dict[str, Any]:
# # # # # #     """
# # # # # #     Upload file to Cloudinary
# # # # # #     Returns URL and metadata
# # # # # #     """
# # # # # #     try:
# # # # # #         # Read file contents
# # # # # #         contents = await file.read()
        
# # # # # #         # Determine folder
# # # # # #         folder = get_file_category(file_type)
        
# # # # # #         # Generate unique public_id
# # # # # #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# # # # # #         public_id = f"{folder}/{user_id}_{file_type}_{timestamp}"
        
# # # # # #         # Upload to Cloudinary
# # # # # #         # For images: auto-optimize
# # # # # #         # For PDFs: store as-is
# # # # # #         resource_type = "image" if file.content_type.startswith("image") else "raw"
        
# # # # # #         upload_result = cloudinary.uploader.upload(
# # # # # #             contents,
# # # # # #             public_id=public_id,
# # # # # #             resource_type=resource_type,
# # # # # #             folder=folder,
# # # # # #             # Optimization for images
# # # # # #             quality="auto" if resource_type == "image" else None,
# # # # # #             fetch_format="auto" if resource_type == "image" else None,
# # # # # #         )
        
# # # # # #         print(f"✅ Uploaded to Cloudinary: {upload_result['secure_url']}")
        
# # # # # #         return {
# # # # # #             "success": True,
# # # # # #             "url": upload_result['secure_url'],
# # # # # #             "public_id": upload_result['public_id'],
# # # # # #             "format": upload_result['format'],
# # # # # #             "size_bytes": upload_result['bytes'],
# # # # # #             "resource_type": resource_type,
# # # # # #         }
        
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Cloudinary upload failed: {e}")
# # # # # #         raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# # # # # # # ============================================================================
# # # # # # # EMAIL TEMPLATES
# # # # # # # ============================================================================

# # # # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # # # #     return f"""
# # # # # #     <!DOCTYPE html>
# # # # # #     <html>
# # # # # #     <head>
# # # # # #         <style>
# # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # #             .header {{ background: #4A90E2; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #4A90E2; }}
# # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # #         </style>
# # # # # #     </head>
# # # # # #     <body>
# # # # # #         <div class="container">
# # # # # #             <div class="header">
# # # # # #                 <h1>✅ Appointment Confirmed</h1>
# # # # # #             </div>
# # # # # #             <div class="content">
# # # # # #                 <p>Hi {patient_name},</p>
# # # # # #                 <p>Great news! Your telemedicine appointment has been confirmed.</p>
                
# # # # # #                 <div class="info-box">
# # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # # # #                 </div>
                
# # # # # #                 <p>You will receive reminder notifications before your appointment.</p>
# # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # #             </div>
# # # # # #             <div class="footer">
# # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # #             </div>
# # # # # #         </div>
# # # # # #     </body>
# # # # # #     </html>
# # # # # #     """


# # # # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # # # #     return f"""
# # # # # #     <!DOCTYPE html>
# # # # # #     <html>
# # # # # #     <head>
# # # # # #         <style>
# # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # #             .header {{ background: #E74C3C; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #E74C3C; }}
# # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # #         </style>
# # # # # #     </head>
# # # # # #     <body>
# # # # # #         <div class="container">
# # # # # #             <div class="header">
# # # # # #                 <h1>❌ Appointment Canceled</h1>
# # # # # #             </div>
# # # # # #             <div class="content">
# # # # # #                 <p>Hi {name},</p>
# # # # # #                 <p>We're writing to inform you that the following appointment has been canceled by the {canceled_by}.</p>
                
# # # # # #                 <div class="info-box">
# # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # #                     <p><strong>📅 Original Date &amp; Time:</strong> {appointment_time}</p>
# # # # # #                 </div>
                
# # # # # #                 <p>You can book a new appointment anytime through the app.</p>
# # # # # #             </div>
# # # # # #             <div class="footer">
# # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # #             </div>
# # # # # #         </div>
# # # # # #     </body>
# # # # # #     </html>
# # # # # #     """


# # # # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # # # #     return f"""
# # # # # #     <!DOCTYPE html>
# # # # # #     <html>
# # # # # #     <head>
# # # # # #         <style>
# # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # #             .header {{ background: #F39C12; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #F39C12; }}
# # # # # #             .reminder-badge {{ background: #F39C12; color: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 20px 0; font-weight: bold; }}
# # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # #         </style>
# # # # # #     </head>
# # # # # #     <body>
# # # # # #         <div class="container">
# # # # # #             <div class="header">
# # # # # #                 <h1>⏰ Appointment Reminder</h1>
# # # # # #             </div>
# # # # # #             <div class="content">
# # # # # #                 <p>Hi {name},</p>
# # # # # #                 <p>This is a friendly reminder about your upcoming appointment.</p>
                
# # # # # #                 <div style="text-align: center;">
# # # # # #                     <span class="reminder-badge">In {hours_until} hour(s)</span>
# # # # # #                 </div>
                
# # # # # #                 <div class="info-box">
# # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # # # #                 </div>
                
# # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # #             </div>
# # # # # #             <div class="footer">
# # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # #             </div>
# # # # # #         </div>
# # # # # #     </body>
# # # # # #     </html>
# # # # # #     """


# # # # # # # ============================================================================
# # # # # # # ENDPOINTS
# # # # # # # ============================================================================

# # # # # # @app.api_route("/", methods=["GET", "HEAD"])
# # # # # # async def root():
# # # # # #     """Health check endpoint"""
# # # # # #     return {
# # # # # #         "status": "healthy",
# # # # # #         "service": "TeleMed Backend",
# # # # # #         "version": "2.0.0",
# # # # # #         "email_provider": "gmail_smtp",
# # # # # #         "file_storage": "cloudinary",
# # # # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # # # #     }


# # # # # # @app.post("/upload-document", response_model=FileUploadResponse)
# # # # # # async def upload_document(
# # # # # #     file: UploadFile = File(...),
# # # # # #     user_id: str = Form(...),
# # # # # #     file_type: str = Form(...),  # e.g., "education_certificate", "id_card"
# # # # # # ):
# # # # # #     """
# # # # # #     Upload a document/image to Cloudinary
    
# # # # # #     Parameters:
# # # # # #     - file: The file to upload (image or PDF)
# # # # # #     - user_id: Firebase user ID
# # # # # #     - file_type: Type of document (education_certificate, id_card, etc.)
    
# # # # # #     Returns:
# # # # # #     - Secure URL to access the file
# # # # # #     - Public ID for potential deletion
# # # # # #     - File metadata
# # # # # #     """
# # # # # #     try:
# # # # # #         # Validate file type
# # # # # #         allowed_types = [
# # # # # #             "image/jpeg", "image/png", "image/jpg", "image/webp",
# # # # # #             "application/pdf"
# # # # # #         ]
        
# # # # # #         if file.content_type not in allowed_types:
# # # # # #             raise HTTPException(
# # # # # #                 status_code=400,
# # # # # #                 detail=f"File type {file.content_type} not allowed. Use JPG, PNG, WEBP, or PDF."
# # # # # #             )
        
# # # # # #         # Check file size (max 10MB)
# # # # # #         file.file.seek(0, 2)  # Seek to end
# # # # # #         file_size = file.file.tell()  # Get position (size)
# # # # # #         file.file.seek(0)  # Reset to start
        
# # # # # #         max_size = 10 * 1024 * 1024  # 10MB
# # # # # #         if file_size > max_size:
# # # # # #             raise HTTPException(
# # # # # #                 status_code=400,
# # # # # #                 detail=f"File too large ({file_size / 1024 / 1024:.1f}MB). Maximum is 10MB."
# # # # # #             )
        
# # # # # #         print(f"📤 Uploading {file_type} for user {user_id}: {file.filename} ({file_size / 1024:.1f}KB)")
        
# # # # # #         # Upload to Cloudinary
# # # # # #         result = await upload_to_cloudinary(file, user_id, file_type)
        
# # # # # #         return FileUploadResponse(
# # # # # #             success=result['success'],
# # # # # #             url=result['url'],
# # # # # #             public_id=result['public_id'],
# # # # # #             format=result['format'],
# # # # # #             size_bytes=result['size_bytes'],
# # # # # #             message="File uploaded successfully"
# # # # # #         )
        
# # # # # #     except HTTPException:
# # # # # #         raise
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Upload endpoint error: {e}")
# # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # @app.post("/booking-confirmed")
# # # # # # async def booking_confirmed(
# # # # # #     request: BookingConfirmedRequest,
# # # # # #     background_tasks: BackgroundTasks
# # # # # # ):
# # # # # #     """
# # # # # #     Called when a new appointment is booked
# # # # # #     Sends notifications to both patient and doctor
# # # # # #     """
# # # # # #     try:
# # # # # #         # Fetch patient and doctor data
# # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # #         if not patient_data or not doctor_data:
# # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # #         # Prepare notification content
# # # # # #         patient_title = "Appointment Confirmed ✅"
# # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} is confirmed for {appointment_time}"
        
# # # # # #         doctor_title = "New Appointment 📅"
# # # # # #         doctor_body = f"New appointment with {patient_name} scheduled for {appointment_time}"
        
# # # # # #         # Send FCM notifications in background
# # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_fcm_notification,
# # # # # #                 patient_fcm,
# # # # # #                 patient_title,
# # # # # #                 patient_body,
# # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # #             )
        
# # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_fcm_notification,
# # # # # #                 doctor_fcm,
# # # # # #                 doctor_title,
# # # # # #                 doctor_body,
# # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # #             )
        
# # # # # #         # Send emails in background
# # # # # #         if patient_email := patient_data.get("email"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_email,
# # # # # #                 patient_email,
# # # # # #                 patient_name,
# # # # # #                 "Appointment Confirmed",
# # # # # #                 booking_confirmed_email(patient_name, doctor_name, appointment_time)
# # # # # #             )
        
# # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_email,
# # # # # #                 doctor_email,
# # # # # #                 doctor_name,
# # # # # #                 "New Appointment Scheduled",
# # # # # #                 booking_confirmed_email(doctor_name, patient_name, appointment_time)
# # # # # #             )
        
# # # # # #         return {
# # # # # #             "success": True,
# # # # # #             "message": "Notifications sent successfully",
# # # # # #             "patient": patient_name,
# # # # # #             "doctor": doctor_name
# # # # # #         }
    
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Error in booking_confirmed: {e}")
# # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # @app.post("/appointment-canceled")
# # # # # # async def appointment_canceled(
# # # # # #     request: AppointmentCanceledRequest,
# # # # # #     background_tasks: BackgroundTasks
# # # # # # ):
# # # # # #     """
# # # # # #     Called when an appointment is canceled
# # # # # #     Sends notifications to both patient and doctor
# # # # # #     """
# # # # # #     try:
# # # # # #         # Fetch patient and doctor data
# # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # #         if not patient_data or not doctor_data:
# # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # #         # Prepare notification content
# # # # # #         title = "Appointment Canceled ❌"
# # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} on {appointment_time} has been canceled"
# # # # # #         doctor_body = f"Appointment with {patient_name} on {appointment_time} has been canceled"
        
# # # # # #         # Send FCM notifications
# # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_fcm_notification,
# # # # # #                 patient_fcm,
# # # # # #                 title,
# # # # # #                 patient_body,
# # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # #             )
        
# # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_fcm_notification,
# # # # # #                 doctor_fcm,
# # # # # #                 title,
# # # # # #                 doctor_body,
# # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # #             )
        
# # # # # #         # Send emails
# # # # # #         if patient_email := patient_data.get("email"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_email,
# # # # # #                 patient_email,
# # # # # #                 patient_name,
# # # # # #                 "Appointment Canceled",
# # # # # #                 appointment_canceled_email(patient_name, doctor_name, appointment_time, request.canceled_by)
# # # # # #             )
        
# # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # #             background_tasks.add_task(
# # # # # #                 send_email,
# # # # # #                 doctor_email,
# # # # # #                 doctor_name,
# # # # # #                 "Appointment Canceled",
# # # # # #                 appointment_canceled_email(doctor_name, patient_name, appointment_time, request.canceled_by)
# # # # # #             )
        
# # # # # #         return {
# # # # # #             "success": True,
# # # # # #             "message": "Cancellation notifications sent",
# # # # # #             "canceled_by": request.canceled_by
# # # # # #         }
    
# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Error in appointment_canceled: {e}")
# # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # @app.get("/check-reminders")
# # # # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # # # #     """
# # # # # #     Called by cron job every hour.
# # # # # #     Checks for appointments in next 24h and 1h windows.
# # # # # #     Sends reminder notifications.
# # # # # #     """
# # # # # #     try:
# # # # # #         now = datetime.now(timezone.utc)
# # # # # #         in_24h = now + timedelta(hours=24)
# # # # # #         in_1h = now + timedelta(hours=1)

# # # # # #         appointments_ref = db.collection("appointments")
# # # # # #         upcoming = (
# # # # # #             appointments_ref
# # # # # #             .where("status", "==", "confirmed")
# # # # # #             .where("appointmentDateTime", ">=", now.isoformat())
# # # # # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # # # # #             .stream()
# # # # # #         )

# # # # # #         reminders_sent = 0

# # # # # #         for doc in upcoming:
# # # # # #             appointment = doc.to_dict()

# # # # # #             try:
# # # # # #                 apt_time_str = appointment.get("appointmentDateTime")
# # # # # #                 apt_time = datetime.fromisoformat(apt_time_str.replace('Z', '+00:00'))
# # # # # #             except Exception:
# # # # # #                 continue

# # # # # #             last_reminder = appointment.get("lastReminderSent")

# # # # # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last_reminder:
# # # # # #                 await send_appointment_reminder(
# # # # # #                     appointment,
# # # # # #                     doc.id,
# # # # # #                     hours_until=24,
# # # # # #                     background_tasks=background_tasks
# # # # # #                 )
# # # # # #                 reminders_sent += 1

# # # # # #             if now <= apt_time <= in_1h and last_reminder != "1h":
# # # # # #                 await send_appointment_reminder(
# # # # # #                     appointment,
# # # # # #                     doc.id,
# # # # # #                     hours_until=1,
# # # # # #                     background_tasks=background_tasks
# # # # # #                 )
# # # # # #                 reminders_sent += 1

# # # # # #         return {
# # # # # #             "success": True,
# # # # # #             "reminders_sent": reminders_sent,
# # # # # #             "checked_at": now.isoformat()
# # # # # #         }

# # # # # #     except Exception as e:
# # # # # #         print(f"❌ Error in check_reminders: {e}")
# # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # async def send_appointment_reminder(
# # # # # #     appointment: Dict[str, Any],
# # # # # #     appointment_id: str,
# # # # # #     hours_until: int,
# # # # # #     background_tasks: BackgroundTasks
# # # # # # ):
# # # # # #     """Helper function to send appointment reminders"""
# # # # # #     patient_id = appointment.get("patientId")
# # # # # #     doctor_id = appointment.get("doctorId")
# # # # # #     appointment_time_str = appointment.get("appointmentDateTime")

# # # # # #     patient_data = await get_user_data(patient_id)
# # # # # #     doctor_data = await get_user_data(doctor_id)

# # # # # #     if not patient_data or not doctor_data:
# # # # # #         return

# # # # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # #     appointment_time = format_datetime(appointment_time_str)

# # # # # #     title = f"⏰ Appointment in {hours_until}h"
# # # # # #     patient_body = f"Reminder: Appointment with Dr. {doctor_name} at {appointment_time}"
# # # # # #     doctor_body = f"Reminder: Appointment with {patient_name} at {appointment_time}"

# # # # # #     if patient_fcm := patient_data.get("fcmToken"):
# # # # # #         background_tasks.add_task(
# # # # # #             send_fcm_notification,
# # # # # #             patient_fcm,
# # # # # #             title,
# # # # # #             patient_body,
# # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # #         )

# # # # # #     if patient_email := patient_data.get("email"):
# # # # # #         background_tasks.add_task(
# # # # # #             send_email,
# # # # # #             patient_email,
# # # # # #             patient_name,
# # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # #             reminder_email(patient_name, doctor_name, appointment_time, hours_until)
# # # # # #         )

# # # # # #     if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # #         background_tasks.add_task(
# # # # # #             send_fcm_notification,
# # # # # #             doctor_fcm,
# # # # # #             title,
# # # # # #             doctor_body,
# # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # #         )

# # # # # #     if doctor_email := doctor_data.get("email"):
# # # # # #         background_tasks.add_task(
# # # # # #             send_email,
# # # # # #             doctor_email,
# # # # # #             doctor_name,
# # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # #             reminder_email(doctor_name, patient_name, appointment_time, hours_until)
# # # # # #         )

# # # # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # # # #     db.collection("appointments").document(appointment_id).update({
# # # # # #         "lastReminderSent": reminder_key
# # # # # #     })

# # # # # #     print(f"✅ Reminder sent for appointment {appointment_id} ({hours_until}h)")


# # # # # # if __name__ == "__main__":
# # # # # #     import uvicorn
# # # # # #     uvicorn.run(app, host="0.0.0.0", port=8000)















# # # # # # # """
# # # # # # # TeleMed FastAPI Backend
# # # # # # # Handles notifications, emails, and scheduled reminders for Firebase app
# # # # # # # """

# # # # # # # import os
# # # # # # # import smtplib
# # # # # # # from email.mime.text import MIMEText
# # # # # # # from email.mime.multipart import MIMEMultipart
# # # # # # # from datetime import datetime, timedelta, timezone
# # # # # # # from typing import Optional, List, Dict, Any
# # # # # # # from dotenv import load_dotenv

# # # # # # # from fastapi import FastAPI, HTTPException, BackgroundTasks
# # # # # # # from fastapi.middleware.cors import CORSMiddleware
# # # # # # # from pydantic import BaseModel, EmailStr

# # # # # # # import firebase_admin
# # # # # # # from firebase_admin import credentials, firestore, messaging

# # # # # # # # Load environment variables FIRST
# # # # # # # load_dotenv()

# # # # # # # # Email configuration
# # # # # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # # # # Initialize FastAPI
# # # # # # # app = FastAPI(
# # # # # # #     title="TeleMed Backend",
# # # # # # #     description="Notification and email service for TeleMed app",
# # # # # # #     version="1.0.0"
# # # # # # # )

# # # # # # # # CORS - Allow your Flutter app to call this API
# # # # # # # app.add_middleware(
# # # # # # #     CORSMiddleware,
# # # # # # #     allow_origins=["*"],  # In production, replace with your actual domain
# # # # # # #     allow_credentials=True,
# # # # # # #     allow_methods=["*"],
# # # # # # #     allow_headers=["*"],
# # # # # # # )

# # # # # # # # Initialize Firebase Admin SDK
# # # # # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # # # # firebase_admin.initialize_app(cred)
# # # # # # # db = firestore.client()


# # # # # # # # ============================================================================
# # # # # # # # PYDANTIC MODELS
# # # # # # # # ============================================================================

# # # # # # # class BookingConfirmedRequest(BaseModel):
# # # # # # #     appointment_id: str
# # # # # # #     patient_id: str
# # # # # # #     doctor_id: str
# # # # # # #     appointment_datetime: str  # ISO format
# # # # # # #     duration_minutes: int


# # # # # # # class AppointmentCanceledRequest(BaseModel):
# # # # # # #     appointment_id: str
# # # # # # #     patient_id: str
# # # # # # #     doctor_id: str
# # # # # # #     canceled_by: str  # "patient" or "doctor"
# # # # # # #     appointment_datetime: str


# # # # # # # # ============================================================================
# # # # # # # # HELPER FUNCTIONS
# # # # # # # # ============================================================================

# # # # # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # # # # #     """Fetch user data from Firestore"""
# # # # # # #     try:
# # # # # # #         user_ref = db.collection("users").document(uid)
# # # # # # #         user_doc = user_ref.get()
# # # # # # #         return user_doc.to_dict() if user_doc.exists else None
# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ Error fetching user data for {uid}: {e}")
# # # # # # #         return None


# # # # # # # async def send_fcm_notification(
# # # # # # #     fcm_token: str,
# # # # # # #     title: str,
# # # # # # #     body: str,
# # # # # # #     data: Optional[Dict[str, str]] = None
# # # # # # # ):
# # # # # # #     """Send FCM push notification"""
# # # # # # #     if not fcm_token:
# # # # # # #         print("⚠️ No FCM token provided")
# # # # # # #         return
    
# # # # # # #     try:
# # # # # # #         message = messaging.Message(
# # # # # # #             notification=messaging.Notification(title=title, body=body),
# # # # # # #             data=data or {},
# # # # # # #             token=fcm_token,
# # # # # # #         )
# # # # # # #         response = messaging.send(message)
# # # # # # #         print(f"✅ FCM sent: {response}")
# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ FCM failed: {e}")


# # # # # # # async def send_email(
# # # # # # #     to_email: str,
# # # # # # #     to_name: str,
# # # # # # #     subject: str,
# # # # # # #     html_content: str
# # # # # # # ):
# # # # # # #     """Send email via Gmail SMTP"""
# # # # # # #     try:
# # # # # # #         msg = MIMEMultipart('alternative')
# # # # # # #         msg['Subject'] = subject
# # # # # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # # # # #         msg['To'] = to_email
        
# # # # # # #         html_part = MIMEText(html_content, 'html')
# # # # # # #         msg.attach(html_part)
        
# # # # # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # # # # #             server.starttls()
# # # # # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # # # # #             server.send_message(msg)
        
# # # # # # #         print(f"✅ Email sent via Gmail to {to_email}")
# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ Email failed for {to_email}: {e}")


# # # # # # # def format_datetime(iso_string: str) -> str:
# # # # # # #     """Format ISO datetime to readable format"""
# # # # # # #     try:
# # # # # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # # # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # # # # #     except:
# # # # # # #         return iso_string


# # # # # # # # ============================================================================
# # # # # # # # EMAIL TEMPLATES
# # # # # # # # ============================================================================

# # # # # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # # # # #     return f"""
# # # # # # #     <!DOCTYPE html>
# # # # # # #     <html>
# # # # # # #     <head>
# # # # # # #         <style>
# # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # #             .header {{ background: #4A90E2; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #4A90E2; }}
# # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # #         </style>
# # # # # # #     </head>
# # # # # # #     <body>
# # # # # # #         <div class="container">
# # # # # # #             <div class="header">
# # # # # # #                 <h1>✅ Appointment Confirmed</h1>
# # # # # # #             </div>
# # # # # # #             <div class="content">
# # # # # # #                 <p>Hi {patient_name},</p>
# # # # # # #                 <p>Great news! Your telemedicine appointment has been confirmed.</p>
                
# # # # # # #                 <div class="info-box">
# # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # # # # #                 </div>
                
# # # # # # #                 <p>You will receive reminder notifications before your appointment.</p>
# # # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # # #             </div>
# # # # # # #             <div class="footer">
# # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # #             </div>
# # # # # # #         </div>
# # # # # # #     </body>
# # # # # # #     </html>
# # # # # # #     """


# # # # # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # # # # #     return f"""
# # # # # # #     <!DOCTYPE html>
# # # # # # #     <html>
# # # # # # #     <head>
# # # # # # #         <style>
# # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # #             .header {{ background: #E74C3C; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #E74C3C; }}
# # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # #         </style>
# # # # # # #     </head>
# # # # # # #     <body>
# # # # # # #         <div class="container">
# # # # # # #             <div class="header">
# # # # # # #                 <h1>❌ Appointment Canceled</h1>
# # # # # # #             </div>
# # # # # # #             <div class="content">
# # # # # # #                 <p>Hi {name},</p>
# # # # # # #                 <p>We're writing to inform you that the following appointment has been canceled by the {canceled_by}.</p>
                
# # # # # # #                 <div class="info-box">
# # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # #                     <p><strong>📅 Original Date &amp; Time:</strong> {appointment_time}</p>
# # # # # # #                 </div>
                
# # # # # # #                 <p>You can book a new appointment anytime through the app.</p>
# # # # # # #             </div>
# # # # # # #             <div class="footer">
# # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # #             </div>
# # # # # # #         </div>
# # # # # # #     </body>
# # # # # # #     </html>
# # # # # # #     """


# # # # # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # # # # #     return f"""
# # # # # # #     <!DOCTYPE html>
# # # # # # #     <html>
# # # # # # #     <head>
# # # # # # #         <style>
# # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # #             .header {{ background: #F39C12; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #F39C12; }}
# # # # # # #             .reminder-badge {{ background: #F39C12; color: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 20px 0; font-weight: bold; }}
# # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # #         </style>
# # # # # # #     </head>
# # # # # # #     <body>
# # # # # # #         <div class="container">
# # # # # # #             <div class="header">
# # # # # # #                 <h1>⏰ Appointment Reminder</h1>
# # # # # # #             </div>
# # # # # # #             <div class="content">
# # # # # # #                 <p>Hi {name},</p>
# # # # # # #                 <p>This is a friendly reminder about your upcoming appointment.</p>
                
# # # # # # #                 <div style="text-align: center;">
# # # # # # #                     <span class="reminder-badge">In {hours_until} hour(s)</span>
# # # # # # #                 </div>
                
# # # # # # #                 <div class="info-box">
# # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # #                     <p><strong>📅 Date &amp; Time:</strong> {appointment_time}</p>
# # # # # # #                 </div>
                
# # # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # # #             </div>
# # # # # # #             <div class="footer">
# # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # #             </div>
# # # # # # #         </div>
# # # # # # #     </body>
# # # # # # #     </html>
# # # # # # #     """


# # # # # # # # ============================================================================
# # # # # # # # ENDPOINTS
# # # # # # # # ============================================================================

# # # # # # # # @app.get("/")
# # # # # # # # async def root():
# # # # # # # #     """Health check endpoint"""
# # # # # # # #     return {
# # # # # # # #         "status": "healthy",
# # # # # # # #         "service": "TeleMed Backend",
# # # # # # # #         "version": "1.0.0",
# # # # # # # #         "email_provider": "gmail_smtp",
# # # # # # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # # # # # #     }


# # # # # # # @app.api_route("/", methods=["GET", "HEAD"])
# # # # # # # async def root():
# # # # # # #     """Health check endpoint"""
# # # # # # #     return {
# # # # # # #         "status": "healthy",
# # # # # # #         "service": "TeleMed Backend",
# # # # # # #         "version": "1.0.0",
# # # # # # #         "email_provider": "gmail_smtp",
# # # # # # #         "timestamp": datetime.now(timezone.utc).isoformat()
# # # # # # #     }


# # # # # # # @app.post("/booking-confirmed")
# # # # # # # async def booking_confirmed(
# # # # # # #     request: BookingConfirmedRequest,
# # # # # # #     background_tasks: BackgroundTasks
# # # # # # # ):
# # # # # # #     """
# # # # # # #     Called when a new appointment is booked
# # # # # # #     Sends notifications to both patient and doctor
# # # # # # #     """
# # # # # # #     try:
# # # # # # #         # Fetch patient and doctor data
# # # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # # #         if not patient_data or not doctor_data:
# # # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # # #         # Prepare notification content
# # # # # # #         patient_title = "Appointment Confirmed ✅"
# # # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} is confirmed for {appointment_time}"
        
# # # # # # #         doctor_title = "New Appointment 📅"
# # # # # # #         doctor_body = f"New appointment with {patient_name} scheduled for {appointment_time}"
        
# # # # # # #         # Send FCM notifications in background
# # # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_fcm_notification,
# # # # # # #                 patient_fcm,
# # # # # # #                 patient_title,
# # # # # # #                 patient_body,
# # # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # # #             )
        
# # # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_fcm_notification,
# # # # # # #                 doctor_fcm,
# # # # # # #                 doctor_title,
# # # # # # #                 doctor_body,
# # # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # # #             )
        
# # # # # # #         # Send emails in background
# # # # # # #         if patient_email := patient_data.get("email"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_email,
# # # # # # #                 patient_email,
# # # # # # #                 patient_name,
# # # # # # #                 "Appointment Confirmed",
# # # # # # #                 booking_confirmed_email(patient_name, doctor_name, appointment_time)
# # # # # # #             )
        
# # # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_email,
# # # # # # #                 doctor_email,
# # # # # # #                 doctor_name,
# # # # # # #                 "New Appointment Scheduled",
# # # # # # #                 booking_confirmed_email(doctor_name, patient_name, appointment_time)
# # # # # # #             )
        
# # # # # # #         return {
# # # # # # #             "success": True,
# # # # # # #             "message": "Notifications sent successfully",
# # # # # # #             "patient": patient_name,
# # # # # # #             "doctor": doctor_name
# # # # # # #         }
    
# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ Error in booking_confirmed: {e}")
# # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # @app.post("/appointment-canceled")
# # # # # # # async def appointment_canceled(
# # # # # # #     request: AppointmentCanceledRequest,
# # # # # # #     background_tasks: BackgroundTasks
# # # # # # # ):
# # # # # # #     """
# # # # # # #     Called when an appointment is canceled
# # # # # # #     Sends notifications to both patient and doctor
# # # # # # #     """
# # # # # # #     try:
# # # # # # #         # Fetch patient and doctor data
# # # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # # #         if not patient_data or not doctor_data:
# # # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # # #         # Prepare notification content
# # # # # # #         title = "Appointment Canceled ❌"
# # # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} on {appointment_time} has been canceled"
# # # # # # #         doctor_body = f"Appointment with {patient_name} on {appointment_time} has been canceled"
        
# # # # # # #         # Send FCM notifications
# # # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_fcm_notification,
# # # # # # #                 patient_fcm,
# # # # # # #                 title,
# # # # # # #                 patient_body,
# # # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # # #             )
        
# # # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_fcm_notification,
# # # # # # #                 doctor_fcm,
# # # # # # #                 title,
# # # # # # #                 doctor_body,
# # # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # # #             )
        
# # # # # # #         # Send emails
# # # # # # #         if patient_email := patient_data.get("email"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_email,
# # # # # # #                 patient_email,
# # # # # # #                 patient_name,
# # # # # # #                 "Appointment Canceled",
# # # # # # #                 appointment_canceled_email(patient_name, doctor_name, appointment_time, request.canceled_by)
# # # # # # #             )
        
# # # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # # #             background_tasks.add_task(
# # # # # # #                 send_email,
# # # # # # #                 doctor_email,
# # # # # # #                 doctor_name,
# # # # # # #                 "Appointment Canceled",
# # # # # # #                 appointment_canceled_email(doctor_name, patient_name, appointment_time, request.canceled_by)
# # # # # # #             )
        
# # # # # # #         return {
# # # # # # #             "success": True,
# # # # # # #             "message": "Cancellation notifications sent",
# # # # # # #             "canceled_by": request.canceled_by
# # # # # # #         }
    
# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ Error in appointment_canceled: {e}")
# # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # @app.get("/check-reminders")
# # # # # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # # # # #     """
# # # # # # #     Called by cron job every hour.
# # # # # # #     Checks for appointments in next 24h and 1h windows.
# # # # # # #     Sends reminder notifications.

# # # # # # #     FIXES APPLIED:
# # # # # # #     1. Use timezone-aware datetime (UTC) to avoid TypeError when comparing
# # # # # # #        with timezone-aware Firestore timestamps.
# # # # # # #     2. Added Firestore date range filter to avoid fetching the entire
# # # # # # #        appointments collection (which caused "output too large" on Render).
# # # # # # #     3. Fixed 1-hour reminder logic: use separate `if` (not `elif`) so
# # # # # # #        appointments within the 1h window that already have a 24h reminder
# # # # # # #        sent can still receive the 1h reminder.
# # # # # # #     """
# # # # # # #     try:
# # # # # # #         # FIX 1: Use timezone-aware UTC datetime so comparisons with
# # # # # # #         # timezone-aware apt_time don't raise TypeError.
# # # # # # #         now = datetime.now(timezone.utc)

# # # # # # #         in_24h = now + timedelta(hours=24)
# # # # # # #         in_1h = now + timedelta(hours=1)

# # # # # # #         # FIX 2: Filter by date range directly in Firestore so we only fetch
# # # # # # #         # appointments actually due for a reminder — not the entire collection.
# # # # # # #         # This requires a composite index on (status, appointmentDateTime).
# # # # # # #         # Create it in Firebase Console or via: firebase deploy --only firestore:indexes
# # # # # # #         appointments_ref = db.collection("appointments")
# # # # # # #         upcoming = (
# # # # # # #             appointments_ref
# # # # # # #             .where("status", "==", "confirmed")
# # # # # # #             .where("appointmentDateTime", ">=", now.isoformat())
# # # # # # #             .where("appointmentDateTime", "<=", in_24h.isoformat())
# # # # # # #             .stream()
# # # # # # #         )

# # # # # # #         reminders_sent = 0

# # # # # # #         for doc in upcoming:
# # # # # # #             appointment = doc.to_dict()

# # # # # # #             # Parse appointment datetime
# # # # # # #             try:
# # # # # # #                 apt_time_str = appointment.get("appointmentDateTime")
# # # # # # #                 apt_time = datetime.fromisoformat(apt_time_str.replace('Z', '+00:00'))
# # # # # # #             except Exception:
# # # # # # #                 continue

# # # # # # #             # Check what reminder was already sent
# # # # # # #             last_reminder = appointment.get("lastReminderSent")

# # # # # # #             # FIX 3a: 24-hour reminder — only if no reminder sent yet
# # # # # # #             # and appointment is NOT yet within the 1h window.
# # # # # # #             if now <= apt_time <= in_24h and apt_time > in_1h and not last_reminder:
# # # # # # #                 await send_appointment_reminder(
# # # # # # #                     appointment,
# # # # # # #                     doc.id,
# # # # # # #                     hours_until=24,
# # # # # # #                     background_tasks=background_tasks
# # # # # # #                 )
# # # # # # #                 reminders_sent += 1

# # # # # # #             # FIX 3b: 1-hour reminder — use plain `if` (not elif) so this
# # # # # # #             # fires independently regardless of the 24h branch above.
# # # # # # #             # Only skip if the 1h reminder was already sent.
# # # # # # #             if now <= apt_time <= in_1h and last_reminder != "1h":
# # # # # # #                 await send_appointment_reminder(
# # # # # # #                     appointment,
# # # # # # #                     doc.id,
# # # # # # #                     hours_until=1,
# # # # # # #                     background_tasks=background_tasks
# # # # # # #                 )
# # # # # # #                 reminders_sent += 1

# # # # # # #         return {
# # # # # # #             "success": True,
# # # # # # #             "reminders_sent": reminders_sent,
# # # # # # #             "checked_at": now.isoformat()
# # # # # # #         }

# # # # # # #     except Exception as e:
# # # # # # #         print(f"❌ Error in check_reminders: {e}")
# # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # async def send_appointment_reminder(
# # # # # # #     appointment: Dict[str, Any],
# # # # # # #     appointment_id: str,
# # # # # # #     hours_until: int,
# # # # # # #     background_tasks: BackgroundTasks
# # # # # # # ):
# # # # # # #     """Helper function to send appointment reminders"""
# # # # # # #     patient_id = appointment.get("patientId")
# # # # # # #     doctor_id = appointment.get("doctorId")
# # # # # # #     appointment_time_str = appointment.get("appointmentDateTime")

# # # # # # #     # Fetch user data
# # # # # # #     patient_data = await get_user_data(patient_id)
# # # # # # #     doctor_data = await get_user_data(doctor_id)

# # # # # # #     if not patient_data or not doctor_data:
# # # # # # #         return

# # # # # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # #     appointment_time = format_datetime(appointment_time_str)

# # # # # # #     # Notification content
# # # # # # #     title = f"⏰ Appointment in {hours_until}h"
# # # # # # #     patient_body = f"Reminder: Appointment with Dr. {doctor_name} at {appointment_time}"
# # # # # # #     doctor_body = f"Reminder: Appointment with {patient_name} at {appointment_time}"

# # # # # # #     # Send to patient
# # # # # # #     if patient_fcm := patient_data.get("fcmToken"):
# # # # # # #         background_tasks.add_task(
# # # # # # #             send_fcm_notification,
# # # # # # #             patient_fcm,
# # # # # # #             title,
# # # # # # #             patient_body,
# # # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # # #         )

# # # # # # #     if patient_email := patient_data.get("email"):
# # # # # # #         background_tasks.add_task(
# # # # # # #             send_email,
# # # # # # #             patient_email,
# # # # # # #             patient_name,
# # # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # # #             reminder_email(patient_name, doctor_name, appointment_time, hours_until)
# # # # # # #         )

# # # # # # #     # Send to doctor
# # # # # # #     if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # #         background_tasks.add_task(
# # # # # # #             send_fcm_notification,
# # # # # # #             doctor_fcm,
# # # # # # #             title,
# # # # # # #             doctor_body,
# # # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # # #         )

# # # # # # #     if doctor_email := doctor_data.get("email"):
# # # # # # #         background_tasks.add_task(
# # # # # # #             send_email,
# # # # # # #             doctor_email,
# # # # # # #             doctor_name,
# # # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # # #             reminder_email(doctor_name, patient_name, appointment_time, hours_until)
# # # # # # #         )

# # # # # # #     # Update Firestore to mark reminder as sent
# # # # # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # # # # #     db.collection("appointments").document(appointment_id).update({
# # # # # # #         "lastReminderSent": reminder_key
# # # # # # #     })

# # # # # # #     print(f"✅ Reminder sent for appointment {appointment_id} ({hours_until}h)")


# # # # # # # if __name__ == "__main__":
# # # # # # #     import uvicorn
# # # # # # #     uvicorn.run(app, host="0.0.0.0", port=8000)





# # # # # # # # """
# # # # # # # # TeleMed FastAPI Backend
# # # # # # # # Handles notifications, emails, and scheduled reminders for Firebase app
# # # # # # # # """

# # # # # # # # import os
# # # # # # # # import smtplib
# # # # # # # # from email.mime.text import MIMEText
# # # # # # # # from email.mime.multipart import MIMEMultipart
# # # # # # # # from datetime import datetime, timedelta
# # # # # # # # from typing import Optional, List, Dict, Any
# # # # # # # # from dotenv import load_dotenv

# # # # # # # # from fastapi import FastAPI, HTTPException, BackgroundTasks
# # # # # # # # from fastapi.middleware.cors import CORSMiddleware
# # # # # # # # from pydantic import BaseModel, EmailStr

# # # # # # # # import firebase_admin
# # # # # # # # from firebase_admin import credentials, firestore, messaging

# # # # # # # # # Load environment variables FIRST
# # # # # # # # load_dotenv()

# # # # # # # # # Email configuration
# # # # # # # # SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
# # # # # # # # SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
# # # # # # # # SMTP_USER = os.getenv("SMTP_USER")
# # # # # # # # SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# # # # # # # # FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# # # # # # # # # Initialize FastAPI
# # # # # # # # app = FastAPI(
# # # # # # # #     title="TeleMed Backend",
# # # # # # # #     description="Notification and email service for TeleMed app",
# # # # # # # #     version="1.0.0"
# # # # # # # # )

# # # # # # # # # CORS - Allow your Flutter app to call this API
# # # # # # # # app.add_middleware(
# # # # # # # #     CORSMiddleware,
# # # # # # # #     allow_origins=["*"],  # In production, replace with your actual domain
# # # # # # # #     allow_credentials=True,
# # # # # # # #     allow_methods=["*"],
# # # # # # # #     allow_headers=["*"],
# # # # # # # # )

# # # # # # # # # Initialize Firebase Admin SDK
# # # # # # # # cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH"))
# # # # # # # # firebase_admin.initialize_app(cred)
# # # # # # # # db = firestore.client()


# # # # # # # # # ============================================================================
# # # # # # # # # PYDANTIC MODELS
# # # # # # # # # ============================================================================

# # # # # # # # class BookingConfirmedRequest(BaseModel):
# # # # # # # #     appointment_id: str
# # # # # # # #     patient_id: str
# # # # # # # #     doctor_id: str
# # # # # # # #     appointment_datetime: str  # ISO format
# # # # # # # #     duration_minutes: int


# # # # # # # # class AppointmentCanceledRequest(BaseModel):
# # # # # # # #     appointment_id: str
# # # # # # # #     patient_id: str
# # # # # # # #     doctor_id: str
# # # # # # # #     canceled_by: str  # "patient" or "doctor"
# # # # # # # #     appointment_datetime: str


# # # # # # # # # ============================================================================
# # # # # # # # # HELPER FUNCTIONS
# # # # # # # # # ============================================================================

# # # # # # # # async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
# # # # # # # #     """Fetch user data from Firestore"""
# # # # # # # #     try:
# # # # # # # #         user_ref = db.collection("users").document(uid)
# # # # # # # #         user_doc = user_ref.get()
# # # # # # # #         return user_doc.to_dict() if user_doc.exists else None
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ Error fetching user data for {uid}: {e}")
# # # # # # # #         return None


# # # # # # # # async def send_fcm_notification(
# # # # # # # #     fcm_token: str,
# # # # # # # #     title: str,
# # # # # # # #     body: str,
# # # # # # # #     data: Optional[Dict[str, str]] = None
# # # # # # # # ):
# # # # # # # #     """Send FCM push notification"""
# # # # # # # #     if not fcm_token:
# # # # # # # #         print("⚠️ No FCM token provided")
# # # # # # # #         return
    
# # # # # # # #     try:
# # # # # # # #         message = messaging.Message(
# # # # # # # #             notification=messaging.Notification(title=title, body=body),
# # # # # # # #             data=data or {},
# # # # # # # #             token=fcm_token,
# # # # # # # #         )
# # # # # # # #         response = messaging.send(message)
# # # # # # # #         print(f"✅ FCM sent: {response}")
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ FCM failed: {e}")


# # # # # # # # async def send_email(
# # # # # # # #     to_email: str,
# # # # # # # #     to_name: str,
# # # # # # # #     subject: str,
# # # # # # # #     html_content: str
# # # # # # # # ):
# # # # # # # #     """Send email via Gmail SMTP"""
# # # # # # # #     try:
# # # # # # # #         msg = MIMEMultipart('alternative')
# # # # # # # #         msg['Subject'] = subject
# # # # # # # #         msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
# # # # # # # #         msg['To'] = to_email
        
# # # # # # # #         html_part = MIMEText(html_content, 'html')
# # # # # # # #         msg.attach(html_part)
        
# # # # # # # #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# # # # # # # #             server.starttls()
# # # # # # # #             server.login(SMTP_USER, SMTP_PASSWORD)
# # # # # # # #             server.send_message(msg)
        
# # # # # # # #         print(f"✅ Email sent via Gmail to {to_email}")
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ Email failed for {to_email}: {e}")


# # # # # # # # def format_datetime(iso_string: str) -> str:
# # # # # # # #     """Format ISO datetime to readable format"""
# # # # # # # #     try:
# # # # # # # #         dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
# # # # # # # #         return dt.strftime("%B %d, %Y at %I:%M %p")
# # # # # # # #     except:
# # # # # # # #         return iso_string


# # # # # # # # # ============================================================================
# # # # # # # # # EMAIL TEMPLATES
# # # # # # # # # ============================================================================

# # # # # # # # def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
# # # # # # # #     return f"""
# # # # # # # #     <!DOCTYPE html>
# # # # # # # #     <html>
# # # # # # # #     <head>
# # # # # # # #         <style>
# # # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # # #             .header {{ background: #4A90E2; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #4A90E2; }}
# # # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # # #         </style>
# # # # # # # #     </head>
# # # # # # # #     <body>
# # # # # # # #         <div class="container">
# # # # # # # #             <div class="header">
# # # # # # # #                 <h1>✅ Appointment Confirmed</h1>
# # # # # # # #             </div>
# # # # # # # #             <div class="content">
# # # # # # # #                 <p>Hi {patient_name},</p>
# # # # # # # #                 <p>Great news! Your telemedicine appointment has been confirmed.</p>
                
# # # # # # # #                 <div class="info-box">
# # # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # # #                     <p><strong>📅 Date & Time:</strong> {appointment_time}</p>
# # # # # # # #                 </div>
                
# # # # # # # #                 <p>You will receive reminder notifications before your appointment.</p>
# # # # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # # # #             </div>
# # # # # # # #             <div class="footer">
# # # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # # #             </div>
# # # # # # # #         </div>
# # # # # # # #     </body>
# # # # # # # #     </html>
# # # # # # # #     """


# # # # # # # # def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
# # # # # # # #     return f"""
# # # # # # # #     <!DOCTYPE html>
# # # # # # # #     <html>
# # # # # # # #     <head>
# # # # # # # #         <style>
# # # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # # #             .header {{ background: #E74C3C; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #E74C3C; }}
# # # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # # #         </style>
# # # # # # # #     </head>
# # # # # # # #     <body>
# # # # # # # #         <div class="container">
# # # # # # # #             <div class="header">
# # # # # # # #                 <h1>❌ Appointment Canceled</h1>
# # # # # # # #             </div>
# # # # # # # #             <div class="content">
# # # # # # # #                 <p>Hi {name},</p>
# # # # # # # #                 <p>We're writing to inform you that the following appointment has been canceled by the {canceled_by}.</p>
                
# # # # # # # #                 <div class="info-box">
# # # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # # #                     <p><strong>📅 Original Date & Time:</strong> {appointment_time}</p>
# # # # # # # #                 </div>
                
# # # # # # # #                 <p>You can book a new appointment anytime through the app.</p>
# # # # # # # #             </div>
# # # # # # # #             <div class="footer">
# # # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # # #             </div>
# # # # # # # #         </div>
# # # # # # # #     </body>
# # # # # # # #     </html>
# # # # # # # #     """


# # # # # # # # def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
# # # # # # # #     return f"""
# # # # # # # #     <!DOCTYPE html>
# # # # # # # #     <html>
# # # # # # # #     <head>
# # # # # # # #         <style>
# # # # # # # #             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
# # # # # # # #             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
# # # # # # # #             .header {{ background: #F39C12; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
# # # # # # # #             .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
# # # # # # # #             .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #F39C12; }}
# # # # # # # #             .reminder-badge {{ background: #F39C12; color: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 20px 0; font-weight: bold; }}
# # # # # # # #             .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
# # # # # # # #         </style>
# # # # # # # #     </head>
# # # # # # # #     <body>
# # # # # # # #         <div class="container">
# # # # # # # #             <div class="header">
# # # # # # # #                 <h1>⏰ Appointment Reminder</h1>
# # # # # # # #             </div>
# # # # # # # #             <div class="content">
# # # # # # # #                 <p>Hi {name},</p>
# # # # # # # #                 <p>This is a friendly reminder about your upcoming appointment.</p>
                
# # # # # # # #                 <div style="text-align: center;">
# # # # # # # #                     <span class="reminder-badge">In {hours_until} hour(s)</span>
# # # # # # # #                 </div>
                
# # # # # # # #                 <div class="info-box">
# # # # # # # #                     <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
# # # # # # # #                     <p><strong>📅 Date & Time:</strong> {appointment_time}</p>
# # # # # # # #                 </div>
                
# # # # # # # #                 <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
# # # # # # # #             </div>
# # # # # # # #             <div class="footer">
# # # # # # # #                 <p>TeleMed - Your Health, Our Priority</p>
# # # # # # # #             </div>
# # # # # # # #         </div>
# # # # # # # #     </body>
# # # # # # # #     </html>
# # # # # # # #     """


# # # # # # # # # ============================================================================
# # # # # # # # # ENDPOINTS
# # # # # # # # # ============================================================================

# # # # # # # # @app.get("/")
# # # # # # # # async def root():
# # # # # # # #     """Health check endpoint"""
# # # # # # # #     return {
# # # # # # # #         "status": "healthy",
# # # # # # # #         "service": "TeleMed Backend",
# # # # # # # #         "version": "1.0.0",
# # # # # # # #         "email_provider": "gmail_smtp",
# # # # # # # #         "timestamp": datetime.utcnow().isoformat()
# # # # # # # #     }


# # # # # # # # @app.post("/booking-confirmed")
# # # # # # # # async def booking_confirmed(
# # # # # # # #     request: BookingConfirmedRequest,
# # # # # # # #     background_tasks: BackgroundTasks
# # # # # # # # ):
# # # # # # # #     """
# # # # # # # #     Called when a new appointment is booked
# # # # # # # #     Sends notifications to both patient and doctor
# # # # # # # #     """
# # # # # # # #     try:
# # # # # # # #         # Fetch patient and doctor data
# # # # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # # # #         if not patient_data or not doctor_data:
# # # # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # # # #         # Prepare notification content
# # # # # # # #         patient_title = "Appointment Confirmed ✅"
# # # # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} is confirmed for {appointment_time}"
        
# # # # # # # #         doctor_title = "New Appointment 📅"
# # # # # # # #         doctor_body = f"New appointment with {patient_name} scheduled for {appointment_time}"
        
# # # # # # # #         # Send FCM notifications in background
# # # # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_fcm_notification,
# # # # # # # #                 patient_fcm,
# # # # # # # #                 patient_title,
# # # # # # # #                 patient_body,
# # # # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # # # #             )
        
# # # # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_fcm_notification,
# # # # # # # #                 doctor_fcm,
# # # # # # # #                 doctor_title,
# # # # # # # #                 doctor_body,
# # # # # # # #                 {"type": "booking_confirmed", "appointment_id": request.appointment_id}
# # # # # # # #             )
        
# # # # # # # #         # Send emails in background
# # # # # # # #         if patient_email := patient_data.get("email"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_email,
# # # # # # # #                 patient_email,
# # # # # # # #                 patient_name,
# # # # # # # #                 "Appointment Confirmed",
# # # # # # # #                 booking_confirmed_email(patient_name, doctor_name, appointment_time)
# # # # # # # #             )
        
# # # # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_email,
# # # # # # # #                 doctor_email,
# # # # # # # #                 doctor_name,
# # # # # # # #                 "New Appointment Scheduled",
# # # # # # # #                 booking_confirmed_email(doctor_name, patient_name, appointment_time)
# # # # # # # #             )
        
# # # # # # # #         return {
# # # # # # # #             "success": True,
# # # # # # # #             "message": "Notifications sent successfully",
# # # # # # # #             "patient": patient_name,
# # # # # # # #             "doctor": doctor_name
# # # # # # # #         }
    
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ Error in booking_confirmed: {e}")
# # # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # # @app.post("/appointment-canceled")
# # # # # # # # async def appointment_canceled(
# # # # # # # #     request: AppointmentCanceledRequest,
# # # # # # # #     background_tasks: BackgroundTasks
# # # # # # # # ):
# # # # # # # #     """
# # # # # # # #     Called when an appointment is canceled
# # # # # # # #     Sends notifications to both patient and doctor
# # # # # # # #     """
# # # # # # # #     try:
# # # # # # # #         # Fetch patient and doctor data
# # # # # # # #         patient_data = await get_user_data(request.patient_id)
# # # # # # # #         doctor_data = await get_user_data(request.doctor_id)
        
# # # # # # # #         if not patient_data or not doctor_data:
# # # # # # # #             raise HTTPException(status_code=404, detail="User not found")
        
# # # # # # # #         patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # # #         doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # # #         appointment_time = format_datetime(request.appointment_datetime)
        
# # # # # # # #         # Prepare notification content
# # # # # # # #         title = "Appointment Canceled ❌"
# # # # # # # #         patient_body = f"Your appointment with Dr. {doctor_name} on {appointment_time} has been canceled"
# # # # # # # #         doctor_body = f"Appointment with {patient_name} on {appointment_time} has been canceled"
        
# # # # # # # #         # Send FCM notifications
# # # # # # # #         if patient_fcm := patient_data.get("fcmToken"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_fcm_notification,
# # # # # # # #                 patient_fcm,
# # # # # # # #                 title,
# # # # # # # #                 patient_body,
# # # # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # # # #             )
        
# # # # # # # #         if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_fcm_notification,
# # # # # # # #                 doctor_fcm,
# # # # # # # #                 title,
# # # # # # # #                 doctor_body,
# # # # # # # #                 {"type": "appointment_canceled", "appointment_id": request.appointment_id}
# # # # # # # #             )
        
# # # # # # # #         # Send emails
# # # # # # # #         if patient_email := patient_data.get("email"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_email,
# # # # # # # #                 patient_email,
# # # # # # # #                 patient_name,
# # # # # # # #                 "Appointment Canceled",
# # # # # # # #                 appointment_canceled_email(patient_name, doctor_name, appointment_time, request.canceled_by)
# # # # # # # #             )
        
# # # # # # # #         if doctor_email := doctor_data.get("email"):
# # # # # # # #             background_tasks.add_task(
# # # # # # # #                 send_email,
# # # # # # # #                 doctor_email,
# # # # # # # #                 doctor_name,
# # # # # # # #                 "Appointment Canceled",
# # # # # # # #                 appointment_canceled_email(doctor_name, patient_name, appointment_time, request.canceled_by)
# # # # # # # #             )
        
# # # # # # # #         return {
# # # # # # # #             "success": True,
# # # # # # # #             "message": "Cancellation notifications sent",
# # # # # # # #             "canceled_by": request.canceled_by
# # # # # # # #         }
    
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ Error in appointment_canceled: {e}")
# # # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # # @app.get("/check-reminders")
# # # # # # # # async def check_reminders(background_tasks: BackgroundTasks):
# # # # # # # #     """
# # # # # # # #     Called by cron job every hour
# # # # # # # #     Checks for appointments in next 24h and 1h
# # # # # # # #     Sends reminder notifications
# # # # # # # #     """
# # # # # # # #     try:
# # # # # # # #         now = datetime.utcnow()
        
# # # # # # # #         # Time windows for reminders
# # # # # # # #         in_24h = now + timedelta(hours=24)
# # # # # # # #         in_1h = now + timedelta(hours=1)
        
# # # # # # # #         # Query appointments in the next 24 hours
# # # # # # # #         appointments_ref = db.collection("appointments")
# # # # # # # #         upcoming = appointments_ref.where("status", "==", "confirmed").stream()
        
# # # # # # # #         reminders_sent = 0
        
# # # # # # # #         for doc in upcoming:
# # # # # # # #             appointment = doc.to_dict()
            
# # # # # # # #             # Parse appointment datetime
# # # # # # # #             try:
# # # # # # # #                 apt_time_str = appointment.get("appointmentDateTime")
# # # # # # # #                 apt_time = datetime.fromisoformat(apt_time_str.replace('Z', '+00:00'))
# # # # # # # #             except:
# # # # # # # #                 continue
            
# # # # # # # #             # Check if reminder was already sent
# # # # # # # #             last_reminder = appointment.get("lastReminderSent")
            
# # # # # # # #             # 24-hour reminder
# # # # # # # #             if now <= apt_time <= in_24h and not last_reminder:
# # # # # # # #                 await send_appointment_reminder(
# # # # # # # #                     appointment, 
# # # # # # # #                     doc.id, 
# # # # # # # #                     hours_until=24,
# # # # # # # #                     background_tasks=background_tasks
# # # # # # # #                 )
# # # # # # # #                 reminders_sent += 1
            
# # # # # # # #             # 1-hour reminder
# # # # # # # #             elif now <= apt_time <= in_1h and last_reminder != "1h":
# # # # # # # #                 await send_appointment_reminder(
# # # # # # # #                     appointment, 
# # # # # # # #                     doc.id, 
# # # # # # # #                     hours_until=1,
# # # # # # # #                     background_tasks=background_tasks
# # # # # # # #                 )
# # # # # # # #                 reminders_sent += 1
        
# # # # # # # #         return {
# # # # # # # #             "success": True,
# # # # # # # #             "reminders_sent": reminders_sent,
# # # # # # # #             "checked_at": now.isoformat()
# # # # # # # #         }
    
# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"❌ Error in check_reminders: {e}")
# # # # # # # #         raise HTTPException(status_code=500, detail=str(e))


# # # # # # # # async def send_appointment_reminder(
# # # # # # # #     appointment: Dict[str, Any],
# # # # # # # #     appointment_id: str,
# # # # # # # #     hours_until: int,
# # # # # # # #     background_tasks: BackgroundTasks
# # # # # # # # ):
# # # # # # # #     """Helper function to send appointment reminders"""
# # # # # # # #     patient_id = appointment.get("patientId")
# # # # # # # #     doctor_id = appointment.get("doctorId")
# # # # # # # #     appointment_time_str = appointment.get("appointmentDateTime")
    
# # # # # # # #     # Fetch user data
# # # # # # # #     patient_data = await get_user_data(patient_id)
# # # # # # # #     doctor_data = await get_user_data(doctor_id)
    
# # # # # # # #     if not patient_data or not doctor_data:
# # # # # # # #         return
    
# # # # # # # #     patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
# # # # # # # #     doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
# # # # # # # #     appointment_time = format_datetime(appointment_time_str)
    
# # # # # # # #     # Notification content
# # # # # # # #     title = f"⏰ Appointment in {hours_until}h"
# # # # # # # #     patient_body = f"Reminder: Appointment with Dr. {doctor_name} at {appointment_time}"
# # # # # # # #     doctor_body = f"Reminder: Appointment with {patient_name} at {appointment_time}"
    
# # # # # # # #     # Send to patient
# # # # # # # #     if patient_fcm := patient_data.get("fcmToken"):
# # # # # # # #         background_tasks.add_task(
# # # # # # # #             send_fcm_notification,
# # # # # # # #             patient_fcm,
# # # # # # # #             title,
# # # # # # # #             patient_body,
# # # # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # # # #         )
    
# # # # # # # #     if patient_email := patient_data.get("email"):
# # # # # # # #         background_tasks.add_task(
# # # # # # # #             send_email,
# # # # # # # #             patient_email,
# # # # # # # #             patient_name,
# # # # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # # # #             reminder_email(patient_name, doctor_name, appointment_time, hours_until)
# # # # # # # #         )
    
# # # # # # # #     # Send to doctor
# # # # # # # #     if doctor_fcm := doctor_data.get("fcmToken"):
# # # # # # # #         background_tasks.add_task(
# # # # # # # #             send_fcm_notification,
# # # # # # # #             doctor_fcm,
# # # # # # # #             title,
# # # # # # # #             doctor_body,
# # # # # # # #             {"type": "reminder", "appointment_id": appointment_id}
# # # # # # # #         )
    
# # # # # # # #     if doctor_email := doctor_data.get("email"):
# # # # # # # #         background_tasks.add_task(
# # # # # # # #             send_email,
# # # # # # # #             doctor_email,
# # # # # # # #             doctor_name,
# # # # # # # #             f"Appointment Reminder - {hours_until}h",
# # # # # # # #             reminder_email(doctor_name, patient_name, appointment_time, hours_until)
# # # # # # # #         )
    
# # # # # # # #     # Update Firestore to mark reminder as sent
# # # # # # # #     reminder_key = "1h" if hours_until == 1 else "24h"
# # # # # # # #     db.collection("appointments").document(appointment_id).update({
# # # # # # # #         "lastReminderSent": reminder_key
# # # # # # # #     })
    
# # # # # # # #     print(f"✅ Reminder sent for appointment {appointment_id} ({hours_until}h)")


# # # # # # # # if __name__ == "__main__":
# # # # # # # #     import uvicorn
# # # # # # # #     uvicorn.run(app, host="0.0.0.0", port=8000)


























