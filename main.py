"""
TeleMed FastAPI Backend
Handles notifications, emails, and scheduled reminders for Firebase app
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

import firebase_admin
from firebase_admin import credentials, firestore, messaging

# Load environment variables FIRST
load_dotenv()

# Email configuration
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_NAME = os.getenv("FROM_NAME", "TeleMed App")

# Initialize FastAPI
app = FastAPI(
    title="TeleMed Backend",
    description="Notification and email service for TeleMed app",
    version="1.0.0"
)

# CORS - Allow your Flutter app to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your actual domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Firebase Admin SDK
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
    appointment_datetime: str  # ISO format
    duration_minutes: int


class AppointmentCanceledRequest(BaseModel):
    appointment_id: str
    patient_id: str
    doctor_id: str
    canceled_by: str  # "patient" or "doctor"
    appointment_datetime: str


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

async def get_user_data(uid: str) -> Optional[Dict[str, Any]]:
    """Fetch user data from Firestore"""
    try:
        user_ref = db.collection("users").document(uid)
        user_doc = user_ref.get()
        return user_doc.to_dict() if user_doc.exists else None
    except Exception as e:
        print(f"❌ Error fetching user data for {uid}: {e}")
        return None


async def send_fcm_notification(
    fcm_token: str,
    title: str,
    body: str,
    data: Optional[Dict[str, str]] = None
):
    """Send FCM push notification"""
    if not fcm_token:
        print("⚠️ No FCM token provided")
        return
    
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data or {},
            token=fcm_token,
        )
        response = messaging.send(message)
        print(f"✅ FCM sent: {response}")
    except Exception as e:
        print(f"❌ FCM failed: {e}")


async def send_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str
):
    """Send email via Gmail SMTP"""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{FROM_NAME} <{SMTP_USER}>"
        msg['To'] = to_email
        
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        print(f"✅ Email sent via Gmail to {to_email}")
    except Exception as e:
        print(f"❌ Email failed for {to_email}: {e}")


def format_datetime(iso_string: str) -> str:
    """Format ISO datetime to readable format"""
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.strftime("%B %d, %Y at %I:%M %p")
    except:
        return iso_string


# ============================================================================
# EMAIL TEMPLATES
# ============================================================================

def booking_confirmed_email(patient_name: str, doctor_name: str, appointment_time: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: #4A90E2; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
            .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #4A90E2; }}
            .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>✅ Appointment Confirmed</h1>
            </div>
            <div class="content">
                <p>Hi {patient_name},</p>
                <p>Great news! Your telemedicine appointment has been confirmed.</p>
                
                <div class="info-box">
                    <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
                    <p><strong>📅 Date & Time:</strong> {appointment_time}</p>
                </div>
                
                <p>You will receive reminder notifications before your appointment.</p>
                <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
            </div>
            <div class="footer">
                <p>TeleMed - Your Health, Our Priority</p>
            </div>
        </div>
    </body>
    </html>
    """


def appointment_canceled_email(name: str, doctor_name: str, appointment_time: str, canceled_by: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: #E74C3C; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
            .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #E74C3C; }}
            .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>❌ Appointment Canceled</h1>
            </div>
            <div class="content">
                <p>Hi {name},</p>
                <p>We're writing to inform you that the following appointment has been canceled by the {canceled_by}.</p>
                
                <div class="info-box">
                    <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
                    <p><strong>📅 Original Date & Time:</strong> {appointment_time}</p>
                </div>
                
                <p>You can book a new appointment anytime through the app.</p>
            </div>
            <div class="footer">
                <p>TeleMed - Your Health, Our Priority</p>
            </div>
        </div>
    </body>
    </html>
    """


def reminder_email(name: str, doctor_name: str, appointment_time: str, hours_until: int) -> str:
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: #F39C12; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
            .info-box {{ background: white; padding: 15px; margin: 20px 0; border-left: 4px solid #F39C12; }}
            .reminder-badge {{ background: #F39C12; color: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 20px 0; font-weight: bold; }}
            .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>⏰ Appointment Reminder</h1>
            </div>
            <div class="content">
                <p>Hi {name},</p>
                <p>This is a friendly reminder about your upcoming appointment.</p>
                
                <div style="text-align: center;">
                    <span class="reminder-badge">In {hours_until} hour(s)</span>
                </div>
                
                <div class="info-box">
                    <p><strong>👨‍⚕️ Doctor:</strong> Dr. {doctor_name}</p>
                    <p><strong>📅 Date & Time:</strong> {appointment_time}</p>
                </div>
                
                <p>Please be ready to join the video call a few minutes before the scheduled time.</p>
            </div>
            <div class="footer">
                <p>TeleMed - Your Health, Our Priority</p>
            </div>
        </div>
    </body>
    </html>
    """


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "TeleMed Backend",
        "version": "1.0.0",
        "email_provider": "gmail_smtp",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/booking-confirmed")
async def booking_confirmed(
    request: BookingConfirmedRequest,
    background_tasks: BackgroundTasks
):
    """
    Called when a new appointment is booked
    Sends notifications to both patient and doctor
    """
    try:
        # Fetch patient and doctor data
        patient_data = await get_user_data(request.patient_id)
        doctor_data = await get_user_data(request.doctor_id)
        
        if not patient_data or not doctor_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
        doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
        appointment_time = format_datetime(request.appointment_datetime)
        
        # Prepare notification content
        patient_title = "Appointment Confirmed ✅"
        patient_body = f"Your appointment with Dr. {doctor_name} is confirmed for {appointment_time}"
        
        doctor_title = "New Appointment 📅"
        doctor_body = f"New appointment with {patient_name} scheduled for {appointment_time}"
        
        # Send FCM notifications in background
        if patient_fcm := patient_data.get("fcmToken"):
            background_tasks.add_task(
                send_fcm_notification,
                patient_fcm,
                patient_title,
                patient_body,
                {"type": "booking_confirmed", "appointment_id": request.appointment_id}
            )
        
        if doctor_fcm := doctor_data.get("fcmToken"):
            background_tasks.add_task(
                send_fcm_notification,
                doctor_fcm,
                doctor_title,
                doctor_body,
                {"type": "booking_confirmed", "appointment_id": request.appointment_id}
            )
        
        # Send emails in background
        if patient_email := patient_data.get("email"):
            background_tasks.add_task(
                send_email,
                patient_email,
                patient_name,
                "Appointment Confirmed",
                booking_confirmed_email(patient_name, doctor_name, appointment_time)
            )
        
        if doctor_email := doctor_data.get("email"):
            background_tasks.add_task(
                send_email,
                doctor_email,
                doctor_name,
                "New Appointment Scheduled",
                booking_confirmed_email(doctor_name, patient_name, appointment_time)
            )
        
        return {
            "success": True,
            "message": "Notifications sent successfully",
            "patient": patient_name,
            "doctor": doctor_name
        }
    
    except Exception as e:
        print(f"❌ Error in booking_confirmed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/appointment-canceled")
async def appointment_canceled(
    request: AppointmentCanceledRequest,
    background_tasks: BackgroundTasks
):
    """
    Called when an appointment is canceled
    Sends notifications to both patient and doctor
    """
    try:
        # Fetch patient and doctor data
        patient_data = await get_user_data(request.patient_id)
        doctor_data = await get_user_data(request.doctor_id)
        
        if not patient_data or not doctor_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
        doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
        appointment_time = format_datetime(request.appointment_datetime)
        
        # Prepare notification content
        title = "Appointment Canceled ❌"
        patient_body = f"Your appointment with Dr. {doctor_name} on {appointment_time} has been canceled"
        doctor_body = f"Appointment with {patient_name} on {appointment_time} has been canceled"
        
        # Send FCM notifications
        if patient_fcm := patient_data.get("fcmToken"):
            background_tasks.add_task(
                send_fcm_notification,
                patient_fcm,
                title,
                patient_body,
                {"type": "appointment_canceled", "appointment_id": request.appointment_id}
            )
        
        if doctor_fcm := doctor_data.get("fcmToken"):
            background_tasks.add_task(
                send_fcm_notification,
                doctor_fcm,
                title,
                doctor_body,
                {"type": "appointment_canceled", "appointment_id": request.appointment_id}
            )
        
        # Send emails
        if patient_email := patient_data.get("email"):
            background_tasks.add_task(
                send_email,
                patient_email,
                patient_name,
                "Appointment Canceled",
                appointment_canceled_email(patient_name, doctor_name, appointment_time, request.canceled_by)
            )
        
        if doctor_email := doctor_data.get("email"):
            background_tasks.add_task(
                send_email,
                doctor_email,
                doctor_name,
                "Appointment Canceled",
                appointment_canceled_email(doctor_name, patient_name, appointment_time, request.canceled_by)
            )
        
        return {
            "success": True,
            "message": "Cancellation notifications sent",
            "canceled_by": request.canceled_by
        }
    
    except Exception as e:
        print(f"❌ Error in appointment_canceled: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/check-reminders")
async def check_reminders(background_tasks: BackgroundTasks):
    """
    Called by cron job every hour
    Checks for appointments in next 24h and 1h
    Sends reminder notifications
    """
    try:
        now = datetime.utcnow()
        
        # Time windows for reminders
        in_24h = now + timedelta(hours=24)
        in_1h = now + timedelta(hours=1)
        
        # Query appointments in the next 24 hours
        appointments_ref = db.collection("appointments")
        upcoming = appointments_ref.where("status", "==", "confirmed").stream()
        
        reminders_sent = 0
        
        for doc in upcoming:
            appointment = doc.to_dict()
            
            # Parse appointment datetime
            try:
                apt_time_str = appointment.get("appointmentDateTime")
                apt_time = datetime.fromisoformat(apt_time_str.replace('Z', '+00:00'))
            except:
                continue
            
            # Check if reminder was already sent
            last_reminder = appointment.get("lastReminderSent")
            
            # 24-hour reminder
            if now <= apt_time <= in_24h and not last_reminder:
                await send_appointment_reminder(
                    appointment, 
                    doc.id, 
                    hours_until=24,
                    background_tasks=background_tasks
                )
                reminders_sent += 1
            
            # 1-hour reminder
            elif now <= apt_time <= in_1h and last_reminder != "1h":
                await send_appointment_reminder(
                    appointment, 
                    doc.id, 
                    hours_until=1,
                    background_tasks=background_tasks
                )
                reminders_sent += 1
        
        return {
            "success": True,
            "reminders_sent": reminders_sent,
            "checked_at": now.isoformat()
        }
    
    except Exception as e:
        print(f"❌ Error in check_reminders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def send_appointment_reminder(
    appointment: Dict[str, Any],
    appointment_id: str,
    hours_until: int,
    background_tasks: BackgroundTasks
):
    """Helper function to send appointment reminders"""
    patient_id = appointment.get("patientId")
    doctor_id = appointment.get("doctorId")
    appointment_time_str = appointment.get("appointmentDateTime")
    
    # Fetch user data
    patient_data = await get_user_data(patient_id)
    doctor_data = await get_user_data(doctor_id)
    
    if not patient_data or not doctor_data:
        return
    
    patient_name = patient_data.get("displayName") or patient_data.get("firstName", "Patient")
    doctor_name = doctor_data.get("displayName") or doctor_data.get("firstName", "Doctor")
    appointment_time = format_datetime(appointment_time_str)
    
    # Notification content
    title = f"⏰ Appointment in {hours_until}h"
    patient_body = f"Reminder: Appointment with Dr. {doctor_name} at {appointment_time}"
    doctor_body = f"Reminder: Appointment with {patient_name} at {appointment_time}"
    
    # Send to patient
    if patient_fcm := patient_data.get("fcmToken"):
        background_tasks.add_task(
            send_fcm_notification,
            patient_fcm,
            title,
            patient_body,
            {"type": "reminder", "appointment_id": appointment_id}
        )
    
    if patient_email := patient_data.get("email"):
        background_tasks.add_task(
            send_email,
            patient_email,
            patient_name,
            f"Appointment Reminder - {hours_until}h",
            reminder_email(patient_name, doctor_name, appointment_time, hours_until)
        )
    
    # Send to doctor
    if doctor_fcm := doctor_data.get("fcmToken"):
        background_tasks.add_task(
            send_fcm_notification,
            doctor_fcm,
            title,
            doctor_body,
            {"type": "reminder", "appointment_id": appointment_id}
        )
    
    if doctor_email := doctor_data.get("email"):
        background_tasks.add_task(
            send_email,
            doctor_email,
            doctor_name,
            f"Appointment Reminder - {hours_until}h",
            reminder_email(doctor_name, patient_name, appointment_time, hours_until)
        )
    
    # Update Firestore to mark reminder as sent
    reminder_key = "1h" if hours_until == 1 else "24h"
    db.collection("appointments").document(appointment_id).update({
        "lastReminderSent": reminder_key
    })
    
    print(f"✅ Reminder sent for appointment {appointment_id} ({hours_until}h)")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


























