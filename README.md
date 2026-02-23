# SheyDoc App Bachend

This repository contains the **FastAPI** backend for sheydoc mobile application. It is responsible for sending push notifications, emails, and scheduled reminders when appointments are booked, canceled, or approaching. It uses Firebase Firestore for user/appointment data and Gmail SMTP for emails.

---

## Features

- **Booking confirmations**: Notify both patient and doctor via FCM and email.
- **Appointment cancellations**: Alerts sent to all parties.
- **Automated reminders**: Hourly cron job checks for appointments in the 24‑h and 1‑h windows, dispatching notifications/emails and marking reminders in Firestore.
- **Health check** endpoint for monitoring.
- CORS enabled to allow cross‑origin requests from your Flutter/React front end.

---

## Prerequisites

- Python 3.11+ (recommended)
- A Firebase project with Firestore
- Service account JSON file (`serviceAccountKey.json` or equivalent path)
- Gmail account with SMTP enabled (or any SMTP server)

---

## 📦 Installation

1. Clone the repo and change directory:

   ```powershell
   git clone <repo-url> sheydoc_backend
   cd sheydoc_backend
   ```

2. Create and activate a virtual environment (Windows example):

   ```powershell
   python -m venv venv
   venv\Scripts\Activate.ps1
   ```

3. Install dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

4. Copy your Firebase service account JSON to the repo (or store it elsewhere and adjust `FIREBASE_SERVICE_ACCOUNT_PATH`).

5. Create a `.env` file with the following variables:

   ```ini
   FIREBASE_SERVICE_ACCOUNT_PATH=serviceAccountKey.json
   SMTP_HOST=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USER=your@gmail.com
   SMTP_PASSWORD=app-specific-password
   FROM_NAME="TeleMed App"
   ```

   > **Note:** For Gmail, generate an App Password and/or enable "less secure" access.

---

## Running the Server

Locally with uvicorn:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000/`.

---

## API Endpoints

| Method | Path                 | Description                                  |
|--------|----------------------|----------------------------------------------|
| GET    | `/`                  | Health check                                 |
| POST   | `/booking-confirmed` | Notify on new appointment                    |
| POST   | `/appointment-canceled` | Notify on cancellation                      |
| GET    | `/check-reminders`   | Trigger reminder logic (call hourly via cron)|

### Payload Schemas

`/booking-confirmed`:
```json
{
  "appointment_id": "string",
  "patient_id": "string",
  "doctor_id": "string",
  "appointment_datetime": "ISO string",
  "duration_minutes": 30
}
```

`/appointment-canceled`:
```json
{
  "appointment_id": "string",
  "patient_id": "string",
  "doctor_id": "string",
  "canceled_by": "patient" | "doctor",
  "appointment_datetime": "ISO string"
}
```

`/check-reminders` has no body; run it hourly via an external scheduler (cron, Cloud Scheduler, etc.).

---

## Testing

- `quick_test.py` contains a simple script that exercises every endpoint against a local server. Update the patient/doctor UIDs to match entries in your Firestore and run:

  ```powershell
  python quick_test.py
  ```

- You can also use `test_backend.py` for pytest‑style unit tests.

---

## Firestore Requirements

- **Users** collection with documents containing at least `email`, `fcmToken`, and optional `displayName`/`firstName`.
- **Appointments** collection with `status` (confirmed), `appointmentDateTime` (ISO string), `patientId`, `doctorId`, and `lastReminderSent` fields.
- Composite index on `status` + `appointmentDateTime` for efficient queries used by reminders.

---

## 🔁 Deployment Notes

- Ensure environment variables are set in your hosting platform.
- Schedule `/check-reminders` to run every hour (e.g. Render cron job or Cloud Scheduler).
- Keep the service account key secure.


