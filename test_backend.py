"""
Test script for TeleMed Backend
Run this locally to test endpoints before deployment
"""

import requests
import json
from datetime import datetime, timedelta

# Change this to your local or deployed URL
BASE_URL = "http://localhost:8000"  # Local development
# BASE_URL = "https://your-app-name.onrender.com"  # Production

print("🧪 TeleMed Backend Test Suite\n")

# ============================================================================
# 1. Health Check
# ============================================================================
print("1️⃣ Testing Health Check...")
try:
    response = requests.get(f"{BASE_URL}/")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    print("   ✅ Health check passed\n")
except Exception as e:
    print(f"   ❌ Health check failed: {e}\n")

# ============================================================================
# 2. Test Booking Confirmation
# ============================================================================
print("2️⃣ Testing Booking Confirmation...")

# You need to replace these with actual UIDs from your Firestore
test_booking = {
    "appointment_id": "test_apt_123",
    "patient_id": "REPLACE_WITH_PATIENT_UID",  # ← Change this
    "doctor_id": "REPLACE_WITH_DOCTOR_UID",    # ← Change this
    "appointment_datetime": (datetime.now() + timedelta(days=2)).isoformat(),
    "duration_minutes": 30
}

try:
    response = requests.post(
        f"{BASE_URL}/booking-confirmed",
        json=test_booking,
        headers={"Content-Type": "application/json"}
    )
    print(f"   Status: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("   ✅ Booking notification sent")
        print("   📱 Check FCM notifications on both devices")
        print("   📧 Check emails for both users\n")
    else:
        print(f"   ⚠️ Unexpected response\n")
except Exception as e:
    print(f"   ❌ Booking test failed: {e}\n")

# ============================================================================
# 3. Test Cancellation
# ============================================================================
print("3️⃣ Testing Appointment Cancellation...")

test_cancellation = {
    "appointment_id": "test_apt_123",
    "patient_id": "REPLACE_WITH_PATIENT_UID",  # ← Change this
    "doctor_id": "REPLACE_WITH_DOCTOR_UID",    # ← Change this
    "canceled_by": "patient",
    "appointment_datetime": (datetime.now() + timedelta(days=2)).isoformat()
}

try:
    response = requests.post(
        f"{BASE_URL}/appointment-canceled",
        json=test_cancellation,
        headers={"Content-Type": "application/json"}
    )
    print(f"   Status: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("   ✅ Cancellation notification sent")
        print("   📱 Check FCM notifications")
        print("   📧 Check emails\n")
    else:
        print(f"   ⚠️ Unexpected response\n")
except Exception as e:
    print(f"   ❌ Cancellation test failed: {e}\n")

# ============================================================================
# 4. Test Reminder Check
# ============================================================================
print("4️⃣ Testing Reminder Check...")

try:
    response = requests.get(f"{BASE_URL}/check-reminders")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    print("   ✅ Reminder check completed\n")
except Exception as e:
    print(f"   ❌ Reminder check failed: {e}\n")

# ============================================================================
# Summary
# ============================================================================
print("=" * 60)
print("📝 Test Summary")
print("=" * 60)
print("""
Next Steps:
1. Update patient_id and doctor_id with real UIDs
2. Ensure users have 'email' and 'fcmToken' in Firestore
3. Check SendGrid dashboard for email delivery status
4. Check device notifications (FCM)
5. Monitor Render logs for errors

For production testing:
- Change BASE_URL to your Render URL
- Run this script from your local machine
- Verify all notifications arrive within 30 seconds
""")