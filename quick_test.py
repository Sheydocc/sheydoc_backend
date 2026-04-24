# # # quick_test.py
# # import firebase_admin
# # from firebase_admin import credentials, firestore

# # cred = credentials.Certificate("serviceAccountKey.json")
# # firebase_admin.initialize_app(cred)
# # db = firestore.client()

# # print("📋 Users in Firestore:\n")
# # users = db.collection("users").limit(5).stream()
# # for user in users:
# #     data = user.to_dict()
# #     print(f"UID: {user.id}")
# #     print(f"  Name: {data.get('displayName', 'N/A')}")
# #     print(f"  Email: {data.get('email', 'N/A')}")
# #     print(f"  FCM Token: {'✅ Present' if data.get('fcmToken') else '❌ Missing'}")
# #     print()


# from datetime import datetime, timedelta


# test_booking = {
#     "appointment_id": "test_apt_123",
#     "patient_id": "qcHVeSAQR4al1P7lcUh87ineEBI3",  # ← Real UID from Firestore
#     "doctor_id": "XxBOByz2DYheGOrZGqqC6Ymvt7M2",    # ← Real UID from Firestore
#     "appointment_datetime": (datetime.now() + timedelta(days=2)).isoformat(),
#     "duration_minutes": 30
# }



# # """
# # Quick helper to fetch user IDs from Firestore
# # Use this to get real UIDs for testing the backend
# # """

# # import firebase_admin
# # from firebase_admin import credentials, firestore
# # import os

# # # Initialize Firebase
# # try:
# #     cred = credentials.Certificate("serviceAccountKey.json")
# #     firebase_admin.initialize_app(cred)
# #     db = firestore.client()
# #     print("✅ Firebase connected\n")
# # except Exception as e:
# #     print(f"❌ Firebase connection failed: {e}")
# #     print("\nMake sure serviceAccountKey.json is in the same folder!")
# #     exit(1)

# # print("=" * 70)
# # print("📋 USERS IN FIRESTORE")
# # print("=" * 70)

# # try:
# #     # Fetch users
# #     users = db.collection("users").limit(10).stream()
# #     user_count = 0
    
# #     for user in users:
# #         user_count += 1
# #         data = user.to_dict()
        
# #         print(f"\n👤 User #{user_count}")
# #         print(f"   UID: {user.id}")
# #         print(f"   Name: {data.get('displayName', data.get('firstName', 'N/A'))}")
# #         print(f"   Email: {data.get('email', '❌ NO EMAIL')}")
# #         print(f"   FCM Token: {'✅ Present' if data.get('fcmToken') else '❌ Missing'}")
# #         print(f"   Role: {data.get('role', 'N/A')}")
# #         print(f"   ---")
    
# #     if user_count == 0:
# #         print("\n⚠️ No users found in Firestore!")
# #         print("   Make sure you have users registered in your app first.")
# #     else:
# #         print(f"\n✅ Found {user_count} user(s)")
# #         print("\n" + "=" * 70)
# #         print("📝 NEXT STEPS:")
# #         print("=" * 70)
# #         print("1. Copy two UIDs from above (one patient, one doctor)")
# #         print("2. Open test_script.py")
# #         print("3. Replace 'REPLACE_WITH_PATIENT_UID' with a patient UID")
# #         print("4. Replace 'REPLACE_WITH_DOCTOR_UID' with a doctor UID")
# #         print("5. Run: python test_script.py")
        
# # except Exception as e:
# #     print(f"\n❌ Error fetching users: {e}")

# # print("\n")




"""
Test script for TeleMed Backend
Run this locally to test endpoints before deployment
"""

import requests
import json
from datetime import datetime, timedelta


BASE_URL = "http://localhost:8000"  # Local development


print("🧪 TeleMed Backend Test Suite\n")


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


test_booking = {
    "appointment_id": "test_apt_123",
    "patient_id": "qcHVeSAQR4al1P7lcUh87ineEBI3",  # Patient UID
    "doctor_id": "XxBOByz2DYheGOrZGqqC6Ymvt7M2",    # Doctor UID
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


# 3. Test Cancellation

print("3️⃣ Testing Appointment Cancellation...")

test_cancellation = {
    "appointment_id": "test_apt_123",
    "patient_id": "qcHVeSAQR4al1P7lcUh87ineEBI3",  # Patient UID
    "doctor_id": "XxBOByz2DYheGOrZGqqC6Ymvt7M2",    # Doctor UID
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
    print("   Reminder check completed\n")
except Exception as e:
    print(f"   ❌ Reminder check failed: {e}\n")

# ============================================================================
# Summary
# ============================================================================
print("=" * 60)
print("Test Summary")
print("=" * 60)
print("""
Next Steps:
1. User IDs are already configured
2. Ensure users have 'email' and 'fcmToken' in Firestore
3. Check email inbox for both users
4. Check device notifications (FCM)
5. Monitor terminal logs for errors

For production testing:
- Change BASE_URL to your Render URL
- Run this script from your local machine
- Verify all notifications arrive within 30 seconds
""")
#