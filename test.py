import requests
import json
import random
import time
import schedule
import pytz
from datetime import datetime, timedelta
import sys

BASE_URL = "https://erp.tbsolutions.mn"
AUTH_URL = f"{BASE_URL}/web/session/authenticate"
CALL_BUTTON_URL = f"{BASE_URL}/web/dataset/call_button"
SEARCH_READ_URL = f"{BASE_URL}/web/dataset/call_kw/hr.employee/search_read"

USERNAME = "gantulga.b@tavanbogd.com"
PASSWORD = "Thelofx123!"
DB_NAME = "prod_tbs240122"

TELEGRAM_BOT_TOKEN = "8472658405:AAGwMoGkZTfH4O7oV89HhEkjmj6dyPDNhwA"
TELEGRAM_CHAT_ID = "6190430690"

UB_TZ = pytz.timezone('Asia/Ulaanbaatar')

def log(message, overwrite=False):
    timestamp = datetime.now(UB_TZ).strftime('%H:%M:%S')
    msg = f"[{timestamp}] {message}"
    
    if overwrite:
        sys.stdout.write(f"\r{msg}          ")
        sys.stdout.flush()
    else:
        if overwrite is False: sys.stdout.write("\n")
        print(msg)

def send_telegram(message):
    """
    Robust Telegram Sender with Retry Logic.
    Tries 3 times to send the message. If it fails, it just logs locally.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    
    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code == 200:
                return 
            elif r.status_code >= 500:
                log(f"⚠️ Telegram Server Error ({r.status_code}). Retrying ({attempt}/3)...")
                time.sleep(2)
            else:
                log(f"⚠️ Telegram Client Error: {r.text}")
                return 
        except Exception as e:
            log(f"⚠️ Telegram Connection Fail. Retrying ({attempt}/3)...")
            time.sleep(2)
    
    log("❌ Telegram failed after 3 attempts. Message skipped.")

def get_session():
    s = requests.Session()
    payload = {
        "jsonrpc": "2.0", "method": "call",
        "params": {"db": DB_NAME, "login": USERNAME, "password": PASSWORD},
        "id": random.randint(1, 1000)
    }
    try:
        r = s.post(AUTH_URL, json=payload, timeout=30)
        data = r.json()
        if "error" in data:
            log(f"Login Fail: {data['error']['message']}")
            send_telegram(f"⚠️ Login Failed: {data['error']['message']}")
            return None, None
        return s, data.get("result", {}).get("uid")
    except Exception as e:
        log(f"Net Error: {e}")
        return None, None

def get_current_status(session, uid):
    payload = {
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "model": "hr.employee",
            "method": "search_read",
            "args": [[['user_id', '=', uid]]],
            "kwargs": {"fields": ["attendance_state"], "limit": 1}
        },
        "id": random.randint(1, 1000)
    }
    headers = {"Content-Type": "application/json", "Referer": BASE_URL}
    try:
        r = session.post(SEARCH_READ_URL, json=payload, headers=headers, timeout=20)
        data = r.json()
        if "result" in data and len(data["result"]) > 0:
            return data["result"][0].get("attendance_state")
    except Exception as e:
        log(f"Status Read Error: {e}")
    return None

def punch_clock(action_type):
    sys.stdout.write("\n")
    log(f"⏰ Executing {action_type}...")
    
    session, uid = get_session()
    if not session: return

    current_state = get_current_status(session, uid)
    
    if action_type == "check_in" and current_state == "checked_in":
        log("⚠️ Already Checked In. Skipping.")
        send_telegram("⚠️ Triggered Check-in, but already Checked In.")
        return
    
    if action_type == "check_out" and current_state == "checked_out":
        log("⚠️ Already Checked Out. Skipping.")
        send_telegram("⚠️ Triggered Check-out, but already Checked Out.")
        return

    time.sleep(random.randint(2, 10))

    payload = {
        "id": random.randint(10, 1000),
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "args": [[1]], 
            "kwargs": {"context": {"compute_calendar_month": True, "lang": "en_US", "tz": "Asia/Ulaanbaatar", "uid": uid, "allowed_company_ids": [1]}},
            "method": action_type, "model": "hr.attendance.calendar"
        }
    }
    
    headers = {"Content-Type": "application/json", "Referer": BASE_URL, "User-Agent": "Mozilla/5.0"}

    try:
        r = session.post(CALL_BUTTON_URL, json=payload, headers=headers, timeout=30)
        if "error" not in r.json():
            msg = f"✅ Success: {action_type.replace('_', ' ').title()}"
            log(msg)
            send_telegram(msg)
        else:
            err = r.json()['error']['message']
            log(f"❌ Odoo Error: {err}")
            send_telegram(f"⚠️ Odoo Error: {err}")
    except Exception as e:
        log(f"Req Error: {e}")

def daily_scheduler():
    schedule.clear('tasks')
    ub_now = datetime.now(UB_TZ)
    
    if ub_now.weekday() >= 5:
        log(f"Weekend ({ub_now.strftime('%A')}). Idling...")
        send_telegram("🏖 Weekend Mode. No actions.")
        return

    m_hour, m_min = 8, random.randint(30, 35)
    e_hour, e_min = 18, random.randint(10, 20)
    
    morning_str = f"{m_hour:02d}:{m_min:02d}"
    evening_str = f"{e_hour:02d}:{e_min:02d}"

    log(f"Target: In@{morning_str}, Out@{evening_str}")

    today_morning = ub_now.replace(hour=m_hour, minute=m_min, second=0, microsecond=0)
    today_evening = ub_now.replace(hour=e_hour, minute=e_min, second=0, microsecond=0)

    if ub_now > today_morning and ub_now < today_evening:
        log("⚠️ Late Start Detected. Checking Status...")
        session, uid = get_session()
        if session:
            state = get_current_status(session, uid)
            if state == "checked_out":
                log("🚨 MISSED MORNING! Recovering...")
                send_telegram("🚨 **Missed Morning.** Recovering...")
                punch_clock("check_in")
            else:
                log("Status OK.")
    
    elif ub_now > today_evening:
         log("⚠️ Late Evening Detected. Checking Status...")
         session, uid = get_session()
         if session:
            state = get_current_status(session, uid)
            if state == "checked_in":
                log("🚨 MISSED EVENING! Recovering...")
                send_telegram("🚨 **Missed Evening.** Recovering...")
                punch_clock("check_out")

    schedule.every().day.at(morning_str).do(punch_clock, action_type="check_in").tag('tasks')
    schedule.every().day.at(evening_str).do(punch_clock, action_type="check_out").tag('tasks')
    
    send_telegram(f"📅 **Plan for {ub_now.strftime('%A')}**\nTarget In: `{morning_str}`\nTarget Out: `{evening_str}`")

if __name__ == "__main__":
    log("System Init. Checking State & Schedule...")
    daily_scheduler()
    schedule.every().day.at("01:00").do(daily_scheduler)
    
    while True:
        schedule.run_pending()
        try:
            next_run = schedule.next_run()
            if next_run:
                delta = next_run - datetime.now()
                minutes = int(delta.total_seconds() / 60)
                log(f"Status: Armed. Next: {minutes} min", overwrite=True)
            else:
                log("Status: Idle (Tasks done or Weekend)", overwrite=True)
        except:
            pass
        time.sleep(1)