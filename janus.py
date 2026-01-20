import telebot
import requests
import json
import random
import time
import schedule
import pytz
import threading
import os
from datetime import datetime, timedelta
import sys

BASE_URL = "https://erp.tbsolutions.mn"
AUTH_URL = f"{BASE_URL}/web/session/authenticate"
CALL_BUTTON_URL = f"{BASE_URL}/web/dataset/call_button"
CALENDAR_READ_URL = f"{BASE_URL}/web/dataset/call_kw/hr.attendance.calendar/read"
DB_NAME = "prod_tbs240122"

API_TOKEN = '8472658405:AAGwMoGkZTfH4O7oV89HhEkjmj6dyPDNhwA'
bot = telebot.TeleBot(API_TOKEN)

UB_TZ = pytz.timezone('Asia/Ulaanbaatar')
DATABASE_FILE = "janus_users.json"

MORNING_WINDOW_MIN = 30 
MORNING_WINDOW_MAX = 45 
WORK_HOURS_TARGET = 9   
OVERTIME_MIN = 10       
OVERTIME_MAX = 15       

def load_users():
    if not os.path.exists(DATABASE_FILE): return {}
    try:
        with open(DATABASE_FILE, 'r') as f: return json.load(f)
    except: return {}

def save_user(chat_id, email, password, name="Unknown"):
    users = load_users()
    users[str(chat_id)] = {"email": email, "password": password, "name": name}
    with open(DATABASE_FILE, 'w') as f: json.dump(users, f, indent=4)

def log(message):
    timestamp = datetime.now(UB_TZ).strftime('%H:%M:%S')
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def send_telegram(chat_id, message):
    try:
        bot.send_message(chat_id, message, parse_mode="Markdown")
    except:
        log(f"Telegram Fail: {message}")

def get_session(email, password):
    s = requests.Session()
    payload = {
        "jsonrpc": "2.0", "method": "call",
        "params": {"db": DB_NAME, "login": email, "password": password},
        "id": random.randint(1, 1000)
    }
    try:
        r = s.post(AUTH_URL, json=payload, timeout=20)
        res = r.json().get("result", {})
        if "uid" in res:
            return s, {"uid": res.get("uid"), "name": res.get("name", "User")}
        return None, "Login Failed"
    except Exception as e: return None, str(e)

def get_calendar_data(session, uid):
    payload = {
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "model": "hr.attendance.calendar",
            "method": "read",
            "args": [[1], ["checked_in_today", "attendance_calendar_json", "name"]],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Ulaanbaatar", "uid": uid, "allowed_company_ids": [1], "compute_calendar_month": True}}
        },
        "id": random.randint(1, 1000)
    }
    try:
        r = session.post(CALENDAR_READ_URL, json=payload, headers={"Content-Type": "application/json", "Referer": BASE_URL}, timeout=20)
        return r.json().get("result", [])
    except: return None

def get_real_checkin_time(calendar_result):
    try:
        today_str = datetime.now(UB_TZ).strftime("%Y-%m-%d")
        if not calendar_result: return None
        
        raw_json = calendar_result[0].get("attendance_calendar_json", {})
        weeks = raw_json.get("weeks", [])
        
        for week in weeks:
            for day_obj in week:
                if day_obj.get("day") == today_str:
                    time_str = day_obj.get("day_data", {}).get("in_out", "")
                    if not time_str: return None
                    
                    check_in_str = time_str.split(" - ")[0].strip()
                    if ":" in check_in_str:
                        return datetime.strptime(f"{today_str} {check_in_str}", "%Y-%m-%d %H:%M:%S")
        return None
    except Exception as e:
        log(f"Parser Error: {e}")
        return None

def execute_punch(chat_id, action_type):
    users = load_users()
    user = users.get(str(chat_id))
    if not user: return

    time.sleep(random.randint(2, 15))

    session, user_data = get_session(user['email'], user['password'])
    if not session:
        send_telegram(chat_id, f"❌ **Login Failed** during {action_type}")
        return

    cal_data = get_calendar_data(session, user_data['uid'])
    is_checked_in = cal_data[0].get("checked_in_today", False) if cal_data else False
    
    if action_type == "check_in" and is_checked_in:
        log(f"Skip In: {user['email']} (Already In)")
        return
    if action_type == "check_out" and not is_checked_in:
        log(f"Skip Out: {user['email']} (Already Out)")
        return

    method = "check_out" if action_type == "check_out" else "check_in"
    payload = {
        "id": random.randint(10, 1000),
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "args": [[1]], 
            "kwargs": {"context": {"tz": "Asia/Ulaanbaatar", "uid": user_data['uid'], "allowed_company_ids": [1]}},
            "method": method, "model": "hr.attendance.calendar"
        }
    }
    
    try:
        r = session.post(CALL_BUTTON_URL, json=payload, headers={"Content-Type": "application/json", "Referer": BASE_URL}, timeout=30)
        if "error" not in r.json():
            send_telegram(chat_id, f"✅ **{action_type.replace('_',' ').title()} Success!**")
            log(f"Success {action_type}: {user['email']}")
        else:
            send_telegram(chat_id, f"⚠️ Odoo Error: {r.json()['error']['message']}")
    except Exception as e:
        log(f"Req Error: {e}")

def plan_checkout_strategy():
    log("Running Mid-Day Checkout Planner...")
    users = load_users()
    ub_now = datetime.now(UB_TZ)
    
    if ub_now.weekday() >= 5: return 

    for chat_id, user in users.items():
        session, u_data = get_session(user['email'], user['password'])
        if not session: continue
        
        cal_data = get_calendar_data(session, u_data['uid'])
        actual_in_dt = get_real_checkin_time(cal_data)
        
        if actual_in_dt:
            buffer_mins = random.randint(OVERTIME_MIN, OVERTIME_MAX)
            target_out_dt = actual_in_dt + timedelta(hours=WORK_HOURS_TARGET, minutes=buffer_mins)
            
            out_str = target_out_dt.strftime("%H:%M:%S")
            in_str = actual_in_dt.strftime("%H:%M:%S")
            
            schedule.every().day.at(out_str).do(execute_punch, chat_id=chat_id, action_type="check_out").tag('daily')
            
            log(f"User {user['email']}: In at {in_str}. Out set for {out_str} (+{buffer_mins}m buffer)")
            send_telegram(chat_id, f"📅 **Day Plan**\n✅ In: `{in_str}`\n🎯 Target Out: `{out_str}`\n(8h + {buffer_mins}m buffer)")
        else:
            fallback_time = f"18:{random.randint(10, 30):02d}:00"
            schedule.every().day.at(fallback_time).do(execute_punch, chat_id=chat_id, action_type="check_out").tag('daily')
            log(f"⚠️ No Check-in found for {user['email']}. Fallback Out set for {fallback_time}")

def schedule_daily_tasks():
    schedule.clear('daily')
    ub_now = datetime.now(UB_TZ)
    
    if ub_now.weekday() >= 5: 
        log("Weekend Mode. No tasks.")
        return

    users = load_users()
    for chat_id, user in users.items():
        m_min = random.randint(MORNING_WINDOW_MIN, MORNING_WINDOW_MAX)
        m_sec = random.randint(0, 59)
        m_time = f"08:{m_min:02d}:{m_sec:02d}"
        
        schedule.every().day.at(m_time).do(execute_punch, chat_id=chat_id, action_type="check_in").tag('daily')
        log(f"Scheduled In for {user['email']}: {m_time}")

    schedule.every().day.at("11:00:00").do(plan_checkout_strategy).tag('daily')
    
    current_time_str = ub_now.strftime("%H:%M")
    if current_time_str > "08:45" and current_time_str < "11:00":
        for chat_id in users:
            threading.Thread(target=execute_punch, args=(chat_id, "check_in")).start()
    
    if current_time_str > "11:00":
        threading.Thread(target=plan_checkout_strategy).start()

@bot.message_handler(commands=['start'])
def welcome(m): bot.reply_to(m, "Janus v2.3 Active.\nUse /register email pass")

@bot.message_handler(commands=['register'])
def reg(m):
    try:
        email, pwd = m.text.split()[1], m.text.split()[2]
        if get_session(email, pwd)[0]:
            save_user(m.chat.id, email, pwd)
            bot.reply_to(m, "Registered.")
            schedule_daily_tasks()
        else: bot.reply_to(m, "Login Failed.")
    except: pass

@bot.message_handler(commands=['status'])
def stat(m):
    users = load_users()
    u = users.get(str(m.chat.id))
    if not u: return
    s, d = get_session(u['email'], u['password'])
    if s:
        cal = get_calendar_data(s, d['uid'])
        real_in = get_real_checkin_time(cal)
        state = "Checked In" if cal[0]['checked_in_today'] else "Checked Out"
        time_txt = real_in.strftime("%H:%M:%S") if real_in else "--:--:--"
        bot.reply_to(m, f"Status: {state}\nIn Time: `{time_txt}`", parse_mode="Markdown")

@bot.message_handler(commands=['in'])
def force_in(m): execute_punch(m.chat.id, "check_in")

@bot.message_handler(commands=['out'])
def force_out(m): execute_punch(m.chat.id, "check_out")

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule.every().day.at("01:00").do(schedule_daily_tasks)
    schedule_daily_tasks()
    
    t = threading.Thread(target=run_scheduler)
    t.daemon = True
    t.start()
    
    print("--- JANUS v2.3 REACTIVE ---")
    while True:
        try:
            bot.polling(non_stop=True, interval=2, timeout=30)
        except Exception as e:
            log(f"Bot crash: {e}")
            time.sleep(5)