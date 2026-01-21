import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import requests
import json
import random
import time
import schedule
import pytz
import threading
import os
import sys
from datetime import datetime, timedelta

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
MORNING_WINDOW_MAX = 59
WORK_HOURS_TARGET = 9    
OVERTIME_MIN = 10        
OVERTIME_MAX = 16        

def load_users():
    if not os.path.exists(DATABASE_FILE):
        return {}
    try:
        with open(DATABASE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_user(chat_id, email, password, name="Unknown", auto_mode=True):
    users = load_users()
    current_auto = users.get(str(chat_id), {}).get("auto_mode", True)
    
    users[str(chat_id)] = {
        "email": email, 
        "password": password, 
        "name": name,
        "auto_mode": auto_mode if auto_mode is not None else current_auto
    }
    with open(DATABASE_FILE, 'w') as f:
        json.dump(users, f, indent=4)

def toggle_auto_mode(chat_id):
    users = load_users()
    if str(chat_id) in users:
        new_val = not users[str(chat_id)].get("auto_mode", True)
        users[str(chat_id)]["auto_mode"] = new_val
        with open(DATABASE_FILE, 'w') as f:
            json.dump(users, f, indent=4)
        return new_val
    return False

def get_ub_time():
    return datetime.now(UB_TZ)

def log(message):
    timestamp = get_ub_time().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def send_telegram(chat_id, message):
    try:
        bot.send_message(chat_id, message, parse_mode="Markdown")
    except Exception as e:
        log(f"Telegram Send Error: {e}")

def main_menu_keyboard():
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(
        InlineKeyboardButton("🟢 Ирэх (Check In)", callback_data="btn_in"),
        InlineKeyboardButton("🔴 Явах (Check Out)", callback_data="btn_out")
    )
    markup.add(InlineKeyboardButton("📊 Төлөв (Status)", callback_data="btn_status"))
    markup.add(InlineKeyboardButton("⚙️ Тохиргоо (Settings)", callback_data="btn_settings"))
    return markup

def settings_keyboard(chat_id):
    users = load_users()
    user = users.get(str(chat_id), {})
    auto_status = "✅ ON" if user.get("auto_mode", True) else "❌ OFF"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(f"Автомат бүртгэл: {auto_status}", callback_data="toggle_auto"))
    markup.add(InlineKeyboardButton("🔙 Буцах", callback_data="back_home"))
    return markup

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
    except Exception as e:
        return None, str(e)

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
    except:
        return None

def get_real_checkin_checkout_time(calendar_result):
    try:
        today_str = get_ub_time().strftime("%Y-%m-%d")
        if not calendar_result: return None, None
        
        raw_json = calendar_result[0].get("attendance_calendar_json", {})
        weeks = raw_json.get("weeks", [])
        
        for week in weeks:
            for day_obj in week:
                if day_obj.get("day") == today_str:
                    time_str = day_obj.get("day_data", {}).get("in_out", "")
                    if not time_str: return None, None
                    
                    parts = time_str.split("-")
                    
                    check_in_dt = None
                    check_out_dt = None
                    
                    if len(parts) >= 1:
                        check_in_str = parts[0].strip()
                        if check_in_str:
                            full_in_str = f"{today_str} {check_in_str}"
                            try:
                                if ":" in check_in_str and check_in_str.count(":") == 2:
                                    check_in_dt = datetime.strptime(full_in_str, "%Y-%m-%d %H:%M:%S")
                                else:
                                    check_in_dt = datetime.strptime(full_in_str, "%Y-%m-%d %H:%M")
                            except ValueError:
                                pass 
                    if len(parts) >= 2:
                        check_out_str = parts[1].strip()
                        if check_out_str:
                             full_out_str = f"{today_str} {check_out_str}"
                             try:
                                if ":" in check_out_str and check_out_str.count(":") == 2:
                                    check_out_dt = datetime.strptime(full_out_str, "%Y-%m-%d %H:%M:%S")
                                else:
                                    check_out_dt = datetime.strptime(full_out_str, "%Y-%m-%d %H:%M")
                             except ValueError:
                                 pass 

                    return check_in_dt, check_out_dt
        return None, None
    except Exception as e:
        log(f"Parser Error: {e}")
        return None, None

def execute_punch(chat_id, action_type, message_id=None):
    users = load_users()
    user = users.get(str(chat_id))
    
    if not user:
        if message_id:
            bot.edit_message_text("❌ Бүртгэл олдсонгүй. /register ашиглана уу.", chat_id, message_id)
        return

    time.sleep(random.randint(1, 3))

    session, user_data = get_session(user['email'], user['password'])
    
    if not session:
        msg = f"❌ **Нэвтрэх амжилтгүй** ({user['email']})"
        if message_id:
            bot.edit_message_text(msg, chat_id, message_id)
        else:
            send_telegram(chat_id, msg)
        return

    cal_data = get_calendar_data(session, user_data['uid'])
    is_checked_in = cal_data[0].get("checked_in_today", False) if cal_data else False
    
    if action_type == "check_in" and is_checked_in:
        log(f"Skip In: {user['email']} (Already In)")
        msg = "⚠️ Та аль хэдийн ИРСЭН байна."
        if message_id:
            bot.edit_message_text(msg, chat_id, message_id)
        else:
            send_telegram(chat_id, msg)
        return

    if action_type == "check_out" and not is_checked_in:
        log(f"Skip Out: {user['email']} (Already Out)")
        msg = "⚠️ Та аль хэдийн ЯВСАН байна."
        if message_id:
            bot.edit_message_text(msg, chat_id, message_id)
        else:
            send_telegram(chat_id, msg)
        return

    method = "check_out" if action_type == "check_out" else "check_in"
    
    payload = {
        "id": random.randint(10, 1000),
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "args": [[1]], 
            "kwargs": {"context": {"tz": "Asia/Ulaanbaatar", "uid": user_data['uid'], "allowed_company_ids": [1]}},
            "method": method, 
            "model": "hr.attendance.calendar"
        }
    }
    
    try:
        r = session.post(CALL_BUTTON_URL, json=payload, headers={"Content-Type": "application/json", "Referer": BASE_URL}, timeout=30)
        resp = r.json()
        
        current_time = get_ub_time().strftime("%H:%M")
        
        if "error" not in resp:
            action_mongolian = "Ирсэн" if action_type == "check_in" else "Явсан"
            success_msg = f"✅ **Амжилттай!**\n👤 {user.get('name', user['email'])}\nтөлөв: *{action_mongolian}*\n⏰ Цаг: `{current_time}`"
            
            log(f"Success {action_type}: {user['email']}")
            
            if message_id:
                bot.edit_message_text(success_msg, chat_id, message_id, parse_mode="Markdown")
            else:
                send_telegram(chat_id, success_msg)
        else:
            err_msg = f"⚠️ Odoo Error: {resp['error']['message']}"
            if message_id:
                bot.edit_message_text(err_msg, chat_id, message_id)
            else:
                send_telegram(chat_id, err_msg)
                
    except Exception as e:
        log(f"Request Error: {e}")
        if message_id:
            bot.edit_message_text("❌ Сүлжээний алдаа гарлаа.", chat_id, message_id)

def plan_checkout_strategy():
    log("Running Mid-Day Checkout Planner")
    users = load_users()
    ub_now = get_ub_time()
    
    if ub_now.weekday() >= 5: return 

    for chat_id, user in users.items():
        if not user.get("auto_mode", True):
            continue

        session, u_data = get_session(user['email'], user['password'])
        if not session: continue
        
        cal_data = get_calendar_data(session, u_data['uid'])
        actual_in_dt, _ = get_real_checkin_checkout_time(cal_data)
        
        if actual_in_dt:
            buffer_mins = random.randint(OVERTIME_MIN, OVERTIME_MAX)
            target_out_dt = actual_in_dt + timedelta(hours=WORK_HOURS_TARGET, minutes=buffer_mins)
            
            target_out_dt = target_out_dt.replace(year=ub_now.year, month=ub_now.month, day=ub_now.day)
            
            out_str = target_out_dt.strftime("%H:%M:%S")
            in_str = actual_in_dt.strftime("%H:%M:%S")
            
            schedule.every().day.at(out_str).do(execute_punch, chat_id=chat_id, action_type="check_out").tag('daily')
            
            log(f"User {user['email']}: In at {in_str}. Scheduled Out: {out_str}")
            send_telegram(chat_id, f"📅 **Өдрийн Төлөвлөгөө**\n✅ Ирсэн: `{in_str}`\n🎯 Явах цаг: `{out_str}`\n(9 цаг + {buffer_mins}мин)")
        else:
            fallback_time = f"18:{random.randint(15, 45):02d}:00"
            
            schedule.every().day.at(fallback_time).do(execute_punch, chat_id=chat_id, action_type="check_out").tag('daily')
            
            log(f"⚠️ No Check-in found for {user['email']}. Fallback Out set for {fallback_time}")
            send_telegram(chat_id, f"⚠️ **Анхааруулга**\nӨглөөний бүртгэл олдсонгүй. Автомат гарах цагийг `{fallback_time}`-д тохирууллаа.\nТа өөрөө шалгаарай.")

def schedule_daily_tasks():
    schedule.clear('daily')
    ub_now = get_ub_time()
    
    if ub_now.weekday() >= 5: 
        log("Weekend Mode. No tasks scheduled.")
        return

    users = load_users()
    log(f"Scheduling tasks for {len(users)} users...")
    
    for chat_id, user in users.items():
        if not user.get("auto_mode", True):
            log(f"Skipping {user['email']} (Auto Mode OFF)")
            continue

        m_min = random.randint(MORNING_WINDOW_MIN, MORNING_WINDOW_MAX)
        m_sec = random.randint(0, 59)
        m_time = f"08:{m_min:02d}:{m_sec:02d}"
        
        schedule.every().day.at(m_time).do(execute_punch, chat_id=chat_id, action_type="check_in").tag('daily')
        log(f"Scheduled In for {user['email']}: {m_time}")

    schedule.every().day.at("11:00:00").do(plan_checkout_strategy).tag('daily')
    
    current_time_str = ub_now.strftime("%H:%M")
    
    if "08:30" < current_time_str < "09:00":
        for chat_id, user in users.items():
            if user.get("auto_mode", True):
                threading.Thread(target=execute_punch, args=(chat_id, "check_in")).start()
    
    if current_time_str >= "11:00":
        threading.Thread(target=plan_checkout_strategy).start()

@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    bot.send_message(
        message.chat.id, 
        "👋 **Сайн байна уу, **\nДоорх цэснээс сонгоно уу:", 
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['register'])
def register_command(message):
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(message, "⚠️ Буруу формат.\nЖишээ: `/register email@tavanbogd.com password`", parse_mode="Markdown")
            return

        email, pwd = parts[1], parts[2]
        bot.reply_to(message, "🔄 Шалгаж байна...")
        
        s, d = get_session(email, pwd)
        if s:
            save_user(message.chat.id, email, pwd, d['name'])
            bot.send_message(message.chat.id, f"✅ **Бүртгэл амжилттай!**\nХэрэглэгч: {d['name']}\nАвтомат горим идэвхжлээ.", reply_markup=main_menu_keyboard(), parse_mode="Markdown")
            
            threading.Thread(target=schedule_daily_tasks).start()
        else:
            bot.reply_to(message, "❌ Нэвтрэх нэр эсвэл нууц үг буруу байна.")
    except Exception as e:
        log(f"Register Error: {e}")

@bot.callback_query_handler(func=lambda call: True)
def handle_query(call):
    chat_id = call.message.chat.id
    
    if call.data == "btn_status":
        bot.answer_callback_query(call.id, "Мэдээлэл татаж байна...")
        users = load_users()
        user = users.get(str(chat_id))
        
        if not user:
            bot.send_message(chat_id, "❌ Та бүртгүүлээгүй байна.")
            return

        session, u_data = get_session(user['email'], user['password'])
        if session:
            cal_data = get_calendar_data(session, u_data['uid'])
            is_checked_in = cal_data[0].get("checked_in_today", False) if cal_data else False
            
            real_in, real_out = get_real_checkin_checkout_time(cal_data)
            
            state_icon = "🟢" if is_checked_in else "🔴"
            state_text = "Ирсэн" if is_checked_in else "Явсан"
            in_time_text = real_in.strftime("%H:%M:%S") if real_in else "--:--:--"
            out_time_text = real_out.strftime("%H:%M:%S") if real_out else "--:--:--"
            
            current_dt = get_ub_time().strftime("%Y-%m-%d %H:%M")
            
            msg = (
                f"📅 **{current_dt}**\n\n"
                f"👤 **{u_data['name']}**\n"
                f"Төлөв: {state_icon} *{state_text}*\n"
                f"🟢 Ирсэн: `{in_time_text}`\n"
                f"🔴 Явсан: `{out_time_text}`"
            )
            bot.send_message(chat_id, msg, parse_mode="Markdown")
        else:
            bot.send_message(chat_id, "❌ Нэвтрэх боломжгүй байна.")

    elif call.data == "btn_in":
        bot.answer_callback_query(call.id, "Боловсруулж байна...")
        msg = bot.send_message(chat_id, "⏳ Ирц бүртгэж байна...")
        threading.Thread(target=execute_punch, args=(chat_id, "check_in", msg.message_id)).start()

    elif call.data == "btn_out":
        bot.answer_callback_query(call.id, "Боловсруулж байна...")
        msg = bot.send_message(chat_id, "⏳ Ирц бүртгэж байна...")
        threading.Thread(target=execute_punch, args=(chat_id, "check_out", msg.message_id)).start()

    elif call.data == "btn_settings":
        bot.edit_message_text("⚙️ **Тохиргоо**", chat_id, call.message.message_id, reply_markup=settings_keyboard(chat_id), parse_mode="Markdown")

    elif call.data == "toggle_auto":
        new_status = toggle_auto_mode(chat_id)
        status_text = "Идэвхжүүллээ ✅" if new_status else "Идэвхгүй ❌"
        bot.answer_callback_query(call.id, f"Автомат горим: {status_text}")
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=settings_keyboard(chat_id))
        threading.Thread(target=schedule_daily_tasks).start()

    elif call.data == "back_home":
        bot.delete_message(chat_id, call.message.message_id)
        send_menu(call.message)

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
    
    log("--- STARTED ---")
    
    while True:
        try:
            bot.polling(non_stop=True, interval=1, timeout=20)
        except Exception as e:
            log(f"Bot Polling Error: {e}")
            time.sleep(5)