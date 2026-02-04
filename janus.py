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

MORNING_START = (8, 30)
MORNING_END = (9, 30)


WORK_HOURS_BASE = 9            

CHECKOUT_JITTER_MIN = 0        

CHECKOUT_JITTER_MAX = 30       

CHECKOUT_JITTER_MODE = 10      

CHECKOUT_FLOOR = (17, 30)      

CHECKOUT_CEILING = (19, 30)    

FALLBACK_OUT_START = (18, 0)   

FALLBACK_OUT_END = (19, 0)

_file_lock = threading.RLock()

_user_schedule = {}
_schedule_lock = threading.Lock()


def load_users():
    with _file_lock:
        if not os.path.exists(DATABASE_FILE):
            return {}
        try:
            with open(DATABASE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}


def save_user(chat_id, email, password, name="Unknown", auto_mode=True):
    with _file_lock:
        users = {}
        if os.path.exists(DATABASE_FILE):
            try:
                with open(DATABASE_FILE, 'r') as f:
                    users = json.load(f)
            except Exception:
                users = {}

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
    with _file_lock:
        users = {}
        if os.path.exists(DATABASE_FILE):
            try:
                with open(DATABASE_FILE, 'r') as f:
                    users = json.load(f)
            except Exception:
                return False

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


def hm_to_secs(h, m):
    return h * 3600 + m * 60


def secs_to_hms(total_secs):
    total_secs = max(0, min(total_secs, 86399))
    h, rem = divmod(total_secs, 3600)
    m, s = divmod(rem, 60)
    return int(h), int(m), int(s)


def humanized_random_time(start_h, start_m, end_h, end_m):
    start_secs = hm_to_secs(start_h, start_m)
    end_secs = hm_to_secs(end_h, end_m)

    mid = (start_secs + end_secs) / 2
    rand_secs = int(random.triangular(start_secs, end_secs, mid))

    h, m, s = secs_to_hms(rand_secs)

    if random.random() < 0.35:
        m = (m // 5) * 5

    s = random.randint(3, 57)

    return f"{h:02d}:{m:02d}:{s:02d}"


def user_tag(chat_id):
    return f"user_{chat_id}"


def set_schedule(chat_id, key, value):
    with _schedule_lock:
        today = get_ub_time().strftime("%Y-%m-%d")
        if str(chat_id) not in _user_schedule or _user_schedule[str(chat_id)].get("date") != today:
            _user_schedule[str(chat_id)] = {"date": today}
        _user_schedule[str(chat_id)][key] = value


def get_schedule(chat_id):
    with _schedule_lock:
        today = get_ub_time().strftime("%Y-%m-%d")
        data = _user_schedule.get(str(chat_id))
        if data and data.get("date") == today:
            return data
        return None


def clear_schedule(chat_id):
    with _schedule_lock:
        _user_schedule.pop(str(chat_id), None)


def main_menu_keyboard():
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(
        InlineKeyboardButton("🟢 Ирэх (Check In)", callback_data="btn_in"),
        InlineKeyboardButton("🔴 Явах (Check Out)", callback_data="btn_out")
    )
    markup.add(InlineKeyboardButton("📊 Төлөв (Status)", callback_data="btn_status"))
    markup.add(InlineKeyboardButton("🤖 Хуваарь (Schedule)", callback_data="btn_schedule"))
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
    today = get_ub_time()
    today_str = today.strftime("%Y-%m-%d")
    
    payload = {
        "jsonrpc": "2.0", "method": "call",
        "params": {
            "model": "hr.attendance.calendar",
            "method": "read",
            "args": [[1], ["checked_in_today", "attendance_calendar_json", "name"]],
            "kwargs": {
                "context": {
                    "lang": "en_US", "tz": "Asia/Ulaanbaatar",
                    "uid": uid, "allowed_company_ids": [1],
                    "compute_calendar_month": True,
                    "calendar_month": today_str, 
                    "default_date": today_str,   
                }
            }
        },
        "id": random.randint(1, 1000)
    }
    try:
        r = session.post(
            CALENDAR_READ_URL, json=payload,
            headers={"Content-Type": "application/json", "Referer": BASE_URL},
            timeout=30 
        )
        result = r.json().get("result", [])
        
        if result:
            cal_json = result[0].get("attendance_calendar_json", {})
            weeks = cal_json.get("weeks", [])
            if weeks:
                first_date = None
                last_date = None
                for week in weeks:
                    for day in week:
                        d = day.get("day")
                        if d:
                            if not first_date:
                                first_date = d
                            last_date = d
                if first_date and last_date:
                    log(f"Calendar API returned dates: {first_date} to {last_date} (looking for {today_str})")
        
        return result
    except Exception as e:
        log(f"Calendar API Error: {e}")
        return None


def get_real_checkin_checkout_time(calendar_result):
    try:
        today_str = get_ub_time().strftime("%Y-%m-%d")
        if not calendar_result:
            log("Calendar result is empty/None")
            return None, None

        raw_json = calendar_result[0].get("attendance_calendar_json", {})
        
        if not raw_json:
            log("attendance_calendar_json is empty")
            return None, None
            
        weeks = raw_json.get("weeks", [])
        
        if not weeks:
            log("No weeks data in calendar")
            return None, None

        all_dates = []
        for week in weeks:
            for day_obj in week:
                day_date = day_obj.get("day")
                if day_date:
                    all_dates.append(day_date)

        for week in weeks:
            for day_obj in week:
                if day_obj.get("day") == today_str:
                    day_data = day_obj.get("day_data", {})
                    time_str = day_data.get("in_out", "")
                    
                    log(f"Found today's data: day_data={day_data}, in_out='{time_str}'")
                    
                    if not time_str:
                        log(f"No in_out string, full day_data: {day_data}")
                        return None, None

                    parts = time_str.split("-")
                    check_in_dt = None
                    check_out_dt = None

                    if len(parts) >= 1:
                        raw = parts[0].strip()
                        if raw:
                            full = f"{today_str} {raw}"
                            try:
                                fmt = "%Y-%m-%d %H:%M:%S" if raw.count(":") == 2 else "%Y-%m-%d %H:%M"
                                check_in_dt = datetime.strptime(full, fmt)
                            except ValueError as e:
                                log(f"Parse error for check-in '{raw}': {e}")

                    if len(parts) >= 2:
                        raw = parts[1].strip()
                        if raw:
                            full = f"{today_str} {raw}"
                            try:
                                fmt = "%Y-%m-%d %H:%M:%S" if raw.count(":") == 2 else "%Y-%m-%d %H:%M"
                                check_out_dt = datetime.strptime(full, fmt)
                            except ValueError as e:
                                log(f"Parse error for check-out '{raw}': {e}")

                    return check_in_dt, check_out_dt
        
        log(f"Today ({today_str}) not found in calendar weeks. Available dates: {all_dates[:7]}...{all_dates[-7:] if len(all_dates) > 7 else ''}")
        return None, None
    except Exception as e:
        log(f"Parser Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def execute_punch(chat_id, action_type, message_id=None, source="scheduled"):
    users = load_users()
    user = users.get(str(chat_id))

    if not user:
        if message_id:
            bot.edit_message_text(
                "❌ Бүртгэл олдсонгүй. /register ашиглана уу.",
                chat_id, message_id
            )
        return

    email = user.get('email', 'unknown')
    ub_now = get_ub_time()
    current_hour = ub_now.hour
    current_time = ub_now.strftime('%H:%M:%S')
    
    log(f"execute_punch called: {email}, action={action_type}, source={source}, time={current_time}")
    
    if action_type == "check_out" and message_id is None: 
        if current_hour < 17:
            log(f"🚨 BLOCKED early checkout for {email} at {current_time} - too early! (source={source})")
            send_telegram(
                chat_id,
                f"🚨 **Автомат checkout ЗОГСООВ!**\n"
                f"Цаг: `{current_time}`\n"
                f"Шалтгаан: 17:00-с өмнө checkout хийх боломжгүй.\n\n"
                f"Хэрэв гараар хийх бол Check Out товчийг дарна уу."
            )
            return

    time.sleep(random.uniform(2.0, 10.0))

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
            "kwargs": {
                "context": {
                    "tz": "Asia/Ulaanbaatar",
                    "uid": user_data['uid'],
                    "allowed_company_ids": [1]
                }
            },
            "method": method,
            "model": "hr.attendance.calendar"
        }
    }

    try:
        r = session.post(
            CALL_BUTTON_URL, json=payload,
            headers={"Content-Type": "application/json", "Referer": BASE_URL},
            timeout=30
        )
        resp = r.json()

        current_time = get_ub_time().strftime("%H:%M")

        if "error" not in resp:
            action_mongolian = "Ирсэн" if action_type == "check_in" else "Явсан"
            success_msg = (
                f"✅ **Амжилттай!**\n"
                f"👤 {user.get('name', user['email'])}\n"
                f"төлөв: *{action_mongolian}*\n"
                f"⏰ Цаг: `{current_time}`"
            )
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


def unschedule_user(chat_id):
    tag = user_tag(chat_id)
    schedule.clear(tag)
    clear_schedule(chat_id)
    log(f"Cleared schedules for user {chat_id}")


def schedule_user_checkin(chat_id, user):
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        return

    if not user.get("auto_mode", True):
        log(f"Skipping {user['email']} (Auto Mode OFF)")
        return

    tag = user_tag(chat_id)
    current_time_str = ub_now.strftime("%H:%M:%S")
    current_hour = ub_now.hour

    m_time = humanized_random_time(*MORNING_START, *MORNING_END)

    if m_time > current_time_str:
        schedule.every().day.at(m_time).do(
            _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_in", source="scheduled"
        ).tag(tag)
        set_schedule(chat_id, "check_in", m_time)
        log(f"Scheduled In for {user['email']}: {m_time}")
    else:
        morning_end_str = f"{MORNING_END[0]:02d}:{MORNING_END[1]:02d}:00"
        if current_time_str <= morning_end_str:
            delay = random.randint(10, 180)
            set_schedule(chat_id, "check_in", f"~{delay}с дараа")
            log(f"Late morning for {user['email']}, executing in {delay}s")
            threading.Timer(delay, lambda: execute_punch(chat_id, "check_in", source="late_morning")).start()
        else:
            set_schedule(chat_id, "check_in", "⏭ өнгөрсөн")
            log(f"Morning window passed for {user['email']}, skipping check-in")
    
    if current_hour < 11:
        set_schedule(chat_id, "check_out", "⏳ 11:00-д тооцоолно")


def calculate_checkout_time(check_in_dt):
    base_out_dt = check_in_dt + timedelta(hours=WORK_HOURS_BASE)
    base_out_secs = base_out_dt.hour * 3600 + base_out_dt.minute * 60 + base_out_dt.second
    
    jitter_minutes = random.triangular(CHECKOUT_JITTER_MIN, CHECKOUT_JITTER_MAX, CHECKOUT_JITTER_MODE)
    jitter_secs = int(jitter_minutes * 60)
    
    target_secs = base_out_secs + jitter_secs
    
    floor_secs = hm_to_secs(*CHECKOUT_FLOOR)
    ceiling_secs = hm_to_secs(*CHECKOUT_CEILING)
    
    min_required_secs = (check_in_dt.hour * 3600 + check_in_dt.minute * 60 + 
                         check_in_dt.second + WORK_HOURS_BASE * 3600)
    
    actual_floor = max(floor_secs, min_required_secs)
    
    target_secs = max(actual_floor, min(target_secs, ceiling_secs))
    
    target_secs += random.randint(3, 57)
    target_secs = min(target_secs, ceiling_secs + 59)
    
    h, m, s = secs_to_hms(target_secs)
    out_str = f"{h:02d}:{m:02d}:{s:02d}"
    
    work_secs = target_secs - (check_in_dt.hour * 3600 + check_in_dt.minute * 60 + check_in_dt.second)
    work_hours = work_secs // 3600
    work_mins = (work_secs % 3600) // 60
    work_str = f"{work_hours}ц {work_mins}м"
    
    return out_str, work_str, int(jitter_minutes)


def plan_user_checkout(chat_id, user):
    """Plan checkout for ONE user based on their actual check-in time."""
    ub_now = get_ub_time()
    tag = user_tag(chat_id)
    email = user.get('email', 'unknown')

    if not user.get("auto_mode", True):
        return

    actual_in_dt = None
    is_checked_in = False
    
    for attempt in range(3):
        session, u_data = get_session(user['email'], user['password'])
        if not session:
            log(f"Login failed for {email} (attempt {attempt + 1}/3)")
            time.sleep(3)
            continue

        time.sleep(1)
        
        cal_data = get_calendar_data(session, u_data['uid'])
        
        if not cal_data:
            log(f"No calendar data for {email} (attempt {attempt + 1}/3)")
            time.sleep(3)
            continue
        
        is_checked_in = cal_data[0].get("checked_in_today", False)
        actual_in_dt, _ = get_real_checkin_checkout_time(cal_data)
        
        if actual_in_dt:
            log(f"Got check-in time for {email}: {actual_in_dt} (attempt {attempt + 1}/3)")
            break
        elif is_checked_in:
            log(f"User {email} checked_in=True but parsing failed (attempt {attempt + 1}/3)")
            time.sleep(3)
        else:
            log(f"User {email} is not checked in (attempt {attempt + 1}/3)")
            break

    if actual_in_dt:
        out_str, work_str, jitter_mins = calculate_checkout_time(actual_in_dt)
        in_str = actual_in_dt.strftime("%H:%M:%S")
        
        current_time_str = ub_now.strftime("%H:%M:%S")
        
        out_hour = int(out_str.split(":")[0])
        if out_hour < 17:
            log(f"🚨 WARNING: Calculated checkout {out_str} is before 17:00 for {email}! Adjusting...")
            out_str = f"17:30:{random.randint(10,59):02d}"
            log(f"Adjusted checkout to {out_str}")

        if out_str > current_time_str:
            log(f"Scheduling checkout for {email} at {out_str} (tag: {tag})")
            schedule.every().day.at(out_str).do(
                _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_out", source="scheduled"
            ).tag(tag)

            set_schedule(chat_id, "check_out", out_str)
            set_schedule(chat_id, "check_in_actual", in_str)
            log(f"User {email}: In={in_str} → Out={out_str} ({work_str})")

            send_telegram(
                chat_id,
                f"📅 **Өдрийн Төлөвлөгөө**\n"
                f"✅ Ирсэн: `{in_str}`\n"
                f"🎯 Явах цаг: `{out_str}`\n"
                f"⏱ Ажлын цаг: ~{work_str}"
            )
        else:
            set_schedule(chat_id, "check_out", "⏭ өнгөрсөн")
            log(f"Checkout time already passed for {email}: {out_str}")
            
    elif is_checked_in:
        log(f"User {email} checked in but couldn't parse time after 3 attempts, using safe estimate")
        
        assumed_in = ub_now.replace(hour=MORNING_END[0], minute=MORNING_END[1], second=0, microsecond=0)
        out_str, work_str, _ = calculate_checkout_time(assumed_in)
        
        current_time_str = ub_now.strftime("%H:%M:%S")
        
        if out_str > current_time_str:
            schedule.every().day.at(out_str).do(
                _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_out", source="scheduled_fallback"
            ).tag(tag)

            set_schedule(chat_id, "check_out", out_str)
            
            send_telegram(
                chat_id,
                f"📅 **Өдрийн Төлөвлөгөө**\n"
                f"✅ Ирсэн: (цаг уншигдсангүй)\n"
                f"🎯 Явах цаг: `{out_str}`\n"
                f"⚠️ Ирсэн цагийг ~{MORNING_END[0]}:{MORNING_END[1]:02d} гэж тооцов\n\n"
                f"💡 /replan командаар дахин оролдоно уу."
            )
    else:
        fallback_time = humanized_random_time(*FALLBACK_OUT_START, *FALLBACK_OUT_END)

        current_time_str = ub_now.strftime("%H:%M:%S")
        if fallback_time > current_time_str:
            schedule.every().day.at(fallback_time).do(
                _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_out", source="no_checkin_fallback"
            ).tag(tag)

            set_schedule(chat_id, "check_out", fallback_time)
            log(f"⚠️ No check-in for {email}. Fallback Out: {fallback_time}")
            send_telegram(
                chat_id,
                f"⚠️ **Анхааруулга**\n"
                f"Өглөөний бүртгэл олдсонгүй!\n"
                f"Та өнөөдөр ирээгүй байж магадгүй.\n\n"
                f"Хэрэв ирсэн бол:\n"
                f"• /replan командаар дахин оролдоно уу\n"
                f"• /debug командаар шалгана уу\n\n"
                f"Fallback гарах цаг: `{fallback_time}`"
            )


def plan_checkout_strategy():
    log("Running Mid-Day Checkout Planner")
    users = load_users()
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        return

    success_count = 0
    fail_count = 0
    skip_count = 0

    log(f"Total users to process: {len(users)}")

    for chat_id, user in users.items():
        email = user.get('email', 'unknown')
        auto_mode = user.get("auto_mode", True)
        
        if not auto_mode:
            log(f"SKIP {email}: auto_mode is OFF")
            skip_count += 1
            continue
        
        time.sleep(random.uniform(0.5, 2))
        
        try:
            log(f"Planning checkout for {email}...")
            plan_user_checkout(chat_id, user)
            success_count += 1
        except Exception as e:
            fail_count += 1
            log(f"FAIL checkout plan for {email}: {e}")
            import traceback
            traceback.print_exc()
            try:
                fallback_time = humanized_random_time(*FALLBACK_OUT_START, *FALLBACK_OUT_END)
                set_schedule(chat_id, "check_out", f"⚠️ {fallback_time} (fallback)")
                send_telegram(
                    chat_id,
                    f"⚠️ **Системийн алдаа**\n"
                    f"Явах цаг тооцоолоход алдаа гарлаа.\n"
                    f"Fallback: `{fallback_time}`\n\n"
                    f"Та /debug команд ашиглан шалгаж болно."
                )
            except:
                pass
    
    log(f"Checkout planning complete: {success_count} success, {fail_count} failed, {skip_count} skipped")


def schedule_all_users():
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        log("Weekend Mode. No tasks scheduled.")
        return

    users = load_users()
    log(f"=== Daily scheduling for {len(users)} users ===")

    for chat_id, user in users.items():
        log(f"Setting up check-in for {user.get('email', 'unknown')} (chat_id: {chat_id})")
        unschedule_user(chat_id)
        schedule_user_checkin(chat_id, user)

    schedule.clear('checkout_planner')
    schedule.every().day.at("11:00:00").do(
        _run_in_thread, plan_checkout_strategy
    ).tag('checkout_planner')

    current_hour = ub_now.hour
    log(f"Current hour: {current_hour}")
    
    if current_hour >= 11:
        log("=== It's past 11:00, running checkout planner NOW ===")
        plan_checkout_strategy()
    else:
        log(f"Checkout planner will run at 11:00 (currently {ub_now.strftime('%H:%M')})")
    
    log("=== Schedule setup complete ===")


def _run_in_thread(func, *args, **kwargs):
    threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return schedule.CancelJob


@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    bot.send_message(
        message.chat.id,
        "👋 **Сайн байна уу!**\nДоорх цэснээс сонгоно уу:",
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )


@bot.message_handler(commands=['register'])
def register_command(message):
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(
                message,
                "⚠️ Буруу формат.\nЖишээ: `/register email@tavanbogd.com password`",
                parse_mode="Markdown"
            )
            return

        email, pwd = parts[1], parts[2]
        bot.reply_to(message, "🔄 Шалгаж байна...")

        s, d = get_session(email, pwd)
        if s:
            save_user(message.chat.id, email, pwd, d['name'])
            bot.send_message(
                message.chat.id,
                f"✅ **Бүртгэл амжилттай!**\n"
                f"Хэрэглэгч: {d['name']}\n"
                f"Автомат горим идэвхжлээ.",
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )
            chat_id_str = str(message.chat.id)
            users = load_users()
            if chat_id_str in users:
                unschedule_user(chat_id_str)
                schedule_user_checkin(chat_id_str, users[chat_id_str])
        else:
            bot.reply_to(message, "❌ Нэвтрэх нэр эсвэл нууц үг буруу байна.")
    except Exception as e:
        log(f"Register Error: {e}")


@bot.message_handler(commands=['debug'])
def debug_command(message):
    users = load_users()
    user = users.get(str(message.chat.id))
    
    if not user:
        bot.reply_to(message, "❌ Бүртгэл олдсонгүй.")
        return
    
    auto_mode = user.get("auto_mode", True)
    
    session, u_data = get_session(user['email'], user['password'])
    if not session:
        bot.reply_to(message, f"❌ Нэвтрэх боломжгүй.\nauto_mode: {auto_mode}")
        return
    
    cal_data = get_calendar_data(session, u_data['uid'])
    
    if cal_data:
        is_checked_in = cal_data[0].get("checked_in_today", False)
        actual_in, actual_out = get_real_checkin_checkout_time(cal_data)
        
        msg = f"🔍 **Debug Info**\n\n"
        msg += f"👤 {user['email']}\n"
        msg += f"auto_mode: `{auto_mode}`\n"
        msg += f"checked_in_today: `{is_checked_in}`\n"
        msg += f"parsed check_in: `{actual_in}`\n"
        msg += f"parsed check_out: `{actual_out}`\n"
        
        sched = get_schedule(message.chat.id)
        msg += f"\n📅 Schedule data:\n`{sched}`\n"
        
        try:
            today_str = get_ub_time().strftime("%Y-%m-%d")
            raw_json = cal_data[0].get("attendance_calendar_json", {})
            weeks = raw_json.get("weeks", [])
            for week in weeks:
                for day_obj in week:
                    if day_obj.get("day") == today_str:
                        day_data = day_obj.get("day_data", {})
                        in_out = day_data.get("in_out", "")
                        msg += f"\nraw in_out: `{in_out}`"
        except:
            pass
            
        bot.reply_to(message, msg, parse_mode="Markdown")
    else:
        bot.reply_to(message, f"❌ Calendar data is None\nauto_mode: {auto_mode}")


@bot.message_handler(commands=['replan'])
def replan_command(message):
    """Manually trigger checkout planning for this user"""
    chat_id = str(message.chat.id)
    users = load_users()
    user = users.get(chat_id)
    
    if not user:
        bot.reply_to(message, "❌ Бүртгэл олдсонгүй. /register ашиглана уу.")
        return
    
    bot.reply_to(message, "⏳ Явах цагийг дахин тооцоолж байна...")
    
    try:
        tag = user_tag(chat_id)

        plan_user_checkout(chat_id, user)
        
        sched = get_schedule(chat_id)
        if sched and sched.get("check_out"):
            bot.send_message(
                message.chat.id,
                f"✅ **Амжилттай!**\n"
                f"Явах цаг: `{sched.get('check_out')}`",
                parse_mode="Markdown"
            )
        else:
            bot.send_message(message.chat.id, "⚠️ Явах цаг тооцоолж чадсангүй. /debug ашиглан шалгана уу.")
    except Exception as e:
        log(f"Replan error for {user['email']}: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(message.chat.id, f"❌ Алдаа: {e}")


@bot.message_handler(commands=['allusers'])
def all_users_command(message):
    ADMIN_CHAT_ID = 6190430690 
    
    if message.chat.id != ADMIN_CHAT_ID:
        bot.reply_to(message, "❌ Зөвхөн админ ашиглах боломжтой.")
        return
    
    users = load_users()
    
    if not users:
        bot.reply_to(message, "Хэрэглэгч олдсонгүй.")
        return
    
    msg = "👥 **Бүх хэрэглэгчид:**\n\n"
    
    for chat_id, user in users.items():
        email = user.get('email', 'unknown')
        name = user.get('name', 'Unknown')
        auto = "✅" if user.get('auto_mode', True) else "❌"
        sched = get_schedule(chat_id)
        
        checkout = "—"
        if sched:
            checkout = sched.get('check_out', '—')
        
        msg += f"{auto} `{chat_id}`\n"
        msg += f"   {name}\n"
        msg += f"   Явах: {checkout}\n\n"
    
    bot.reply_to(message, msg, parse_mode="Markdown")


@bot.message_handler(commands=['jobs'])
def jobs_command(message):
    ADMIN_CHAT_ID = 6190430690
    
    if message.chat.id != ADMIN_CHAT_ID:
        bot.reply_to(message, "❌ Зөвхөн админ ашиглах боломжтой.")
        return
    
    all_jobs = schedule.get_jobs()
    
    if not all_jobs:
        bot.reply_to(message, "📋 Хуваарьт ажил байхгүй.")
        return
    
    msg = f"📋 **Хуваарьт ажлууд ({len(all_jobs)}):**\n\n"
    
    for i, job in enumerate(all_jobs[:20], 1):  # Limit to 20
        tags = ", ".join(job.tags) if job.tags else "no tag"
        next_run = job.next_run.strftime("%H:%M:%S") if job.next_run else "?"
        msg += f"{i}. `{next_run}` [{tags}]\n"
    
    if len(all_jobs) > 20:
        msg += f"\n... +{len(all_jobs) - 20} more"
    
    bot.reply_to(message, msg, parse_mode="Markdown")


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
            auto_status = "✅ ON" if user.get("auto_mode", True) else "❌ OFF"

            msg = (
                f"📅 **{current_dt}**\n\n"
                f"👤 **{u_data['name']}**\n"
                f"Төлөв: {state_icon} *{state_text}*\n"
                f"🟢 Ирсэн: `{in_time_text}`\n"
                f"🔴 Явсан: `{out_time_text}`\n"
                f"⚙️ Автомат: {auto_status}"
            )
            bot.send_message(chat_id, msg, parse_mode="Markdown")
        else:
            bot.send_message(chat_id, "❌ Нэвтрэх боломжгүй байна.")

    elif call.data == "btn_in":
        bot.answer_callback_query(call.id, "Боловсруулж байна...")
        msg = bot.send_message(chat_id, "⏳ Ирц бүртгэж байна...")
        threading.Thread(
            target=execute_punch,
            args=(chat_id, "check_in", msg.message_id),
            daemon=True
        ).start()

    elif call.data == "btn_schedule":
        bot.answer_callback_query(call.id)
        users = load_users()
        user = users.get(str(chat_id))

        if not user:
            bot.send_message(chat_id, "❌ Та бүртгүүлээгүй байна.")
            return

        ub_now = get_ub_time()
        today_str = ub_now.strftime("%Y-%m-%d")
        day_names = ["Даваа", "Мягмар", "Лхагва", "Пүрэв", "Баасан", "Бямба", "Ням"]
        day_name = day_names[ub_now.weekday()]

        auto_on = user.get("auto_mode", True)

        if not auto_on:
            msg = (
                f"🤖 **Хуваарь** — {today_str} ({day_name})\n\n"
                f"⚙️ Автомат горим: ❌ OFF\n\n"
                f"Автомат горимыг идэвхжүүлэхийн тулд\n"
                f"Тохиргоо руу орно уу."
            )
            bot.send_message(chat_id, msg, parse_mode="Markdown")
            return

        if ub_now.weekday() >= 5:
            bot.send_message(
                chat_id,
                f"🤖 **Хуваарь** — {today_str} ({day_name})\n\n"
                f"🏖 Амралтын өдөр — хуваарь байхгүй.",
                parse_mode="Markdown"
            )
            return

        sched = get_schedule(chat_id)
        now_str = ub_now.strftime("%H:%M:%S")
        current_hour = ub_now.hour

        real_in_time = None
        real_out_time = None
        try:
            session, u_data = get_session(user['email'], user['password'])
            if session:
                cal_data = get_calendar_data(session, u_data['uid'])
                if cal_data:
                    actual_in, actual_out = get_real_checkin_checkout_time(cal_data)
                    if actual_in:
                        real_in_time = actual_in.strftime("%H:%M:%S")
                    if actual_out:
                        real_out_time = actual_out.strftime("%H:%M:%S")
        except Exception as e:
            log(f"Schedule fetch error: {e}")

        if current_hour >= 11:
            needs_checkout_planning = False
            
            if not sched:
                needs_checkout_planning = True
            elif not sched.get("check_out") or sched.get("check_out", "").startswith("⏳ 11"):
                needs_checkout_planning = True
            
            if needs_checkout_planning:
                bot.send_message(chat_id, "⏳ Явах цагийг тооцоолж байна...")
                try:
                    plan_user_checkout(str(chat_id), user)
                    sched = get_schedule(chat_id) 
                except Exception as e:
                    log(f"On-demand checkout planning failed for {user['email']}: {e}")
                    bot.send_message(chat_id, f"⚠️ Явах цаг тооцоолоход алдаа: {e}")

        sched = get_schedule(chat_id)

        if real_in_time:
            in_display = f"✅ `{real_in_time}`"
        elif sched and sched.get("check_in"):
            in_time = sched.get("check_in")
            if in_time.startswith("⏭") or in_time.startswith("~"):
                in_display = in_time
            elif now_str >= in_time:
                in_display = f"✅ `{in_time}` (дууссан)"
            else:
                in_display = f"⏳ `{in_time}` (хүлээж байна)"
        else:
            in_display = "—"

        if real_out_time:
            out_display = f"✅ `{real_out_time}` (дууссан)"
        elif sched and sched.get("check_out"):
            out_time = sched.get("check_out")
            if not out_time or out_time == "—":
                out_display = "—"
            elif out_time.startswith("⏭") or out_time.startswith("~") or out_time.startswith("⏳") or out_time.startswith("⚠️"):
                out_display = out_time
            elif now_str >= out_time:
                out_display = f"✅ `{out_time}` (дууссан)"
            else:
                out_display = f"⏳ `{out_time}` (хүлээж байна)"
        else:
            out_display = "—"

        msg = (
            f"🤖 **Хуваарь** — {today_str} ({day_name})\n\n"
            f"🟢 Ирсэн:  {in_display}\n"
            f"🔴 Явах:   {out_display}\n\n"
            f"⏰ Одоо: `{ub_now.strftime('%H:%M:%S')}`"
        )

        bot.send_message(chat_id, msg, parse_mode="Markdown")

    elif call.data == "btn_out":
        bot.answer_callback_query(call.id, "Боловсруулж байна...")
        msg = bot.send_message(chat_id, "⏳ Ирц бүртгэж байна...")
        threading.Thread(
            target=execute_punch,
            args=(chat_id, "check_out", msg.message_id),
            daemon=True
        ).start()

    elif call.data == "btn_settings":
        bot.edit_message_text(
            "⚙️ **Тохиргоо**",
            chat_id, call.message.message_id,
            reply_markup=settings_keyboard(chat_id),
            parse_mode="Markdown"
        )

    elif call.data == "toggle_auto":
        new_status = toggle_auto_mode(chat_id)
        status_text = "Идэвхжүүллээ ✅" if new_status else "Идэвхгүй ❌"
        bot.answer_callback_query(call.id, f"Автомат горим: {status_text}")
        bot.edit_message_reply_markup(
            chat_id, call.message.message_id,
            reply_markup=settings_keyboard(chat_id)
        )

        chat_id_str = str(chat_id)
        unschedule_user(chat_id_str)
        if new_status:
            users = load_users()
            if chat_id_str in users:
                threading.Thread(
                    target=schedule_user_checkin,
                    args=(chat_id_str, users[chat_id_str]),
                    daemon=True
                ).start()

    elif call.data == "back_home":
        bot.delete_message(chat_id, call.message.message_id)
        send_menu(call.message)


def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)


def smart_recovery():
    ub_now = get_ub_time()
    
    if ub_now.weekday() >= 5:
        log("Weekend - no recovery needed")
        return
    
    users = load_users()
    current_time = ub_now.strftime("%H:%M:%S")
    current_hour = ub_now.hour
    
    log(f"=== SMART RECOVERY at {current_time} ===")
    
    for chat_id, user in users.items():
        if not user.get("auto_mode", True):
            continue
            
        email = user.get('email', 'unknown')
        
        try:
            session, u_data = get_session(user['email'], user['password'])
            if not session:
                log(f"Recovery: Can't login {email}")
                continue
            
            cal_data = get_calendar_data(session, u_data['uid'])
            is_checked_in = cal_data[0].get("checked_in_today", False) if cal_data else False
            actual_in, actual_out = get_real_checkin_checkout_time(cal_data)
            
            morning_end = f"{MORNING_END[0]:02d}:{MORNING_END[1]:02d}:00"
            
            if current_time <= morning_end and not is_checked_in:
                log(f"Recovery: {email} needs check-in NOW")
                delay = random.randint(5, 60)
                threading.Timer(delay, lambda: execute_punch(chat_id, "check_in", source="recovery")).start()
                send_telegram(chat_id, f"🔄 Бот дахин асаав. Ирцийг ~{delay}с дараа бүртгэнэ.")
            
            elif current_hour >= 17 and is_checked_in and not actual_out:
                if actual_in:
                    min_checkout = actual_in + timedelta(hours=WORK_HOURS_BASE)
                    if ub_now >= min_checkout:
                        log(f"Recovery: {email} needs check-out NOW (past 9 hours)")
                        delay = random.randint(5, 120)
                        threading.Timer(delay, lambda: execute_punch(chat_id, "check_out", source="recovery")).start()
                        send_telegram(chat_id, f"🔄 Бот дахин асаав. 9+ цаг болсон тул явах бүртгэлийг ~{delay}с дараа хийнэ.")
                else:
                    if current_hour >= 18:
                        log(f"Recovery: {email} late checkout NOW")
                        delay = random.randint(5, 120)
                        threading.Timer(delay, lambda: execute_punch(chat_id, "check_out", source="recovery")).start()
                        send_telegram(chat_id, f"🔄 Бот дахин асаав. Оройтсон тул явах бүртгэл хийнэ.")
            
            elif current_hour >= 11 and current_hour < 17 and is_checked_in:
                sched = get_schedule(chat_id)
                if not sched or not sched.get("check_out") or sched.get("check_out", "").startswith("⏳"):
                    log(f"Recovery: Planning checkout for {email}")
                    plan_user_checkout(chat_id, user)
            
            else:
                log(f"Recovery: {email} - no action needed (in={is_checked_in}, out={actual_out is not None})")
                
        except Exception as e:
            log(f"Recovery error for {email}: {e}")
        
        time.sleep(random.uniform(1, 3))
    
    log("=== SMART RECOVERY COMPLETE ===")


def periodic_health_check():
    ub_now = get_ub_time()
    current_hour = ub_now.hour
    
    if ub_now.weekday() < 5 and 8 <= current_hour <= 20:
        smart_recovery()


if __name__ == "__main__":
    schedule.every().day.at("01:00").do(
        _run_in_thread, schedule_all_users
    ).tag('system')
    
    schedule.every(30).minutes.do(
        _run_in_thread, periodic_health_check
    ).tag('health_check')

    schedule_all_users()
    
    threading.Thread(target=smart_recovery, daemon=True).start()

    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()

    log("--- JANUS BOT STARTED (WITH SMART RECOVERY) ---")
    log("Health check runs every 30 minutes during work hours")

    while True:
        try:
            bot.polling(non_stop=True, interval=1, timeout=20)
        except Exception as e:
            log(f"Bot Polling Error: {e}")
            time.sleep(5)