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

CHECKOUT_JITTER_MIN = -30      
CHECKOUT_JITTER_MAX = 60       
CHECKOUT_FLOOR = (17, 30)      
CHECKOUT_CEILING = (19, 30)    

FALLBACK_OUT_START = (17, 30)
FALLBACK_OUT_END = (19, 30)

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
                    "compute_calendar_month": True
                }
            }
        },
        "id": random.randint(1, 1000)
    }
    try:
        r = session.post(
            CALENDAR_READ_URL, json=payload,
            headers={"Content-Type": "application/json", "Referer": BASE_URL},
            timeout=20
        )
        return r.json().get("result", [])
    except Exception:
        return None


def get_real_checkin_checkout_time(calendar_result):
    try:
        today_str = get_ub_time().strftime("%Y-%m-%d")
        if not calendar_result:
            return None, None

        raw_json = calendar_result[0].get("attendance_calendar_json", {})
        weeks = raw_json.get("weeks", [])

        for week in weeks:
            for day_obj in week:
                if day_obj.get("day") == today_str:
                    time_str = day_obj.get("day_data", {}).get("in_out", "")
                    if not time_str:
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
                            except ValueError:
                                pass

                    if len(parts) >= 2:
                        raw = parts[1].strip()
                        if raw:
                            full = f"{today_str} {raw}"
                            try:
                                fmt = "%Y-%m-%d %H:%M:%S" if raw.count(":") == 2 else "%Y-%m-%d %H:%M"
                                check_out_dt = datetime.strptime(full, fmt)
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
            bot.edit_message_text(
                "❌ Бүртгэл олдсонгүй. /register ашиглана уу.",
                chat_id, message_id
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
    """Clear ONLY this user's scheduled jobs. Other users untouched."""
    tag = user_tag(chat_id)
    schedule.clear(tag)
    clear_schedule(chat_id)
    log(f"Cleared schedules for user {chat_id}")


def schedule_user_checkin(chat_id, user):
    """Schedule morning check-in for ONE user only."""
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        return

    if not user.get("auto_mode", True):
        log(f"Skipping {user['email']} (Auto Mode OFF)")
        return

    tag = user_tag(chat_id)
    current_time_str = ub_now.strftime("%H:%M:%S")

    m_time = humanized_random_time(*MORNING_START, *MORNING_END)

    if m_time > current_time_str:
        schedule.every().day.at(m_time).do(
            _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_in"
        ).tag(tag)
        set_schedule(chat_id, "check_in", m_time)
        log(f"Scheduled In for {user['email']}: {m_time}")
    else:
        morning_end_str = f"{MORNING_END[0]:02d}:{MORNING_END[1]:02d}:00"
        if current_time_str <= morning_end_str:
            delay = random.randint(10, 180)
            set_schedule(chat_id, "check_in", f"~{delay}с дараа")
            log(f"Late morning for {user['email']}, executing in {delay}s")
            threading.Timer(delay, execute_punch, args=(chat_id, "check_in")).start()
        else:
            set_schedule(chat_id, "check_in", "⏭ өнгөрсөн")
            log(f"Morning window passed for {user['email']}, skipping check-in")


def plan_user_checkout(chat_id, user):
    """Plan checkout for ONE user based on their actual check-in time."""
    ub_now = get_ub_time()
    tag = user_tag(chat_id)

    if not user.get("auto_mode", True):
        return

    session, u_data = get_session(user['email'], user['password'])
    if not session:
        log(f"Login failed for checkout planning: {user['email']}")
        return

    cal_data = get_calendar_data(session, u_data['uid'])
    actual_in_dt, _ = get_real_checkin_checkout_time(cal_data)

    floor_secs = hm_to_secs(*CHECKOUT_FLOOR)
    ceiling_secs = hm_to_secs(*CHECKOUT_CEILING)

    if actual_in_dt:
        base_out_dt = actual_in_dt + timedelta(hours=WORK_HOURS_BASE)
        base_out_secs = base_out_dt.hour * 3600 + base_out_dt.minute * 60 + base_out_dt.second

        jitter_minutes = random.triangular(CHECKOUT_JITTER_MIN, CHECKOUT_JITTER_MAX, 10)
        jitter_secs = int(jitter_minutes * 60)

        target_secs = base_out_secs + jitter_secs

        target_secs = max(floor_secs, min(target_secs, ceiling_secs))

        target_secs += random.randint(3, 57)
        target_secs = min(target_secs, ceiling_secs + 59)

        h, m, s = secs_to_hms(target_secs)
        out_str = f"{h:02d}:{m:02d}:{s:02d}"
        in_str = actual_in_dt.strftime("%H:%M:%S")

        current_time_str = ub_now.strftime("%H:%M:%S")

        if out_str > current_time_str:
            schedule.every().day.at(out_str).do(
                _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_out"
            ).tag(tag)

            set_schedule(chat_id, "check_out", out_str)
            log(f"User {user['email']}: In={in_str} → Out={out_str}")

            jitter_display = int(jitter_minutes)
            direction = "илүү" if jitter_display >= 0 else "эрт"
            send_telegram(
                chat_id,
                f"📅 **Өдрийн Төлөвлөгөө**\n"
                f"✅ Ирсэн: `{in_str}`\n"
                f"🎯 Явах цаг: `{out_str}`\n"
                f"(~{WORK_HOURS_BASE} цаг {abs(jitter_display)} мин {direction})"
            )
        else:
            set_schedule(chat_id, "check_out", "⏭ өнгөрсөн")
            log(f"Checkout time already passed for {user['email']}: {out_str}")
    else:
        fallback_time = humanized_random_time(*FALLBACK_OUT_START, *FALLBACK_OUT_END)

        current_time_str = ub_now.strftime("%H:%M:%S")
        if fallback_time > current_time_str:
            schedule.every().day.at(fallback_time).do(
                _run_in_thread, execute_punch, chat_id=chat_id, action_type="check_out"
            ).tag(tag)

            set_schedule(chat_id, "check_out", fallback_time)
            log(f"⚠️ No check-in for {user['email']}. Fallback Out: {fallback_time}")
            send_telegram(
                chat_id,
                f"⚠️ **Анхааруулга**\n"
                f"Өглөөний бүртгэл олдсонгүй.\n"
                f"Автомат гарах цаг: `{fallback_time}`\n"
                f"Та өөрөө шалгаарай."
            )


def plan_checkout_strategy():
    log("Running Mid-Day Checkout Planner")
    users = load_users()
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        return

    for chat_id, user in users.items():
        if not user.get("auto_mode", True):
            continue
        time.sleep(random.uniform(2, 8))
        try:
            plan_user_checkout(chat_id, user)
        except Exception as e:
            log(f"Checkout plan error for {user['email']}: {e}")


def schedule_all_users():
    ub_now = get_ub_time()

    if ub_now.weekday() >= 5:
        log("Weekend Mode. No tasks scheduled.")
        return

    users = load_users()
    log(f"Daily scheduling for {len(users)} users...")

    for chat_id, user in users.items():
        unschedule_user(chat_id)
        schedule_user_checkin(chat_id, user)

    schedule.clear('checkout_planner')
    schedule.every().day.at("11:00:00").do(
        _run_in_thread, plan_checkout_strategy
    ).tag('checkout_planner')

    if ub_now.strftime("%H:%M") >= "11:00":
        threading.Thread(target=plan_checkout_strategy, daemon=True).start()


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

        if sched:
            in_time = sched.get("check_in", "—")
            out_time = sched.get("check_out", "⏳ 11:00-д тооцоолно")

            def time_status(planned, now):
                if planned.startswith("⏭") or planned.startswith("~") or planned.startswith("⏳") or planned == "—":
                    return planned
                if now >= planned:
                    return f"✅ `{planned}` (дууссан)"
                else:
                    return f"⏳ `{planned}` (хүлээж байна)"

            in_display = time_status(in_time, now_str)
            out_display = time_status(out_time, now_str)

            msg = (
                f"🤖 **Хуваарь** — {today_str} ({day_name})\n\n"
                f"🟢 Ирэх:  {in_display}\n"
                f"🔴 Явах:  {out_display}\n\n"
                f"⏰ Одоо: `{ub_now.strftime('%H:%M:%S')}`"
            )
        else:
            msg = (
                f"🤖 **Хуваарь** — {today_str} ({day_name})\n\n"
                f"📋 Өнөөдрийн хуваарь хараахан үүсээгүй.\n"
                f"Бот өглөө 01:00-д автоматаар хуваарь гаргана.\n\n"
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


if __name__ == "__main__":
    schedule.every().day.at("01:00").do(
        _run_in_thread, schedule_all_users
    ).tag('system')

    schedule_all_users()

    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()

    log("--- STARTED ---")

    while True:
        try:
            bot.polling(non_stop=True, interval=1, timeout=20)
        except Exception as e:
            log(f"Bot Polling Error: {e}")
            time.sleep(5)