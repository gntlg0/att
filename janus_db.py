import sqlite3
import threading
from datetime import datetime, date
import pytz

UB_TZ = pytz.timezone('Asia/Ulaanbaatar')
DB_PATH = "janus.db"

_conn = None
_db_lock = threading.RLock()


def _get_conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")      # concurrent reads while writing
        _conn.execute("PRAGMA busy_timeout=5000")      # wait up to 5s if locked
    return _conn


def init_db():
    with _db_lock:
        conn = _get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                date TEXT NOT NULL,
                check_in_time TEXT,
                check_out_time TEXT,
                check_in_source TEXT DEFAULT 'unknown',
                check_out_source TEXT DEFAULT 'unknown',
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(chat_id, date)
            );

            CREATE TABLE IF NOT EXISTS holidays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                recurring INTEGER DEFAULT 0
            );
        """)
        conn.commit()
        _seed_holidays(conn)


def _seed_holidays(conn):
    """Insert default Mongolian holidays if table is empty."""
    count = conn.execute("SELECT COUNT(*) FROM holidays").fetchone()[0]
    if count > 0:
        return

    holidays = [
        # Fixed holidays (recurring every year)
        ("01-01", "Шинэ жил (New Year)", 1),
        ("03-08", "Олон улсын эмэгтэйчүүдийн өдөр", 1),
        ("06-01", "Хүүхдийн баяр / Эхийн баяр", 1),
        ("07-11", "Наадам", 1),
        ("07-12", "Наадам", 1),
        ("07-13", "Наадам", 1),
        ("07-14", "Наадам", 1),
        ("07-15", "Наадам", 1),
        ("11-26", "Тусгаар тогтнолын өдөр", 1),
        ("12-29", "Тусгаар тогтнолын өдөр", 1),

        # 2026 Tsagaan Sar (changes every year — update annually)
        ("2026-02-17", "Цагаан сар", 0),
        ("2026-02-18", "Цагаан сар", 0),
        ("2026-02-19", "Цагаан сар", 0),

        # 2027 Tsagaan Sar (approximate)
        ("2027-02-06", "Цагаан сар", 0),
        ("2027-02-07", "Цагаан сар", 0),
        ("2027-02-08", "Цагаан сар", 0),
    ]

    for h_date, name, recurring in holidays:
        conn.execute(
            "INSERT OR IGNORE INTO holidays (date, name, recurring) VALUES (?, ?, ?)",
            (h_date, name, recurring)
        )
    conn.commit()


# ── Holiday helpers ──────────────────────────────────────────

def is_holiday(check_date=None):
    if check_date is None:
        check_date = datetime.now(UB_TZ).date()
    elif isinstance(check_date, str):
        check_date = datetime.strptime(check_date, "%Y-%m-%d").date()

    conn = _get_conn()
    date_str = check_date.strftime("%Y-%m-%d")
    mm_dd = check_date.strftime("%m-%d")

    row = conn.execute(
        "SELECT name FROM holidays WHERE date = ? OR (recurring = 1 AND date = ?)",
        (date_str, mm_dd)
    ).fetchone()

    return row["name"] if row else None


def is_workday(check_date=None):
    if check_date is None:
        check_date = datetime.now(UB_TZ).date()

    if check_date.weekday() >= 5:
        return False
    if is_holiday(check_date):
        return False
    return True


def add_holiday(date_str, name, recurring=False):
    with _db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO holidays (date, name, recurring) VALUES (?, ?, ?)",
            (date_str, name, 1 if recurring else 0)
        )
        conn.commit()


def remove_holiday(date_str):
    with _db_lock:
        conn = _get_conn()
        conn.execute("DELETE FROM holidays WHERE date = ?", (date_str,))
        conn.commit()


def list_holidays(year=None):
    conn = _get_conn()
    if year:
        rows = conn.execute(
            "SELECT date, name, recurring FROM holidays WHERE date LIKE ? OR recurring = 1 ORDER BY date",
            (f"{year}%",)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT date, name, recurring FROM holidays ORDER BY date"
        ).fetchall()
    return [dict(r) for r in rows]


# ── Attendance tracking ──────────────────────────────────────

def record_checkin(chat_id, check_time=None, source="unknown"):
    with _db_lock:
        conn = _get_conn()
        today = datetime.now(UB_TZ).strftime("%Y-%m-%d")
        time_str = check_time or datetime.now(UB_TZ).strftime("%H:%M:%S")

        conn.execute("""
            INSERT INTO attendance (chat_id, date, check_in_time, check_in_source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, date) DO UPDATE SET
                check_in_time = excluded.check_in_time,
                check_in_source = excluded.check_in_source
        """, (str(chat_id), today, time_str, source))
        conn.commit()


def record_checkout(chat_id, check_time=None, source="unknown"):
    with _db_lock:
        conn = _get_conn()
        today = datetime.now(UB_TZ).strftime("%Y-%m-%d")
        time_str = check_time or datetime.now(UB_TZ).strftime("%H:%M:%S")

        conn.execute("""
            INSERT INTO attendance (chat_id, date, check_out_time, check_out_source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, date) DO UPDATE SET
                check_out_time = excluded.check_out_time,
                check_out_source = excluded.check_out_source
        """, (str(chat_id), today, time_str, source))
        conn.commit()


def get_today_attendance(chat_id):
    conn = _get_conn()
    today = datetime.now(UB_TZ).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT * FROM attendance WHERE chat_id = ? AND date = ?",
        (str(chat_id), today)
    ).fetchone()
    return dict(row) if row else None


def get_unchecked_out_users(target_date=None):
    conn = _get_conn()
    if target_date is None:
        target_date = datetime.now(UB_TZ).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT chat_id, check_in_time FROM attendance WHERE date = ? AND check_in_time IS NOT NULL AND check_out_time IS NULL",
        (target_date,)
    ).fetchall()
    return [dict(r) for r in rows]
