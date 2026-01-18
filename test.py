import os
import re
import json
import time
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup
)
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiogram.dispatcher.middlewares.base import BaseMiddleware

import redis.asyncio as redis
import asyncpg


# =========================
# ENV
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi. .env faylga BOT_TOKEN=... yozing.")

ADMIN_IDS = set()
_admin_raw = os.getenv("ADMIN_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

REDIS_URL = os.getenv("REDIS_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

SEARCH_TIMEOUT = int(os.getenv("SEARCH_TIMEOUT", "90"))
RL_WINDOW_SECONDS = int(os.getenv("RL_WINDOW_SECONDS", "3"))
RL_MAX_MESSAGES = int(os.getenv("RL_MAX_MESSAGES", "7"))


# =========================
# Bot / Dispatcher
# =========================
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())  # FSM storage (Redis bo'lsa ham ishlayveradi; ma'lumotlar Redisda)


# =========================
# Constants
# =========================
GENDERS = ["Erkak", "Ayol"]
REGIONS = [
    "Toshkent sh.", "Toshkent", "Andijon", "Buxoro", "Farg'ona", "Jizzax",
    "Xorazm", "Namangan", "Navoiy", "Qashqadaryo", "Samarqand",
    "Sirdaryo", "Surxondaryo", "Qoraqalpog'iston"
]
MIN_AGE = 14
MAX_AGE = 80


# =========================
# Models
# =========================
@dataclass
class Profile:
    gender: str
    age: int
    region: str


@dataclass
class SearchPref:
    mode: str  # "random" | "filtered"
    want_gender: Optional[str] = None   # None => any
    use_age: bool = False
    age_min: Optional[int] = None
    age_max: Optional[int] = None
    use_region: bool = False
    want_region: Optional[str] = None


# =========================
# Redis / Postgres connectors
# =========================
rds: Optional[redis.Redis] = None
pg_pool: Optional[asyncpg.Pool] = None

def now_ts() -> int:
    return int(time.time())

async def init_redis():
    global rds
    if not REDIS_URL:
        rds = None
        return
    rds = redis.from_url(REDIS_URL, decode_responses=True)
    # ping test
    await rds.ping()

async def init_postgres():
    global pg_pool
    if not DATABASE_URL:
        pg_pool = None
        return
    pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    # create tables
    async with pg_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS profiles (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            gender TEXT NOT NULL,
            age INT NOT NULL,
            region TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS bans (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            until_ts BIGINT NOT NULL,
            reason TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """)
    # ok


# =========================
# Redis key helpers
# =========================
def k_user(uid: int) -> str: return f"user:{uid}"
def k_profile(uid: int) -> str: return f"profile:{uid}"
def k_prefs(uid: int) -> str: return f"prefs:{uid}"
def k_state(uid: int) -> str: return f"state:{uid}"              # IDLE/SEARCHING/CHATTING
def k_partner(uid: int) -> str: return f"partner:{uid}"          # partner uid
def k_block(uid: int) -> str: return f"block:{uid}"              # set of blocked uids
def k_ban(uid: int) -> str: return f"ban:{uid}"                  # until_ts
def k_rl(uid: int) -> str: return f"rl:{uid}"                    # list timestamps
K_USERS = "users:set"                                            # set of all user ids
K_QUEUE = "queue:list"                                           # list of searching users
K_REPORTS = "reports:list"                                       # list of JSON reports


# =========================
# Safe edit helpers
# =========================
async def safe_edit_text(msg: Message, text: str, reply_markup=None):
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise

async def safe_edit_markup(msg: Message, reply_markup=None):
    try:
        await msg.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


# =========================
# Reply keyboards (chat menu)
# =========================
def chat_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⏭ Skip"), KeyboardButton(text="🛑 Stop")],
            [KeyboardButton(text="🚫 Block"), KeyboardButton(text="⚠️ Report")],
            [KeyboardButton(text="🔎 Search")]
        ],
        resize_keyboard=True
    )

def idle_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Search"), KeyboardButton(text="👤 Profile")],
            [KeyboardButton(text="📌 Status")]
        ],
        resize_keyboard=True
    )


# =========================
# Inline keyboards
# =========================
def kb_gender(prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for g in GENDERS:
        kb.button(text=g, callback_data=f"{prefix}:{g}")
    kb.adjust(2)
    return kb.as_markup()

def kb_regions(prefix: str, page: int = 0, per_page: int = 7) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    start = page * per_page
    chunk = REGIONS[start:start + per_page]
    for r in chunk:
        kb.button(text=r, callback_data=f"{prefix}:{r}")

    nav = InlineKeyboardBuilder()
    if start > 0:
        nav.button(text="⬅️", callback_data=f"{prefix}_page:{page - 1}")
    if start + per_page < len(REGIONS):
        nav.button(text="➡️", callback_data=f"{prefix}_page:{page + 1}")
    if nav.buttons:
        kb.row(*nav.buttons)

    kb.adjust(1)
    return kb.as_markup()

def kb_search_mode() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Tasodifiy", callback_data="mode:random")
    kb.button(text="🎯 Filtr bilan", callback_data="mode:filtered")
    kb.adjust(1)
    return kb.as_markup()

def kb_any_or_gender() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Farqi yo'q", callback_data="want_gender:any")
    kb.button(text="Erkak", callback_data="want_gender:Erkak")
    kb.button(text="Ayol", callback_data="want_gender:Ayol")
    kb.adjust(1)
    return kb.as_markup()

def kb_yes_no(prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Ha", callback_data=f"{prefix}:yes")
    kb.button(text="Yo'q", callback_data=f"{prefix}:no")
    kb.adjust(2)
    return kb.as_markup()

def kb_report_reasons() -> InlineKeyboardMarkup:
    reasons = ["Spam/Reklama", "Haqorat", "18+", "Firib", "Boshqa"]
    kb = InlineKeyboardBuilder()
    for r in reasons:
        kb.button(text=r, callback_data=f"report_reason:{r}")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# FSM
# =========================
class Anketa(StatesGroup):
    gender = State()
    age = State()
    region = State()

class SearchFlow(StatesGroup):
    mode = State()
    want_gender = State()
    use_age = State()
    age_min = State()
    age_max = State()
    use_region = State()
    region = State()

class ReportFlow(StatesGroup):
    reason = State()
    comment = State()


# =========================
# In-process timers for search timeout
# =========================
search_tasks: Dict[int, asyncio.Task] = {}  # uid -> task


# =========================
# Middleware: Rate limit
# =========================
class RateLimitMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        # only messages
        if not isinstance(event, Message):
            return await handler(event, data)

        uid = event.from_user.id

        # adminlar cheklanmasin
        if uid in ADMIN_IDS:
            return await handler(event, data)

        # faqat "chat relay" paytida qattiqroq cheklash (xohlasangiz)
        # hozir barcha message'lar uchun ishlaydi
        if rds is None:
            # redis yo'q bo'lsa, cheklamaymiz (yoki local dict bilan ham qilsa bo'ladi)
            return await handler(event, data)

        key = k_rl(uid)
        ts = now_ts()

        # listga qo'shamiz
        await rds.rpush(key, ts)
        # eski qiymatlarni o'chirish
        cutoff = ts - RL_WINDOW_SECONDS
        # listni tozalash: kichik loyiha uchun oddiy usul
        values = await rds.lrange(key, 0, -1)
        values_int = [int(x) for x in values if str(x).isdigit()]
        values_int = [x for x in values_int if x >= cutoff]
        await rds.delete(key)
        if values_int:
            await rds.rpush(key, *values_int)
        await rds.expire(key, RL_WINDOW_SECONDS + 2)

        if len(values_int) > RL_MAX_MESSAGES:
            # flood
            await event.answer("Juda tez yozayapsiz. Biroz sekinroq.", reply_markup=chat_menu_kb())
            return  # block

        return await handler(event, data)


dp.message.middleware(RateLimitMiddleware())


# =========================
# Data layer (Redis + optional Postgres)
# =========================
async def add_user(uid: int):
    if rds:
        await rds.sadd(K_USERS, uid)
        await rds.set(k_user(uid), "1", ex=60 * 60 * 24 * 365)
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING;", uid)

async def set_profile(uid: int, prof: Profile):
    if rds:
        await rds.set(k_profile(uid), json.dumps(prof.__dict__), ex=60 * 60 * 24 * 365)
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO profiles(user_id, gender, age, region)
                VALUES($1,$2,$3,$4)
                ON CONFLICT (user_id) DO UPDATE SET gender=EXCLUDED.gender, age=EXCLUDED.age, region=EXCLUDED.region, updated_at=NOW();
            """, uid, prof.gender, prof.age, prof.region)

async def get_profile(uid: int) -> Optional[Profile]:
    if rds:
        raw = await rds.get(k_profile(uid))
        if raw:
            d = json.loads(raw)
            return Profile(**d)
    if pg_pool:
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT gender, age, region FROM profiles WHERE user_id=$1;", uid)
            if row:
                return Profile(gender=row["gender"], age=row["age"], region=row["region"])
    return None

async def set_prefs(uid: int, pref: SearchPref):
    if rds:
        await rds.set(k_prefs(uid), json.dumps(pref.__dict__), ex=60 * 60 * 24 * 365)

async def get_prefs(uid: int) -> SearchPref:
    if rds:
        raw = await rds.get(k_prefs(uid))
        if raw:
            d = json.loads(raw)
            return SearchPref(**d)
    return SearchPref(mode="random")

async def set_user_state(uid: int, st: str):
    if rds:
        await rds.set(k_state(uid), st, ex=60 * 60 * 24 * 7)

async def get_user_state(uid: int) -> str:
    if rds:
        v = await rds.get(k_state(uid))
        if v:
            return v
    return "IDLE"

async def set_partner(uid: int, pid: Optional[int]):
    if rds:
        if pid is None:
            await rds.delete(k_partner(uid))
        else:
            await rds.set(k_partner(uid), pid, ex=60 * 60 * 24)

async def get_partner(uid: int) -> Optional[int]:
    if rds:
        v = await rds.get(k_partner(uid))
        if v and str(v).isdigit():
            return int(v)
    return None

async def add_block(uid: int, blocked_uid: int):
    if rds:
        await rds.sadd(k_block(uid), blocked_uid)

async def is_blocked(uid: int, other: int) -> bool:
    if not rds:
        return False
    # uid blocked other OR other blocked uid
    a = await rds.sismember(k_block(uid), other)
    b = await rds.sismember(k_block(other), uid)
    return bool(a or b)

async def ban_user(uid: int, until_ts: int, reason: str = ""):
    if rds:
        await rds.set(k_ban(uid), str(until_ts), ex=max(10, until_ts - now_ts()))
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bans(user_id, until_ts, reason)
                VALUES($1,$2,$3)
                ON CONFLICT (user_id) DO UPDATE SET until_ts=EXCLUDED.until_ts, reason=EXCLUDED.reason, updated_at=NOW();
            """, uid, until_ts, reason)

async def unban_user(uid: int):
    if rds:
        await rds.delete(k_ban(uid))
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM bans WHERE user_id=$1;", uid)

async def is_banned(uid: int) -> Tuple[bool, int, str]:
    # returns (banned, until_ts, reason)
    until_ts = 0
    reason = ""
    if rds:
        v = await rds.get(k_ban(uid))
        if v and str(v).isdigit():
            until_ts = int(v)
            if until_ts > now_ts():
                return True, until_ts, reason
    if pg_pool:
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT until_ts, reason FROM bans WHERE user_id=$1;", uid)
            if row:
                until_ts = int(row["until_ts"])
                reason = row["reason"] or ""
                if until_ts > now_ts():
                    # cache in redis
                    if rds:
                        await rds.set(k_ban(uid), str(until_ts), ex=max(10, until_ts - now_ts()))
                    return True, until_ts, reason
                else:
                    # expired
                    await conn.execute("DELETE FROM bans WHERE user_id=$1;", uid)
    return False, 0, ""

async def push_report(report: dict):
    if rds:
        await rds.lpush(K_REPORTS, json.dumps(report))
        await rds.ltrim(K_REPORTS, 0, 200)  # keep last 200


# =========================
# Matchmaking logic (Redis queue)
# =========================
async def in_chat(uid: int) -> bool:
    st = await get_user_state(uid)
    pid = await get_partner(uid)
    return st == "CHATTING" and pid is not None

def pref_accepts(pref: SearchPref, other: Profile) -> bool:
    if pref.mode == "random":
        return True
    if pref.want_gender and pref.want_gender in GENDERS and other.gender != pref.want_gender:
        return False
    if pref.use_age:
        if pref.age_min is not None and other.age < pref.age_min:
            return False
        if pref.age_max is not None and other.age > pref.age_max:
            return False
    if pref.use_region and pref.want_region and other.region != pref.want_region:
        return False
    return True

async def can_match(a: int, b: int) -> bool:
    if a == b:
        return False
    if await is_blocked(a, b):
        return False

    pa = await get_profile(a)
    pb = await get_profile(b)
    if not pa or not pb:
        return False

    # ban check
    banned_a, _, _ = await is_banned(a)
    banned_b, _, _ = await is_banned(b)
    if banned_a or banned_b:
        return False

    # both not in chat
    if await get_user_state(a) == "CHATTING" or await get_user_state(b) == "CHATTING":
        return False

    pref_a = await get_prefs(a)
    pref_b = await get_prefs(b)

    return pref_accepts(pref_a, pb) and pref_accepts(pref_b, pa)

async def pair_users(a: int, b: int):
    await set_partner(a, b)
    await set_partner(b, a)
    await set_user_state(a, "CHATTING")
    await set_user_state(b, "CHATTING")
    # cancel search tasks
    await cancel_search_task(a)
    await cancel_search_task(b)

async def end_chat(uid: int) -> Optional[int]:
    pid = await get_partner(uid)
    if pid is not None:
        await set_partner(pid, None)
        await set_user_state(pid, "IDLE")
    await set_partner(uid, None)
    await set_user_state(uid, "IDLE")
    return pid

async def remove_from_queue(uid: int):
    if not rds:
        return
    # remove all occurrences (list)
    await rds.lrem(K_QUEUE, 0, uid)

async def enqueue_search(uid: int):
    if not rds:
        return
    await remove_from_queue(uid)
    await rds.rpush(K_QUEUE, uid)
    await set_user_state(uid, "SEARCHING")

async def try_match(uid: int) -> Optional[int]:
    if not rds:
        return None
    # scan queue and pick first compatible
    queue_ids = await rds.lrange(K_QUEUE, 0, -1)
    for raw in queue_ids:
        if not str(raw).isdigit():
            continue
        other = int(raw)
        if other == uid:
            continue
        if await can_match(uid, other):
            # remove other from queue
            await rds.lrem(K_QUEUE, 1, other)
            # also remove uid if present
            await rds.lrem(K_QUEUE, 0, uid)
            await pair_users(uid, other)
            return other
    return None


# =========================
# Search timeout + auto retry (filter relax)
# =========================
async def cancel_search_task(uid: int):
    t = search_tasks.pop(uid, None)
    if t and not t.done():
        t.cancel()

async def schedule_search_timeout(uid: int):
    await cancel_search_task(uid)

    async def job():
        try:
            await asyncio.sleep(SEARCH_TIMEOUT)
            # still searching?
            st = await get_user_state(uid)
            if st != "SEARCHING":
                return

            # try one more match first
            other = await try_match(uid)
            if other:
                await bot.send_message(uid, "✅ Sherik topildi.", reply_markup=chat_menu_kb())
                await bot.send_message(other, "✅ Sherik topildi.", reply_markup=chat_menu_kb())
                return

            # auto-relax: if filtered -> disable region then age then gender
            pref = await get_prefs(uid)
            changed = False
            if pref.mode == "filtered":
                if pref.use_region:
                    pref.use_region = False
                    pref.want_region = None
                    changed = True
                elif pref.use_age:
                    pref.use_age = False
                    pref.age_min = None
                    pref.age_max = None
                    changed = True
                elif pref.want_gender is not None:
                    pref.want_gender = None
                    changed = True
                else:
                    pref.mode = "random"
                    changed = True
            else:
                # random -> just retry
                changed = False

            if changed:
                await set_prefs(uid, pref)
                await bot.send_message(uid, "Topilmadi. Filtr avtomatik yumshatildi va qidiruv davom etyapti.", reply_markup=idle_menu_kb())
            else:
                await bot.send_message(uid, "Hali sherik topilmadi. Qidiruv davom etyapti.", reply_markup=idle_menu_kb())

            # schedule next timeout again
            await schedule_search_timeout(uid)

        except asyncio.CancelledError:
            return

    search_tasks[uid] = asyncio.create_task(job())


# =========================
# UI / helpers
# =========================
def fmt_pref(pref: SearchPref) -> str:
    if pref.mode == "random":
        return "Qidiruv: Tasodifiy"
    parts = ["Qidiruv: Filtr"]
    if pref.want_gender:
        parts.append(f"Jins: {pref.want_gender}")
    else:
        parts.append("Jins: farqi yo'q")
    if pref.use_age:
        parts.append(f"Yosh: {pref.age_min}-{pref.age_max}")
    else:
        parts.append("Yosh: o'chirilgan")
    if pref.use_region and pref.want_region:
        parts.append(f"Hudud: {pref.want_region}")
    else:
        parts.append("Hudud: o'chirilgan")
    return "\n".join(parts)

async def ensure_profile(uid: int, m: Message, state: FSMContext) -> bool:
    prof = await get_profile(uid)
    if prof:
        return True
    await m.answer("Avval anketani to‘ldiring.\nJinsingizni tanlang:", reply_markup=kb_gender("anketa_gender"))
    await state.set_state(Anketa.gender)
    return False


# =========================
# Commands: basic
# =========================
@dp.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    uid = m.from_user.id
    await add_user(uid)

    banned, until_ts, reason = await is_banned(uid)
    if banned:
        await m.answer(f"Siz ban qilingansiz. Tugash: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(until_ts))}\n{reason}")
        return

    await m.answer(
        "Anonim chat bot.\n\n"
        "Buyruqlar:\n"
        "/profile - anketa\n"
        "/me - profilni ko‘rish\n"
        "/search - qidiruv\n"
        "/status - holat\n"
        "/stop - to‘xtatish\n",
        reply_markup=idle_menu_kb()
    )

    prof = await get_profile(uid)
    if not prof:
        await m.answer("Keling, anketani to‘ldiramiz.\nJinsingizni tanlang:", reply_markup=kb_gender("anketa_gender"))
        await state.set_state(Anketa.gender)

@dp.message(Command("status"))
async def cmd_status(m: Message):
    uid = m.from_user.id
    st = await get_user_state(uid)
    pid = await get_partner(uid)
    if st == "CHATTING" and pid:
        await m.answer(f"Holat: CHAT\nSherik: anonim\n(/skip /stop)", reply_markup=chat_menu_kb())
    elif st == "SEARCHING":
        await m.answer("Holat: QIDIRUV", reply_markup=idle_menu_kb())
    else:
        await m.answer("Holat: BO‘SH", reply_markup=idle_menu_kb())

@dp.message(Command("me"))
async def cmd_me(m: Message):
    uid = m.from_user.id
    prof = await get_profile(uid)
    pref = await get_prefs(uid)
    st = await get_user_state(uid)

    if not prof:
        await m.answer("Profil yo‘q. /profile bilan to‘ldiring.", reply_markup=idle_menu_kb())
        return

    await m.answer(
        f"👤 Profil:\nJins: {prof.gender}\nYosh: {prof.age}\nViloyat: {prof.region}\n\n"
        f"{fmt_pref(pref)}\n\n"
        f"Status: {st}\n\n"
        f"Profilni o‘zgartirish: /profile\nQidiruv sozlamasi: /search",
        reply_markup=idle_menu_kb()
    )

@dp.message(Command("profile"))
async def cmd_profile(m: Message, state: FSMContext):
    uid = m.from_user.id
    if await in_chat(uid):
        await m.answer("Siz hozir chatdasiz. /stop qilib, keyin profilni o‘zgartiring.", reply_markup=chat_menu_kb())
        return
    await remove_from_queue(uid)
    await set_user_state(uid, "IDLE")
    await cancel_search_task(uid)

    await m.answer("Jinsingizni tanlang:", reply_markup=kb_gender("anketa_gender"))
    await state.set_state(Anketa.gender)

@dp.message(Command("stop"))
async def cmd_stop(m: Message):
    uid = m.from_user.id

    # stop search
    st = await get_user_state(uid)
    if st == "SEARCHING":
        await remove_from_queue(uid)
        await set_user_state(uid, "IDLE")
        await cancel_search_task(uid)
        await m.answer("Qidiruv to‘xtatildi.", reply_markup=idle_menu_kb())
        return

    # stop chat
    pid = await end_chat(uid)
    await cancel_search_task(uid)
    if pid is not None:
        await m.answer("Chat yakunlandi. /search bilan qayta qidiring.", reply_markup=idle_menu_kb())
        await bot.send_message(pid, "Sherik chatni yakunladi. /search bilan yangisini toping.", reply_markup=idle_menu_kb())
    else:
        await m.answer("Siz hozir chatda emassiz. /search bosing.", reply_markup=idle_menu_kb())

@dp.message(Command("skip"))
async def cmd_skip(m: Message):
    uid = m.from_user.id
    if not await in_chat(uid):
        await m.answer("Siz chatda emassiz. /search bosing.", reply_markup=idle_menu_kb())
        return

    pid = await end_chat(uid)
    if pid is not None:
        await bot.send_message(pid, "Sherik /skip qildi. /search bilan yangisini toping.", reply_markup=idle_menu_kb())

    await m.answer("Yangi suhbatdosh qidiramiz...", reply_markup=idle_menu_kb())
    # restart search with existing prefs
    await start_search(uid, m)

# =========================
# Text menu buttons (reply keyboard)
# =========================
@dp.message(F.text == "🔎 Search")
async def btn_search(m: Message, state: FSMContext):
    await cmd_search(m, state)

@dp.message(F.text == "👤 Profile")
async def btn_profile(m: Message, state: FSMContext):
    await cmd_profile(m, state)

@dp.message(F.text == "📌 Status")
async def btn_status(m: Message):
    await cmd_status(m)

@dp.message(F.text == "⏭ Skip")
async def btn_skip(m: Message):
    await cmd_skip(m)

@dp.message(F.text == "🛑 Stop")
async def btn_stop(m: Message):
    await cmd_stop(m)

@dp.message(F.text == "🚫 Block")
async def btn_block(m: Message):
    await do_block(m)

@dp.message(F.text == "⚠️ Report")
async def btn_report(m: Message, state: FSMContext):
    await start_report(m, state)


# =========================
# Search flow
# =========================
@dp.message(Command("search"))
async def cmd_search(m: Message, state: FSMContext):
    uid = m.from_user.id
    banned, until_ts, reason = await is_banned(uid)
    if banned:
        await m.answer(f"Siz ban qilingansiz. Tugash: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(until_ts))}\n{reason}")
        return

    if await in_chat(uid):
        await m.answer("Siz allaqachon chatdasiz. /skip yoki /stop ishlating.", reply_markup=chat_menu_kb())
        return

    ok = await ensure_profile(uid, m, state)
    if not ok:
        return

    await remove_from_queue(uid)
    await set_user_state(uid, "IDLE")
    await cancel_search_task(uid)

    await m.answer("Qidiruv turini tanlang:", reply_markup=kb_search_mode())
    await state.set_state(SearchFlow.mode)

async def start_search(uid: int, m: Message):
    if not rds:
        await m.answer("Redis yo‘q. Qidiruv ishlashi uchun REDIS_URL ni sozlang.")
        return

    # Try immediate match
    other = await try_match(uid)
    if other:
        await bot.send_message(uid, "✅ Sherik topildi. Yozishni boshlang.", reply_markup=chat_menu_kb())
        await bot.send_message(other, "✅ Sherik topildi. Yozishni boshlang.", reply_markup=chat_menu_kb())
        return

    await enqueue_search(uid)
    await m.answer("🔎 Sherik qidirilmoqda...", reply_markup=idle_menu_kb())
    await schedule_search_timeout(uid)

@dp.callback_query(F.data.startswith("mode:"))
async def search_mode(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    mode = cb.data.split(":", 1)[1]

    if mode == "random":
        await set_prefs(uid, SearchPref(mode="random"))
        await state.clear()
        await safe_edit_text(cb.message, "🎲 Tasodifiy qidiruv boshlandi...")
        await cb.answer()
        await start_search(uid, cb.message)
        return

    await set_prefs(uid, SearchPref(mode="filtered"))
    await safe_edit_text(cb.message, "Qaysi jinsdagi suhbatdosh kerak?", reply_markup=kb_any_or_gender())
    await state.set_state(SearchFlow.want_gender)
    await cb.answer()

@dp.callback_query(F.data.startswith("want_gender:"))
async def want_gender(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    val = cb.data.split(":", 1)[1]

    pref = await get_prefs(uid)
    pref.mode = "filtered"
    pref.want_gender = None if val == "any" else val
    await set_prefs(uid, pref)

    await safe_edit_text(cb.message, "Yosh bo‘yicha ham filtr qo‘shamizmi?", reply_markup=kb_yes_no("use_age"))
    await state.set_state(SearchFlow.use_age)
    await cb.answer()

@dp.callback_query(F.data.startswith("use_age:"))
async def use_age(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    ans = cb.data.split(":", 1)[1]
    pref = await get_prefs(uid)
    pref.mode = "filtered"

    if ans == "yes":
        pref.use_age = True
        pref.age_min = None
        pref.age_max = None
        await set_prefs(uid, pref)

        await safe_edit_text(cb.message, "Minimal yoshni kiriting (masalan 18).", reply_markup=None)
        await state.set_state(SearchFlow.age_min)
    else:
        pref.use_age = False
        pref.age_min = None
        pref.age_max = None
        await set_prefs(uid, pref)

        await safe_edit_text(cb.message, "Hudud (viloyat) bo‘yicha filtr qo‘shamizmi?", reply_markup=kb_yes_no("use_region"))
        await state.set_state(SearchFlow.use_region)

    await cb.answer()

@dp.message(SearchFlow.age_min)
async def set_age_min(m: Message, state: FSMContext):
    uid = m.from_user.id
    try:
        amin = int(m.text.strip())
    except Exception:
        await m.answer("Faqat son. Masalan: 18")
        return
    if not (MIN_AGE <= amin <= MAX_AGE):
        await m.answer(f"{MIN_AGE}–{MAX_AGE} oralig‘ida kiriting.")
        return

    pref = await get_prefs(uid)
    pref.age_min = amin
    await set_prefs(uid, pref)

    await m.answer("Maksimal yoshni kiriting (masalan 30).")
    await state.set_state(SearchFlow.age_max)

@dp.message(SearchFlow.age_max)
async def set_age_max(m: Message, state: FSMContext):
    uid = m.from_user.id
    try:
        amax = int(m.text.strip())
    except Exception:
        await m.answer("Faqat son. Masalan: 30")
        return
    if not (MIN_AGE <= amax <= MAX_AGE):
        await m.answer(f"{MIN_AGE}–{MAX_AGE} oralig‘ida kiriting.")
        return

    pref = await get_prefs(uid)
    if pref.age_min is not None and amax < pref.age_min:
        await m.answer("Maksimal yosh minimaldan kichik bo‘lmasin. Qaytadan kiriting.")
        return

    pref.age_max = amax
    await set_prefs(uid, pref)

    await m.answer("Hudud (viloyat) bo‘yicha filtr qo‘shamizmi?", reply_markup=kb_yes_no("use_region"))
    await state.set_state(SearchFlow.use_region)

@dp.callback_query(F.data.startswith("use_region:"))
async def use_region(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    ans = cb.data.split(":", 1)[1]
    pref = await get_prefs(uid)
    pref.mode = "filtered"

    if ans == "yes":
        pref.use_region = True
        await set_prefs(uid, pref)

        await safe_edit_text(cb.message, "Qaysi viloyatdan bo‘lsin?", reply_markup=kb_regions("want_region", page=0))
        await state.set_state(SearchFlow.region)
        await cb.answer()
        return

    pref.use_region = False
    pref.want_region = None
    await set_prefs(uid, pref)

    await state.clear()
    await safe_edit_text(cb.message, "🎯 Filtr bilan qidiruv boshlandi...")
    await cb.answer()
    await start_search(uid, cb.message)

@dp.callback_query(F.data.startswith("want_region_page:"))
async def want_region_page(cb: CallbackQuery):
    page = int(cb.data.split(":", 1)[1])
    await safe_edit_markup(cb.message, reply_markup=kb_regions("want_region", page=page))
    await cb.answer()

@dp.callback_query(F.data.startswith("want_region:"))
async def set_want_region(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    region = cb.data.split(":", 1)[1]

    pref = await get_prefs(uid)
    pref.mode = "filtered"
    pref.use_region = True
    pref.want_region = region
    await set_prefs(uid, pref)

    await state.clear()
    await safe_edit_text(cb.message, "🎯 Filtr bilan qidiruv boshlandi...")
    await cb.answer()
    await start_search(uid, cb.message)


# =========================
# Anketa flow
# =========================
@dp.callback_query(F.data.startswith("anketa_gender:"))
async def anketa_gender(cb: CallbackQuery, state: FSMContext):
    gender = cb.data.split(":", 1)[1]
    await state.update_data(gender=gender)
    await safe_edit_text(cb.message, "Yoshingizni kiriting (masalan 20).", reply_markup=None)
    await state.set_state(Anketa.age)
    await cb.answer()

@dp.message(Anketa.age)
async def anketa_age(m: Message, state: FSMContext):
    try:
        age = int(m.text.strip())
    except Exception:
        await m.answer("Faqat son kiriting. Masalan: 20")
        return
    if not (MIN_AGE <= age <= MAX_AGE):
        await m.answer(f"Yosh {MIN_AGE}–{MAX_AGE} oralig‘ida bo‘lsin.")
        return

    await state.update_data(age=age)
    await m.answer("Viloyatingizni tanlang:", reply_markup=kb_regions("anketa_region", page=0))
    await state.set_state(Anketa.region)

@dp.callback_query(F.data.startswith("anketa_region_page:"))
async def anketa_region_page(cb: CallbackQuery):
    page = int(cb.data.split(":", 1)[1])
    await safe_edit_markup(cb.message, reply_markup=kb_regions("anketa_region", page=page))
    await cb.answer()

@dp.callback_query(F.data.startswith("anketa_region:"))
async def anketa_region(cb: CallbackQuery, state: FSMContext):
    region = cb.data.split(":", 1)[1]
    data = await state.get_data()
    uid = cb.from_user.id

    prof = Profile(gender=data["gender"], age=data["age"], region=region)
    await add_user(uid)
    await set_profile(uid, prof)
    await state.clear()

    await safe_edit_text(
        cb.message,
        f"✅ Anketa saqlandi:\nJins: {prof.gender}\nYosh: {prof.age}\nViloyat: {prof.region}\n\n/search bosing.",
        reply_markup=None
    )
    await cb.answer()


# =========================
# Chat actions: Block / Report
# =========================
async def do_block(m: Message):
    uid = m.from_user.id
    pid = await get_partner(uid)
    if not pid:
        await m.answer("Siz chatda emassiz.", reply_markup=idle_menu_kb())
        return

    await add_block(uid, pid)
    await end_chat(uid)

    await m.answer("✅ Sherik bloklandi. /search bilan yangi sherik toping.", reply_markup=idle_menu_kb())
    await bot.send_message(pid, "Sherik chatni yakunladi.", reply_markup=idle_menu_kb())

async def start_report(m: Message, state: FSMContext):
    uid = m.from_user.id
    pid = await get_partner(uid)
    if not pid:
        await m.answer("Siz chatda emassiz.", reply_markup=idle_menu_kb())
        return
    await m.answer("Shikoyat sababini tanlang:", reply_markup=kb_report_reasons())
    await state.set_state(ReportFlow.reason)

@dp.callback_query(F.data.startswith("report_reason:"))
async def report_reason(cb: CallbackQuery, state: FSMContext):
    reason = cb.data.split(":", 1)[1]
    await state.update_data(reason=reason)
    await safe_edit_text(cb.message, "Izoh yozing (ixtiyoriy). Bo‘sh qoldirish uchun '-' yuboring.", reply_markup=None)
    await state.set_state(ReportFlow.comment)
    await cb.answer()

@dp.message(ReportFlow.comment)
async def report_comment(m: Message, state: FSMContext):
    uid = m.from_user.id
    pid = await get_partner(uid)
    data = await state.get_data()
    reason = data.get("reason", "Boshqa")
    comment = m.text.strip()
    if comment == "-":
        comment = ""

    report = {
        "ts": now_ts(),
        "from_uid": uid,
        "against_uid": pid,
        "reason": reason,
        "comment": comment
    }
    await push_report(report)
    await state.clear()

    await m.answer("✅ Shikoyat yuborildi. /search bilan davom eting.", reply_markup=idle_menu_kb())


# =========================
# Relay (anonymous)
# =========================
@dp.message(F.content_type.in_({"text", "photo", "video", "audio", "voice", "document", "sticker"}))
async def relay(m: Message):
    uid = m.from_user.id
    pid = await get_partner(uid)
    if not pid:
        return

    # menu tugmalar orqali ham boshqariladi
    await bot.copy_message(chat_id=pid, from_chat_id=m.chat.id, message_id=m.message_id)


# =========================
# Admin
# =========================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    if not rds:
        await m.answer("Redis yo‘q. Admin statistikasi uchun Redis tavsiya.")
        return

    qlen = await rds.llen(K_QUEUE)
    users_count = await rds.scard(K_USERS)

    await m.answer(
        f"Admin panel\n\n"
        f"Users: {users_count}\n"
        f"Queue: {qlen}\n\n"
        f"Buyruqlar:\n"
        f"/reports - reportlar\n"
        f"/ban <user_id> <minutes> <reason>\n"
        f"/unban <user_id>\n"
        f"/broadcast <matn>"
    )

@dp.message(Command("reports"))
async def cmd_reports(m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    if not rds:
        await m.answer("Redis yo‘q.")
        return

    items = await rds.lrange(K_REPORTS, 0, 9)
    if not items:
        await m.answer("Reportlar yo‘q.")
        return

    text_lines = ["Oxirgi reportlar (10 ta):"]
    for i, raw in enumerate(items, 1):
        try:
            d = json.loads(raw)
            text_lines.append(
                f"{i}) from={d.get('from_uid')} against={d.get('against_uid')} reason={d.get('reason')} comment={d.get('comment','')}"
            )
        except Exception:
            text_lines.append(f"{i}) (parse error)")
    await m.answer("\n".join(text_lines))

@dp.message(Command("ban"))
async def cmd_ban(m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    parts = m.text.split(maxsplit=3)
    if len(parts) < 3:
        await m.answer("Format: /ban <user_id> <minutes> <reason(ixtiyoriy)>")
        return

    if not parts[1].isdigit() or not parts[2].isdigit():
        await m.answer("user_id va minutes son bo‘lsin.")
        return

    target = int(parts[1])
    minutes = int(parts[2])
    reason = parts[3] if len(parts) >= 4 else ""
    until = now_ts() + minutes * 60
    await ban_user(target, until, reason)

    await m.answer(f"✅ Ban qilindi: {target} ({minutes} min).")
    try:
        await bot.send_message(target, f"Siz ban qilindingiz ({minutes} min). {reason}")
    except Exception:
        pass

@dp.message(Command("unban"))
async def cmd_unban(m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    parts = m.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Format: /unban <user_id>")
        return

    target = int(parts[1])
    await unban_user(target)
    await m.answer(f"✅ Unban: {target}")

@dp.message(Command("broadcast"))
async def cmd_broadcast(m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    text = m.text.replace("/broadcast", "", 1).strip()
    if not text:
        await m.answer("Format: /broadcast <matn>")
        return
    if not rds:
        await m.answer("Redis yo‘q. Broadcast uchun Redis kerak.")
        return

    ids = await rds.smembers(K_USERS)
    sent = 0
    fail = 0
    for raw in ids:
        if not str(raw).isdigit():
            continue
        to_uid = int(raw)
        try:
            await bot.send_message(to_uid, f"📢 {text}")
            sent += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.03)  # Telegram limitlarga yumshoq

    await m.answer(f"Broadcast yakunlandi. Sent={sent}, Fail={fail}")


# =========================
# Main
# =========================
async def main():
    # init dbs
    try:
        await init_redis()
        print("Redis: OK" if rds else "Redis: OFF")
    except Exception as e:
        print("Redis init error:", e)
        # Redis bo'lmasa ham bot ishga tushadi, lekin queue/match/admin/broadcast cheklanadi
        # Siz professional ishlatmoqchi bo'lsangiz, Redisni albatta yoqing.
        pass

    try:
        await init_postgres()
        print("Postgres: OK" if pg_pool else "Postgres: OFF")
    except Exception as e:
        print("Postgres init error:", e)
        pass

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
