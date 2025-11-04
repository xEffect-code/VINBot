import os
import json
import asyncio
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

# ───────── config
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MOD_CHAT_ID = int(os.getenv("MOD_CHAT_ID", "0"))
MOD_TOPIC_ID = int(os.getenv("MOD_TOPIC_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ASSISTANT_IDS = {int(x) for x in os.getenv("ASSISTANT_IDS", "").split(",") if x.strip()}
DATA_PATH = os.getenv("DATA_PATH", "data.json")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ───────── storage
_LOCK = asyncio.Lock()

def now_iso() -> str:
    return datetime.utcnow().isoformat()

def norm_vin(v: str) -> str:
    return "".join((v or "").upper().split())

def is_valid_vin(v: str) -> bool:
    v = norm_vin(v)
    if len(v) != 17:
        return False
    bad = set("IOQ")
    return all(ch.isalnum() and ch not in bad for ch in v)

class Status:
    NEW = "NEW"
    APPROVED = "APPROVED"
    SHIPPED = "SHIPPED"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"

class Store:
    def __init__(self, path: str):
        self.path = path
        self.data = {
            "meta": {"next_app_id": 1},
            "users": {},
            "applications": {},
            "events": []
        }

    async def load(self):
        async with _LOCK:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)

    async def save(self):
        async with _LOCK:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)

    async def upsert_user(self, u):
        uid = str(u.id)
        cur = self.data["users"].get(uid, {})
        cur.update({
            "id": u.id,
            "username": u.username,
            "first_name": u.first_name,
            "last_name": u.last_name,
            "created_at": cur.get("created_at") or now_iso()
        })
        self.data["users"][uid] = cur
        await self.save()
        return cur

    def get_user(self, user_id: int):
        return self.data["users"].get(str(user_id))

    def _next_id(self) -> int:
        nid = self.data["meta"].get("next_app_id", 1)
        self.data["meta"]["next_app_id"] = nid + 1
        return nid

    def find_by_vin(self, vin_norm: str) -> list[dict]:
        return [a for a in self.data["applications"].values() if a["vin_norm"] == vin_norm]

    def get_app(self, app_id: int) -> Optional[dict]:
        return self.data["applications"].get(str(app_id))

    async def create_app(self, payload: dict) -> dict:
        for a in self.find_by_vin(payload["vin_norm"]):
            if a["status"] != Status.REJECTED:
                raise ValueError("VIN already used by active application")
        app_id = self._next_id()
        app = {
            "id": app_id,
            "vin_norm": payload["vin_norm"],
            "vin_raw": payload["vin_raw"],
            "photo_reg_file_id": payload.get("photo_reg_file_id"),
            "photo_vin_file_id": payload.get("photo_vin_file_id"),
            "full_name": payload["full_name"],
            # phone — старое поле, заполним телефоном получателя
            "phone": payload["receiver_phone"],
            "owner_phone": payload["owner_phone"],
            "receiver_phone": payload["receiver_phone"],
            "sdek_address": payload["sdek_address"],
            "client_id": payload["client_id"],
            "status": Status.NEW,
            "created_at": now_iso(),
            "mod_chat_message_id": None,
            "approved_by": None,
            "approved_at": None,
            "shipped_by": None,
            "shipped_at": None,
            "tracking_number": None,
            "tracking_photo_file_id": None
        }
        self.data["applications"][str(app_id)] = app
        await self.save()
        return app

    async def update_app(self, app: dict):
        self.data["applications"][str(app["id"])] = app
        await self.save()

    async def add_event(self, app_id: int, actor_id: int, action: str, data: str = ""):
        self.data["events"].append({
            "app_id": app_id,
            "ts": now_iso(),
            "actor_id": actor_id,
            "action": action,
            "data": data
        })
        await self.save()

    def export_rows(self):
        return list(self.data["applications"].values())

store = Store(DATA_PATH)

# ───────── FSM
class NewApp(StatesGroup):
    VIN = State()
    PHOTOS = State()
    FULLNAME = State()
    OWNER_PHONE = State()
    RECEIVER_PHONE = State()
    ADDRESS = State()
    CONFIRM = State()

class ShipFlow(StatesGroup):
    WAIT_TRACK_PHOTO = State()

class RejectFlow(StatesGroup):
    WAIT_COMMENT = State()

# ───────── keyboards / helpers for keyboards
def kb_user_confirm():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="usr_send"),
         InlineKeyboardButton(text="↩️ Изменить", callback_data="usr_edit")]
    ])

def kb_photos_done():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Фото готово", callback_data="photos_done")]
    ])

def kb_mod_start(app_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"appr:{app_id}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"rej:{app_id}")]
    ])

def kb_mod_after_approve(app_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Отправить фото с треком", callback_data=f"ship:{app_id}")]
    ])

def kb_mod_after_ship(app_id: int):
    # после отправки — только кнопка "показать фото"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Фото с треком", callback_data=f"viewtrack:{app_id}")]
    ])

def kb_back_to_app(app_id: int, status: str):
    # когда смотрим фото трека — нужна кнопка вернуться
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Назад к заявке", callback_data=f"backapp:{app_id}")]
    ])

# ───────── helpers
def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

def is_assistant(uid: int) -> bool:
    return uid in ASSISTANT_IDS or is_owner(uid)

async def is_chat_admin_or_assistant(user_id: int, chat_id: int) -> bool:
    if is_assistant(user_id):
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in ("administrator", "creator")

def phone_ok(s: str) -> bool:
    s = s.strip()
    if len(s) < 10 or len(s) > 18:
        return False
    allowed = set("+0123456789-() ")
    return all(ch in allowed for ch in s)

def thread_kwargs():
    return {"message_thread_id": MOD_TOPIC_ID} if MOD_TOPIC_ID > 0 else {}

def build_app_caption(app: dict) -> str:
    u = store.get_user(app["client_id"]) or {}
    return (
        f"🆕 <b>Заявка #{app['id']}</b>\n"
        f"VIN: <code>{app['vin_raw']}</code>\n"
        f"Клиент: @{u.get('username') or '—'} (id {app['client_id']})\n"
        f"ФИО: {app['full_name']}\n"
        f"Телефон владельца: {app.get('owner_phone') or '—'}\n"
        f"Телефон получателя СДЭК: {app.get('receiver_phone') or app.get('phone') or '—'}\n"
        f"Адрес СДЭК: {app['sdek_address']}"
    )

def app_keyboard_for_status(app: dict) -> InlineKeyboardMarkup:
    if app["status"] == Status.NEW:
        return kb_mod_start(app["id"])
    if app["status"] == Status.APPROVED:
        return kb_mod_after_approve(app["id"])
    if app["status"] == Status.SHIPPED:
        return kb_mod_after_ship(app["id"])
    return InlineKeyboardMarkup(inline_keyboard=[])

async def send_card_as_photo(app: dict, keyboard: InlineKeyboardMarkup) -> Message:
    caption = build_app_caption(app)
    photo_id = app.get("photo_reg_file_id") or app.get("photo_vin_file_id")
    if photo_id:
        return await bot.send_photo(
            MOD_CHAT_ID,
            photo=photo_id,
            caption=caption,
            reply_markup=keyboard,
            **thread_kwargs()
        )
    return await bot.send_message(MOD_CHAT_ID, caption, reply_markup=keyboard, **thread_kwargs())

# ───────── startup
@dp.startup()
async def on_startup():
    await store.load()
    print(f"➡️ storage loaded | MOD_CHAT_ID={MOD_CHAT_ID} | MOD_TOPIC_ID={MOD_TOPIC_ID}")

# ───────── client flow
@dp.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await store.upsert_user(m.from_user)
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text="🧩 Оформить заявку")]
    ])
    await m.answer("Привет! Давай оформим заявку на отправку. Нажми «Оформить заявку».", reply_markup=kb)

@dp.message(F.text == "🧩 Оформить заявку")
async def ask_vin(m: Message, state: FSMContext):
    await state.set_state(NewApp.VIN)
    await m.answer("Шаг 1/6. Введи VIN (17 символов, без I/O/Q).", reply_markup=ReplyKeyboardRemove())

@dp.message(NewApp.VIN, F.text)
async def take_vin(m: Message, state: FSMContext):
    vin_raw = m.text.strip()
    if not is_valid_vin(vin_raw):
        await m.answer("Похоже, VIN некорректен. Проверь и отправь снова.")
        return
    vnorm = norm_vin(vin_raw)
    for a in store.find_by_vin(vnorm):
        if a["status"] != Status.REJECTED:
            await m.answer("❗️По этому VIN уже есть активная заявка. Подожди решение или напиши нам.")
            return
    await state.update_data(vin_raw=vin_raw, vin_norm=vnorm, photos=[])
    await state.set_state(NewApp.PHOTOS)
    await m.answer(
        "Шаг 2/6. Пришли фото:\n1) Страница СТС с VIN\n2) Табличка VIN на авто\n"
        "Можно одним фото, где видны оба. Когда закончишь — нажми кнопку ниже.",
        reply_markup=kb_photos_done()
    )

@dp.message(NewApp.PHOTOS, F.photo)
async def take_photos(m: Message, state: FSMContext):
    d = await state.get_data()
    ph = d.get("photos", [])
    ph.append(m.photo[-1].file_id)
    await state.update_data(photos=ph)
    await m.answer("Фото принято. Если есть ещё — пришли. Когда закончишь — нажми «✅ Фото готово».", reply_markup=kb_photos_done())

@dp.callback_query(F.data == "photos_done")
async def photos_done(c: CallbackQuery, state: FSMContext):
    await c.answer()
    d = await state.get_data()
    if not d.get("photos"):
        await c.message.answer("Нужно минимум одно фото, где видны документ и табличка VIN. Пришли фото.")
        return
    await state.set_state(NewApp.FULLNAME)
    await c.message.answer("Шаг 3/6. Напиши ФИО получателя (как в документе).")

@dp.message(NewApp.FULLNAME, F.text)
async def take_fullname(m: Message, state: FSMContext):
    await state.update_data(full_name=m.text.strip())
    await state.set_state(NewApp.OWNER_PHONE)
    await m.answer("Шаг 4/6. Пришли телефон <b>владельца автомобиля</b> (только текстом, напр. +7 ...).", reply_markup=ReplyKeyboardRemove())

@dp.message(NewApp.OWNER_PHONE, F.text)
async def take_owner_phone(m: Message, state: FSMContext):
    phone = m.text.strip()
    if not phone_ok(phone):
        await m.answer("Телефон выглядит странно. Пришли в формате +7XXXXXXXXXX (или с пробелами/скобками).")
        return
    await state.update_data(owner_phone=phone)
    await state.set_state(NewApp.RECEIVER_PHONE)
    await m.answer("Шаг 5/6. Пришли телефон <b>получателя СДЭК</b> (куда будет приходить заказ).")

@dp.message(NewApp.RECEIVER_PHONE, F.text)
async def take_receiver_phone(m: Message, state: FSMContext):
    phone = m.text.strip()
    if not phone_ok(phone):
        await m.answer("Телефон выглядит странно. Пришли в формате +7XXXXXXXXXX (или с пробелами/скобками).")
        return
    await state.update_data(receiver_phone=phone)
    await ask_address(m, state)

async def ask_address(m: Message, state: FSMContext):
    await state.set_state(NewApp.ADDRESS)
    await m.answer("Шаг 6/6. Укажи адрес СДЭК (ПВЗ/постамат/курьер): город, улица, дом, контакты.", reply_markup=ReplyKeyboardRemove())

@dp.message(NewApp.ADDRESS, F.text)
async def take_address(m: Message, state: FSMContext):
    await state.update_data(sdek_address=m.text.strip())
    d = await state.get_data()
    txt = (f"<b>Проверь данные:</b>\n"
           f"VIN: <code>{d['vin_raw']}</code>\n"
           f"ФИО: {d['full_name']}\n"
           f"Телефон владельца: {d['owner_phone']}\n"
           f"Телефон получателя СДЭК: {d['receiver_phone']}\n"
           f"Адрес СДЭК: {d['sdek_address']}\n\n"
           f"Если всё ок — жми «✅ Отправить».")
    await state.set_state(NewApp.CONFIRM)
    await m.answer(txt, reply_markup=kb_user_confirm())

@dp.callback_query(F.data == "usr_edit")
async def usr_edit(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await ask_vin(c.message, state)

@dp.callback_query(F.data == "usr_send")
async def usr_send(c: CallbackQuery, state: FSMContext):
    await c.answer()
    d = await state.get_data()
    for a in store.find_by_vin(d["vin_norm"]):
        if a["status"] != Status.REJECTED:
            await c.message.answer("❗️По этому VIN уже есть активная заявка. Новую создать нельзя.")
            return
    await store.upsert_user(c.from_user)
    photos = d.get("photos", [])
    payload = {
        "vin_norm": d["vin_norm"],
        "vin_raw": d["vin_raw"],
        "full_name": d["full_name"],
        "owner_phone": d["owner_phone"],
        "receiver_phone": d["receiver_phone"],
        "sdek_address": d["sdek_address"],
        "client_id": c.from_user.id,
        "photo_reg_file_id": photos[0] if photos else None,
        "photo_vin_file_id": photos[1] if len(photos) > 1 else None
    }
    try:
        app = await store.create_app(payload)
    except ValueError:
        await c.message.answer("❗️По этому VIN уже есть активная заявка.")
        return

    try:
        msg = await send_card_as_photo(app, kb_mod_start(app["id"]))
        app["mod_chat_message_id"] = msg.message_id
        await store.update_app(app)

        # если есть второе фото — отправляем сразу следом (НЕ реплаем)
        second = None
        if app["photo_reg_file_id"] and app["photo_vin_file_id"]:
            second = app["photo_vin_file_id"]
        elif app["photo_vin_file_id"]:
            second = None
        if second:
            await bot.send_photo(MOD_CHAT_ID, second, caption="Доп. фото", **thread_kwargs())

    except Exception as e:
        try:
            await bot.send_message(OWNER_ID, f"❗️Не могу отправить карточку в админ-чат ({MOD_CHAT_ID}). Ошибка:\n<code>{e}</code>")
        except:
            pass

    await c.message.answer("Заявка принята на проверку ✅. Мы скоро ответим сюда.")
    await state.clear()

# ───────── admin actions
def only_owner(cb: CallbackQuery) -> bool:
    return cb.from_user.id == OWNER_ID

@dp.callback_query(F.data.startswith("appr:"))
async def cb_approve(c: CallbackQuery):
    # только владелец может принять
    if c.from_user.id != OWNER_ID:
        await c.answer("Недостаточно прав", show_alert=True)
        return
    app_id = int(c.data.split(":")[1])
    app = store.get_app(app_id)
    if not app:
        return
    if app["status"] in {Status.REJECTED, Status.CLOSED, Status.SHIPPED}:
        await c.answer("Нельзя принять этот статус", show_alert=True)
        return

    app["status"] = Status.APPROVED
    app["approved_by"] = c.from_user.id
    app["approved_at"] = now_iso()
    await store.update_app(app)
    await store.add_event(app_id, c.from_user.id, "APPROVE", "")

    try:
        await bot.edit_message_reply_markup(
            chat_id=MOD_CHAT_ID,
            message_id=app["mod_chat_message_id"],
            reply_markup=kb_mod_after_approve(app_id)
        )
    except:
        pass

    await bot.send_message(app["client_id"], f"✅ Ваша заявка №{app_id} подтверждена. Ожидайте — скоро отправим и пришлём фото с треком.")
    await c.answer("Принято ✅")

@dp.callback_query(F.data.startswith("rej:"))
async def cb_reject(c: CallbackQuery, state: FSMContext):
    # только владелец может отклонить
    if c.from_user.id != OWNER_ID:
        await c.answer("Недостаточно прав", show_alert=True)
        return
    app_id = int(c.data.split(":")[1])
    app = store.get_app(app_id)
    if not app:
        return
    if app["status"] in {Status.REJECTED, Status.CLOSED}:
        await c.answer("Уже отклонено/закрыто", show_alert=True)
        return

    # просим владелца написать причину
    await state.set_state(RejectFlow.WAIT_COMMENT)
    await state.update_data(reject_app_id=app_id)
    prompt_msg = await c.message.reply("Напишите причину отказа 👇")
    await state.update_data(reject_prompt_msg_id=prompt_msg.message_id)
    await c.answer()

@dp.message(RejectFlow.WAIT_COMMENT, F.text)
async def reject_comment_take(m: Message, state: FSMContext):
    # только OWNER сюда должен писать
    if m.from_user.id != OWNER_ID:
        await m.reply("Только владелец может указывать причину отказа.")
        return
    d = await state.get_data()
    app_id = d.get("reject_app_id")
    prompt_id = d.get("reject_prompt_msg_id")
    app = store.get_app(int(app_id)) if app_id else None
    if not app:
        await m.reply("Заявка не найдена.")
        await state.clear()
        return

    comment = m.text.strip()

    # ставим статус
    app["status"] = Status.REJECTED
    await store.update_app(app)
    await store.add_event(app["id"], m.from_user.id, "REJECT", comment)

    # отправляем пользователю с комментом
    await bot.send_message(
        app["client_id"],
        f"❌ Ваша заявка №{app['id']} отклонена.\nКомментарий: {comment}"
    )

    # удаляем карточку и комментарий из мод-чата
    try:
        if app.get("mod_chat_message_id"):
            await bot.delete_message(MOD_CHAT_ID, app["mod_chat_message_id"])
    except:
        pass
    try:
        # удаляем сообщение с подсказкой
        if prompt_id:
            await bot.delete_message(m.chat.id, prompt_id)
    except:
        pass
    try:
        # удаляем сам комментарий OWNER
        await bot.delete_message(m.chat.id, m.message_id)
    except:
        pass

    await state.clear()

@dp.callback_query(F.data.startswith("ship:"))
async def cb_ship(c: CallbackQuery, state: FSMContext):
    # вот тут — любой админ/ассистент
    if not await is_chat_admin_or_assistant(c.from_user.id, c.message.chat.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return
    app_id = int(c.data.split(":")[1])
    app = store.get_app(app_id)
    if not app or app["status"] != Status.APPROVED:
        await c.answer("Заявка не в статусе APPROVED", show_alert=True)
        return
    await state.update_data(ship_app_id=app_id)
    await state.set_state(ShipFlow.WAIT_TRACK_PHOTO)
    prompt_msg = await c.message.reply("Пришли фото с треком/квитанцией СДЭК. Без фото отправку не зафиксируем.")
    await state.update_data(ship_prompt_msg_id=prompt_msg.message_id)
    await c.answer()

@dp.message(ShipFlow.WAIT_TRACK_PHOTO, F.photo)
async def ship_track_photo(m: Message, state: FSMContext):
    await finalize_shipping(m, state, m.photo[-1].file_id)

@dp.message(ShipFlow.WAIT_TRACK_PHOTO)
async def ship_track_photo_only_photo(m: Message, state: FSMContext):
    await m.reply("Нужно прислать именно фото с треком 📸")

async def finalize_shipping(m: Message, state: FSMContext, photo_id: Optional[str]):
    d = await state.get_data()
    app_id = int(d.get("ship_app_id"))
    prompt_id = d.get("ship_prompt_msg_id")
    app = store.get_app(app_id)
    if not app:
        await m.reply("Заявка не найдена.")
        return

    app["status"] = Status.SHIPPED
    app["shipped_by"] = m.from_user.id
    app["shipped_at"] = now_iso()
    app["tracking_number"] = None
    app["tracking_photo_file_id"] = photo_id
    await store.update_app(app)
    await store.add_event(app_id, m.from_user.id, "SHIP", "PHOTO")

    # клиенту — как раньше
    await bot.send_message(app["client_id"], f"📦 Ваша заявка №{app_id} отправлена. Фото трека ниже.")
    if photo_id:
        await bot.send_photo(app["client_id"], photo_id, caption="Фото квитанции/трек СДЭК")

    # карточке в мод-чате ставим кнопку "Фото с треком"
    try:
        await bot.edit_message_reply_markup(
            chat_id=MOD_CHAT_ID,
            message_id=app["mod_chat_message_id"],
            reply_markup=kb_mod_after_ship(app_id)
        )
    except:
        pass

    # удаляем служебные сообщения из мод-чата
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except:
        pass
    if prompt_id:
        try:
            await bot.delete_message(m.chat.id, prompt_id)
        except:
            pass

    # администратору — в личку что всё ок
    try:
        await bot.send_message(m.from_user.id, f"Отправка по заявке #{app_id} с фото зафиксирована ✅")
    except:
        pass

    await state.clear()

# показать фото трека в самой карточке
@dp.callback_query(F.data.startswith("viewtrack:"))
async def cb_viewtrack(c: CallbackQuery):
    if not await is_chat_admin_or_assistant(c.from_user.id, c.message.chat.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return
    app_id = int(c.data.split(":")[1])
    app = store.get_app(app_id)
    if not app:
        await c.answer("Заявка не найдена", show_alert=True)
        return
    photo_id = app.get("tracking_photo_file_id")
    if not photo_id:
        await c.answer("Фото трека не найдено", show_alert=True)
        return

    media = InputMediaPhoto(media=photo_id, caption=f"📸 Фото трека по заявке #{app_id}")
    try:
        await bot.edit_message_media(
            chat_id=MOD_CHAT_ID,
            message_id=app["mod_chat_message_id"],
            media=media,
            reply_markup=kb_back_to_app(app_id, app["status"])
        )
    except Exception as e:
        await c.answer(f"Не могу показать фото: {e}", show_alert=True)
        return

    await c.answer()

# вернуть карточку обратно
@dp.callback_query(F.data.startswith("backapp:"))
async def cb_backapp(c: CallbackQuery):
    if not await is_chat_admin_or_assistant(c.from_user.id, c.message.chat.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return
    app_id = int(c.data.split(":")[1])
    app = store.get_app(app_id)
    if not app:
        await c.answer("Заявка не найдена", show_alert=True)
        return

    caption = build_app_caption(app)
    main_photo = app.get("photo_reg_file_id") or app.get("photo_vin_file_id")
    kb = app_keyboard_for_status(app)

    try:
        if main_photo:
            media = InputMediaPhoto(media=main_photo, caption=caption)
            await bot.edit_message_media(
                chat_id=MOD_CHAT_ID,
                message_id=app["mod_chat_message_id"],
                media=media,
                reply_markup=kb
            )
        else:
            await bot.edit_message_caption(
                chat_id=MOD_CHAT_ID,
                message_id=app["mod_chat_message_id"],
                caption=caption,
                reply_markup=kb
            )
    except:
        pass

    await c.answer()

# ───────── utils
@dp.message(Command("whereami"))
async def whereami(m: Message):
    await m.answer(f"chat.id = <code>{m.chat.id}</code>\nchat.type = <code>{m.chat.type}</code>")

@dp.message(Command("test_mod"))
async def test_mod(m: Message):
    try:
        await bot.send_message(MOD_CHAT_ID, "Тест: бот может писать в мод-чат ✅", **thread_kwargs())
        await m.answer("Ок: сообщение отправлено в мод-чат.")
    except Exception as e:
        await m.answer(f"Не удалось отправить в MOD_CHAT_ID={MOD_CHAT_ID}\nОшибка: <code>{e}</code>")

@dp.message(Command("export_csv"))
async def export_csv(m: Message):
    if not is_owner(m.from_user.id):
        return
    import csv
    path = "export.csv"
    rows = store.export_rows()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "id", "vin", "status", "fio",
            "owner_phone", "receiver_phone",
            "phone_legacy",
            "sdek", "client_id", "username",
            "created_at", "track"
        ])
        for r in rows:
            u = store.get_user(r["client_id"]) or {}
            w.writerow([
                r["id"],
                r["vin_raw"],
                r["status"],
                r["full_name"],
                r.get("owner_phone", ""),
                r.get("receiver_phone", ""),
                r.get("phone", ""),
                r["sdek_address"],
                r["client_id"],
                u.get("username", ""),
                r.get("created_at", ""),
                r.get("tracking_number") or ""
            ])
    await m.reply_document(open(path, "rb"))

# ───────── run
if __name__ == "__main__":
    import sys
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        print("❗️ BOT_TOKEN отсутствует или неверен. Проверь .env"); sys.exit(1)
    import asyncio
    asyncio.run(dp.start_polling(bot))
