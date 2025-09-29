import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.types import FSInputFile
from aiogram.filters import CommandStart, Command
from aiohttp import web
import asyncio
from datetime import datetime
from collections import defaultdict
import os

# === Конфиг ===
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL")  # Render задаёт автоматически
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53

# Память
day_overrides = {}
unit_sessions = {}

def get_day_number(today: datetime) -> int:
    delta = (today - BASE_DATE).days
    return BASE_NUMBER + delta

def get_output_filename(file_name: str, day_number: int):
    if "MFO5" in file_name:
        return f"Б0 ({day_number}).txt", "Б0"
    elif "6_web" in file_name:
        return None, "6_web"
    elif "253" in file_name:
        return f"253 ({day_number}).txt", "253"
    elif "345" in file_name:
        return f"345 ({day_number}).txt", "345"
    elif "389" in file_name:
        return f"Н1 ({day_number}).txt", "Н1"
    elif "390" in file_name:
        return f"Н2 ({day_number}).txt", "Н2"
    else:
        return None, None

async def process_csv(file_path: str, day_number: int):
    output_data = defaultdict(list)
    file_stats = []
    group_stats = defaultdict(int)
    total_lines = 0
    try:
        df = pd.read_csv(file_path)
        fname = os.path.basename(file_path)

        if 'phone' not in df.columns:
            return None, f"❌ В файле {fname} нет столбца 'phone'"

        output_name, group_key = get_output_filename(fname, day_number)

        if group_key == "6_web":
            if 'channel_id' not in df.columns:
                return None, f"❌ В файле {fname} нет столбца 'channel_id'"

            local_stats = defaultdict(int)
            for _, row in df.iterrows():
                phone = str(row['phone']).replace("+", "").strip()
                if not phone:
                    continue
                ch = str(row['channel_id']).strip()
                if ch == "15883":
                    group = "ББ"
                elif ch == "15686":
                    group = "ББ ДОП_1"
                elif ch == "15273":
                    group = "ББ ДОП_2"
                else:
                    group = "ББ ДОП_3"
                output_data[group].append(phone)
                group_stats[group] += 1
                local_stats[group] += 1
                total_lines += 1
            details = ", ".join([f"{g}: {c}" for g, c in local_stats.items()])
            file_stats.append(f"{fname}: {sum(local_stats.values())} строк → {details}")
        else:
            phones = df['phone'].dropna().astype(str)
            cleaned_phones = [p.replace("+", "").strip() for p in phones if p.strip()]
            lines_count = len(cleaned_phones)
            total_lines += lines_count
            if output_name:
                output_data[group_key].extend(cleaned_phones)
                group_stats[group_key] += lines_count
                file_stats.append(f"{fname}: {lines_count} строк → {group_key}")
            else:
                file_stats.append(f"{fname}: пропущен (не распознан)")

        saved_files = []
        for group_key, phones in output_data.items():
            filename = f"{group_key} ({day_number}).txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write("\n".join(phones))
            saved_files.append(filename)

        stats = "\n".join(file_stats)
        summary = "\n".join([f"{k}: {v} строк" for k, v in group_stats.items()])
        stats += f"\n\n{summary}\nВсего: {total_lines} строк"
        return saved_files, stats
    except Exception as e:
        return None, f"❌ Ошибка при обработке {file_path}: {e}"

# === Telegram Bot ===
bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: types.Message):
    await message.answer(
        "Привет 👋\n"
        "Пришли CSV файл, и я сделаю TXT с номерами.\n\n"
        "Команды:\n"
        "• /day <число> — задать номер дня вручную\n"
        "• /unit [число] — объединить 2 TXT в Б1 (номер дня)"
    )

@dp.message(Command("day"))
async def set_day(message: types.Message):
    user_id = message.from_user.id
    try:
        day_number = int(message.text.split()[1])
        day_overrides[user_id] = day_number
        await message.answer(f"✅ Номер дня установлен: {day_number}")
    except:
        await message.answer("❌ Используй: /day <число>")

@dp.message(Command("unit"))
async def start_unit(message: types.Message):
    user_id = message.from_user.id
    parts = message.text.split()
    if len(parts) > 1:
        try:
            day_number = int(parts[1])
        except:
            return await message.answer("❌ Используй: /unit <число>")
    else:
        today = datetime.today()
        day_number = get_day_number(today)
    unit_sessions[user_id] = {"files": [], "day": day_number}
    await message.answer(
        f"📂 Режим объединения активирован.\n"
        f"Пришли 2 TXT файла, и я соберу их в один `Б1 ({day_number}).txt`"
    )

@dp.message()
async def handle_files(message: types.Message):
    user_id = message.from_user.id
    if user_id in unit_sessions:
        if not message.document:
            return await message.answer("❌ Жду файл в формате TXT")
        file = message.document
        if not file.file_name.endswith(".txt"):
            return await message.answer("❌ Поддерживаются только TXT файлы.")
        file_path = f"temp_{user_id}_{len(unit_sessions[user_id]['files'])}.txt"
        await bot.download(file, destination=file_path)
        unit_sessions[user_id]["files"].append(file_path)
        if len(unit_sessions[user_id]["files"]) == 2:
            day_number = unit_sessions[user_id]["day"]
            output_name = f"Б1 ({day_number}).txt"
            phones = []
            for f in unit_sessions[user_id]["files"]:
                with open(f, encoding="utf-8") as fh:
                    phones.extend([line.strip() for line in fh if line.strip()])
                os.remove(f)
            with open(output_name, "w", encoding="utf-8") as out:
                out.write("\n".join(phones))
            await message.answer_document(FSInputFile(output_name))
            os.remove(output_name)
            del unit_sessions[user_id]
        else:
            await message.answer("✅ Первый файл получен. Жду второй...")
        return
    if not message.document:
        return await message.answer("📂 Пришли CSV файл.")
    file = message.document
    if not file.file_name.endswith(".csv"):
        return await message.answer("❌ Поддерживаются только CSV файлы.")
    file_path = f"temp_{file.file_name}"
    await bot.download(file, destination=file_path)
    today = datetime.today()
    day_number = day_overrides.get(user_id, get_day_number(today))
    saved_files, stats = await process_csv(file_path, day_number)
    if not saved_files:
        await message.answer(stats)
    else:
        await message.answer(f"✅ Обработка завершена:\n{stats}")
        for fname in saved_files:
            await message.answer_document(FSInputFile(fname))
            os.remove(fname)
    os.remove(file_path)

# === Webhook сервер ===
async def on_startup(app: web.Application):
    print(f"Устанавливаю webhook {WEBHOOK_URL}")
    await bot.set_webhook(WEBHOOK_URL)

async def on_shutdown(app: web.Application):
    await bot.delete_webhook()
    await bot.session.close()

def main():
    app = web.Application()

    # создаём обработчик webhook для aiogram
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)

    # навешиваем стартап/шутдаун
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

if __name__ == "__main__":
    main()
