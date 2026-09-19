import asyncio
import os
import signal
import aiohttp
from pathlib import Path
from dotenv import load_dotenv

from pymax import WebClient, ExtraConfig
from pymax.types import (
    PhotoAttachment,
    VideoAttachment,
    FileAttachment,
    AudioAttachment,
)

# ---------- ЗАГРУЗКА .env ----------
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")


def _get_int(key: str, default: int) -> int:
    """Безопасно читает int из .env, с fallback на default."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        print(f"[ENV] {key}={value!r} — не число, использую {default}")
        return default


# ---------- ОБЯЗАТЕЛЬНЫЕ ПЕРЕМЕННЫЕ ----------
MAX_CHAT_ID = _get_int("MAX_CHAT_ID", 0)
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = _get_int("TG_CHAT_ID", 0)
TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"

if not TG_TOKEN or not MAX_CHAT_ID or not TG_CHAT_ID:
    raise SystemExit(
        "Не заданы обязательные переменные в .env:\n"
        "  MAX_CHAT_ID, TG_TOKEN, TG_CHAT_ID"
    )

# ---------- НАСТРОЙКИ СКАЧИВАНИЯ ----------
CHUNK_SIZE = _get_int("CHUNK_SIZE", 64 * 1024)
DOWNLOAD_RETRIES = _get_int("DOWNLOAD_RETRIES", 3)
DOWNLOAD_RETRY_DELAY = _get_int("DOWNLOAD_RETRY_DELAY", 2)

TEMP_DIR = BASE_DIR / "temp_media"
CACHE_DIR = BASE_DIR / "cache"
TEMP_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

bridge_queue: asyncio.Queue = asyncio.Queue()


# ============================================================
#  КЛИЕНТ MAX (QR-авторизация)
# ============================================================

max_client = WebClient(
    work_dir=str(CACHE_DIR),
    extra_config=ExtraConfig(log_level="INFO", reconnect=True),
)


# ============================================================
#  ОТПРАВКА В TELEGRAM
# ============================================================

async def send_text(text: str) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{TG_API}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": text},
        ) as resp:
            result = await resp.json()
            if not result.get("ok"):
                print(f"[TG] Ошибка текста: {result}")


async def send_file(path: Path, caption: str = "", is_voice: bool = False) -> None:
    ext = path.suffix.lower()

    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        method, field = "sendPhoto", "photo"
    elif ext in (".mp4", ".mov", ".avi", ".mkv"):
        method, field = "sendVideo", "video"
    elif is_voice or ext == ".ogg":
        method, field = "sendVoice", "voice"
    elif ext in (".mp3", ".wav", ".m4a", ".flac"):
        method, field = "sendAudio", "audio"
    else:
        method, field = "sendDocument", "document"

    if len(caption) > 1024:
        caption = caption[:1021] + "..."

    with open(path, "rb") as f:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(TG_CHAT_ID))
        if caption:
            form.add_field("caption", caption)
        form.add_field(field, f, filename=path.name)

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{TG_API}/{method}", data=form) as resp:
                result = await resp.json()
                if not result.get("ok"):
                    print(f"[TG] Ошибка {path.name}: {result}")
                else:
                    print(f"[TG] Отправлено: {path.name}")


async def telegram_sender():
    while True:
        task = await bridge_queue.get()
        try:
            if task["type"] == "text":
                await send_text(task["text"])
                print(f"[TG] Текст отправлен: {task['text'][:50]!r}")

            elif task["type"] == "file":
                path = Path(task["path"])
                await send_file(
                    path,
                    caption=task.get("caption", ""),
                    is_voice=task.get("is_voice", False),
                )
                path.unlink(missing_ok=True)

        except Exception as e:
            print(f"[TG] Ошибка отправки: {e}")
        finally:
            bridge_queue.task_done()


# ============================================================
#  ПРИЁМ ИЗ MAX
# ============================================================

async def download_to_file(url: str, path: Path) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            with open(path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)


async def download_with_retry(url: str, path: Path) -> bool:
    """Скачивает с ретраями — на случай падения коннекта pymax."""
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            await download_to_file(url, path)
            return True
        except Exception as e:
            print(f"[MAX] Попытка {attempt}/{DOWNLOAD_RETRIES} скачать {path.name} провалилась: {e}")
            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(DOWNLOAD_RETRY_DELAY)
    return False


def is_url_valid(url: str | None) -> bool:
    """
    True, если URL содержит путь к файлу, а не только домен.
    MAX иногда возвращает битые ссылки вида https://cdn.host/?params — их отсеиваем.
    """
    if not url or "?" not in url:
        return False
    path_part = url.split("?")[0]
    without_scheme = path_part.split("://", 1)[-1]
    if "/" not in without_scheme:
        return False
    _, _, file_path = without_scheme.partition("/")
    return bool(file_path)


async def get_download_url(attach, message, client: WebClient) -> str | None:
    # Фото — прямая ссылка сразу
    if isinstance(attach, PhotoAttachment):
        return attach.base_url

    # Видео — через API, с ретраем на битую ссылку
    if isinstance(attach, VideoAttachment):
        url = None
        for attempt in range(DOWNLOAD_RETRIES):
            info = await client.get_video_by_id(
                chat_id=message.chat_id,
                message_id=message.id,
                video_id=attach.video_id,
            )
            url = info.url if info else None
            if is_url_valid(url):
                return url
            print(f"[MAX] Битая ссылка на видео (попытка {attempt + 1}/{DOWNLOAD_RETRIES})")
            await asyncio.sleep(DOWNLOAD_RETRY_DELAY)
        return url

    # Аудио (голосовое или музыка) — url уже в объекте
    if isinstance(attach, AudioAttachment):
        url = getattr(attach, "url", None)
        if url:
            return url
        audio_id = getattr(attach, "audio_id", None)
        if audio_id and hasattr(client, "get_audio_by_id"):
            info = await client.get_audio_by_id(
                chat_id=message.chat_id,
                message_id=message.id,
                audio_id=audio_id,
            )
            return info.url if info else None
        return None

    # Файл — url, потом file_id
    if isinstance(attach, FileAttachment):
        url = getattr(attach, "url", None)
        if url:
            return url

        file_id = getattr(attach, "file_id", None)
        if not file_id:
            return None
        info = await client.get_file_by_id(
            chat_id=message.chat_id,
            message_id=message.id,
            file_id=file_id,
        )
        return info.url if info else None

    return None


@max_client.on_message()
async def on_max_message(message, client: WebClient):
    try:
        if message.chat_id != MAX_CHAT_ID:
            return

        caption = message.text or ""

        if not message.attaches:
            if caption:
                await bridge_queue.put({"type": "text", "text": caption})
            return

        for idx, attach in enumerate(message.attaches or []):
            try:
                url = await get_download_url(attach, message, client)
            except Exception as e:
                print(f"[MAX] Не удалось получить URL ({type(attach).__name__}): {e}")
                continue

            if not url:
                print(f"[MAX] URL пустой для {type(attach).__name__}")
                continue

            ext = ".bin"
            is_voice = False

            if isinstance(attach, PhotoAttachment):
                ext = ".jpg"
            elif isinstance(attach, VideoAttachment):
                ext = ".mp4"
            elif isinstance(attach, AudioAttachment):
                is_voice = bool(
                    getattr(attach, "wave", None)
                    or getattr(attach, "transcription_status", None)
                )
                ext = ".ogg" if is_voice else ".mp3"
            elif isinstance(attach, FileAttachment):
                name = getattr(attach, "name", "") or ""
                ext = Path(name).suffix or ".bin"

            filename = f"max_{message.id}_{idx}{ext}"
            filepath = TEMP_DIR / filename

            ok = await download_with_retry(url, filepath)
            if not ok:
                print(f"[MAX] Не удалось скачать {filename} после {DOWNLOAD_RETRIES} попыток")
                filepath.unlink(missing_ok=True)
                continue

            size_kb = filepath.stat().st_size / 1024
            print(f"[MAX] Скачано: {filename} ({size_kb:.1f} КБ)")

            await bridge_queue.put({
                "type": "file",
                "path": str(filepath),
                "caption": caption if idx == 0 else "",
                "is_voice": is_voice,
            })

    except Exception as e:
        print(f"[MAX] Ошибка в обработчике: {e}")
        import traceback
        traceback.print_exc()


# ============================================================
#  ЗАПУСК
# ============================================================

async def main():
    asyncio.create_task(telegram_sender())

    await max_client.start()
    print("Мост MAX → Telegram запущен. Слушаю сообщения…")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()
    print("Останавливаюсь…")


if __name__ == "__main__":
    asyncio.run(main())