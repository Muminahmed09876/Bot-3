#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
import subprocess
import traceback
import json 
from flask import Flask, render_template_string
import requests
import time
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))
# New env var from previous code
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
# New state for dynamic captions
USER_COUNTERS = {}
# New state for edit caption mode
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
# Stores multiple files waiting for audio order, keyed by the audio list prompt message ID
PENDING_AUDIO_ORDERS = {} # {prompt_message_id: {'uid': int, 'path': str, 'original_name': str, 'tracks': list}} 
# ------------------------------

# --- NEW STATE FOR POST CREATION ---
CREATE_POST_MODE = set()
# Stores the state of the post creation process {uid: {'image_path': str, 'message_ids': list, 'state': str, 'post_data': dict, 'post_message_id': int}}
POST_CREATION_STATE = {} 

# --- New states for post data (initial values) ---
DEFAULT_POST_DATA = {
    'image_name': "Image Name",
    'genres': "",
    'season_list_raw': "1, 2" # Stores the raw input, used for dynamic season list
}
# ------------------------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url or "docs.google.com" in url

def extract_drive_id(url: str) -> str:
    patterns = [
        r"/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"open\?id=([a-zA-Z0-9_-]+)",
        r"https://drive.google.com/file/d/([a-zA-Z0-9_-]+)/"
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

# Helper function for consistent renaming
def generate_new_filename(original_name: str) -> str:
    """Generates the new standardized filename while preserving the original extension."""
    BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"
    file_path = Path(original_name)
    file_ext = file_path.suffix.lower()
    
    # Clean up the extension and ensure it starts with a dot
    file_ext = "." + file_ext.lstrip('.')
    
    # If a file like 'video_id' or 'file_id' comes without a proper extension, default to .mp4
    if not file_ext or file_ext == '.':
        return BASE_NEW_NAME + ".mp4"
        
    return BASE_NEW_NAME + file_ext

# --- MODIFIED: Replaced get_video_duration with a comprehensive metadata extractor ---
def extract_video_metadata(file_path: Path) -> dict:
    """
    Hachoir ব্যবহার করে ভিডিও থেকে duration, width, height বের করে।
    """
    metadata = {'duration': None, 'width': None, 'height': None}
    try:
        parser = createParser(str(file_path))
        if not parser:
            return metadata
        with parser:
            meta = extractMetadata(parser)
            if meta:
                # সেকেন্ডে duration
                if meta.has('duration'):
                    metadata['duration'] = meta.get('duration').total_seconds()
                # পিক্সেলে width এবং height
                if meta.has('width'):
                    metadata['width'] = meta.get('width')
                if meta.has('height'):
                    metadata['height'] = meta.get('height')
    except Exception as e:
        logger.warning(f"Metadata extraction failed: {e}")
        
    # নিশ্চিত করুন যে পূর্ণসংখ্যা আছে
    if metadata.get('duration') is not None:
        metadata['duration'] = int(metadata['duration'])
    if metadata.get('width') is not None:
        metadata['width'] = int(metadata['width'])
    if metadata.get('height') is not None:
        metadata['height'] = int(metadata['height'])

    return metadata
# --- END MODIFIED ---

def parse_time(time_str: str) -> int:
    """Parses a time string like '5s', '1m', '1h 30s' into seconds."""
    total_seconds = 0
    parts = time_str.lower().split()
    for part in parts:
        if part.endswith('s'):
            total_seconds += int(part[:-1])
        elif part.endswith('m'):
            total_seconds += int(part[:-1]) * 60
        elif part.endswith('h'):
            total_seconds += int(part[:-1]) * 3600
    return total_seconds

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

def delete_caption_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Delete Caption 🗑️", callback_data="delete_caption")]])

# --- NEW UTILITY: Keyboard for Mode Check ---
def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    # Check if a file is waiting for track order input
    waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
    waiting_status = f" ({waiting_count}টি অর্ডার বাকি)" if waiting_count > 0 else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# --- NEW UTILITY: FFprobe to get audio tracks (Existing in snippets) ---
def get_audio_tracks_ffprobe(file_path: Path) -> list:
    """Uses ffprobe to get a list of audio streams with their index and title."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        
        audio_tracks = []
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'audio':
                stream_index = stream.get('index') 
                title = stream.get('tags', {}).get('title', 'N/A')
                language = stream.get('tags', {}).get('language', 'und') # 'und' is undefined
                audio_tracks.append({
                    'stream_index': stream_index,
                    'title': title,
                    'language': language
                })
        return audio_tracks
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return []
# ---------------------------------------------

# --- UTILITY: Generate Post Caption (UPDATED) ---
def generate_post_caption(data: dict) -> str:
    """Generates the full caption based on the post_data with required formatting."""
    image_name = data.get('image_name', DEFAULT_POST_DATA['image_name'])
    genres = data.get('genres', DEFAULT_POST_DATA['genres'])
    season_list_raw = data.get('season_list_raw', DEFAULT_POST_DATA['season_list_raw'])

    # 1. Dynamic Season List Generation
    season_entries = []
    
    # Clean up the input string and split by comma or space
    parts = re.split(r'[,\s]+', season_list_raw.strip())
    parts = [p.strip() for p in parts if p.strip()]

    for part in parts:
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                # Ensure start <= end to avoid infinite loop
                if start > end:
                    start, end = end, start
                for i in range(start, end + 1):
                    # Use two digits padding for season numbers (e.g. 01, 02)
                    season_entries.append(f"**{image_name} Season {i:02d}**") 
            except ValueError:
                continue
        else:
            try:
                num = int(part)
                season_entries.append(f"**{image_name} Season {num:02d}**")
            except ValueError:
                continue

    # Remove duplicates and ensure list has at least "Coming Soon..."
    unique_season_entries = list(dict.fromkeys(season_entries))
    if not unique_season_entries:
        unique_season_entries.append("**Coming Soon...**")
    # Add Coming Soon if it's not the last entry and there are other entries
    elif unique_season_entries[-1] != "**Coming Soon...**" and unique_season_entries[0] != "**Coming Soon...**":
        unique_season_entries.append("**Coming Soon...**")
        
    # season_text is now just a list of the bolded season/coming soon entries, separated by \n
    season_text = "\n".join(unique_season_entries)

    # 2. Main Caption Template (All bold as per user request)
    base_caption = (
        f"**{image_name}**\n"
        f"**────────────────────**\n"
        f"**‣ Audio - Hindi Official**\n"
        f"**‣ Quality - 480p, 720p, 1080p**\n"
        f"**‣ Genres - {genres}**\n"
        f"**────────────────────**"
    )

    # 3. The Collapsible/Quote Block Part (All bold and in a quote block)
    # The quote block mimics a collapsible section in standard Telegram Markdown.
    
    # Start the quote block with the header
    collapsible_text_parts = [
        f"> **{image_name} All Season List :-**", 
        "> " # ফাঁকা লাইন যোগ
    ]
    
    # Add each season entry, prepending a quote character '>' and adding a blank line after it.
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}") # Season line
        collapsible_text_parts.append("> ") # Blank line after season
        
    # Remove the extra blank quote line added after the last season/Coming Soon... entry
    if collapsible_text_parts and collapsible_text_parts[-1] == "> ":
        collapsible_text_parts.pop()
        
    collapsible_text = "\n".join(collapsible_text_parts)

    # Combine everything
    final_caption = f"{base_caption}\n\n{collapsible_text}"
    
    return final_caption
# ---------------------------------------------


# ---- progress callback helpers (removed live progress) ----
async def progress_callback(current, total, message: Message, start_time, task="Progress"):
    pass

def pyrogram_progress_wrapper(current, total, message_obj, start_time_obj, task_str="Progress"):
    pass

# ---- robust download stream with retries ----
async def download_stream(resp, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    total = 0
    try:
        size = int(resp.headers.get("Content-Length", 0))
    except:
        size = 0
    chunk_size = 1024 * 1024
    try:
        with out_path.open("wb") as f:
            async for chunk in resp.content.iter_chunked(chunk_size):
                if cancel_event and cancel_event.is_set():
                    return False, "অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।"
                if not chunk:
                    break
                if total > MAX_SIZE:
                    return False, "ফাইলের সাইজ 4GB এর বেশি হতে পারে না।"
                total += len(chunk)
                f.write(chunk)
    except Exception as e:
        return False, str(e)
    return True, None

async def fetch_with_retries(session, url, method="GET", max_tries=3, **kwargs):
    backoff = 1
    for attempt in range(1, max_tries + 1):
        try:
            resp = await session.request(method, url, **kwargs)
            return resp
        except Exception as e:
            if attempt == max_tries:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2
    raise RuntimeError("unreachable")

async def download_url_generic(url: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, cancel_event=cancel_event)
        except Exception as e:
            return False, str(e)

async def download_drive_file(file_id: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(base, allow_redirects=True) as resp:
                if resp.status == 200 and "content-disposition" in (k.lower() for k in resp.headers.keys()):
                    return await download_stream(resp, out_path, message, cancel_event=cancel_event)
                text = await resp.text(errors="ignore")
                m = re.search(r"confirm=([0-9A-Za-z-_]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                for k, v in resp.cookies.items():
                    if k.startswith("download_warning"):
                        token = v.value
                        download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                        async with sess.get(download_url, allow_redirects=True) as resp2:
                            if resp2.status != 200:
                                return False, f"HTTP {resp2.status}"
                            return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                return False, "ডাউনলোডের জন্য Google Drive থেকে অনুমতি প্রয়োজন বা লিংক পাবলিক নয়।"
        except Exception as e:
            return False, str(e)

async def set_bot_commands():
    cmds = [
        BotCommand("start", "বট চালু/হেল্প"),
        BotCommand("upload_url", "URL থেকে ফাইল ডাউনলোড ও আপলোড (admin only)"),
        BotCommand("setthumb", "কাস্টম থাম্বনেইল সেট করুন (admin only)"),
        BotCommand("view_thumb", "আপনার থাম্বনেইল দেখুন (admin only)"),
        BotCommand("del_thumb", "আপনার থাম্বনেইল মুছে ফেলুন (admin only)"),
        BotCommand("set_caption", "কাস্টম ক্যাপশন সেট করুন (admin only)"),
        BotCommand("view_caption", "আপনার ক্যাপশন দেখুন (admin only)"),
        BotCommand("edit_caption_mode", "শুধু ক্যাপশন এডিট করুন (admin only)"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন (admin only)"),
        BotCommand("mkv_video_audio_change", "MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন (admin only)"),
        BotCommand("create_post", "নতুন পোস্ট তৈরি করুন (admin only)"), # NEW COMMAND
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"), 
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

# ---- handlers ----
@app.on_message(filters.command("start") & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    SUBSCRIBERS.add(m.chat.id)
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "নোট: বটের অনেক কমান্ড শুধু অ্যাডমিন (owner) চালাতে পারবে।\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ডাউনলোড ও Telegram-এ আপলোড (admin only)\n"
        "/setthumb - একটি ছবি পাঠান, সেট হবে আপনার থাম্বনেইল (admin only)\n"
        "/view_thumb - আপনার থাম্বনেইল দেখুন (admin only)\n"
        "/del_thumb - আপনার থাম্বনেইল মুছে ফেলুন (admin only)\n"
        "/set_caption - একটি ক্যাপশন সেট করুন (admin only)\n"
        "/view_caption - আপনার ক্যাপশন দেখুন (admin only)\n"
        "/edit_caption_mode - শুধু ক্যাপশন এডিট করার মোড টগল করুন (admin only)\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন (admin only)\n"
        "/mkv_video_audio_change - MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন মোড টগল করুন (admin only)\n"
        "/create_post - নতুন পোস্ট তৈরি করুন (admin only)\n" # NEW COMMAND in help
        "/mode_check - বর্তমান মোড স্ট্যাটাস চেক করুন এবং পরিবর্তন করুন (admin only)\n" 
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    await m.reply_text(text)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    uid = m.from_user.id
    if len(m.command) > 1:
        time_str = " ".join(m.command[1:])
        seconds = parse_time(time_str)
        if seconds > 0:
            USER_THUMB_TIME[uid] = seconds
            await m.reply_text(f"থাম্বনেইল তৈরির সময় সেট হয়েছে: {seconds} সেকেন্ড।")
        else:
            await m.reply_text("সঠিক ফরম্যাটে সময় দিন। উদাহরণ: `/setthumb 5s`, `/setthumb 1m`, `/setthumb 1m 30s`")
    else:
        SET_THUMB_REQUEST.add(uid)
        await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")


@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    thumb_time = USER_THUMB_TIME.get(uid)
    
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    elif thumb_time:
        await m.reply_text(f"আপনার থাম্বনেইল তৈরির সময় সেট করা আছে: {thumb_time} সেকেন্ড।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল বা থাম্বনেইল তৈরির সময় সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
    
    if uid in USER_THUMB_TIME:
        USER_THUMB_TIME.pop(uid)

    if not (thumb_path or uid in USER_THUMB_TIME):
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")
    else:
        await m.reply_text("আপনার থাম্বনেইল/থাম্বনেইল তৈরির সময় মুছে ফেলা হয়েছে।")


@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    
    # --- NEW: Handle Create Post Mode ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE and POST_CREATION_STATE[uid]['state'] == 'awaiting_image':
        
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) # Track user's image message
        
        out = TMP / f"post_img_{uid}.jpg"
        try:
            download_msg = await m.reply_text("ছবি ডাউনলোড হচ্ছে...")
            state_data['message_ids'].append(download_msg.id)
            
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((1080, 1080)) # Resize for reasonable Telegram limit
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            state_data['image_path'] = str(out)
            state_data['state'] = 'awaiting_name_change'
            
            # Initial Post Send (for display and ID)
            initial_caption = generate_post_caption(state_data['post_data'])
            
            post_msg = await c.send_photo(
                chat_id=m.chat.id, 
                photo=str(out), 
                caption=initial_caption, 
                parse_mode=ParseMode.MARKDOWN
            )
            state_data['post_message_id'] = post_msg.id # Store the post ID
            state_data['message_ids'].append(post_msg.id) # Track the post message ID for final cleanup exclusion
            
            # Send prompt for the first edit step
            prompt_msg = await m.reply_text(
                f"✅ পোস্টের ছবি সেট হয়েছে।\n\n**এখন ছবির নামটি পরিবর্তন করুন।**\n"
                f"বর্তমান নাম: `{state_data['post_data']['image_name']}`\n"
                f"অনুগ্রহ করে শুধু **নামটি** পাঠান। উদাহরণ: `One Piece`"
            )
            state_data['message_ids'].append(prompt_msg.id)

        except Exception as e:
            logger.error(f"Post creation image error: {e}")
            await m.reply_text(f"ছবি সেভ করতে সমস্যা: {e}")
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            if out.exists(): out.unlink(missing_ok=True)
        return
    # --- END NEW: Handle Create Post Mode ---
    
    if uid in SET_THUMB_REQUEST:
        SET_THUMB_REQUEST.discard(uid)
        out = TMP / f"thumb_{uid}.jpg"
        try:
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            USER_THUMBS[uid] = str(out)
            # Make sure to clear the time setting if a photo is set
            USER_THUMB_TIME.pop(uid, None)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
    else:
        pass

# Handlers for caption
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    # Reset counter data when a new caption is about to be set
    USER_COUNTERS.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. **নম্বর বৃদ্ধি:** `[01]`, `[(01)]` (নম্বর স্বয়ংক্রিয়ভাবে বাড়বে)\n"
        "2. **গুণমানের সাইকেল:** `[re (480p, 720p)]`\n"
        "3. **শর্তসাপেক্ষ টেক্সট (নতুন):** `[TEXT (XX)]` - যেমন: `[End (02)]`, `[hi (05)]` (যদি বর্তমান পর্বের নম্বর `XX` এর **সমান** হয়, তাহলে `TEXT` যোগ হবে)।"
    )

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    caption = USER_CAPTIONS.get(uid)
    if caption:
        await m.reply_text(f"আপনার সেভ করা ক্যাপশন:\n\n`{caption}`", reply_markup=delete_caption_keyboard())
    else:
        await m.reply_text("আপনার কোনো ক্যাপশন সেভ করা নেই। /set_caption দিয়ে সেট করুন।")

@app.on_callback_query(filters.regex("delete_caption"))
async def delete_caption_cb(c, cb):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
    if uid in USER_CAPTIONS:
        USER_CAPTIONS.pop(uid)
        USER_COUNTERS.pop(uid, None) # New: delete counter data
        await cb.message.edit_text("আপনার ক্যাপশন মুছে ফেলা হয়েছে।")
    else:
        await cb.answer("আপনার কোনো ক্যাপশন সেভ করা নেই।", show_alert=True)

# Handler to toggle edit caption mode
@app.on_message(filters.command("edit_caption_mode") & filters.private)
async def toggle_edit_caption_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in EDIT_CAPTION_MODE:
        EDIT_CAPTION_MODE.discard(uid)
        await m.reply_text("edit video caption mod **OFF**.\nএখন থেকে আপলোড করা ভিডিওর রিনেম ও থাম্বনেইল পরিবর্তন হবে, এবং সেভ করা ক্যাপশন যুক্ত হবে।")
    else:
        EDIT_CAPTION_MODE.add(uid)
        await m.reply_text("edit video caption mod **ON**.\nএখন থেকে শুধু সেভ করা ক্যাপশন ভিডিওতে যুক্ত হবে। ভিডিওর নাম এবং থাম্বনেইল একই থাকবে।")

# --- HANDLER: /mkv_video_audio_change ---
@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in MKV_AUDIO_CHANGE_MODE:
        MKV_AUDIO_CHANGE_MODE.discard(uid)
        
        # NOTE: Do NOT clean up PENDING_AUDIO_ORDERS here. 
        # Cleanup happens on successful reply or cancellation button press on the prompt message.
        
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অফ** করা হয়েছে।")
    else:
        MKV_AUDIO_CHANGE_MODE.add(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে। এখন আপনি একটি **MKV ফাইল** অথবা অন্য কোনো **ভিডিও ফাইল** পাঠান।\n(এই মোড ম্যানুয়ালি অফ না করা পর্যন্ত চালু থাকবে।)")

# --- NEW HANDLER: /create_post ---
@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
        # Clean up any pending state
        if uid in POST_CREATION_STATE:
            state_data = POST_CREATION_STATE.pop(uid)
            try:
                # Delete image file
                if state_data.get('image_path'):
                    Path(state_data['image_path']).unlink(missing_ok=True)
                # Delete all conversation messages except the final post if it was created
                messages_to_delete = state_data.get('message_ids', [])
                post_id = state_data.get('post_message_id')
                # Remove the final post ID from the delete list
                if post_id and post_id in messages_to_delete:
                    messages_to_delete.remove(post_id) 
                if messages_to_delete:
                    await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Post mode OFF cleanup error: {e}")
                
        await m.reply_text("Create Post Mode **অফ** করা হয়েছে।")
    else:
        CREATE_POST_MODE.add(uid)
        # Initialize state, track command message ID
        POST_CREATION_STATE[uid] = {
            'image_path': None, 
            'message_ids': [m.id], 
            'state': 'awaiting_image', 
            'post_data': DEFAULT_POST_DATA.copy(),
            'post_message_id': None
        }
        await m.reply_text("Create Post Mode **অন** করা হয়েছে।\nএকটি ছবি (**Photo**) পাঠান যা পোস্টের ইমেজ হিসেবে ব্যবহার হবে।")
# ---------------------------------------------


# --- NEW HANDLER: /mode_check ---
@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
    waiting_status_text = f"{waiting_count}টি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if waiting_count > 0 else "কোনো ফাইল অপেক্ষা করছে না।"
    
    status_text = (
        "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
        f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
        f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
        f"2. **Edit Caption Mode:** `{caption_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
        "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
    )
    
    await m.reply_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons ---
@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE:
            # Turning OFF: Clear mode
            MKV_AUDIO_CHANGE_MODE.discard(uid)
            message = "MKV Audio Change Mode OFF."
        else:
            # Turning ON
            MKV_AUDIO_CHANGE_MODE.add(uid)
            message = "MKV Audio Change Mode ON."
            
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE:
            EDIT_CAPTION_MODE.discard(uid)
            message = "Edit Caption Mode OFF."
        else:
            EDIT_CAPTION_MODE.add(uid)
            message = "Edit Caption Mode ON."
            
    # Refresh the keyboard and edit the original message (similar to mode_check_cmd)
    try:
        audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
        caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
        
        waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
        waiting_status_text = f"{waiting_count}টি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if waiting_count > 0 else "কোনো ফাইল অপেক্ষা করছে না।"

        status_text = (
            "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
            f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
            f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
            f"2. **Edit Caption Mode:** `{caption_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
            "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
        )
        
        await cb.message.edit_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)
        await cb.answer(message, show_alert=True)
    except Exception as e:
        logger.error(f"Callback edit error: {e}")
        await cb.answer(message, show_alert=True)


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    # Handle set caption request
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None) # New: reset counter data
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে।")
        return

    # --- Handle Audio Order Reply (Existing logic from snippets) ---
    if m.reply_to_message and m.reply_to_message.id in PENDING_AUDIO_ORDERS:
        prompt_message_id = m.reply_to_message.id
        file_data = PENDING_AUDIO_ORDERS.get(prompt_message_id)
        
        if file_data and file_data['uid'] == uid:
            tracks = file_data['tracks']
            num_tracks_in_file = len(tracks)
            new_order_str = [t.strip() for t in text.split(',') if t.strip().isdigit()]
            num_tracks_requested = len(new_order_str)
            
            try:
                # --- MODIFIED VALIDATION LOGIC (from snippet 15/11 context) ---
                if num_tracks_in_file < 5 and num_tracks_requested != num_tracks_in_file:
                    await m.reply_text(f"এই ফাইলে {num_tracks_in_file}টি ট্র্যাক আছে। আপনাকে অবশ্যই ঠিক {num_tracks_in_file}টি ট্র্যাকের অর্ডার দিতে হবে।")
                    return
                if num_tracks_requested == 0:
                    await m.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `3,2,1`")
                    return
                if num_tracks_requested > num_tracks_in_file:
                    await m.reply_text(f"আপনি {num_tracks_requested}টি ট্র্যাক চেয়েছেন, কিন্তু ফাইলে মাত্র {num_tracks_in_file}টি ট্র্যাক আছে।")
                    return
                # --- END MODIFIED VALIDATION LOGIC ---
                
                new_stream_map = []
                valid_user_indices = list(range(1, num_tracks_in_file + 1))
                for user_track_num_str in new_order_str:
                    user_track_num = int(user_track_num_str) # ValueError is caught by outer try-except
                    if user_track_num not in valid_user_indices:
                        await m.reply_text(f"ভুল ট্র্যাক নম্বর: {user_track_num}। ট্র্যাক নম্বরগুলো হতে হবে: {', '.join(map(str, valid_user_indices))}")
                        return
                    
                    stream_index_to_map = tracks[user_track_num - 1]['stream_index']
                    new_stream_map.append(f"0:{stream_index_to_map}")
                    
                # Start the audio remux process
                asyncio.create_task(
                    handle_audio_remux(
                        c, m, file_data['path'], file_data['original_name'], new_stream_map,
                        messages_to_delete=[prompt_message_id, m.id]
                    )
                )
                # Clear state immediately
                PENDING_AUDIO_ORDERS.pop(prompt_message_id, None)
                return
            except ValueError:
                await m.reply_to_message.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `3,2,1`")
                return
            except Exception as e:
                logger.error(f"Audio remux preparation error: {e}")
                await m.reply_to_message.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া শুরু করতে সমস্যা: {e}")
                # Clean up files before clearing state
                try: 
                    Path(file_data['path']).unlink(missing_ok=True)
                except Exception: 
                    pass
                PENDING_AUDIO_ORDERS.pop(prompt_message_id, None)
                return
        # -----------------------------------------------------

    # --- NEW: Handle Post Creation Editing Steps ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        state_data = POST_CREATION_STATE[uid]
        current_state = state_data['state']
        
        state_data['message_ids'].append(m.id) # Track user's text message
        
        if current_state == 'awaiting_name_change':
            # Step 1: Image Name Change
            state_data['post_data']['image_name'] = text
            state_data['state'] = 'awaiting_genres'
            
            # Update caption on the post message
            new_caption = generate_post_caption(state_data['post_data'])
            await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)

            prompt_msg = await m.reply_text(
                f"✅ ছবির নাম পরিবর্তন হয়েছে: `{text}`\n\n**এখন Genres পরিবর্তন করুন।**\n"
                f"বর্তমান Genres: `{state_data['post_data']['genres']}`\n"
                f"অনুগ্রহ করে শুধু **Genres** কমা-সেপারেটেড করে পাঠান। উদাহরণ: `Animation, Adventure, Action, Comedy`"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_genres':
            # Step 2: Genres Change
            state_data['post_data']['genres'] = text
            state_data['state'] = 'awaiting_season_list'
            
            # Update caption on the post message
            new_caption = generate_post_caption(state_data['post_data'])
            await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)

            prompt_msg = await m.reply_text(
                f"✅ Genres সেট হয়েছে: `{text}`\n\n**এখন Season List পরিবর্তন করুন।**\n"
                f"Change Season List এর মানে \"{state_data['post_data']['image_name']}\" Season 01 কয়টি add করব?\n"
                f"ফরম্যাট: সিজন নম্বর অথবা রেঞ্জ কমা বা স্পেস-সেপারেটেড দিন।\n"
                f"উদাহরণ:\n"
                f"‣ `1` (Season 01)\n"
                f"‣ `1-2` (Season 01 থেকে Season 02)\n"
                f"‣ `1-2 4-5` বা `1-2, 4-5` (Season 01-02 এবং 04-05)"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_season_list':
            # Step 3: Season List Change (FINAL STEP)
            if not text.strip():
                state_data['post_data']['season_list_raw'] = ""
            else:
                state_data['post_data']['season_list_raw'] = text
            
            # Final Caption Update
            new_caption = generate_post_caption(state_data['post_data'])
            
            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in season list: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            # Cleanup and Final Message
            all_messages = state_data.get('message_ids', [])
            # Remove the final post ID from the delete list
            post_id = state_data.get('post_message_id')
            if post_id and post_id in all_messages:
                all_messages.remove(post_id)
            
            # Delete all conversation messages
            if all_messages:
                try:
                    await c.delete_messages(m.chat.id, all_messages)
                except Exception as e:
                    logger.warning(f"Error deleting post creation messages: {e}")
            
            # Cleanup state image_path
            image_path = state_data['image_path']
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
            
            # Final message
            await m.reply_text("✅ **পোস্ট তৈরি এবং এডিটিং সম্পন্ন হয়েছে।**\n\nআপনার চূড়ান্ত পোস্টটি উপরে আছে।\n\n`Create Post Mode` অফ করতে `/create_post` ব্যবহার করুন।")
            
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            return
    # --- END NEW: Handle Post Creation Editing Steps ---
    
    # Handle /rename
    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.document) and m.text.startswith("/") is False:
        # Check if the text looks like a new filename (contains extension)
        if "." in text:
            await handle_rename_file(c, m, text)
            return

    if is_admin(uid):
        # Handle broadcast
        if m.text.startswith("/broadcast"):
            # Existing logic for broadcast
            parts = m.text.split(maxsplit=1)
            if len(parts) < 2:
                await m.reply_text("ব্রডকাস্টের জন্য টেক্সট দিন। উদাহরণ: `/broadcast Hello everyone!`")
                return
            await broadcast_message(m.chat.id, parts[1])
            return

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    uid = m.from_user.id
    if len(m.command) < 2:
        await m.reply_text("URL দিন। উদাহরণ: `/upload_url <link>`")
        return
        
    url = m.text.split(maxsplit=1)[1]
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    # Run the download and upload process in the background
    asyncio.create_task(handle_url_upload(c, m, url, cancel_event))

async def handle_url_upload(c: Client, m: Message, url: str, cancel_event: asyncio.Event):
    uid = m.from_user.id
    status_msg = None
    try:
        if cancel_event.is_set():
            return
            
        status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        
        url_path = Path(url)
        # Try to infer a filename from the URL path if not drive
        if not is_drive_url(url):
            safe_name = url_path.name
        else:
            safe_name = "google_drive_file"
            
        tmp_in = TMP / f"url_dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        
        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                try:
                    await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                except Exception:
                    await m.reply_text("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                TASKS[uid].remove(cancel_event)
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)
            
        if not ok:
            try:
                await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            except Exception:
                await m.reply_text(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            try:
                if tmp_in.exists(): tmp_in.unlink()
            except:
                pass
            TASKS[uid].remove(cancel_event)
            return

        try:
            await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        except Exception:
            await m.reply_text("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
            
        # NEW RENAME FEATURE: URL আপলোডের জন্য নাম পরিবর্তন
        renamed_file = generate_new_filename(safe_name)
        # -------------------------------------------------------
        
        await process_file_and_upload(c, m, tmp_in, original_name=renamed_file, messages_to_delete=[status_msg.id])
        
    except Exception as e:
        traceback.print_exc()
        try:
            await status_msg.edit(f"অপস! কিছু ভুল হয়েছে: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"অপস! কিছু ভুল হয়েছে: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

async def handle_caption_only_upload(c: Client, m: Message):
    uid = m.from_user.id
    caption_to_use = USER_CAPTIONS.get(uid)
    if not caption_to_use:
        await m.reply_text("ক্যাপশন এডিট মোড চালু আছে কিন্তু কোনো সেভ করা ক্যাপশন নেই। /set_caption দিয়ে ক্যাপশন সেট করুন।")
        return
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    file_info = m.video or m.document
    if not file_info:
        await m.reply_text("এই মেসেজে কোনো ভিডিও বা ডকুমেন্ট পাওয়া যায়নি।")
        TASKS[uid].remove(cancel_event)
        return

    try:
        # Check for dynamic caption update
        final_caption = await asyncio.to_thread(process_dynamic_caption, uid, caption_to_use)
        
        # We only want to edit the caption of the forwarded message, not re-upload
        # Note: Editing caption of forwarded message only works if the bot is admin in both chats and has permission, 
        # but the primary use here is to upload it again with a new caption if forwarding is not working.
        
        if file_info.file_name:
            original_name = file_info.file_name
        elif m.video:
            original_name = f"video_{file_info.file_unique_id}.mp4"
        else:
            original_name = f"file_{file_info.file_unique_id}"

        status_msg = await m.reply_text("ফাইল ডাউনলোড ও নতুন ক্যাপশন সহ আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        tmp_path = TMP / f"caption_only_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        
        # Download the file
        await m.download(file_name=str(tmp_path))
        
        # --- NEW: Extract Metadata for Full Screen ---
        metadata = await asyncio.to_thread(extract_video_metadata, tmp_path)
        duration = metadata.get('duration')
        width = metadata.get('width')
        height = metadata.get('height')
        # --------------------------------------------
        
        # If it's a video, check for thumb
        thumb_path = USER_THUMBS.get(uid)
        temp_thumb_path = None
        
        if m.video and not thumb_path:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            thumb_time_sec = USER_THUMB_TIME.get(uid, 1) # Default to 1 second
            ok = await generate_video_thumbnail(tmp_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
            if ok:
                thumb_path = str(temp_thumb_path)
            else:
                temp_thumb_path = None
                thumb_path = None
                
        # Final Upload
        if m.video:
            await c.send_video(
                chat_id=m.chat.id,
                video=str(tmp_path),
                caption=final_caption,
                file_name=original_name,
                progress=pyrogram_progress_wrapper,
                progress_args=(m, datetime.now(), 'Uploading'),
                # --- NEW PARAMS for Full Screen ---
                duration=duration,
                width=width,
                height=height,
                thumb=thumb_path if thumb_path else None, # Use the path if valid
                supports_streaming=True, # Crucial for full screen/streaming
                # -----------------------------------
            )
        else: # Document
            await c.send_document(
                chat_id=m.chat.id,
                document=str(tmp_path),
                caption=final_caption,
                file_name=original_name,
                progress=pyrogram_progress_wrapper,
                progress_args=(m, datetime.now(), 'Uploading'),
                thumb=thumb_path if thumb_path else None
            )

        await status_msg.delete()
        
    except Exception as e:
        logger.error(f"Caption only upload error: {e}")
        try:
            await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
    finally:
        try:
            if tmp_path.exists(): tmp_path.unlink()
            if temp_thumb_path and Path(temp_thumb_path).exists(): Path(temp_thumb_path).unlink()
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


@app.on_message(filters.private & (filters.video | filters.document))
async def forwarded_file_or_direct_file(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    # --- Check for MKV Audio Change Mode first ---
    if uid in MKV_AUDIO_CHANGE_MODE:
        await handle_audio_change_file(c, m)
        return
    # -------------------------------------------------
    
    # Fallback to existing logic (Forwarded/direct file for rename/re-upload logic)
    # Check if the user is in edit caption mode
    if uid in EDIT_CAPTION_MODE and m.forward_date: 
        # Only apply to forwarded media to avoid accidental re-upload of direct files
        await handle_caption_only_upload(c, m)
        return

    # If not in any special mode, and it's a forwarded video/document, start the download/re-upload process
    if m.forward_date:
        # Original logic for forwarded file handling
        cancel_event = asyncio.Event()
        TASKS.setdefault(uid, []).append(cancel_event)

        file_info = m.video or m.document
        if file_info and file_info.file_name:
            original_name = file_info.file_name
        elif m.video:
            original_name = f"video_{file_info.file_unique_id}.mp4"
        else:
            original_name = f"file_{file_info.file_unique_id}"

        try:
            status_msg = await m.reply_text("ফরওয়ার্ড করা ফাইল ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("ফরওয়ার্ড করা ফাইল ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
            
        tmp_path = TMP / f"forwarded_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        
        try:
            await m.download(file_name=str(tmp_path), progress=pyrogram_progress_wrapper, progress_args=(status_msg, datetime.now(), 'Downloading'))

            # NEW RENAME FEATURE: Forwarded ফাইল রিনেম
            renamed_file = generate_new_filename(original_name)
            # -------------------------------------------------------

            await process_file_and_upload(c, m, tmp_path, original_name=renamed_file, messages_to_delete=[status_msg.id])

        except Exception as e:
            traceback.print_exc()
            try:
                await status_msg.edit(f"ডাউনলোড বা আপলোড ব্যর্থ: {e}", reply_markup=None)
            except Exception:
                await m.reply_text(f"ডাউনলোড বা আপলোড ব্যর্থ: {e}", reply_markup=None)
        finally:
            try:
                if tmp_path.exists(): tmp_path.unlink()
                TASKS[uid].remove(cancel_event)
            except Exception:
                pass


@app.on_message(filters.command("rename") & filters.private)
async def rename_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.reply_to_message:
        await m.reply_text("একটি ভিডিও বা ডকুমেন্টে রিপ্লাই করে /rename <নতুন নাম.এক্সটেনশন> ব্যবহার করুন।")
        return
    if not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("রিপ্লাই করা মেসেজটি একটি ভিডিও বা ডকুমেন্ট হতে হবে।")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন নাম (এক্সটেনশন সহ) দিন। উদাহরণ: `/rename my_video.mp4`")
        return

    new_name = " ".join(m.command[1:])
    await handle_rename_file(c, m.reply_to_message, new_name)

async def handle_rename_file(c: Client, m: Message, new_name: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    file_info = m.video or m.document
    original_name = file_info.file_name if file_info and file_info.file_name else "file"
    
    try:
        status_msg = await m.reply_text(f"ফাইল ডাউনলোড হচ্ছে এবং `{new_name}` নামে রিনেম করা হচ্ছে...", reply_markup=progress_keyboard())
        
        tmp_in = TMP / f"rename_in_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        tmp_out = TMP / f"rename_out_{uid}_{int(datetime.now().timestamp())}_{new_name}"

        # 1. Download the file
        await m.download(file_name=str(tmp_in), progress=pyrogram_progress_wrapper, progress_args=(status_msg, datetime.now(), 'Downloading'))

        # 2. Rename (by simply using the new name for the upload process, no actual file rename needed here)
        
        # 3. Process and Upload
        try:
            await status_msg.edit(f"ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        except Exception:
            await m.reply_text(f"ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)

        await process_file_and_upload(c, m, tmp_in, original_name=new_name, messages_to_delete=[status_msg.id])
        
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        try:
            if tmp_in.exists(): tmp_in.unlink(missing_ok=True)
            if tmp_out.exists(): tmp_out.unlink(missing_ok=True)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    # Get the ID of the message that contained the button
    prompt_message_id = cb.message.id
    
    # Check if this message ID is a pending audio order prompt
    if prompt_message_id in PENDING_AUDIO_ORDERS:
        file_data = PENDING_AUDIO_ORDERS.pop(prompt_message_id)
        # Check if the user is the one who initiated the task
        if file_data['uid'] == uid:
            # Clean up the file
            try: 
                Path(file_data['path']).unlink(missing_ok=True)
            except Exception: 
                pass
            # Cancel associated download tasks (if any were running just before the prompt)
            for ev in list(TASKS.get(uid, [])):
                try: ev.set()
                except: pass
            
            await cb.answer("অডিও পরিবর্তন প্রক্রিয়া বাতিল করা হয়েছে।", show_alert=True)
            try:
                await cb.message.delete()
            except Exception:
                pass
            return
            
    # If not a pending audio order, check general tasks (mostly for URL/rename downloads)
    if uid in TASKS and TASKS[uid]:
        for ev in list(TASKS[uid]):
            try:
                ev.set()
            except:
                pass
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try:
            await cb.message.edit("❌ অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    else:
        await cb.answer("বাতিল করার কোনো সক্রিয় অপারেশন নেই।", show_alert=True)


# --- Reconstructed/Required Utility Functions ---
# (Required for context and functionality of process_file_and_upload)

async def convert_to_mkv(in_path: Path, out_path: Path, status_msg: Message = None) -> tuple[bool, str]:
    """Converts a video file to MKV format using ffmpeg, copying video/audio/subtitles if possible."""
    try:
        # FFmpeg command to convert to MKV, copying streams if possible
        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            "-map", "0",
            "-c", "copy",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23", # For non-copyable video streams, re-encode to x264
            "-c:a", "copy",
            "-movflags", "+faststart", # For MP4/MOV compatibility if it remains
            str(out_path)
        ]

        result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600)
        
        if result.returncode != 0:
            logger.error(f"MKV Conversion failed: {result.stderr}")
            # Fallback to a simpler, more robust re-encoding if the copy failed
            cmd_full = [
                "ffmpeg",
                "-i", str(in_path),
                "-map", "0:v:0", "-map", "0:a:0?", "-map", "0:s:0?", # Map only first video/audio/sub
                "-c:v", "libx264", "-preset", "fast", "-crf", "23", 
                "-c:a", "copy",
                "-map_metadata", "0", # Keep metadata from input
                "-movflags", "+faststart", # For MP4
                str(out_path)
            ]
            result_full = await asyncio.to_thread(subprocess.run, cmd_full, capture_output=True, text=True, check=False, timeout=3600)

            if result_full.returncode != 0:
                raise Exception(f"Full re-encoding failed: {result_full.stderr}")
        
        if not out_path.exists() or out_path.stat().st_size == 0:
            raise Exception("Converted file not found or is empty.")
            
        return True, None
    except Exception as e:
        logger.error(f"Video conversion error: {e}")
        return False, str(e)


def process_dynamic_caption(uid, caption_template):
    # Initialize user state if it doesn't exist
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'episode_numbers': {}, 'dynamic_counters': {}, 're_options_count': 0}

    # Increment upload counter for the current user
    USER_COUNTERS[uid]['uploads'] += 1

    # --- 1. Quality Cycle Logic (e.g., [re (480p, 720p, 1080p)]) ---
    quality_match = re.search(r"\[re\s*\((.*?)\)\]", caption_template)
    quality_replacement = ""
    if quality_match:
        options_str = quality_match.group(1).strip()
        options = [opt.strip() for opt in options_str.split(',') if opt.strip()]
        
        if options:
            if USER_COUNTERS[uid]['re_options_count'] >= len(options):
                USER_COUNTERS[uid]['re_options_count'] = 0 # Reset counter
            
            quality_replacement = options[USER_COUNTERS[uid]['re_options_count']]
            USER_COUNTERS[uid]['re_options_count'] += 1
            
        # Replace the placeholder with the selected option
        caption_template = re.sub(r"\[re\s*\((.*?)\)\]", quality_replacement, caption_template)

    # --- 2. Dynamic Counter Logic (e.g., [01], [(01)]) ---
    # Find all dynamic counter placeholders
    matches = re.findall(r"\[(\(?(?:0\d+|\d+)\)?)\]", caption_template)
    
    # Process each unique match to set up/update its counter
    for match in sorted(list(set(matches))):
        # Extract the number part, removing optional parentheses
        num_part = re.sub(r'[()]', '', match)
        initial_value = int(num_part)
        
        # Check if it has parentheses (e.g., [(01)])
        has_paren = match.startswith('(') and match.endswith(')')
        
        if match not in USER_COUNTERS[uid]['dynamic_counters']:
            # Initialize counter: start at the found number
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': initial_value, 'has_paren': has_paren}
        else:
            # Increment: only increment after the placeholder has been found/used
            USER_COUNTERS[uid]['dynamic_counters'][match]['value'] += 1
            
    # Replacement happens after all counters are updated, inside the next loop, 
    # as the dynamic_counters dict now holds the *next* values.

    # Replace placeholders with their current values
    for match, data in USER_COUNTERS[uid]['dynamic_counters'].items():
        value = data['value']
        has_paren = data['has_paren']
        
        # Format the number with leading zeros if necessary (02, 03, etc.)
        # Use the length of the original match to determine padding (e.g., '[01]' should be 2 digits)
        original_num_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{value:0{original_num_len}d}"
        
        # Add parentheses back if they existed
        final_value = f"({formatted_value})" if has_paren else formatted_value
        
        # This regex will replace all occurrences of the specific placeholder, e.g., '[12]' or '[(21)]'
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)

    # --- 3. New Conditional Text Logic (e.g., [End (02)], [hi (05)]) ---
    # Find the current episode number. We assume the smallest starting number counter 
    # (e.g. from [01]) represents the episode number.
    current_episode_num = 0
    # Find the smallest starting value among dynamic counters to represent the "episode number"
    if USER_COUNTERS[uid].get('dynamic_counters'):
        current_episode_num = min(data['value'] for data in USER_COUNTERS[uid]['dynamic_counters'].values())

    # New regex to find [TEXT (XX)] format.
    # Group 1: TEXT (e.g., End, hi)
    # Group 2: XX (e.g., 02, 05)
    conditional_matches = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", caption_template)
    
    for match in conditional_matches:
        text_to_insert = match[0].strip()
        target_num_str = match[1].strip()
        
        try:
            target_num = int(target_num_str)
        except ValueError:
            # Ignore if the target number is not a valid integer
            continue
            
        placeholder = f"[{text_to_insert} ({target_num_str})]"
        
        if current_episode_num == target_num:
            # Replace placeholder with just the text_to_insert
            caption_template = caption_template.replace(placeholder, text_to_insert)
        else:
            # Remove the entire placeholder
            caption_template = caption_template.replace(placeholder, "")

    return caption_template


# --- NEW UTILITY: Thumbnail generation for Full Screen (using ffmpeg) ---
async def generate_video_thumbnail(video_path: Path, output_path: Path, timestamp_sec: int) -> bool:
    """
    ভিডিওর একটি নির্দিষ্ট সময় থেকে ffmpeg ব্যবহার করে থাম্বনেইল তৈরি করে।
    """
    time_str = str(timedelta(seconds=timestamp_sec))
    try:
        # Use scale='min(320,iw)':-1 to ensure max width/height is around 320 for efficiency
        cmd = [
            "ffmpeg",
            "-i", str(video_path),
            "-ss", time_str,
            "-vframes", "1",
            "-filter:v", "scale='min(320,iw)':-1", 
            "-an",
            "-y",
            str(output_path)
        ]
        # Run command in a separate thread to avoid blocking the event loop
        await asyncio.to_thread(subprocess.run, cmd, check=True, capture_output=True, timeout=60)

        if output_path.exists() and output_path.stat().st_size > 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        return False
# -----------------------------------------------------------------------


# --- Reconstructed/Modified process_file_and_upload Function ---
# (Assumed to be in this position based on flow)
async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str, messages_to_delete: list = None):
    uid = m.from_user.id
    start_time = datetime.now()
    status_msg = None
    temp_thumb_path = None
    upload_path = in_path
    final_name = original_name
    
    # Get the cancel event if one was created
    cancel_event = [ev for ev in TASKS.get(uid, []) if ev.is_set() is False]
    cancel_event = cancel_event[0] if cancel_event else asyncio.Event()

    try:
        # Check if caption is set and process it
        caption_template = USER_CAPTIONS.get(uid)
        final_caption = None
        if caption_template:
            final_caption = await asyncio.to_thread(process_dynamic_caption, uid, caption_template)
            
        # Determine if it's a video file type (based on snippets)
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".ts", ".webm"}
        is_video = bool(m.video) or any(in_path.suffix.lower() == ext for ext in video_exts)
        
        # Default to document processing if no caption or video is detected
        is_document = not is_video

        # --- VIDEO PROCESSING ---
        if is_video:
            # 1. Conversion to MKV if needed
            if in_path.suffix.lower() not in {".mp4", ".mkv"}:
                mkv_path = TMP / f"{in_path.stem}.mkv"
                try:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
                except Exception:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
                
                if messages_to_delete:
                    messages_to_delete.append(status_msg.id)
                else:
                    messages_to_delete = [status_msg.id]
                    
                ok, err = await convert_to_mkv(in_path, mkv_path, status_msg)
                
                if not ok:
                    try:
                        await status_msg.edit(f"কনভার্সন ব্যর্থ: {err}\nমূল ফাইলটি আপলোড করা হচ্ছে...", reply_markup=None)
                    except Exception:
                        await m.reply_text(f"কনভার্সন ব্যর্থ: {err}\nমূল ফাইলটি আপলোড করা হচ্ছে...", reply_markup=None)
                    upload_path = in_path
                    final_name = original_name
                else:
                    upload_path = mkv_path
                    final_name = Path(original_name).stem + ".mkv"
            else:
                upload_path = in_path
                final_name = original_name

            # 2. Extract Metadata (NEW)
            metadata = await asyncio.to_thread(extract_video_metadata, upload_path)
            duration = metadata.get('duration')
            width = metadata.get('width')
            height = metadata.get('height')

            # 3. Thumbnail Setup
            thumb_path = USER_THUMBS.get(uid)
            if not thumb_path:
                temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
                thumb_time_sec = USER_THUMB_TIME.get(uid, 1) # Default to 1 second
                
                # Use the new async thumbnail generation function
                ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec) 
                
                if ok:
                    thumb_path = str(temp_thumb_path)
                else:
                    temp_thumb_path = None
                    thumb_path = None
                    
            # 4. Upload the Video (MODIFIED)
            upload_attempts = 0
            MAX_RETRIES = 3
            last_exc = None
            
            # Update status message for upload
            if status_msg:
                try:
                    await status_msg.edit("Telegram-এ আপলোড হচ্ছে...", reply_markup=progress_keyboard())
                except:
                    pass
            
            while upload_attempts < MAX_RETRIES:
                upload_attempts += 1
                try:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=upload_path,
                        caption=final_caption,
                        file_name=final_name,
                        progress=pyrogram_progress_wrapper,
                        progress_args=(status_msg if status_msg else m, start_time, 'Uploading'),
                        # --- FULL SCREEN PARAMETERS ---
                        duration=duration,
                        width=width,
                        height=height,
                        thumb=thumb_path if thumb_path else None, 
                        supports_streaming=True, # <--- এটি গুরুত্বপূর্ণ
                        # -----------------------------
                    )
                    # Successful upload
                    if status_msg: await status_msg.delete()
                    if messages_to_delete: await c.delete_messages(m.chat.id, messages_to_delete)
                    return
                except Exception as e:
                    last_exc = e
                    logger.error(f"Video upload attempt {upload_attempts} failed: {e}")
                    await asyncio.sleep(2 ** upload_attempts) # Exponential backoff

            # If loop finishes without success
            raise last_exc # Re-raise the last exception

        # --- DOCUMENT PROCESSING (if not a video) ---
        else:
            # Set default name if needed (should be covered by original_name)
            if not final_name.lower().endswith(in_path.suffix.lower()):
                final_name = final_name + in_path.suffix
                
            # Upload the Document
            await c.send_document(
                chat_id=m.chat.id,
                document=upload_path,
                caption=final_caption,
                file_name=final_name,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg if status_msg else m, start_time, 'Uploading'),
            )
            if status_msg: await status_msg.delete()
            if messages_to_delete: await c.delete_messages(m.chat.id, messages_to_delete)
            return


    except Exception as e:
        logger.error(f"process_file_and_upload error: {e}")
        # Error handling (from snippet 21)
        if status_msg: await status_msg.edit(f"আপলোডে ত্রুটি: {e}", reply_markup=None)
        else: await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            # Clean up files
            if upload_path != in_path and upload_path.exists(): upload_path.unlink(missing_ok=True)
            if in_path.exists(): in_path.unlink(missing_ok=True)
            if temp_thumb_path and Path(temp_thumb_path).exists(): Path(temp_thumb_path).unlink(missing_ok=True)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass
            
            
# --- Reconstructed/Required Audio Change Handlers ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, out_name: str, new_stream_map: list, messages_to_delete: list = None):
    # This is a placeholder, as the actual logic is complex and not fully provided, but required for context.
    # The new_stream_map is used in FFmpeg call based on snippet 16
    uid = m.from_user.id
    
    if not out_name.lower().endswith(".mkv"):
        out_name = Path(out_name).stem + ".mkv"
        
    out_path = TMP / f"remux_{uid}_{int(datetime.now().timestamp())}_{out_name}"
    
    map_args = ["-map", "0:v", "-map", "0:s?", "-map", "0:d?"]
    for stream_index in new_stream_map:
        map_args.extend(["-map", stream_index])
        
    cmd = [
        "ffmpeg",
        "-i", str(in_path),
        "-disposition:a", "0", 
        *map_args, 
        "-disposition:a:0", "default", 
        "-c", "copy", "-metadata", "handler_name=", 
        str(out_path)
    ]
    status_msg = None
    try:
        status_msg = await m.reply_text("অডিও ট্র্যাক অর্ডার পরিবর্তন করা হচ্ছে (Remuxing)...", reply_markup=progress_keyboard())
        
        result = await asyncio.to_thread(
            subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600
        )
        if result.returncode != 0:
            logger.error(f"FFmpeg Remux failed: {result.stderr}")
            out_path.unlink(missing_ok=True)
            raise Exception(f"FFmpeg Remux ব্যর্থ হয়েছে। ত্রুটি: {result.stderr[:500]}...")

        if not out_path.exists() or out_path.stat().st_size == 0:
            raise Exception("পরিবর্তিত ফাইলটি পাওয়া যায়নি বা শূন্য আকারের।")

        await status_msg.edit("অডিও পরিবর্তন সম্পন্ন, ফাইল আপলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        
        all_messages_to_delete = messages_to_delete if messages_to_delete else []
        all_messages_to_delete.append(status_msg.id)
        
        # Now upload the remuxed file
        await process_file_and_upload(c, m, out_path, original_name=out_name, messages_to_delete=all_messages_to_delete)
        
    except Exception as e:
        logger.error(f"handle_audio_remux failed: {e}")
        try:
            if status_msg: await status_msg.edit(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}", reply_markup=None)
            else: await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}")
        except:
            pass
    finally:
        # Clean up files
        try:
            in_path.unlink(missing_ok=True)
            if out_path.exists(): out_path.unlink(missing_ok=True)
        except Exception:
            pass
            
            
async def handle_audio_change_file(c: Client, m: Message):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    file_info = m.video or m.document
    if not file_info:
        await m.reply_text("এই মেসেজে কোনো ভিডিও বা ডকুমেন্ট পাওয়া যায়নি।")
        TASKS[uid].remove(cancel_event)
        return

    original_name = file_info.file_name if file_info.file_name else f"file_{file_info.file_unique_id}"
    tmp_path = TMP / f"audio_change_{uid}_{int(datetime.now().timestamp())}_{original_name}"
    
    status_msg = None
    try:
        status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে এবং অডিও ট্র্যাক চেক করা হচ্ছে...", reply_markup=progress_keyboard())
        
        # 1. Download the file
        await m.download(file_name=str(tmp_path), progress=pyrogram_progress_wrapper, progress_args=(status_msg, datetime.now(), 'Downloading'))
        
        # 2. Get audio tracks using ffprobe
        audio_tracks = await asyncio.to_thread(get_audio_tracks_ffprobe, tmp_path)
        
        if not audio_tracks:
            await status_msg.edit("এই ভিডিওতে কোনো অডিও ট্র্যাক পাওয়া যায়নি বা FFprobe চলতে পারেনি।")
            tmp_path.unlink(missing_ok=True)
            return

        # --- MODIFIED: Handle single audio track auto-remux ---
        if len(audio_tracks) == 1:
            await status_msg.edit("ফাইলটিতে ১টি অডিও ট্র্যাক রয়েছে। স্বয়ংক্রিয়ভাবে রিমাক্স করা হচ্ছে...", reply_markup=progress_keyboard())
            # Get the stream index of the only audio track
            stream_index = audio_tracks[0]['stream_index']
            new_stream_map = [f"0:{stream_index}"]
            
            # Call the remux function directly
            asyncio.create_task(
                handle_audio_remux(
                    c, m, tmp_path, original_name, new_stream_map,
                    messages_to_delete=[status_msg.id]
                )
            )
            return
        # --- END MODIFIED ---

        # 3. Prepare and send the track list (for 2 or more tracks)
        track_list_text = "ফাইলের অডিও ট্র্যাকসমূহ:\n\n"
        for i, track in enumerate(audio_tracks, 1):
            track_list_text += f"{i}. **Stream Index:** {track['stream_index']}, **Language:** {track['language']}, **Title:** {track['title']}\n"
            
        track_list_text += (
            "\n**অডিও অর্ডার দিতে এই মেসেজটিতে রিপ্লাই করে** কমা-সেপারেটেড সংখ্যায় আপনার ট্র্যাক নম্বরগুলো দিন।\n"
            "যেমন, যদি আপনি ৩য় ট্র্যাকটি প্রথমে, ২য়টি দ্বিতীয় এবং ১মটি তৃতীয়তে চান, তাহলে রিপ্লাই করুন: `3,2,1`\n"
        )
        
        # --- MODIFIED: Add info about track deletion for 5+ tracks ---
        if len(audio_tracks) >= 5:
            track_list_text += (
                f"\n**নোট:** এই ফাইলে {len(audio_tracks)}টি ট্র্যাক আছে। আপনি যদি অর্ডারে কম ট্র্যাক দেন (যেমন `1,2`), তাহলে শুধু সেই ট্র্যাকগুলোই রাখা হবে এবং বাকিগুলো মুছে ফেলা হবে।\n"
            )
        else:
            track_list_text += (
                f"\n**নোট:** এই ফাইলে {len(audio_tracks)}টি ট্র্যাক আছে। আপনাকে অবশ্যই {len(audio_tracks)}টি ট্র্যাকের অর্ডার দিতে হবে।"
            )
        
        # 4. Save state and send prompt
        # Delete previous status message
        await status_msg.delete() 

        prompt_msg = await m.reply_text(track_list_text, reply_markup=progress_keyboard(), parse_mode=ParseMode.MARKDOWN)
        
        # Store file data, waiting for user reply
        PENDING_AUDIO_ORDERS[prompt_msg.id] = {
            'uid': uid, 
            'path': str(tmp_path), 
            'original_name': original_name, 
            'tracks': audio_tracks
        }
        
    except Exception as e:
        logger.error(f"Handle audio change failed: {e}")
        try:
            if status_msg: await status_msg.edit(f"অডিও ট্র্যাক প্রক্রিয়া ব্যর্থ: {e}", reply_markup=None)
            else: await m.reply_text(f"অডিও ট্র্যাক প্রক্রিয়া ব্যর্থ: {e}")
        except:
            pass
        finally:
            try:
                if tmp_path.exists(): tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass
# -----------------------------------------------
# *** সংশোধিত: ব্রডকাস্ট কমান্ড *** # ... (broadcast_cmd_no_reply and broadcast_message functions are added here to complete the code based on the snippet)
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd_no_reply(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.reply_text("ব্রডকাস্টের জন্য টেক্সট দিন। উদাহরণ: `/broadcast Hello everyone!`")
        return
        
    await broadcast_message(m.chat.id, parts[1])


async def broadcast_message(admin_chat_id: int, message_text: str):
    success_count = 0
    fail_count = 0
    
    temp_subscribers = list(SUBSCRIBERS) # Create a copy for safe iteration

    for sub_id in temp_subscribers:
        try:
            await app.send_message(sub_id, message_text)
            success_count += 1
            await asyncio.sleep(0.1) # Be gentle with API limits
        except Exception:
            fail_count += 1
            SUBSCRIBERS.discard(sub_id) # Remove failed subscriber
            
    await app.send_message(
        admin_chat_id, 
        f"✅ ব্রডকাস্ট সম্পন্ন হয়েছে।\n\nসফল: {success_count} জন\nব্যর্থ: {fail_count} জন (অক্ষম বা ব্লক করা)"
    )
# -----------------------------------------------

# --- FLASK AND RUNNING THE BOT (Existing) ---
@flask_app.route('/')
def home():
    return render_template_string("<h1>Bot is Running!</h1>")

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        # Assuming int() conversion is done in env loading
        # int("Render URL is not set. Ping service is disabled.") 
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            response = requests.get(url, timeout=10)
            print(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error pinging {url}: {e}")
        time.sleep(600)

def run_flask_and_ping():
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()
    print("Flask and Ping services started.")

async def periodic_cleanup():
    while True:
        try:
            now = datetime.now()
            for p in TMP.iterdir():
                try:
                    if p.is_file():
                        if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(days=3):
                            p.unlink()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(3600)

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(app.start())
        print("Pyrogram Client started.")
        loop.create_task(periodic_cleanup())
        loop.run_forever()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        # FIX: The known Pyrogram exit TypeError is caught here, causing the misleading print.
        # We check the error message and suppress the print if it's the known TypeError during shutdown.
        if "An asyncio.Future, a coroutine or an awaitable is required" not in str(e):
             # Only log unknown/unexpected errors
             print(f"An unexpected error occurred: {e}")
        # Otherwise, silently proceed to finally block for shutdown cleanup
    finally:
        # --- FIX: TypeError: An asyncio.Future, a coroutine or an awaitable is required ---
        # app.stop() কে একটি try/except ব্লক-এ রাখা হলো। যদি app.stop() কোনো coroutine না ফিরিয়ে
        # None ফেরত দেয় (যা ক্লায়েন্ট ইতিমধ্যে বন্ধ হলে হতে পারে), তবে TypeError টি ধরা হবে এবং উপেক্ষা করা হবে।
        try:
            loop.run_until_complete(app.stop())
        except TypeError:
            # Pyrogram client was already stopped or in an invalid state.
            pass
        except Exception as e:
            # অন্য কোনো ত্রুটি হলে তা print করা হবে।
            print(f"Error while attempting to stop Pyrogram client: {e}")
        # --- END FIX ---
        print("Bot has stopped.")
