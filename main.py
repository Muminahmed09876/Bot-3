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
# Stores the path of the downloaded file waiting for audio order
AUDIO_CHANGE_FILE = {} 
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

def get_video_duration(file_path: Path) -> int:
    try:
        parser = createParser(str(file_path))
        if not parser:
            return 0
        with parser:
            metadata = extractMetadata(parser)
        if metadata and metadata.has("duration"):
            return int(metadata.get("duration").total_seconds())
    except Exception:
        return 0
    return 0

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
    waiting_status = " (অর্ডার বাকি)" if uid in AUDIO_CHANGE_FILE else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# --- NEW UTILITY: FFprobe to get audio tracks ---
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
        # Clean up any pending file path
        if uid in AUDIO_CHANGE_FILE:
            try:
                Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                    await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
            except Exception:
                pass
            AUDIO_CHANGE_FILE.pop(uid, None)
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
    
    waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"
    
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
            # Turning OFF: Clear mode and cleanup pending file
            MKV_AUDIO_CHANGE_MODE.discard(uid)
            if uid in AUDIO_CHANGE_FILE:
                try:
                    Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                    if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                        await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
                except Exception:
                    pass
                AUDIO_CHANGE_FILE.pop(uid, None)
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
        
        waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"

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
        USER_COUNTERS.pop(uid, None) # New: reset counter on new caption set
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- Handle audio order input if in mode and file is set ---
    if uid in MKV_AUDIO_CHANGE_MODE and uid in AUDIO_CHANGE_FILE:
        file_data = AUDIO_CHANGE_FILE.get(uid)
        if not file_data or not file_data.get('tracks'):
            await m.reply_text("অডিও ট্র্যাকের তথ্য পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।")
            AUDIO_CHANGE_FILE.pop(uid, None)
            return

        tracks = file_data['tracks']
        try:
            # Parse the input like "3,2,1"
            new_order_str = [x.strip() for x in text.split(',') if x.strip()]
            
            # --- MODIFIED VALIDATION LOGIC ---
            num_tracks_in_file = len(tracks)
            num_tracks_requested = len(new_order_str)

            # Case 1: File has less than 5 tracks. User MUST provide all tracks.
            if num_tracks_in_file < 5:
                if num_tracks_requested != num_tracks_in_file:
                    await m.reply_text(f"এই ফাইলে {num_tracks_in_file}টি অডিও ট্র্যাক আছে। আপনাকে অবশ্যই {num_tracks_in_file}টি ট্র্যাকের অর্ডার দিতে হবে। উদাহরণ: `3,2,1`")
                    return
            
            # Case 2: File has 5 or more tracks. User can provide 1 to num_tracks_in_file tracks.
            else: # num_tracks_in_file >= 5
                if num_tracks_requested == 0:
                    await m.reply_text("আপনাকে অন্তত একটি ট্র্যাক নম্বর দিতে হবে।")
                    return
                if num_tracks_requested > num_tracks_in_file:
                    await m.reply_text(f"আপনি {num_tracks_requested}টি ট্র্যাক চেয়েছেন, কিন্তু ফাইলে মাত্র {num_tracks_in_file}টি ট্র্যাক আছে।")
                    return
                # If num_tracks_requested is valid (1 to num_tracks_in_file), we proceed.
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

            track_list_message_id = file_data.get('message_id')

            # Start the audio remux process
            asyncio.create_task(
                handle_audio_remux(
                    c, m, file_data['path'], 
                    file_data['original_name'], 
                    new_stream_map, 
                    messages_to_delete=[track_list_message_id, m.id]
                )
            )

            # Clear state immediately
            AUDIO_CHANGE_FILE.pop(uid, None) # Clear only the waiting file state
            return

        except ValueError:
            await m.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `3,2,1`")
            return
        except Exception as e:
            logger.error(f"Audio remux preparation error: {e}")
            await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া শুরু করতে সমস্যা: {e}")
            AUDIO_CHANGE_FILE.pop(uid, None)
            return
    # -----------------------------------------------------

    # --- NEW: Handle Post Creation Editing Steps ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) # Track user's response message
        
        current_state = state_data['state']
        
        if current_state == 'awaiting_name_change':
            # Step 1: Image Name Change
            if not text:
                prompt_msg = await m.reply_text("নাম খালি রাখা যাবে না। সঠিক নামটি দিন।")
                state_data['message_ids'].append(prompt_msg.id)
                return
            
            state_data['post_data']['image_name'] = text
            state_data['state'] = 'awaiting_genres_add'
            
            new_caption = generate_post_caption(state_data['post_data'])
            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in name change: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            # Send prompt for the next edit step
            prompt_msg = await m.reply_text(
                f"✅ ছবির নাম সেট হয়েছে: `{text}`\n\n**এখন Genres যোগ করুন।**\n"
                f"উদাহরণ: `Comedy, Romance, Action`"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_genres_add':
            # Step 2: Genres Add
            state_data['post_data']['genres'] = text # Text can be empty here if user wants no genres
            state_data['state'] = 'awaiting_season_list'
            
            new_caption = generate_post_caption(state_data['post_data'])
            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in genres add: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            # Send prompt for the final edit step
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
            
            # Cleanup state image_path = state_data['image_path']
            image_path = state_data['image_path']
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
            
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            
            await m.reply_text("✅ পোস্ট তৈরি সফলভাবে সম্পন্ন হয়েছে এবং সমস্ত অতিরিক্ত বার্তা মুছে ফেলা হয়েছে।")
            return
    # --- END NEW: Handle Post Creation Editing Steps ---


    # Handle auto URL upload
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))
    
@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>\nউদাহরণ: /upload_url https://example.com/file.mp4")
        return
    url = m.text.split(None, 1)[1].strip()
    asyncio.create_task(handle_url_download_and_upload(c, m, url))

async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    try:
        status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)

        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
            safe_name += ".mp4"

        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        ok, err = False, None
        
        try:
            await status_msg.edit("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())

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
                if tmp_in.exists():
                    tmp_in.unlink()
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
    
    try:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        source_message = m
        file_info = source_message.video or source_message.document

        if not file_info:
            try:
                await status_msg.edit("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            except Exception:
                await m.reply_text("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            return
        
        # Process the dynamic caption
        final_caption = process_dynamic_caption(uid, caption_to_use)
        
        if file_info.file_id:
            try:
                if source_message.video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=file_info.file_id,
                        caption=final_caption,
                        thumb=file_info.thumbs[0].file_id if file_info.thumbs else None,
                        duration=file_info.duration,
                        supports_streaming=True,
                        parse_mode=ParseMode.MARKDOWN
                    )
                elif source_message.document:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=file_info.file_id,
                        file_name=file_info.file_name,
                        caption=final_caption,
                        thumb=file_info.thumbs[0].file_id if file_info.thumbs else None,
                        parse_mode=ParseMode.MARKDOWN
                    )
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                try:
                    await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                except Exception:
                    await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                return
        else:
            try:
                await status_msg.edit("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            except Exception:
                await m.reply_text("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            return
        
        # New code to auto-delete the success message
        try:
            success_msg = await status_msg.edit("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            await success_msg.delete()
        except Exception:
            success_msg = await m.reply_text("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            await success_msg.delete()

    except Exception as e:
        traceback.print_exc()
        try:
            await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
    finally:
        try:
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
    if uid in EDIT_CAPTION_MODE and m.forward_date: # Only apply to forwarded media to avoid accidental re-upload of direct files
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
            await m.download(file_name=str(tmp_path))
            try:
                await status_msg.edit("ডাউনলোড সম্পন্ন, এখন Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
            except Exception:
                await m.reply_text("ডাউনলোড সম্পন্ন, এখন Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
                
            # NEW RENAME FEATURE: ফরওয়ার্ডেড ফাইলের জন্য নাম পরিবর্তন
            renamed_file = generate_new_filename(original_name)
            # -------------------------------------------------------

            await process_file_and_upload(c, m, tmp_path, original_name=renamed_file, messages_to_delete=[status_msg.id])
        except Exception as e:
            await m.reply_text(f"ফাইল প্রসেসিংয়ে সমস্যা: {e}")
        finally:
            try:
                TASKS[uid].remove(cancel_event)
            except Exception:
                pass
    else:
        # A direct video/document which isn't handled by another mode. Pass.
        pass

# --- HANDLER FUNCTION: Handle file in audio change mode ---
async def handle_audio_change_file(c: Client, m: Message):
    uid = m.from_user.id
    file_info = m.video or m.document
    
    if not file_info:
        await m.reply_text("এটি একটি ভিডিও ফাইল নয়।")
        return

    # If there's already a file waiting for audio order, cancel the previous one
    if uid in AUDIO_CHANGE_FILE:
        try:
            Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
            if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
        except Exception:
            pass
        AUDIO_CHANGE_FILE.pop(uid, None)
    
    # Download the file
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    tmp_path = None
    status_msg = None
    try:
        original_name = file_info.file_name or f"video_{file_info.file_unique_id}.mkv"
        # Ensure it has an extension for ffprobe
        if not '.' in original_name:
            original_name += '.mkv'
            
        tmp_path = TMP / f"audio_change_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        
        status_msg = await m.reply_text("অডিও ট্র্যাক বিশ্লেষণের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        await m.download(file_name=str(tmp_path))
        
        # Use FFprobe to get audio tracks
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
                    c, m, tmp_path, 
                    original_name, 
                    new_stream_map, 
                    messages_to_delete=[status_msg.id]
                )
            )
            
            # We don't set AUDIO_CHANGE_FILE, so the function ends here.
            # tmp_path will be deleted by handle_audio_remux
            return 
        # --- END MODIFIED ---

        # Prepare and send the track list (for 2 or more tracks)
        track_list_text = "ফাইলের অডিও ট্র্যাকসমূহ:\n\n"
        for i, track in enumerate(audio_tracks, 1):
            track_list_text += f"{i}. **Stream Index:** {track['stream_index']}, **Language:** {track['language']}, **Title:** {track['title']}\n"
            
        track_list_text += (
            "\nএখন আপনি কোন অডিও ট্র্যাকটি প্রথমে (primary) চান, সেই অনুযায়ী ট্র্যাক নম্বর (উপরে ১, ২, ৩...) কমা-সেপারেটেড সংখ্যায় দিন।\n"
            "যেমন, যদি আপনি ৩য় ট্র্যাকটি প্রথমে, ২য়টি দ্বিতীয় এবং ১মটি তৃতীয়তে চান, তাহলে লিখুন: `3,2,1`\n"
        )
        
        # --- MODIFIED: Add info about track deletion for 5+ tracks ---
        if len(audio_tracks) >= 5:
            track_list_text += (
                f"\n**নোট:** এই ফাইলে {len(audio_tracks)}টি ট্র্যাক আছে। আপনি যদি অর্ডারে কম ট্র্যাক দেন (যেমন `1,2`), তাহলে শুধু সেই ট্র্যাকগুলোই রাখা হবে এবং বাকিগুলো মুছে ফেলা হবে।\n"
            )
        else:
            track_list_text += (
                f"\n**নোট:** এই ফাইলে {len(audio_tracks)}টি ট্র্যাক আছে। আপনাকে অবশ্যই {len(audio_tracks)}টি ট্র্যাকের অর্ডার দিতে হবে।\n"
            )
        # --- END MODIFIED ---
            
        track_list_text += (
            "আপনি যদি অডিও পরিবর্তন না করতে চান, তাহলে `/mkv_video_audio_change` লিখে মোড অফ করুন।"
        )
        
        await status_msg.edit(track_list_text) 
        
        # Store file info for the next text message handler
        AUDIO_CHANGE_FILE[uid] = {
            'path': tmp_path, 
            'original_name': original_name,
            'tracks': audio_tracks,
            'message_id': status_msg.id
        }
        
    except Exception as e:
        logger.error(f"Audio track analysis error: {e}")
        if status_msg:
            await status_msg.edit(f"অডিও ট্র্যাক বিশ্লেষণে সমস্যা: {e}")
        else:
            await m.reply_text(f"অডিও ট্র্যাক বিশ্লেষণে সমস্যা: {e}")
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass
# -----------------------------------------------------

# --- HANDLER FUNCTION: Handle audio remux ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    # NEW RENAME FEATURE: অডিও পরিবর্তন করার পর নতুন নাম সেট করা
    out_name = generate_new_filename(original_name)
    # Ensure the output is an MKV file after remuxing
    if not out_name.lower().endswith(".mkv"):
        out_name = out_name.split(".")[0] + ".mkv"
    # ------------------------------------------------------------------
    out_path = TMP / f"remux_{uid}_{int(datetime.now().timestamp())}_{out_name}"
    
    map_args = ["-map", "0:v", "-map", "0:s?", "-map", "0:d?"] # 0:s? and 0:d? maps them if they exist
    # Add the user-specified audio maps
    for stream_index in new_stream_map:
        map_args.extend(["-map", stream_index])
        
    cmd = [
        "ffmpeg",
        "-i", str(in_path),
        "-disposition:a", "0",            # FIX: সমস্ত অডিও ট্র্যাকের 'Default' ফ্ল্যাগ রিসেট
        *map_args,
        "-disposition:a:0", "default",    # FIX: নতুন অর্ডারের প্রথম ট্র্যাককে (a:0) ডিফল্ট হিসেবে সেট
        "-c", "copy",
        "-metadata", "handler_name=", # Clear metadata
        str(out_path)
    ]

    status_msg = None
    try:
        status_msg = await m.reply_text("অডিও ট্র্যাক অর্ডার পরিবর্তন করা হচ্ছে (Remuxing)...", reply_markup=progress_keyboard())
        
        # Run the FFmpeg command
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=3600
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

        # Proceed to final upload
        await process_file_and_upload(c, m, out_path, original_name=out_name, messages_to_delete=all_messages_to_delete) 

    except Exception as e:
        logger.error(f"Audio remux process error: {e}")
        try:
            if status_msg:
                await status_msg.edit(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}")
            else:
                await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}")
        except Exception:
            pass
    finally:
        try:
            in_path.unlink(missing_ok=True)
            out_path.unlink(missing_ok=True)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass
# ---------------------------------------------------


@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("ভিডিও/ডকুমেন্ট ফাইলের reply দিয়ে এই কমান্ড দিন।\nUsage: /rename new_name.mp4")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন ফাইল নাম দিন। উদাহরণ: /rename new_video.mp4")
        return
    new_name = m.text.split(None, 1)[1].strip()
    new_name = re.sub(r"[\\/*?\"<>|:]", "_", new_name)
    
    # NOTE: /rename is an explicit user command to set a custom name, so we don't apply the auto-rename here.
    
    await m.reply_text(f"ভিডিও রিনেম করা হবে: {new_name}\n(রিনেম করতে reply করা ফাইলটি পুনরায় ডাউনলোড করে আপলোড করা হবে)")

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    try:
        status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    tmp_out = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{new_name}"
    try:
        await m.reply_to_message.download(file_name=str(tmp_out))
        try:
            await status_msg.edit("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        except Exception:
            await m.reply_text("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_out, original_name=new_name, messages_to_delete=[status_msg.id])
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    if uid in TASKS and TASKS[uid]:
        for ev in list(TASKS[uid]):
            try:
                ev.set()
            except:
                pass
        
        # New: Clean up audio change state if in progress
        if uid in MKV_AUDIO_CHANGE_MODE:
            # We don't clear the mode, but clear the waiting file state if it exists
            if uid in AUDIO_CHANGE_FILE:
                if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                    try:
                        await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
                    except Exception:
                        pass
                try:
                    Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                except Exception:
                    pass
                AUDIO_CHANGE_FILE.pop(uid, None)
            
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

# ---- main processing and upload (functions simplified for brevity, assuming they work) ----
async def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int = 1):
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-ss", str(timestamp_sec),
            "-vframes", "1",
            "-vf", "scale=320:-1",
            str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        logger.warning("Thumbnail generate error: %s", e)
        return False

async def convert_to_mkv(in_path: Path, out_path: Path, status_msg: Message):
    try:
        try:
            await status_msg.edit("ভিডিওটি MKV ফরম্যাটে কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            await status_msg.edit("ভিডিওটি MKV ফরম্যাটে কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
        # Use simple stream copy first
        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            "-codec", "copy",
            str(out_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=1200)
        
        if result.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
            # Fallback to full re-encoding if stream copy fails
            logger.warning("Container conversion failed or output is empty, attempting full re-encoding.")
            try:
                await status_msg.edit("ভিডিওটি MKV ফরম্যাটে পুনরায় এনকোড করা হচ্ছে...", reply_markup=progress_keyboard())
            except Exception:
                await status_msg.edit("ভিডিওটি MKV ফরম্যাটে পুনরায় এনকোড করা হচ্ছে...", reply_markup=progress_keyboard())
            
            # Remove failed output before re-encoding
            out_path.unlink(missing_ok=True) 

            cmd_full = [
                "ffmpeg",
                "-i", str(in_path),
                "-c:v", "libx264",
                "-preset", "fast",
                "-crf", "23",
                "-c:a", "copy",
                "-map_metadata", "0", # Keep metadata from input
                "-movflags", "+faststart", # For MP4
                str(out_path)
            ]
            result_full = subprocess.run(cmd_full, capture_output=True, text=True, check=False, timeout=3600)
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
    if quality_match:
        options_str = quality_match.group(1)
        options = [opt.strip() for opt in options_str.split(',')]
        
        # Store the number of options if not already stored
        if not USER_COUNTERS[uid]['re_options_count']:
            USER_COUNTERS[uid]['re_options_count'] = len(options)
        
        # Calculate the current index in the cycle
        current_index = (USER_COUNTERS[uid]['uploads'] - 1) % len(options)
        current_quality = options[current_index]
        
        # Replace the placeholder with the current quality
        caption_template = caption_template.replace(quality_match.group(0), current_quality)

        # Check if a full cycle has completed and increment counters
        # Increment happens when we are about to start a new cycle (i.e., when (uploads - 1) % len == 0, but for uploads > 1)
        if (USER_COUNTERS[uid]['uploads'] - 1) % USER_COUNTERS[uid]['re_options_count'] == 0 and USER_COUNTERS[uid]['uploads'] > 1:
            # Increment all dynamic counters
            for key in USER_COUNTERS[uid]['dynamic_counters']:
                USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1
    elif USER_COUNTERS[uid]['uploads'] > 1: # Increment all counters if no quality cycle is used
        for key in USER_COUNTERS[uid].get('dynamic_counters', {}):
             USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1


    # --- 2. Main counter logic (e.g., [12], [(21)]) ---
    # Find all number-based placeholders
    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    
    # Initialize counters on the first upload
    if USER_COUNTERS[uid]['uploads'] == 1:
        for match in counter_matches:
            # Check if the number has parentheses
            has_paren = match.startswith('(') and match.endswith(')')
            # Clean the number to use as a key
            clean_match = re.sub(r'[()]', '', match)
            # Store the original format and the starting value
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': int(clean_match), 'has_paren': has_paren}
    
    # If not first upload but no quality cycle, the counter has already been incremented above. 
    # If the quality cycle is used, the increment happens inside the quality cycle logic.

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
        text_to_add = match[0].strip() # e.g., "End", "hi"
        target_num_str = re.sub(r'[^0-9]', '', match[1]).strip() # e.g., "02", "05"

        placeholder = re.escape(f"[{match[0].strip()} ({match[1].strip()})]")
        
        try:
            target_num = int(target_num_str)
        except ValueError:
            # Invalid number, skip or replace with empty string
            caption_template = re.sub(placeholder, "", caption_template)
            continue
        
        # FIX: New logic - show TEXT only if current_episode_num IS EQUAL TO target_num
        if current_episode_num == target_num:
            # Replace placeholder with the actual TEXT
            caption_template = re.sub(placeholder, text_to_add, caption_template)
        else:
            # Replace placeholder with an empty string
            caption_template = re.sub(placeholder, "", caption_template)

    # Final formatting
    return "**" + "\n".join(caption_template.splitlines()) + "**"


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    temp_thumb_path = None
    final_caption_template = USER_CAPTIONS.get(uid)
    status_msg = None # Initialize status_msg

    try:
        # NOTE: original_name is already the desired final name due to changes in calling functions
        final_name = original_name or in_path.name
        
        # সংশোধিত লাইন: Pyrogram-এর ডিটেকশন ব্যর্থ হলেও ফাইলের এক্সটেনশন দেখে ভিডিও হিসেবে চিহ্নিত করবে।
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        is_video = bool(m.video) or any(in_path.suffix.lower() == ext for ext in video_exts)
        
        if is_video:
            # Only convert if it's NOT .mp4 OR .mkv, as mkv is the preferred format for video/document
            if in_path.suffix.lower() not in {".mp4", ".mkv"}:
                mkv_path = TMP / f"{in_path.stem}.mkv"
                try:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
                except Exception:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_auto.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
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
                else:
                    upload_path = mkv_path
                    # Since we successfully converted to MKV, the final name must reflect this extension
                    final_name = Path(final_name).stem + ".mkv" 
        
        thumb_path = USER_THUMBS.get(uid)
        
        if is_video and not thumb_path:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            thumb_time_sec = USER_THUMB_TIME.get(uid, 1) # Default to 1 second
            ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
            if ok:
                thumb_path = str(temp_thumb_path)

        try:
            # If status_msg exists from conversion, edit it. Otherwise, send new.
            if status_msg:
                await status_msg.edit("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
            else:
                status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
             status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
             
        if messages_to_delete:
            if status_msg.id not in messages_to_delete:
                messages_to_delete.append(status_msg.id)
        else:
            messages_to_delete = [status_msg.id]


        if cancel_event.is_set():
            if messages_to_delete:
                try:
                    await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                except Exception:
                    pass
            try:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            except Exception:
                await m.reply_text("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            TASKS[uid].remove(cancel_event)
            return
        
        duration_sec = get_video_duration(upload_path) if upload_path.exists() else 0
        
        caption_to_use = final_name
        if final_caption_template:
            caption_to_use = process_dynamic_caption(uid, final_caption_template)

        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=thumb_path,
                        duration=duration_sec,
                        supports_streaming=True,
                        file_name=final_name, # Pass the final name for video uploads
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=final_name,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                if messages_to_delete:
                    try:
                        # Delete all tracked messages on SUCCESS
                        await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                    except Exception:
                        pass
                
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                logger.warning("Upload attempt %s failed: %s", attempt, e)
                await asyncio.sleep(2 * attempt)
                if cancel_event.is_set():
                    if messages_to_delete:
                        try:
                            await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                        except Exception:
                            pass
                    break

        if last_exc:
            if status_msg:
                await status_msg.edit(f"আপলোড ব্যর্থ: {last_exc}", reply_markup=None)
            else:
                await m.reply_text(f"আপলোড ব্যর্থ: {last_exc}", reply_markup=None)
    except Exception as e:
        if status_msg:
            await status_msg.edit(f"আপলোডে ত্রুটি: {e}")
        else:
            await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            # Clean up files
            if upload_path != in_path and upload_path.exists():
                upload_path.unlink()
            if in_path.exists():
                in_path.unlink()
            if temp_thumb_path and Path(temp_thumb_path).exists():
                Path(temp_thumb_path).unlink()
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# *** সংশোধিত: ব্রডকাস্ট কমান্ড ***
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd_no_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message:
        await m.reply_text("ব্রডকাস্ট করতে যেকোনো মেসেজে (ছবি, ভিডিও বা টেক্সট) **রিপ্লাই করে** এই কমান্ড দিন।")
        return

@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_cmd_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    
    source_message = m.reply_to_message
    if not source_message:
        await m.reply_text("ব্রডকাস্ট করার জন্য একটি মেসেজে রিপ্লাই করে এই কমান্ড দিন।")
        return

    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} সাবস্ক্রাইবারে...", quote=True)
    failed = 0
    sent = 0
    for chat_id in list(SUBSCRIBERS):
        if chat_id == m.chat.id:
            continue
        try:
            await c.forward_messages(chat_id=chat_id, from_chat_id=source_message.chat.id, message_ids=source_message.id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            failed += 1
            logger.warning("Broadcast to %s failed: %s", chat_id, e)

    await m.reply_text(f"ব্রডকাস্ট শেষ। পাঠানো: {sent}, ব্যর্থ: {failed}")

# --- Flask Web Server ---
@flask_app.route('/')
def home():
    html_content = """
    <!DOCTYPE-html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot Status</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f0f2f5;
                color: #333;
                text-align: center;
                padding-top: 50px;
            }
            .container {
                background-color: #fff;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                display: inline-block;
            }
            h1 {
                color: #28a745;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>TA File Share Bot is running! ✅</h1>
            <p>This page confirms that the bot's web server is active.</p>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_content)

# Ping service to keep the bot alive
def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        print("Render URL is not set. Ping service is disabled.")
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
        loop.create_task(periodic_cleanup())
    except RuntimeError:
        pass
    app.run()
