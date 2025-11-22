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

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 
ADMIN_ID = int(os.getenv("ADMIN_ID", ""))

# Constants
MAX_SIZE = 4 * 1024 * 1024 * 1024
TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# --- NEW CONSTANTS FOR RENAMING ---
AUDIO_TITLE_TAG = "[@TA_HD_Anime] Telegram Channel"
BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"

# Global State Variables
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
USER_COUNTERS = {}
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}
MKV_AUDIO_CHANGE_MODE = set()
PENDING_AUDIO_ORDERS = {} 
CREATE_POST_MODE = set()
POST_CREATION_STATE = {} 

# Default Post Data
DEFAULT_POST_DATA = {
    'image_name': "Image Name",
    'genres': "",
    'season_list_raw': "1, 2" 
}

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ==================================================================
#                        UTILITY FUNCTIONS
# ==================================================================

def is_admin(uid: int) -> bool:
    """Check if user is admin."""
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    """Check if URL is a Google Drive URL."""
    return "drive.google.com" in url or "docs.google.com" in url

def extract_drive_id(url: str) -> str:
    """Extract file ID from Google Drive URL."""
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

def generate_new_filename(original_name: str, target_ext: str = None) -> str:
    """
    Generates the new standardized filename.
    If target_ext is provided, it overrides the original extension.
    """
    file_path = Path(original_name)
    
    if target_ext:
        # Ensure dot is present
        if not target_ext.startswith('.'):
            file_ext = '.' + target_ext
        else:
            file_ext = target_ext
    else:
        file_ext = file_path.suffix.lower()
        # Default to mp4 if extension is missing or invalid
        if not file_ext or file_ext == '.':
            file_ext = ".mp4"
        
    return BASE_NEW_NAME + file_ext

def check_has_opus_audio(file_path: Path) -> bool:
    """Checks if the file has any OPUS audio stream using ffprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "a",
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        for stream in metadata.get('streams', []):
            if stream.get('codec_name') == 'opus':
                return True
    except Exception as e:
        logger.error(f"Opus check failed: {e}")
    return False

def get_video_metadata(file_path: Path) -> dict:
    """Extracts duration, width, and height using FFprobe (with Hachoir fallback)."""
    data = {'duration': 0, 'width': 0, 'height': 0}
    try:
        # 1. FFprobe (Primary Method)
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format", 
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        
        video_stream = None
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'video':
                video_stream = stream
                break
        
        if video_stream:
            data['width'] = int(video_stream.get('width', 0))
            data['height'] = int(video_stream.get('height', 0))
        
        duration_str = metadata.get('format', {}).get('duration')
        if not duration_str and video_stream:
            duration_str = video_stream.get('duration')
            
        if duration_str:
            try:
                data['duration'] = int(float(duration_str))
            except (ValueError, TypeError):
                data['duration'] = 0 
        
        if data['width'] == 0 or data['height'] == 0:
            raise Exception("FFprobe returned 0 dimensions, trying Hachoir")

    except Exception as e:
        logger.warning(f"FFprobe metadata extraction failed: {e}. Trying Hachoir fallback...")
        # 2. Hachoir (Fallback Method)
        try:
            parser = createParser(str(file_path))
            if not parser:
                return data 
            with parser:
                h_metadata = extractMetadata(parser)
            if not h_metadata:
                return data 
            
            if h_metadata.has("duration") and data['duration'] == 0:
                data['duration'] = int(h_metadata.get("duration").total_seconds())
            if h_metadata.has("width") and data['width'] == 0:
                data['width'] = int(h_metadata.get("width"))
            if h_metadata.has("height") and data['height'] == 0:
                data['height'] = int(h_metadata.get("height"))
        except Exception as he:
            logger.error(f"Hachoir fallback ALSO failed: {he}")
    
    return data

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

def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
    waiting_status = f" ({waiting_count}টি অর্ডার বাকি)" if waiting_count > 0 else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)

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
                language = stream.get('tags', {}).get('language', 'und')
                audio_tracks.append({
                    'stream_index': stream_index,
                    'title': title,
                    'language': language
                })
        return audio_tracks
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return []

def generate_post_caption(data: dict) -> str:
    image_name = data.get('image_name', DEFAULT_POST_DATA['image_name'])
    genres = data.get('genres', DEFAULT_POST_DATA['genres'])
    season_list_raw = data.get('season_list_raw', DEFAULT_POST_DATA['season_list_raw'])

    season_entries = []
    parts = re.split(r'[,\s]+', season_list_raw.strip())
    parts = [p.strip() for p in parts if p.strip()]

    for part in parts:
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                if start > end: start, end = end, start
                for i in range(start, end + 1):
                    season_entries.append(f"**{image_name} Season {i:02d}**") 
            except ValueError: continue
        else:
            try:
                num = int(part)
                season_entries.append(f"**{image_name} Season {num:02d}**")
            except ValueError: continue

    unique_season_entries = list(dict.fromkeys(season_entries))
    if not unique_season_entries:
        unique_season_entries.append("**Coming Soon...**")
    elif unique_season_entries[-1] != "**Coming Soon...**" and unique_season_entries[0] != "**Coming Soon...**":
        unique_season_entries.append("**Coming Soon...**")
        
    season_text = "\n".join(unique_season_entries)
    
    collapsible_text_parts = [f"> **{image_name} All Season List :-**", "> "]
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}")
        collapsible_text_parts.append("> ")
        
    if collapsible_text_parts and collapsible_text_parts[-1] == "> ":
        collapsible_text_parts.pop()
        
    base_caption = (
        f"**{image_name}**\n"
        f"**────────────────────**\n"
        f"**‣ Audio - Hindi Official**\n"
        f"**‣ Quality - 480p, 720p, 1080p**\n"
        f"**‣ Genres - {genres}**\n"
        f"**────────────────────**"
    )
    
    final_caption = f"{base_caption}\n\n" + "\n".join(collapsible_text_parts)
    return final_caption

# ==================================================================
#                        NETWORK HELPERS
# ==================================================================

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
        BotCommand("create_post", "নতুন পোস্ট তৈরি করুন (admin only)"),
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"), 
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

# ==================================================================
#                        COMMAND HANDLERS
# ==================================================================

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
        "/create_post - নতুন পোস্ট তৈরি করুন (admin only)\n"
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
            await m.reply_text("সঠিক ফরম্যাটে সময় দিন। উদাহরণ: `/setthumb 5s`, `/setthumb 1m`")
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

    await m.reply_text("আপনার থাম্বনেইল/থাম্বনেইল তৈরির সময় মুছে ফেলা হয়েছে।")

@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    
    # Handle Create Post Mode Image
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE and POST_CREATION_STATE[uid]['state'] == 'awaiting_image':
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id)
        
        out = TMP / f"post_img_{uid}.jpg"
        try:
            download_msg = await m.reply_text("ছবি ডাউনলোড হচ্ছে...")
            state_data['message_ids'].append(download_msg.id)
            
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((1080, 1080))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            state_data['image_path'] = str(out)
            state_data['state'] = 'awaiting_name_change'
            
            initial_caption = generate_post_caption(state_data['post_data'])
            
            post_msg = await c.send_photo(
                chat_id=m.chat.id, 
                photo=str(out), 
                caption=initial_caption, 
                parse_mode=ParseMode.MARKDOWN
            )
            state_data['post_message_id'] = post_msg.id
            state_data['message_ids'].append(post_msg.id)
            
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
        return

    # Handle Thumbnail Setting
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
            USER_THUMB_TIME.pop(uid, None)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
    else:
        pass

@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    USER_COUNTERS.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. `[01]` (নম্বর বৃদ্ধি)\n"
        "2. `[re (480p, 720p)]` (সাইকেল)\n"
        "3. `[End (02)]` (শর্তসাপেক্ষ)"
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
        USER_COUNTERS.pop(uid, None)
        await cb.message.edit_text("আপনার ক্যাপশন মুছে ফেলা হয়েছে।")
    else:
        await cb.answer("আপনার কোনো ক্যাপশন সেভ করা নেই।", show_alert=True)

@app.on_message(filters.command("edit_caption_mode") & filters.private)
async def toggle_edit_caption_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in EDIT_CAPTION_MODE:
        EDIT_CAPTION_MODE.discard(uid)
        await m.reply_text("edit video caption mod **OFF**.")
    else:
        EDIT_CAPTION_MODE.add(uid)
        await m.reply_text("edit video caption mod **ON**.")

@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in MKV_AUDIO_CHANGE_MODE:
        MKV_AUDIO_CHANGE_MODE.discard(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অফ** করা হয়েছে।")
    else:
        MKV_AUDIO_CHANGE_MODE.add(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে।")

@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
        if uid in POST_CREATION_STATE:
            state_data = POST_CREATION_STATE.pop(uid)
            try:
                if state_data.get('image_path'):
                    Path(state_data['image_path']).unlink(missing_ok=True)
                messages_to_delete = state_data.get('message_ids', [])
                post_id = state_data.get('post_message_id')
                if post_id and post_id in messages_to_delete:
                    messages_to_delete.remove(post_id) 
                if messages_to_delete:
                    await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Post mode OFF cleanup error: {e}")
                
        await m.reply_text("Create Post Mode **অফ** করা হয়েছে।")
    else:
        CREATE_POST_MODE.add(uid)
        POST_CREATION_STATE[uid] = {
            'image_path': None, 
            'message_ids': [m.id], 
            'state': 'awaiting_image', 
            'post_data': DEFAULT_POST_DATA.copy(),
            'post_message_id': None
        }
        await m.reply_text("Create Post Mode **অন** করা হয়েছে।\nএকটি ছবি (**Photo**) পাঠান যা পোস্টের ইমেজ হিসেবে ব্যবহার হবে।")

@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    await m.reply_text("বর্তমান মোড স্ট্যাটাস:", reply_markup=mode_check_keyboard(uid))

@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE: MKV_AUDIO_CHANGE_MODE.discard(uid)
        else: MKV_AUDIO_CHANGE_MODE.add(uid)    
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE: EDIT_CAPTION_MODE.discard(uid)
        else: EDIT_CAPTION_MODE.add(uid)
            
    try:
        await cb.message.edit_text("বর্তমান মোড স্ট্যাটাস:", reply_markup=mode_check_keyboard(uid))
        await cb.answer("মোড পরিবর্তন করা হয়েছে।", show_alert=True)
    except Exception:
        await cb.answer("মোড পরিবর্তন করা হয়েছে।", show_alert=True)

@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None)
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে।")
        return

    # --- Handle audio order input (UPDATED FOR FLEXIBLE INPUT) ---
    if m.reply_to_message and m.reply_to_message.id in PENDING_AUDIO_ORDERS:
        prompt_message_id = m.reply_to_message.id
        file_data = PENDING_AUDIO_ORDERS.get(prompt_message_id)
        
        if file_data['uid'] != uid:
             await m.reply_text("আপনি এই ফাইলের জন্য অর্ডার দিতে পারবেন না।")
             return

        tracks = file_data['tracks']
        try:
            # Parse input "1,3" or "2, 4"
            parts = [x.strip() for x in text.split(',') if x.strip()]
            
            if not parts:
                await m.reply_text("অন্তত একটি ট্র্যাক নম্বর দিন।")
                return

            new_stream_map = []
            valid_indices = list(range(1, len(tracks) + 1))
            
            for p in parts:
                try:
                    idx = int(p)
                except ValueError:
                    await m.reply_text("শুধুমাত্র সংখ্যা ব্যবহার করুন (যেমন 1, 3)।")
                    return
                    
                if idx not in valid_indices:
                     await m.reply_text(f"ভুল ট্র্যাক নম্বর: {idx}। ট্র্যাক নম্বরগুলো হতে হবে: {', '.join(map(str, valid_indices))}")
                     return
                
                # Map to source stream index
                stream_index_to_map = tracks[idx-1]['stream_index']
                new_stream_map.append(f"0:{stream_index_to_map}") 

            # Start the audio remux process
            asyncio.create_task(
                handle_audio_remux(
                    c, m, file_data['path'], 
                    file_data['original_name'], 
                    new_stream_map, 
                    messages_to_delete=[prompt_message_id, m.id]
                )
            )

            PENDING_AUDIO_ORDERS.pop(prompt_message_id, None) 
            return

        except Exception as e:
            logger.error(f"Audio remux error: {e}")
            await m.reply_to_message.reply_text(f"ত্রুটি: {e}")
            try: Path(file_data['path']).unlink(missing_ok=True)
            except Exception: pass
            PENDING_AUDIO_ORDERS.pop(prompt_message_id, None)
            return
    # -----------------------------------------------------

    # --- Handle Post Creation Steps ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id)
        current_state = state_data['state']
        
        if current_state == 'awaiting_name_change':
            if not text: return
            state_data['post_data']['image_name'] = text
            state_data['state'] = 'awaiting_genres_add'
            new_caption = generate_post_caption(state_data['post_data'])
            await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            prompt_msg = await m.reply_text(f"✅ ছবির নাম সেট হয়েছে: `{text}`\n\n**এখন Genres যোগ করুন।**")
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_genres_add':
            state_data['post_data']['genres'] = text 
            state_data['state'] = 'awaiting_season_list'
            new_caption = generate_post_caption(state_data['post_data'])
            await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            prompt_msg = await m.reply_text(f"✅ Genres সেট হয়েছে: `{text}`\n\n**এখন Season List পরিবর্তন করুন।** (e.g. `1-2`)")
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_season_list':
            state_data['post_data']['season_list_raw'] = text if text.strip() else ""
            new_caption = generate_post_caption(state_data['post_data'])
            await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            
            all_messages = state_data.get('message_ids', [])
            post_id = state_data.get('post_message_id')
            if post_id and post_id in all_messages:
                all_messages.remove(post_id) 
            if all_messages:
                try: await c.delete_messages(m.chat.id, all_messages)
                except Exception: pass
            
            image_path = state_data['image_path']
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
            
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            await m.reply_text("✅ পোস্ট তৈরি সফলভাবে সম্পন্ন হয়েছে।")
            return

    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))
    
@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>")
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

        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        
        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি।")
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)

        if not ok:
            await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            if tmp_in.exists(): tmp_in.unlink()
            return

        await status_msg.edit("ডাউনলোড সম্পন্ন, প্রসেসিং হচ্ছে...", reply_markup=None)
        
        # --- UPDATED: Pass to smart processor ---
        await process_file_and_upload(c, m, tmp_in, original_name=safe_name, messages_to_delete=[status_msg.id])
        
    except Exception as e:
        traceback.print_exc()
        await status_msg.edit(f"অপস! কিছু ভুল হয়েছে: {e}", reply_markup=None)
    finally:
        try: TASKS[uid].remove(cancel_event)
        except: pass

async def handle_caption_only_upload(c: Client, m: Message):
    uid = m.from_user.id
    caption_to_use = USER_CAPTIONS.get(uid)
    if not caption_to_use:
        await m.reply_text("কোনো সেভ করা ক্যাপশন নেই।")
        return

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
        source_message = m
        file_info = source_message.video or source_message.document

        if not file_info:
            await status_msg.edit("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            return
        
        final_caption = process_dynamic_caption(uid, caption_to_use)
        
        if source_message.video:
            await c.send_video(
                chat_id=m.chat.id,
                video=file_info.file_id,
                caption=final_caption,
                thumb=file_info.thumbs[0].file_id if file_info.thumbs else None,
                duration=file_info.duration,
                width=file_info.width,
                height=file_info.height,
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
            success_msg = await m.reply_text("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।")
            await asyncio.sleep(5)
            await success_msg.delete()
        except Exception: pass

    except Exception as e:
        traceback.print_exc()
        await status_msg.edit(f"ত্রুটি: {e}")
    finally:
        try: TASKS[uid].remove(cancel_event)
        except: pass

@app.on_message(filters.private & (filters.video | filters.document))
async def forwarded_file_or_direct_file(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid): return

    # Audio Change Mode Check
    if uid in MKV_AUDIO_CHANGE_MODE:
        await handle_audio_change_file(c, m)
        return

    # Caption Edit Mode Check
    if uid in EDIT_CAPTION_MODE and m.forward_date:
        await handle_caption_only_upload(c, m)
        return

    # Normal Download/Upload Logic
    if m.forward_date:
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
            await status_msg.edit("ডাউনলোড সম্পন্ন, প্রসেসিং হচ্ছে...", reply_markup=None)
            
            # --- UPDATED: Pass to smart processor ---
            await process_file_and_upload(c, m, tmp_path, original_name=original_name, messages_to_delete=[status_msg.id])
        except Exception as e:
            await m.reply_text(f"ফাইল প্রসেসিংয়ে সমস্যা: {e}")
        finally:
            try: TASKS[uid].remove(cancel_event)
            except: pass

async def handle_audio_change_file(c: Client, m: Message):
    uid = m.from_user.id
    file_info = m.video or m.document
    
    if not file_info:
        await m.reply_text("এটি একটি ভিডিও ফাইল নয়।")
        return
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    tmp_path = None
    status_msg = None
    try:
        original_name = file_info.file_name or f"video_{file_info.file_unique_id}.mkv"
        if not '.' in original_name: original_name += '.mkv'
            
        tmp_path = TMP / f"audio_change_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        status_msg = await m.reply_text("অডিও ট্র্যাক বিশ্লেষণের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        await m.download(file_name=str(tmp_path))
        
        audio_tracks = await asyncio.to_thread(get_audio_tracks_ffprobe, tmp_path)
        
        if not audio_tracks:
            await status_msg.edit("এই ভিডিওতে কোনো অডিও ট্র্যাক পাওয়া যায়নি।")
            tmp_path.unlink(missing_ok=True)
            return

        # --- MODIFIED: Even with 1 track, we process it to ensure title naming ---
        if len(audio_tracks) == 1:
            await status_msg.edit("ফাইলটিতে ১টি অডিও ট্র্যাক রয়েছে। স্বয়ংক্রিয়ভাবে প্রসেস করা হচ্ছে...")
            await process_file_and_upload(c, m, tmp_path, original_name=original_name, messages_to_delete=[status_msg.id])
            return 

        track_list_text = "ফাইলের অডিও ট্র্যাকসমূহ:\n\n"
        for i, track in enumerate(audio_tracks, 1):
            track_list_text += f"{i}. **Index:** {track['stream_index']} | {track['language']} | {track['title']}\n"
            
        track_list_text += (
            "\n**অডিও অর্ডার দিতে নম্বর লিখুন (কমা দিয়ে আলাদা করে):**\n"
            "উদাহরণ: `1, 3` (১ এবং ৩ থাকবে, বাকি ডিলিট), `2` (শুধু ২ থাকবে)।\n"
            "যেই অর্ডারে নম্বর দিবেন, সেই অর্ডারে ট্র্যাক সেট হবে।"
        )
        
        await status_msg.edit(track_list_text, reply_markup=progress_keyboard()) 
        PENDING_AUDIO_ORDERS[status_msg.id] = {
            'uid': uid,
            'path': tmp_path, 
            'original_name': original_name,
            'tracks': audio_tracks
        }
        
    except Exception as e:
        logger.error(f"Audio track analysis error: {e}")
        if status_msg: await status_msg.edit(f"সমস্যা: {e}")
        if tmp_path and tmp_path.exists(): tmp_path.unlink(missing_ok=True)
    finally:
        try: TASKS[uid].remove(cancel_event)
        except: pass

# --- MODIFIED: Handle audio remux (With Title Setting) ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    # Use .mkv for intermediate remux to support everything
    out_name = f"remux_{int(time.time())}.mkv"
    out_path = TMP / f"remux_{uid}_{int(datetime.now().timestamp())}_{out_name}"
    
    map_args = ["-map", "0:v", "-map", "0:s?", "-map", "0:d?"] 
    for stream_index in new_stream_map:
        map_args.extend(["-map", stream_index])
        
    # *** APPLY AUDIO TITLE HERE TOO ***
    cmd = [
        "ffmpeg",
        "-i", str(in_path),
        "-disposition:a", "0",
        *map_args,
        "-disposition:a:0", "default",
        "-c", "copy",
        "-metadata", "handler_name=", 
        "-metadata:s:a", f"title={AUDIO_TITLE_TAG}", # Set title for ALL new audio streams
        str(out_path)
    ]

    status_msg = await m.reply_text("অডিও ট্র্যাক অর্ডার পরিবর্তন করা হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600)
        if result.returncode != 0:
            raise Exception(f"Remux ব্যর্থ হয়েছে: {result.stderr[:300]}")

        await status_msg.edit("অডিও পরিবর্তন সম্পন্ন, আপলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        
        all_messages_to_delete = messages_to_delete if messages_to_delete else []
        all_messages_to_delete.append(status_msg.id)

        # Pass to main uploader. We pass 'original_name' so it can decide final name/extension.
        # The process_file_and_upload will check the extension again. Since we made it MKV, it's fine.
        await process_file_and_upload(c, m, out_path, original_name=original_name, messages_to_delete=all_messages_to_delete) 

    except Exception as e:
        logger.error(f"Audio remux process error: {e}")
        try: await status_msg.edit(f"ব্যর্থ: {e}")
        except: pass
    finally:
        try:
            in_path.unlink(missing_ok=True)
            TASKS[uid].remove(cancel_event)
        except Exception: pass

@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message:
        await m.reply_text("ভিডিও/ডকুমেন্ট ফাইলের reply দিয়ে এই কমান্ড দিন।")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন ফাইল নাম দিন। উদাহরণ: /rename new_video.mp4")
        return
    new_name = m.text.split(None, 1)[1].strip()
    new_name = re.sub(r"[\\/*?\"<>|:]", "_", new_name)
    
    await m.reply_text(f"ভিডিও রিনেম করা হবে: {new_name}")

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    try:
        status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        tmp_out = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{new_name}"
        await m.reply_to_message.download(file_name=str(tmp_out))
        await status_msg.edit("ডাউনলোড সম্পন্ন, প্রসেসিং হচ্ছে...", reply_markup=None)
        
        # --- UPDATED: Pass to smart processor ---
        await process_file_and_upload(c, m, tmp_out, original_name=new_name, messages_to_delete=[status_msg.id])
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        try: TASKS[uid].remove(cancel_event)
        except: pass

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    prompt_message_id = cb.message.id

    if prompt_message_id in PENDING_AUDIO_ORDERS:
        file_data = PENDING_AUDIO_ORDERS.pop(prompt_message_id)
        if file_data['uid'] == uid:
            try: Path(file_data['path']).unlink(missing_ok=True)
            except Exception: pass
            
            for ev in list(TASKS.get(uid, [])):
                try: ev.set()
                except: pass

            await cb.answer("অডিও পরিবর্তন প্রক্রিয়া বাতিল করা হয়েছে।", show_alert=True)
            try: await cb.message.delete()
            except Exception: pass
            return
    
    if uid in TASKS and TASKS[uid]:
        for ev in list(TASKS[uid]):
            try: ev.set()
            except: pass
        
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try: await cb.message.delete()
        except Exception: pass
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

async def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int = 1):
    try:
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path), "-ss", str(timestamp_sec),
            "-vframes", "1", "-vf", "scale=320:-1", str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        logger.warning("Thumbnail generate error: %s", e)
        return False

def process_dynamic_caption(uid, caption_template):
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'episode_numbers': {}, 'dynamic_counters': {}, 're_options_count': 0}
    USER_COUNTERS[uid]['uploads'] += 1

    # [re (480p, 720p)] Logic
    quality_match = re.search(r"\[re\s*\((.*?)\)\]", caption_template)
    if quality_match:
        options_str = quality_match.group(1)
        options = [opt.strip() for opt in options_str.split(',')]
        if not USER_COUNTERS[uid]['re_options_count']:
            USER_COUNTERS[uid]['re_options_count'] = len(options)
        current_index = (USER_COUNTERS[uid]['uploads'] - 1) % len(options)
        caption_template = caption_template.replace(quality_match.group(0), options[current_index])
        if (USER_COUNTERS[uid]['uploads'] - 1) % USER_COUNTERS[uid]['re_options_count'] == 0 and USER_COUNTERS[uid]['uploads'] > 1:
            for key in USER_COUNTERS[uid]['dynamic_counters']:
                USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1
    elif USER_COUNTERS[uid]['uploads'] > 1:
        for key in USER_COUNTERS[uid].get('dynamic_counters', {}):
             USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1

    # [01] Logic
    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    if USER_COUNTERS[uid]['uploads'] == 1:
        for match in counter_matches:
            has_paren = match.startswith('(')
            val = int(re.sub(r'[()]', '', match))
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': val, 'has_paren': has_paren}
    
    for match, data in USER_COUNTERS[uid]['dynamic_counters'].items():
        val = data['value']
        orig_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{val:0{orig_len}d}"
        final_value = f"({formatted_value})" if data['has_paren'] else formatted_value
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)

    # [End (02)] Logic
    current_episode_num = 0
    if USER_COUNTERS[uid].get('dynamic_counters'):
        current_episode_num = min(data['value'] for data in USER_COUNTERS[uid]['dynamic_counters'].values())
    conditional_matches = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", caption_template)
    for match in conditional_matches:
        text_to_add = match[0].strip()
        target_num_str = re.sub(r'[^0-9]', '', match[1]).strip()
        placeholder = re.escape(f"[{match[0].strip()} ({match[1].strip()})]")
        try:
            target_num = int(target_num_str)
            if current_episode_num == target_num:
                caption_template = re.sub(placeholder, text_to_add, caption_template)
            else:
                caption_template = re.sub(placeholder, "", caption_template)
        except ValueError:
            caption_template = re.sub(placeholder, "", caption_template)

    return "**" + "\n".join(caption_template.splitlines()) + "**"


# ==================================================================
#            MAIN PROCESSING LOGIC (Format, Opus, Title)
# ==================================================================

async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    temp_thumb_path = None
    status_msg = None

    try:
        final_name_input = original_name or in_path.name
        
        # --- 1. DETERMINE FORMAT & EXTENSION ---
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        ext = Path(final_name_input).suffix.lower()
        is_video = bool(m.video) or ext in video_exts
        
        if is_video:
            # Logic: 
            # - MP4 + No Opus -> Keep .mp4
            # - MP4 + Opus -> Convert to .mkv
            # - Others -> Convert to .mkv
            
            target_ext = ".mkv" # Default target
            has_opus = check_has_opus_audio(in_path)
            
            if ext == ".mp4" and not has_opus:
                target_ext = ".mp4"
                
            # Generate the final branded name with the correct extension
            final_filename = generate_new_filename(final_name_input, target_ext=target_ext)
            
            # Temp path for processed file
            processed_path = TMP / f"proc_{uid}_{int(time.time())}_{final_filename}"

            try:
                status_msg = await m.reply_text("ভিডিও প্রসেসিং এবং মেটাডেটা আপডেট হচ্ছে...", reply_markup=progress_keyboard())
            except Exception:
                status_msg = await m.reply_text("ভিডিও প্রসেসিং এবং মেটাডেটা আপডেট হচ্ছে...", reply_markup=progress_keyboard())
            
            if messages_to_delete:
                if status_msg.id not in messages_to_delete:
                    messages_to_delete.append(status_msg.id)
            else:
                messages_to_delete = [status_msg.id]

            # --- 2. FFmpeg Processing (Convert/Copy + Set Audio Title) ---
            # We use 'ffmpeg' to standardize container AND set audio titles globally
            cmd = [
                "ffmpeg",
                "-i", str(in_path),
                "-map", "0",        # Map all streams
                "-c", "copy",       # Try stream copy first
                "-metadata", "handler_name=",
                "-metadata:s:a", f"title={AUDIO_TITLE_TAG}", # SET GLOBAL TITLE
                "-movflags", "+faststart",
                str(processed_path)
            ]
            
            # Run FFmpeg
            res = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600)
            
            if res.returncode != 0 or not processed_path.exists() or processed_path.stat().st_size == 0:
                # If copy failed (e.g. incompatible container), try re-encode
                logger.warning("Stream copy failed, trying re-encode...")
                processed_path.unlink(missing_ok=True)
                
                cmd_encode = [
                    "ffmpeg", "-i", str(in_path),
                    "-map", "0",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                    "-c:a", "copy", # Try copying audio
                    "-metadata:s:a", f"title={AUDIO_TITLE_TAG}",
                    str(processed_path)
                ]
                res2 = await asyncio.to_thread(subprocess.run, cmd_encode, capture_output=True, text=True, check=False, timeout=3600)
                if res2.returncode != 0:
                     try: await status_msg.edit("প্রসেসিং ব্যর্থ হয়েছে। অরিজিনাল ফাইল আপলোড করা হচ্ছে...")
                     except: pass
                     upload_path = in_path
                     final_filename = generate_new_filename(final_name_input) # Just rename
                else:
                    upload_path = processed_path
            else:
                upload_path = processed_path

        else:
            # Not video
            final_filename = final_name_input

        # Thumbnail Logic
        thumb_path = USER_THUMBS.get(uid)
        
        if is_video and not thumb_path:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            thumb_time_sec = USER_THUMB_TIME.get(uid, 1)
            ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
            if ok:
                thumb_path = str(temp_thumb_path)

        try:
            if status_msg: await status_msg.edit("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
            else: status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception: pass
        
        if messages_to_delete and status_msg.id not in messages_to_delete:
            messages_to_delete.append(status_msg.id)

        if cancel_event.is_set():
            if messages_to_delete:
                try: await c.delete_messages(m.chat.id, messages_to_delete)
                except: pass
            TASKS[uid].remove(cancel_event)
            return
        
        video_metadata = get_video_metadata(upload_path) if (is_video and upload_path.exists()) else {'duration': 0, 'width': 0, 'height': 0}
        
        caption_to_use = final_filename
        if USER_CAPTIONS.get(uid):
            caption_to_use = process_dynamic_caption(uid, USER_CAPTIONS.get(uid))

        for attempt in range(1, 4):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=thumb_path,
                        duration=video_metadata.get('duration', 0),
                        width=video_metadata.get('width', 0),
                        height=video_metadata.get('height', 0),
                        supports_streaming=True,
                        file_name=final_filename,
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=final_filename,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                if messages_to_delete:
                    try: await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                    except Exception: pass
                break
            except Exception as e:
                logger.warning(f"Upload attempt {attempt} failed: {e}")
                await asyncio.sleep(2 * attempt)
                if cancel_event.is_set(): break

    except Exception as e:
        if status_msg: await status_msg.edit(f"আপলোডে ত্রুটি: {e}")
        else: await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            if upload_path != in_path and upload_path.exists(): upload_path.unlink()
            if in_path.exists(): in_path.unlink()
            if temp_thumb_path and Path(temp_thumb_path).exists(): Path(temp_thumb_path).unlink()
            TASKS[uid].remove(cancel_event)
        except Exception: pass

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
    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} সাবস্ক্রাইবারে...", quote=True)
    failed = 0
    sent = 0
    for chat_id in list(SUBSCRIBERS):
        if chat_id == m.chat.id: continue
        try:
            await c.forward_messages(chat_id=chat_id, from_chat_id=source_message.chat.id, message_ids=source_message.id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            failed += 1
            logger.warning("Broadcast to %s failed: %s", chat_id, e)

    await m.reply_text(f"ব্রডকাস্ট শেষ। পাঠানো: {sent}, ব্যর্থ: {failed}")

# --- Flask Web Server & Ping ---
@flask_app.route('/')
def home():
    html_content = """
    <!DOCTYPE-html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot Status</title>
    </head>
    <body>
        <h1>TA File Share Bot is running! ✅</h1>
    </body>
    </html>
    """
    return render_template_string(html_content)

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
                except Exception: pass
        except Exception: pass
        await asyncio.sleep(3600)

if __name__ == "__main__":
    print("Bot চালু হচ্ছে...")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except RuntimeError: pass
    app.run()
