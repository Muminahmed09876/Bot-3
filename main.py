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
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# --- CONSTANTS ---
AUDIO_TITLE_TAG = "[@TA_HD_Anime] Telegram Channel"
BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"

# state
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
USER_COUNTERS = {}
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
PENDING_AUDIO_ORDERS = {} 

# --- STATE FOR POST CREATION ---
CREATE_POST_MODE = set()
POST_CREATION_STATE = {} 
DEFAULT_POST_DATA = {
    'image_name': "Image Name",
    'genres': "",
    'season_list_raw': "1, 2"
}

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

def generate_new_filename(original_name: str, target_ext: str = None) -> str:
    """Generates the new standardized filename."""
    file_path = Path(original_name)
    if target_ext:
        file_ext = target_ext if target_ext.startswith('.') else f".{target_ext}"
    else:
        file_ext = file_path.suffix.lower()
        if not file_ext or file_ext == '.':
            file_ext = ".mp4"
    
    return BASE_NEW_NAME + file_ext

def get_video_metadata(file_path: Path) -> dict:
    data = {'duration': 0, 'width': 0, 'height': 0}
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", str(file_path)
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
            raise Exception("FFprobe returned 0 dimensions")

    except Exception as e:
        logger.warning(f"FFprobe metadata extraction failed: {e}. Trying Hachoir fallback...")
        try:
            parser = createParser(str(file_path))
            if parser:
                with parser:
                    h_metadata = extractMetadata(parser)
                if h_metadata:
                    if h_metadata.has("duration") and data['duration'] == 0:
                        data['duration'] = int(h_metadata.get("duration").total_seconds())
                    if h_metadata.has("width") and data['width'] == 0:
                        data['width'] = int(h_metadata.get("width"))
                    if h_metadata.has("height") and data['height'] == 0:
                        data['height'] = int(h_metadata.get("height"))
        except Exception as he:
            logger.error(f"Hachoir fallback ALSO failed: {he}")
    
    return data

def check_has_opus_audio(file_path: Path) -> bool:
    """Checks if the file has any OPUS audio stream."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "a", str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        for stream in metadata.get('streams', []):
            if stream.get('codec_name') == 'opus':
                return True
    except Exception as e:
        logger.error(f"Opus check failed: {e}")
    return False

def parse_time(time_str: str) -> int:
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
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", str(file_path)
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

    base_caption = (
        f"**{image_name}**\n"
        f"**────────────────────**\n"
        f"**‣ Audio - Hindi Official**\n"
        f"**‣ Quality - 480p, 720p, 1080p**\n"
        f"**‣ Genres - {genres}**\n"
        f"**────────────────────**"
    )
    
    collapsible_text_parts = [f"> **{image_name} All Season List :-**", "> "]
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}")
        collapsible_text_parts.append("> ")
        
    if collapsible_text_parts and collapsible_text_parts[-1] == "> ":
        collapsible_text_parts.pop()
        
    final_caption = f"{base_caption}\n\n" + "\n".join(collapsible_text_parts)
    return final_caption

# ---- Network & Download Utilities ----
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
                    return False, "Cancelled by user."
                if not chunk: break
                if total > MAX_SIZE:
                    return False, "File size > 4GB."
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
                if resp.status != 200: return False, f"HTTP {resp.status}"
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
                        if resp2.status != 200: return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                return False, "Drive download failed (No perm/public)."
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

# ---- handlers ----
@app.on_message(filters.command(["start", "help"]) & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    SUBSCRIBERS.add(m.chat.id)
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ডাউনলোড ও আপলোড\n"
        "/setthumb - থাম্বনেইল সেট\n"
        "/view_thumb - থাম্বনেইল দেখুন\n"
        "/del_thumb - থাম্বনেইল মুছুন\n"
        "/set_caption - ক্যাপশন সেট\n"
        "/view_caption - ক্যাপশন দেখুন\n"
        "/edit_caption_mode - শুধু ক্যাপশন মোড\n"
        "/rename <newname> - রিনেম\n"
        "/mkv_video_audio_change - অডিও ট্র্যাক পরিবর্তন\n"
        "/create_post - পোস্ট তৈরি\n"
        "/mode_check - মোড চেক\n"
        "/broadcast - ব্রডকাস্ট\n"
    )
    await m.reply_text(text)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    if len(m.command) > 1:
        seconds = parse_time(" ".join(m.command[1:]))
        if seconds > 0:
            USER_THUMB_TIME[uid] = seconds
            await m.reply_text(f"থাম্বনেইল টাইম সেট: {seconds}s")
        else:
            await m.reply_text("সঠিক ফরম্যাট দিন (e.g. 5s, 1m)")
    else:
        SET_THUMB_REQUEST.add(uid)
        await m.reply_text("ছবি পাঠান থাম্বনেইল হিসেবে সেট করতে।")

@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    path = USER_THUMBS.get(uid)
    if path and Path(path).exists():
        await c.send_photo(m.chat.id, path, caption="আপনার থাম্বনেইল।")
    elif uid in USER_THUMB_TIME:
        await m.reply_text(f"থাম্বনেইল টাইম: {USER_THUMB_TIME[uid]}s")
    else:
        await m.reply_text("কোনো থাম্বনেইল নেই।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    path = USER_THUMBS.get(uid)
    if path: Path(path).unlink(missing_ok=True)
    USER_THUMBS.pop(uid, None)
    USER_THUMB_TIME.pop(uid, None)
    await m.reply_text("থাম্বনেইল মুছে ফেলা হয়েছে।")

@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE and POST_CREATION_STATE[uid]['state'] == 'awaiting_image':
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id)
        out = TMP / f"post_img_{uid}.jpg"
        try:
            dl_msg = await m.reply_text("ছবি ডাউনলোড হচ্ছে...")
            state_data['message_ids'].append(dl_msg.id)
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((1080, 1080))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            state_data['image_path'] = str(out)
            state_data['state'] = 'awaiting_name_change'
            initial_caption = generate_post_caption(state_data['post_data'])
            post_msg = await c.send_photo(m.chat.id, str(out), caption=initial_caption, parse_mode=ParseMode.MARKDOWN)
            state_data['post_message_id'] = post_msg.id
            state_data['message_ids'].append(post_msg.id)
            pm = await m.reply_text(f"✅ ছবি সেট হয়েছে। **নাম পরিবর্তন করুন:**\nবর্তমান: `{state_data['post_data']['image_name']}`")
            state_data['message_ids'].append(pm.id)
        except Exception as e:
            await m.reply_text(f"Error: {e}")
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
        return

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
            await m.reply_text("থাম্বনেইল সেট হয়েছে।")
        except Exception as e:
            await m.reply_text(f"Error: {e}")

@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m):
    if not is_admin(m.from_user.id): return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    USER_COUNTERS.pop(m.from_user.id, None)
    await m.reply_text("ক্যাপশন দিন। কোড: `[01]`, `[re (480p, 720p)]`, `[End (02)]`")

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m):
    if not is_admin(m.from_user.id): return
    cap = USER_CAPTIONS.get(m.from_user.id)
    if cap: await m.reply_text(f"`{cap}`", reply_markup=delete_caption_keyboard())
    else: await m.reply_text("ক্যাপশন নেই।")

@app.on_callback_query(filters.regex("delete_caption"))
async def delete_caption_cb(c, cb):
    if not is_admin(cb.from_user.id): return
    USER_CAPTIONS.pop(cb.from_user.id, None)
    USER_COUNTERS.pop(cb.from_user.id, None)
    await cb.message.edit_text("ক্যাপশন ডিলিট হয়েছে।")

@app.on_message(filters.command("edit_caption_mode") & filters.private)
async def toggle_edit_caption_mode(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    if uid in EDIT_CAPTION_MODE:
        EDIT_CAPTION_MODE.discard(uid)
        await m.reply_text("Caption Edit Mode: OFF")
    else:
        EDIT_CAPTION_MODE.add(uid)
        await m.reply_text("Caption Edit Mode: ON")

@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    if uid in MKV_AUDIO_CHANGE_MODE:
        MKV_AUDIO_CHANGE_MODE.discard(uid)
        await m.reply_text("MKV Audio Change Mode: OFF")
    else:
        MKV_AUDIO_CHANGE_MODE.add(uid)
        await m.reply_text("MKV Audio Change Mode: ON")

@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
        if uid in POST_CREATION_STATE:
            sd = POST_CREATION_STATE.pop(uid)
            if sd.get('image_path'): Path(sd['image_path']).unlink(missing_ok=True)
            try:
                msgs = [mid for mid in sd.get('message_ids', []) if mid != sd.get('post_message_id')]
                if msgs: await c.delete_messages(m.chat.id, msgs)
            except: pass
        await m.reply_text("Create Post Mode: OFF")
    else:
        CREATE_POST_MODE.add(uid)
        POST_CREATION_STATE[uid] = {
            'image_path': None, 'message_ids': [m.id], 
            'state': 'awaiting_image', 'post_data': DEFAULT_POST_DATA.copy(), 'post_message_id': None
        }
        await m.reply_text("Create Post Mode: ON. ছবি পাঠান।")

@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m):
    if not is_admin(m.from_user.id): return
    await m.reply_text("মোড স্ট্যাটাস:", reply_markup=mode_check_keyboard(m.from_user.id))

@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
async def mode_toggle_callback(c, cb):
    if not is_admin(cb.from_user.id): return
    uid = cb.from_user.id
    action = cb.data
    msg = ""
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE:
            MKV_AUDIO_CHANGE_MODE.discard(uid)
            msg = "Audio Mode OFF"
        else:
            MKV_AUDIO_CHANGE_MODE.add(uid)
            msg = "Audio Mode ON"
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE:
            EDIT_CAPTION_MODE.discard(uid)
            msg = "Caption Mode OFF"
        else:
            EDIT_CAPTION_MODE.add(uid)
            msg = "Caption Mode ON"
    
    await cb.message.edit_reply_markup(mode_check_keyboard(uid))
    await cb.answer(msg, show_alert=True)

@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid): return
    text = m.text.strip()
    
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None)
        await m.reply_text("ক্যাপশন সেভ হয়েছে।")
        return

    # --- UPDATED: AUDIO ORDER HANDLER ---
    if m.reply_to_message and m.reply_to_message.id in PENDING_AUDIO_ORDERS:
        prompt_id = m.reply_to_message.id
        data = PENDING_AUDIO_ORDERS.get(prompt_id)
        if data['uid'] != uid: return

        tracks = data['tracks']
        try:
            # Parse input "1,3" or "2"
            parts = [x.strip() for x in text.split(',') if x.strip()]
            new_stream_map = []
            
            if not parts:
                await m.reply_text("অন্তত একটি ট্র্যাক নম্বর দিন।")
                return

            valid_indices = range(1, len(tracks) + 1)
            
            for p in parts:
                idx = int(p)
                if idx not in valid_indices:
                    await m.reply_text(f"ভুল নম্বর: {idx}. ভ্যালিড: 1-{len(tracks)}")
                    return
                # Map to source stream index
                src_idx = tracks[idx-1]['stream_index']
                new_stream_map.append(f"0:{src_idx}")

            # Start remux with filtered tracks
            asyncio.create_task(
                handle_audio_remux(
                    c, m, data['path'], data['original_name'], 
                    new_stream_map, messages_to_delete=[prompt_id, m.id]
                )
            )
            PENDING_AUDIO_ORDERS.pop(prompt_id, None)
            return
        except ValueError:
            await m.reply_text("ভুল ফরম্যাট। উদাহরণ: `1, 3`")
            return
        except Exception as e:
            await m.reply_text(f"Error: {e}")
            Path(data['path']).unlink(missing_ok=True)
            PENDING_AUDIO_ORDERS.pop(prompt_id, None)
            return

    # --- POST CREATION EDITING ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        sd = POST_CREATION_STATE[uid]
        sd['message_ids'].append(m.id)
        curr = sd['state']
        
        if curr == 'awaiting_name_change':
            if not text: return
            sd['post_data']['image_name'] = text
            sd['state'] = 'awaiting_genres_add'
            await c.edit_message_caption(m.chat.id, sd['post_message_id'], caption=generate_post_caption(sd['post_data']), parse_mode=ParseMode.MARKDOWN)
            pm = await m.reply_text(f"✅ নাম সেট। **Genres দিন:** (e.g. Comedy, Action)")
            sd['message_ids'].append(pm.id)
            
        elif curr == 'awaiting_genres_add':
            sd['post_data']['genres'] = text
            sd['state'] = 'awaiting_season_list'
            await c.edit_message_caption(m.chat.id, sd['post_message_id'], caption=generate_post_caption(sd['post_data']), parse_mode=ParseMode.MARKDOWN)
            pm = await m.reply_text(f"✅ Genres সেট। **Season List দিন:** (e.g. `1-2`, `1, 2`)")
            sd['message_ids'].append(pm.id)
            
        elif curr == 'awaiting_season_list':
            sd['post_data']['season_list_raw'] = text
            await c.edit_message_caption(m.chat.id, sd['post_message_id'], caption=generate_post_caption(sd['post_data']), parse_mode=ParseMode.MARKDOWN)
            
            msgs = sd.get('message_ids', [])
            if sd['post_message_id'] in msgs: msgs.remove(sd['post_message_id'])
            if msgs: await c.delete_messages(m.chat.id, msgs)
            if sd['image_path']: Path(sd['image_path']).unlink(missing_ok=True)
            
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            await m.reply_text("✅ পোস্ট তৈরি সম্পন্ন।")
        return

    if text.startswith(("http://", "https://")):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m):
    if not is_admin(m.from_user.id): return
    if len(m.command) < 2:
        await m.reply_text("/upload_url <url>")
        return
    url = m.text.split(None, 1)[1].strip()
    asyncio.create_task(handle_url_download_and_upload(c, m, url))

async def handle_url_download_and_upload(c, m, url):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    msg = await m.reply_text("ডাউনলোড শুরু...", reply_markup=progress_keyboard())
    
    try:
        fname = url.split("/")[-1].split("?")[0] or f"dl_{int(time.time())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)
        if not any(safe_name.lower().endswith(ext) for ext in {".mp4", ".mkv", ".avi", ".webm"}):
            safe_name += ".mp4"

        tmp_in = TMP / f"dl_{uid}_{int(time.time())}_{safe_name}"
        
        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid: 
                await msg.edit("Drive ID not found.")
                return
            ok, err = await download_drive_file(fid, tmp_in, msg, cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, msg, cancel_event)

        if not ok:
            await msg.edit(f"Download Fail: {err}")
            if tmp_in.exists(): tmp_in.unlink()
            return

        await msg.edit("Processing & Uploading...")
        await process_file_and_upload(c, m, tmp_in, original_name=safe_name, messages_to_delete=[msg.id])
        
    except Exception as e:
        logger.error(traceback.format_exc())
        await msg.edit(f"Error: {e}")
    finally:
        TASKS[uid].remove(cancel_event)

async def handle_caption_only_upload(c, m):
    uid = m.from_user.id
    cap_tmpl = USER_CAPTIONS.get(uid)
    if not cap_tmpl:
        await m.reply_text("সেভ করা ক্যাপশন নেই।")
        return
    
    msg = await m.reply_text("ক্যাপশন আপডেট হচ্ছে...")
    try:
        file_info = m.video or m.document
        if not file_info: return
        
        final_cap = process_dynamic_caption(uid, cap_tmpl)
        
        if m.video:
            await c.send_video(m.chat.id, file_info.file_id, caption=final_cap,
                               duration=file_info.duration, width=file_info.width, height=file_info.height,
                               supports_streaming=True, parse_mode=ParseMode.MARKDOWN)
        else:
            await c.send_document(m.chat.id, file_info.file_id, caption=final_cap, parse_mode=ParseMode.MARKDOWN)
        
        await msg.delete()
        await m.reply_text("✅ সম্পন্ন।")
    except Exception as e:
        await msg.edit(f"Error: {e}")

@app.on_message(filters.private & (filters.video | filters.document))
async def forwarded_file_or_direct_file(c, m):
    uid = m.from_user.id
    if not is_admin(uid): return

    if uid in MKV_AUDIO_CHANGE_MODE:
        await handle_audio_change_file(c, m)
        return

    if uid in EDIT_CAPTION_MODE and m.forward_date:
        await handle_caption_only_upload(c, m)
        return

    if m.forward_date:
        cancel_event = asyncio.Event()
        TASKS.setdefault(uid, []).append(cancel_event)
        file_info = m.video or m.document
        fname = file_info.file_name or "video.mp4"
        
        msg = await m.reply_text("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        tmp_path = TMP / f"fwd_{uid}_{int(time.time())}_{fname}"
        
        try:
            await m.download(file_name=str(tmp_path))
            await msg.edit("Processing...")
            await process_file_and_upload(c, m, tmp_path, original_name=fname, messages_to_delete=[msg.id])
        except Exception as e:
            await m.reply_text(f"Error: {e}")
        finally:
            TASKS[uid].remove(cancel_event)

async def handle_audio_change_file(c, m):
    uid = m.from_user.id
    file_info = m.video or m.document
    if not file_info: return
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    fname = file_info.file_name or "video.mkv"
    if '.' not in fname: fname += ".mkv"
    tmp_path = TMP / f"audio_chg_{uid}_{int(time.time())}_{fname}"
    msg = await m.reply_text("Downloading & Analyzing...", reply_markup=progress_keyboard())
    
    try:
        await m.download(file_name=str(tmp_path))
        tracks = await asyncio.to_thread(get_audio_tracks_ffprobe, tmp_path)
        
        if not tracks:
            await msg.edit("No audio tracks found.")
            tmp_path.unlink()
            return

        if len(tracks) == 1:
            # Auto process single track (convert/standardize)
            await msg.edit("১টি অডিও ট্র্যাক। প্রসেসিং...")
            # Just pass to normal upload, it handles title renaming
            await process_file_and_upload(c, m, tmp_path, original_name=fname, messages_to_delete=[msg.id])
            return

        txt = "অডিও ট্র্যাক তালিকা:\n\n"
        for i, t in enumerate(tracks, 1):
            txt += f"{i}. **Index:** {t['stream_index']} | {t['language']} | {t['title']}\n"
        txt += "\n**Reply with numbers to KEEP & REORDER.**\nExamples: `2` (Keep only 2nd), `3,1` (Keep 3rd then 1st)."
        
        await msg.edit(txt, reply_markup=progress_keyboard())
        PENDING_AUDIO_ORDERS[msg.id] = {'uid': uid, 'path': tmp_path, 'original_name': fname, 'tracks': tracks}
        
    except Exception as e:
        await msg.edit(f"Error: {e}")
        if tmp_path.exists(): tmp_path.unlink()
    finally:
        TASKS[uid].remove(cancel_event)

async def handle_audio_remux(c, m, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    # Remux to MKV implies standardization
    out_name = generate_new_filename(original_name, target_ext=".mkv")
    out_path = TMP / f"remux_{uid}_{int(time.time())}_{out_name}"
    
    map_args = ["-map", "0:v"]
    # Add subtitle/data if exist? We map them if we want. Let's map subs if any
    map_args.extend(["-map", "0:s?"])
    
    # Add user selected audio
    for s_map in new_stream_map:
        map_args.extend(["-map", s_map])

    # *** APPLY AUDIO TITLE HERE GLOBALLY ***
    cmd = [
        "ffmpeg", "-i", str(in_path),
        "-disposition:a", "0", *map_args, "-disposition:a:0", "default",
        "-c", "copy",
        "-metadata", "handler_name=",
        "-metadata:s:a", f"title={AUDIO_TITLE_TAG}", # Set title for ALL audio streams
        str(out_path)
    ]

    msg = await m.reply_text("Remuxing & Renaming Audio...", reply_markup=progress_keyboard())
    try:
        res = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600)
        if res.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
            raise Exception(f"FFmpeg Fail: {res.stderr[:300]}")

        await msg.edit("Uploading...")
        if messages_to_delete: messages_to_delete.append(msg.id)
        else: messages_to_delete = [msg.id]
        
        # Pass the ALREADY processed file to upload, skipping further processing logic if possible, 
        # but process_file_and_upload checks extension again. 
        # Since it's now MKV and has titles, process_file_and_upload should just pass it through or do a quick copy.
        # To be safe and avoid double processing, we can modify process_file_and_upload 
        # OR just ensure process_file_and_upload handles MKV->MKV efficiently.
        
        await process_file_and_upload(c, m, out_path, original_name=out_name, messages_to_delete=messages_to_delete, skip_processing=True)

    except Exception as e:
        await msg.edit(f"Remux Error: {e}")
        if out_path.exists(): out_path.unlink()
    finally:
        if in_path.exists(): in_path.unlink()
        TASKS[uid].remove(cancel_event)

@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m):
    if not is_admin(m.from_user.id): return
    if not m.reply_to_message: return
    if len(m.command) < 2:
        await m.reply_text("/rename name.mp4")
        return
    
    new_name = m.text.split(None, 1)[1].strip()
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    msg = await m.reply_text("Downloading for rename...", reply_markup=progress_keyboard())
    tmp_path = TMP / f"rn_{uid}_{int(time.time())}_{re.sub(r'[^a-zA-Z0-9.]', '_', new_name)}"
    
    try:
        await m.reply_to_message.download(file_name=str(tmp_path))
        await msg.edit("Processing...")
        await process_file_and_upload(c, m, tmp_path, original_name=new_name, messages_to_delete=[msg.id])
    except Exception as e:
        await msg.edit(f"Error: {e}")
    finally:
        TASKS[uid].remove(cancel_event)

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    pid = cb.message.id
    if pid in PENDING_AUDIO_ORDERS:
        d = PENDING_AUDIO_ORDERS.pop(pid)
        if d['uid'] == uid: Path(d['path']).unlink(missing_ok=True)
        await cb.answer("Cancelled Audio Order.", show_alert=True)
        await cb.message.delete()
        return

    if uid in TASKS and TASKS[uid]:
        for ev in TASKS[uid]: ev.set()
        await cb.answer("Task Cancelled.", show_alert=True)
    else:
        await cb.answer("Nothing to cancel.", show_alert=True)

async def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int = 1):
    try:
        cmd = ["ffmpeg", "-y", "-i", str(video_path), "-ss", str(timestamp_sec), "-vframes", "1", "-vf", "scale=320:-1", str(thumb_path)]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except: return False

# --- CORE PROCESSING LOGIC ---
async def standardize_video_and_metadata(in_path: Path, original_name: str, status_msg: Message):
    """
    Handles format conversion (MP4+Opus->MKV, Others->MKV) and sets Audio Metadata.
    Returns (bool_success, path_to_upload, final_filename)
    """
    ext = in_path.suffix.lower()
    input_is_mp4 = ext == ".mp4"
    
    # Check codecs
    has_opus = check_has_opus_audio(in_path)
    
    # Determine Output Format
    # Rule: MP4 + Opus -> MKV. All non-MP4 -> MKV. MP4 (no opus) -> MP4.
    if input_is_mp4 and not has_opus:
        target_ext = ".mp4"
    else:
        target_ext = ".mkv"
        
    final_name = generate_new_filename(original_name, target_ext=target_ext)
    out_path = TMP / f"std_{int(time.time())}_{final_name}"

    # Build FFmpeg Command
    # We always copy video stream if possible. We copy audio but change metadata.
    cmd = [
        "ffmpeg", "-i", str(in_path),
        "-map", "0", # Map everything
        "-c", "copy", # Stream copy
        "-metadata", "handler_name=", # Clean handlers
        "-metadata:s:a", f"title={AUDIO_TITLE_TAG}", # SET AUDIO TITLE FOR ALL TRACKS
        "-movflags", "+faststart", # Good for MP4
        str(out_path)
    ]
    
    # If converting container (e.g. avi->mkv, or mp4+opus->mkv), copy usually works fine.
    # However, if input is something weird like wmv/flv, copy might fail for video.
    # We try copy first.
    
    try:
        await status_msg.edit(f"Processing format ({target_ext}) & Metadata...", reply_markup=progress_keyboard())
        
        res = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=1800)
        
        if res.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
            # Copy failed, try re-encoding (fallback)
            logger.warning(f"Stream copy failed ({res.stderr[:200]}), trying re-encode...")
            out_path.unlink(missing_ok=True)
            
            cmd_encode = [
                "ffmpeg", "-i", str(in_path),
                "-map", "0",
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-c:a", "copy", # Try copying audio still
                "-metadata:s:a", f"title={AUDIO_TITLE_TAG}",
                str(out_path)
            ]
            res2 = await asyncio.to_thread(subprocess.run, cmd_encode, capture_output=True, text=True, check=False, timeout=3600)
            if res2.returncode != 0:
                raise Exception(f"Re-encode failed: {res2.stderr[:300]}")

        return True, out_path, final_name
        
    except Exception as e:
        logger.error(f"Standardize error: {e}")
        return False, in_path, original_name # Fail safe return original

def process_dynamic_caption(uid, tmpl):
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'episode_numbers': {}, 'dynamic_counters': {}, 're_options_count': 0}
    USER_COUNTERS[uid]['uploads'] += 1
    
    # Logic for [re (...)]
    qm = re.search(r"\[re\s*\((.*?)\)\]", tmpl)
    if qm:
        opts = [x.strip() for x in qm.group(1).split(',')]
        if not USER_COUNTERS[uid]['re_options_count']: USER_COUNTERS[uid]['re_options_count'] = len(opts)
        idx = (USER_COUNTERS[uid]['uploads'] - 1) % len(opts)
        tmpl = tmpl.replace(qm.group(0), opts[idx])
        if (USER_COUNTERS[uid]['uploads'] - 1) % len(opts) == 0 and USER_COUNTERS[uid]['uploads'] > 1:
            for k in USER_COUNTERS[uid]['dynamic_counters']:
                 USER_COUNTERS[uid]['dynamic_counters'][k]['value'] += 1
    elif USER_COUNTERS[uid]['uploads'] > 1:
        for k in USER_COUNTERS[uid].get('dynamic_counters', {}):
             USER_COUNTERS[uid]['dynamic_counters'][k]['value'] += 1

    # Logic for [01]
    matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", tmpl)
    if USER_COUNTERS[uid]['uploads'] == 1:
        for m in matches:
            has_p = m.startswith('(')
            val = int(re.sub(r'[()]', '', m))
            USER_COUNTERS[uid]['dynamic_counters'][m] = {'value': val, 'has_paren': has_p}
            
    for m, d in USER_COUNTERS[uid]['dynamic_counters'].items():
        val = d['value']
        orig_len = len(re.sub(r'[()]', '', m))
        fval = f"{val:0{orig_len}d}"
        if d['has_paren']: fval = f"({fval})"
        tmpl = re.sub(re.escape(f"[{m}]"), fval, tmpl)

    # Logic for [Text (02)]
    cur_ep = 0
    if USER_COUNTERS[uid]['dynamic_counters']:
        cur_ep = min(d['value'] for d in USER_COUNTERS[uid]['dynamic_counters'].values())
    
    conds = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", tmpl)
    for txt, num_str in conds:
        ph = re.escape(f"[{txt} ({num_str})]")
        try:
            if cur_ep == int(re.sub(r'[^0-9]', '', num_str)):
                tmpl = re.sub(ph, txt.strip(), tmpl)
            else:
                tmpl = re.sub(ph, "", tmpl)
        except: pass
        
    return "**" + "\n".join(tmpl.splitlines()) + "**"

async def process_file_and_upload(c, m, in_path: Path, original_name: str, messages_to_delete: list = None, skip_processing=False):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    final_name = original_name
    msg = None
    
    try:
        # Determine if it's video
        is_video = bool(m.video) or in_path.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        
        if is_video and not skip_processing:
            try:
                msg = await m.reply_text("Analyzing & Standardizing...", reply_markup=progress_keyboard())
                if messages_to_delete: messages_to_delete.append(msg.id)
                else: messages_to_delete = [msg.id]
            except: pass
            
            ok, processed_path, processed_name = await standardize_video_and_metadata(in_path, original_name, msg)
            if ok:
                upload_path = processed_path
                final_name = processed_name
            else:
                await msg.edit("Processing failed, uploading original...")

        # Thumbnail
        thumb_path = USER_THUMBS.get(uid)
        temp_thumb = None
        if is_video and not thumb_path:
            temp_thumb = TMP / f"th_{uid}_{int(time.time())}.jpg"
            if await generate_video_thumbnail(upload_path, temp_thumb, USER_THUMB_TIME.get(uid, 1)):
                thumb_path = str(temp_thumb)

        if msg: await msg.edit("Uploading...", reply_markup=progress_keyboard())
        
        # Metadata for upload
        meta = get_video_metadata(upload_path) if is_video else {}
        
        cap = final_name
        if USER_CAPTIONS.get(uid):
            cap = process_dynamic_caption(uid, USER_CAPTIONS.get(uid))
            
        for attempt in range(1, 4):
            try:
                if is_video:
                    await c.send_video(
                        m.chat.id, str(upload_path), caption=cap, thumb=thumb_path,
                        duration=meta.get('duration', 0), width=meta.get('width', 0), height=meta.get('height', 0),
                        file_name=final_name, supports_streaming=True, parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await c.send_document(m.chat.id, str(upload_path), file_name=final_name, caption=cap, parse_mode=ParseMode.MARKDOWN)
                
                if messages_to_delete:
                    try: await c.delete_messages(m.chat.id, messages_to_delete)
                    except: pass
                break
            except Exception as e:
                logger.warning(f"Up fail {attempt}: {e}")
                await asyncio.sleep(2 * attempt)
                if cancel_event.is_set(): break

    except Exception as e:
        logger.error(f"Main Process Error: {e}")
        await m.reply_text(f"Error: {e}")
    finally:
        if upload_path != in_path and upload_path.exists(): upload_path.unlink()
        if in_path.exists(): in_path.unlink()
        if temp_thumb and temp_thumb.exists(): temp_thumb.unlink()
        TASKS[uid].remove(cancel_event)

# --- Broadcast ---
@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_cmd_reply(c, m):
    if not is_admin(m.from_user.id): return
    await m.reply_text(f"Broadcasting to {len(SUBSCRIBERS)} users...")
    cnt = 0
    for cid in SUBSCRIBERS:
        try:
            await m.reply_to_message.copy(cid)
            cnt += 1
            await asyncio.sleep(0.1)
        except: pass
    await m.reply_text(f"Sent to {cnt} users.")

# --- Flask & Main ---
@flask_app.route('/')
def home(): return "Bot Running"

def run_flask(): flask_app.run(host="0.0.0.0", port=PORT)

async def periodic_cleanup():
    while True:
        now = datetime.now()
        for p in TMP.iterdir():
            if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(hours=1):
                try: p.unlink()
                except: pass
        await asyncio.sleep(3600)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except: pass
    app.run()
