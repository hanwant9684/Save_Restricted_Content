# Copyright (C) @Wolfy004
# Telethon-compatible version

import os
import gc
import asyncio
from time import time
from logger import LOGGER
from typing import Optional
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for

def get_intra_request_delay(is_premium):
    """
    Get the appropriate delay between items in media groups or batch downloads.
    
    Args:
        is_premium: Boolean indicating if user is premium/admin (True) or free (False)
        
    Returns:
        int: Delay in seconds (1s for premium, 3s for free users)
    """
    from config import PyroConf
    return PyroConf.PREMIUM_INTRA_DELAY if is_premium else PyroConf.FREE_INTRA_DELAY

from telethon.tl.types import (
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
)

from helpers.files import (
    fileSizeLimit,
    cleanup_download,
    cleanup_download_delayed,
    get_download_path
)

from helpers.msg import (
    get_parsed_msg,
    get_file_name
)

from helpers.transfer import download_media_fast

# Ultra-minimal progress template (near-zero RAM)
# No string formatting needed - computed inline

async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    try:
        stdout = stdout.decode().strip()
    except:
        stdout = "Unable to decode the response!"
    try:
        stderr = stderr.decode().strip()
    except:
        stderr = "Unable to decode the error!"
    return stdout, stderr, proc.returncode


async def has_video_stream(video_path):
    """
    Check if a file has a video stream using ffprobe.
    Properly handles process cleanup to prevent resource leaks.
    
    Returns:
        tuple: (has_video: bool, duration: float or None, error_msg: str or None)
    """
    proc = None
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=codec_type,duration",
            "-of", "csv=p=0", video_path
        ]
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        
        try:
            stdout, stderr = await wait_for(proc.communicate(), timeout=10.0)
        except asyncio.TimeoutError:
            LOGGER(__name__).debug(f"ffprobe timeout checking {os.path.basename(video_path)}")
            if proc:
                try:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except:
                    pass
            return False, None, "ffprobe timed out"
        
        stdout_str = stdout.decode().strip() if stdout else ""
        stderr_str = stderr.decode().strip() if stderr else ""
        
        if proc.returncode != 0 or not stdout_str:
            if stderr_str:
                LOGGER(__name__).debug(f"ffprobe error for {os.path.basename(video_path)}: {stderr_str[:100]}")
            return False, None, stderr_str or "No video stream found"
        
        parts = stdout_str.split(',')
        if parts and 'video' in str(parts[0]).lower():
            duration = None
            if len(parts) > 1 and parts[1] and parts[1] != 'N/A':
                try:
                    duration = float(parts[1])
                except ValueError:
                    pass
            return True, duration, None
        return False, None, "No video stream detected"
    except Exception as e:
        LOGGER(__name__).debug(f"has_video_stream exception: {e}")
        if proc and proc.returncode is None:
            try:
                proc.kill()
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except:
                pass
        return False, None, str(e)


async def create_placeholder_thumbnail(thumb_path, width=320, height=240):
    """
    Create a simple placeholder image as last resort fallback.
    Uses solid color with video icon text - very fast and reliable.
    """
    proc = None
    try:
        cmd = [
            "ffmpeg", "-y", "-f", "lavfi", "-i",
            f"color=c=0x1a1a2e:s={width}x{height}:d=1",
            "-frames:v", "1", thumb_path
        ]
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        except asyncio.TimeoutError:
            if proc:
                try:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except:
                    pass
            return None
        
        if proc.returncode == 0 and os.path.exists(thumb_path):
            if os.path.getsize(thumb_path) > 0:
                LOGGER(__name__).debug(f"Placeholder thumbnail created")
                return thumb_path
        return None
    except Exception as e:
        LOGGER(__name__).debug(f"Placeholder creation failed: {e}")
        return None
    finally:
        if proc and proc.returncode is None:
            try:
                proc.kill()
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except:
                pass


async def generate_thumbnail(video_path, thumb_path=None, duration=None):
    """
    Generate thumbnail from video using FFmpeg with high precision and dynamic timeouts.
    Uses -q:v 2 for best quality and optimized scaling.
    """
    if thumb_path is None:
        thumb_path = video_path + "_thumb.jpg"
    
    # Dynamic timeout based on file size (from old utils logic)
    # This prevents timeouts on large files in resource-constrained environments
    file_size = 0
    try:
        file_size = os.path.getsize(video_path)
    except: pass
    
    timeout = 15.0
    if file_size > 100 * 1024 * 1024: # > 100MB
        timeout = 25.0
    if file_size > 500 * 1024 * 1024: # > 500MB
        timeout = 40.0

    # Extract frame at 5 seconds (or middle if shorter)
    seek_time = 5
    if duration and duration < 5:
        seek_time = duration / 2

    try:
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(seek_time),
            "-i", video_path,
            "-vframes", "1",
            "-q:v", "2",
            "-vf", "scale=320:-1",
            thumb_path
        ]
        
        # Use subprocess with timeout to prevent hanging
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            await wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            if proc:
                try: proc.kill()
                except: pass
            LOGGER(__name__).warning(f"Thumbnail generation timed out after {timeout}s for {os.path.basename(video_path)}")
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
            
    except Exception as e:
        LOGGER(__name__).error(f"Thumbnail generation failed: {e}")
    
    return await create_placeholder_thumbnail(thumb_path)


async def get_media_info(path):
    """
    Extract media info with multi-stage fallback for 100% accuracy.
    Matches logic from devgaganin repo for stability.
    """
    duration = 0
    artist = None
    title = None
    width = 480
    height = 320
    
    try:
        # Stage 1: Fast JSON probe
        cmd = [
            "ffprobe", "-v", "quiet", "-hide_banner",
            "-show_entries", "format=duration,tags:stream=duration,width,height,codec_type",
            "-print_format", "json", path
        ]
        stdout, stderr, code = await cmd_exec(cmd)
        
        if code == 0 and stdout:
            import ujson as json
            data = json.loads(stdout)
            
            # Format info
            f_data = data.get("format", {})
            duration = float(f_data.get("duration") or 0)
            tags = f_data.get("tags", {})
            artist = tags.get("artist") or tags.get("ARTIST")
            title = tags.get("title") or tags.get("TITLE")
            
            # Stream info
            for s in data.get("streams", []):
                if duration <= 0:
                    duration = float(s.get("duration") or 0)
                if s.get("codec_type") == "video":
                    width = int(s.get("width") or width)
                    height = int(s.get("height") or height)

        # Stage 2: Aggressive duration detection (Stderr match)
        if duration <= 0:
            _, stderr, _ = await cmd_exec(["ffmpeg", "-i", path])
            import re
            match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+|\d+)", stderr)
            if match:
                h, m, s = match.groups()
                duration = int(h) * 3600 + int(m) * 60 + float(s)

        # Stage 3: Packet-level PTS detection (Deep scan)
        if duration <= 0:
            cmd_pts = [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "packet=pts_time", "-of", "compact=p=0:nk=1", path
            ]
            pts_out, _, _ = await cmd_exec(cmd_pts)
            if pts_out:
                lines = pts_out.strip().split('\n')
                if lines:
                    try: duration = float(lines[-1])
                    except: pass

    except Exception as e:
        LOGGER(__name__).error(f"Metadata extraction failed: {e}")
    
    return round(duration), artist, title, width, height


# Progress Throttle Helper to prevent Telegram API rate limits
class ProgressThrottle:
    """
    Centralized progress throttling to prevent Telegram API rate limits.
    Enforces minimum time between updates and handles rate limit errors gracefully.
    Also tracks transfer progress for accurate speed calculations.
    """
    def __init__(self):
        self.message_throttles = {}  # message_id -> throttle data
        self._last_sweep = time()
        self._sweep_interval = 300  # Sweep every 5 minutes
        self._max_age = 3600  # Remove entries older than 1 hour
    
    def _sweep_stale_entries(self, now):
        """Remove stale throttle entries to prevent memory accumulation"""
        if now - self._last_sweep < self._sweep_interval:
            return
        
        self._last_sweep = now
        stale_keys = []
        for msg_id, data in self.message_throttles.items():
            if now - data.get('last_update_time', 0) > self._max_age:
                stale_keys.append(msg_id)
        
        for key in stale_keys:
            del self.message_throttles[key]
        
        if stale_keys:
            LOGGER(__name__).debug(f"Cleaned up {len(stale_keys)} stale progress throttle entries")
    
    def should_update(self, message_id, current, total, now):
        """
        Determine if progress should be updated based on throttle rules.
        
        Rules:
        - Minimum 4 seconds between updates (or 10% progress change)
        - If rate limited, exponential backoff up to 60 seconds
        - Always allow 100% completion
        """
        self._sweep_stale_entries(now)
        
        if message_id not in self.message_throttles:
            self.message_throttles[message_id] = {
                'last_update_time': 0,
                'last_percentage': 0,
                'last_bytes': 0,
                'last_speed_time': now,
                'rate_limited': False,
                'backoff_duration': 5,  # Start with 5 seconds
                'cooldown_until': 0
            }
        
        throttle = self.message_throttles[message_id]
        percentage = (current / total) * 100 if total > 0 else 0
        
        # Always allow 100% completion
        if percentage >= 100:
            return True
        
        # If we're in cooldown from rate limiting, don't update
        if throttle['cooldown_until'] > now:
            return False
        
        # Check time and percentage thresholds
        time_diff = now - throttle['last_update_time']
        percentage_diff = percentage - throttle['last_percentage']
        
        # Balanced Optimization: 5 seconds OR 15% progress
        # This prevents the bot from spending too much CPU on message edits
        # while keeping the progress bar smooth enough.
        return time_diff >= 5 or percentage_diff >= 15
    
    def get_current_speed(self, message_id, current, now):
        """
        Calculate current transfer speed based on bytes transferred since last update.
        Returns speed in bytes per second.
        """
        if message_id not in self.message_throttles:
            return 0
        
        throttle = self.message_throttles[message_id]
        last_bytes = throttle.get('last_bytes', 0)
        last_time = throttle.get('last_speed_time', now)
        
        time_diff = now - last_time
        bytes_diff = current - last_bytes
        
        if time_diff > 0 and bytes_diff > 0:
            return bytes_diff / time_diff
        return 0
    
    def mark_updated(self, message_id, percentage, now, current_bytes=0):
        """Mark that an update was successfully sent"""
        if message_id in self.message_throttles:
            throttle = self.message_throttles[message_id]
            throttle['last_update_time'] = now
            throttle['last_percentage'] = percentage
            throttle['last_bytes'] = current_bytes
            throttle['last_speed_time'] = now
            # Reset backoff on successful update
            throttle['rate_limited'] = False
            throttle['backoff_duration'] = 5
    
    def mark_rate_limited(self, message_id, now):
        """Mark that we hit a rate limit and implement exponential backoff"""
        if message_id in self.message_throttles:
            throttle = self.message_throttles[message_id]
            throttle['rate_limited'] = True
            # Exponential backoff: 5s -> 10s -> 20s -> 40s -> 60s (max)
            throttle['backoff_duration'] = min(throttle['backoff_duration'] * 2, 60)
            throttle['cooldown_until'] = now + throttle['backoff_duration']
            LOGGER(__name__).info(f"Rate limited - backing off for {throttle['backoff_duration']}s")
    
    def cleanup(self, message_id):
        """Remove throttle data when done"""
        if message_id in self.message_throttles:
            del self.message_throttles[message_id]

# Global throttle instance
_progress_throttle = ProgressThrottle()

# Native Telethon progress callback (replaces Pyleaves to reduce RAM)
async def safe_progress_callback(current, total, *args):
    """
    Native Telethon progress callback - lightweight and RAM-efficient
    """
    progress_message = None
    try:
        # Unpack args
        action = args[0] if len(args) > 0 else "Progress"
        progress_message = args[1] if len(args) > 1 else None
        start_time = args[2] if len(args) > 2 else time()
        
        if not progress_message:
            return
        
        now = time()
        percentage = (current / total) * 100 if total > 0 else 0
        message_id = progress_message.id
        
        # ALWAYS use standard throttle settings for consistent progress bar behavior
        if not _progress_throttle.should_update(message_id, current, total, now):
            return
        
        # Calculate current speed
        current_speed = _progress_throttle.get_current_speed(message_id, current, now)
        elapsed_time = now - start_time
        if current_speed == 0 and elapsed_time > 0:
            current_speed = current / elapsed_time
        
        eta = (total - current) / current_speed if current_speed > 0 else 0
        from helpers.files import get_readable_file_size, get_readable_time
        
        # RAM-efficient visual progress bar using string slicing (no multiplication)
        # Pre-built 20-character templates - only ~40 bytes total
        pct = int(percentage)
        filled_count = pct // 5  # 0-20 filled blocks
        
        # Pre-built full strings (sliced, not multiplied - minimal RAM)
        FILLED_BAR = "████████████████████"  # 20 filled chars
        EMPTY_BAR = "░░░░░░░░░░░░░░░░░░░░"   # 20 empty chars
        
        # Build progress bar by slicing pre-built strings
        progress_bar = f"[{FILLED_BAR[:filled_count]}{EMPTY_BAR[:20-filled_count]}]"
        
        # Optimized: Pre-calculate readable sizes once to save CPU
        readable_current = get_readable_file_size(current)
        readable_total = get_readable_file_size(total)
        readable_speed = get_readable_file_size(current_speed)
        readable_eta = get_readable_time(int(eta))
        
        # Ultra-simplified: Percent • Speed • ETA (Zero Overhead)
        progress_text = f"**{action}** `{pct}%` • `{readable_speed}/s` • `{readable_eta}`"
        
        # Try to update message
        await progress_message.edit(progress_text)
        # Mark successful update with current bytes for next speed calculation
        _progress_throttle.mark_updated(message_id, percentage, now, current)
            
    except Exception as e:
        # Reduced logging level for progress errors to save CPU
        error_str = str(e).lower()
        
        # Check if it's a rate limit error
        if 'wait of' in error_str and 'seconds is required' in error_str:
            # This is a rate limit error - mark it and back off
            if progress_message:
                _progress_throttle.mark_rate_limited(progress_message.id, time())
        # Silently ignore errors related to deleted or invalid messages
        elif any(err in error_str for err in ['message_id_invalid', 'message not found', 'message to edit not found', 'message can\'t be edited']):
            pass
        else:
            # Only log actual unexpected errors
            pass


async def forward_to_dump_channel(bot, sent_message, user_id, caption=None, source_url=None):
    """
    Send media to dump channel for monitoring (if configured).
    Uses the media from sent_message (no re-upload) with custom caption showing user ID.
    
    Args:
        bot: Telethon Client instance
        sent_message: The message object that was sent to the user
        user_id: User ID who downloaded this
        caption: Original caption (optional, added below user ID)
        source_url: Original download URL (optional, shows where user downloaded from)
    """
    from config import PyroConf
    
    # Only send if dump channel is configured
    if not PyroConf.DUMP_CHANNEL_ID:
        return
    
    try:
        # Convert channel ID to integer format
        channel_id = int(PyroConf.DUMP_CHANNEL_ID)
        
        # Build custom caption with User ID at the top
        custom_caption = f"👤 User ID: {user_id}"
        
        # Add source URL if present (no extra RAM - just a string)
        if source_url:
            custom_caption += f"\n🔗 Source: {source_url}"
        
        # Add original caption below if present
        if caption:
            custom_caption += f"\n\n📝 Original Caption:\n{caption[:3900]}"  # Reduced limit to fit URL
        
        # Send media using the media from sent_message (no re-upload!)
        # Telethon reuses the file reference, so this is RAM-efficient
        await bot.send_file(
            channel_id,
            sent_message.media,
            caption=custom_caption
        )
        
        LOGGER(__name__).info(f"✅ Sent media to dump channel for user {user_id} (RAM-efficient, no re-upload, no 'Forwarded from' label)")
    except Exception as e:
        # Silently log errors - don't interrupt user's download
        LOGGER(__name__).warning(f"Failed to send to dump channel: {e}")

# Generate progress args for downloading/uploading (minimal tuple - low RAM)
def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time)


async def send_media(
    bot, message, media_path, media_type, caption, progress_message, start_time, user_id=None, source_url=None
):
    """Upload media with all safeguards (size checks, fast uploads, thumbnails, dump channel).
    
    Args:
        source_url: Original download URL for tracking in dump channel (no extra RAM usage)
    
    Returns:
        bool: True if upload succeeded, False if it was rejected or failed
    """
    file_size = os.path.getsize(media_path)

    if not await fileSizeLimit(file_size, message, "upload"):
        return False

    
    progress_args = progressArgs("📤 Uploading", progress_message, start_time)
    LOGGER(__name__).debug(f"Uploading media: {media_path} ({media_type})")

    if media_type == "photo":
        from helpers.transfer import upload_media_fast
        
        fast_file = await upload_media_fast(
            bot, 
            media_path, 
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # FastTelethon upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                force_document=False,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=False
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption, source_url)
        
        return True
    elif media_type == "video":
        # Get video duration, metadata, and dimensions in one highly optimized go
        duration, artist, title, width, height = await get_media_info(media_path)
        
        # Generate high-quality thumbnail (will use the duration we just extracted)
        thumb_path = await generate_thumbnail(media_path, duration=duration)

        # Prepare video attributes with precise info
        attributes = []
        if duration and duration > 0:
            attributes.append(DocumentAttributeVideo(
                duration=int(duration),
                w=width,
                h=height,
                supports_streaming=True
            ))
        
        # Final fallback for thumbnail if generation failed
        if not thumb_path and user_id:
            from database_sqlite import db
            user_data = db.get_user(user_id)
            user_thumb = user_data.get('custom_thumbnail') if user_data else None
            if user_thumb and os.path.exists(user_thumb):
                thumb_path = user_thumb
                LOGGER(__name__).info(f"Using custom thumbnail for user {user_id}: {thumb_path}")
        
        sent_message = None
        try:
            from helpers.transfer import upload_media_fast
            
            fast_file = await upload_media_fast(
                bot,
                media_path,
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
            )
            
            if fast_file:
                sent_message = await bot.send_file(
                    message.chat_id,
                    fast_file,
                    caption=caption or "",
                    attributes=attributes if attributes else None,
                    thumb=thumb_path,
                    force_document=False,
                    file_name=os.path.basename(media_path)
                )
            else:
                sent_message = await bot.send_file(
                    message.chat_id,
                    media_path,
                    caption=caption or "",
                    attributes=attributes if attributes else None,
                    thumb=thumb_path,
                    progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                    force_document=False
                )
        except Exception as e:
            LOGGER(__name__).error(f"Upload failed: {e}")
            raise
        finally:
            # Clean up thumbnail file
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass
        
        # Forward to dump channel if upload was successful (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption, source_url)
        
        return True
    elif media_type == "audio":
        # Get audio metadata including duration, artist, and title
        duration, artist, title, _, _ = await get_media_info(media_path)
        
        # Prepare audio attributes with high precision
        attributes = []
        if duration and duration > 0:
            attributes.append(DocumentAttributeAudio(
                duration=int(duration),
                performer=artist or "Bot",
                title=title or os.path.basename(media_path),
                voice=False
            ))
        
        from helpers.transfer import upload_media_fast
        
        fast_file = await upload_media_fast(
            bot,
            media_path,
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # FastTelethon upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                attributes=attributes if attributes else None,
                force_document=False,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                attributes=attributes if attributes else None,
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=False
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption, source_url)
        
        return True
    elif media_type == "document":
        from helpers.transfer import upload_media_fast
        
        fast_file = await upload_media_fast(
            bot,
            media_path,
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # FastTelethon upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                force_document=True,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=True
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption, source_url)
        
        return True


PER_FILE_TIMEOUT_SECONDS = 2700


async def _process_single_media_file(
    client_for_download, bot, user_message, msg, download_path, 
    idx, total_files, progress_message, file_start_time, user_id, source_url
):
    """
    Process a single file from a media group - download and upload.
    
    CRITICAL: This function is defined OUTSIDE processMediaGroup to prevent closure capture.
    All parameters are passed explicitly to avoid holding references to Telethon Message objects.
    
    Args:
        client_for_download: Telethon client for downloading
        bot: Bot client for uploading to user
        user_message: The user's original message (for reply context)
        msg: The Telethon Message object to download (WILL BE USED AND RELEASED)
        download_path: Path to save the downloaded file
        idx: Current file index (1-based)
        total_files: Total number of files
        progress_message: Progress message to update
        file_start_time: Start time for progress calculation
        user_id: User ID for tracking
        source_url: Source URL for dump channel
        
    Returns:
        tuple: (result_path, upload_success)
    """
    # STEP 1: Download this file
    result_path = await download_media_fast(
        client=client_for_download,
        message=msg,
        file=download_path,
        progress_callback=lambda c, t: safe_progress_callback(
            c, t, *progressArgs(f"📥 Downloading {idx}/{total_files}", progress_message, file_start_time)
        )
    )
    
    if not result_path:
        LOGGER(__name__).warning(f"File {idx}/{total_files} download failed: no media path returned")
        return None, False
    
    # RAM OPTIMIZATION: Release download buffers before upload starts
    # This ensures peak RAM usage is minimized by clearing download memory before allocating upload buffers
    gc.collect()
    LOGGER(__name__).debug(f"RAM released after download, before upload: file {idx}/{total_files}")
    
    # Determine media type from msg attributes
    media_type = (
        "photo" if msg.photo
        else "video" if msg.video
        else "audio" if msg.audio
        else "document"
    )
    
    # Get caption
    caption_text = msg.text or ""
    
    # STEP 2: Upload this file
    LOGGER(__name__).info(f"Uploading file {idx}/{total_files} to user (via send_media)")
    upload_success = await send_media(
        bot=bot,
        message=user_message,
        media_path=result_path,
        media_type=media_type,
        caption=caption_text,
        progress_message=progress_message,
        start_time=file_start_time,
        user_id=user_id,
        source_url=source_url
    )
    
    return result_path, upload_success


async def processMediaGroup(chat_message, bot, message, user_id=None, user_client=None, source_url=None):
    """Process and download a media group (multiple files in one post)
    
    ONE-AT-A-TIME APPROACH: Downloads and uploads each file sequentially to minimize RAM usage.
    Uses send_media() helper to preserve all safeguards (size checks, fast uploads, thumbnails, dump channel).
    Files are deleted immediately after upload to free memory.
    
    PER-FILE TIMEOUT: Each file gets its own 45-minute timeout instead of sharing one
    timeout for the entire media group. This ensures large files don't starve smaller ones.
    
    RAM OPTIMIZATION: Message objects are NOT retained in lists. We extract message IDs first,
    then re-fetch each message individually inside the loop. This prevents Telethon from
    caching large document objects (~4-12MB each) for the entire duration of media group processing.
    
    Args:
        chat_message: The Telegram message containing the media group
        bot: Bot client (for sending to user)
        message: User's message
        user_id: User ID for dump channel tracking
        user_client: User's Telegram client (for downloading from private channels)
        source_url: Original download URL for tracking in dump channel (no extra RAM usage)
        
    Returns:
        int: Number of files successfully downloaded and sent (0 if failed)
    """
    
    # Log memory at start of media group processing
    
    # Use user_client to fetch messages from private/public channels
    # Fall back to bot if user_client is not provided (backward compatibility)
    client_for_download = user_client if user_client else bot
    
    # Get the chat_id and grouped_id for later use (lightweight references)
    chat_id = chat_message.chat_id
    grouped_id = chat_message.grouped_id
    
    # Get all messages in the media group
    media_group_messages = await client_for_download.get_messages(
        chat_id,
        ids=[chat_message.id + i for i in range(-10, 11)]
    )
    
    # CRITICAL RAM FIX: Extract only message IDs, then immediately clear the message list
    # This prevents Telethon Message objects (with cached document data ~4-12MB each) 
    # from being held in memory for the entire duration of media group processing
    message_ids = []
    if grouped_id:
        for msg in media_group_messages:
            if msg and hasattr(msg, 'grouped_id') and msg.grouped_id == grouped_id:
                message_ids.append(msg.id)
    else:
        message_ids = [chat_message.id]
    
    # Sort IDs to maintain order
    message_ids.sort()
    
    # CRITICAL: Clear references to message objects immediately to allow GC
    del media_group_messages
    gc.collect()
    
    total_files = len(message_ids)
    
    # Determine user tier once for all files (avoid blocking DB calls in loop)
    is_premium = False
    if user_id:
        try:
            from database_sqlite import db
            user_type = db.get_user_type(user_id)
            is_premium = user_type in ['paid', 'admin']
        except Exception as e:
            LOGGER(__name__).warning(f"Could not determine user tier, using free tier: {e}")

    # Slice message_ids based on remaining quota for free users ONLY
    original_total = total_files
    if not is_premium:
        from database_sqlite import db
        can_dl, _, remaining_quota = db.can_download(user_id) if user_id else (True, "", 9999)
        if remaining_quota < total_files:
            message_ids = message_ids[:remaining_quota]
            total_files = len(message_ids)
            LOGGER(__name__).info(f"Partial download: Free user {user_id} has {remaining_quota} quota, slicing {original_total} items to {total_files}")
    
    files_sent_count = 0
    
    # ... rest of the setup ...
    start_time = time()
    progress_message = await message.reply(f"📥 Processing media group ({total_files} files)..." if total_files == original_total else f"📥 Processing media group (Partial: {total_files}/{original_total} files based on quota)...")
    LOGGER(__name__).info(
        f"Processing media group with {total_files} items (one-at-a-time mode for low RAM usage)..."
    )

    # Process each file one at a time: download → upload (via send_media) → delete → next
    # Each file gets its own 45-minute timeout (PER_FILE_TIMEOUT_SECONDS)
    # CRITICAL RAM FIX: We iterate over message IDs and re-fetch each message individually
    # This prevents holding all Message objects in memory (each can be 4-12MB with cached document data)
    for idx, msg_id in enumerate(message_ids, 1):
        msg = None  # Will be set after fetching
        media_path = None
        file_start_time = time()
        
        # PERMANENT FLOODWAIT FIX: Add delay between items in media group
        if idx > 1:
            delay = get_intra_request_delay(is_premium)
            LOGGER(__name__).debug(f"MediaGroup: Waiting {delay}s before next file to avoid FloodWait")
            await asyncio.sleep(delay)
            
        try:
            # Update progress
            await progress_message.edit(
                f"📥 Processing file {idx}/{total_files} (45min timeout per file)..."
            )
            
            # CRITICAL RAM FIX: Re-fetch the message fresh for each file
            # This prevents closure capture and allows each message to be GC'd after processing
            msg = await client_for_download.get_messages(chat_id, ids=msg_id)
            
            if not msg or not (msg.media or msg.photo or msg.video or msg.document or msg.audio):
                LOGGER(__name__).warning(f"File {idx}/{total_files}: No media found in message {msg_id}")
                continue
            
            # Get filename from message
            filename = get_file_name(msg.id, msg)
            # Use message.id as folder_id to group all media group files together
            download_path = get_download_path(message.id, filename)
            
            # Set expected path BEFORE download starts - ensures cleanup works even if timeout during download
            media_path = download_path
            
            # STEP 1 & 2: Download and upload using external helper (no closure capture)
            LOGGER(__name__).info(f"Downloading file {idx}/{total_files}: {filename} (45min timeout)")
            
            # Execute with per-file timeout (45 minutes)
            # CRITICAL: Uses external helper function to avoid closure capture
            try:
                result_path, upload_success = await asyncio.wait_for(
                    _process_single_media_file(
                        client_for_download=client_for_download,
                        bot=bot,
                        user_message=message,
                        msg=msg,
                        download_path=download_path,
                        idx=idx,
                        total_files=total_files,
                        progress_message=progress_message,
                        file_start_time=file_start_time,
                        user_id=user_id,
                        source_url=source_url
                    ),
                    timeout=PER_FILE_TIMEOUT_SECONDS
                )
                
                if result_path:
                    media_path = result_path
                
                # Only count as sent if upload succeeded
                if upload_success:
                    files_sent_count += 1
                    elapsed = time() - file_start_time
                    LOGGER(__name__).info(f"Successfully processed file {idx}/{total_files} in {elapsed:.1f}s")
                else:
                    LOGGER(__name__).warning(f"File {idx}/{total_files} was not sent (rejected by size limit or other error)")
                    
            except asyncio.TimeoutError:
                elapsed = time() - file_start_time
                LOGGER(__name__).error(
                    f"PER-FILE TIMEOUT: File {idx}/{total_files} timed out after {elapsed:.1f}s "
                    f"(limit: {PER_FILE_TIMEOUT_SECONDS}s / 45min)"
                )
                try:
                    await progress_message.edit(
                        f"⏰ File {idx}/{total_files} timed out after 45 minutes. Moving to next file..."
                    )
                except:
                    pass
            
            # STEP 3: Delete the file and release RAM (critical for 512MB limit)
            if media_path:
                try:
                    from database_sqlite import db
                    await cleanup_download_delayed(media_path, user_id, db)
                    LOGGER(__name__).info(f"Cleaned up file {idx}/{total_files}: {os.path.basename(media_path)}")
                except Exception as cleanup_err:
                    LOGGER(__name__).warning(f"Failed to cleanup file {idx}/{total_files}: {cleanup_err}")
            
            # STEP 4: Tier-aware cooldown between files (same as single file downloads)
            # This wait time prevents RAM spikes by allowing memory to be fully reclaimed
            if idx < total_files:
                delay = get_intra_request_delay(is_premium)
                LOGGER(__name__).info(f"⏳ Waiting {delay}s before next file (RAM cooldown, same as single files)")
                await asyncio.sleep(delay)
            
        except asyncio.CancelledError:
            LOGGER(__name__).info(f"File {idx}/{total_files} processing cancelled")
            if media_path:
                try:
                    from database_sqlite import db
                    await cleanup_download_delayed(media_path, user_id, db)
                except:
                    pass
            raise
            
        except Exception as e:
            LOGGER(__name__).error(f"Error processing file {idx}/{total_files} from message {msg_id}: {e}")
            # Clean up on error and release RAM
            if media_path:
                try:
                    from database_sqlite import db
                    await cleanup_download_delayed(media_path, user_id, db)
                except:
                    pass
            
            # Apply tier-aware cooldown even on error (same as single files)
            if idx < total_files:
                delay = get_intra_request_delay(is_premium)
                LOGGER(__name__).info(f"⏳ Waiting {delay}s after error before next file")
                await asyncio.sleep(delay)
            
            continue
        
        finally:
            # CRITICAL RAM FIX: Explicitly delete msg reference after each iteration
            # This allows Telethon to release cached document data (~4-12MB per message)
            if msg is not None:
                del msg
                msg = None
            if media_path is not None:
                del media_path
                media_path = None
            # Force garbage collection after each file to release Telethon buffers
            gc.collect()

    # Cleanup throttle data for this progress message
    _progress_throttle.cleanup(progress_message.id)
    
    # Delete progress message
    await progress_message.delete()
    
    # Log memory at end of media group processing
    
    # Force final garbage collection after media group
    gc.collect()
    
    if files_sent_count == 0:
        await message.reply("**❌ No valid media found in the group**")
        return 0
    
    # Don't send completion message here - let main.py handle it based on user type
    # This allows customized messages for free vs premium users
    LOGGER(__name__).info(f"Media group complete: {files_sent_count}/{total_files} files sent successfully")
    return files_sent_count
