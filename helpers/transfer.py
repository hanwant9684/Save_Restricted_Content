"""
HIGH-SPEED TRANSFER MODULE for Per-User Sessions
=================================================

This module implements fast file transfers using FastTelethon.
Since each user has their own Telegram session, no global connection
pooling is needed - each session can use full connection capacity.

CONFIGURATION (Environment Variables):
- CONNECTIONS_PER_TRANSFER: Connections per download/upload (default: 16)
"""
import os
import asyncio
import math
import inspect
import psutil
import gc
import time
import socket
from typing import Optional, Callable, BinaryIO, Set, Dict
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation, MessageMediaPaidMedia
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

CONNECTIONS_PER_TRANSFER = int(os.getenv("CONNECTIONS_PER_TRANSFER", "16"))

def get_system_diagnostics():
    """Get comprehensive system diagnostics for troubleshooting"""
    try:
        process = psutil.Process(os.getpid())
        cpu_percent = process.cpu_percent(interval=0.1)
        mem = process.memory_info()
        mem_mb = mem.rss / 1024 / 1024
        num_threads = process.num_threads()
        num_fds = len(process.open_files()) if hasattr(process, 'open_files') else 0
        
        # Network stats
        net_io = psutil.net_io_counters()
        
        # CPU overall
        cpu_count = psutil.cpu_count()
        cpu_overall = psutil.cpu_percent(interval=0.1)
        
        return {
            'process_cpu': cpu_percent,
            'process_mem_mb': round(mem_mb, 1),
            'threads': num_threads,
            'open_files': num_fds,
            'system_cpu': cpu_overall,
            'cpu_cores': cpu_count,
            'net_bytes_sent': net_io.bytes_sent,
            'net_bytes_recv': net_io.bytes_recv
        }
    except Exception as e:
        LOGGER(__name__).debug(f"Failed to get diagnostics: {e}")
        return {}

def get_ram_usage_mb():
    """Get current RAM usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def create_speed_monitor_callback(original_callback: Optional[Callable], file_size: int, file_name: str):
    """
    DETAILED DIAGNOSTICS: Monitor download speed and system state to identify throttling cause.
    Logs: Speed, CPU, RAM, threads, connections - identifies if issue is CODE/TELEGRAM/RENDER.
    """
    start_time = time.time()
    last_logged_bytes = 0
    last_logged_time = start_time
    last_speed = 0
    speed_history = []
    stall_detected = False
    start_diag = get_system_diagnostics()
    
    def speed_monitor(current: int, total: int):
        nonlocal last_logged_bytes, last_logged_time, last_speed, stall_detected, speed_history
        
        if total <= 0:
            if original_callback:
                return original_callback(current, total)
            return
        
        current_time = time.time()
        elapsed_since_start = current_time - start_time
        elapsed_since_log = current_time - last_logged_time
        
        # Log speed every 0.5 seconds
        if elapsed_since_log >= 0.5:
            bytes_transferred = current - last_logged_bytes
            current_speed_mb_s = bytes_transferred / (1024 * 1024 * elapsed_since_log) if elapsed_since_log > 0 else 0
            overall_speed = current / (1024 * 1024 * elapsed_since_start) if elapsed_since_start > 0 else 0
            
            speed_history.append(current_speed_mb_s)
            if len(speed_history) > 20:
                speed_history.pop(0)
            
            # System diagnostics DURING transfer
            diag = get_system_diagnostics()
            percent = (current / total) * 100
            
            # Detect speed issues
            is_stall = current_speed_mb_s < 2 and last_speed > 10
            is_burst = current_speed_mb_s > 20
            
            # Diagnose cause of slowdown
            cpu_issue = diag.get('process_cpu', 0) > 80 or diag.get('system_cpu', 0) > 90
            mem_issue = diag.get('process_mem_mb', 0) > 400
            thread_issue = diag.get('threads', 0) > 100
            
            status = "🚀 BURST" if is_burst else "⚠️ STALL" if is_stall else "⚙️ NORMAL"
            
            # Log speed with diagnostics
            LOGGER(__name__).info(
                f"[SPEED-DIAG] {status} | {file_name} {percent:.1f}% | "
                f"Speed: {current_speed_mb_s:.1f}MB/s (avg: {overall_speed:.1f}MB/s) | "
                f"CPU: {diag.get('process_cpu', 0):.0f}% (sys: {diag.get('system_cpu', 0):.0f}%) | "
                f"RAM: {diag.get('process_mem_mb', 0):.0f}MB | "
                f"Threads: {diag.get('threads', 0)} | FDs: {diag.get('open_files', 0)}"
            )
            
            # DETAILED STALL DIAGNOSIS
            if is_stall and not stall_detected:
                stall_detected = True
                
                # Analyze what's causing the stall
                cause = "UNKNOWN"
                if cpu_issue:
                    cause = "HIGH CPU (CODE/RENDER bottleneck)"
                elif mem_issue:
                    cause = "HIGH RAM (Memory pressure)"
                elif thread_issue:
                    cause = "TOO MANY THREADS (Connection queue issue)"
                else:
                    cause = "LIKELY TELEGRAM API THROTTLING (Network stall)"
                
                LOGGER(__name__).warning(
                    f"[STALL-CAUSE] {file_name} | "
                    f"Speed: {last_speed:.1f}→{current_speed_mb_s:.1f}MB/s | "
                    f"CAUSE: {cause} | "
                    f"CPU:{diag.get('process_cpu', 0):.0f}% RAM:{diag.get('process_mem_mb', 0):.0f}MB "
                    f"Threads:{diag.get('threads', 0)} FDs:{diag.get('open_files', 0)}"
                )
                
            elif current_speed_mb_s > 10 and stall_detected:
                stall_detected = False
                cpu_now = diag.get('process_cpu', 0)
                mem_now = diag.get('process_mem_mb', 0)
                LOGGER(__name__).info(
                    f"[STALL-RECOVERY] {file_name} | Speed recovered: {current_speed_mb_s:.1f}MB/s | "
                    f"CPU:{cpu_now:.0f}% RAM:{mem_now:.0f}MB"
                )
            
            last_logged_bytes = current
            last_logged_time = current_time
            last_speed = current_speed_mb_s
        
        if original_callback:
            return original_callback(current, total)
    
    return speed_monitor

IS_CONSTRAINED = False

MAX_CONNECTIONS = CONNECTIONS_PER_TRANSFER
MAX_UPLOAD_CONNECTIONS = CONNECTIONS_PER_TRANSFER
MAX_DOWNLOAD_CONNECTIONS = CONNECTIONS_PER_TRANSFER

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Download media using FastTelethon with full connection capacity.
    
    Since each user has their own Telegram session, each download can
    use the full connection capacity without needing global pooling.
    """
    if not message.media:
        raise ValueError("Message has no media")
    
    if isinstance(message.media, MessageMediaPaidMedia):
        LOGGER(__name__).warning(f"Paid media detected - attempting extended media extraction")
        if hasattr(message.media, 'extended_media') and message.media.extended_media:
            extended = message.media.extended_media
            if isinstance(extended, list) and len(extended) > 0:
                first_media = extended[0]
                if hasattr(first_media, 'media') and first_media.media:
                    LOGGER(__name__).info(f"Extracted media from paid media container")
                    return await client.download_media(first_media.media, file=file, progress_callback=progress_callback)
            elif hasattr(extended, 'media') and extended.media:
                LOGGER(__name__).info(f"Extracted single media from paid media container")
                return await client.download_media(extended.media, file=file, progress_callback=progress_callback)
        raise ValueError("Paid media (premium content) cannot be downloaded - the content owner requires payment to access this media")
    
    try:
        file_size = 0
        media_location = None
        
        if message.document:
            file_size = message.document.size
            media_location = message.document
        elif message.video:
            file_size = getattr(message.video, 'size', 0)
            media_location = message.video
        elif message.audio:
            file_size = getattr(message.audio, 'size', 0)
            media_location = message.audio
        elif message.photo:
            photo_sizes = [size for size in message.photo.sizes if hasattr(size, 'size')]
            if photo_sizes:
                largest_size = max(photo_sizes, key=lambda s: s.size)
                file_size = largest_size.size
                media_location = message.photo
        elif message.voice:
            file_size = getattr(message.voice, 'size', 0)
            media_location = message.voice
        elif message.video_note:
            file_size = getattr(message.video_note, 'size', 0)
            media_location = message.video_note
        elif message.sticker:
            file_size = getattr(message.sticker, 'size', 0)
            media_location = message.sticker
        
        connection_count = get_connection_count_for_size(file_size)
        
        file_name = os.path.basename(file)
        speed_callback = create_speed_monitor_callback(progress_callback, file_size, file_name)
        
        if media_location and file_size > 0:
            with open(file, 'wb') as f:
                await fast_download(
                    client=client,
                    location=media_location,
                    out=f,
                    progress_callback=speed_callback,
                    file_size=file_size,
                    connection_count=connection_count
                )
            gc.collect()
            return file
        else:
            LOGGER(__name__).warning(
                f"FastTelethon bypassed for {file_name}: media_location={media_location is not None}, "
                f"file_size={file_size} - falling back to standard download"
            )
            return await client.download_media(message, file=file, progress_callback=progress_callback)
        
    except Exception as e:
        error_str = str(e).lower()
        if 'paidmedia' in error_str or 'paid' in error_str:
            raise ValueError("Paid media (premium content) cannot be downloaded - the content owner requires payment to access this media")
        LOGGER(__name__).error(f"FastTelethon download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    Upload media using FastTelethon with full connection capacity.
    
    Since each user has their own Telegram session, each upload can
    use the full connection capacity without needing global pooling.
    """
    file_size = os.path.getsize(file_path)
    connection_count = get_connection_count_for_size(file_size)
    
    file_handle = None
    result = None
    
    try:
        file_name = os.path.basename(file_path)
        speed_callback = create_speed_monitor_callback(progress_callback, file_size, file_name)
        
        file_handle = open(file_path, 'rb')
        result = await fast_upload(
            client=client,
            file=file_handle,
            progress_callback=speed_callback,
            connection_count=connection_count
        )
        return result
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon upload failed: {e}")
        return None
        
    finally:
        if file_handle:
            try:
                file_handle.close()
            except:
                pass
        
        gc.collect()


def get_connection_count_for_size(file_size: int, max_count: int = CONNECTIONS_PER_TRANSFER) -> int:
    """
    Determine optimal connection count based on file size.
    
    Larger files benefit from more connections, while smaller files
    don't need as many.
    """
    if file_size >= 10 * 1024 * 1024:
        return max_count
    elif file_size >= 1 * 1024 * 1024:
        return min(12, max_count)
    elif file_size >= 100 * 1024:
        return min(8, max_count)
    elif file_size >= 10 * 1024:
        return min(6, max_count)
    else:
        return min(4, max_count)


def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """Connection count function for uploads."""
    return get_connection_count_for_size(file_size, max_count)

def _optimized_connection_count_download(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """Connection count function for downloads."""
    return get_connection_count_for_size(file_size, max_count)


ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
