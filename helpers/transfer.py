"""
HIGH-SPEED TRANSFER APPROACH for Maximum Throughput (~20MB/s)
=============================================================

DOWNLOADS: FastTelethon (Parallel)
- Uses FastTelethon for parallel download connections
- 20 concurrent connections for maximum speed
- Target: ~20MB/s download speed

UPLOADS: FastTelethon (Parallel)
- Uses FastTelethon for parallel upload connections
- 20 concurrent connections for maximum speed
- Target: ~20MB/s upload speed

This approach prioritizes SPEED over RAM efficiency.
Note: May use more RAM (~100-200MB for large files)
"""
import os
import asyncio
import math
import inspect
import psutil
from typing import Optional, Callable, BinaryIO, Set
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation, MessageMediaPaidMedia
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

def get_ram_usage_mb():
    """Get current RAM usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def create_ram_logging_callback(original_callback: Optional[Callable], file_size: int, operation: str, file_name: str):
    """
    Wrap progress callback to log RAM usage at 25%, 50%, 75% progress.
    """
    logged_thresholds: Set[int] = set()
    start_ram = get_ram_usage_mb()
    LOGGER(__name__).info(f"[RAM] {operation} START: {file_name} - RAM: {start_ram:.1f}MB")
    
    def ram_logging_wrapper(current: int, total: int):
        nonlocal logged_thresholds
        
        if total <= 0:
            if original_callback:
                return original_callback(current, total)
            return
        
        percent = (current / total) * 100
        
        for threshold in [25, 50, 75, 100]:
            if percent >= threshold and threshold not in logged_thresholds:
                logged_thresholds.add(threshold)
                current_ram = get_ram_usage_mb()
                ram_increase = current_ram - start_ram
                LOGGER(__name__).info(
                    f"[RAM] {operation} {threshold}%: {file_name} - "
                    f"RAM: {current_ram:.1f}MB (+{ram_increase:.1f}MB from start)"
                )
        
        if original_callback:
            return original_callback(current, total)
    
    return ram_logging_wrapper

IS_CONSTRAINED = False

MAX_CONNECTIONS = 20
MAX_UPLOAD_CONNECTIONS = 20
MAX_DOWNLOAD_CONNECTIONS = 20

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Download media using FASTTTELETHON for maximum speed (~20MB/s).
    Uses 20 parallel connections for high-throughput downloads.
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
        
        connection_count = _optimized_connection_count_download(file_size)
        LOGGER(__name__).debug(f"FastTelethon download: {file} ({file_size/1024/1024:.1f}MB, {connection_count} connections)")
        
        file_name = os.path.basename(file)
        ram_callback = create_ram_logging_callback(progress_callback, file_size, "DOWNLOAD", file_name)
        
        if media_location and file_size > 0:
            import gc
            with open(file, 'wb') as f:
                await fast_download(
                    client=client,
                    location=media_location,
                    out=f,
                    progress_callback=ram_callback,
                    file_size=file_size,
                    connection_count=connection_count
                )
            end_ram = get_ram_usage_mb()
            LOGGER(__name__).info(f"[RAM] DOWNLOAD COMPLETE: {file_name} - RAM before GC: {end_ram:.1f}MB")
            
            gc.collect()
            after_gc_ram = get_ram_usage_mb()
            ram_released = end_ram - after_gc_ram
            LOGGER(__name__).info(f"[RAM] DOWNLOAD GC: {file_name} - RAM after GC: {after_gc_ram:.1f}MB (released: {ram_released:.1f}MB)")
            return file
        else:
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
    Upload media using FASTTTELETHON for optimized parallel uploads.
    This uses parallel connections for faster uploads while managing RAM efficiently.
    
    FastTelethon uploads stream data in chunks, preventing full file loading into RAM.
    Connection count is automatically optimized based on file size.
    
    CRITICAL: Uses try/finally to ensure cleanup runs even on failures,
    preventing memory leaks from orphaned file handles and buffers.
    """
    import gc
    
    file_size = os.path.getsize(file_path)
    
    # Calculate connection count to verify the monkeypatch is working
    connection_count = _optimized_connection_count_upload(file_size)
    
    file_handle = None
    result = None
    
    try:
        file_name = os.path.basename(file_path)
        LOGGER(__name__).debug(f"Upload starting: {file_path} ({file_size/1024/1024:.1f}MB)")
        
        ram_callback = create_ram_logging_callback(progress_callback, file_size, "UPLOAD", file_name)
        
        file_handle = open(file_path, 'rb')
        result = await fast_upload(
            client=client,
            file=file_handle,
            progress_callback=ram_callback,
            connection_count=connection_count
        )
        
        end_ram = get_ram_usage_mb()
        LOGGER(__name__).info(f"[RAM] UPLOAD COMPLETE: {file_name} - RAM before GC: {end_ram:.1f}MB")
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
        
        before_gc = get_ram_usage_mb()
        gc.collect()
        after_gc = get_ram_usage_mb()
        ram_released = before_gc - after_gc
        LOGGER(__name__).info(f"[RAM] UPLOAD GC: {os.path.basename(file_path)} - RAM after GC: {after_gc:.1f}MB (released: {ram_released:.1f}MB)")

def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    HIGH-SPEED: Optimized connections for fastest uploads.
    20 parallel connections for large files, scaled down for smaller files.
    """
    if file_size >= 10 * 1024 * 1024:  # 10MB+
        return MAX_UPLOAD_CONNECTIONS  # 20 connections
    elif file_size >= 1 * 1024 * 1024:  # 1MB-10MB
        return min(15, MAX_UPLOAD_CONNECTIONS)
    elif file_size >= 100 * 1024:  # 100KB-1MB
        return min(10, MAX_UPLOAD_CONNECTIONS)
    elif file_size >= 10 * 1024:  # 10KB-100KB
        return min(8, MAX_UPLOAD_CONNECTIONS)
    else:  # < 10KB
        return min(5, MAX_UPLOAD_CONNECTIONS)

def _optimized_connection_count_download(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    HIGH-SPEED: Optimized connections for fastest downloads.
    20 parallel connections for large files, scaled down for smaller files.
    """
    if file_size >= 10 * 1024 * 1024:  # 10MB+
        return MAX_DOWNLOAD_CONNECTIONS  # 20 connections
    elif file_size >= 1 * 1024 * 1024:  # 1MB-10MB
        return min(15, MAX_DOWNLOAD_CONNECTIONS)
    elif file_size >= 100 * 1024:  # 100KB-1MB
        return min(10, MAX_DOWNLOAD_CONNECTIONS)
    elif file_size >= 10 * 1024:  # 10KB-100KB
        return min(8, MAX_DOWNLOAD_CONNECTIONS)
    else:  # < 10KB
        return min(5, MAX_DOWNLOAD_CONNECTIONS)

ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
