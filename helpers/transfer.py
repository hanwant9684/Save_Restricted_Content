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
import gc
from typing import Optional, Callable, BinaryIO, Set, Dict
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation, MessageMediaPaidMedia
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

CONNECTIONS_PER_TRANSFER = int(os.getenv("CONNECTIONS_PER_TRANSFER", "16"))

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
        raise ValueError("Paid media (premium content) cannot be downloaded")
    
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
        
        # Replit Optimization: Use 8 parallel connections to balance speed and RAM
        connection_count = 16
        
        LOGGER(__name__).info(
            f"Starting download: {os.path.basename(file)} "
            f"({file_size/1024/1024:.1f}MB, {connection_count} connections)"
        )
        
        if media_location and file_size > 0:
            with open(file, 'wb') as f:
                await fast_download(
                    client=client,
                    location=media_location,
                    out=f,
                    progress_callback=progress_callback,
                    file_size=file_size,
                    connection_count=connection_count
                )
            gc.collect()
            return file
        else:
            return await client.download_media(message, file=file, progress_callback=progress_callback)
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    Upload media using FastTelethon with full connection capacity.
    """
    file_size = os.path.getsize(file_path)
    connection_count = get_connection_count_for_size(file_size)
    
    file_handle = None
    try:
        LOGGER(__name__).info(
            f"Starting upload: {os.path.basename(file_path)} "
            f"({file_size/1024/1024:.1f}MB, {connection_count} connections)"
        )
        
        file_handle = open(file_path, 'rb')
        return await fast_upload(
            client=client,
            file=file_handle,
            progress_callback=progress_callback,
            connection_count=connection_count
        )
        
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
    if file_size >= 50 * 1024 * 1024:
        return max_count
    elif file_size >= 10 * 1024 * 1024:
        return min(8, max_count)
    elif file_size >= 1 * 1024 * 1024:
        return min(8, max_count)
    elif file_size >= 10 * 1024:
        return min(8, max_count)
    else:
        return min(8, max_count)


def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """Connection count function for uploads."""
    return get_connection_count_for_size(file_size, max_count)

def _optimized_connection_count_download(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """Connection count function for downloads."""
    return get_connection_count_for_size(file_size, max_count)


ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
