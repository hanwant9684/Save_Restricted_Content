"""
HIGH-SPEED TRANSFER MODULE for Per-User Sessions
=================================================

This module implements fast file transfers using FastTelethon.
Since each user has their own Telegram session, no global connection
pooling is needed - each session can use full connection capacity.
"""
import os
import asyncio
import gc
from typing import Optional, Callable
from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaPaidMedia
from logger import LOGGER
from connection_manager import download_file_optimized, upload_file_optimized

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Download media using optimized connection capacity.
    """
    if not message.media:
        raise ValueError("Message has no media")
    
    if isinstance(message.media, MessageMediaPaidMedia):
        # ... existing logic for paid media ...
        if hasattr(message.media, 'extended_media') and message.media.extended_media:
            extended = message.media.extended_media
            if isinstance(extended, list) and len(extended) > 0:
                first_media = extended[0]
                if hasattr(first_media, 'media') and first_media.media:
                    return await client.download_media(first_media.media, file=file, progress_callback=progress_callback)
            elif hasattr(extended, 'media') and extended.media:
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
        
        if media_location and file_size > 0:
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, 'wb') as f:
                await download_file_optimized(
                    client=client,
                    location=media_location,
                    out=f,
                    progress_callback=progress_callback,
                    file_size=file_size
                )
            
            if os.path.exists(file) and os.path.getsize(file) > 0:
                gc.collect()
                return file
            else:
                raise IOError("File download resulted in empty or missing file")
        else:
            return await client.download_media(message, file=file, progress_callback=progress_callback)
        
    except Exception as e:
        LOGGER(__name__).error(f"Optimized download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    Upload media using optimized connection capacity.
    """
    file_handle = None
    try:
        file_handle = open(file_path, 'rb')
        result = await upload_file_optimized(
            client=client,
            file=file_handle,
            progress_callback=progress_callback
        )
        return result
    except Exception as e:
        LOGGER(__name__).error(f"Optimized upload failed: {e}")
        return None
    finally:
        if file_handle:
            try:
                file_handle.close()
            except:
                pass
