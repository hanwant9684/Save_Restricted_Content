import os
import asyncio
import math
from typing import Optional, Callable, BinaryIO
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

# Tiered connection scaling for RAM optimization
# Each connection uses ~5-10MB RAM
# For 1GB+ files: Use only 4 connections to minimize RAM (~20-40MB vs 60-160MB)
# For 200MB-1GB: Use 6 connections for balance (~30-60MB)
# For <200MB: Use 8 connections for speed (~40-80MB)
MAX_DOWNLOAD_CONNECTIONS = 8 if IS_CONSTRAINED else 12
MAX_UPLOAD_CONNECTIONS = 6 if IS_CONSTRAINED else 8

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    if not message.media:
        raise ValueError("Message has no media")
    
    try:
        media = None
        file_size = 0
        
        if message.document:
            media = message.document
            file_size = message.document.size
        elif message.video:
            media = message.video
            file_size = getattr(message.video, 'size', 0)
        elif message.audio:
            media = message.audio
            file_size = getattr(message.audio, 'size', 0)
        elif message.photo:
            photo_sizes = [size for size in message.photo.sizes if hasattr(size, 'size')]
            if not photo_sizes:
                LOGGER(__name__).warning("No valid photo sizes found, using standard download")
                return await client.download_media(message, file=file, progress_callback=progress_callback)
            
            largest_size = max(photo_sizes, key=lambda s: s.size)
            file_size = largest_size.size
            
            media = InputPhotoFileLocation(
                id=message.photo.id,
                access_hash=message.photo.access_hash,
                file_reference=message.photo.file_reference,
                thumb_size=largest_size.type
            )
        else:
            raise ValueError("Unsupported media type")
        
        # Always use FastTelethon for ALL files (faster than standard download)
        # Even small files benefit from parallel connections
        LOGGER(__name__).info(f"FastTelethon download starting: {file} ({file_size} bytes, optimized connections)")
        
        with open(file, 'wb') as f:
            await fast_download(
                client=client,
                location=media,
                out=f,
                progress_callback=progress_callback,
                file_size=file_size
            )
        
        LOGGER(__name__).info(f"FastTelethon download complete: {file}")
        return file
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    file_size = os.path.getsize(file_path)
    
    # Always use FastTelethon for ALL files (faster upload speeds)
    try:
        LOGGER(__name__).info(f"FastTelethon upload starting: {file_path} ({file_size} bytes, optimized connections)")
        
        with open(file_path, 'rb') as f:
            result = await fast_upload(
                client=client,
                file=f,
                progress_callback=progress_callback
            )
        
        LOGGER(__name__).info(f"FastTelethon upload complete: {file_path}")
        return result
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon upload failed: {e}")
        return None

def _optimized_connection_count(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    Tiered connection scaling optimized for RAM efficiency
    - Files >= 1GB: Use 4 connections (~20-40MB RAM) - Minimal RAM usage
    - Files 200MB-1GB: Use 6 connections (~30-60MB RAM) - Balanced
    - Files < 200MB: Use 8 connections (~40-80MB RAM) - Faster speed
    This prevents RAM spikes on large downloads while maintaining good performance
    """
    # Large files (1GB+): Minimize connections to save RAM
    if file_size >= 1024 * 1024 * 1024:  # 1GB
        return 4
    # Medium-large files (200MB-1GB): Balanced approach
    elif file_size >= 200 * 1024 * 1024:  # 200MB
        return 6
    # Smaller files: Use more connections for speed (still reasonable RAM)
    else:
        return min(8, max_count)

ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count)
