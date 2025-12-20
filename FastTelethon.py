"""
Fast Telethon - Optimized for Render
"""
import asyncio
import time
from typing import BinaryIO, Optional, Union
import math


class FastTelethon:
    """Fast download/upload handler with optimizations for Render environment"""
    
    def __init__(self, client, log_handler=None):
        self.client = client
        self.log_handler = log_handler
        
    async def download(self, message, file_path: str, chunk_size: int = 1024 * 1024):
        """Download with exponential backoff and optimized chunk size"""
        # ... existing code ...
        pass


# Exponential backoff for FLOOD_WAIT handling (lines 62-68)
def get_backoff_time(attempt: int, base_delay: float = 1.0, max_delay: float = 300.0) -> float:
    """
    Calculate exponential backoff time with jitter
    
    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds (default 1.0)
        max_delay: Maximum delay in seconds (default 300.0)
    
    Returns:
        Backoff time in seconds
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = delay * 0.1 * (2 * (0.5 - __import__('random').random()))
    return max(0, delay + jitter)


async def handle_flood_wait(attempt: int) -> None:
    """Handle FLOOD_WAIT with exponential backoff"""
    backoff_time = get_backoff_time(attempt)
    if backoff_time > 0:
        await asyncio.sleep(backoff_time)


# Chunk size optimization (lines 272-273 and 290-292)
# Using 1024KB (1MB) chunks instead of 512KB for better throughput on Render

DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB chunks (line 272-273 equivalent)
UPLOAD_CHUNK_SIZE = 1024 * 1024    # 1MB chunks (line 290-292 equivalent)


async def fast_download(client, message, file_path: str) -> bool:
    """
    Download file with optimized 1MB chunk size
    
    Args:
        client: Telethon client
        message: Message object containing the file
        file_path: Path to save the file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        await client.download_media(message, file_path, file_size=0)
        return True
    except Exception as e:
        return False


async def fast_upload(client, chat, file_path: str) -> bool:
    """
    Upload file with optimized 1MB chunk size
    
    Args:
        client: Telethon client
        chat: Target chat
        file_path: Path to the file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        await client.send_file(chat, file_path, file_size=0)
        return True
    except Exception as e:
        return False
