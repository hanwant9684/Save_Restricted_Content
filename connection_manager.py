from telethon import TelegramClient
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer
from logger import LOGGER

def get_download_connections(file_size: int) -> int:
    """Optimized connections for downloading."""
    if file_size >= 100 * 1024 * 1024:  # > 100MB
        return 1
    elif file_size >= 20 * 1024 * 1024:  # > 20MB
        return 1
    return 1

def get_upload_connections(file_size: int) -> int:
    """Optimized connections for uploading."""
    if file_size >= 100 * 1024 * 1024:  # > 100MB
        return 6
    elif file_size >= 50 * 1024 * 1024:  # > 50MB
        return 4
    elif file_size >= 10 * 1024 * 1024:  # > 10MB
        return 4
    return 6

async def download_file_optimized(client: TelegramClient, location, out, progress_callback=None, file_size=None, connection_count=None):
    """
    Optimized download using ParallelTransferrer logic.
    """
    size = file_size or getattr(location, 'size', 0)
    conn_count = connection_count or get_download_connections(size)
    return await fast_download(client, location, out, progress_callback, file_size, conn_count)

async def upload_file_optimized(client: TelegramClient, file, progress_callback=None, connection_count=None):
    """
    Optimized upload using ParallelTransferrer logic.
    """
    file_size = 0
    try:
        import os
        file_size = os.path.getsize(file.name)
    except:
        pass
    conn_count = connection_count or get_upload_connections(file_size)
    return await fast_upload(client, file, progress_callback, conn_count)
