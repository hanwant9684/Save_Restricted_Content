# Telegram Save Restricted Content Bot

A Telegram bot that allows users to download restricted content from Telegram channels and chats.

## Overview

This bot is built with Telethon (Python Telegram client library) and provides:
- Download restricted content from Telegram channels
- Single and batch download capabilities
- User authentication via phone number
- Premium/free user tiers with download limits
- Ad monetization integration
- Cloud backup support

## Project Structure

- `main.py` - Main bot entry point with command handlers
- `config.py` - Configuration loading from environment variables
- `database_sqlite.py` - SQLite database for user data
- `helpers/` - Utility modules for downloads, files, messages
- `admin_commands.py` - Admin command handlers
- `access_control.py` - User access control and limits

## Required Environment Variables

The bot requires the following secrets to be configured:

**Required (Telegram API):**
- `API_ID` - Telegram API ID (get from https://my.telegram.org)
- `API_HASH` - Telegram API Hash
- `BOT_TOKEN` - Bot token from @BotFather
- `OWNER_ID` - Your Telegram user ID

**Optional:**
- `BOT_USERNAME` - Bot's username
- `SESSION_STRING` - Session string for user client
- `FORCE_SUBSCRIBE_CHANNEL` - Channel to force subscribe
- `DUMP_CHANNEL_ID` - Channel ID to dump files
- `ADMIN_USERNAME` - Admin username for support
- `PAYPAL_URL`, `UPI_ID`, `TELEGRAM_TON`, `CRYPTO_ADDRESS` - Payment options
- `RICHADS_PUBLISHER_ID`, `RICHADS_WIDGET_ID` - Ad configuration
- `GITHUB_TOKEN`, `GITHUB_BACKUP_REPO` - Cloud backup

## Running the Bot

The bot runs via the `telegram-bot` workflow which executes `python main.py`.

## Recent Changes

- 2026-01-15: Initial import to Replit environment
  - Installed Python 3.11
  - Installed all required packages from requirements.txt
