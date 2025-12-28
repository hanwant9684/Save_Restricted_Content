# Save Restricted Content Bot

Project migrated to Replit environment.

## Overview
A Telegram bot to save restricted content from channels.

## Recent Changes
- Migrated project from external source to Replit.
- Integrated RichAds with direct API fetching.
- Integrated AdsGram as a fallback ad network.
- Implemented a unified `ad_manager` for fallback logic.
- Enhanced legal compliance with ad displays.

## Architecture
- `main.py`: Entry point for Telethon bot.
- `ad_manager.py`: Logic for choosing between RichAds and AdsGram.
- `richads.py`: Handler for RichAds XML API.
- `adsgram.py`: Handler for AdsGram API.
- `legal_acceptance.py`: Manages T&C/Privacy Policy acceptance.
- `database_sqlite.py`: Local persistent storage.
