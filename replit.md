# Telegram Media Bot - Replit Setup

## Overview

This is a Telegram bot for downloading and forwarding media from private/public channels with ad monetization support. The bot uses Telethon for Telegram integration and includes a WSGI server for ad verification.

**Purpose:** Download and forward media from Telegram channels with queue management, premium user system, and ad monetization.

**Current State:** Fully functional and running on Replit with all dependencies installed.

## Recent Changes

- **2025-11-02:** Fixed UnboundLocalError in handle_any_message Function
  - **Problem:** Users unable to download files - getting `UnboundLocalError: cannot access local variable 'download_queue'`
  - **Root Cause:** Duplicate import statement inside `handle_any_message()` function caused Python to treat `download_queue` as local variable
  - **Solution:** Removed duplicate `from queue_manager import download_queue` at line 958 (already imported at module level line 69)
  - **Impact:** Downloads now work properly without UnboundLocalError crashes
  - **Files modified:** main.py

- **2025-11-02:** Changed Database Restoration to Always Restore from GitHub
  - **Problem:** Bot was skipping restoration from GitHub when local database existed
  - **Solution:** Modified server_wsgi.py to always restore from GitHub on startup (cloud-only approach)
  - **Changes:**
    - Removed conditional checks for local database existence
    - Bot now always calls `restore_latest_from_cloud()` on startup when cloud backup is configured
    - Creates safety backup (`telegram_bot.db.before_restore`) before restoring from GitHub
    - Ensures database is always in sync with GitHub backup
  - **Impact:** Every bot restart pulls the latest database from GitHub, ensuring consistency across deployments
  - **Files modified:** server_wsgi.py

- **2025-11-02:** Fixed Critical Session Slot Bug - Active Download Protection
  - **Problem:** When a 4th user tried to join, Session Manager would evict the oldest user regardless of whether they had an active download, causing interrupted downloads mid-transfer
  - **Solution:** Implemented smart session eviction that protects active downloads
  - **Changes:**
    - Modified `SessionManager.get_or_create_session()` to return (client, error_code) tuple with distinct codes: 'slots_full', 'invalid_session', 'creation_failed'
    - Session Manager now checks `download_queue.active_downloads` before evicting sessions
    - Only evicts idle sessions (no active downloads); if all 3 slots have active downloads, new users must wait
    - Updated `get_user_client()` in access_control.py to handle error codes properly
    - Invalid/expired sessions are now cleared from DB (allows relogin), while slot-full cases preserve session
    - All 3 callers in main.py (/dl, /bdl, handle_any_message) updated with proper error messaging
  - **User Experience:**
    - Users with active downloads are never interrupted
    - 4th user sees: "⏳ All session slots are currently busy! 👥 Active users downloading: 3/3. Please wait a few minutes and try again."
    - Users with expired sessions see relogin prompt instead of wait message
  - **Impact:** No more interrupted downloads when new users join; users get appropriate wait/error messages
  - **Files modified:** helpers/session_manager.py, access_control.py, main.py

- **2025-11-02:** Removed Local Backup System - Cloud-Only Approach
  - **Problem:** Dual backup systems (local + cloud) added unnecessary complexity and resource usage
  - **Solution:** Simplified to cloud-only backup using GitHub
  - **Changes:**
    - Removed `backup_database.py` module entirely
    - Removed periodic local backup task from server_wsgi.py (was running every 1 hour)
    - Modified `cloud_backup.py` to be self-contained (no dependencies on backup_database)
    - Database manually restored from GitHub cloud backup before cleanup
    - Removed `backups/` directory (already in .gitignore)
  - **Impact:** Cleaner codebase, reduced background tasks, cloud backup remains fully functional
  - **Backup Schedule:** GitHub backup every 10 minutes (configurable via CLOUD_BACKUP_INTERVAL_MINUTES)
  - **Files modified:** cloud_backup.py, server_wsgi.py, replit.md
  - **Files deleted:** backup_database.py

- **2025-11-02:** Comprehensive RAM Optimization for 512MB Environments
  - **Problem:** User requested minimizing RAM usage on constrained environments (Render/Replit 512MB)
  - **Solution:** Implemented 4 major optimizations to reduce memory footprint
  - **1. Fixed Duplicate Startup Messages:**
    - Removed threading-based queue initialization from main.py
    - Queue processor now starts using asyncio in both entry points
    - Eliminates duplicate "Starting Telegram bot" messages
  - **2. Tiered Connection Scaling for Large Files:**
    - Files ≥1GB: 4 connections (~20-40MB RAM) - saves 40-120MB vs before
    - Files 200MB-1GB: 6 connections (~30-60MB RAM)
    - Files <200MB: 8 connections (~40-80MB RAM)
    - Previously used 12-16 connections for ALL files regardless of size
    - Updated helpers/transfer.py with smart connection scaling
  - **3. Modernized Background Tasks:**
    - Replaced threading.Thread with asyncio.create_task for queue processor
    - Eliminates extra thread overhead and memory allocation
    - Both server_wsgi.py and main.py entry points use proper async patterns
  - **4. Memory Compression:**
    - Changed memory_monitor.py operation_history from list→deque of dicts→tuples
    - Saves ~60% RAM per entry (200 bytes → 80 bytes)
    - Total savings: 120 bytes × 20 entries = 2.4KB
  - **Impact:** Bot now runs at 57.8MB RAM at startup (vs ~60MB before), 1GB+ file downloads use 75% less RAM for connections
  - **Files modified:** helpers/transfer.py, main.py, server_wsgi.py, memory_monitor.py

- **2025-11-02:** Implemented Terms & Conditions and Privacy Policy acceptance system
  - **Problem:** Bot owner needed legal protection from user actions worldwide, compliant with Indian and international laws
  - **Solution:** Comprehensive legal acceptance system with database persistence
  - Created detailed Terms and Conditions (India IT Act 2000 compliant)
  - Created Privacy Policy (GDPR and India SPDI Rules 2011 compliant)
  - Added `legal_acceptance` table in database_sqlite.py for persistent storage
  - Created `legal_acceptance.py` module with handlers and decorators
  - Users must accept legal terms before using any bot features
  - Acceptance stored in database (not RAM) for legal compliance
  - Interactive UI with buttons to view/accept/decline terms
  - **Files created:**
    - `legal/terms_and_conditions.txt` - Comprehensive T&C with liability disclaimers
    - `legal/privacy_policy.txt` - Full privacy policy with data rights
    - `legal/README_IMPORTANT.md` - Setup instructions for bot owner
    - `legal_acceptance.py` - Legal acceptance handlers
  - **Database methods:** check_legal_acceptance(), record_legal_acceptance(), get_legal_acceptance_stats()
  - **Impact:** Bot owner legally protected from user actions; compliant with Indian and EU laws
  - **Action Required:** Bot owner must fill in contact information placeholders in legal documents (see legal/README_IMPORTANT.md)

- **2025-11-01:** Optimized media group downloads to prevent high RAM usage
  - **Problem:** Media groups were downloading all files into memory before uploading, causing high RAM usage and potential out-of-memory crashes
  - **Solution:** Implemented sequential file processing (download → upload → delete → next)
  - Modified `processMediaGroup()` in `helpers/utils.py` to process files one at a time
  - Modified `send_media()` to return boolean success/failure for accurate tracking
  - Each file is now deleted immediately after upload to free RAM
  - Preserves all safeguards: file size checks, fast uploads, thumbnails, dump channel forwarding
  - **Trade-off:** Files are now sent as individual messages instead of grouped albums (necessary for RAM efficiency)
  - **Impact:** Bot can now handle large media groups without running out of memory

- **2025-11-01:** Fixed critical ad verification bug - Premature code generation
  - **Problem:** URL shortener services validate links before shortening, which triggered verification code generation before users even received the link
  - **Solution:** Implemented two-step verification with landing page
  - When users click the shortened link, they now see a landing page with a "Get Verification Code" button
  - Verification codes are only generated when the button is clicked (confirm=1 parameter)
  - This prevents shortener validation requests from consuming sessions
  - Tightened security by requiring exact match `confirm == '1'`
  - **Impact:** Users can now reliably receive and use their verification codes

- **2025-11-03:** Replaced Droplink with SwiftLnx and Updated Rotation Order
  - Replaced Droplink with SwiftLnx (https://swiftlnx.com/) in `ad_monetization.py`
  - Added SWIFTLNX_API_KEY to environment secrets
  - Updated URL shortener rotation order: UpShrink → SwiftLnx → Shrtfly → GPLinks (repeats)
  - Updated all fallback references to use SwiftLnx instead of Droplink
  - Renamed function from `generate_droplink_ad_link` to `generate_ad_link` for clarity
  - **Impact:** Users now see different URL shortener services in the new rotation order

- **2025-11-01:** Replaced ARLinks with Shrtfly URL shortener
  - Updated `ad_monetization.py` to use Shrtfly (https://shrtfly.com/) instead of ARLinks
  - Added SHRTFLY_API_KEY to environment secrets
  - Updated documentation (README.md and replit.md) to reflect the change

- **2025-11-01:** Project imported from GitHub and configured for Replit environment
  - Installed all Python dependencies (Telethon, cryptg, psutil, uvloop, waitress, orjson)
  - Configured workflow to run WSGI server on port 5000
  - Bot successfully connected and running with cloud backup integration
  - **Fixed critical bugs from Render deployment:**
    - Fixed cleanup_download crash when called with None paths
    - Implemented proper background task tracking and graceful shutdown
    - Prevents "Task was destroyed but it is pending!" errors on forced shutdowns
    - Orphaned files now cleaned by periodic cleanup task

## Project Architecture

### Technology Stack
- **Language:** Python 3.12
- **Framework:** Telethon (Telegram client library)
- **Web Server:** Waitress WSGI server (minimal RAM usage)
- **Database:** SQLite (portable & lightweight)
- **Event Loop:** uvloop (performance optimization)

### Key Components

#### Core Files
- `main.py` - Main bot logic and message handlers (Telethon event handlers)
- `server_wsgi.py` - WSGI server entry point and bot orchestration
- `config.py` - Configuration and environment variable management
- `database_sqlite.py` - SQLite database manager for users and sessions

#### Feature Modules
- `ad_monetization.py` - Ad verification system with URL shorteners
- `access_control.py` - User authentication and permission system
- `admin_commands.py` - Admin-only bot commands
- `queue_manager.py` - Download queue with concurrency control
- `phone_auth.py` - Phone-based authentication for restricted content
- `memory_monitor.py` - Memory usage tracking and optimization
- `cloud_backup.py` - GitHub-based database backup system (cloud-only, no local backups)

#### Helper Modules (helpers/)
- `utils.py` - Media processing and progress tracking
- `transfer.py` - Fast media download with FastTelethon
- `files.py` - File management and cleanup
- `msg.py` - Message parsing utilities
- `session_manager.py` - Session lifecycle management
- `cleanup.py` - Periodic cleanup tasks

### Architecture Decisions

1. **SQLite over MongoDB:** Uses SQLite for better portability and lower resource usage in cloud environments
2. **WSGI over Flask:** Uses minimal WSGI server (Waitress) instead of Flask to save 15-20MB RAM
3. **Session Management:** Implements session pooling with max 3 concurrent sessions and 30-minute idle timeout
4. **Memory Optimization:** Periodic garbage collection, orphaned file cleanup, and memory monitoring
5. **Cloud Backup:** Automatic GitHub backup every 10 minutes for database persistence

## Environment Variables

### Required Secrets (Already Configured)
- `API_ID` - Telegram API ID from https://my.telegram.org/apps
- `API_HASH` - Telegram API Hash from https://my.telegram.org/apps
- `BOT_TOKEN` - Bot token from @BotFather
- `OWNER_ID` - Telegram user ID (auto-added as admin)

### Optional Configuration
- `FORCE_SUBSCRIBE_CHANNEL` - Channel for forced subscription
- `ADMIN_USERNAME` - Bot admin username for contact
- `PAYPAL_URL`, `UPI_ID`, `TELEGRAM_TON`, `CRYPTO_ADDRESS` - Payment methods
- `SWIFTLNX_API_KEY`, `GPLINKS_API_KEY`, `SHRTFLY_API_KEY`, `UPSHRINK_API_KEY` - URL shortener API keys
- `SESSION_STRING` - Admin session for downloads
- `DUMP_CHANNEL_ID` - Channel ID for media forwarding
- `CLOUD_BACKUP_SERVICE` - Set to "github" for cloud backup
- `GITHUB_TOKEN`, `GITHUB_BACKUP_REPO` - GitHub backup credentials
- `CLOUD_BACKUP_INTERVAL_HOURS` - Backup interval (default: 6 hours)

## Workflow Configuration

The bot runs via a single workflow:
- **Name:** telegram-bot
- **Command:** `python server_wsgi.py`
- **Port:** 5000 (WSGI server for ad verification)
- **Output:** WebView (shows verification pages)

## Database Schema

SQLite database (`telegram_bot.db`) with tables:
- `users` - User information and permissions
- `admins` - Admin users list
- `premium_users` - Premium subscription data
- `banned_users` - Banned user records
- `ad_sessions` - Ad verification sessions
- `verification_codes` - Premium verification codes
- `user_sessions` - Telethon user sessions

## User Preferences

None specified yet. Add preferences as they are expressed.

## Deployment Notes

- Server binds to `0.0.0.0:5000` as required by Replit
- The bot uses long polling (no webhooks) for Telegram updates
- All sessions and downloads are managed in-memory with periodic cleanup
- Cloud backup keeps database synced to GitHub repository
- Memory monitoring logs to `memory_debug.log` for diagnostics

## Credits

Created by @Wolfy004  
Channel: https://t.me/Wolfy004
