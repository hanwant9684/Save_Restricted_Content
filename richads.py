# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# RichAds Telegram Bot Integration

import os
import html
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List
from time import time
from logger import LOGGER
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup

RICHADS_API_URL = "http://15068.xml.adx1.com/telegram-mb"

class RichAdsManager:
    def __init__(self):
        self.publisher_id = os.getenv("RICHADS_PUBLISHER_ID", "")
        self.widget_id = os.getenv("RICHADS_WIDGET_ID", "")
        self.production = os.getenv("RICHADS_PRODUCTION", "true").lower() == "true"
        self.session = None
        self.last_ad_time = {}  # Track last ad time per user to avoid rate limiting
        self.ad_cooldown = 300  # 5 minutes cooldown between ads per user
        
        if self.publisher_id:
            LOGGER(__name__).info(f"RichAds initialized - Publisher: {self.publisher_id}")
        else:
            LOGGER(__name__).warning("RichAds not configured - RICHADS_PUBLISHER_ID not set")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create persistent aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        """Close the session"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def is_enabled(self) -> bool:
        """Check if RichAds is configured"""
        return bool(self.publisher_id)
    
    def _check_cooldown(self, user_id: int) -> bool:
        """Check if user is within ad cooldown period"""
        now = time()
        last_time = self.last_ad_time.get(user_id, 0)
        if now - last_time >= self.ad_cooldown:
            self.last_ad_time[user_id] = now
            return True
        return False
    
    async def fetch_ad(self, language_code: str = "en", telegram_id: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Fetch ad from RichAds API with rate limiting"""
        if not self.is_enabled():
            return None
        
        # Check rate limit
        if telegram_id and not self._check_cooldown(int(telegram_id)):
            LOGGER(__name__).debug(f"RichAds: Ad cooldown active for user {telegram_id}")
            return None
            
        payload = {
            "language_code": language_code[:2].lower() if language_code else "en",
            "publisher_id": self.publisher_id,
            "production": self.production
        }
        
        if self.widget_id:
            payload["widget_id"] = self.widget_id
        if telegram_id:
            payload["telegram_id"] = str(telegram_id)
        
        try:
            session = await self._get_session()
            LOGGER(__name__).info(f"RichAds: Fetching ads from API for user {telegram_id} (lang: {language_code})")
            
            async with session.post(
                RICHADS_API_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "TelegramBot/1.0"}
            ) as response:
                if response.status == 200:
                    ads = await response.json()
                    if ads and len(ads) > 0:
                        LOGGER(__name__).info(f"✅ RichAds: Received {len(ads)} ad(s) for user {telegram_id}")
                        return ads
                    else:
                        LOGGER(__name__).info(f"⚠️ RichAds: No ads available right now for user {telegram_id} - RichAds server has no ads in inventory")
                        return None
                else:
                    LOGGER(__name__).warning(f"❌ RichAds: API error {response.status} for user {telegram_id}")
                    return None
        except asyncio.TimeoutError:
            LOGGER(__name__).warning(f"❌ RichAds: API timeout while fetching ads for user {telegram_id}")
            return None
        except Exception as e:
            LOGGER(__name__).error(f"❌ RichAds: Fetch error for user {telegram_id}: {e}")
            return None
    
    async def notify_impression(self, notification_url: str) -> bool:
        """Notify RichAds that ad impression happened (async, no wait)"""
        if not notification_url:
            return False
        
        try:
            session = await self._get_session()
            # Fire and forget with asyncio.create_task to avoid blocking
            asyncio.create_task(self._notify_impression_task(session, notification_url))
            return True
        except Exception as e:
            LOGGER(__name__).error(f"RichAds impression error: {e}")
            return False
    
    async def _notify_impression_task(self, session: aiohttp.ClientSession, notification_url: str):
        """Background task to notify impressions"""
        try:
            async with session.get(
                notification_url,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                if response.status != 200:
                    LOGGER(__name__).debug(f"RichAds impression notification failed: {response.status}")
        except Exception as e:
            LOGGER(__name__).debug(f"RichAds impression task error: {e}")
    
    async def send_ad_to_user(self, client, chat_id: int, language_code: str = "en") -> bool:
        """Fetch and send RichAd to user as photo message"""
        if not self.is_enabled():
            return False
        
        # Check cooldown before fetching
        if not self._check_cooldown(chat_id):
            return False
            
        ads = await self.fetch_ad(language_code=language_code, telegram_id=str(chat_id))
        
        if not ads or len(ads) == 0:
            LOGGER(__name__).info(f"⚠️ RichAds: User {chat_id} requested ad but no ads available at this moment - user will NOT see ad")
            return False
        
        ad = ads[0]  # Use first ad
        
        try:
            # Decode HTML entities in URLs (RichAds returns &amp; instead of &)
            notification_url = ad.get("notification_url", "")
            if notification_url:
                notification_url = html.unescape(notification_url)
            
            click_url = ad.get("link", "")
            if click_url:
                click_url = html.unescape(click_url)
            else:
                # Skip if no click URL
                return False
            
            image_url = ad.get("image") or ad.get("image_preload")
            if image_url:
                image_url = html.unescape(image_url)
            
            # Build caption from title and message
            caption = "📢 **Sponsored**\n\n"
            if ad.get("title"):
                caption += f"**{ad['title']}**\n"
            if ad.get("message"):
                caption += ad["message"]
            if ad.get("brand"):
                caption += f"\n\n🏷️ {ad['brand']}"
            
            # Build inline button
            button_text = ad.get("button", "Learn More")
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton.url(f"👉 {button_text}", click_url)]
            ])
            
            if image_url:
                await client.send_file(
                    chat_id,
                    file=image_url,
                    caption=caption,
                    buttons=buttons.to_telethon(),
                    parse_mode='md'
                )
            else:
                # Fallback to text message if no image
                await client.send_message(
                    chat_id,
                    caption,
                    buttons=buttons.to_telethon(),
                    parse_mode='md'
                )
            
            # Notify impression asynchronously (non-blocking)
            if notification_url:
                await self.notify_impression(notification_url)
            
            ad_title = ad.get("title", "Ad")
            LOGGER(__name__).info(f"✅ RichAds: Ad '{ad_title}' successfully sent to user {chat_id}")
            return True
            
        except Exception as e:
            LOGGER(__name__).error(f"Error sending RichAd to {chat_id}: {e}")
            return False


# Global instance
richads = RichAdsManager()
