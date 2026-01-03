# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004

import secrets
from datetime import datetime, timedelta
from typing import Tuple, Optional
from logger import LOGGER
from database_sqlite import db

class PromoCodeManager:
    def __init__(self):
        LOGGER(__name__).info("PromoCodeManager initialized")
    
    def generate_code(self, length: int = 8) -> str:
        """Generate a random promo code"""
        return secrets.token_hex(length // 2).upper()
    
    def create_promo_code(self, days: int, max_users: int, created_by: int, expiration_date: Optional[str] = None) -> Tuple[bool, str]:
        """Create a new promo code"""
        try:
            code = self.generate_code()
            
            # Ensure code doesn't exist
            while db.get_promo_code(code):
                code = self.generate_code()
            
            success = db.create_promo_code(code, days, max_users, created_by, expiration_date)
            
            if success:
                LOGGER(__name__).info(f"Created promo code {code} - {days} days, max {max_users} users")
                return True, code
            else:
                return False, "Failed to create promo code"
        except Exception as e:
            LOGGER(__name__).error(f"Error creating promo code: {e}")
            return False, str(e)
    
    def validate_and_apply(self, code: str, user_id: int) -> Tuple[bool, str]:
        """Validate and apply promo code to user"""
        try:
            # Validate
            is_valid, message = db.validate_promo_code(code, user_id)
            
            if not is_valid:
                return False, message
            
            # Apply
            success = db.apply_promo_code(code, user_id)
            
            if success:
                promo = db.get_promo_code(code)
                user = db.get_user(user_id)
                if not promo:
                    return False, "❌ Failed to retrieve promo code details."
                days = promo['days_of_premium']
                end_date = user['subscription_end'] if user else "Unknown"
                return True, f"✅ **Promo code applied!**\n\n🎁 **+{days} days** of premium access\n📅 **Expires:** `{end_date}`"
            else:
                return False, "❌ Failed to apply promo code."
        except Exception as e:
            LOGGER(__name__).error(f"Error validating/applying promo code: {e}")
            return False, "❌ Error applying promo code."
    
    def get_promo_stats(self) -> str:
        """Get formatted promo code statistics"""
        try:
            codes = db.list_promo_codes()
            
            if not codes:
                return "No active promo codes."
            
            stats = "**🎁 Active Promo Codes:**\n\n"
            
            for code in codes:
                remaining = code['max_users'] - code['usage_count']
                expires = code['expiration_date'] or "Never"
                stats += (
                    f"• **Code:** `{code['code']}`\n"
                    f"  ├ Duration: `{code['days_of_premium']} days`\n"
                    f"  ├ Usage: `{code['usage_count']}/{code['max_users']}` (remaining: {remaining})\n"
                    f"  └ Expires: `{expires}`\n\n"
                )
            
            return stats
        except Exception as e:
            LOGGER(__name__).error(f"Error getting promo stats: {e}")
            return "Error fetching promo codes."

promo_manager = PromoCodeManager()
