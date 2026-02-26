# -*- coding: utf-8 -*-
"""
Supabase store base class for MediaCrawler.
Provides upsert operations for the unified schema (contents, comments, creators).
Each platform store inherits this and provides platform-specific field mapping.
"""

from typing import Dict, Optional, Set

import config
from database.supabase_client import get_supabase
from tools import utils
from tools.time_util import get_current_timestamp


class SupabaseStoreBase:
    """
    Base class for Supabase storage.

    Subclasses set `self.platform` (e.g. "xhs", "dy", "bili") and call
    the save_content / save_comment / save_creator methods with a dict
    that maps common fields + platform_data JSONB.
    """

    # Class-level dict: share relevant content IDs across all instances of the same platform.
    # This is needed because StoreFactory.create_store() creates a new instance on every call,
    # but we need to remember which content_ids passed the relevance filter so their comments
    # can also be saved.
    _relevant_content_ids_by_platform: dict[str, Set[str]] = {}

    def __init__(self, platform: str):
        self.platform = platform
        if platform not in self._relevant_content_ids_by_platform:
            self._relevant_content_ids_by_platform[platform] = set()

    @property
    def _relevant_content_ids(self) -> Set[str]:
        return self._relevant_content_ids_by_platform[self.platform]

    # ------------------------------------------------------------------
    # Relevance filter
    # ------------------------------------------------------------------
    def _is_content_relevant(self, title: str, description: str) -> bool:
        """
        Check if content actually mentions the target entities.
        Returns True if filter is disabled or content matches.
        """
        if not getattr(config, "ENABLE_RELEVANCE_FILTER", False):
            return True

        must_contain = getattr(config, "RELEVANCE_MUST_CONTAIN", [])
        if not must_contain:
            return True

        # Combine title + description for matching
        text = f"{title or ''} {description or ''}".lower()
        for keyword in must_contain:
            if keyword.lower() in text:
                return True

        return False

    # ------------------------------------------------------------------
    # Content (posts / videos / notes / articles)
    # ------------------------------------------------------------------
    async def save_content(self, content_item: Dict):
        """Upsert a content row into the unified `contents` table."""
        content_id = content_item.get("content_id")
        if not content_id:
            return

        # Relevance filter: skip content that doesn't mention target entities
        title = content_item.get("title", "")
        description = content_item.get("description", "")
        if not self._is_content_relevant(title, description):
            utils.logger.info(
                f"[SupabaseStore] SKIPPED (irrelevant) {self.platform}/{content_id}: "
                f"{(title or description or '')[:60]}"
            )
            return

        # Mark this content_id as relevant so its comments will also be saved
        self._relevant_content_ids.add(str(content_id))

        sb = get_supabase()
        now_ts = int(get_current_timestamp())

        row = {
            "platform": self.platform,
            "content_id": str(content_id),
            "content_type": content_item.get("content_type", ""),
            "title": content_item.get("title", ""),
            "description": content_item.get("description", ""),
            "content_url": content_item.get("content_url", ""),
            "cover_url": content_item.get("cover_url", ""),
            "user_id": str(content_item.get("user_id", "")),
            "nickname": content_item.get("nickname", ""),
            "avatar": content_item.get("avatar", ""),
            "ip_location": content_item.get("ip_location", ""),
            "liked_count": int(content_item.get("liked_count", 0) or 0),
            "comment_count": int(content_item.get("comment_count", 0) or 0),
            "share_count": int(content_item.get("share_count", 0) or 0),
            "collected_count": int(content_item.get("collected_count", 0) or 0),
            "platform_data": content_item.get("platform_data", {}),
            "source_keyword": content_item.get("source_keyword", ""),
            "publish_time": content_item.get("publish_time"),
            "last_modify_ts": now_ts,
        }

        # Upsert: on conflict(platform, content_id) update engagement metrics
        result = (
            sb.table("contents")
            .upsert(row, on_conflict="platform,content_id")
            .execute()
        )
        utils.logger.info(
            f"[SupabaseStore] Upserted content {self.platform}/{content_id}"
        )
        return result

    # ------------------------------------------------------------------
    # Comment
    # ------------------------------------------------------------------
    async def save_comment(self, comment_item: Dict):
        """Upsert a comment row into the unified `comments` table."""
        comment_id = comment_item.get("comment_id")
        if not comment_id:
            return

        # Only save comments belonging to content that passed the relevance filter
        if getattr(config, "ENABLE_RELEVANCE_FILTER", False):
            parent_content_id = str(comment_item.get("content_id", ""))
            if parent_content_id and parent_content_id not in self._relevant_content_ids:
                return

        sb = get_supabase()
        now_ts = int(get_current_timestamp())

        row = {
            "platform": self.platform,
            "comment_id": str(comment_id),
            "content_id": str(comment_item.get("content_id", "")),
            "parent_comment_id": str(comment_item.get("parent_comment_id", "")),
            "content": comment_item.get("content", ""),
            "pictures": comment_item.get("pictures", ""),
            "user_id": str(comment_item.get("user_id", "")),
            "nickname": comment_item.get("nickname", ""),
            "avatar": comment_item.get("avatar", ""),
            "ip_location": comment_item.get("ip_location", ""),
            "like_count": int(comment_item.get("like_count", 0) or 0),
            "dislike_count": int(comment_item.get("dislike_count", 0) or 0),
            "sub_comment_count": int(comment_item.get("sub_comment_count", 0) or 0),
            "platform_data": comment_item.get("platform_data", {}),
            "publish_time": comment_item.get("publish_time"),
            "last_modify_ts": now_ts,
        }

        result = (
            sb.table("comments")
            .upsert(row, on_conflict="platform,comment_id")
            .execute()
        )
        utils.logger.info(
            f"[SupabaseStore] Upserted comment {self.platform}/{comment_id}"
        )
        return result

    # ------------------------------------------------------------------
    # Creator
    # ------------------------------------------------------------------
    async def save_creator(self, creator_item: Dict):
        """Upsert a creator row into the unified `creators` table."""
        user_id = creator_item.get("user_id")
        if not user_id:
            return

        sb = get_supabase()
        now_ts = int(get_current_timestamp())

        row = {
            "platform": self.platform,
            "user_id": str(user_id),
            "nickname": creator_item.get("nickname", ""),
            "avatar": creator_item.get("avatar", ""),
            "description": creator_item.get("description", ""),
            "gender": creator_item.get("gender", ""),
            "ip_location": creator_item.get("ip_location", ""),
            "follows_count": int(creator_item.get("follows_count", 0) or 0),
            "fans_count": int(creator_item.get("fans_count", 0) or 0),
            "interaction_count": int(creator_item.get("interaction_count", 0) or 0),
            "platform_data": creator_item.get("platform_data", {}),
            "last_modify_ts": now_ts,
        }

        result = (
            sb.table("creators")
            .upsert(row, on_conflict="platform,user_id")
            .execute()
        )
        utils.logger.info(
            f"[SupabaseStore] Upserted creator {self.platform}/{user_id}"
        )
        return result

    # ------------------------------------------------------------------
    # Bilibili-specific: contacts
    # ------------------------------------------------------------------
    async def save_bilibili_contact(self, contact_item: Dict):
        """Upsert into bilibili_contacts table (Bilibili only)."""
        sb = get_supabase()
        now_ts = int(get_current_timestamp())

        row = {
            "up_id": str(contact_item.get("up_id", "")),
            "fan_id": str(contact_item.get("fan_id", "")),
            "up_name": contact_item.get("up_name", ""),
            "fan_name": contact_item.get("fan_name", ""),
            "up_sign": contact_item.get("up_sign", ""),
            "fan_sign": contact_item.get("fan_sign", ""),
            "up_avatar": contact_item.get("up_avatar", ""),
            "fan_avatar": contact_item.get("fan_avatar", ""),
            "last_modify_ts": now_ts,
        }

        result = (
            sb.table("bilibili_contacts")
            .upsert(row, on_conflict="up_id,fan_id")
            .execute()
        )
        return result

    # ------------------------------------------------------------------
    # Bilibili-specific: dynamics
    # ------------------------------------------------------------------
    async def save_bilibili_dynamic(self, dynamic_item: Dict):
        """Upsert into bilibili_dynamics table (Bilibili only)."""
        sb = get_supabase()
        now_ts = int(get_current_timestamp())

        row = {
            "dynamic_id": str(dynamic_item.get("dynamic_id", "")),
            "user_id": str(dynamic_item.get("user_id", "")),
            "user_name": dynamic_item.get("user_name", ""),
            "text": dynamic_item.get("text", ""),
            "type": dynamic_item.get("type", ""),
            "pub_ts": dynamic_item.get("pub_ts"),
            "total_comments": int(dynamic_item.get("total_comments", 0) or 0),
            "total_forwards": int(dynamic_item.get("total_forwards", 0) or 0),
            "total_liked": int(dynamic_item.get("total_liked", 0) or 0),
            "last_modify_ts": now_ts,
        }

        result = (
            sb.table("bilibili_dynamics")
            .upsert(row, on_conflict="dynamic_id")
            .execute()
        )
        return result
