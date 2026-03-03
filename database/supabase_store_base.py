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

    # Class-level dicts: shared across all instances of the same platform within one process.
    # StoreFactory.create_store() creates a new instance on every call, so class-level state
    # is the only way to persist data across calls within the same crawl session.

    # content_ids that passed the relevance filter (comments are saved only for these)
    _relevant_content_ids_by_platform: dict[str, Set[str]] = {}

    # content_ids already fully upserted this session OR already existing in DB
    # (dedup across keyword searches AND across sessions)
    _seen_content_ids_by_platform: dict[str, Set[str]] = {}

    # platforms for which we've already pre-loaded existing IDs from DB this session
    _preloaded_platforms: set = set()

    # Session counters: newly saved vs skipped vs filtered
    _new_content_by_platform: dict[str, int] = {}
    _new_comment_by_platform: dict[str, int] = {}
    _skipped_dedup_by_platform: dict[str, int] = {}
    _filtered_irrelevant_by_platform: dict[str, int] = {}
    _filtered_low_engagement_by_platform: dict[str, int] = {}
    _filtered_short_comment_by_platform: dict[str, int] = {}

    def __init__(self, platform: str):
        self.platform = platform
        if platform not in self._relevant_content_ids_by_platform:
            self._relevant_content_ids_by_platform[platform] = set()
        if platform not in self._seen_content_ids_by_platform:
            self._seen_content_ids_by_platform[platform] = set()
        if platform not in self._new_content_by_platform:
            self._new_content_by_platform[platform] = 0
        if platform not in self._new_comment_by_platform:
            self._new_comment_by_platform[platform] = 0
        if platform not in self._skipped_dedup_by_platform:
            self._skipped_dedup_by_platform[platform] = 0
        if platform not in self._filtered_irrelevant_by_platform:
            self._filtered_irrelevant_by_platform[platform] = 0
        if platform not in self._filtered_low_engagement_by_platform:
            self._filtered_low_engagement_by_platform[platform] = 0
        if platform not in self._filtered_short_comment_by_platform:
            self._filtered_short_comment_by_platform[platform] = 0

    @property
    def _relevant_content_ids(self) -> Set[str]:
        return self._relevant_content_ids_by_platform[self.platform]

    @property
    def _seen_content_ids(self) -> Set[str]:
        return self._seen_content_ids_by_platform[self.platform]

    # ------------------------------------------------------------------
    # Cross-session dedup: pre-load existing IDs from DB
    # ------------------------------------------------------------------
    async def _ensure_preloaded(self):
        """
        Pre-load existing content IDs from DB into in-memory sets (runs once per platform per session).

        This serves two purposes:
        1. Dedup: content already in DB won't be re-upserted on subsequent runs.
        2. Comment continuity: IDs pre-loaded into _relevant_content_ids allow comments
           for previously-saved content to be saved even on re-runs where that content
           is skipped.  This is the key fix for "comments only on first-run platforms".
        """
        if self.platform in self._preloaded_platforms:
            return

        self._preloaded_platforms.add(self.platform)
        try:
            sb = get_supabase()
            result = sb.table("contents").select("content_id").eq("platform", self.platform).execute()
            rows = result.data or []
            existing_ids = {str(row["content_id"]) for row in rows if row.get("content_id")}
            self._seen_content_ids.update(existing_ids)
            self._relevant_content_ids.update(existing_ids)
            utils.logger.info(
                f"[SupabaseStore] Preloaded {len(existing_ids)} existing {self.platform} content IDs "
                f"(dedup + comment continuity)"
            )
        except Exception as e:
            utils.logger.warning(
                f"[SupabaseStore] Failed to preload existing IDs for {self.platform}: {e}. "
                f"Dedup and comment continuity for this session may be incomplete."
            )

    # ------------------------------------------------------------------
    # Relevance filter
    # ------------------------------------------------------------------
    def _is_content_relevant(self, title: str, description: str) -> bool:
        """
        Check if content actually mentions the target entities.
        Returns True if filter is disabled or content matches.
        Returns False if content matches an exclusion keyword.
        """
        if not getattr(config, "ENABLE_RELEVANCE_FILTER", False):
            return True

        # Combine title + description for matching
        text = f"{title or ''} {description or ''}".lower()

        # Exclusion keywords take priority — reject if any match
        exclude_keywords = getattr(config, "RELEVANCE_EXCLUDE_KEYWORDS", [])
        for keyword in exclude_keywords:
            if keyword.lower() in text:
                return False

        must_contain = getattr(config, "RELEVANCE_MUST_CONTAIN", [])
        if not must_contain:
            return True

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

        content_id = str(content_id)

        # Pre-load existing IDs from DB on first call for this platform
        await self._ensure_preloaded()

        # Cross-session + within-session dedup: skip content already in DB or seen this run
        if content_id in self._seen_content_ids:
            utils.logger.debug(
                f"[SupabaseStore] SKIP (already in DB or processed this session) "
                f"{self.platform}/{content_id}"
            )
            # NOTE: content_id is also in _relevant_content_ids (set during preload or
            # first-time save), so its comments can still be saved.
            self._skipped_dedup_by_platform[self.platform] += 1
            return

        title = content_item.get("title", "")
        description = content_item.get("description", "")
        source_keyword = content_item.get("source_keyword", "")

        # Official account content: bypass relevance + engagement filters entirely.
        # source_keyword starts with "@" when content comes from a tracked official account.
        is_official_account = source_keyword.startswith("@")

        if not is_official_account:
            # Relevance filter: skip content that doesn't mention target entities
            if not self._is_content_relevant(title, description):
                utils.logger.info(
                    f"[SupabaseStore] SKIPPED (irrelevant) {self.platform}/{content_id}: "
                    f"{(title or description or '')[:60]}"
                )
                # Mark seen so we don't re-evaluate on future keyword searches
                self._seen_content_ids.add(content_id)
                self._filtered_irrelevant_by_platform[self.platform] += 1
                return

            # Engagement filter: skip low-quality / zero-interaction posts
            min_engagement = getattr(config, "MIN_CONTENT_ENGAGEMENT", 0)
            if min_engagement > 0:
                liked = int(content_item.get("liked_count", 0) or 0)
                comments = int(content_item.get("comment_count", 0) or 0)
                if liked + comments < min_engagement:
                    utils.logger.info(
                        f"[SupabaseStore] SKIPPED (low engagement: {liked}👍 {comments}💬) "
                        f"{self.platform}/{content_id}: {(title or description or '')[:50]}"
                    )
                    self._seen_content_ids.add(content_id)
                    self._filtered_low_engagement_by_platform[self.platform] += 1
                    return

        # Mark as seen and relevant
        self._seen_content_ids.add(content_id)
        self._relevant_content_ids.add(content_id)

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
        self._new_content_by_platform[self.platform] += 1
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

        # Pre-load ensures _relevant_content_ids contains IDs from previous runs
        await self._ensure_preloaded()

        # Only save comments belonging to content that passed the relevance filter
        # (either in this session or a previous one — both are in _relevant_content_ids)
        if getattr(config, "ENABLE_RELEVANCE_FILTER", False):
            parent_content_id = str(comment_item.get("content_id", ""))
            if parent_content_id and parent_content_id not in self._relevant_content_ids:
                utils.logger.debug(
                    f"[SupabaseStore] SKIP comment {self.platform}/{comment_id}: "
                    f"parent content {parent_content_id} not relevant"
                )
                return

        # Comment length filter: skip very short / spam comments
        min_len = getattr(config, "MIN_COMMENT_LENGTH", 0)
        if min_len > 0:
            comment_text = comment_item.get("content", "") or ""
            if len(comment_text.strip()) < min_len:
                utils.logger.debug(
                    f"[SupabaseStore] SKIP comment {self.platform}/{comment_id}: "
                    f"too short ({len(comment_text.strip())} chars)"
                )
                self._filtered_short_comment_by_platform[self.platform] += 1
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
        self._new_comment_by_platform[self.platform] += 1
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
    # Session summary
    # ------------------------------------------------------------------
    @classmethod
    def get_session_summary(cls, platform: str) -> dict:
        """Return a dict with new/skipped/filtered counts for a platform this session."""
        return {
            "new_content": cls._new_content_by_platform.get(platform, 0),
            "new_comment": cls._new_comment_by_platform.get(platform, 0),
            "skipped_dedup": cls._skipped_dedup_by_platform.get(platform, 0),
            "filtered_irrelevant": cls._filtered_irrelevant_by_platform.get(platform, 0),
            "filtered_low_engagement": cls._filtered_low_engagement_by_platform.get(platform, 0),
            "filtered_short_comment": cls._filtered_short_comment_by_platform.get(platform, 0),
        }

    @classmethod
    def print_session_summary(cls, platform: str) -> None:
        """Print a human-readable summary for a platform."""
        s = cls.get_session_summary(platform)
        platform_names = {
            "xhs": "小红书", "dy": "抖音", "bili": "Bilibili",
            "wb": "微博", "zhihu": "知乎", "ks": "快手", "tieba": "贴吧",
        }
        name = platform_names.get(platform, platform.upper())
        print(
            f"\n{'='*60}\n"
            f"[{name}] 本次爬取统计\n"
            f"  ✅ 新增内容:   {s['new_content']} 条\n"
            f"  💬 新增评论:   {s['new_comment']} 条\n"
            f"  ⏭️  已有跳过:   {s['skipped_dedup']} 条（数据库去重）\n"
            f"  🚫 无关过滤:   {s['filtered_irrelevant']} 条（不含目标关键词）\n"
            f"  📉 低互动过滤: {s['filtered_low_engagement']} 条（互动量不足）\n"
            f"  ✂️  短评过滤:   {s['filtered_short_comment']} 条（评论过短）\n"
            f"{'='*60}"
        )

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
