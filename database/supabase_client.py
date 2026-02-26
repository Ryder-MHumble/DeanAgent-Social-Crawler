# -*- coding: utf-8 -*-
"""
Supabase client singleton for MediaCrawler.
Uses supabase-py async client to interact with Supabase (PostgreSQL).
"""

import asyncio
from typing import Optional

from supabase import create_client, Client

from config import supabase_config
from tools import utils


class SupabaseClient:
    """Singleton Supabase client."""

    _instance: Optional["SupabaseClient"] = None
    _client: Optional[Client] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _ensure_client(self) -> Client:
        if self._client is None:
            url = supabase_config.get("url", "")
            key = supabase_config.get("key", "")
            if not url or not key:
                raise ValueError(
                    "Supabase URL and Key must be set. "
                    "Set SUPABASE_URL and SUPABASE_KEY in .env file."
                )
            self._client = create_client(url, key)
            utils.logger.info("[SupabaseClient] Connected to Supabase")
        return self._client

    @property
    def client(self) -> Client:
        return self._ensure_client()


# Module-level convenience accessor
_sb_client = SupabaseClient()


def get_supabase() -> Client:
    """Get the singleton Supabase client."""
    return _sb_client.client
