-- =============================================================================
-- MediaCrawler Supabase Schema Migration
-- =============================================================================
-- Unified schema: 3 core tables with `platform` discriminator + JSONB
-- for platform-specific data. Designed for cross-platform backend queries.
-- =============================================================================

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- 1. crawl_tasks — Track crawl sessions
-- =============================================================================
CREATE TABLE IF NOT EXISTS crawl_tasks (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    platform        TEXT NOT NULL,           -- xhs | dy | bili | wb | ks | tieba | zhihu
    crawler_type    TEXT NOT NULL,           -- search | detail | creator
    keywords        TEXT,                    -- comma-separated search keywords
    status          TEXT DEFAULT 'running',  -- running | completed | failed
    notes_count     INTEGER DEFAULT 0,
    comments_count  INTEGER DEFAULT 0,
    creators_count  INTEGER DEFAULT 0,
    started_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_crawl_tasks_platform ON crawl_tasks(platform);
CREATE INDEX idx_crawl_tasks_status ON crawl_tasks(status);

-- =============================================================================
-- 2. contents — Unified content table (posts, videos, notes, articles, etc.)
-- =============================================================================
-- Common fields extracted across all 7 platforms, platform-specific fields in JSONB.
--
-- platform_data examples:
--   XHS:      {"video_url":"...", "image_list":"...", "tag_list":"...", "xsec_token":"..."}
--   Douyin:   {"aweme_type":"...", "video_download_url":"...", "music_download_url":"...", "sec_uid":"..."}
--   Bilibili: {"video_play_count":1000, "video_coin_count":50, "video_danmaku":200, "video_cover_url":"..."}
--   Weibo:    {"profile_url":"...", "note_url":"..."}
--   Kuaishou: {"video_url":"...", "video_cover_url":"...", "video_play_url":"...", "viewd_count":500}
--   Tieba:    {"tieba_name":"...", "tieba_link":"...", "total_replay_num":10, "total_replay_page":2}
--   Zhihu:    {"content_type":"answer", "question_id":"...", "content_text":"...", "voteup_count":100}
-- =============================================================================
CREATE TABLE IF NOT EXISTS contents (
    id                  BIGSERIAL PRIMARY KEY,
    platform            TEXT NOT NULL,
    content_id          TEXT NOT NULL,           -- platform-specific content ID (note_id / aweme_id / video_id / etc.)
    content_type        TEXT,                    -- video | note | article | answer | post | thread

    -- Content data
    title               TEXT,
    description         TEXT,
    content_url         TEXT,
    cover_url           TEXT,

    -- Author info (denormalized for fast queries)
    user_id             TEXT,
    nickname            TEXT,
    avatar              TEXT,
    ip_location         TEXT,

    -- Engagement metrics (common columns, queryable)
    liked_count         INTEGER DEFAULT 0,
    comment_count       INTEGER DEFAULT 0,
    share_count         INTEGER DEFAULT 0,
    collected_count     INTEGER DEFAULT 0,

    -- Platform-specific data
    platform_data       JSONB DEFAULT '{}',

    -- Crawl metadata
    source_keyword      TEXT DEFAULT '',
    crawl_task_id       UUID REFERENCES crawl_tasks(id) ON DELETE SET NULL,

    -- Timestamps
    publish_time        BIGINT,                  -- original platform timestamp
    add_ts              BIGINT,                  -- first crawled timestamp
    last_modify_ts      BIGINT,                  -- last updated timestamp
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),

    UNIQUE(platform, content_id)
);

CREATE INDEX idx_contents_platform ON contents(platform);
CREATE INDEX idx_contents_content_id ON contents(content_id);
CREATE INDEX idx_contents_user_id ON contents(user_id);
CREATE INDEX idx_contents_publish_time ON contents(publish_time);
CREATE INDEX idx_contents_source_keyword ON contents(source_keyword);
CREATE INDEX idx_contents_platform_data ON contents USING GIN(platform_data);

-- =============================================================================
-- 3. comments — Unified comments table
-- =============================================================================
CREATE TABLE IF NOT EXISTS comments (
    id                  BIGSERIAL PRIMARY KEY,
    platform            TEXT NOT NULL,
    comment_id          TEXT NOT NULL,
    content_id          TEXT NOT NULL,            -- the content this comment belongs to
    parent_comment_id   TEXT DEFAULT '',          -- for nested/reply comments

    -- Comment data
    content             TEXT,
    pictures            TEXT DEFAULT '',          -- comma-separated image URLs

    -- Author info
    user_id             TEXT,
    nickname            TEXT,
    avatar              TEXT,
    ip_location         TEXT,

    -- Engagement
    like_count          INTEGER DEFAULT 0,
    dislike_count       INTEGER DEFAULT 0,
    sub_comment_count   INTEGER DEFAULT 0,

    -- Platform-specific data
    platform_data       JSONB DEFAULT '{}',

    -- Timestamps
    publish_time        BIGINT,
    add_ts              BIGINT,
    last_modify_ts      BIGINT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),

    UNIQUE(platform, comment_id)
);

CREATE INDEX idx_comments_platform ON comments(platform);
CREATE INDEX idx_comments_content_id ON comments(content_id);
CREATE INDEX idx_comments_comment_id ON comments(comment_id);
CREATE INDEX idx_comments_parent ON comments(parent_comment_id);
CREATE INDEX idx_comments_user_id ON comments(user_id);
CREATE INDEX idx_comments_publish_time ON comments(publish_time);

-- =============================================================================
-- 4. creators — Unified creators/profiles table
-- =============================================================================
CREATE TABLE IF NOT EXISTS creators (
    id                  BIGSERIAL PRIMARY KEY,
    platform            TEXT NOT NULL,
    user_id             TEXT NOT NULL,

    -- Profile info
    nickname            TEXT,
    avatar              TEXT,
    description         TEXT,
    gender              TEXT,
    ip_location         TEXT,

    -- Social metrics
    follows_count       INTEGER DEFAULT 0,
    fans_count          INTEGER DEFAULT 0,
    interaction_count   INTEGER DEFAULT 0,

    -- Platform-specific data
    -- XHS:      {"tag_list": {...}}
    -- Bilibili: {"total_liked":1000, "user_rank":5, "is_official":1}
    -- Douyin:   {"sec_uid":"...", "videos_count":50}
    -- Tieba:    {"user_name":"...", "registration_duration":"5年"}
    -- Zhihu:    {"url_token":"...", "answer_count":10, "article_count":5, "video_count":2, ...}
    platform_data       JSONB DEFAULT '{}',

    -- Timestamps
    add_ts              BIGINT,
    last_modify_ts      BIGINT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),

    UNIQUE(platform, user_id)
);

CREATE INDEX idx_creators_platform ON creators(platform);
CREATE INDEX idx_creators_user_id ON creators(user_id);
CREATE INDEX idx_creators_fans ON creators(fans_count DESC);

-- =============================================================================
-- 5. Bilibili-specific: contact_info (follower relationships)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bilibili_contacts (
    id              BIGSERIAL PRIMARY KEY,
    up_id           TEXT NOT NULL,
    fan_id          TEXT NOT NULL,
    up_name         TEXT,
    fan_name        TEXT,
    up_sign         TEXT,
    fan_sign        TEXT,
    up_avatar       TEXT,
    fan_avatar      TEXT,
    add_ts          BIGINT,
    last_modify_ts  BIGINT,
    created_at      TIMESTAMPTZ DEFAULT now(),

    UNIQUE(up_id, fan_id)
);

CREATE INDEX idx_bilibili_contacts_up ON bilibili_contacts(up_id);
CREATE INDEX idx_bilibili_contacts_fan ON bilibili_contacts(fan_id);

-- =============================================================================
-- 6. Bilibili-specific: up_dynamic (creator dynamics/updates)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bilibili_dynamics (
    id              BIGSERIAL PRIMARY KEY,
    dynamic_id      TEXT NOT NULL UNIQUE,
    user_id         TEXT,
    user_name       TEXT,
    text            TEXT,
    type            TEXT,
    pub_ts          BIGINT,
    total_comments  INTEGER DEFAULT 0,
    total_forwards  INTEGER DEFAULT 0,
    total_liked     INTEGER DEFAULT 0,
    add_ts          BIGINT,
    last_modify_ts  BIGINT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_bilibili_dynamics_user ON bilibili_dynamics(user_id);

-- =============================================================================
-- 7. Helpful views for backend consumption
-- =============================================================================

-- Latest contents per platform
CREATE OR REPLACE VIEW v_latest_contents AS
SELECT *
FROM contents
ORDER BY publish_time DESC
LIMIT 1000;

-- Content with comment count summary
CREATE OR REPLACE VIEW v_content_stats AS
SELECT
    c.id,
    c.platform,
    c.content_id,
    c.title,
    c.liked_count,
    c.comment_count,
    c.share_count,
    c.collected_count,
    c.source_keyword,
    c.publish_time,
    COUNT(cm.id) AS actual_comment_count
FROM contents c
LEFT JOIN comments cm ON c.platform = cm.platform AND c.content_id = cm.content_id
GROUP BY c.id;

-- Creator ranking by fans
CREATE OR REPLACE VIEW v_creator_ranking AS
SELECT
    platform,
    user_id,
    nickname,
    fans_count,
    follows_count,
    interaction_count
FROM creators
ORDER BY fans_count DESC;

-- =============================================================================
-- 8. Row Level Security (RLS) — for backend service role access
-- =============================================================================
-- Enable RLS on all tables
ALTER TABLE crawl_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE contents ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE creators ENABLE ROW LEVEL SECURITY;
ALTER TABLE bilibili_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE bilibili_dynamics ENABLE ROW LEVEL SECURITY;

-- Service role (used by crawler and backend) has full access
CREATE POLICY "Service role full access on crawl_tasks"
    ON crawl_tasks FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access on contents"
    ON contents FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access on comments"
    ON comments FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access on creators"
    ON creators FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access on bilibili_contacts"
    ON bilibili_contacts FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access on bilibili_dynamics"
    ON bilibili_dynamics FOR ALL
    USING (true) WITH CHECK (true);

-- =============================================================================
-- 9. Auto-update updated_at trigger
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_contents_updated_at
    BEFORE UPDATE ON contents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_comments_updated_at
    BEFORE UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_creators_updated_at
    BEFORE UPDATE ON creators
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
