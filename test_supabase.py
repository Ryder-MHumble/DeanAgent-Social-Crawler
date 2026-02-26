#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Supabase 连接及存储测试脚本
测试内容:
1. Supabase 连接是否正常
2. 各个平台数据能否正常写入 (contents, comments, creators)
3. 写入后能否正常读取
4. 清理测试数据
"""

import asyncio
import sys
import time
import traceback

# 确保 .env 被加载
from dotenv import load_dotenv
load_dotenv()

from config import db_config

# ==================== 测试配置 ====================
TEST_PREFIX = "__test__"  # 用于标识测试数据，方便清理

PLATFORMS = ["xhs", "dy", "bili", "wb", "ks", "tieba", "zhihu"]

PLATFORM_NAMES = {
    "xhs": "小红书",
    "dy": "抖音",
    "bili": "Bilibili",
    "wb": "微博",
    "ks": "快手",
    "tieba": "贴吧",
    "zhihu": "知乎",
}


def print_header(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def print_result(test_name: str, success: bool, detail: str = ""):
    icon = "✅" if success else "❌"
    print(f"  {icon} {test_name}")
    if detail:
        print(f"     {detail}")


# ==================== 测试 1: 连接测试 ====================
def test_connection():
    """测试 Supabase 连接"""
    print_header("测试 1: Supabase 连接测试")

    # 检查环境变量
    url = db_config.SUPABASE_URL
    key = db_config.SUPABASE_KEY

    print(f"  SUPABASE_URL: {url[:30]}..." if url else "  SUPABASE_URL: (未配置)")
    print(f"  SUPABASE_KEY: {key[:15]}..." if key else "  SUPABASE_KEY: (未配置)")
    print()

    if not url or not key:
        print_result("环境变量检查", False, "请在 .env 文件中配置 SUPABASE_URL 和 SUPABASE_KEY")
        return False

    print_result("环境变量检查", True, "URL 和 KEY 已配置")

    # 尝试创建客户端
    try:
        from database.supabase_client import get_supabase
        sb = get_supabase()
        print_result("客户端创建", True, f"类型: {type(sb).__name__}")
    except Exception as e:
        print_result("客户端创建", False, str(e))
        return False

    # 测试读取 - 检查表是否存在
    tables_to_check = ["contents", "comments", "creators", "crawl_tasks",
                       "bilibili_contacts", "bilibili_dynamics"]
    all_tables_ok = True

    for table in tables_to_check:
        try:
            result = sb.table(table).select("id").limit(1).execute()
            print_result(f"表 '{table}' 访问", True, f"返回 {len(result.data)} 行")
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "404" in error_msg:
                print_result(f"表 '{table}' 访问", False, "表不存在，请先运行 schema/supabase_migration.sql")
            else:
                print_result(f"表 '{table}' 访问", False, error_msg[:100])
            all_tables_ok = False

    return all_tables_ok


# ==================== 测试 2: 各平台数据写入 ====================
def generate_test_content(platform: str) -> dict:
    """生成各平台的测试内容数据"""
    ts = int(time.time())
    base = {
        "content_id": f"{TEST_PREFIX}{platform}_content_001",
        "content_type": "test",
        "title": f"[测试] {PLATFORM_NAMES.get(platform, platform)} 测试标题",
        "description": f"这是 {PLATFORM_NAMES.get(platform, platform)} 平台的测试内容，用于验证 Supabase 存储",
        "content_url": f"https://test.example.com/{platform}/content/001",
        "cover_url": "",
        "user_id": f"{TEST_PREFIX}{platform}_user_001",
        "nickname": f"测试用户_{platform}",
        "avatar": "https://test.example.com/avatar.png",
        "ip_location": "北京",
        "liked_count": 42,
        "comment_count": 10,
        "share_count": 5,
        "collected_count": 3,
        "source_keyword": "测试关键词",
        "publish_time": ts,
        "platform_data": {"test_field": f"{platform}_test_value"},
    }
    return base


def generate_test_comment(platform: str) -> dict:
    """生成各平台的测试评论数据"""
    ts = int(time.time())
    return {
        "comment_id": f"{TEST_PREFIX}{platform}_comment_001",
        "content_id": f"{TEST_PREFIX}{platform}_content_001",
        "parent_comment_id": "",
        "content": f"[测试评论] 来自 {PLATFORM_NAMES.get(platform, platform)} 的测试评论",
        "pictures": "",
        "user_id": f"{TEST_PREFIX}{platform}_commenter_001",
        "nickname": f"评论用户_{platform}",
        "avatar": "https://test.example.com/avatar2.png",
        "ip_location": "上海",
        "like_count": 7,
        "dislike_count": 0,
        "sub_comment_count": 2,
        "publish_time": ts,
        "platform_data": {"test_comment_field": True},
    }


def generate_test_creator(platform: str) -> dict:
    """生成各平台的测试创作者数据"""
    return {
        "user_id": f"{TEST_PREFIX}{platform}_user_001",
        "nickname": f"测试用户_{platform}",
        "avatar": "https://test.example.com/avatar.png",
        "description": f"{PLATFORM_NAMES.get(platform, platform)} 测试创作者简介",
        "gender": "男",
        "ip_location": "北京",
        "follows_count": 100,
        "fans_count": 5000,
        "interaction_count": 10000,
        "platform_data": {"test_creator_field": f"{platform}_creator_value"},
    }


async def test_platform_write():
    """测试各平台数据写入"""
    print_header("测试 2: 各平台数据写入测试")

    from database.supabase_store_base import SupabaseStoreBase
    import config

    # 临时禁用 relevance filter，否则测试数据会被过滤掉
    original_filter = getattr(config, "ENABLE_RELEVANCE_FILTER", False)
    config.ENABLE_RELEVANCE_FILTER = False

    results = {"content": {}, "comment": {}, "creator": {}}

    for platform in PLATFORMS:
        platform_name = PLATFORM_NAMES.get(platform, platform)
        print(f"\n  --- {platform_name} ({platform}) ---")

        store = SupabaseStoreBase(platform=platform)

        # 写入 content
        try:
            content_data = generate_test_content(platform)
            await store.save_content(content_data)
            print_result(f"写入 content", True)
            results["content"][platform] = True
        except Exception as e:
            print_result(f"写入 content", False, str(e)[:120])
            results["content"][platform] = False

        # 写入 comment
        try:
            comment_data = generate_test_comment(platform)
            await store.save_comment(comment_data)
            print_result(f"写入 comment", True)
            results["comment"][platform] = True
        except Exception as e:
            print_result(f"写入 comment", False, str(e)[:120])
            results["comment"][platform] = False

        # 写入 creator
        try:
            creator_data = generate_test_creator(platform)
            await store.save_creator(creator_data)
            print_result(f"写入 creator", True)
            results["creator"][platform] = True
        except Exception as e:
            print_result(f"写入 creator", False, str(e)[:120])
            results["creator"][platform] = False

    # 恢复 relevance filter
    config.ENABLE_RELEVANCE_FILTER = original_filter

    return results


# ==================== 测试 3: 数据读取验证 ====================
def test_read_verification():
    """验证写入的数据能否正确读取"""
    print_header("测试 3: 数据读取验证")

    from database.supabase_client import get_supabase
    sb = get_supabase()

    all_ok = True

    # 检查 contents
    print("  --- contents 表 ---")
    try:
        result = sb.table("contents").select("*").like("content_id", f"{TEST_PREFIX}%").execute()
        count = len(result.data)
        print_result(f"测试 content 记录数", count == len(PLATFORMS),
                     f"期望 {len(PLATFORMS)} 条，实际 {count} 条")
        if count > 0:
            platforms_found = sorted(set(r["platform"] for r in result.data))
            print(f"     覆盖平台: {', '.join(platforms_found)}")

            # 展示一条示例
            sample = result.data[0]
            print(f"     示例: [{sample['platform']}] {sample['title']}")
            print(f"            liked={sample['liked_count']}, comments={sample['comment_count']}")

        if count != len(PLATFORMS):
            all_ok = False
    except Exception as e:
        print_result("读取 contents", False, str(e)[:120])
        all_ok = False

    # 检查 comments
    print("\n  --- comments 表 ---")
    try:
        result = sb.table("comments").select("*").like("comment_id", f"{TEST_PREFIX}%").execute()
        count = len(result.data)
        print_result(f"测试 comment 记录数", count == len(PLATFORMS),
                     f"期望 {len(PLATFORMS)} 条，实际 {count} 条")
        if count > 0:
            sample = result.data[0]
            print(f"     示例: [{sample['platform']}] {sample['content'][:50]}")

        if count != len(PLATFORMS):
            all_ok = False
    except Exception as e:
        print_result("读取 comments", False, str(e)[:120])
        all_ok = False

    # 检查 creators
    print("\n  --- creators 表 ---")
    try:
        result = sb.table("creators").select("*").like("user_id", f"{TEST_PREFIX}%").execute()
        count = len(result.data)
        print_result(f"测试 creator 记录数", count == len(PLATFORMS),
                     f"期望 {len(PLATFORMS)} 条，实际 {count} 条")
        if count > 0:
            sample = result.data[0]
            print(f"     示例: [{sample['platform']}] {sample['nickname']} (fans={sample['fans_count']})")

        if count != len(PLATFORMS):
            all_ok = False
    except Exception as e:
        print_result("读取 creators", False, str(e)[:120])
        all_ok = False

    return all_ok


# ==================== 测试 4: 清理测试数据 ====================
def test_cleanup():
    """清理测试数据"""
    print_header("测试 4: 清理测试数据")

    from database.supabase_client import get_supabase
    sb = get_supabase()

    tables_and_columns = [
        ("contents", "content_id"),
        ("comments", "comment_id"),
        ("creators", "user_id"),
    ]

    all_ok = True
    for table, col in tables_and_columns:
        try:
            result = sb.table(table).delete().like(col, f"{TEST_PREFIX}%").execute()
            deleted = len(result.data) if result.data else 0
            print_result(f"清理 {table}", True, f"删除了 {deleted} 条测试记录")
        except Exception as e:
            print_result(f"清理 {table}", False, str(e)[:120])
            all_ok = False

    return all_ok


# ==================== 测试 5: 检查已有真实数据 ====================
def test_existing_data():
    """检查数据库中是否有已存在的真实爬取数据"""
    print_header("附加: 查看已有数据统计")

    from database.supabase_client import get_supabase
    sb = get_supabase()

    for table in ["contents", "comments", "creators"]:
        try:
            # 按平台分组统计
            result = sb.table(table).select("platform").execute()
            if result.data:
                from collections import Counter
                platform_counts = Counter(r["platform"] for r in result.data)
                total = len(result.data)
                print(f"  {table} (总计 {total} 条):")
                for platform, count in sorted(platform_counts.items()):
                    name = PLATFORM_NAMES.get(platform, platform)
                    print(f"    {name:10} ({platform}): {count} 条")
            else:
                print(f"  {table}: 空表")
        except Exception as e:
            print(f"  {table}: 查询失败 - {str(e)[:80]}")

    print()


# ==================== 主测试流程 ====================
async def main():
    print("\n" + "=" * 60)
    print("  MediaCrawler Supabase 连接及存储测试")
    print("=" * 60)

    total_tests = 0
    passed_tests = 0

    # 测试 1: 连接
    total_tests += 1
    if test_connection():
        passed_tests += 1
        print("\n  ✅ 连接测试通过")
    else:
        print("\n  ❌ 连接测试失败，后续测试无法执行")
        print(f"\n{'='*60}")
        print(f"  测试结果: {passed_tests}/{total_tests} 通过")
        print(f"{'='*60}\n")
        return

    # 测试 2: 写入
    total_tests += 1
    write_results = await test_platform_write()
    write_all_ok = all(
        all(results.values())
        for results in write_results.values()
    )
    if write_all_ok:
        passed_tests += 1
        print("\n  ✅ 所有平台写入测试通过")
    else:
        # 统计失败
        failed = []
        for op, platform_results in write_results.items():
            for platform, ok in platform_results.items():
                if not ok:
                    failed.append(f"{platform}/{op}")
        print(f"\n  ⚠️  部分写入失败: {', '.join(failed)}")

    # 测试 3: 读取验证
    total_tests += 1
    if test_read_verification():
        passed_tests += 1
        print("\n  ✅ 数据读取验证通过")
    else:
        print("\n  ⚠️  数据读取验证有问题")

    # 测试 4: 清理
    total_tests += 1
    if test_cleanup():
        passed_tests += 1
        print("\n  ✅ 测试数据清理完成")
    else:
        print("\n  ⚠️  测试数据清理有问题")

    # 附加: 查看已有数据
    test_existing_data()

    # 总结
    print(f"{'='*60}")
    all_pass = passed_tests == total_tests
    icon = "🎉" if all_pass else "⚠️"
    print(f"  {icon} 测试结果: {passed_tests}/{total_tests} 通过")
    if all_pass:
        print("  Supabase 连接正常，所有平台数据读写均正常！")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 测试执行出错: {e}")
        traceback.print_exc()
        sys.exit(1)
