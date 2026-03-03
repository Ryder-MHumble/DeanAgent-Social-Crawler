#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MediaCrawler 并行爬虫监控脚本
提供实时进度显示、状态监控和日志查看

Usage:
    python run_parallel_monitor.py              # 正常模式：检查登录 -> 并行爬取
    python run_parallel_monitor.py --crawl-only # 仅爬取模式（跳过登录检查）
    python run_parallel_monitor.py --no-ui      # 无UI模式（适合后台运行）
"""

import subprocess
import sys
import time
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple
import re

# 导入 Cookie 配置
try:
    import cookies_config
    COOKIES_AVAILABLE = True
except ImportError:
    COOKIES_AVAILABLE = False
    cookies_config = None

# ==================== 配置区域 ====================
KEYWORDS = "中关村人工智能研究院,北京中关村学院,中关村学院,中关村 河套 创智"
PLATFORMS = ["xhs", "dy", "bili", "zhihu"]
LOGIN_TYPE = "cookie"
CRAWLER_TYPE = "search"
SAVE_DATA_OPTION = "supabase"
ENABLE_COMMENTS = "yes"

PLATFORM_OVERRIDES: dict[str, dict] = {
    "dy":    {"max_notes_count": 10},
    "bili":  {"max_notes_count": 15},
    "zhihu": {"max_notes_count": 10},
}

PLATFORM_NAMES = {
    "xhs": "小红书",
    "dy": "抖音",
    "bili": "Bilibili",
    "wb": "微博",
    "ks": "快手",
    "tieba": "贴吧",
    "zhihu": "知乎"
}
# ==================== 配置区域结束 ====================


class PlatformStatus:
    """平台爬取状态"""
    def __init__(self, platform: str):
        self.platform = platform
        self.status = "等待中"  # 等待中, 运行中, 完成, 失败
        self.start_time = None
        self.end_time = None
        self.content_count = 0
        self.comment_count = 0
        self.error_msg = ""
        self.current_keyword = ""
        self.log_lines = []

    def get_duration(self) -> float:
        """获取运行时长（秒）"""
        if not self.start_time:
            return 0
        end = self.end_time or time.time()
        return end - self.start_time

    def get_status_icon(self) -> str:
        """获取状态图标"""
        icons = {
            "等待中": "⏳",
            "运行中": "🔄",
            "完成": "✅",
            "失败": "❌"
        }
        return icons.get(self.status, "❓")


class CrawlerMonitor:
    """爬虫监控器"""
    def __init__(self, platforms: List[str], enable_ui: bool = True):
        self.platforms = platforms
        self.enable_ui = enable_ui
        self.statuses: Dict[str, PlatformStatus] = {
            p: PlatformStatus(p) for p in platforms
        }
        self.lock = threading.Lock()
        self.ui_thread = None
        self.running = True

    def update_status(self, platform: str, **kwargs):
        """更新平台状态"""
        with self.lock:
            status = self.statuses[platform]
            for key, value in kwargs.items():
                setattr(status, key, value)

    def parse_log_line(self, platform: str, line: str):
        """解析日志行，提取进度信息"""
        line = line.strip()
        if not line:
            return

        # 保存日志行
        with self.lock:
            self.statuses[platform].log_lines.append(line)
            if len(self.statuses[platform].log_lines) > 50:
                self.statuses[platform].log_lines.pop(0)

        # 提取内容数量
        content_match = re.search(r'保存.*?(\d+).*?内容', line)
        if content_match:
            count = int(content_match.group(1))
            self.update_status(platform, content_count=count)

        # 提取评论数量
        comment_match = re.search(r'保存.*?(\d+).*?评论', line)
        if comment_match:
            count = int(comment_match.group(1))
            self.update_status(platform, comment_count=count)

        # 提取当前关键词
        keyword_match = re.search(r'关键词[:：]\s*(.+)', line)
        if keyword_match:
            keyword = keyword_match.group(1).strip()
            self.update_status(platform, current_keyword=keyword)

    def render_ui(self):
        """渲染UI界面"""
        while self.running:
            if not self.enable_ui:
                time.sleep(1)
                continue

            # 清屏
            print("\033[2J\033[H", end="")

            # 标题
            print("╔" + "═" * 78 + "╗")
            print("║" + " " * 20 + "MediaCrawler 并行爬虫监控" + " " * 33 + "║")
            print("╚" + "═" * 78 + "╝\n")

            # 当前时间
            print(f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

            # 平台状态表格
            print("┌" + "─" * 78 + "┐")
            print(f"│ {'平台':<8} {'状态':<6} {'耗时':<8} {'内容':<6} {'评论':<6} {'当前关键词':<30} │")
            print("├" + "─" * 78 + "┤")

            with self.lock:
                for platform in self.platforms:
                    status = self.statuses[platform]
                    name = PLATFORM_NAMES.get(platform, platform)
                    icon = status.get_status_icon()
                    duration = f"{status.get_duration():.0f}s" if status.start_time else "-"
                    content = str(status.content_count) if status.content_count > 0 else "-"
                    comment = str(status.comment_count) if status.comment_count > 0 else "-"
                    keyword = status.current_keyword[:28] if status.current_keyword else "-"

                    print(f"│ {name:<6} {icon} {status.status:<4} {duration:<8} {content:<6} {comment:<6} {keyword:<30} │")

            print("└" + "─" * 78 + "┘\n")

            # 总体统计
            with self.lock:
                total_content = sum(s.content_count for s in self.statuses.values())
                total_comment = sum(s.comment_count for s in self.statuses.values())
                running_count = sum(1 for s in self.statuses.values() if s.status == "运行中")
                completed_count = sum(1 for s in self.statuses.values() if s.status == "完成")
                failed_count = sum(1 for s in self.statuses.values() if s.status == "失败")

            print(f"📊 总体统计:")
            print(f"   运行中: {running_count}  完成: {completed_count}  失败: {failed_count}")
            print(f"   总内容: {total_content}  总评论: {total_comment}\n")

            # 最新日志
            print("📝 最新日志 (最近5条):")
            print("─" * 80)
            with self.lock:
                all_logs = []
                for platform in self.platforms:
                    status = self.statuses[platform]
                    for log in status.log_lines[-5:]:
                        all_logs.append((platform, log))

                for platform, log in all_logs[-5:]:
                    name = PLATFORM_NAMES.get(platform, platform)
                    print(f"[{name}] {log[:70]}")

            print("\n💡 提示: 按 Ctrl+C 可以安全退出")

            time.sleep(2)

    def start_ui(self):
        """启动UI线程"""
        if self.enable_ui:
            self.ui_thread = threading.Thread(target=self.render_ui, daemon=True)
            self.ui_thread.start()

    def stop_ui(self):
        """停止UI线程"""
        self.running = False
        if self.ui_thread:
            self.ui_thread.join(timeout=1)


def run_platform_with_monitor(platform: str, monitor: CrawlerMonitor) -> Tuple[str, bool, float]:
    """运行单个平台的爬虫并监控"""
    platform_name = PLATFORM_NAMES.get(platform, platform)

    monitor.update_status(platform, status="运行中", start_time=time.time())

    # 构建命令
    cmd = [
        sys.executable, "main.py",
        "--platform", platform,
        "--lt", LOGIN_TYPE,
        "--type", CRAWLER_TYPE,
        "--keywords", KEYWORDS,
        "--save_data_option", SAVE_DATA_OPTION,
        "--get_comment", ENABLE_COMMENTS,
    ]

    # 应用平台专属配置
    overrides = PLATFORM_OVERRIDES.get(platform, {})
    if "max_notes_count" in overrides:
        cmd.extend(["--max_notes_count", str(overrides["max_notes_count"])])

    # Cookie 登录
    if COOKIES_AVAILABLE and cookies_config:
        cookie = cookies_config.get_cookie(platform)
        if cookie:
            cmd.extend(["--cookies", cookie])

    try:
        # 启动子进程并实时读取输出
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=Path(__file__).parent
        )

        # 实时读取输出
        for line in process.stdout:
            monitor.parse_log_line(platform, line)

        process.wait()

        end_time = time.time()
        duration = end_time - monitor.statuses[platform].start_time

        if process.returncode == 0:
            monitor.update_status(platform, status="完成", end_time=end_time)
            return (platform, True, duration)
        else:
            monitor.update_status(platform, status="失败", end_time=end_time,
                                error_msg=f"退出码: {process.returncode}")
            return (platform, False, duration)

    except Exception as e:
        end_time = time.time()
        duration = end_time - monitor.statuses[platform].start_time
        monitor.update_status(platform, status="失败", end_time=end_time,
                            error_msg=str(e))
        return (platform, False, duration)


def check_cookie_config() -> List[str]:
    """检查Cookie配置，返回可用平台列表"""
    if not COOKIES_AVAILABLE or not cookies_config:
        print("❌ 未找到 cookies_config.py 文件")
        print("📝 请参考 COOKIE_GUIDE.md 创建配置文件\n")
        return []

    print("🔍 检查 Cookie 配置\n")

    available_platforms = []
    for platform in PLATFORMS:
        name = PLATFORM_NAMES.get(platform, platform)
        if cookies_config.is_cookie_configured(platform):
            print(f"  ✅ {name:<10} - Cookie 已配置")
            available_platforms.append(platform)
        else:
            print(f"  ❌ {name:<10} - Cookie 未配置")

    print()
    return available_platforms


def parallel_crawl_with_monitor(platforms: List[str], enable_ui: bool = True) -> bool:
    """并行爬取并监控"""
    if not platforms:
        print("❌ 没有可用的平台")
        return False

    print(f"🚀 准备启动 {len(platforms)} 个平台的并行爬虫")
    print(f"📋 目标关键词: {KEYWORDS}")
    print(f"💾 数据保存: {SAVE_DATA_OPTION.upper()}\n")

    time.sleep(2)

    # 创建监控器
    monitor = CrawlerMonitor(platforms, enable_ui=enable_ui)
    monitor.start_ui()

    start_time = time.time()
    results = []

    try:
        # 并行执行
        with ThreadPoolExecutor(max_workers=len(platforms)) as executor:
            futures = {
                executor.submit(run_platform_with_monitor, p, monitor): p
                for p in platforms
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    platform = futures[future]
                    print(f"❌ [{platform.upper()}] 未捕获异常: {e}")
                    results.append((platform, False, 0))

    finally:
        monitor.stop_ui()

    total_duration = time.time() - start_time

    # 最终总结
    print("\n" + "="*80)
    print("📊 爬取任务完成总结")
    print("="*80 + "\n")

    success_count = sum(1 for _, success, _ in results if success)
    failed_count = len(results) - success_count

    print(f"总体统计:")
    print(f"  ✅ 成功: {success_count}/{len(results)} 个平台")
    print(f"  ❌ 失败: {failed_count}/{len(results)} 个平台")
    print(f"  ⏱️  总耗时: {total_duration:.1f} 秒\n")

    print(f"详细结果:")
    for platform, success, duration in sorted(results, key=lambda x: x[0]):
        name = PLATFORM_NAMES.get(platform, platform)
        status = monitor.statuses[platform]
        if success:
            print(f"  ✅ [{platform.upper()}] {name:<10} - 成功 (耗时: {duration:.1f}s, 内容: {status.content_count}, 评论: {status.comment_count})")
        else:
            print(f"  ❌ [{platform.upper()}] {name:<10} - 失败 ({status.error_msg})")
            # 显示最后几行日志帮助诊断
            if status.log_lines:
                print(f"     最后日志: {status.log_lines[-1][:80]}")

    if success_count == 0:
        print(f"\n⚠️  所有平台都失败了！")
        print(f"💡 常见问题:")
        print(f"   1. Python 依赖未安装 - 运行: pip3 install -r requirements.txt")
        print(f"   2. Cookie 已失效 - 需要重新获取")
        print(f"   3. 网络连接问题")
    else:
        print(f"\n💾 数据保存位置: Supabase")

    print("="*80 + "\n")

    return success_count > 0


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='MediaCrawler 并行爬虫监控脚本')
    parser.add_argument('--crawl-only', action='store_true', help='仅执行爬取（跳过登录检查）')
    parser.add_argument('--no-ui', action='store_true', help='禁用UI界面（适合后台运行）')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 20 + "MediaCrawler 并行爬虫监控" + " " * 33 + "║")
    print("╚" + "═" * 78 + "╝\n")

    # 检查Cookie配置
    available_platforms = check_cookie_config()

    if not available_platforms:
        print("❌ 没有可用的平台，请先配置 Cookie")
        print("📖 Cookie 配置指南: COOKIE_GUIDE.md")
        return 1

    print(f"✅ 找到 {len(available_platforms)} 个已配置的平台\n")

    # 开始爬取
    enable_ui = not args.no_ui
    success = parallel_crawl_with_monitor(available_platforms, enable_ui=enable_ui)

    if success:
        print("🎉 爬取任务完成！")
        return 0
    else:
        print("❌ 爬取任务失败！请检查上述错误信息")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
