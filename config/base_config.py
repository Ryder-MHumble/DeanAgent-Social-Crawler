# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/config/base_config.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

# Basic configuration
PLATFORM = "xhs"  # Platform, xhs | dy | ks | bili | wb | tieba | zhihu
KEYWORDS = "编程副业,编程兼职"  # Keyword search configuration, separated by English commas

# ==================== Content Relevance Filter ====================
# Enable to filter out content that doesn't actually mention the target entities.
# Platform search is fuzzy — "中关村人工智能研究院" returns lots of generic AI content.
# When enabled, only content containing at least one RELEVANCE_MUST_CONTAIN keyword
# in its title or description will be saved. Comments are saved only if their parent
# content passed the filter.
ENABLE_RELEVANCE_FILTER = True

# Content MUST contain at least one of these strings (case-insensitive for English)
# to be considered relevant. These should be the core entity names you care about.
RELEVANCE_MUST_CONTAIN = [
    "中关村人工智能研究院",
    "中关村学院",
    "北京中关村学院",
    "中关村AI研究院",
]
LOGIN_TYPE = "qrcode"  # qrcode or phone or cookie
COOKIES = ""
CRAWLER_TYPE = (
    "search"  # Crawling type, search (keyword search) | detail (post details) | creator (creator homepage data)
)
# Whether to enable IP proxy
ENABLE_IP_PROXY = False

# Number of proxy IP pools
IP_PROXY_POOL_COUNT = 2

# Proxy IP provider name
IP_PROXY_PROVIDER_NAME = "kuaidaili"  # kuaidaili | wandouhttp

# Setting to True will not open the browser (headless browser)
# Setting False will open a browser
# If Xiaohongshu keeps scanning the code to log in but fails, open the browser and manually pass the sliding verification code.
# If Douyin keeps prompting failure, open the browser and see if mobile phone number verification appears after scanning the QR code to log in. If it does, manually go through it and try again.
HEADLESS = False

# Whether to save login status
SAVE_LOGIN_STATE = True

# ==================== CDP (Chrome DevTools Protocol) Configuration ====================
# Whether to enable CDP mode - use the user's existing Chrome/Edge browser to crawl, providing better anti-detection capabilities
# Once enabled, the user's Chrome/Edge browser will be automatically detected and started, and controlled through the CDP protocol.
# This method uses the real browser environment, including the user's extensions, cookies and settings, greatly reducing the risk of detection.
ENABLE_CDP_MODE = True

# CDP debug port, used to communicate with the browser
# If the port is occupied, the system will automatically try the next available port
CDP_DEBUG_PORT = 9222

# Custom browser path (optional)
# If it is empty, the system will automatically detect the installation path of Chrome/Edge
# Windows example: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
# macOS example: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CUSTOM_BROWSER_PATH = ""

# Whether to enable headless mode in CDP mode
# NOTE: Even if set to True, some anti-detection features may not work well in headless mode
CDP_HEADLESS = False

# Browser startup timeout (seconds)
BROWSER_LAUNCH_TIMEOUT = 60

# Whether to automatically close the browser when the program ends
# Set to False to keep the browser running, preserving cookies and login state across runs
AUTO_CLOSE_BROWSER = False

# Data saving type option configuration. It is best to save to DB, with deduplication function.
# Supported: csv | db | json | sqlite | excel | postgres | mongodb | supabase
SAVE_DATA_OPTION = "supabase"  # csv or db or json or sqlite or excel or postgres or mongodb or supabase

# Data saving path, if not specified by default, it will be saved to the data folder.
SAVE_DATA_PATH = ""

# Browser file configuration cached by the user's browser
USER_DATA_DIR = "%s_user_data_dir"  # %s will be replaced by platform name

# The number of pages to start crawling starts from the first page by default
START_PAGE = 1

# Control the number of crawled videos/posts per keyword per run
# Keep this low (10-20) to avoid triggering anti-bot detection
CRAWLER_MAX_NOTES_COUNT = 10

# Controlling the number of concurrent crawlers (1 = safest, looks most human-like)
MAX_CONCURRENCY_NUM = 1

# Whether to enable crawling media mode (including image or video resources), crawling media is not enabled by default
ENABLE_GET_MEIDAS = False

# Whether to enable comment crawling mode. Comment crawling is enabled by default.
ENABLE_GET_COMMENTS = True

# Control the number of crawled first-level comments (single video/post)
# Keep moderate to avoid excessive requests per post
CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = 5

# Whether to enable the mode of crawling second-level comments. By default, crawling of second-level comments is not enabled.
# Enabling this significantly increases request volume — higher ban risk
ENABLE_GET_SUB_COMMENTS = False

# word cloud related
# Whether to enable generating comment word clouds
ENABLE_GET_WORDCLOUD = False
# Custom words and their groups
# Add rule: xx:yy where xx is a custom-added phrase, and yy is the group name to which the phrase xx is assigned.
CUSTOM_WORDS = {
    "零几": "年份",  # Recognize "zero points" as a whole
    "高频词": "专业术语",  # Example custom words
}

# Deactivate (disabled) word file path
STOP_WORDS_FILE = "./docs/hit_stopwords.txt"

# Chinese font file path
FONT_PATH = "./docs/STZHONGS.TTF"

# Crawl interval (seconds) — random sleep between requests
# Higher = safer. Recommended: 3-5 for normal use, 5-10 if you've been warned
CRAWLER_MAX_SLEEP_SEC = 5

from .bilibili_config import *
from .xhs_config import *
from .dy_config import *
from .ks_config import *
from .weibo_config import *
from .tieba_config import *
from .zhihu_config import *
