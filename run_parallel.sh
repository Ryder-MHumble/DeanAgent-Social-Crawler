#!/usr/bin/env bash
# MediaCrawler 并行爬虫启动脚本
# 提供实时进度监控和状态显示

set -e

# ==================== 配置区域 ====================
KEYWORDS="中关村人工智能研究院,北京中关村学院,中关村学院,中关村 河套 创智"
PLATFORMS=("xhs" "dy" "bili" "zhihu")
LOGIN_TYPE="cookie"
CRAWLER_TYPE="search"
SAVE_DATA_OPTION="supabase"
ENABLE_COMMENTS="yes"
# ==================== 配置区域结束 ====================

# 获取平台专属配置
get_max_notes() {
    case "$1" in
        dy) echo "10" ;;
        bili) echo "15" ;;
        zhihu) echo "10" ;;
        *) echo "" ;;
    esac
}

# 获取平台名称
get_platform_name() {
    case "$1" in
        xhs) echo "小红书" ;;
        dy) echo "抖音" ;;
        bili) echo "Bilibili" ;;
        wb) echo "微博" ;;
        zhihu) echo "知乎" ;;
        *) echo "$1" ;;
    esac
}

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 临时文件目录
TEMP_DIR="/tmp/mediacrawler_$$"
mkdir -p "$TEMP_DIR"

# 清理函数
cleanup() {
    echo -e "\n${YELLOW}⚠️  清理临时文件...${NC}"
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT INT TERM

# 打印横幅
print_banner() {
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║           MediaCrawler 并行爬虫启动脚本                      ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
}

# 展示各平台 Cookie 状态（仅提示，不过滤——平台列表由 PLATFORMS 数组决定）
show_platform_status() {
    echo "🔍 Cookie 状态检查"
    echo ""

    local missing=0
    for platform in "${PLATFORMS[@]}"; do
        local name=$(get_platform_name "$platform")
        if [ -f "cookies_config.py" ] && \
           .venv/bin/python -c "import cookies_config; exit(0 if cookies_config.is_cookie_configured('$platform') else 1)" 2>/dev/null; then
            echo -e "  ${GREEN}✅${NC} ${name} - Cookie 已配置"
        else
            echo -e "  ${YELLOW}⚠️${NC}  ${name} - Cookie 未配置（将尝试扫码登录）"
            missing=$((missing + 1))
        fi
    done

    echo ""
    if [ $missing -gt 0 ]; then
        echo -e "${YELLOW}⚠️  ${missing} 个平台缺少 Cookie，将自动弹出浏览器扫码${NC}"
        echo "   如需 Cookie 登录，请参考 COOKIE_GUIDE.md"
    else
        echo -e "${GREEN}✅ 所有平台 Cookie 均已配置${NC}"
    fi
    echo ""
}

# 运行单个平台爬虫
run_platform() {
    local platform=$1
    local name=$(get_platform_name "$platform")
    local log_file="$TEMP_DIR/${platform}.log"
    local status_file="$TEMP_DIR/${platform}.status"
    local start_time=$(date +%s)

    # 初始化状态
    echo "运行中" > "$status_file"
    echo -e "${BLUE}🚀 [$platform] 开始爬取 ${name}...${NC}" | tee -a "$log_file"

    local PYTHON="${PYTHON_BIN:-.venv/bin/python}"

    # 获取该平台的 Cookie（避免字符串拼接导致特殊字符解析错误）
    local cookie=""
    if [ -f "cookies_config.py" ]; then
        cookie=$("$PYTHON" -c \
            "import cookies_config; print(cookies_config.get_cookie('$platform'), end='')" \
            2>/dev/null || echo "")
    fi

    # 用数组构建命令，避免 eval + 字符串拼接的 quoting 问题
    local -a cmd=(
        "$PYTHON" "main.py"
        "--platform" "$platform"
        "--lt"       "$LOGIN_TYPE"
        "--type"     "$CRAWLER_TYPE"
        "--keywords" "$KEYWORDS"
        "--save_data_option" "$SAVE_DATA_OPTION"
        "--get_comment"      "$ENABLE_COMMENTS"
    )

    # Cookie 非空才传入（空字符串传入会覆盖 config 默认值）
    if [ -n "$cookie" ]; then
        cmd+=("--cookies" "$cookie")
    fi

    # 平台专属配置
    local max_notes
    max_notes=$(get_max_notes "$platform")
    if [ -n "$max_notes" ]; then
        cmd+=("--max_notes_count" "$max_notes")
    fi

    # 执行（输出同时到终端和日志，方便看到二维码等交互内容）
    "${cmd[@]}" > >(tee -a "$log_file") 2>&1
    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    if [ $exit_code -eq 0 ]; then
        echo "完成:$duration" > "$status_file"
        echo -e "${GREEN}✅ [$platform] ${name} 爬取完成！耗时: ${duration}秒${NC}"
    else
        echo "失败:$duration" > "$status_file"
        echo -e "${RED}❌ [$platform] ${name} 爬取失败！（退出码: $exit_code）${NC}"
    fi
}

# 监控进度
monitor_progress() {
    local platforms=("$@")
    local total=${#platforms[@]}

    echo ""
    echo "📊 实时监控 (每5秒刷新一次，按 Ctrl+C 停止监控但不影响爬虫运行)"
    echo "════════════════════════════════════════════════════════════════"
    echo ""

    while true; do
        local completed=0
        local running=0
        local failed=0

        clear
        echo "╔══════════════════════════════════════════════════════════════╗"
        echo "║              MediaCrawler 并行爬虫实时监控                   ║"
        echo "╚══════════════════════════════════════════════════════════════╝"
        echo ""
        echo "⏰ 当前时间: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        echo "┌────────────────────────────────────────────────────────────┐"
        printf "│ %-12s %-10s %-15s %-20s │\n" "平台" "状态" "耗时" "最新日志"
        echo "├────────────────────────────────────────────────────────────┤"

        for platform in "${platforms[@]}"; do
            local name=$(get_platform_name "$platform")
            local status_file="$TEMP_DIR/${platform}.status"
            local log_file="$TEMP_DIR/${platform}.log"

            if [ -f "$status_file" ]; then
                local status=$(cat "$status_file")
                local icon=""
                local duration=""

                if [[ $status == "运行中" ]]; then
                    icon="🔄"
                    running=$((running + 1))
                    duration="运行中"
                elif [[ $status == 完成:* ]]; then
                    icon="✅"
                    completed=$((completed + 1))
                    duration="${status#完成:}s"
                elif [[ $status == 失败:* ]]; then
                    icon="❌"
                    failed=$((failed + 1))
                    duration="${status#失败:}s"
                fi

                # 获取最新日志行
                local last_log=""
                if [ -f "$log_file" ]; then
                    last_log=$(tail -n 1 "$log_file" | cut -c1-20)
                fi

                printf "│ %-10s ${icon} %-8s %-13s %-20s │\n" "$name" "$status" "$duration" "$last_log"
            else
                printf "│ %-10s ⏳ %-8s %-13s %-20s │\n" "$name" "等待中" "-" "-"
            fi
        done

        echo "└────────────────────────────────────────────────────────────┘"
        echo ""
        echo "📊 总体统计:"
        echo "   运行中: $running  完成: $completed  失败: $failed  总计: $total"
        echo ""
        echo "💡 提示: 监控窗口可以安全关闭，爬虫会继续在后台运行"
        echo "   日志文件位置: $TEMP_DIR/*.log"

        # 检查是否全部完成
        if [ $((completed + failed)) -eq $total ]; then
            echo ""
            echo -e "${GREEN}🎉 所有平台爬取任务已完成！${NC}"
            break
        fi

        sleep 5
    done
}

# 生成最终报告
generate_report() {
    local platforms=("$@")

    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "📊 爬取任务完成总结"
    echo "════════════════════════════════════════════════════════════════"
    echo ""

    local success=0
    local failed=0
    local total_content=0
    local total_comment=0

    echo "详细结果:"
    for platform in "${platforms[@]}"; do
        local name=$(get_platform_name "$platform")
        local status_file="$TEMP_DIR/${platform}.status"
        local log_file="$TEMP_DIR/${platform}.log"

        if [ -f "$status_file" ]; then
            local status=$(cat "$status_file")

            if [[ $status == 完成:* ]]; then
                local duration="${status#完成:}"

                # 从 print_session_summary 输出的结构化日志中提取数据
                local new_content=$(grep -oP '新增内容:\s*\K\d+' "$log_file" 2>/dev/null | tail -1)
                local new_comment=$(grep -oP '新增评论:\s*\K\d+' "$log_file" 2>/dev/null | tail -1)
                local skipped=$(grep -oP '已有跳过:\s*\K\d+' "$log_file" 2>/dev/null | tail -1)
                local filtered=$(grep -oP '无关过滤:\s*\K\d+' "$log_file" 2>/dev/null | tail -1)
                new_content=${new_content:-0}
                new_comment=${new_comment:-0}
                skipped=${skipped:-0}
                filtered=${filtered:-0}

                total_content=$((total_content + new_content))
                total_comment=$((total_comment + new_comment))
                success=$((success + 1))

                echo -e "  ${GREEN}✅${NC} [$platform] ${name} - 成功 (耗时: ${duration}s)"
                echo -e "      ✅ 新增内容: ${new_content}  💬 新增评论: ${new_comment}  ⏭️  已有跳过: ${skipped}  🚫 无关过滤: ${filtered}"
            elif [[ $status == 失败:* ]]; then
                failed=$((failed + 1))
                # 显示错误信息（日志最后几行）
                local error_msg=$(tail -3 "$log_file" 2>/dev/null | head -1 || echo "未知错误")
                echo -e "  ${RED}❌${NC} [$platform] ${name} - 失败"
                echo -e "     错误: ${error_msg:0:80}"
            fi
        fi
    done

    echo ""
    echo "总体统计:"
    echo "  ✅ 成功: $success/${#platforms[@]} 个平台"
    echo "  ❌ 失败: $failed/${#platforms[@]} 个平台"
    echo "  📝 总内容: $total_content"
    echo "  💬 总评论: $total_comment"
    echo ""

    if [ $success -eq 0 ]; then
        echo -e "${RED}⚠️  所有平台都失败了！${NC}"
        echo "📋 请检查日志文件: $TEMP_DIR/*.log"
        echo "💡 常见问题:"
        echo "   1. Python 依赖未安装 - 运行: pip3 install -r requirements.txt"
        echo "   2. Cookie 已失效 - 需要重新获取"
        echo "   3. 网络连接问题"
    else
        echo "💾 数据保存位置: Supabase"
    fi

    echo "📋 日志文件: $TEMP_DIR/*.log"
    echo "════════════════════════════════════════════════════════════════"
    echo ""

    # 返回失败状态码
    if [ $success -eq 0 ]; then
        return 1
    fi
    return 0
}

# 主函数
main() {
    print_banner

    # 展示 Cookie 状态（仅提示，不过滤平台）
    show_platform_status

    echo "🚀 准备启动 ${#PLATFORMS[@]} 个平台的并行爬虫"
    echo "📋 目标关键词: $KEYWORDS"
    echo "💾 数据保存: $SAVE_DATA_OPTION"
    echo ""

    sleep 2

    # 并行启动所有平台（PLATFORMS 数组即为实际运行的平台列表）
    echo "▶ 同时启动所有平台的爬虫..."
    echo ""

    for platform in "${PLATFORMS[@]}"; do
        run_platform "$platform" &
    done

    # 等待一秒让所有进程启动
    sleep 1

    # 启动监控（可以被 Ctrl+C 中断，但不影响后台爬虫）
    monitor_progress "${PLATFORMS[@]}" || true

    # 等待所有后台任务完成
    echo ""
    echo "⏳ 等待所有爬虫任务完成..."
    wait

    # 生成最终报告
    if generate_report "${PLATFORMS[@]}"; then
        echo "🎉 爬取任务完成！"
        exit 0
    else
        echo -e "${RED}❌ 爬取任务失败！${NC}"
        exit 1
    fi
}

# 运行主函数
main "$@"
