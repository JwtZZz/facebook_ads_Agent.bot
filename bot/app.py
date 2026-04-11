"""Telegram Bot 初始化 + Handler 注册"""
import asyncio
import logging
import os

from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, CallbackQueryHandler, filters,
)

from bot.handlers.base import start, help_cmd, clear_cmd, status_cmd
from bot.handlers.fb_ads import (
    setfb, campaigns_cmd, report_cmd,
    pause_cmd, resume_cmd, addbudget_cmd,
    normal_cmd, publish_last_cmd, automonitor_cmd,
    monitor_toggle_callback, monitor_confirm_callback,
    # 按钮引导式创建
    OD_TOKEN, OD_ACCOUNT, OD_PIXEL, OD_PAGE, OD_EVENT, OD_NAME, OD_COUNT, OD_BUDGET, OD_URL,
    OD_COUNTRY, OD_DEVICE, OD_GENDER, OD_AGE, OD_CONFIRM,
    OD_MEDIA, OD_TEXT, OD_TITLE, OD_PUBLISH,
    OD_MODE, OD_AI_CHOICE, OD_MANUAL_COPY,
    od_start, od_token_input, od_account, od_pixel, od_page, od_event,
    od_name, od_mode, od_count, od_budget, od_url,
    od_country, od_device, od_gender, od_age, od_confirm,
    od_media, od_text, od_title, od_ai_choice, od_manual_copy, od_publish, od_cancel,
)
from bot.handlers.media import upload_video_cmd, video_confirm_callback
from bot.handlers.adspower import (
    profiles_cmd, open_cmd, close_cmd,
    active_cmd, info_cmd, login_cmd, regdev_cmd,
    handle_profile_id, acceptbm_cmd,
    setupbm_cmd, setupbm_token, setupbm_cancel,
    WAITING_TOKEN,
)
from services.llm import ask_llm
from services.monitor import monitor_loop
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def _handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """普通文字消息 → LLM（流式输出）"""
    if not update.message or not update.message.text:
        return
    user = update.message.from_user
    logger.info(f"[{update.message.chat_id}] {user.first_name}: {update.message.text}")
    msg = await update.message.reply_text("💭 ...")
    await ask_llm(update.message.chat_id, update.message.text, reply_message=msg)


def build_app():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN 未设置")

    builder = ApplicationBuilder().token(token)

    proxy = os.getenv("TELEGRAM_PROXY", "")
    if proxy:
        builder = builder.proxy(proxy).get_updates_proxy(proxy)

    app = builder.build()

    # 基础命令
    app.add_handler(CommandHandler("start",   start))
    app.add_handler(CommandHandler("help",    help_cmd))
    app.add_handler(CommandHandler("clear",   clear_cmd))
    app.add_handler(CommandHandler("status",  status_cmd))

    # FB 广告命令
    app.add_handler(CommandHandler("setfb",        setfb))
    app.add_handler(CommandHandler("campaigns",    campaigns_cmd))
    app.add_handler(CommandHandler("report",       report_cmd))
    app.add_handler(CommandHandler("pause",        pause_cmd))
    app.add_handler(CommandHandler("resume",       resume_cmd))
    app.add_handler(CommandHandler("addbudget",    addbudget_cmd))
    app.add_handler(CommandHandler("publish_last", publish_last_cmd))
    app.add_handler(CommandHandler("automonitor",  automonitor_cmd))

    # Adspower 命令
    app.add_handler(CommandHandler("profiles", profiles_cmd))
    app.add_handler(CommandHandler("open",     open_cmd))
    app.add_handler(CommandHandler("close",    close_cmd))
    app.add_handler(CommandHandler("active",   active_cmd))
    app.add_handler(CommandHandler("info",     info_cmd))
    app.add_handler(CommandHandler("login",    login_cmd))
    app.add_handler(CommandHandler("regdev",   regdev_cmd))
    app.add_handler(CommandHandler("acceptbm", acceptbm_cmd))

    # setupbm 多步对话（必须在 _text_dispatch 之前注册）
    setupbm_conv = ConversationHandler(
        entry_points=[CommandHandler("setupbm", setupbm_cmd)],
        states={
            WAITING_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, setupbm_token)],
        },
        fallbacks=[CommandHandler("cancel", setupbm_cancel)],
        conversation_timeout=600,
    )
    app.add_handler(setupbm_conv)

    # 正常跑法 — 按钮引导式对话
    ads_conv = ConversationHandler(
        entry_points=[
            CommandHandler("normal", od_start),
        ],
        states={
            OD_TOKEN:       [MessageHandler(filters.TEXT & ~filters.COMMAND, od_token_input)],
            OD_ACCOUNT:     [CallbackQueryHandler(od_account, pattern=r"^od_acct:")],
            OD_PIXEL:       [CallbackQueryHandler(od_pixel, pattern=r"^od_pixel:")],
            OD_PAGE:        [CallbackQueryHandler(od_page, pattern=r"^od_page:")],
            OD_EVENT:       [CallbackQueryHandler(od_event, pattern=r"^od_event:")],
            OD_NAME:        [MessageHandler(filters.TEXT & ~filters.COMMAND, od_name)],
            OD_MODE:        [CallbackQueryHandler(od_mode, pattern=r"^od_flow:")],
            OD_COUNT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, od_count)],
            OD_BUDGET:      [MessageHandler(filters.TEXT & ~filters.COMMAND, od_budget)],
            OD_URL:         [MessageHandler(filters.TEXT & ~filters.COMMAND, od_url)],
            OD_COUNTRY:     [CallbackQueryHandler(od_country, pattern=r"^od_country:")],
            OD_DEVICE:      [CallbackQueryHandler(od_device, pattern=r"^od_device:")],
            OD_GENDER:      [CallbackQueryHandler(od_gender, pattern=r"^od_gender:")],
            OD_AGE:         [CallbackQueryHandler(od_age, pattern=r"^od_age:")],
            OD_CONFIRM:     [CallbackQueryHandler(od_confirm, pattern=r"^od_confirm:")],
            OD_MEDIA:       [MessageHandler(
                (filters.VIDEO | filters.Document.VIDEO | filters.PHOTO) & ~filters.COMMAND,
                od_media,
            )],
            OD_TEXT:        [MessageHandler(filters.TEXT & ~filters.COMMAND, od_text)],
            OD_TITLE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, od_title)],
            OD_AI_CHOICE:   [CallbackQueryHandler(od_ai_choice, pattern=r"^od_copy:")],
            OD_MANUAL_COPY: [MessageHandler(filters.TEXT & ~filters.COMMAND, od_manual_copy)],
            OD_PUBLISH:     [CallbackQueryHandler(od_publish, pattern=r"^od_publish:")],
        },
        fallbacks=[CommandHandler("cancel", od_cancel)],
        conversation_timeout=300,
    )
    app.add_handler(ads_conv)

    # 素材：直接发视频文件触发（显示确认按钮）
    app.add_handler(MessageHandler(
        (filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND,
        upload_video_cmd,
    ))
    # 视频确认/放弃按钮回调
    app.add_handler(CallbackQueryHandler(video_confirm_callback, pattern=r"^video_confirm:"))

    # 监控系列选择按钮回调
    app.add_handler(CallbackQueryHandler(monitor_toggle_callback, pattern=r"^monitor_toggle:"))
    app.add_handler(CallbackQueryHandler(monitor_confirm_callback, pattern=r"^monitor_confirm"))

    # 文字消息：自动识别环境编号或 BM 链接，否则走 LLM
    async def _text_dispatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = (update.message.text or "").strip()
        is_group = update.effective_chat.type != "private"

        if "business.facebook.com/invitation" in text:
            await handle_profile_id(update, ctx)
        elif text.isdigit() and len(text) >= 3:
            await handle_profile_id(update, ctx)
        elif is_group:
            # 群聊里不对普通文字做 AI 回复，避免干扰其他机器人
            return
        else:
            await _handle_text(update, ctx)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _text_dispatch))

    # 启动后台监控循环 + 设置命令菜单
    async def post_init(application):
        from telegram import BotCommand
        await application.bot.set_my_commands([
            BotCommand("start",        "显示帮助"),
            BotCommand("profiles",     "列出指纹环境"),
            BotCommand("open",         "打开环境 /open <编号>"),
            BotCommand("close",        "关闭环境 /close <编号>"),
            BotCommand("active",       "查看已开启环境"),
            BotCommand("acceptbm",    "接受BM邀请 /acceptbm <编号> <链接>"),
            BotCommand("info",        "查看环境详情 /info <编号>"),
            BotCommand("login",       "自动登录FB /login <编号>"),
            BotCommand("regdev",      "注册FB开发者 /regdev <编号>"),
            BotCommand("setupbm",    "配置FB账户（发Token自动获取ID）"),
            BotCommand("setfb",        "配置FB账户"),
            BotCommand("campaigns",    "列出广告系列"),
            BotCommand("report",       "数据报表"),
            BotCommand("normal",       "创建广告投放"),
            BotCommand("pause",        "暂停广告组"),
            BotCommand("resume",       "启动广告组"),
            BotCommand("addbudget",    "增加预算"),
            BotCommand("publish_last", "绑定素材并发布"),
            BotCommand("automonitor",  "自动监控开关"),
            BotCommand("status",       "查看当前状态"),
            BotCommand("clear",        "清除对话历史"),
        ])
        asyncio.create_task(monitor_loop(application))

    app.post_init = post_init

    return app
