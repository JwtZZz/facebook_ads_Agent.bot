"""Telegram Bot 初始化 + Handler 注册"""
import asyncio
import logging
import os

from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters,
)

from bot.handlers.base  import start, help_cmd, clear_cmd, status_cmd
from bot.handlers.fb_ads import (
    setfb, campaigns_cmd, report_cmd,
    pause_cmd, resume_cmd, addbudget_cmd,
    onedollar_cmd, publish_last_cmd, automonitor_cmd,
)
from bot.handlers.media import upload_video_cmd
from services.llm       import ask_llm
from services.monitor   import monitor_loop
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def _handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """普通文字消息 → LLM"""
    if not update.message or not update.message.text:
        return
    user = update.message.from_user
    logger.info(f"[{update.message.chat_id}] {user.first_name}: {update.message.text}")
    reply = await ask_llm(update.message.chat_id, update.message.text)
    await update.message.reply_text(reply)


def build_app():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN 未设置")

    app = ApplicationBuilder().token(token).build()

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
    app.add_handler(CommandHandler("onedollar",    onedollar_cmd))
    app.add_handler(CommandHandler("publish_last", publish_last_cmd))
    app.add_handler(CommandHandler("automonitor",  automonitor_cmd))

    # 素材：直接发视频文件触发
    app.add_handler(MessageHandler(
        (filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND,
        upload_video_cmd,
    ))

    # 文字消息 → LLM
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_text))

    # 启动后台监控循环
    async def post_init(application):
        asyncio.create_task(monitor_loop(application))

    app.post_init = post_init

    return app
