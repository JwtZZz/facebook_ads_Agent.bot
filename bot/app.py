"""Telegram Bot 初始化 + Handler 注册"""
import asyncio
import logging
import os

from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, CallbackQueryHandler, filters,
)

from bot.handlers.base import start, help_cmd, clear_cmd, status_cmd
from bot.handlers.go_flow import (
    GO_TOKEN, GO_ACCOUNTS, GO_PAGE, GO_PIXEL,
    go_start, go_token_input, go_account_toggle, go_page, go_pixel, go_cancel,
)
from services.llm import ask_llm
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
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("help",   help_cmd))
    app.add_handler(CommandHandler("clear",  clear_cmd))
    app.add_handler(CommandHandler("status", status_cmd))

    # 批量投放 /go — 4 步，其他配置在网页填
    go_conv = ConversationHandler(
        entry_points=[CommandHandler("go", go_start)],
        states={
            GO_TOKEN:    [MessageHandler(filters.TEXT & ~filters.COMMAND, go_token_input)],
            GO_ACCOUNTS: [CallbackQueryHandler(go_account_toggle, pattern=r"^go_acc_")],
            GO_PAGE:     [CallbackQueryHandler(go_page, pattern=r"^go_page:")],
            GO_PIXEL:    [CallbackQueryHandler(go_pixel, pattern=r"^go_pixel:")],
        },
        fallbacks=[CommandHandler("cancel", go_cancel)],
        conversation_timeout=600,
    )
    app.add_handler(go_conv)

    # 普通文字消息 → LLM
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        _handle_text,
    ))

    # 启动后设置命令菜单
    async def post_init(application):
        from telegram import BotCommand
        await application.bot.set_my_commands([
            BotCommand("start",  "显示帮助"),
            BotCommand("go",     "批量投放多账户"),
        ])
        await application.bot.delete_my_commands()
        await application.bot.set_my_commands([
            BotCommand("start", "开始使用"),
            BotCommand("go", "批量投放"),
        ])

    app.post_init = post_init

    return app
