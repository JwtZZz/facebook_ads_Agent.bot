"""基础命令：start / help / status / clear"""
import os
from telegram import Update
from telegram.ext import ContextTypes

from store.state import chat_histories, fb_configs, monitor_chats
from services.llm import MODEL


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "🤖 *主调度机器人已上线*\n\n"
        "*FB 广告命令：*\n"
        "/setfb — 配置 FB 账户\n"
        "/campaigns — 列出广告系列\n"
        "/report — 今日数据报表\n"
        "/normal — 创建广告投放\n"
        "/pause — 暂停广告组\n"
        "/resume — 启动广告组\n"
        "/addbudget — 增加预算\n"
        "/automonitor — 自动监控开关\n\n"
        "*素材命令：*\n"
        "发送视频文件 — 上传到 FB\n"
        "/publish_last — 绑定素材并发布\n\n"
        "*其他：*\n"
        "/status — 查看当前状态\n"
        "/clear — 清除对话历史\n\n"
        "直接发消息可与 AI 对话。"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await start(update, ctx)


async def clear_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_histories.pop(update.message.chat_id, None)
    await update.message.reply_text("✅ 对话历史已清除。")


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    cfg = fb_configs.get(chat_id)
    fb_info = (
        f"账户: {cfg.account}\n像素: {cfg.pixel_id or '未设置'}\n主页: {cfg.page_id or '未设置'}"
        if cfg else "未配置（使用 /setfb）"
    )
    monitoring = "✅ 开启" if monitor_chats.get(chat_id) else "❌ 关闭"
    history_len = len(chat_histories.get(chat_id, []))
    await update.message.reply_text(
        f"🤖 状态: 运行中\n"
        f"模型: {MODEL}\n"
        f"对话历史: {history_len} 条\n"
        f"自动监控: {monitoring}\n\n"
        f"📊 FB 账户:\n{fb_info}"
    )
