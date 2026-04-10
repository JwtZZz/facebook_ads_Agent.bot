"""Report Bot — BM/广告户封禁自动通知"""
import asyncio
import logging
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, CallbackQueryHandler, filters,
    ContextTypes,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

GRAPH = "https://graph.facebook.com/v20.0"
BOT_TOKEN = os.getenv("REPORT_BOT_TOKEN", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))

# 状态常量
ST_BM_ID, ST_TOKEN, ST_CONFIRM = range(3)

# 全局监控数据: {chat_id: {"bm_ids": [...], "token": str, "accounts": [...], "enabled": bool}}
monitor_data: dict[int, dict] = {}
# 已通知的异常账户，避免重复通知: {account_id_status: True}
notified: set[str] = set()

ACCOUNT_STATUS_MAP = {
    1: "✅ 正常", 2: "⛔ 已封禁", 3: "⚠️ 未结算",
    7: "⚠️ 风控审核中", 8: "⚠️ 待结算", 9: "⚠️ 宽限期",
    100: "⚠️ 待关闭", 101: "⛔ 已关闭",
}


def fb_get(path: str, token: str, params: dict = None) -> dict:
    p = {"access_token": token}
    if params:
        p.update(params)
    resp = requests.get(f"{GRAPH}/{path}", params=p)
    return resp.json()


# ── 对话流程 ──────────────────────────────────────────────────

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    info = monitor_data.get(chat_id)
    if info and info.get("enabled"):
        count = len(info.get("accounts", []))
        await update.message.reply_text(
            f"🤖 监控运行中 — {count} 个广告账户\n"
            f"每 {CHECK_INTERVAL} 秒检查一次\n\n"
            f"/monitor — 重新设置监控\n"
            f"/stop — 停止监控\n"
            f"/status — 查看所有账户状态"
        )
    else:
        await update.message.reply_text(
            "🤖 广告户封禁监控机器人\n\n"
            "发 /monitor 开始设置监控\n"
            "发 /status 手动查一次状态"
        )


async def monitor_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """入口：开始设置监控"""
    await update.message.reply_text(
        "📋 请输入要监控的 BM ID\n"
        "多个用逗号或换行分隔\n\n"
        "例如：\n"
        "155378867237811\n"
        "或 155378867237811, 267489012345678\n\n"
        "发 /cancel 取消"
    )
    return ST_BM_ID


async def receive_bm_ids(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到 BM ID"""
    text = update.message.text.strip()
    # 解析多个 BM ID
    bm_ids = [b.strip() for b in text.replace("\n", ",").split(",") if b.strip()]
    bm_ids = [b for b in bm_ids if b.isdigit()]

    if not bm_ids:
        await update.message.reply_text("❌ 没有找到有效的 BM ID，请重新输入纯数字：")
        return ST_BM_ID

    ctx.chat_data["bm_ids"] = bm_ids
    await update.message.reply_text(
        f"✅ 收到 {len(bm_ids)} 个 BM ID\n\n"
        f"🔑 请发送 Access Token\n"
        f"（需要有这些 BM 的管理权限）"
    )
    return ST_TOKEN


async def receive_token(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到 Token → 拉取所有 BM 下的广告账户"""
    token = update.message.text.strip()
    if len(token) < 20:
        await update.message.reply_text("❌ Token 太短，请重新输入：")
        return ST_TOKEN

    bm_ids = ctx.chat_data.get("bm_ids", [])
    ctx.chat_data["token"] = token

    msg = await update.message.reply_text(f"⏳ 正在查询 {len(bm_ids)} 个 BM 下的广告账户...")

    all_accounts = []
    errors = []

    for bm_id in bm_ids:
        try:
            data = fb_get(f"{bm_id}/owned_ad_accounts", token, {
                "fields": "id,account_id,name,account_status,disable_reason",
                "limit": "100",
            })

            if "error" in data:
                err = data["error"]
                errors.append(f"BM {bm_id}: {err.get('message', '未知错误')[:80]}")
                continue

            accounts = data.get("data", [])
            for acc in accounts:
                acc["_bm_id"] = bm_id
                all_accounts.append(acc)

        except Exception as e:
            errors.append(f"BM {bm_id}: {str(e)[:80]}")

    if not all_accounts and errors:
        await msg.edit_text(
            "❌ 查询失败：\n" + "\n".join(errors) + "\n\n请检查 BM ID 和 Token 是否正确，重新发 /monitor"
        )
        return ConversationHandler.END

    ctx.chat_data["accounts"] = all_accounts

    # 显示账户列表
    lines = [f"📋 找到 {len(all_accounts)} 个广告账户：\n"]
    for acc in all_accounts:
        name = acc.get("name", "")
        acc_id = acc.get("account_id", "")
        status = acc.get("account_status", 1)
        status_text = ACCOUNT_STATUS_MAP.get(status, f"未知({status})")
        card = name[-4:] if len(name) >= 4 else "—"
        lines.append(f"• {name} ({acc_id}) {status_text} 卡尾号:{card}")

    if errors:
        lines.append(f"\n⚠️ 部分 BM 查询失败：")
        lines.extend([f"• {e}" for e in errors])

    lines.append(f"\n确认监控这 {len(all_accounts)} 个账户？")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 确认开始监控", callback_data="confirm_yes"),
         InlineKeyboardButton("❌ 取消", callback_data="confirm_no")]
    ])

    await msg.edit_text("\n".join(lines), reply_markup=keyboard)
    return ST_CONFIRM


async def confirm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """确认或取消"""
    query = update.callback_query
    await query.answer()

    if query.data != "confirm_yes":
        await query.edit_message_text("❌ 已取消。发 /monitor 重新设置。")
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    accounts = ctx.chat_data.get("accounts", [])
    token = ctx.chat_data.get("token", "")
    bm_ids = ctx.chat_data.get("bm_ids", [])

    monitor_data[chat_id] = {
        "bm_ids": bm_ids,
        "token": token,
        "accounts": accounts,
        "enabled": True,
    }

    await query.edit_message_text(
        f"✅ 监控已开启！\n\n"
        f"📋 监控 {len(accounts)} 个广告账户\n"
        f"⏱ 每 {CHECK_INTERVAL} 秒检查一次\n\n"
        f"发现封禁/异常会立即通知你\n"
        f"发 /stop 停止 | /status 手动查"
    )
    return ConversationHandler.END


async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ 已取消。")
    return ConversationHandler.END


async def stop_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in monitor_data:
        monitor_data[chat_id]["enabled"] = False
    notified.clear()
    await update.message.reply_text("⏹ 监控已停止。发 /monitor 重新开启。")


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """手动查一次所有账户状态"""
    chat_id = update.effective_chat.id
    info = monitor_data.get(chat_id)

    if not info or not info.get("accounts"):
        await update.message.reply_text("⚠️ 还没设置监控，发 /monitor 开始设置。")
        return

    token = info["token"]
    accounts = info["accounts"]
    msg = await update.message.reply_text(f"⏳ 正在查询 {len(accounts)} 个账户状态...")

    lines = [f"📊 账户状态报告 — {datetime.now().strftime('%H:%M:%S')}\n"]
    normal = 0
    abnormal = 0

    for acc in accounts:
        acc_id = acc.get("account_id", "")
        try:
            data = fb_get(f"act_{acc_id}", token, {
                "fields": "name,account_status,disable_reason",
            })
            name = data.get("name", acc.get("name", ""))
            status = data.get("account_status", 1)
            status_text = ACCOUNT_STATUS_MAP.get(status, f"未知({status})")
            card = name[-4:] if len(name) >= 4 else "—"

            if status == 1:
                normal += 1
                lines.append(f"✅ {name} ({acc_id}) 卡:{card}")
            else:
                abnormal += 1
                lines.append(f"⛔ {name} ({acc_id}) 卡:{card} → {status_text}")
        except Exception as e:
            abnormal += 1
            lines.append(f"❓ act_{acc_id} → 查询失败")

    lines.insert(1, f"🟢 正常: {normal}  🔴 异常: {abnormal}\n")
    await msg.edit_text("\n".join(lines))


# ── 后台监控循环 ──────────────────────────────────────────────

async def check_loop(app):
    """后台循环检查所有监控中的账户"""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)

        for chat_id, info in list(monitor_data.items()):
            if not info.get("enabled"):
                continue

            token = info.get("token", "")
            accounts = info.get("accounts", [])
            if not token or not accounts:
                continue

            for acc in accounts:
                acc_id = acc.get("account_id", "")
                try:
                    data = fb_get(f"act_{acc_id}", token, {
                        "fields": "name,account_status,disable_reason",
                    })

                    if "error" in data:
                        # Token 可能过期
                        continue

                    name = data.get("name", acc.get("name", ""))
                    status = data.get("account_status", 1)
                    disable_reason = data.get("disable_reason", 0)
                    card = name[-4:] if len(name) >= 4 else "未知"

                    if status != 1:
                        key = f"{acc_id}_{status}"
                        if key not in notified:
                            notified.add(key)
                            status_text = ACCOUNT_STATUS_MAP.get(status, f"未知({status})")
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            text = (
                                f"🚨 <b>广告户异常</b>\n"
                                f"{'━' * 20}\n"
                                f"户名：<b>{name}</b>\n"
                                f"户编号：<code>{acc_id}</code>\n"
                                f"状态：{status_text}\n"
                                f"卡片尾号：<b>{card}</b>\n"
                                f"封禁原因码：{disable_reason}\n"
                                f"{'━' * 20}\n"
                                f"时间：{now}"
                            )
                            try:
                                await app.bot.send_message(chat_id, text, parse_mode="HTML")
                                logger.info(f"已通知 [{chat_id}]: {name} ({acc_id}) {status_text}")
                            except Exception as e:
                                logger.error(f"通知发送失败: {e}")
                    else:
                        # 恢复正常
                        recover_keys = [k for k in notified if k.startswith(f"{acc_id}_")]
                        if recover_keys:
                            for k in recover_keys:
                                notified.discard(k)
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            text = (
                                f"✅ <b>广告户恢复正常</b>\n"
                                f"{'━' * 20}\n"
                                f"户名：<b>{name}</b>\n"
                                f"户编号：<code>{acc_id}</code>\n"
                                f"{'━' * 20}\n"
                                f"时间：{now}"
                            )
                            try:
                                await app.bot.send_message(chat_id, text, parse_mode="HTML")
                            except Exception:
                                pass

                except Exception as e:
                    logger.error(f"检查账户失败 {acc_id}: {e}")


# ── 启动 ──────────────────────────────────────────────────────

def build_app():
    if not BOT_TOKEN:
        raise RuntimeError("REPORT_BOT_TOKEN 未设置")

    builder = ApplicationBuilder().token(BOT_TOKEN)
    proxy = os.getenv("TELEGRAM_PROXY", "")
    if proxy:
        builder = builder.proxy(proxy).get_updates_proxy(proxy)

    app = builder.build()

    # 命令
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("status", status_cmd))

    # 监控设置对话
    conv = ConversationHandler(
        entry_points=[CommandHandler("monitor", monitor_cmd)],
        states={
            ST_BM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_bm_ids)],
            ST_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_token)],
            ST_CONFIRM: [CallbackQueryHandler(confirm_callback, pattern=r"^confirm_")],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        conversation_timeout=300,
    )
    app.add_handler(conv)

    # 后台循环
    async def post_init(application):
        asyncio.create_task(check_loop(application))

    app.post_init = post_init

    return app


if __name__ == "__main__":
    app = build_app()
    logger.info("=== Report Bot 已启动 ===")
    app.run_polling()
