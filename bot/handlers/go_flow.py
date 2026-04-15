"""新版批量投放流程 /go —— 精简版

流程(4 步)：
    /go
      ↓ GO_TOKEN     （粘 token，拉取 BM 元数据）
      ↓ GO_ACCOUNTS  （多选账户）
      ↓ GO_PAGE      （全局主页单选，单主页自动跳过）
      ↓ GO_PIXEL     （全局像素单选，单像素自动跳过）
      ↓ 生成上传任务 → 发链接

其他所有配置（事件/URL/国家/设备/性别/年龄/预算/广告数/系列名/标题/文案）
都在上传页面（upload2.html）上填，最后点"一键创建 + 发布"时后端并发创建
campaign + adset 并 fanout 发布。
"""
import asyncio
import logging
import os

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from services.fb_fetch import fetch_bm_all
from services.web import create_multi_upload_task
from store.pool import (
    get_chat_pool, save_bm_metadata, save_last_used, has_bm_metadata,
)

logger = logging.getLogger(__name__)

# ── 状态常量（只保留 4 个必要状态） ──────────────────────────
GO_TOKEN, GO_ACCOUNTS, GO_PAGE, GO_PIXEL = range(100, 104)


# ══════════════════════════════════════════════════════════════════
# 入口 + token 粘贴
# ══════════════════════════════════════════════════════════════════

async def go_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/go — 批量投放入口"""
    chat_id = update.effective_chat.id
    ctx.chat_data["go_chat_id"] = chat_id

    # 进程内有 token 且池里有数据 → 跳过 token 直接进账户选择
    token = ctx.chat_data.get("go_token")
    if token and has_bm_metadata(chat_id):
        return await _show_account_selection(update, ctx)

    hint = ""
    if has_bm_metadata(chat_id):
        hint = "（之前已拉取过 BM 元数据，重粘 token 会刷新账户列表）\n"

    await update.message.reply_text(
        f"🚀 批量投放 /go\n\n"
        f"🔑 请粘贴 Access Token（EAA 开头）\n"
        f"{hint}"
        f"\n其他配置（定向/预算/URL 等）都在网页上填\n\n"
        f"发 /cancel 取消"
    )
    return GO_TOKEN


async def go_token_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到 token → 并发拉取 BM accounts/pages/pixels → 落盘 → 进账户选择"""
    token = (update.message.text or "").strip()
    if len(token) < 20:
        await update.message.reply_text("❌ Token 太短，请重新发送：")
        return GO_TOKEN

    msg = await update.message.reply_text("⏳ 正在拉取 BM 的账户 / 主页 / 像素...")

    loop = asyncio.get_event_loop()
    accounts, pages, pixels = await loop.run_in_executor(None, fetch_bm_all, token)

    if not accounts:
        await msg.edit_text(
            "❌ 没拉到任何广告账户。可能原因：\n"
            "  • Token 无效或已过期\n"
            "  • Token 没有 ads_management 权限\n\n"
            "请重新发送 Token："
        )
        return GO_TOKEN

    chat_id = update.effective_chat.id
    ctx.chat_data["go_token"] = token
    save_bm_metadata(chat_id, accounts, pages, pixels)

    await msg.edit_text(
        f"✅ 拉取完成\n"
        f"  • 账户: {len(accounts)}\n"
        f"  • 主页: {len(pages)}\n"
        f"  • 像素: {len(pixels)}"
    )

    return await _show_account_selection(update, ctx)


# ══════════════════════════════════════════════════════════════════
# 账户多选
# ══════════════════════════════════════════════════════════════════

async def _show_account_selection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """渲染账户多选按钮（toggle）"""
    chat_id = update.effective_chat.id
    pool = get_chat_pool(chat_id)
    accounts = pool.get("bm", {}).get("accounts", [])

    if not accounts:
        target_send = (
            update.message.reply_text if update.message else update.effective_chat.send_message
        )
        await target_send("❌ 池里没有账户，请先 /go 粘 token 拉取。")
        return ConversationHandler.END

    last_used = pool.get("last_used", {})
    preselect = set(last_used.get("account_ids") or [])
    selected = ctx.chat_data.get("go_selected_accounts_set")
    if selected is None:
        selected = preselect.copy() if preselect else set()
    ctx.chat_data["go_selected_accounts_set"] = selected
    ctx.chat_data["go_accounts_list"] = accounts

    keyboard = _build_account_keyboard(accounts, selected)
    text = (
        f"🏦 请选择要投放的广告账户（多选）\n"
        f"共 {len(accounts)} 个，已选 {len(selected)} 个\n"
        f"✓ 点击切换选中状态 → 点「下一步」继续"
    )
    target_send = (
        update.message.reply_text if update.message else update.effective_chat.send_message
    )
    await target_send(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return GO_ACCOUNTS


def _build_account_keyboard(accounts: list, selected: set) -> list:
    status_icons = {1: "🟢", 2: "🔴", 3: "🟡"}
    rows = []
    for i, acc in enumerate(accounts):
        acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
        name = acc.get("name", f"账户{i+1}")
        status = acc.get("account_status", 0)
        icon = status_icons.get(status, "⚪")
        check = "☑" if acc_id in selected else "☐"
        label = f"{check} {icon} {name[:20]} ({acc_id[-6:]})"
        rows.append([InlineKeyboardButton(label, callback_data=f"go_acc_toggle:{i}")])

    rows.append([
        InlineKeyboardButton("☑ 全选", callback_data="go_acc_all"),
        InlineKeyboardButton("☐ 清空", callback_data="go_acc_none"),
    ])
    rows.append([
        InlineKeyboardButton("✅ 下一步", callback_data="go_acc_next"),
    ])
    return rows


async def go_account_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """账户 toggle / 全选 / 清空 / 下一步 的统一回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    accounts = ctx.chat_data.get("go_accounts_list", [])
    selected = ctx.chat_data.get("go_selected_accounts_set", set())

    if data == "go_acc_all":
        selected = set()
        for acc in accounts:
            acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
            if acc_id:
                selected.add(acc_id)
    elif data == "go_acc_none":
        selected = set()
    elif data == "go_acc_next":
        if not selected:
            await query.answer("⚠️ 至少选一个账户", show_alert=True)
            return GO_ACCOUNTS
        selected_list = [
            acc for acc in accounts
            if (acc.get("account_id") or acc.get("id", "").replace("act_", "")) in selected
        ]
        ctx.chat_data["go_selected_accounts"] = selected_list
        return await _show_page_selection(update, ctx, query.message)
    elif data.startswith("go_acc_toggle:"):
        idx = int(data.split(":")[1])
        if 0 <= idx < len(accounts):
            acc = accounts[idx]
            acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
            if acc_id in selected:
                selected.remove(acc_id)
            else:
                selected.add(acc_id)

    ctx.chat_data["go_selected_accounts_set"] = selected
    keyboard = _build_account_keyboard(accounts, selected)
    text = (
        f"🏦 请选择要投放的广告账户（多选）\n"
        f"共 {len(accounts)} 个，已选 {len(selected)} 个"
    )
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return GO_ACCOUNTS


# ══════════════════════════════════════════════════════════════════
# 主页 / 像素
# ══════════════════════════════════════════════════════════════════

async def _show_page_selection(update, ctx, edit_msg):
    chat_id = update.effective_chat.id
    pool = get_chat_pool(chat_id)
    pages = pool.get("bm", {}).get("pages", [])

    if not pages:
        ctx.chat_data["go_page_id"] = ""
        ctx.chat_data["go_page_name"] = ""
        return await _show_pixel_selection(update, ctx, edit_msg)

    if len(pages) == 1:
        p = pages[0]
        ctx.chat_data["go_page_id"] = p.get("id", "")
        ctx.chat_data["go_page_name"] = p.get("name", "")
        return await _show_pixel_selection(update, ctx, edit_msg)

    last_used = pool.get("last_used", {})
    default_page = last_used.get("page_id", "")

    keyboard = []
    for i, p in enumerate(pages):
        pid = p.get("id", "")
        name = p.get("name", "")
        marker = " ✓" if pid == default_page else ""
        keyboard.append([InlineKeyboardButton(
            f"📄 {name[:30]}{marker}",
            callback_data=f"go_page:{i}",
        )])

    await edit_msg.edit_text(
        f"📄 请选择主页（所有选中账户共用）\n共 {len(pages)} 个",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    ctx.chat_data["go_pages_list"] = pages
    return GO_PAGE


async def go_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    pages = ctx.chat_data.get("go_pages_list", [])
    if 0 <= idx < len(pages):
        p = pages[idx]
        ctx.chat_data["go_page_id"] = p.get("id", "")
        ctx.chat_data["go_page_name"] = p.get("name", "")
    return await _show_pixel_selection(update, ctx, query.message)


async def _show_pixel_selection(update, ctx, edit_msg):
    chat_id = update.effective_chat.id
    pool = get_chat_pool(chat_id)
    pixels = pool.get("bm", {}).get("pixels", [])

    if not pixels:
        ctx.chat_data["go_pixel_id"] = ""
        return await _finalize(update, ctx, edit_msg)

    if len(pixels) == 1:
        ctx.chat_data["go_pixel_id"] = pixels[0].get("id", "")
        return await _finalize(update, ctx, edit_msg)

    last_used = pool.get("last_used", {})
    default_pixel = last_used.get("pixel_id", "")

    keyboard = []
    for i, px in enumerate(pixels):
        pxid = px.get("id", "")
        name = px.get("name", "")
        marker = " ✓" if pxid == default_pixel else ""
        keyboard.append([InlineKeyboardButton(
            f"📊 {name[:30]}{marker}",
            callback_data=f"go_pixel:{i}",
        )])

    await edit_msg.edit_text(
        f"📊 请选择像素（所有选中账户共用）",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    ctx.chat_data["go_pixels_list"] = pixels
    return GO_PIXEL


async def go_pixel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    pixels = ctx.chat_data.get("go_pixels_list", [])
    if 0 <= idx < len(pixels):
        ctx.chat_data["go_pixel_id"] = pixels[idx].get("id", "")
    return await _finalize(update, ctx, query.message)


# ══════════════════════════════════════════════════════════════════
# 最终：生成上传任务并发链接
# ══════════════════════════════════════════════════════════════════

async def _finalize(update, ctx, edit_msg):
    """收齐 token/accounts/page/pixel 后生成 upload task 并发链接"""
    chat_id = update.effective_chat.id
    d = ctx.chat_data

    token = d.get("go_token", "")
    accounts = d.get("go_selected_accounts", [])
    page_id = d.get("go_page_id", "")
    pixel_id = d.get("go_pixel_id", "")

    if not token or not accounts:
        await edit_msg.edit_text("❌ 缺少 token 或账户，流程中断。")
        return ConversationHandler.END

    # 构造 upload_task 的 targets 数组（只有账户元数据，其他字段在网页填）
    task_targets = []
    for acc in accounts:
        acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
        acc_name = acc.get("name", "") or f"Acct-{acc_id[-6:]}"
        task_targets.append({
            "account_id": acc_id,
            "account_name": acc_name,
            "account_alias": acc_id[-6:],
            "page_id": page_id,
            "pixel_id": pixel_id,
        })

    task_id = create_multi_upload_task(
        chat_id=chat_id,
        token=token,
        targets=task_targets,
    )

    # 保存 last_used（只记会持续用到的：账户和 page/pixel）
    save_last_used(
        chat_id,
        account_ids=[t["account_id"] for t in task_targets],
        page_id=page_id,
        pixel_id=pixel_id,
    )

    host = os.getenv("DASHBOARD_HOST", "localhost:8080")
    upload_url = f"http://{host}/upload?task={task_id}"

    summary = [
        f"✅ 已就绪 {len(task_targets)} 个账户",
        "",
    ]
    for t in task_targets:
        summary.append(f"  • {t['account_name']} ({t['account_alias']})")
    summary += [
        "",
        "其他所有配置（事件/URL/定向/预算/文案 等）都在网页上填。",
        "填完并传完素材后点「一键创建 + 发布」。",
    ]

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📎 打开批量投放页面", url=upload_url),
    ]])
    await edit_msg.edit_text("\n".join(summary), reply_markup=keyboard)
    return ConversationHandler.END


async def go_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/cancel 退出 /go 流程"""
    for k in list(ctx.chat_data.keys()):
        if k.startswith("go_"):
            ctx.chat_data.pop(k, None)
    if update.message:
        await update.message.reply_text("已取消 /go 流程")
    return ConversationHandler.END
