"""新版投放流程 /go —— 多账户批量投放

流程:
    /go
      ↓ GO_TOKEN      （粘 token，拉取 BM 元数据）
      ↓ GO_ACCOUNTS   （多选账户）
      ↓ GO_PAGE       （全局主页单选）
      ↓ GO_PIXEL      （全局像素单选）
      ↓ GO_EVENT      （订阅 / PWA 二选一，应用暂不支持）
      ↓ GO_URL_PICK   （URL 按钮 + 新 URL 选项）
      ↓ GO_URL_INPUT  （可选：文本输入新 URL）
      ↓ GO_COUNTRY    （国家单选）
      ↓ GO_DEVICE     （设备单选）
      ↓ GO_GENDER     （性别单选）
      ↓ GO_AGE        （年龄范围单选）
      ↓ GO_BUDGET     （单账户日预算）
      ↓ GO_COUNT      （每账户广告数 N）
      ↓ GO_NAME       （系列基础名）
      ↓ GO_CONFIRM    （确认 → 并发创建 M 个系列 → 生成统一上传链接）

设计要点:
- Token 仅在内存 (chat_data)，不落盘
- BM 元数据 (accounts/pages/pixels) 写 pool.json，粘 token 刷新
- 每一步尽量用 last_used 做默认预选
"""
import asyncio
import logging
import os

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from services.fb_fetch import fetch_bm_all
from services.campaign import TargetSpec, create_targets_parallel
from services.web import create_multi_upload_task
from store.pool import (
    get_chat_pool, save_bm_metadata, push_url, save_last_used, has_bm_metadata,
)

logger = logging.getLogger(__name__)

# ── 状态常量 ───────────────────────────────────────────────────
(
    GO_TOKEN, GO_ACCOUNTS, GO_PAGE, GO_PIXEL, GO_EVENT,
    GO_URL_PICK, GO_URL_INPUT,
    GO_COUNTRY, GO_DEVICE, GO_GENDER, GO_AGE,
    GO_BUDGET, GO_COUNT, GO_NAME, GO_CONFIRM,
) = range(100, 115)

# ── 常量表 ────────────────────────────────────────────────────
EVENT_MAP = {
    "订阅": {"code": "SUBSCRIBE", "cta": "SUBSCRIBE", "label": "订阅"},
    "PWA":  {"code": "PURCHASE",  "cta": "SHOP_NOW",  "label": "PWA（购物）"},
}

COUNTRIES_ROW1 = [("🇧🇷 BR", "BR"), ("🇺🇸 US", "US"), ("🇮🇳 IN", "IN")]
COUNTRIES_ROW2 = [("🇵🇭 PH", "PH"), ("🇲🇽 MX", "MX"), ("🇮🇩 ID", "ID")]
COUNTRIES_ROW3 = [("🇻🇳 VN", "VN"), ("🇹🇭 TH", "TH"), ("🇨🇴 CO", "CO")]

DEVICES = [("📱 Android", "Android"), ("🍎 iOS", "iOS"), ("💻 All", "All")]
GENDERS = [("👥 全部", "0"), ("👨 男", "1"), ("👩 女", "2")]
AGES = [
    ("18-65（默认）", "18-65"),
    ("18-45", "18-45"),
    ("25-55", "25-55"),
    ("18-35", "18-35"),
]


# ══════════════════════════════════════════════════════════════════
# 入口 + token 粘贴
# ══════════════════════════════════════════════════════════════════

async def go_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/go — 多账户批量投放入口"""
    chat_id = update.effective_chat.id
    ctx.chat_data["go_chat_id"] = chat_id

    # 池里已有 token 内存态？（进程未重启时）
    token = ctx.chat_data.get("go_token")
    if token and has_bm_metadata(chat_id):
        # 跳过 token 输入，直接进账户选择
        return await _show_account_selection(update, ctx)

    hint = ""
    if has_bm_metadata(chat_id):
        hint = "（之前已拉取过 BM 元数据，此次重粘 token 会刷新账户列表）"

    await update.message.reply_text(
        f"🚀 批量投放 /go\n\n"
        f"🔑 请粘贴 Access Token（EAA 开头）\n"
        f"{hint}\n\n"
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

    # 保存到内存
    chat_id = update.effective_chat.id
    ctx.chat_data["go_token"] = token
    # 落盘元数据
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

    # 初始化选中集合：上次选过的作为预选
    last_used = pool.get("last_used", {})
    preselect = set(last_used.get("account_ids") or [])
    selected = ctx.chat_data.get("go_selected_accounts_set")
    if selected is None:
        selected = preselect.copy() if preselect else set()
    ctx.chat_data["go_selected_accounts_set"] = selected
    ctx.chat_data["go_accounts_list"] = accounts  # 固定顺序

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
    """把账户列表渲染成 toggle 按钮"""
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
        # 转成有序 list 按 accounts 顺序
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
# 主页 / 像素 / 事件
# ══════════════════════════════════════════════════════════════════

async def _show_page_selection(update, ctx, edit_msg):
    """显示主页单选"""
    chat_id = update.effective_chat.id
    pool = get_chat_pool(chat_id)
    pages = pool.get("bm", {}).get("pages", [])

    if not pages:
        # 没有主页，跳过
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
        return await _show_event_selection(update, ctx, edit_msg)

    if len(pixels) == 1:
        ctx.chat_data["go_pixel_id"] = pixels[0].get("id", "")
        return await _show_event_selection(update, ctx, edit_msg)

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
    return await _show_event_selection(update, ctx, query.message)


async def _show_event_selection(update, ctx, edit_msg):
    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default_event = last_used.get("event", "订阅")

    keyboard = [
        [
            InlineKeyboardButton(
                ("✓ " if default_event == "订阅" else "") + "📋 订阅",
                callback_data="go_event:订阅",
            ),
            InlineKeyboardButton(
                ("✓ " if default_event == "PWA" else "") + "🛒 PWA",
                callback_data="go_event:PWA",
            ),
        ],
        [
            InlineKeyboardButton("📱 应用（暂不支持）", callback_data="go_event:_app"),
        ],
    ]
    await edit_msg.edit_text(
        "🎯 请选择转化事件（所有账户共用）",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return GO_EVENT


async def go_event(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ev = query.data.split(":")[1]
    if ev == "_app":
        await query.answer("⚠️ 应用类型暂未实现", show_alert=True)
        return GO_EVENT
    if ev not in EVENT_MAP:
        ev = "订阅"
    ctx.chat_data["go_event"] = ev
    return await _show_url_selection(update, ctx, query.message)


# ══════════════════════════════════════════════════════════════════
# URL
# ══════════════════════════════════════════════════════════════════

async def _show_url_selection(update, ctx, edit_msg):
    pool = get_chat_pool(update.effective_chat.id)
    urls = pool.get("urls", [])[:5]

    keyboard = []
    for i, url in enumerate(urls):
        short = url if len(url) <= 45 else url[:42] + "..."
        keyboard.append([InlineKeyboardButton(f"🔗 {short}", callback_data=f"go_url:{i}")])
    keyboard.append([InlineKeyboardButton("✏️ 输入新 URL", callback_data="go_url:_new")])

    ctx.chat_data["go_urls_list"] = urls
    await edit_msg.edit_text(
        "🔗 请选择落地页 URL（或输入新的）",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return GO_URL_PICK


async def go_url_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split(":", 1)[1]
    if data == "_new":
        await query.edit_message_text(
            "✏️ 请输入新的落地页 URL（http/https 开头）："
        )
        return GO_URL_INPUT
    urls = ctx.chat_data.get("go_urls_list", [])
    try:
        idx = int(data)
        if 0 <= idx < len(urls):
            ctx.chat_data["go_url"] = urls[idx]
            return await _show_country_selection(update, ctx, query.message)
    except ValueError:
        pass
    return GO_URL_PICK


async def go_url_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = (update.message.text or "").strip()
    if not url.startswith("http"):
        await update.message.reply_text("❌ 必须以 http 或 https 开头，请重新输入：")
        return GO_URL_INPUT
    ctx.chat_data["go_url"] = url
    # 发新消息展示后续按钮（没有现有 msg 可 edit）
    return await _show_country_selection(update, ctx, None)


# ══════════════════════════════════════════════════════════════════
# 定向 - country / device / gender / age
# ══════════════════════════════════════════════════════════════════

async def _show_country_selection(update, ctx, edit_msg):
    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default = last_used.get("country", "BR")

    def _mark(code):
        return "✓ " if code == default else ""

    keyboard = []
    for row in (COUNTRIES_ROW1, COUNTRIES_ROW2, COUNTRIES_ROW3):
        keyboard.append([
            InlineKeyboardButton(_mark(code) + label, callback_data=f"go_country:{code}")
            for label, code in row
        ])

    text = "🌍 请选择投放国家"
    if edit_msg:
        await edit_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return GO_COUNTRY


async def go_country(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.chat_data["go_country"] = query.data.split(":")[1]
    return await _show_device_selection(update, ctx, query.message)


async def _show_device_selection(update, ctx, edit_msg):
    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default = last_used.get("device", "Android")

    keyboard = [[
        InlineKeyboardButton(("✓ " if code == default else "") + label,
                             callback_data=f"go_device:{code}")
        for label, code in DEVICES
    ]]
    await edit_msg.edit_text(
        "📱 请选择设备",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return GO_DEVICE


async def go_device(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.chat_data["go_device"] = query.data.split(":")[1]
    return await _show_gender_selection(update, ctx, query.message)


async def _show_gender_selection(update, ctx, edit_msg):
    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default = str(last_used.get("gender", 0))

    keyboard = [[
        InlineKeyboardButton(("✓ " if code == default else "") + label,
                             callback_data=f"go_gender:{code}")
        for label, code in GENDERS
    ]]
    await edit_msg.edit_text(
        "🧑 请选择性别",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return GO_GENDER


async def go_gender(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.chat_data["go_gender"] = int(query.data.split(":")[1])
    return await _show_age_selection(update, ctx, query.message)


async def _show_age_selection(update, ctx, edit_msg):
    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default = f"{last_used.get('age_min', 18)}-{last_used.get('age_max', 65)}"

    rows = []
    row_buf = []
    for label, code in AGES:
        marker = "✓ " if code == default else ""
        row_buf.append(InlineKeyboardButton(marker + label, callback_data=f"go_age:{code}"))
        if len(row_buf) == 2:
            rows.append(row_buf)
            row_buf = []
    if row_buf:
        rows.append(row_buf)

    await edit_msg.edit_text(
        "🎂 请选择年龄范围",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return GO_AGE


async def go_age(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    age_str = query.data.split(":")[1]
    age_min, age_max = map(int, age_str.split("-"))
    ctx.chat_data["go_age_min"] = age_min
    ctx.chat_data["go_age_max"] = age_max

    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default_budget = last_used.get("budget", 20.0)
    await query.edit_message_text(
        f"💰 请输入单账户日预算（美元，上次: ${default_budget}）："
    )
    return GO_BUDGET


# ══════════════════════════════════════════════════════════════════
# 预算 / 数量 / 系列名
# ══════════════════════════════════════════════════════════════════

async def go_budget(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    try:
        budget = float(text)
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字（例如 20）：")
        return GO_BUDGET
    ctx.chat_data["go_budget"] = budget

    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default_count = last_used.get("count", 10)
    await update.message.reply_text(
        f"🎬 请输入每账户广告数量（1-20，上次: {default_count}）："
    )
    return GO_COUNT


async def go_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    try:
        count = int(text)
        if count < 1 or count > 20:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ 请输入 1-20 的整数：")
        return GO_COUNT
    ctx.chat_data["go_count"] = count

    last_used = get_chat_pool(update.effective_chat.id).get("last_used", {})
    default_name = last_used.get("base_name", "")
    hint = f"（上次: {default_name}）" if default_name else ""
    await update.message.reply_text(
        f"📝 请输入系列基础名 {hint}\n"
        f"命名格式建议: 产品-渠道-优化师-日期\n"
        f"例如: bet7-778-DT-0415\n"
        f"系统会自动在末尾加上账户 alias，避免 FB 后台重名"
    )
    return GO_NAME


async def go_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name = (update.message.text or "").strip()
    if not name or name.startswith("/"):
        await update.message.reply_text("❌ 名称不能为空或以 / 开头，重试：")
        return GO_NAME
    ctx.chat_data["go_base_name"] = name

    return await _show_confirm(update, ctx)


# ══════════════════════════════════════════════════════════════════
# 确认 + 并发创建
# ══════════════════════════════════════════════════════════════════

async def _show_confirm(update, ctx):
    d = ctx.chat_data
    accounts = d["go_selected_accounts"]
    event_cfg = EVENT_MAP.get(d["go_event"], EVENT_MAP["订阅"])

    # 预览系列名
    preview_names = []
    for acc in accounts[:3]:
        acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
        alias = acc_id[-6:]
        preview_names.append(f"  • {d['go_base_name']}-{alias}")
    if len(accounts) > 3:
        preview_names.append(f"  • ...（共 {len(accounts)} 个）")

    gender_label = {0: "全部", 1: "男", 2: "女"}[d["go_gender"]]
    total_ads = len(accounts) * d["go_count"]
    total_budget = len(accounts) * d["go_budget"]

    summary = (
        f"📋 请确认批量投放配置\n"
        f"{'─' * 28}\n"
        f"🏦 账户数: {len(accounts)}\n"
        f"📄 主页: {d.get('go_page_name', '') or d.get('go_page_id', '') or '未设置'}\n"
        f"📊 像素: {d.get('go_pixel_id', '') or '未设置'}\n"
        f"🎯 事件: {event_cfg['label']}\n"
        f"🔗 URL: {d['go_url']}\n"
        f"🌍 {d['go_country']} / {d['go_device']} / {gender_label} / {d['go_age_min']}-{d['go_age_max']}岁\n"
        f"💰 单账户预算: ${d['go_budget']}/天\n"
        f"🎬 每账户广告数: {d['go_count']}\n"
        f"📝 系列命名预览:\n" + "\n".join(preview_names) + "\n"
        f"{'─' * 28}\n"
        f"📊 总计: {len(accounts)} 系列 / {len(accounts)} 组 / {total_ads} 广告\n"
        f"💰 总预算: ${total_budget:.2f}/天"
    )

    keyboard = [[
        InlineKeyboardButton("✅ 确认创建", callback_data="go_confirm:yes"),
        InlineKeyboardButton("❌ 取消", callback_data="go_confirm:no"),
    ]]

    target_send = (
        update.message.reply_text if update.message else update.effective_chat.send_message
    )
    await target_send(summary, reply_markup=InlineKeyboardMarkup(keyboard))
    return GO_CONFIRM


async def go_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]
    if action != "yes":
        await query.edit_message_text("❌ 已取消")
        return ConversationHandler.END

    d = ctx.chat_data
    chat_id = update.effective_chat.id
    token = d["go_token"]
    accounts = d["go_selected_accounts"]
    event_cfg = EVENT_MAP[d["go_event"]]

    await query.edit_message_text(
        f"⏳ 正在为 {len(accounts)} 个账户并发创建广告系列..."
    )

    # 构造 TargetSpec 列表
    specs = []
    for acc in accounts:
        acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
        acc_name = acc.get("name", "") or f"Acct-{acc_id[-6:]}"
        alias = acc_id[-6:]
        camp_name = f"{d['go_base_name']}-{alias}"
        specs.append(TargetSpec(
            account_id=acc_id,
            account_name=acc_name,
            token=token,
            page_id=d.get("go_page_id", ""),
            pixel_id=d.get("go_pixel_id", ""),
            campaign_name=camp_name,
            daily_budget_usd=d["go_budget"],
            country=d["go_country"],
            device_os=d["go_device"],
            age_min=d["go_age_min"],
            age_max=d["go_age_max"],
            gender=d["go_gender"],
            conversion_event=event_cfg["code"],
            landing_url=d["go_url"],
            cta=event_cfg["cta"],
            count=d["go_count"],
        ))

    results = await create_targets_parallel(specs)

    ok_specs = [s for s in results if s.campaign_id]
    failed = [s for s in results if not s.campaign_id]

    if not ok_specs:
        err_lines = "\n".join(f"  ✗ {s.account_name}: {s.error[:60]}" for s in failed)
        await query.edit_message_text(
            f"❌ 所有账户都创建失败:\n{err_lines}\n\n请检查 Token 或账户权限。"
        )
        return ConversationHandler.END

    # 构造 upload_task 的 targets 数组
    task_targets = []
    for s in ok_specs:
        task_targets.append({
            "account_id":    s.account_id,
            "account_name":  s.account_name,
            "account_alias": s.account_id[-6:],
            "page_id":       s.page_id,
            "pixel_id":      s.pixel_id,
            "campaign_id":   s.campaign_id,
            "adset_id":      s.adset_id,
            "landing_url":   s.landing_url,
            "cta":           s.cta,
            "event":         s.conversion_event,
        })

    task_id = create_multi_upload_task(
        chat_id=chat_id,
        token=token,
        targets=task_targets,
        count=d["go_count"],
    )

    # 写 last_used + URL 历史
    save_last_used(
        chat_id,
        account_ids=[s.account_id for s in ok_specs],
        page_id=d.get("go_page_id", ""),
        pixel_id=d.get("go_pixel_id", ""),
        event=d["go_event"],
        url=d["go_url"],
        country=d["go_country"],
        device=d["go_device"],
        gender=d["go_gender"],
        age_min=d["go_age_min"],
        age_max=d["go_age_max"],
        budget=d["go_budget"],
        count=d["go_count"],
        base_name=d["go_base_name"],
    )
    push_url(chat_id, d["go_url"])

    host = os.getenv("DASHBOARD_HOST", "localhost:8080")
    upload_url = f"http://{host}/upload?task={task_id}"

    lines = [
        f"✅ 已为 {len(ok_specs)}/{len(accounts)} 个账户创建系列",
        "",
    ]
    for s in ok_specs:
        lines.append(f"  ✓ {s.account_name} → camp {s.campaign_id[-8:]}")
    for s in failed:
        lines.append(f"  ✗ {s.account_name}: {s.error[:60]}")
    lines += [
        "",
        "点击下方按钮打开上传页，一次拖素材到所有账户。",
    ]

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📎 打开素材上传页面", url=upload_url),
    ]])
    await query.edit_message_text("\n".join(lines), reply_markup=keyboard)
    return ConversationHandler.END


async def go_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/cancel 退出 /go 流程"""
    # 清理本次 /go 会话的临时数据
    for k in list(ctx.chat_data.keys()):
        if k.startswith("go_"):
            ctx.chat_data.pop(k, None)
    if update.message:
        await update.message.reply_text("已取消 /go 流程")
    return ConversationHandler.END
