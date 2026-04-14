"""FB 广告命令：配置、报表、系列管理、预算、监控"""
import functools
import logging
import os
import requests as _requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from fb import FBConfig, FBError
from fb.insights import parse_action, parse_action_value, format_report_row
from store.state import fb_configs, monitor_chats, get_fb
from services.campaign import normal_flow

GRAPH = "https://graph.facebook.com/v20.0"

logger = logging.getLogger(__name__)


def _require_fb(func):
    """装饰器：命令需要先配置 FB 账户"""
    @functools.wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not get_fb(update.effective_chat.id):
            await update.message.reply_text(
                "⚠️ 未配置 FB 账户，请先发送：\n"
                "`/setfb <token> <account_id> [pixel_id] [page_id]`",
                parse_mode="Markdown",
            )
            return
        return await func(update, ctx)
    return wrapper


# ── 账户配置 ────────────────────────────────────────────────────
async def setfb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /setfb <access_token> <ad_account_id> [pixel_id] [page_id]
    """
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "用法：\n`/setfb <access_token> <ad_account_id> [pixel_id] [page_id]`",
            parse_mode="Markdown",
        )
        return
    chat_id = update.effective_chat.id
    fb_configs[chat_id] = FBConfig(
        access_token=args[0],
        ad_account_id=args[1],
        pixel_id=args[2] if len(args) > 2 else "",
        page_id=args[3] if len(args) > 3 else "",
    )
    await update.message.reply_text(
        f"✅ FB 账户已配置\n"
        f"账户: act_{args[1]}\n"
        f"像素: {args[2] if len(args) > 2 else '未设置'}\n"
        f"主页: {args[3] if len(args) > 3 else '未设置'}"
    )


# ── 广告系列列表 ────────────────────────────────────────────────
@_require_fb
async def campaigns_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    fb = get_fb(update.effective_chat.id)
    msg = await update.message.reply_text("⏳ 正在拉取广告系列...")
    try:
        camps = fb.list_campaigns("ACTIVE") + fb.list_campaigns("PAUSED")
        if not camps:
            await msg.edit_text("当前无广告系列。")
            return
        lines = ["📋 *广告系列列表：*\n"]
        for c in camps[:20]:
            budget = int(c.get("daily_budget", 0)) / 100
            icon = "🟢" if c["status"] == "ACTIVE" else "⏸"
            lines.append(f"{icon} `{c['id']}` — {c['name']} (${budget:.0f}/天)")
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
    except FBError as e:
        await msg.edit_text(f"❌ FB API 错误: {e}")


# ── 数据报表 ────────────────────────────────────────────────────
@_require_fb
async def report_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /report [today|yesterday|last_7d|last_30d] [campaign_id]
    """
    fb  = get_fb(update.effective_chat.id)
    cfg = fb.cfg

    date_preset = "today"
    campaign_id = None
    for arg in (ctx.args or []):
        if arg in ("today", "yesterday", "last_7d", "last_30d"):
            date_preset = arg
        elif arg.isdigit():
            campaign_id = arg

    object_id = campaign_id or cfg.account
    msg = await update.message.reply_text(f"⏳ 正在拉取数据（{date_preset}）...")

    try:
        rows = fb.get_insights(object_id, level="adset", date_preset=date_preset)
        if not rows:
            await msg.edit_text(f"📭 {date_preset} 暂无数据。")
            return

        total_spend    = sum(float(r.get("spend", 0)) for r in rows)
        total_revenue  = sum(parse_action_value(r, "offsite_conversion.fb_pixel_purchase") for r in rows)
        total_charges  = sum(parse_action(r, "offsite_conversion.fb_pixel_purchase") for r in rows)
        total_regs     = sum(parse_action(r, "offsite_conversion.fb_pixel_complete_registration") for r in rows)
        roas = (total_revenue / total_spend * 100) if total_spend > 0 else 0
        cpa  = (total_spend / total_charges) if total_charges > 0 else 0

        lines = [
            f"📊 *{date_preset} 数据报表*（{len(rows)} 个广告组）",
            "─" * 28,
            f"💰 总消耗: ${total_spend:.2f}",
            f"📝 总注册: {total_regs:.0f}",
            f"🛒 总首充: {total_charges:.0f}",
            f"💵 总充值: ${total_revenue:.2f}",
            f"📈 ROAS: {roas:.0f}%   🎯 CPA: ${cpa:.2f}",
            "",
        ]
        rows_sorted = sorted(rows, key=lambda r: float(r.get("spend", 0)), reverse=True)
        for row in rows_sorted[:10]:
            lines.append(format_report_row(row))
            lines.append("")

        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
    except FBError as e:
        await msg.edit_text(f"❌ FB API 错误: {e}")


# ── 暂停 / 启动 ─────────────────────────────────────────────────
@_require_fb
async def pause_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/pause <id>"""
    if not ctx.args:
        await update.message.reply_text("用法：`/pause <广告组ID>`", parse_mode="Markdown")
        return
    fb = get_fb(update.effective_chat.id)
    try:
        fb.set_adset_status(ctx.args[0], "PAUSED")
        await update.message.reply_text(f"✅ 已暂停: `{ctx.args[0]}`", parse_mode="Markdown")
    except FBError as e:
        await update.message.reply_text(f"❌ {e}")


@_require_fb
async def resume_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/resume <id>"""
    if not ctx.args:
        await update.message.reply_text("用法：`/resume <广告组ID>`", parse_mode="Markdown")
        return
    fb = get_fb(update.effective_chat.id)
    try:
        fb.set_adset_status(ctx.args[0], "ACTIVE")
        await update.message.reply_text(f"✅ 已启动: `{ctx.args[0]}`", parse_mode="Markdown")
    except FBError as e:
        await update.message.reply_text(f"❌ {e}")


# ── 增加预算 ────────────────────────────────────────────────────
@_require_fb
async def addbudget_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /addbudget <campaign_id> <金额>
    /addbudget <campaign_id> +30%
    """
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "用法：`/addbudget <campaign_id> <金额 或 +30%>`",
            parse_mode="Markdown",
        )
        return
    fb = get_fb(update.effective_chat.id)
    campaign_id, amount_str = ctx.args[0], ctx.args[1]
    try:
        data = fb._req("GET", campaign_id, params={"fields": "daily_budget,name"})
        current = int(data.get("daily_budget", 0)) / 100
        name    = data.get("name", campaign_id)

        if amount_str.startswith("+") and amount_str.endswith("%"):
            pct = float(amount_str[1:-1]) / 100
            new_budget = current * (1 + pct)
        else:
            new_budget = float(amount_str)

        fb.update_campaign_budget(campaign_id, new_budget)
        await update.message.reply_text(
            f"✅ 预算已更新\n{name}\n${current:.2f} → ${new_budget:.2f}/天"
        )
    except (FBError, ValueError) as e:
        await update.message.reply_text(f"❌ {e}")


# ── 参数解析 ────────────────────────────────────────────────────
# 国家代码列表（常用）
_COUNTRIES = {
    "BR", "US", "IN", "ID", "PH", "VN", "TH", "MX", "CO", "AR",
    "PE", "CL", "EC", "NG", "EG", "KE", "ZA", "PK", "BD", "MY",
    "SG", "JP", "KR", "TW", "HK", "GB", "DE", "FR", "IT", "ES",
    "PT", "RU", "TR", "SA", "AE", "AU", "CA", "NZ",
}


def _parse_extra_args(args: list[str]) -> dict:
    """
    解析 /normal 的额外参数
    返回 {"mode", "landing_url", "country", "device_os", "age_min", "age_max", "gender"}
    """
    result = {
        "mode": "转化",
        "landing_url": "",
        "country": "",
        "device_os": "Android",
        "age_min": 18,
        "age_max": 65,
        "gender": 0,
    }
    for arg in args:
        upper = arg.upper()
        # 模式
        if arg in ("互动", "engagement"):
            result["mode"] = "互动"
        # 国家
        elif upper in _COUNTRIES:
            result["country"] = upper
        # 设备
        elif upper in ("ANDROID", "IOS", "ALL"):
            result["device_os"] = {"ANDROID": "Android", "IOS": "iOS", "ALL": "All"}[upper]
        # 性别
        elif arg in ("male", "男", "m"):
            result["gender"] = 1
        elif arg in ("female", "女", "f"):
            result["gender"] = 2
        # 年龄范围 如 18-45
        elif "-" in arg and arg.replace("-", "").isdigit():
            parts = arg.split("-")
            if len(parts) == 2:
                result["age_min"] = int(parts[0])
                result["age_max"] = int(parts[1])
        # URL
        elif arg.startswith("http"):
            result["landing_url"] = arg
    return result


# ── 正常跑法 ──────────────────────────────────────────────────
@_require_fb
async def normal_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /normal <系列名> <组数> <日预算> [参数...]
    正常跑法：广告系列预算（CBO），所有广告组共享预算
    """
    if len(ctx.args) < 3:
        await update.message.reply_text(
            "用法：`/normal <系列名> <组数> <日预算> [参数...]`\n\n"
            "示例命名：产品-渠道号-优化师-日期-系列几\n"
            "如：`bet7-778-DT-0402-1`\n\n"
            "可选参数（顺序随意）：\n"
            "• `BR` / `US` — 国家（默认BR）\n"
            "• `Android` / `iOS` / `All` — 设备（默认Android）\n"
            "• `18-45` — 年龄范围（默认18-65）\n"
            "• `male` / `female` — 性别（默认全部）\n"
            "• `https://...` — 落地页（必填）\n\n"
            "示例：\n"
            "`/normal bet7-778-DT-0402-1 5 20 https://xxx.com BR`\n"
            "`/normal bet7-778-DT-0402-1 3 50 https://xxx.com US iOS 25-55 male`",
            parse_mode="Markdown",
        )
        return

    fb = get_fb(update.effective_chat.id)
    camp_name, count_str, budget_str = ctx.args[0], ctx.args[1], ctx.args[2]
    opts = _parse_extra_args(ctx.args[3:])

    landing_url = opts["landing_url"]
    if not landing_url:
        await update.message.reply_text("❌ 正常跑法需要提供落地页 URL")
        return

    try:
        count = int(count_str)
        daily_budget = float(budget_str)
    except ValueError:
        await update.message.reply_text("❌ 组数必须是整数，预算必须是数字")
        return

    if count > 100:
        await update.message.reply_text("⚠️ 最多一次创建 100 组。")
        return

    country_label = opts["country"] or "BR(默认)"
    gender_label = {0: "全部", 1: "男", 2: "女"}[opts["gender"]]
    msg = await update.message.reply_text(
        f"⏳ 创建正常跑法（CBO 广告系列预算）...\n"
        f"系列: {camp_name} | {count} 组 | 系列预算 ${daily_budget}/天\n"
        f"定向: {country_label} | {opts['device_os']} | {opts['age_min']}-{opts['age_max']}岁 | {gender_label}"
    )
    try:
        camp_id, adset_ids = normal_flow(
            fb, camp_name, count,
            daily_budget_usd=daily_budget,
            country=opts["country"],
            device_os=opts["device_os"],
            age_min=opts["age_min"],
            age_max=opts["age_max"],
            gender=opts["gender"],
        )

        ctx.chat_data["last_campaign_id"] = camp_id
        ctx.chat_data["last_adset_ids"]   = adset_ids
        ctx.chat_data["last_landing_url"] = landing_url

        await msg.edit_text(
            f"✅ 正常跑法创建完成！\n\n"
            f"📋 系列 ID: {camp_id}\n"
            f"💰 系列预算: ${daily_budget}/天（CBO 自动分配）\n"
            f"📦 广告组: {count} 组（均为 PAUSED）\n"
            f"🌍 定向: {country_label} | {opts['device_os']} | {opts['age_min']}-{opts['age_max']}岁 | {gender_label}\n"
            f"🔗 落地页: {landing_url}\n\n"
            f"下一步：发送视频给我，然后用 /publish_last 发布",
        )
    except FBError as e:
        await msg.edit_text(f"❌ 创建失败: {e}")


# ── 发布到最近的广告组 ─────────────────────────────────────────
@_require_fb
async def publish_last_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /publish_last [正文文案|标题]
    """
    fb         = get_fb(update.effective_chat.id)
    adset_ids  = ctx.chat_data.get("last_adset_ids", [])
    video_id   = ctx.chat_data.get("last_video_id", "")
    image_hash = ctx.chat_data.get("last_image_hash", "")
    landing    = ctx.chat_data.get("last_landing_url", "")
    camp_id    = ctx.chat_data.get("last_campaign_id", "")

    if not adset_ids:
        await update.message.reply_text("❌ 没有待发布的广告组，请先运行 /normal")
        return
    if not video_id and not image_hash:
        await update.message.reply_text("❌ 没有上传的视频，请先发送视频给我")
        return

    text = " ".join(ctx.args) if ctx.args else ""
    if "|" in text:
        parts = text.split("|", 1)
        message_text = parts[0].strip()
        title_text   = parts[1].strip()
    else:
        message_text = text
        title_text   = "Se você não quer ganhar, não clique!"

    msg = await update.message.reply_text(
        f"⏳ 正在为 {len(adset_ids)} 个广告组绑定素材并发布..."
    )
    try:
        from services.campaign import bind_and_publish
        creative_id = bind_and_publish(
            fb=fb,
            adset_ids=adset_ids,
            landing_url=landing,
            message=message_text,
            title=title_text,
            camp_id=camp_id,
            video_id=video_id,
            image_hash=image_hash,
        )
        await msg.edit_text(
            f"🚀 发布成功！\n\n"
            f"✅ 创意 ID: `{creative_id}`\n"
            f"✅ {len(adset_ids)} 个广告组已启动\n\n"
            f"FB 审核通常需要 1-4 小时。\n"
            f"用 /report 查看数据，/automonitor on 开启自动监控。",
            parse_mode="Markdown",
        )
    except FBError as e:
        await msg.edit_text(f"❌ 发布失败: {e}")


# ── 自动监控开关 ────────────────────────────────────────────────
async def automonitor_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/automonitor on|off|status"""
    chat_id = update.effective_chat.id
    arg = (ctx.args[0].lower() if ctx.args else "on")

    if arg == "off":
        monitor_chats[chat_id] = {"enabled": False}
        await update.message.reply_text("❌ 自动监控已关闭。")
        return

    if arg == "status":
        info = monitor_chats.get(chat_id, {})
        enabled = info.get("enabled", False)
        cids = info.get("campaign_ids", [])
        status = "✅ 开启中" if enabled else "❌ 已关闭"
        camp_info = f"监控 {len(cids)} 个系列" if cids else "未选择系列"

        host = os.getenv("DASHBOARD_HOST", "localhost:8080")
        secret = os.getenv("DASHBOARD_SECRET", "admin")
        url_line = f"\n📊 实时面板：http://{host}/dashboard?key={secret}"

        await update.message.reply_text(
            f"🤖 自动监控状态：{status}\n"
            f"📋 {camp_info}\n"
            f"⏱ 数据刷新：每 2 分钟{url_line}\n\n"
            f"关停规则（按转化事件自动匹配）：\n"
            f"📋 订阅类：$3无点击→关 | $5无订阅→关\n"
            f"🛒 购物类：$3无点击→关 | $5无注册→关 | $9无购买→关\n"
            f"📝 注册类：$3无点击→关 | $5无注册→关"
        )
        return

    # on → 拉取广告系列列表，显示多选按钮
    fb = get_fb(chat_id)
    if not fb:
        await update.message.reply_text("⚠️ 未配置 FB 账户，请先运行 /setupbm 或 /setfb")
        return

    msg = await update.message.reply_text("⏳ 正在拉取广告系列...")
    try:
        camps = fb.list_campaigns("ACTIVE")
        if not camps:
            await msg.edit_text("📭 当前没有 ACTIVE 的广告系列，无法开启监控。")
            return

        # 初始化选中列表（默认全选）
        ctx.chat_data["monitor_selected"] = {c["id"] for c in camps}
        ctx.chat_data["monitor_camps"] = camps

        keyboard = _build_monitor_keyboard(camps, ctx.chat_data["monitor_selected"])
        await msg.edit_text(
            f"🤖 请选择要监控的广告系列（点击切换选中）：\n"
            f"共 {len(camps)} 个系列，默认全选",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except FBError as e:
        await msg.edit_text(f"❌ {e}")


def _build_monitor_keyboard(camps: list, selected: set) -> list:
    """构建监控系列多选按钮"""
    keyboard = []
    for c in camps:
        cid = c["id"]
        name = c.get("name", cid)
        budget = int(c.get("daily_budget", 0)) / 100
        check = "✅" if cid in selected else "⬜"
        label = f"{check} {name} (${budget:.0f}/天)"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"monitor_toggle:{cid}")])
    keyboard.append([InlineKeyboardButton("🚀 确认开始监控", callback_data="monitor_confirm")])
    return keyboard


async def monitor_toggle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：toggle 选中/取消某个系列"""
    query = update.callback_query
    await query.answer()

    cid = query.data.split(":")[1]
    selected = ctx.chat_data.get("monitor_selected", set())
    camps = ctx.chat_data.get("monitor_camps", [])

    if cid in selected:
        selected.discard(cid)
    else:
        selected.add(cid)
    ctx.chat_data["monitor_selected"] = selected

    keyboard = _build_monitor_keyboard(camps, selected)
    count = len(selected)
    await query.edit_message_text(
        f"🤖 请选择要监控的广告系列（点击切换选中）：\n"
        f"已选 {count} 个系列",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def monitor_confirm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：确认开始监控"""
    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    selected = ctx.chat_data.get("monitor_selected", set())
    camps = ctx.chat_data.get("monitor_camps", [])

    if not selected:
        await query.edit_message_text("❌ 至少选择一个广告系列。")
        return

    # 保存监控配置（包含当前 FBConfig）
    fb = get_fb(chat_id)
    monitor_chats[chat_id] = {
        "enabled": True,
        "campaign_ids": list(selected),
        "fb_config": fb.cfg if fb else None,
    }

    # 构建已选系列名称
    selected_names = []
    for c in camps:
        if c["id"] in selected:
            selected_names.append(f"• {c.get('name', c['id'])}")

    # 清理临时数据
    ctx.chat_data.pop("monitor_selected", None)
    ctx.chat_data.pop("monitor_camps", None)

    # 固定 Dashboard 链接
    host = os.getenv("DASHBOARD_HOST", "localhost:8080")
    secret = os.getenv("DASHBOARD_SECRET", "admin")
    url = f"http://{host}/dashboard?key={secret}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 打开实时面板", url=url)]
    ])
    await query.edit_message_text(
        f"✅ 自动监控已开启\n\n"
        f"📋 监控 {len(selected)} 个系列：\n"
        + "\n".join(selected_names) + "\n\n"
        f"关停规则（按转化事件自动匹配）：\n"
        f"📋 订阅类：$3无点击→关 | $5无订阅→关\n"
        f"🛒 购物类：$3无点击→关 | $5无注册→关 | $9无购买→关\n"
        f"📝 注册类：$3无点击→关 | $5无注册→关\n\n"
        f"发 /automonitor off 关闭",
        reply_markup=keyboard,
    )

    # 立即推送一次数据到 Dashboard
    from services.monitor import run_once
    await run_once(ctx.application, chat_id)


# ══════════════════════════════════════════════════════════════════
# 按钮引导式创建广告
# ══════════════════════════════════════════════════════════════════
(OD_TOKEN, OD_ACCOUNT, OD_PIXEL, OD_PAGE, OD_EVENT, OD_NAME, OD_COUNT, OD_BUDGET, OD_URL,
 OD_COUNTRY, OD_DEVICE, OD_GENDER, OD_AGE, OD_CONFIRM,
 OD_MEDIA, OD_TEXT, OD_TITLE, OD_PUBLISH,
 OD_MODE, OD_AI_CHOICE, OD_MANUAL_COPY) = range(21)


def _fetch_ad_accounts(token: str) -> list[dict]:
    """用 Token 调 Graph API 拉取所有广告账户"""
    try:
        resp = _requests.get(f"{GRAPH}/me/adaccounts", params={
            "access_token": token,
            "fields": "id,account_id,name,account_status",
            "limit": 50,
        })
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        logger.warning(f"拉取广告账户失败: {e}")
        return []


async def od_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """入口：/normal（无参数进入引导，有参数走旧逻辑）"""
    # 有参数 → 走旧的一行命令逻辑
    if ctx.args:
        await normal_cmd(update, ctx)
        return ConversationHandler.END

    # 无参数 → 进入按钮引导模式
    ctx.chat_data["od_mode"] = "normal"

    await update.message.reply_text(
        f"📋 开始创建广告（CBO 广告系列预算）\n\n"
        f"🔑 请发送 Access Token\n"
        f"（以 EAA 开头的长字符串）\n\n"
        f"发 /cancel 取消",
    )
    return OD_TOKEN


async def od_token_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到 Token → 拉取广告账户列表 → 显示选择按钮"""
    token = (update.message.text or "").strip()

    if len(token) < 20:
        await update.message.reply_text(
            "❌ Token 太短，请发送完整的 Access Token（以 EAA 开头）："
        )
        return OD_TOKEN

    msg = await update.message.reply_text("⏳ 正在验证 Token 并获取广告账户列表...")

    accounts = _fetch_ad_accounts(token)
    if not accounts:
        await msg.edit_text(
            "❌ 该 Token 下没有找到广告账户，请检查 Token 是否正确。\n\n"
            "请重新发送 Token 或发 /cancel 取消："
        )
        return OD_TOKEN

    ctx.chat_data["od_token"] = token
    ctx.chat_data["od_accounts"] = accounts

    if len(accounts) == 1:
        # 只有 1 个账户，自动选中，跳到下一步
        acc = accounts[0]
        acc_id = acc.get("account_id", "")
        acc_name = acc.get("name", "")
        ctx.chat_data["od_selected_account"] = acc
        await msg.edit_text(f"⏳ 正在获取 {acc_name} (act_{acc_id}) 的配置...")
        # 直接走 _setup_selected_account 逻辑
        return await _finish_account_selection(update, ctx, msg)

    # 多个账户 → 显示选择按钮
    status_icons = {1: "🟢", 2: "🔴", 3: "🟡"}
    keyboard = []
    for i, acc in enumerate(accounts):
        acc_id = acc.get("account_id", "")
        acc_name = acc.get("name", f"账户{i+1}")
        status = acc.get("account_status", 0)
        icon = status_icons.get(status, "⚪")
        label = f"{icon} {acc_name} (act_{acc_id})"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"od_acct:{i}")])

    mode = ctx.chat_data.get("od_mode", "normal")
    mode_label = "创建广告"

    await msg.edit_text(
        f"✅ Token 验证通过！\n\n"
        f"📋 {mode_label} — 🏦 请选择广告账户：\n"
        f"（共 {len(accounts)} 个账户）",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_ACCOUNT


async def _finish_account_selection(update: Update, ctx, edit_msg=None):
    """选完账户后，查像素和主页，配置 FBConfig，进入 OD_NAME"""
    acc = ctx.chat_data.get("od_selected_account", {})
    token = ctx.chat_data.get("od_token", "")
    acc_id = acc.get("account_id", "")
    acc_name = acc.get("name", "")
    chat_id = update.effective_chat.id

    # 查所有像素
    pixels = []
    try:
        resp = _requests.get(f"{GRAPH}/act_{acc_id}/adspixels", params={
            "access_token": token, "fields": "id,name", "limit": 20,
        })
        pixels = resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"查询像素失败: {e}")

    # 查所有主页（4种方式依次尝试）
    pages = []
    try:
        # 方式1: 通过 BM owned_pages
        resp = _requests.get(f"{GRAPH}/act_{acc_id}", params={
            "access_token": token, "fields": "business",
        })
        biz = resp.json().get("business", {})
        biz_id = biz.get("id", "")
        if biz_id:
            resp = _requests.get(f"{GRAPH}/{biz_id}/owned_pages", params={
                "access_token": token, "fields": "id,name", "limit": 20,
            })
            pages = resp.json().get("data", [])
        # 方式2: promote_pages
        if not pages:
            resp = _requests.get(f"{GRAPH}/act_{acc_id}/promote_pages", params={
                "access_token": token, "fields": "id,name", "limit": 20,
            })
            pages = resp.json().get("data", [])
        # 方式3: me/accounts（个人用户 token）
        if not pages:
            resp = _requests.get(f"{GRAPH}/me/accounts", params={
                "access_token": token, "fields": "id,name", "limit": 20,
            })
            pages = resp.json().get("data", [])
        # 方式4: 系统用户 assigned_pages
        if not pages:
            me_resp = _requests.get(f"{GRAPH}/me", params={
                "access_token": token, "fields": "id",
            })
            me_id = me_resp.json().get("id", "")
            if me_id:
                resp = _requests.get(f"{GRAPH}/{me_id}/assigned_pages", params={
                    "access_token": token, "fields": "id,name", "limit": 20,
                })
                pages = resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"查询主页失败: {e}")

    ctx.chat_data.pop("od_accounts", None)
    ctx.chat_data.pop("od_selected_account", None)
    ctx.chat_data["od_pixels"] = pixels
    ctx.chat_data["od_pages"] = pages
    ctx.chat_data["od_acc_id"] = acc_id
    ctx.chat_data["od_acc_name"] = acc_name

    # 像素选择
    if len(pixels) == 0:
        # 没有像素，跳过
        ctx.chat_data["od_pixel_id"] = ""
        return await _show_page_selection(update, ctx, edit_msg)
    elif len(pixels) == 1:
        # 只有一个，自动选中
        ctx.chat_data["od_pixel_id"] = pixels[0].get("id", "")
        return await _show_page_selection(update, ctx, edit_msg)
    else:
        # 多个，让投手选
        keyboard = []
        for i, p in enumerate(pixels):
            keyboard.append([InlineKeyboardButton(
                f"📊 {p.get('name', '')} ({p.get('id', '')})",
                callback_data=f"od_pixel:{i}"
            )])
        text = f"✅ 已选择：{acc_name} (act_{acc_id})\n\n📊 请选择像素："
        if edit_msg:
            await edit_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return OD_PIXEL


async def od_pixel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：选择像素"""
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    pixels = ctx.chat_data.get("od_pixels", [])
    if idx < len(pixels):
        ctx.chat_data["od_pixel_id"] = pixels[idx].get("id", "")
    return await _show_page_selection(update, ctx, edit_msg=query.message)


async def _show_page_selection(update, ctx, edit_msg=None):
    """显示主页选择"""
    pages = ctx.chat_data.get("od_pages", [])
    acc_name = ctx.chat_data.get("od_acc_name", "")
    pixel_id = ctx.chat_data.get("od_pixel_id", "")

    if len(pages) == 0:
        ctx.chat_data["od_page_id"] = ""
        return await _finish_pixel_page(update, ctx, edit_msg)
    elif len(pages) == 1:
        ctx.chat_data["od_page_id"] = pages[0].get("id", "")
        return await _finish_pixel_page(update, ctx, edit_msg)
    else:
        keyboard = []
        for i, p in enumerate(pages):
            keyboard.append([InlineKeyboardButton(
                f"📄 {p.get('name', '')} ({p.get('id', '')})",
                callback_data=f"od_page:{i}"
            )])
        text = f"✅ 像素：{pixel_id or '无'}\n\n📄 请选择主页："
        if edit_msg:
            await edit_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return OD_PAGE


async def od_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：选择主页"""
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    pages = ctx.chat_data.get("od_pages", [])
    if idx < len(pages):
        ctx.chat_data["od_page_id"] = pages[idx].get("id", "")
    return await _finish_pixel_page(update, ctx, edit_msg=query.message)


async def _finish_pixel_page(update, ctx, edit_msg=None):
    """像素和主页都选完，保存 FBConfig，进入转化事件选择"""
    token = ctx.chat_data.get("od_token", "")
    acc_id = ctx.chat_data.get("od_acc_id", "")
    acc_name = ctx.chat_data.get("od_acc_name", "")
    pixel_id = ctx.chat_data.get("od_pixel_id", "")
    page_id = ctx.chat_data.get("od_page_id", "")
    chat_id = update.effective_chat.id

    fb_configs[chat_id] = FBConfig(
        access_token=token,
        ad_account_id=acc_id,
        pixel_id=pixel_id,
        page_id=page_id,
    )

    keyboard = [
        [InlineKeyboardButton("📋 订阅", callback_data="od_event:SUBSCRIBE"),
         InlineKeyboardButton("📝 注册", callback_data="od_event:COMPLETE_REGISTRATION"),
         InlineKeyboardButton("🛒 购物", callback_data="od_event:PURCHASE")],
    ]

    text = (
        f"✅ 已选择：{acc_name} (act_{acc_id})\n"
        f"像素: {pixel_id or '未找到'} | 主页: {page_id or '未找到'}\n\n"
        f"🎯 请选择转化事件："
    )

    if edit_msg:
        await edit_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.effective_chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))

    return OD_EVENT


async def od_account(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：选择广告账户"""
    query = update.callback_query
    await query.answer()

    idx = int(query.data.split(":")[1])
    accounts = ctx.chat_data.get("od_accounts", [])

    if idx >= len(accounts):
        await query.edit_message_text("❌ 无效的账户选择。")
        return ConversationHandler.END

    acc = accounts[idx]
    ctx.chat_data["od_selected_account"] = acc
    acc_id = acc.get("account_id", "")
    acc_name = acc.get("name", "")

    await query.edit_message_text(f"⏳ 正在获取 {acc_name} (act_{acc_id}) 的配置...")
    return await _finish_account_selection(update, ctx, edit_msg=query.message)


EVENT_CONFIG = {
    "SUBSCRIBE": {"label": "订阅", "cta": "SUBSCRIBE"},
    "COMPLETE_REGISTRATION": {"label": "注册", "cta": "SIGN_UP"},
    "PURCHASE": {"label": "购物", "cta": "SHOP_NOW"},
}


async def od_event(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：选择转化事件，进入系列名"""
    query = update.callback_query
    await query.answer()
    event = query.data.split(":")[1]
    cfg = EVENT_CONFIG.get(event, EVENT_CONFIG["SUBSCRIBE"])

    ctx.chat_data["od_conversion_event"] = event
    ctx.chat_data["od_cta"] = cfg["cta"]

    await query.edit_message_text(
        f"✅ 转化事件：{cfg['label']}\n\n"
        f"📋 请输入系列名称\n"
        f"命名格式：产品-渠道号-优化师-日期-系列几\n"
        f"例如：bet7-778-DT-0403-1\n\n"
        f"发 /cancel 取消"
    )
    return OD_NAME


async def od_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到系列名，问投放模式"""
    name = update.message.text.strip()
    if not name or name.startswith("/"):
        await update.message.reply_text("❌ 名称不能为空或以 / 开头，请重新输入：")
        return OD_NAME

    ctx.chat_data["od_camp_name"] = name

    keyboard = [
        [InlineKeyboardButton("📦 多广告组（每组1条广告）", callback_data="od_flow:multi_adset")],
        [InlineKeyboardButton("🎬 单组多广告（1组×N条广告）", callback_data="od_flow:multi_ad")],
    ]
    await update.message.reply_text(
        f"✅ 系列名：{name}\n\n"
        f"请选择投放模式：\n"
        f"• 多广告组：创建多个广告组，每组1条广告（适合测试不同受众）\n"
        f"• 单组多广告：1个广告组下创建多条不同文案的广告（适合测试文案）",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_MODE


async def od_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：选择投放模式"""
    query = update.callback_query
    await query.answer()
    flow = query.data.split(":")[1]
    ctx.chat_data["od_flow_mode"] = flow  # "multi_adset" or "multi_ad"

    if flow == "multi_ad":
        await query.edit_message_text(
            f"✅ 模式：单组多广告\n\n"
            f"请输入广告数量（1-20）：",
        )
    else:
        await query.edit_message_text(
            f"✅ 模式：多广告组\n\n"
            f"请输入广告组数量（1-100）：",
        )
    return OD_COUNT


async def od_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到数量（广告组数 或 广告条数）"""
    text = update.message.text.strip()
    flow = ctx.chat_data.get("od_flow_mode", "multi_adset")
    max_count = 20 if flow == "multi_ad" else 100

    try:
        count = int(text)
        if count < 1 or count > max_count:
            raise ValueError
    except ValueError:
        await update.message.reply_text(f"❌ 请输入 1-{max_count} 的整数：")
        return OD_COUNT

    ctx.chat_data["od_count"] = count

    label = "广告条数" if flow == "multi_ad" else "广告组数"
    await update.message.reply_text(
        f"✅ {label}：{count}\n\n请输入系列日预算（美元）：",
    )
    return OD_BUDGET


async def od_budget(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到预算（仅正常跑法）"""
    text = update.message.text.strip()
    try:
        budget = float(text)
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的预算金额（数字）：")
        return OD_BUDGET

    ctx.chat_data["od_budget"] = budget
    await update.message.reply_text(
        f"✅ 系列日预算：${budget}\n\n请输入落地页链接：",
    )
    return OD_URL


async def od_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到落地页，发国家按钮"""
    url = update.message.text.strip()
    if not url.startswith("http"):
        await update.message.reply_text("❌ 请输入以 http 开头的链接：")
        return OD_URL

    ctx.chat_data["od_url"] = url

    keyboard = [
        [InlineKeyboardButton("🇧🇷 BR", callback_data="od_country:BR"),
         InlineKeyboardButton("🇺🇸 US", callback_data="od_country:US"),
         InlineKeyboardButton("🇮🇳 IN", callback_data="od_country:IN")],
        [InlineKeyboardButton("🇵🇭 PH", callback_data="od_country:PH"),
         InlineKeyboardButton("🇲🇽 MX", callback_data="od_country:MX"),
         InlineKeyboardButton("🇮🇩 ID", callback_data="od_country:ID")],
        [InlineKeyboardButton("🇻🇳 VN", callback_data="od_country:VN"),
         InlineKeyboardButton("🇹🇭 TH", callback_data="od_country:TH"),
         InlineKeyboardButton("🇨🇴 CO", callback_data="od_country:CO")],
    ]
    await update.message.reply_text(
        "✅ 落地页已设置\n\n🌍 请选择投放国家：",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_COUNTRY


async def od_country(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：收到国家，发设备按钮"""
    query = update.callback_query
    await query.answer()
    country = query.data.split(":")[1]
    ctx.chat_data["od_country"] = country

    keyboard = [
        [InlineKeyboardButton("📱 Android", callback_data="od_device:Android"),
         InlineKeyboardButton("🍎 iOS", callback_data="od_device:iOS"),
         InlineKeyboardButton("💻 All", callback_data="od_device:All")],
    ]
    await query.edit_message_text(
        f"✅ 国家：{country}\n\n📱 请选择设备：",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_DEVICE


async def od_device(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：收到设备，发性别按钮"""
    query = update.callback_query
    await query.answer()
    device = query.data.split(":")[1]
    ctx.chat_data["od_device"] = device

    keyboard = [
        [InlineKeyboardButton("👥 全部", callback_data="od_gender:0"),
         InlineKeyboardButton("👨 男", callback_data="od_gender:1"),
         InlineKeyboardButton("👩 女", callback_data="od_gender:2")],
    ]
    await query.edit_message_text(
        f"✅ 设备：{device}\n\n🧑 请选择性别：",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_GENDER


async def od_gender(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：收到性别，发年龄按钮"""
    query = update.callback_query
    await query.answer()
    gender = int(query.data.split(":")[1])
    ctx.chat_data["od_gender"] = gender
    gender_label = {0: "全部", 1: "男", 2: "女"}[gender]

    keyboard = [
        [InlineKeyboardButton("18-65（默认）", callback_data="od_age:18-65"),
         InlineKeyboardButton("18-45", callback_data="od_age:18-45")],
        [InlineKeyboardButton("25-55", callback_data="od_age:25-55"),
         InlineKeyboardButton("18-35", callback_data="od_age:18-35")],
    ]
    await query.edit_message_text(
        f"✅ 性别：{gender_label}\n\n🎂 请选择年龄范围：",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_AGE


async def od_age(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：收到年龄，显示确认摘要"""
    query = update.callback_query
    await query.answer()
    age_str = query.data.split(":")[1]
    age_min, age_max = map(int, age_str.split("-"))
    ctx.chat_data["od_age_min"] = age_min
    ctx.chat_data["od_age_max"] = age_max

    # 构造确认摘要
    d = ctx.chat_data
    gender_label = {0: "全部", 1: "男", 2: "女"}[d["od_gender"]]
    flow_mode = d.get("od_flow_mode", "multi_adset")
    event_label = EVENT_CONFIG.get(d.get("od_conversion_event", "SUBSCRIBE"), {}).get("label", "订阅")

    if flow_mode == "multi_ad":
        count_line = f"🎬 广告数量：{d['od_count']} 条（1个广告组）"
        mode_desc = "单组多广告"
    else:
        count_line = f"📦 广告组：{d['od_count']} 组"
        mode_desc = "多广告组"

    summary = (
        f"📋 请确认广告配置：\n"
        f"{'─' * 24}\n"
        f"🗂 模式：{mode_desc}\n"
        f"💰 系列预算：${d['od_budget']}/天（CBO 自动分配）\n"
        f"📝 系列名：{d['od_camp_name']}\n"
        f"🎯 转化事件：{event_label}\n"
        f"{count_line}\n"
        f"🔗 落地页：{d['od_url']}\n"
        f"🌍 国家：{d['od_country']}\n"
        f"📱 设备：{d['od_device']}\n"
        f"🧑 性别：{gender_label}\n"
        f"🎂 年龄：{age_min}-{age_max}\n"
        f"{'─' * 24}"
    )

    keyboard = [
        [InlineKeyboardButton("✅ 确认创建", callback_data="od_confirm:yes"),
         InlineKeyboardButton("❌ 取消", callback_data="od_confirm:no")],
    ]
    await query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return OD_CONFIRM


async def od_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：确认或取消"""
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action != "yes":
        await query.edit_message_text("❌ 已取消创建。")
        return ConversationHandler.END

    d = ctx.chat_data
    fb = get_fb(update.effective_chat.id)
    flow_mode = d.get("od_flow_mode", "multi_adset")

    await query.edit_message_text("⏳ 正在创建广告系列，请稍候...")

    try:
        if flow_mode == "multi_ad":
            # 单组多广告：创建1个广告组
            camp_id, adset_ids = normal_flow(
                fb, d["od_camp_name"], 1,
                daily_budget_usd=d["od_budget"],
                country=d["od_country"],
                device_os=d["od_device"],
                age_min=d["od_age_min"],
                age_max=d["od_age_max"],
                gender=d["od_gender"],
                conversion_event=d.get("od_conversion_event", "SUBSCRIBE"),
            )
            # 初始化广告列表，每条广告占一个槽位
            ctx.chat_data["od_ad_list"] = [None] * d["od_count"]
            ctx.chat_data["od_ad_idx"] = 0  # 当前正在填写的广告序号
        else:
            # 多广告组：创建 N 个广告组
            camp_id, adset_ids = normal_flow(
                fb, d["od_camp_name"], d["od_count"],
                daily_budget_usd=d["od_budget"],
                country=d["od_country"],
                device_os=d["od_device"],
                age_min=d["od_age_min"],
                age_max=d["od_age_max"],
                gender=d["od_gender"],
                conversion_event=d.get("od_conversion_event", "SUBSCRIBE"),
            )

        ctx.chat_data["od_cta"] = d.get("od_cta", "SUBSCRIBE")
        ctx.chat_data["last_campaign_id"] = camp_id
        ctx.chat_data["last_adset_ids"]   = adset_ids
        ctx.chat_data["last_landing_url"] = d["od_url"]

        # 创建上传任务，返回网页链接
        from services.web import create_upload_task
        task_id = create_upload_task(
            chat_id=update.effective_chat.id,
            campaign_id=camp_id,
            adset_ids=adset_ids,
            landing_url=d["od_url"],
            cta=d.get("od_cta", "SUBSCRIBE"),
            count=d["od_count"],
            fb_config=fb.cfg,
            flow_mode=flow_mode,
        )

        host = os.getenv("DASHBOARD_HOST", "localhost:8080")
        upload_url = f"http://{host}/upload?task={task_id}"

        gender_label = {0: "全部", 1: "男", 2: "女"}[d["od_gender"]]
        mode_label = "单组多广告" if flow_mode == "multi_ad" else "多广告组"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📎 打开素材上传页面", url=upload_url)]
        ])

        await query.edit_message_text(
            f"✅ 广告系列创建完成！\n\n"
            f"📋 系列 ID：{camp_id}\n"
            f"💰 系列预算：${d['od_budget']}/天（CBO 自动分配）\n"
            f"🗂 模式：{mode_label} × {d['od_count']}\n"
            f"🌍 {d['od_country']} | {d['od_device']} | "
            f"{d['od_age_min']}-{d['od_age_max']}岁 | {gender_label}\n\n"
            f"📎 点击下方按钮上传素材并发布：",
            reply_markup=keyboard,
        )
        return ConversationHandler.END
    except FBError as e:
        await query.edit_message_text(f"❌ 创建失败：{e}")
        return ConversationHandler.END


async def od_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到视频/图片素材，上传到 FB"""
    import tempfile
    from pathlib import Path

    fb = get_fb(update.effective_chat.id)
    if not fb:
        await update.message.reply_text("❌ 未配置 FB 账户。")
        return ConversationHandler.END

    file_obj = update.message.video or update.message.document or None
    is_photo = False
    if not file_obj and update.message.photo:
        file_obj = update.message.photo[-1]
        is_photo = True

    if not file_obj:
        await update.message.reply_text("❌ 请发送视频或图片文件：")
        return OD_MEDIA

    msg = await update.message.reply_text("⏳ 正在上传素材到 FB...")

    suffix = ".jpg" if is_photo else ".mp4"
    file_name = getattr(file_obj, "file_name", None) or f"media{suffix}"

    try:
        tg_file = await file_obj.get_file()
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
        await tg_file.download_to_drive(tmp_path)

        if is_photo:
            image_hash = fb.upload_image(tmp_path)
            media_info = {"type": "image", "id": "", "hash": image_hash}
            media_id = image_hash
        else:
            video_id = fb.upload_video(tmp_path, title=file_name)
            media_info = {"type": "video", "id": video_id, "hash": ""}
            media_id = video_id

        Path(tmp_path).unlink(missing_ok=True)

        flow_mode = ctx.chat_data.get("od_flow_mode", "multi_adset")

        if flow_mode == "multi_ad":
            idx = ctx.chat_data.get("od_ad_idx", 0)
            total = ctx.chat_data.get("od_count", 1)
            # 保存当前素材
            ctx.chat_data["od_current_media"] = media_info

            if idx == 0:
                # 第1条广告：手动输入文案
                await msg.edit_text(
                    f"✅ 第 {idx+1}/{total} 条素材上传成功！（ID：{media_id}）\n\n"
                    f"📝 请输入第 {idx+1} 条广告正文\n"
                    f"发「跳过」使用默认文案\n"
                    f"（发 /cancel 取消）",
                )
                return OD_TEXT
            else:
                # 后续广告：问复制方式
                keyboard = [
                    [InlineKeyboardButton("🤖 AI 生成变体文案", callback_data="od_copy:ai")],
                    [InlineKeyboardButton("📋 复制第1条文案", callback_data="od_copy:copy")],
                    [InlineKeyboardButton("✏️ 手动输入文案", callback_data="od_copy:manual")],
                ]
                await msg.edit_text(
                    f"✅ 第 {idx+1}/{total} 条素材上传成功！\n\n"
                    f"📝 第 {idx+1} 条广告文案来源：",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
                return OD_AI_CHOICE
        else:
            # 多广告组模式：沿用原来逻辑
            ctx.chat_data["last_image_hash"] = media_info["hash"] if is_photo else ""
            ctx.chat_data["last_video_id"] = media_info["id"] if not is_photo else ""
            await msg.edit_text(
                f"✅ 素材上传成功！（ID：{media_id}）\n\n"
                f"📝 请输入广告正文（显示在视频上方）\n"
                f"发「跳过」使用默认文案\n"
                f"（发 /cancel 取消）",
            )
            return OD_TEXT
    except FBError as e:
        await msg.edit_text(f"❌ 素材上传失败：{e}\n\n请重新发送素材：")
        return OD_MEDIA
    except Exception as e:
        logger.error(f"素材上传异常: {e}")
        await msg.edit_text(f"❌ 上传异常：{e}\n\n请重新发送素材：")
        return OD_MEDIA


async def od_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到广告正文，问标题"""
    text = update.message.text.strip()
    if text in ("跳过", "skip"):
        ctx.chat_data["od_ad_text"] = ""
    else:
        ctx.chat_data["od_ad_text"] = text

    flow_mode = ctx.chat_data.get("od_flow_mode", "multi_adset")
    idx = ctx.chat_data.get("od_ad_idx", 0) if flow_mode == "multi_ad" else 0
    total = ctx.chat_data.get("od_count", 1) if flow_mode == "multi_ad" else 0

    label = f"第 {idx+1}/{total} 条 " if flow_mode == "multi_ad" else ""
    await update.message.reply_text(
        f"✅ {label}广告正文：{text if text not in ('跳过', 'skip') else '（默认）'}\n\n"
        f"📝 请输入{label}广告标题（显示在视频下方）\n"
        f"发「跳过」使用默认标题",
    )
    return OD_TITLE


async def od_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到广告标题"""
    text = update.message.text.strip()
    default_title = "Se você não quer ganhar, não clique!"
    if text in ("跳过", "skip"):
        ad_title = default_title
        title_display = f"（默认：{default_title}）"
    else:
        ad_title = text
        title_display = text

    ctx.chat_data["od_ad_title"] = ad_title
    d = ctx.chat_data
    flow_mode = d.get("od_flow_mode", "multi_adset")

    if flow_mode == "multi_ad":
        idx = d.get("od_ad_idx", 0)
        total = d.get("od_count", 1)
        media_info = d.get("od_current_media", {})
        ad_list = d.get("od_ad_list", [])
        ad_list[idx] = {
            "media": media_info,
            "text": d.get("od_ad_text", ""),
            "title": ad_title,
        }
        d["od_ad_list"] = ad_list
        next_idx = idx + 1

        if next_idx < total:
            d["od_ad_idx"] = next_idx
            await update.message.reply_text(
                f"✅ 第 {idx+1}/{total} 条广告文案已保存\n\n"
                f"📎 第 {next_idx+1}/{total} 条广告 — 请发送视频或图片素材\n"
                f"（发 /cancel 取消）",
            )
            return OD_MEDIA
        else:
            # 所有广告素材和文案收集完毕，显示发布确认
            lines = [f"🚀 即将发布 {total} 条广告：\n{'─' * 24}"]
            for i, ad in enumerate(ad_list):
                lines.append(
                    f"广告 {i+1}：{ad['media']['type']} | "
                    f"正文：{(ad['text'] or '（空）')[:30]} | "
                    f"标题：{ad['title'][:20]}"
                )
            lines.append(f"{'─' * 24}")
            lines.append(f"🔗 落地页：{d.get('last_landing_url', d.get('od_url', ''))}")
            lines.append("\n⚠️ 点击发布后广告将开始投放并产生费用！")

            keyboard = [
                [InlineKeyboardButton("🚀 发布广告", callback_data="od_publish:yes"),
                 InlineKeyboardButton("❌ 取消", callback_data="od_publish:no")],
            ]
            await update.message.reply_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return OD_PUBLISH
    else:
        # 多广告组模式
        adset_count = len(d.get("last_adset_ids", []))
        ad_text = d.get("od_ad_text", "")
        summary = (
            f"🚀 即将发布广告：\n"
            f"{'─' * 24}\n"
            f"📋 系列：{d.get('od_camp_name', '')}\n"
            f"📦 广告组：{adset_count} 组\n"
            f"🎬 素材：已上传\n"
            f"📝 正文：{ad_text or '（空）'}\n"
            f"🏷 标题：{title_display}\n"
            f"🔗 落地页：{d.get('od_url', '')}\n"
            f"{'─' * 24}\n\n"
            f"⚠️ 点击发布后广告将开始投放并产生费用！"
        )
        keyboard = [
            [InlineKeyboardButton("🚀 发布广告", callback_data="od_publish:yes"),
             InlineKeyboardButton("❌ 取消", callback_data="od_publish:no")],
        ]
        await update.message.reply_text(
            summary,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return OD_PUBLISH


async def od_ai_choice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """按钮回调：后续广告文案来源（AI / 复制 / 手动）"""
    query = update.callback_query
    await query.answer()
    choice = query.data.split(":")[1]  # "ai" / "copy" / "manual"

    d = ctx.chat_data
    idx = d.get("od_ad_idx", 1)
    total = d.get("od_count", 1)

    if choice == "manual":
        await query.edit_message_text(
            f"✏️ 第 {idx+1}/{total} 条广告\n\n"
            f"📝 请输入广告正文：\n"
            f"（发「跳过」使用默认文案）",
        )
        return OD_MANUAL_COPY

    # AI 生成 或 直接复制
    ad_list = d.get("od_ad_list", [])
    first_ad = ad_list[0] if ad_list and ad_list[0] else {}
    orig_text = first_ad.get("text", "")
    orig_title = first_ad.get("title", "")

    if choice == "ai":
        await query.edit_message_text(f"🤖 AI 正在生成第 {idx+1}/{total} 条文案变体...")
        try:
            from services.llm import generate_ad_copy_variant
            new_text, new_title = await generate_ad_copy_variant(orig_text, orig_title)
        except Exception as e:
            logger.error(f"AI 生成失败: {e}")
            new_text, new_title = orig_text, orig_title
    else:
        # copy
        new_text, new_title = orig_text, orig_title

    media_info = d.get("od_current_media", {})
    ad_list[idx] = {"media": media_info, "text": new_text, "title": new_title}
    d["od_ad_list"] = ad_list

    next_idx = idx + 1
    if next_idx < total:
        d["od_ad_idx"] = next_idx
        tag = "（AI生成）" if choice == "ai" else "（已复制）"
        await query.edit_message_text(
            f"✅ 第 {idx+1}/{total} 条文案已保存 {tag}\n"
            f"正文：{new_text[:40] or '（空）'}\n"
            f"标题：{new_title[:30]}\n\n"
            f"📎 第 {next_idx+1}/{total} 条广告 — 请发送视频或图片素材\n"
            f"（发 /cancel 取消）",
        )
        return OD_MEDIA
    else:
        # 全部完成，显示发布确认
        lines = [f"🚀 即将发布 {total} 条广告：\n{'─' * 24}"]
        for i, ad in enumerate(ad_list):
            lines.append(
                f"广告 {i+1}：{ad['media']['type']} | "
                f"正文：{(ad['text'] or '（空）')[:30]} | "
                f"标题：{ad['title'][:20]}"
            )
        lines.append(f"{'─' * 24}")
        lines.append(f"🔗 落地页：{d.get('last_landing_url', d.get('od_url', ''))}")
        lines.append("\n⚠️ 点击发布后广告将开始投放并产生费用！")
        keyboard = [
            [InlineKeyboardButton("🚀 发布广告", callback_data="od_publish:yes"),
             InlineKeyboardButton("❌ 取消", callback_data="od_publish:no")],
        ]
        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return OD_PUBLISH


async def od_manual_copy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到手动输入的正文，再问标题"""
    text = update.message.text.strip()
    if text in ("跳过", "skip"):
        ctx.chat_data["od_ad_text"] = ""
    else:
        ctx.chat_data["od_ad_text"] = text

    d = ctx.chat_data
    idx = d.get("od_ad_idx", 0)
    total = d.get("od_count", 1)

    await update.message.reply_text(
        f"✅ 第 {idx+1}/{total} 条正文已保存\n\n"
        f"📝 请输入第 {idx+1}/{total} 条广告标题\n"
        f"（发「跳过」使用默认标题）",
    )
    return OD_TITLE


async def od_publish(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """最终确认：发布或取消"""
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action != "yes":
        await query.edit_message_text(
            "❌ 已取消发布。广告组保持 PAUSED 状态，不会花钱。\n"
            "之后可以用 /publish_last 手动发布。"
        )
        return ConversationHandler.END

    d = ctx.chat_data
    fb = get_fb(update.effective_chat.id)
    flow_mode = d.get("od_flow_mode", "multi_adset")
    landing   = d.get("last_landing_url", d.get("od_url", ""))
    camp_id   = d.get("last_campaign_id", "")
    adset_ids = d.get("last_adset_ids", [])

    await query.edit_message_text("⏳ 正在绑定素材并发布广告...")

    try:
        if flow_mode == "multi_ad":
            ad_list = d.get("od_ad_list", [])
            if not ad_list or not adset_ids:
                await query.edit_message_text("❌ 缺少广告数据。")
                return ConversationHandler.END

            from services.campaign import bind_and_publish_multi_ads
            ad_ids = bind_and_publish_multi_ads(
                fb=fb,
                adset_id=adset_ids[0],
                landing_url=landing,
                camp_id=camp_id,
                ad_list=ad_list,
                cta=d.get("od_cta", "SUBSCRIBE"),
            )
            await query.edit_message_text(
                f"🚀 发布成功！\n\n"
                f"✅ {len(ad_ids)} 条广告已创建并启动\n"
                f"广告组 ID：{adset_ids[0]}\n\n"
                f"FB 审核通常需要 1-4 小时。\n"
                f"用 /report 查看数据，/automonitor on 开启自动监控。",
            )
        else:
            video_id   = d.get("last_video_id", "")
            image_hash = d.get("last_image_hash", "")
            ad_text    = d.get("od_ad_text", "")
            ad_title   = d.get("od_ad_title", "Se você não quer ganhar, não clique!")

            if not adset_ids or (not video_id and not image_hash):
                await query.edit_message_text("❌ 缺少广告组或素材信息。")
                return ConversationHandler.END

            from services.campaign import bind_and_publish
            creative_id = bind_and_publish(
                fb=fb,
                adset_ids=adset_ids,
                landing_url=landing,
                message=ad_text,
                title=ad_title,
                camp_id=camp_id,
                video_id=video_id,
                image_hash=image_hash,
                cta=d.get("od_cta", "SUBSCRIBE"),
            )
            await query.edit_message_text(
                f"🚀 发布成功！\n\n"
                f"✅ 创意 ID：{creative_id}\n"
                f"✅ {len(adset_ids)} 个广告组已启动\n\n"
                f"FB 审核通常需要 1-4 小时。\n"
                f"用 /report 查看数据，/automonitor on 开启自动监控。",
            )
    except FBError as e:
        await query.edit_message_text(f"❌ 发布失败：{e}")

    return ConversationHandler.END


async def od_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/cancel 取消引导流程"""
    await update.message.reply_text("❌ 已取消创建。")
    return ConversationHandler.END
