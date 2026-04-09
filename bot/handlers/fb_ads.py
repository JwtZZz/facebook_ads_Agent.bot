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
        from services.web import revoke_token
        revoke_token(chat_id)
        await update.message.reply_text("❌ 自动监控已关闭，面板链接已失效。")
        return

    if arg == "status":
        info = monitor_chats.get(chat_id, {})
        enabled = info.get("enabled", False)
        cids = info.get("campaign_ids", [])
        status = "✅ 开启中" if enabled else "❌ 已关闭"
        camp_info = f"监控 {len(cids)} 个系列" if cids else "未选择系列"

        from store.state import chat_id_tokens
        token = chat_id_tokens.get(chat_id)
        host = os.getenv("DASHBOARD_HOST", "localhost:8080")
        url_line = f"\n📊 实时面板：http://{host}/dashboard?token={token}" if token else ""

        await update.message.reply_text(
            f"🤖 自动监控状态：{status}\n"
            f"📋 {camp_info}\n"
            f"⏱ 数据刷新：每 10 分钟{url_line}\n\n"
            f"关停规则：\n"
            f"• 展示≥1000 CTR<0.5% → 暂停（素材不行）\n"
            f"• 点击≥50 订阅率<5% → 暂停（落地页/定向问题）"
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

    # 生成 Dashboard token 和链接
    from services.web import generate_token
    token = generate_token(chat_id)
    host = os.getenv("DASHBOARD_HOST", "localhost:8080")
    url = f"http://{host}/dashboard?token={token}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 打开实时面板", url=url)]
    ])
    await query.edit_message_text(
        f"✅ 自动监控已开启\n\n"
        f"📋 监控 {len(selected)} 个系列：\n"
        + "\n".join(selected_names) + "\n\n"
        f"关停规则：\n"
        f"• 展示≥1000 CTR<0.5% → 暂停（素材不行）\n"
        f"• 点击≥50 订阅率<5% → 暂停（落地页/定向问题）\n\n"
        f"发 /automonitor off 关闭",
        reply_markup=keyboard,
    )

    # 立即推送一次数据到 Dashboard
    from services.monitor import run_once
    await run_once(ctx.application, chat_id)


# ══════════════════════════════════════════════════════════════════
# 按钮引导式创建广告
# ══════════════════════════════════════════════════════════════════
(OD_TOKEN, OD_ACCOUNT, OD_NAME, OD_COUNT, OD_BUDGET, OD_URL,
 OD_COUNTRY, OD_DEVICE, OD_GENDER, OD_AGE, OD_CONFIRM,
 OD_MEDIA, OD_TEXT, OD_TITLE, OD_PUBLISH) = range(15)


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

    # 查像素
    pixel_id = ""
    try:
        resp = _requests.get(f"{GRAPH}/act_{acc_id}/adspixels", params={
            "access_token": token, "fields": "id,name", "limit": 5,
        })
        pixels = resp.json().get("data", [])
        if pixels:
            pixel_id = pixels[0].get("id", "")
    except Exception as e:
        logger.warning(f"查询像素失败: {e}")

    # 查主页 — 通过广告账户的 business 字段拿 BM ID，再查 owned_pages
    page_id = ""
    try:
        # 方式1: 广告账户 → business_id → owned_pages
        resp = _requests.get(f"{GRAPH}/act_{acc_id}", params={
            "access_token": token, "fields": "business",
        })
        biz = resp.json().get("business", {})
        biz_id = biz.get("id", "")
        if biz_id:
            resp = _requests.get(f"{GRAPH}/{biz_id}/owned_pages", params={
                "access_token": token, "fields": "id,name", "limit": 10,
            })
            pages = resp.json().get("data", [])
            if pages:
                page_id = pages[0].get("id", "")

        # 方式2: promote_pages（兜底）
        if not page_id:
            resp = _requests.get(f"{GRAPH}/act_{acc_id}/promote_pages", params={
                "access_token": token, "fields": "id,name", "limit": 5,
            })
            pages = resp.json().get("data", [])
            if pages:
                page_id = pages[0].get("id", "")
    except Exception as e:
        logger.warning(f"查询主页失败: {e}")

    # 更新 FBConfig
    fb_configs[chat_id] = FBConfig(
        access_token=token,
        ad_account_id=acc_id,
        pixel_id=pixel_id,
        page_id=page_id,
    )

    # 清理临时数据
    ctx.chat_data.pop("od_accounts", None)
    ctx.chat_data.pop("od_selected_account", None)

    mode = ctx.chat_data.get("od_mode", "normal")
    mode_label = "创建广告"

    text = (
        f"✅ 已选择：{acc_name} (act_{acc_id})\n"
        f"像素: {pixel_id or '未找到'} | 主页: {page_id or '未找到'}\n\n"
        f"📋 {mode_label} — 请输入系列名称\n"
        f"命名格式：产品-渠道号-优化师-日期-系列几\n"
        f"例如：bet7-778-DT-0403-1\n\n"
        f"发 /cancel 取消"
    )

    if edit_msg:
        await edit_msg.edit_text(text)
    else:
        await update.effective_chat.send_message(text)

    return OD_NAME


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


async def od_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到系列名，问组数"""
    name = update.message.text.strip()
    if not name or name.startswith("/"):
        await update.message.reply_text("❌ 名称不能为空或以 / 开头，请重新输入：")
        return OD_NAME

    ctx.chat_data["od_camp_name"] = name
    await update.message.reply_text(
        f"✅ 系列名：{name}\n\n请输入广告组数量（1-100）：",
    )
    return OD_COUNT


async def od_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到组数"""
    text = update.message.text.strip()
    try:
        count = int(text)
        if count < 1 or count > 100:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ 请输入 1-100 的整数：")
        return OD_COUNT

    ctx.chat_data["od_count"] = count

    await update.message.reply_text(
        f"✅ 广告组数：{count}\n\n请输入系列日预算（美元）：",
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
    mode = d["od_mode"]
    gender_label = {0: "全部", 1: "男", 2: "女"}[d["od_gender"]]

    mode_line = f"💰 系列预算：${d['od_budget']}/天（CBO 自动分配）"

    summary = (
        f"📋 请确认广告配置：\n"
        f"{'─' * 24}\n"
        f"{mode_line}\n"
        f"📝 系列名：{d['od_camp_name']}\n"
        f"📦 广告组：{d['od_count']} 组\n"
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

    await query.edit_message_text("⏳ 正在创建广告系列，请稍候...")

    try:
        camp_id, adset_ids = normal_flow(
            fb, d["od_camp_name"], d["od_count"],
            daily_budget_usd=d["od_budget"],
            country=d["od_country"],
            device_os=d["od_device"],
            age_min=d["od_age_min"],
            age_max=d["od_age_max"],
            gender=d["od_gender"],
        )

        ctx.chat_data["last_campaign_id"] = camp_id
        ctx.chat_data["last_adset_ids"]   = adset_ids
        ctx.chat_data["last_landing_url"] = d["od_url"]


        gender_label = {0: "全部", 1: "男", 2: "女"}[d["od_gender"]]
        await query.edit_message_text(
            f"✅ 广告系列创建完成！\n\n"
            f"📋 系列 ID：{camp_id}\n"
            f"💰 系列预算：${d['od_budget']}/天（CBO 自动分配）\n"
            f"📦 广告组：{d['od_count']} 组（均为 PAUSED）\n"
            f"🌍 {d['od_country']} | {d['od_device']} | "
            f"{d['od_age_min']}-{d['od_age_max']}岁 | {gender_label}\n\n"
            f"📎 请发送视频或图片素材\n"
            f"（发 /cancel 取消）",
        )
        return OD_MEDIA
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
        file_obj = update.message.photo[-1]  # 取最大尺寸
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
            ctx.chat_data["last_image_hash"] = image_hash
            ctx.chat_data.pop("last_video_id", None)
            media_id = image_hash
        else:
            video_id = fb.upload_video(tmp_path, title=file_name)
            ctx.chat_data["last_video_id"] = video_id
            ctx.chat_data.pop("last_image_hash", None)
            media_id = video_id

        await msg.edit_text(
            f"✅ 素材上传成功！（ID：{media_id}）\n\n"
            f"📝 请输入广告正文（显示在视频上方）\n"
            f"发「跳过」使用默认文案\n"
            f"（发 /cancel 取消）",
        )
        Path(tmp_path).unlink(missing_ok=True)
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

    await update.message.reply_text(
        f"✅ 广告正文：{text if text not in ('跳过', 'skip') else '（默认）'}\n\n"
        f"📝 请输入广告标题（显示在视频下方）\n"
        f"发「跳过」使用默认标题",
    )
    return OD_TITLE


async def od_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """收到广告标题，显示最终确认"""
    text = update.message.text.strip()
    default_title = "Se você não quer ganhar, não clique!"
    if text in ("跳过", "skip"):
        ctx.chat_data["od_ad_title"] = default_title
        title_display = f"（默认：{default_title}）"
    else:
        ctx.chat_data["od_ad_title"] = text
        title_display = text

    d = ctx.chat_data
    ad_text = d.get("od_ad_text", "")
    adset_count = len(d.get("last_adset_ids", []))

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
    adset_ids  = d.get("last_adset_ids", [])
    video_id   = d.get("last_video_id", "")
    image_hash = d.get("last_image_hash", "")
    landing    = d.get("last_landing_url", d.get("od_url", ""))
    camp_id    = d.get("last_campaign_id", "")
    ad_text    = d.get("od_ad_text", "")
    ad_title   = d.get("od_ad_title", "Se você não quer ganhar, não clique!")

    if not adset_ids or (not video_id and not image_hash):
        await query.edit_message_text("❌ 缺少广告组或素材信息。")
        return ConversationHandler.END

    await query.edit_message_text("⏳ 正在绑定素材并发布广告...")

    try:
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
