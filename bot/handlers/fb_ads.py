"""FB 广告命令：配置、报表、系列管理、预算、监控"""
import logging
from telegram import Update
from telegram.ext import ContextTypes

from fb import FBConfig, FBError
from fb.insights import parse_action, parse_action_value, format_report_row
from store.state import fb_configs, monitor_chats, get_fb
from services.campaign import one_dollar_flow

logger = logging.getLogger(__name__)


def _require_fb(func):
    """装饰器：命令需要先配置 FB 账户"""
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
    cfg = fb_configs.get(update.effective_chat.id) or fb.cfg

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


# ── 一刀流 ──────────────────────────────────────────────────────
@_require_fb
async def onedollar_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /onedollar <系列名> <组数> <落地页URL>
    """
    if len(ctx.args) < 3:
        await update.message.reply_text(
            "用法：`/onedollar <系列名> <组数> <落地页URL>`\n\n"
            "示例：`/onedollar bet7-778-DT-0402 50 https://betxxx.com/app/index.html`",
            parse_mode="Markdown",
        )
        return

    fb = get_fb(update.effective_chat.id)
    camp_name, count_str, landing_url = ctx.args[0], ctx.args[1], ctx.args[2]

    try:
        count = int(count_str)
    except ValueError:
        await update.message.reply_text("❌ 组数必须是整数")
        return

    if count > 100:
        await update.message.reply_text("⚠️ 最多一次创建 100 组。")
        return

    msg = await update.message.reply_text(
        f"⏳ 创建一刀流...\n系列: {camp_name}\n广告组: {count} 组（$1/天）"
    )
    try:
        camp_id, adset_ids = one_dollar_flow(fb, camp_name, count)

        ctx.chat_data["last_campaign_id"]  = camp_id
        ctx.chat_data["last_adset_ids"]    = adset_ids
        ctx.chat_data["last_landing_url"]  = landing_url

        await msg.edit_text(
            f"✅ 一刀流创建完成！\n\n"
            f"📋 系列 ID: `{camp_id}`\n"
            f"📦 广告组: {count} 组（均为 PAUSED）\n"
            f"💰 每组预算: $1/天\n"
            f"🔗 落地页: {landing_url}\n\n"
            f"下一步：发送视频给我，然后用 /publish_last 发布",
            parse_mode="Markdown",
        )
    except FBError as e:
        await msg.edit_text(f"❌ 创建失败: {e}")


# ── 发布到最近的广告组 ─────────────────────────────────────────
@_require_fb
async def publish_last_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /publish_last [正文文案|标题]
    """
    fb        = get_fb(update.effective_chat.id)
    adset_ids = ctx.chat_data.get("last_adset_ids", [])
    video_id  = ctx.chat_data.get("last_video_id", "")
    landing   = ctx.chat_data.get("last_landing_url", "")
    camp_id   = ctx.chat_data.get("last_campaign_id", "")

    if not adset_ids:
        await update.message.reply_text("❌ 没有待发布的广告组，请先运行 /onedollar")
        return
    if not video_id:
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
            video_id=video_id,
            landing_url=landing,
            message=message_text,
            title=title_text,
            camp_id=camp_id,
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
    """/automonitor on|off"""
    chat_id = update.effective_chat.id
    arg = (ctx.args[0].lower() if ctx.args else "on")

    if arg == "off":
        monitor_chats[chat_id] = False
        await update.message.reply_text("❌ 自动监控已关闭。")
    else:
        monitor_chats[chat_id] = True
        await update.message.reply_text(
            "✅ 自动监控已开启（每30分钟检查）\n\n"
            "规则：\n"
            "• 消耗$3 无点击 → 暂停\n"
            "• 消耗$5 无注册 → 暂停\n"
            "• 消耗$9 无首充 → 暂停"
        )
