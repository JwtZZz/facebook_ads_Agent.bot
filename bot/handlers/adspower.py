"""Adspower 指纹浏览器命令"""
import logging
import os
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from fb.config import FBConfig
from store.state import fb_configs
from services.adspower import (
    list_profiles, start_profile, stop_profile,
    list_active, check_status, get_profile_info,
    create_profile, convert_cookies_to_json, AdspowerError,
)
from services.browser import (
    accept_bm_invite, login_fb, register_developer,
    scrape_bm_settings, BrowserError,
)

logger = logging.getLogger(__name__)


async def profiles_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/profiles — 列出所有指纹环境"""
    try:
        profiles = list_profiles()
        if not profiles:
            await update.message.reply_text("📭 没有找到任何指纹环境。")
            return

        active_serials = {p.get("serial_number") for p in list_active()}

        lines = ["🖥 *指纹环境列表：*\n"]
        for p in profiles[:30]:
            sn     = p.get("serial_number", "?")
            name   = p.get("name") or p.get("username") or sn
            status = "🟢 已开启" if sn in active_serials else "⚫ 未开启"
            lines.append(f"{status}  `{sn}`  {name}")

        lines.append(f"\n共 {len(profiles)} 个环境")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except AdspowerError as e:
        await update.message.reply_text(f"❌ {e}")


async def login_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/login <编号> — 自动登录 FB（从环境信息读取账密和2FA）"""
    sn = ctx.args[0] if ctx.args else ""
    if not sn:
        await update.message.reply_text("用法：`/login <编号>`", parse_mode="Markdown")
        return

    msg = await update.message.reply_text(
        f"⏳ 正在用环境 `{sn}` 自动登录 FB...", parse_mode="Markdown"
    )
    try:
        result = await login_fb(sn)
        await msg.edit_text(f"✅ {result}\n环境: `{sn}`", parse_mode="Markdown")
    except (BrowserError, AdspowerError) as e:
        await msg.edit_text(f"❌ {e}", parse_mode="Markdown")


async def regdev_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/regdev <编号> — 自动注册 FB 开发者并尝试获取 token"""
    sn = ctx.args[0] if ctx.args else ""
    if not sn:
        await update.message.reply_text("用法：`/regdev <编号>`", parse_mode="Markdown")
        return

    msg = await update.message.reply_text(
        f"⏳ 正在用环境 `{sn}` 注册开发者...", parse_mode="Markdown"
    )
    try:
        result = await register_developer(sn)
        await msg.edit_text(f"📋 {result}\n环境: `{sn}`", parse_mode="Markdown")
    except (BrowserError, AdspowerError) as e:
        await msg.edit_text(f"❌ {e}", parse_mode="Markdown")


async def open_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /open <编号>  — 打开指纹浏览器
    也可以直接发编号（纯数字），Bot 自动识别
    """
    sn = ctx.args[0] if ctx.args else ""
    if not sn:
        await update.message.reply_text(
            "用法：`/open <编号>`\n\n"
            "或直接发编号给我，例如：`3185`",
            parse_mode="Markdown",
        )
        return

    msg = await update.message.reply_text(f"⏳ 正在打开环境 `{sn}`...", parse_mode="Markdown")
    try:
        if check_status(sn):
            await msg.edit_text(f"✅ 环境 `{sn}` 已经是开启状态", parse_mode="Markdown")
            return

        result = start_profile(sn)

        ctx.chat_data[f"cdp_{sn}"] = result["ws_puppeteer"]
        ctx.chat_data["last_serial_number"] = sn

        lines = [
            f"✅ 浏览器已开启！",
            f"编号: `{sn}`",
        ]
        if result["debug_port"]:
            lines.append(f"调试端口: `{result['debug_port']}`")

        await msg.edit_text("\n".join(lines), parse_mode="Markdown")

    except AdspowerError as e:
        await msg.edit_text(f"❌ 打开失败: {e}")


async def close_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/close <编号> — 关闭指纹浏览器"""
    sn = ctx.args[0] if ctx.args else ctx.chat_data.get("last_serial_number", "")
    if not sn:
        await update.message.reply_text("用法：`/close <编号>`", parse_mode="Markdown")
        return

    try:
        stop_profile(sn)
        ctx.chat_data.pop(f"cdp_{sn}", None)
        await update.message.reply_text(f"✅ 环境 `{sn}` 已关闭", parse_mode="Markdown")
    except AdspowerError as e:
        await update.message.reply_text(f"❌ 关闭失败: {e}")


async def info_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/info <编号> — 查看环境详细信息"""
    sn = ctx.args[0] if ctx.args else ctx.chat_data.get("last_serial_number", "")
    if not sn:
        await update.message.reply_text("用法：`/info <编号>`", parse_mode="Markdown")
        return

    try:
        p = get_profile_info(sn)
        if not p:
            await update.message.reply_text(f"❌ 找不到编号 `{sn}` 的环境", parse_mode="Markdown")
            return

        proxy = p.get("user_proxy_config", {})
        is_active = check_status(sn)
        last_open = p.get("last_open_time", "")
        if last_open:
            last_open = datetime.fromtimestamp(int(last_open)).strftime("%m-%d %H:%M")

        lines = [
            f"📋 *环境信息 — {sn}*\n",
            f"名称: `{p.get('name', '?')}`",
            f"状态: {'🟢 已开启' if is_active else '⚫ 未开启'}",
            f"分组: {p.get('group_name', '?')}",
            f"",
            f"👤 *账号信息*",
            f"FB账号: `{p.get('username', '?')}`",
            f"密码: `{p.get('password', '?')}`",
            f"2FA秘钥: `{p.get('fakey', '?')}`",
            f"",
            f"🌐 *代理信息*",
            f"IP: `{p.get('ip', '?')}` ({p.get('ip_country', '?').upper()})",
            f"类型: {proxy.get('proxy_type', '?')}",
            f"地址: `{proxy.get('proxy_host', '')}:{proxy.get('proxy_port', '')}`",
            f"账号: `{proxy.get('proxy_user', '')}`",
            f"",
            f"🕐 最后打开: {last_open or '从未'}",
        ]

        remark = p.get("remark", "").strip()
        if remark:
            lines.append(f"\n📝 *备注*\n{remark[:200]}")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except AdspowerError as e:
        await update.message.reply_text(f"❌ {e}")


async def active_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/active — 查看当前已开启的环境"""
    try:
        profiles = list_active()
        if not profiles:
            await update.message.reply_text("当前没有开启的环境。")
            return
        lines = [f"🟢 *当前开启的环境（{len(profiles)} 个）：*\n"]
        for p in profiles:
            lines.append(f"• `{p.get('serial_number', '?')}`  {p.get('name', '')}")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except AdspowerError as e:
        await update.message.reply_text(f"❌ {e}")


async def acceptbm_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    支持单条和批量：
    /acceptbm <编号> <BM链接>
    或直接发多行：
      https://business.facebook.com/invitation/?token=xxx 3165
      https://business.facebook.com/invitation/?token=yyy 3185
    """
    text = update.message.text or ""
    # 去掉命令前缀
    if text.startswith("/acceptbm"):
        text = text[len("/acceptbm"):].strip()

    tasks = _parse_bm_tasks(text)

    if not tasks:
        await update.message.reply_text(
            "用法：`/acceptbm <编号> <BM链接>`\n\n"
            "批量：每行一组 `BM链接 编号`\n"
            "```\nhttps://...token=xxx 3165\nhttps://...token=yyy 3185\n```",
            parse_mode="Markdown",
        )
        return

    # 单条
    if len(tasks) == 1:
        sn, bm_url = tasks[0]
        msg = await update.message.reply_text(
            f"⏳ 正在用环境 `{sn}` 接受 BM 邀请...", parse_mode="Markdown"
        )
        try:
            result = await accept_bm_invite(sn, bm_url)
            await msg.edit_text(f"✅ {result}\n环境: `{sn}`", parse_mode="Markdown")
        except (BrowserError, AdspowerError) as e:
            await msg.edit_text(f"❌ 环境 `{sn}`: {e}", parse_mode="Markdown")
        return

    # 批量
    msg = await update.message.reply_text(
        f"⏳ 批量接受 BM 邀请（共 {len(tasks)} 条）..."
    )
    results = []
    for i, (sn, bm_url) in enumerate(tasks, 1):
        await msg.edit_text(
            f"⏳ 进度: {i}/{len(tasks)}，正在处理环境 `{sn}`...",
            parse_mode="Markdown",
        )
        try:
            result = await accept_bm_invite(sn, bm_url)
            results.append(f"✅ `{sn}`: {result}")
        except (BrowserError, AdspowerError) as e:
            results.append(f"❌ `{sn}`: {e}")

    lines = [f"📋 *批量接受 BM 结果（{len(tasks)} 条）：*\n"] + results
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")


def _parse_bm_tasks(text: str) -> list[tuple[str, str]]:
    """
    从文本中解析出 (编号, BM链接) 列表
    支持多行，每行格式：BM链接 编号 或 编号 BM链接
    """
    tasks = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or "business.facebook.com/invitation" not in line:
            continue
        parts = line.split()
        sn, bm_url = "", ""
        for part in parts:
            if "business.facebook.com/invitation" in part:
                bm_url = part
            elif part.isdigit():
                sn = part
        if sn and bm_url:
            tasks.append((sn, bm_url))
    return tasks


async def handle_profile_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    自动识别：
    - 纯数字(3位+) → 打开环境
    - 包含 BM 邀请链接 → 接受 BM（支持多行批量）
    """
    text = (update.message.text or "").strip()

    if "business.facebook.com/invitation" in text:
        await acceptbm_cmd(update, ctx)
        return

    if text.isdigit() and len(text) >= 3:
        ctx.args = [text]
        await open_cmd(update, ctx)


# ════════════════════════════════════════════════════════════════
# /setupbm — 一键配置 BM（多步对话）
# ════════════════════════════════════════════════════════════════

WAITING_TOKEN = 0


def _parse_account_info(text: str) -> dict | None:
    """
    解析管道符分隔的账号信息
    格式: UID|密码|2FA秘钥|邮箱|邮箱密码|备用邮箱|token_cookie|profile_uuid||cookie_string|
    """
    parts = text.strip().split("|")
    # 去掉末尾空元素
    while parts and not parts[-1].strip():
        parts.pop()

    if len(parts) < 3:
        return None

    uid = parts[0].strip()
    if not uid.isdigit():
        return None

    info = {
        "uid": uid,
        "password": parts[1].strip() if len(parts) > 1 else "",
        "fakey": parts[2].strip() if len(parts) > 2 else "",
        "email": parts[3].strip() if len(parts) > 3 else "",
        "email_password": parts[4].strip() if len(parts) > 4 else "",
        "recovery_email": parts[5].strip() if len(parts) > 5 else "",
        "token_cookie": parts[6].strip() if len(parts) > 6 else "",
        "profile_uuid": parts[7].strip() if len(parts) > 7 else "",
        "cookie_string": "",
    }

    # cookie_string 通常在第 9 个字段（index 9），包含 datr=xxx;sb=xxx;...
    for i in range(8, len(parts)):
        val = parts[i].strip()
        if val and "=" in val and ("datr" in val or "c_user" in val or "xs=" in val):
            info["cookie_string"] = val
            break

    return info


async def setupbm_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """
    /setupbm
    入口：直接问 Token，用 API 查询所有 ID
    """
    await update.message.reply_text(
        "🔑 请发送你的 **Access Token**\n"
        "（以 EAA 开头的长字符串）\n\n"
        "发送后机器人会自动通过 API 获取：\n"
        "• 广告账户 ID\n"
        "• 像素 ID\n"
        "• 主页 ID\n\n"
        "发 /cancel 取消",
        parse_mode="Markdown",
    )
    return WAITING_TOKEN


async def setupbm_token(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """接收 Token → 用 API 查询真实 ID → 配置 FBConfig → 完成"""
    token = (update.message.text or "").strip()

    if len(token) < 20:
        await update.message.reply_text(
            "❌ Token 太短，请发送完整的 Access Token\n"
            "（以 EAA 开头的长字符串，发 /cancel 取消）"
        )
        return WAITING_TOKEN

    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ 正在通过 API 验证 Token 并获取账户信息...")

    # 用 Token 通过 Graph API 查询真实的广告账户、像素、主页
    ad_account = ""
    pixel_id = ""
    page_id = ""
    api_errors = []

    try:
        import requests
        GRAPH = "https://graph.facebook.com/v20.0"
        headers = {}
        params_base = {"access_token": token}

        # 1. 查广告账户 — /me/adaccounts
        try:
            resp = requests.get(f"{GRAPH}/me/adaccounts",
                                params={**params_base, "fields": "id,name,account_id", "limit": 5})
            data = resp.json()
            accounts = data.get("data", [])
            if accounts:
                # account_id 是纯数字（不带 act_ 前缀）
                ad_account = accounts[0].get("account_id", "")
                logger.info(f"API 查到广告账户: {ad_account}")
        except Exception as e:
            api_errors.append(f"广告账户: {e}")
            logger.warning(f"API 查询广告账户失败: {e}")

        # 2. 查像素 — /act_{id}/adspixels
        if ad_account:
            try:
                resp = requests.get(f"{GRAPH}/act_{ad_account}/adspixels",
                                    params={**params_base, "fields": "id,name", "limit": 5})
                data = resp.json()
                pixels = data.get("data", [])
                if pixels:
                    pixel_id = pixels[0].get("id", "")
                    logger.info(f"API 查到像素: {pixel_id}")
            except Exception as e:
                api_errors.append(f"像素: {e}")
                logger.warning(f"API 查询像素失败: {e}")

        # 3. 查主页 — 通过广告账户的 business → owned_pages
        if ad_account:
            try:
                resp = requests.get(f"{GRAPH}/act_{ad_account}",
                                    params={**params_base, "fields": "business"})
                biz = resp.json().get("business", {})
                biz_id = biz.get("id", "")
                if biz_id:
                    resp = requests.get(f"{GRAPH}/{biz_id}/owned_pages",
                                        params={**params_base, "fields": "id,name", "limit": 10})
                    pages = resp.json().get("data", [])
                    if pages:
                        page_id = pages[0].get("id", "")
                        logger.info(f"API 查到主页(owned_pages): {page_id}")
            except Exception as e:
                logger.warning(f"owned_pages 查询失败: {e}")

        # 兜底: promote_pages
        if not page_id and ad_account:
            try:
                resp = requests.get(f"{GRAPH}/act_{ad_account}/promote_pages",
                                    params={**params_base, "fields": "id,name", "limit": 5})
                pages = resp.json().get("data", [])
                if pages:
                    page_id = pages[0].get("id", "")
                    logger.info(f"API 查到主页(promote_pages): {page_id}")
            except Exception as e:
                api_errors.append(f"主页: {e}")
                logger.warning(f"API 查询主页失败: {e}")

    except Exception as e:
        logger.error(f"API 查询整体失败: {e}")

    # 如果 API 没查到，用页面抓取结果兜底
    if not ad_account:
        ad_account = ctx.chat_data.get("setupbm_ad_account", "") or os.getenv("FB_AD_ACCOUNT_ID", "")
    if not pixel_id:
        pixel_id = ctx.chat_data.get("setupbm_pixel", "") or os.getenv("FB_PIXEL_ID", "")
    if not page_id:
        page_id = ctx.chat_data.get("setupbm_page", "") or os.getenv("FB_PAGE_ID", "")

    # 配置 FBConfig（内存）
    fb_configs[chat_id] = FBConfig(
        access_token=token,
        ad_account_id=ad_account,
        pixel_id=pixel_id,
        page_id=page_id,
    )

    # 同时写入 .env 文件持久化
    try:
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
        _update_env(env_path, {
            "FB_ACCESS_TOKEN": token,
            "FB_AD_ACCOUNT_ID": ad_account,
            "FB_PIXEL_ID": pixel_id,
            "FB_PAGE_ID": page_id,
        })
    except Exception as e:
        logger.warning(f"写入 .env 失败: {e}")

    # 清理 chat_data
    for key in list(ctx.chat_data.keys()):
        if key.startswith("setupbm_"):
            del ctx.chat_data[key]

    error_note = ""
    if api_errors:
        error_note = "\n⚠️ 部分 API 查询失败：" + "、".join(api_errors)

    await msg.edit_text(
        f"✅ BM 配置完成！（通过 API 验证）\n\n"
        f"广告账户: act_{ad_account}\n"
        f"像素 ID: {pixel_id or '❌ 未找到'}\n"
        f"主页 ID: {page_id or '❌ 未找到'}\n"
        f"Token: {token[:20]}...{error_note}\n\n"
        f"现在可以使用以下命令：\n"
        f"/campaigns — 查看广告系列\n"
        f"/normal — 创建广告投放\n"
        f"/report — 查看数据报表"
    )
    return ConversationHandler.END


def _update_env(env_path: str, updates: dict):
    """更新 .env 文件中的指定键值"""
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    updated_keys = set()
    new_lines = []
    for line in lines:
        key = line.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}\n")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    # 追加没找到的 key
    for key, value in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


async def setupbm_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """取消 setupbm 流程"""
    sn = ctx.chat_data.get("setupbm_sn", "")
    if sn:
        try:
            stop_profile(sn)
        except Exception:
            pass

    # 清理 chat_data
    for key in list(ctx.chat_data.keys()):
        if key.startswith("setupbm_"):
            del ctx.chat_data[key]

    await update.message.reply_text("❌ 已取消 BM 配置流程。")
    return ConversationHandler.END
