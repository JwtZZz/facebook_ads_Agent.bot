"""浏览器自动化服务 — 通过 Adspower CDP 控制浏览器"""
import logging
import re
import pyotp
from playwright.async_api import async_playwright

from services.adspower import start_profile, stop_profile, check_status, get_profile_info

logger = logging.getLogger(__name__)


class BrowserError(Exception):
    pass


async def accept_bm_invite(serial_number: str, bm_url: str) -> str:
    """
    打开指纹环境，访问 BM 邀请链接，接受邀请
    返回结果文字
    """
    already_open = check_status(serial_number)
    result = start_profile(serial_number)
    debug_port = result["debug_port"]
    if not debug_port:
        raise BrowserError("无法获取浏览器调试端口")

    try:
        return await _do_accept(debug_port, bm_url)
    finally:
        if not already_open:
            try:
                stop_profile(serial_number)
            except Exception:
                pass


async def _do_accept(debug_port: str, bm_url: str) -> str:
    """连接浏览器，执行接受 BM 邀请操作"""
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        context = browser.contexts[0]
        pages = context.pages
        page = pages[0] if pages else await context.new_page()

        try:
            # 1. 打开 BM 邀请链接
            await page.goto(bm_url, wait_until="load", timeout=30000)
            await page.wait_for_timeout(3000)

            # 2. 如果在登录页，点击 FB 登录
            if "loginpage" in page.url or ("login" in page.url and "invitation" not in page.url):
                fb_btn = page.locator(
                    '[role="button"]:has-text("Facebook"), '
                    'button:has-text("Facebook 帳號"), '
                    'button:has-text("Facebook 账号"), '
                    'button:has-text("Log in with Facebook")'
                ).first
                if await fb_btn.count() > 0:
                    await fb_btn.click()
                    await page.wait_for_timeout(5000)
                    await page.wait_for_load_state("load", timeout=20000)
                else:
                    raise BrowserError("找不到 Facebook 登录按钮，该环境可能未登录 FB")

            # 3. 登录后如果不在邀请页，重新 goto
            if "invitation" not in page.url:
                await page.goto(bm_url, wait_until="load", timeout=30000)
                await page.wait_for_timeout(5000)

            # 再次检查是否在登录页
            if "loginpage" in page.url or "login.php" in page.url:
                raise BrowserError("环境的 FB 登录已过期，请手动登录后重试")

            bm_name = ""

            # 4. 循环点击所有按钮（接受、继续等）直到进入 BM 管理界面
            for step in range(10):
                body_text = await page.inner_text("body")
                if not bm_name:
                    bm_name = _extract_bm_name(body_text)

                # 检查是否已进入 BM 管理界面
                if _is_inside_bm(page.url):
                    return f"BM 邀请接受成功，已进入管理界面！（{bm_name}）"

                # 检查过期/无效
                if "過期" in body_text or "expired" in body_text.lower() or "过期" in body_text:
                    raise BrowserError("BM 邀请链接已过期")
                if "無效" in body_text or "invalid" in body_text.lower() or "无效" in body_text:
                    raise BrowserError("BM 邀请链接无效")

                # 依次尝试点击：接受 → 继续 → 开始
                btn = page.locator(
                    '[role="button"]:has-text("接受"), [role="button"]:has-text("Accept"), '
                    '[role="button"]:has-text("加入"), [role="button"]:has-text("Join"), '
                    'button:has-text("接受"), button:has-text("Accept"), '
                    'button:has-text("加入"), button:has-text("Join")'
                ).first

                if await btn.count() == 0:
                    # 找"继续"按钮
                    btn = page.locator(
                        '[role="button"]:has-text("繼續"), [role="button"]:has-text("继续"), '
                        '[role="button"]:has-text("Continue"), '
                        'button:has-text("繼續"), button:has-text("继续"), '
                        'button:has-text("Continue"), '
                        'a:has-text("繼續"), a:has-text("继续"), a:has-text("Continue")'
                    ).first

                if await btn.count() == 0:
                    # 找"开始"/"完成"按钮
                    btn = page.locator(
                        '[role="button"]:has-text("開始"), [role="button"]:has-text("开始"), '
                        '[role="button"]:has-text("完成"), [role="button"]:has-text("Done"), '
                        '[role="button"]:has-text("Get Started"), '
                        'button:has-text("開始"), button:has-text("开始"), '
                        'button:has-text("完成"), button:has-text("Done")'
                    ).first

                if await btn.count() > 0:
                    logger.info(f"BM accept step {step}: clicking button")
                    await btn.click()
                    await page.wait_for_timeout(4000)
                    await page.wait_for_load_state("load", timeout=15000)
                else:
                    # 没有更多按钮可点了
                    if "已接受" in body_text or "already accepted" in body_text.lower():
                        # 尝试点 "继续前往商业工具" 链接
                        link = page.locator('a:has-text("商業工具"), a:has-text("商业工具"), a:has-text("business tools")').first
                        if await link.count() > 0:
                            await link.click()
                            await page.wait_for_timeout(4000)
                            if _is_inside_bm(page.url):
                                return f"BM 邀请已接受，已进入管理界面！（{bm_name}）"
                        return f"BM 邀请已接受（{bm_name}）"
                    break

            # 最终检查
            if _is_inside_bm(page.url):
                return f"BM 邀请接受成功，已进入管理界面！（{bm_name}）"

            body_final = await page.inner_text("body")
            if "已接受" in body_final or "accepted" in body_final.lower():
                return f"BM 邀请已接受（{bm_name}）"

            raise BrowserError(f"未能完成接受流程，页面内容: {body_final[:200]}")

        except BrowserError:
            raise
        except Exception as e:
            raise BrowserError(f"操作失败: {e}") from e
        finally:
            await browser.close()


def _is_inside_bm(url: str) -> bool:
    """判断是否已进入 BM 管理界面"""
    bm_pages = [
        "business.facebook.com/settings",
        "business.facebook.com/home",
        "business.facebook.com/overview",
        "business.facebook.com/latest",
        "business.facebook.com/ads",
        "business.facebook.com/asset",
    ]
    return any(p in url for p in bm_pages)


def _extract_bm_name(text: str) -> str:
    """从页面文字中提取 BM 名称"""
    for marker in ["加入", "join ", "Join "]:
        if marker in text:
            start = text.index(marker) + len(marker)
            chunk = text[start:start + 80].strip()
            name = chunk.split("\n")[0].strip().strip("「」\"'")
            if name:
                return name[:50]
    return "BM"


# ════════════════════════════════════════════════════════════════
# 自动登录 FB
# ════════════════════════════════════════════════════════════════

async def login_fb(serial_number: str) -> str:
    """
    自动登录 FB：从 Adspower 环境信息读取账号密码和2FA秘钥
    返回结果文字
    """
    profile = get_profile_info(serial_number)
    if not profile:
        raise BrowserError(f"找不到编号 {serial_number} 的环境")

    username = profile.get("username", "")
    password = profile.get("password", "")
    fakey = profile.get("fakey", "")

    if not username or not password:
        raise BrowserError("环境缺少账号或密码信息")

    already_open = check_status(serial_number)
    result = start_profile(serial_number)
    debug_port = result["debug_port"]
    if not debug_port:
        raise BrowserError("无法获取浏览器调试端口")

    try:
        return await _do_login(debug_port, username, password, fakey)
    finally:
        if not already_open:
            try:
                stop_profile(serial_number)
            except Exception:
                pass


async def _do_login(debug_port: str, username: str, password: str, fakey: str) -> str:
    """连接浏览器，执行 FB 登录"""
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        try:
            await page.goto("https://www.facebook.com/", wait_until="load", timeout=20000)
            await page.wait_for_timeout(2000)

            # 检查是否已登录
            cookies = await context.cookies("https://www.facebook.com")
            c_user = [c for c in cookies if c["name"] == "c_user"]
            if c_user:
                return f"已经是登录状态（账号: {c_user[0]['value']}）"

            # 查找登录表单
            email_input = page.locator('#email, [name="email"]')
            pass_input = page.locator('#pass, [name="pass"]')

            if await email_input.count() == 0:
                # 可能是新版登录页
                await page.goto("https://www.facebook.com/login/", wait_until="load", timeout=20000)
                await page.wait_for_timeout(2000)
                email_input = page.locator('#email, [name="email"]')
                pass_input = page.locator('#pass, [name="pass"]')

            if await email_input.count() == 0 or await pass_input.count() == 0:
                raise BrowserError("找不到登录表单")

            # 填写账号密码
            await email_input.fill(username)
            await page.wait_for_timeout(500)
            await pass_input.fill(password)
            await page.wait_for_timeout(500)

            # 点击登录
            login_btn = page.locator('[name="login"], [type="submit"], button[data-testid="royal_login_button"]').first
            if await login_btn.count() > 0:
                await login_btn.click()
            else:
                await pass_input.press("Enter")

            await page.wait_for_timeout(5000)
            await page.wait_for_load_state("load", timeout=20000)

            # 检查是否需要 2FA
            body = await page.inner_text("body")
            url = page.url

            if "two_step_verification" in url or "checkpoint" in url or "验证" in body or "驗證" in body or "approval_code" in url or "2fac" in url.lower():
                if not fakey:
                    raise BrowserError("需要二步验证但没有 2FA 秘钥")

                # 生成 TOTP 验证码
                totp = pyotp.TOTP(fakey)
                code = totp.now()
                logger.info(f"2FA code generated: {code}")

                # 找验证码输入框
                code_input = page.locator(
                    '#approvals_code, [name="approvals_code"], '
                    'input[type="text"], input[type="tel"], input[type="number"]'
                ).first

                if await code_input.count() > 0:
                    await code_input.fill(code)
                    await page.wait_for_timeout(500)

                    # 点提交
                    submit_btn = page.locator(
                        'button:has-text("提交"), button:has-text("Submit"), '
                        'button:has-text("繼續"), button:has-text("Continue"), '
                        'button[type="submit"], #checkpointSubmitButton'
                    ).first
                    if await submit_btn.count() > 0:
                        await submit_btn.click()
                    else:
                        await code_input.press("Enter")

                    await page.wait_for_timeout(5000)
                    await page.wait_for_load_state("load", timeout=20000)

                    # 可能还有"记住浏览器"等确认步骤
                    for _ in range(3):
                        cont_btn = page.locator(
                            'button:has-text("繼續"), button:has-text("Continue"), '
                            'button:has-text("确定"), button:has-text("OK"), '
                            'button:has-text("This was me"), button:has-text("是我本人")'
                        ).first
                        if await cont_btn.count() > 0:
                            await cont_btn.click()
                            await page.wait_for_timeout(3000)
                        else:
                            break
                else:
                    raise BrowserError("找不到验证码输入框")

            # 最终检查登录状态
            cookies = await context.cookies("https://www.facebook.com")
            c_user = [c for c in cookies if c["name"] == "c_user"]
            if c_user:
                return f"登录成功（账号: {c_user[0]['value']}）"

            # 检查是否有错误提示
            body_final = await page.inner_text("body")
            if "密码" in body_final and "错误" in body_final:
                raise BrowserError("密码错误")
            if "password" in body_final.lower() and "incorrect" in body_final.lower():
                raise BrowserError("密码错误")
            if "被锁" in body_final or "locked" in body_final.lower() or "disabled" in body_final.lower():
                raise BrowserError("账号已被锁定或停用")

            raise BrowserError(f"登录未成功，当前页面: {page.url[:100]}")

        except BrowserError:
            raise
        except Exception as e:
            raise BrowserError(f"登录失败: {e}") from e
        finally:
            await browser.close()


# ════════════════════════════════════════════════════════════════
# 开发者注册 + 获取 Token
# ════════════════════════════════════════════════════════════════

async def register_developer(serial_number: str) -> str:
    """
    自动完成 FB 开发者注册流程
    返回结果文字（可能需要人工验证手机/邮箱）
    """
    already_open = check_status(serial_number)
    result = start_profile(serial_number)
    debug_port = result["debug_port"]
    if not debug_port:
        raise BrowserError("无法获取浏览器调试端口")

    try:
        return await _do_register_dev(debug_port)
    finally:
        if not already_open:
            try:
                stop_profile(serial_number)
            except Exception:
                pass


async def _do_register_dev(debug_port: str) -> str:
    """连接浏览器，执行开发者注册"""
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        try:
            # 先检查是否已经是开发者
            await page.goto(
                "https://developers.facebook.com/tools/explorer/",
                wait_until="load", timeout=30000,
            )
            await page.wait_for_timeout(3000)

            body = await page.inner_text("body")
            if "Register" not in body and "注册" not in body:
                # 已经是开发者，尝试获取 token
                return await _try_get_token(page)

            # 需要注册 — 打开注册页
            await page.goto(
                "https://developers.facebook.com/async/registration/",
                wait_until="load", timeout=30000,
            )
            await page.wait_for_timeout(3000)

            # 点继续同意条款
            continue_btn = page.locator(
                'button:has-text("繼續"), button:has-text("Continue"), '
                'button:has-text("继续")'
            ).first
            if await continue_btn.count() > 0:
                await continue_btn.click()
                await page.wait_for_timeout(5000)

            # 检查是否需要验证
            body = await page.inner_text("body")
            if "验证" in body or "Verify" in body or "驗證" in body or "phone" in body.lower() or "手机" in body or "電話" in body:
                return "需要验证手机号/邮箱才能完成注册，请手动在浏览器中完成验证"

            # 继续点击后续步骤
            for _ in range(5):
                btn = page.locator(
                    'button:has-text("繼續"), button:has-text("Continue"), '
                    'button:has-text("完成"), button:has-text("Done"), '
                    'button:has-text("Submit"), button:has-text("提交"), '
                    'button:has-text("Skip"), button:has-text("略過")'
                ).first
                if await btn.count() > 0:
                    await btn.click()
                    await page.wait_for_timeout(3000)
                else:
                    break

            # 检查注册是否成功
            await page.goto(
                "https://developers.facebook.com/tools/explorer/",
                wait_until="load", timeout=30000,
            )
            await page.wait_for_timeout(3000)
            body = await page.inner_text("body")

            if "Register" in body or "注册" in body:
                return "注册未完成，可能需要手动验证手机/邮箱"

            return "开发者注册成功！可以使用 Graph API Explorer 获取 token"

        except BrowserError:
            raise
        except Exception as e:
            raise BrowserError(f"注册失败: {e}") from e
        finally:
            await browser.close()


async def _try_get_token(page) -> str:
    """在 Graph API Explorer 页面尝试获取 token"""
    # 检查是否有 "获取权杖" / "Get Token" 按钮
    body = await page.inner_text("body")

    if "沒有可使用的應用程式" in body or "没有可用的应用" in body or "No app available" in body.lower():
        return "开发者已注册，但需要先创建一个应用才能获取 token。请访问 developers.facebook.com/apps 创建应用"

    # 尝试点获取 token
    get_btn = page.locator(
        'button:has-text("取得權杖"), button:has-text("获取权杖"), '
        'button:has-text("Get Token"), button:has-text("Generate")'
    ).first

    if await get_btn.count() > 0:
        await get_btn.click()
        await page.wait_for_timeout(3000)

        # 找 User Token 选项
        user_token = page.locator(
            'a:has-text("User Token"), a:has-text("用戶權杖"), '
            'a:has-text("用户访问口令"), [role="menuitem"]:has-text("User")'
        ).first
        if await user_token.count() > 0:
            await user_token.click()
            await page.wait_for_timeout(5000)

            # 查找页面上的 EAA token
            eaa = await page.evaluate('''() => {
                const inputs = document.querySelectorAll('input, textarea');
                for (const inp of inputs) {
                    if (inp.value && inp.value.startsWith('EAA')) return inp.value;
                }
                const html = document.body.innerHTML;
                const m = html.match(/EAA[A-Za-z0-9_-]{20,}/);
                return m ? m[0] : null;
            }''')

            if eaa:
                return f"Token 获取成功！\n`{eaa}`"

    return "开发者已注册，请手动在 Graph API Explorer 中获取 token"


# ════════════════════════════════════════════════════════════════
# 抓取 BM 设置（广告账户 ID、像素 ID、主页 ID）
# ════════════════════════════════════════════════════════════════

async def scrape_bm_settings(debug_port: str, bm_id: str = "") -> dict:
    """
    连接已打开的浏览器，进入 BM 设置页面抓取三个 ID
    返回 {"ad_account_id": "...", "pixel_id": "...", "page_id": "..."}
    """
    result = {"ad_account_id": "", "pixel_id": "", "page_id": ""}

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        try:
            # 先从当前页面 URL 提取 business_id 或 asset_id
            current_url = page.url
            if not bm_id:
                bm_id = _extract_bm_id(current_url)

            # ── 1. 抓取广告账户 ID ──
            # 尝试多个可能的 URL 路径
            ad_account_urls = [
                f"https://business.facebook.com/settings/ad-accounts?business_id={bm_id}" if bm_id else "",
                "https://business.facebook.com/latest/settings/ad-accounts",
                f"https://business.facebook.com/latest/settings/ad_accounts?business_id={bm_id}" if bm_id else "",
            ]
            for url in ad_account_urls:
                if not url:
                    continue
                try:
                    await page.goto(url, wait_until="load", timeout=30000)
                    await page.wait_for_timeout(5000)

                    # 从页面 HTML 中提取（更可靠）
                    html = await page.content()
                    # 匹配 act_XXXXX 格式
                    act_ids = re.findall(r'act_(\d{10,20})', html)
                    if act_ids:
                        result["ad_account_id"] = act_ids[0]
                        logger.info(f"抓取到广告账户 ID: {act_ids[0]}")
                        break

                    # 从页面文字匹配编号标签旁的数字
                    body = await page.inner_text("body")
                    ids = re.findall(r'(?:編號|编号|ID)[：:\s]*(\d{10,20})', body)
                    if ids:
                        result["ad_account_id"] = ids[0]
                        logger.info(f"抓取到广告账户 ID: {ids[0]}")
                        break

                    # 兜底：15-17位纯数字
                    ids = re.findall(r'\b(\d{15,17})\b', body)
                    if ids:
                        result["ad_account_id"] = ids[0]
                        logger.info(f"抓取到广告账户 ID（兜底）: {ids[0]}")
                        break
                except Exception as e:
                    logger.warning(f"抓取广告账户失败 ({url}): {e}")
                    continue

            # ── 2. 抓取像素 ID ──
            pixel_urls = [
                f"https://business.facebook.com/settings/data-sources/pixels?business_id={bm_id}" if bm_id else "",
                "https://business.facebook.com/latest/settings/data-sources/pixels",
            ]
            for url in pixel_urls:
                if not url:
                    continue
                try:
                    await page.goto(url, wait_until="load", timeout=30000)
                    await page.wait_for_timeout(5000)

                    # 点击左侧列表里「广告像素」（不是 WhatsApp 那条）
                    clicked = False
                    try:
                        # 找左侧列表里所有包含「像素」或「Pixel」的条目
                        items = page.locator('text=/广告像素|广告象素|Ad Pixel/i')
                        count = await items.count()
                        for idx in range(count):
                            item_text = await items.nth(idx).inner_text()
                            # 排除 WhatsApp 相关的条目
                            if "whatsapp" not in item_text.lower():
                                await items.nth(idx).click()
                                await page.wait_for_timeout(3000)
                                clicked = True
                                logger.info(f"点击了「{item_text.strip()[:20]}」")
                                break
                    except Exception as e:
                        logger.warning(f"点击广告像素条目失败: {e}")

                    if not clicked:
                        # 没找到可点击的，尝试直接点第一个列表项
                        try:
                            first_item = page.locator('[role="listitem"], [role="row"]').first
                            if await first_item.count() > 0:
                                await first_item.click()
                                await page.wait_for_timeout(3000)
                        except Exception:
                            pass

                    # 从右侧详情面板提取 — 找「广告像素」标题后面的编号
                    body = await page.inner_text("body")

                    # 方法 1：找右侧面板「广告像素」标题附近的编号
                    # 截图结构：「广告像素\n编号：1601725267764373」
                    m = re.search(
                        r'(?:广告像素|广告象素|Ad Pixel)\s*(?:編號|编号|ID)[：:\s]*(\d{13,20})',
                        body,
                    )
                    if m and m.group(1) != result["ad_account_id"]:
                        result["pixel_id"] = m.group(1)
                        logger.info(f"抓取到像素 ID（面板标题）: {m.group(1)}")
                        break

                    # 方法 2：HTML 里的数字链接（编号在截图里是蓝色可点击链接）
                    html = await page.content()
                    link_ids = re.findall(r'<a[^>]*>(\d{13,20})</a>', html)
                    link_ids = [i for i in link_ids if i != result["ad_account_id"]]
                    if link_ids:
                        # 如果有多个链接，优先取第一个（点击广告像素后右侧显示的）
                        result["pixel_id"] = link_ids[0]
                        logger.info(f"抓取到像素 ID（链接）: {link_ids[0]}")
                        break

                    # 方法 3：兜底 — 所有编号排除广告账户和 WhatsApp 相关
                    ids = re.findall(r'(?:編號|编号|ID)[：:\s]*(\d{13,20})', body)
                    ids = [i for i in ids if i != result["ad_account_id"]]
                    if ids:
                        result["pixel_id"] = ids[0]
                        logger.info(f"抓取到像素 ID（兜底）: {ids[0]}")
                        break
                except Exception as e:
                    logger.warning(f"抓取像素失败 ({url}): {e}")
                    continue

            # ── 3. 抓取主页 ID ──
            page_urls = [
                f"https://business.facebook.com/settings/pages?business_id={bm_id}" if bm_id else "",
                "https://business.facebook.com/latest/settings/pages",
            ]
            for url in page_urls:
                if not url:
                    continue
                try:
                    await page.goto(url, wait_until="load", timeout=30000)
                    await page.wait_for_timeout(5000)

                    html = await page.content()
                    body = await page.inner_text("body")

                    # 从编号标签提取
                    ids = re.findall(r'(?:編號|编号|ID)[：:\s]*(\d{10,20})', body)
                    existing = {result["ad_account_id"], result["pixel_id"]}
                    ids = [i for i in ids if i not in existing]
                    if ids:
                        result["page_id"] = ids[0]
                        logger.info(f"抓取到主页 ID: {ids[0]}")
                        break

                    # 兜底
                    ids = re.findall(r'\b(\d{15,17})\b', body)
                    ids = [i for i in ids if i not in existing]
                    if ids:
                        result["page_id"] = ids[0]
                        logger.info(f"抓取到主页 ID（兜底）: {ids[0]}")
                        break
                except Exception as e:
                    logger.warning(f"抓取主页失败 ({url}): {e}")
                    continue

        except Exception as e:
            logger.error(f"BM 设置抓取出错: {e}")

    return result


def _extract_bm_id(url: str) -> str:
    """从 URL 中提取 business_id 或 asset_id"""
    m = re.search(r'business_id=(\d+)', url)
    if m:
        return m.group(1)
    m = re.search(r'asset_id=(\d+)', url)
    if m:
        return m.group(1)
    return ""
