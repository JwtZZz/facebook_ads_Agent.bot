"""Web 服务：Dashboard HTTP + WebSocket 实时推送 + 素材上传"""
import asyncio
import json
import logging
import os
import secrets
import tempfile
from pathlib import Path

import aiohttp
from aiohttp import web

from store.state import monitor_chats, custom_rules

logger = logging.getLogger(__name__)

# 固定密钥（从 .env 读取）
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "admin")

# WebSocket 连接池: set[WebSocketResponse]（全局，不分 chat）
ws_connections: set[web.WebSocketResponse] = set()

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# ── 推送 ───────────────────────────────────────────────────────

async def push_to_dashboard(chat_id: int, payload: dict):
    """向所有 WS 连接推送数据"""
    global ws_connections
    if not ws_connections:
        return

    data = json.dumps(payload, ensure_ascii=False)
    dead: set[web.WebSocketResponse] = set()
    for ws in ws_connections:
        try:
            await ws.send_str(data)
        except Exception:
            dead.add(ws)
    ws_connections -= dead


async def push_all_data():
    """拉取所有监控中的系列数据并推送到面板"""
    global ws_connections
    if not ws_connections:
        return
    try:
        from services.monitor import collect_all_campaigns
        payload = await collect_all_campaigns()
        if payload:
            data = json.dumps(payload, ensure_ascii=False)
            dead = set()
            for ws in ws_connections:
                try:
                    await ws.send_str(data)
                except Exception:
                    dead.add(ws)
            ws_connections -= dead
    except Exception as e:
        logger.error(f"推送数据失败: {e}")


# ── HTTP 路由 ──────────────────────────────────────────────────

async def handle_dashboard(request: web.Request) -> web.Response:
    """GET /dashboard — 返回前端页面"""
    # 验证密钥
    secret = request.query.get("key", "")
    if secret != DASHBOARD_SECRET:
        return web.Response(text="Access denied. URL format: /dashboard?key=your_secret", status=403)

    html_path = STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        return web.Response(text="Dashboard not found", status=404)

    return web.Response(
        text=html_path.read_text(encoding="utf-8"),
        content_type="text/html",
    )


async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    """GET /ws — WebSocket 升级"""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    authed = False

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                if data.get("type") == "auth":
                    key = data.get("key", "")
                    if key == DASHBOARD_SECRET:
                        authed = True
                        ws_connections.add(ws)
                        await ws.send_json({"type": "auth_ok"})
                        logger.info("Dashboard WS 已连接")
                        # 连接后立即推送一次数据
                        asyncio.create_task(push_all_data())
                    else:
                        await ws.send_json({"type": "auth_error", "message": "Invalid key"})
                        await ws.close()
                        return ws

                elif data.get("type") == "save_rules":
                    # 保存或删除自定义关停规则
                    campaign_id = data.get("campaign_id", "")
                    rules = data.get("rules", [])
                    if campaign_id:
                        if rules:
                            custom_rules[campaign_id] = rules
                            logger.info(f"自定义规则已保存 [campaign={campaign_id}]")
                        else:
                            custom_rules.pop(campaign_id, None)
                            logger.info(f"自定义规则已删除 [campaign={campaign_id}]")
                        await ws.send_json({"type": "rules_saved", "campaign_id": campaign_id})

                elif data.get("type") == "pong":
                    pass

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    finally:
        ws_connections.discard(ws)
        if authed:
            logger.info("Dashboard WS 已断开")

    return ws


# ── 素材上传任务 ──────────────────────────────────────────────

# 上传任务: {task_id: {chat_id, campaign_id, adset_ids, landing_url, cta, count, fb_config, slots: [...]}}
upload_tasks: dict[str, dict] = {}


def create_upload_task(chat_id: int, campaign_id: str, adset_ids: list,
                       landing_url: str, cta: str, count: int,
                       fb_config, flow_mode: str) -> str:
    task_id = secrets.token_urlsafe(16)
    upload_tasks[task_id] = {
        "chat_id": chat_id,
        "campaign_id": campaign_id,
        "adset_ids": adset_ids,
        "landing_url": landing_url,
        "cta": cta,
        "count": count,
        "fb_config": fb_config,
        "flow_mode": flow_mode,
        "slots": [None] * count,  # [{media_path, media_type, text, title}, ...]
        "published": False,
    }
    return task_id


async def handle_upload_page(request: web.Request) -> web.Response:
    """GET /upload?task=xxx — 素材上传页面"""
    task_id = request.query.get("task", "")
    if not task_id or task_id not in upload_tasks:
        return web.Response(text="Invalid or expired task link", status=403)
    html_path = STATIC_DIR / "upload.html"
    if not html_path.exists():
        return web.Response(text="upload.html not found", status=404)
    return web.Response(text=html_path.read_text(encoding="utf-8"), content_type="text/html")


async def handle_upload_info(request: web.Request) -> web.Response:
    """GET /upload/info?task=xxx — 获取任务信息"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    return web.json_response({
        "count": task["count"],
        "flow_mode": task["flow_mode"],
        "campaign_id": task["campaign_id"],
        "landing_url": task["landing_url"],
        "slots": [
            {"filled": s is not None, "media_type": (s or {}).get("media_type", ""),
             "text": (s or {}).get("text", ""), "title": (s or {}).get("title", "")}
            for s in task["slots"]
        ],
        "published": task["published"],
    })


async def handle_upload_file(request: web.Request) -> web.Response:
    """POST /upload/file?task=xxx&slot=0 — 上传单个素材文件"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task["published"]:
        return web.json_response({"error": "Already published"}, status=400)

    slot_idx = int(request.query.get("slot", "0"))
    if slot_idx < 0 or slot_idx >= task["count"]:
        return web.json_response({"error": "Invalid slot"}, status=400)

    reader = await request.multipart()
    field = await reader.next()
    if not field:
        return web.json_response({"error": "No file"}, status=400)

    filename = field.filename or "media"
    is_image = any(filename.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"])
    suffix = ".jpg" if is_image else ".mp4"

    # 保存到临时文件
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    size = 0
    while True:
        chunk = await field.read_chunk()
        if not chunk:
            break
        size += len(chunk)
        tmp.write(chunk)
    tmp.close()

    # 上传到 FB（在线程池中执行，不阻塞事件循环）
    from fb import FBClient
    fb = FBClient(task["fb_config"])
    loop = asyncio.get_event_loop()
    try:
        if is_image:
            media_hash = await loop.run_in_executor(None, fb.upload_image, tmp.name)
            media_info = {"media_type": "image", "media_id": "", "media_hash": media_hash, "media_path": tmp.name}
        else:
            video_id = await loop.run_in_executor(None, fb.upload_video, tmp.name, filename)
            media_info = {"media_type": "video", "media_id": video_id, "media_hash": "", "media_path": tmp.name}
    except Exception as e:
        Path(tmp.name).unlink(missing_ok=True)
        return web.json_response({"error": str(e)}, status=500)

    # 保留已有的 text/title
    old = task["slots"][slot_idx]
    media_info["text"] = (old or {}).get("text", "")
    media_info["title"] = (old or {}).get("title", "")
    task["slots"][slot_idx] = media_info

    return web.json_response({"ok": True, "slot": slot_idx, "media_type": media_info["media_type"]})


async def handle_upload_clear(request: web.Request) -> web.Response:
    """POST /upload/clear?task=xxx&slot=N — 取消某个占位符的素材（删除临时文件，重置 slot）"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task["published"]:
        return web.json_response({"error": "Already published"}, status=400)

    slot_idx = int(request.query.get("slot", "0"))
    if slot_idx < 0 or slot_idx >= task["count"]:
        return web.json_response({"error": "Invalid slot"}, status=400)

    old = task["slots"][slot_idx]
    if old:
        p = old.get("media_path")
        if p:
            Path(p).unlink(missing_ok=True)
        task["slots"][slot_idx] = None

    return web.json_response({"ok": True, "slot": slot_idx})


async def handle_upload_text(request: web.Request) -> web.Response:
    """POST /upload/text?task=xxx — 保存文案和标题"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)

    body = await request.json()
    slots_data = body.get("slots", [])
    for item in slots_data:
        idx = item.get("slot", 0)
        if 0 <= idx < task["count"] and task["slots"][idx]:
            task["slots"][idx]["text"] = item.get("text", "")
            task["slots"][idx]["title"] = item.get("title", "")

    return web.json_response({"ok": True})


async def handle_upload_publish(request: web.Request) -> web.Response:
    """POST /upload/publish?task=xxx — 一键发布所有广告"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task["published"]:
        return web.json_response({"error": "Already published"}, status=400)

    # 检查所有 slot 是否填满
    filled = [s for s in task["slots"] if s is not None]
    if not filled:
        return web.json_response({"error": "No media uploaded"}, status=400)

    from fb import FBClient
    from services.campaign import bind_and_publish
    fb = FBClient(task["fb_config"])

    results = []
    try:
        if task["flow_mode"] == "multi_ad":
            # 单组多广告：一个广告组下创建多条广告
            adset_id = task["adset_ids"][0]
            for i, slot in enumerate(task["slots"]):
                if not slot:
                    continue
                text = slot.get("text", "")
                title = slot.get("title", "")
                video_id = slot.get("media_id", "")
                image_hash = slot.get("media_hash", "")

                if image_hash:
                    creative_id = fb.create_image_creative(
                        name=f"creative-{i}", image_hash=image_hash,
                        landing_url=task["landing_url"], message=text,
                        title=title, cta=task["cta"])
                else:
                    creative_id = fb.create_video_creative(
                        name=f"creative-{i}", video_id=video_id,
                        landing_url=task["landing_url"], message=text,
                        title=title, cta=task["cta"])

                ad_id = fb.create_ad(adset_id=adset_id, creative_id=creative_id, name=f"ad-{i}")
                fb.set_ad_status(ad_id, "ACTIVE")
                results.append({"slot": i, "ad_id": ad_id, "creative_id": creative_id})

            # 激活广告组和系列
            fb.set_adset_status(adset_id, "ACTIVE")
            fb.set_campaign_status(task["campaign_id"], "ACTIVE")
        else:
            # 多广告组：每个广告组绑一个素材
            for i, adset_id in enumerate(task["adset_ids"]):
                slot = task["slots"][i] if i < len(task["slots"]) else task["slots"][0]
                if not slot:
                    slot = filled[0]  # fallback 用第一个有素材的

                text = slot.get("text", "")
                title = slot.get("title", "")
                video_id = slot.get("media_id", "")
                image_hash = slot.get("media_hash", "")

                if image_hash:
                    creative_id = fb.create_image_creative(
                        name=f"creative-{i}", image_hash=image_hash,
                        landing_url=task["landing_url"], message=text,
                        title=title, cta=task["cta"])
                else:
                    creative_id = fb.create_video_creative(
                        name=f"creative-{i}", video_id=video_id,
                        landing_url=task["landing_url"], message=text,
                        title=title, cta=task["cta"])

                ad_id = fb.create_ad(adset_id=adset_id, creative_id=creative_id, name=f"ad-{i}")
                fb.set_ad_status(ad_id, "ACTIVE")
                fb.set_adset_status(adset_id, "ACTIVE")
                results.append({"slot": i, "adset_id": adset_id, "ad_id": ad_id})

            fb.set_campaign_status(task["campaign_id"], "ACTIVE")

        task["published"] = True
        return web.json_response({"ok": True, "results": results})

    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


# ── App 工厂 ───────────────────────────────────────────────────

def create_web_app() -> web.Application:
    app = web.Application(client_max_size=100 * 1024 * 1024)  # 100MB max upload
    app.router.add_get("/dashboard", handle_dashboard)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/upload", handle_upload_page)
    app.router.add_get("/upload/info", handle_upload_info)
    app.router.add_post("/upload/file", handle_upload_file)
    app.router.add_post("/upload/clear", handle_upload_clear)
    app.router.add_post("/upload/text", handle_upload_text)
    app.router.add_post("/upload/publish", handle_upload_publish)
    return app


async def start_web_server(
    app: web.Application,
    host: str = "0.0.0.0",
    port: int = 8080,
) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Dashboard 已启动: http://{host}:{port}/dashboard?key={DASHBOARD_SECRET}")
    return runner
