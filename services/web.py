"""Web 服务：Dashboard HTTP + WebSocket 实时推送"""
import asyncio
import json
import logging
import os
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


# ── App 工厂 ───────────────────────────────────────────────────

def create_web_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/dashboard", handle_dashboard)
    app.router.add_get("/ws", handle_ws)
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
