"""Web 服务：Dashboard HTTP + WebSocket 实时推送"""
import asyncio
import json
import logging
import os
import secrets
from pathlib import Path

import aiohttp
from aiohttp import web

from store.state import dashboard_tokens, chat_id_tokens

logger = logging.getLogger(__name__)

# WebSocket 连接池: chat_id -> set[WebSocketResponse]
ws_connections: dict[int, set[web.WebSocketResponse]] = {}

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# ── Token 管理 ─────────────────────────────────────────────────

def generate_token(chat_id: int) -> str:
    """为 chat_id 生成 dashboard token，旧 token 自动作废"""
    old = chat_id_tokens.get(chat_id)
    if old:
        dashboard_tokens.pop(old, None)

    token = secrets.token_urlsafe(32)
    dashboard_tokens[token] = chat_id
    chat_id_tokens[chat_id] = token
    return token


def validate_token(token: str) -> int | None:
    """验证 token，返回 chat_id 或 None"""
    return dashboard_tokens.get(token)


def revoke_token(chat_id: int):
    """撤销某 chat 的 token 并关闭所有 WS 连接"""
    old = chat_id_tokens.pop(chat_id, None)
    if old:
        dashboard_tokens.pop(old, None)
    # 关闭该 chat 的所有 WS 连接
    conns = ws_connections.pop(chat_id, set())
    for ws in conns:
        asyncio.create_task(_safe_close(ws))


async def _safe_close(ws: web.WebSocketResponse):
    try:
        await ws.close()
    except Exception:
        pass


# ── 推送 ───────────────────────────────────────────────────────

async def _instant_push(chat_id: int):
    """客户端连接后立即拉取一次数据并推送"""
    try:
        from services.monitor import run_once
        await run_once(None, chat_id)
    except Exception as e:
        logger.error(f"即时推送失败 [chat={chat_id}]: {e}")


async def push_to_dashboard(chat_id: int, payload: dict):
    """向该 chat 的所有 WS 连接推送数据"""
    conns = ws_connections.get(chat_id)
    if not conns:
        return

    data = json.dumps(payload, ensure_ascii=False)
    dead: set[web.WebSocketResponse] = set()
    for ws in conns:
        try:
            await ws.send_str(data)
        except Exception:
            dead.add(ws)
    conns -= dead


# ── HTTP 路由 ──────────────────────────────────────────────────

async def handle_dashboard(request: web.Request) -> web.Response:
    """GET /dashboard?token=xxx — 返回前端页面（token 在 WS 阶段验证）"""
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

    chat_id: int | None = None

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                if data.get("type") == "auth":
                    token = data.get("token", "")
                    chat_id = validate_token(token)
                    if chat_id:
                        ws_connections.setdefault(chat_id, set()).add(ws)
                        await ws.send_json({"type": "auth_ok", "chat_id": chat_id})
                        logger.info(f"Dashboard WS 已连接 [chat={chat_id}]")
                        # 连接后立即推送一次数据
                        asyncio.create_task(_instant_push(chat_id))
                    else:
                        await ws.send_json({"type": "auth_error", "message": "Invalid token"})
                        await ws.close()
                        return ws

                elif data.get("type") == "pong":
                    pass  # 心跳响应

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    finally:
        if chat_id:
            conns = ws_connections.get(chat_id)
            if conns:
                conns.discard(ws)
            logger.info(f"Dashboard WS 已断开 [chat={chat_id}]")

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
    logger.info(f"Dashboard 已启动: http://{host}:{port}")
    return runner
