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

# 上传任务数据结构（v2，多账户 fanout 模型）:
# {
#   task_id: {
#     "chat_id": int,
#     "token": str,              # 所有 targets 共用的 token
#     "count": int,              # 每账户广告数 N
#     "targets": [
#       {
#         "account_id": str,         # 不含 act_
#         "account_name": str,
#         "account_alias": str,      # 简短别名（系列命名后缀用）
#         "page_id": str,
#         "pixel_id": str,
#         "campaign_id": str,        # 创建后回填
#         "adset_id": str,
#         "landing_url": str,
#         "cta": str,
#         "event": str,
#         "title": str,              # 上传页填
#         "text": str,
#         "status": str,             # pending/ready/publishing/done/failed
#         "error": str,
#         "ad_ids": list[str],
#       },
#       ...
#     ],
#     "slots": [                   # N 行 × M 账户
#       {                          # 每行一个 dict，key=account_id
#         "<account_id>": {
#           "status": str,         # idle/queued/uploading/done/failed
#           "media_type": str,     # video/image
#           "media_id": str,       # 视频 id
#           "media_hash": str,     # 图片 hash
#           "media_path": str,     # 本地临时路径
#           "filename": str,
#           "error": str,
#         },
#         ...
#       },
#       ...
#     ],
#     "published": bool,
#     "results": list[dict],        # 发布结果汇总
#   }
# }
upload_tasks: dict[str, dict] = {}


def _empty_slot_row(account_ids: list[str]) -> dict:
    """初始化一行（所有账户的 cell 都是 idle）"""
    return {
        aid: {
            "status": "idle",
            "media_type": "",
            "media_id": "",
            "media_hash": "",
            "media_path": "",
            "filename": "",
            "error": "",
        }
        for aid in account_ids
    }


def create_multi_upload_task(
    chat_id: int,
    token: str,
    targets: list[dict],
    count: int = 10,
    base_name: str = "",
) -> str:
    """创建 v2 多账户上传任务（延迟绑定，配置在网页填）

    最少只需要: token + targets (账户元数据 + page_id + pixel_id)。
    campaign_id / adset_id / event / url / 定向参数 等在网页配置后再创建。
    """
    task_id = secrets.token_urlsafe(16)

    # 补全 target 默认字段
    for t in targets:
        t.setdefault("campaign_id", "")
        t.setdefault("adset_id", "")
        t.setdefault("landing_url", "")
        t.setdefault("event", "SUBSCRIBE")
        t.setdefault("cta", "SUBSCRIBE")
        t.setdefault("country", "BR")
        t.setdefault("device", "Android")
        t.setdefault("gender", 0)
        t.setdefault("age_min", 18)
        t.setdefault("age_max", 45)
        t.setdefault("budget", 20.0)
        t.setdefault("title", "")
        t.setdefault("text", "")
        t.setdefault("status", "pending")
        t.setdefault("error", "")
        t.setdefault("ad_ids", [])

    account_ids = [t["account_id"] for t in targets]
    upload_tasks[task_id] = {
        "chat_id": chat_id,
        "token": token,
        "count": count,
        "base_name": base_name,
        "targets": targets,
        "slots": [_empty_slot_row(account_ids) for _ in range(count)],
        "published": False,
        "results": [],
    }
    return task_id


def create_upload_task(chat_id: int, campaign_id: str, adset_ids: list,
                       landing_url: str, cta: str, count: int,
                       fb_config, flow_mode: str) -> str:
    """兼容旧 /normal 的入口：内部转成 v2 单 target 任务

    旧 /normal 可能传:
    - multi_ad: 1 campaign + 1 adset + N 广告 → 直接映射为 1 个 target
    - multi_adset: 1 campaign + N adset + 1 广告 → v2 不原生支持，按 target 拆成 N 个？
      (暂不支持，保留旧行为外挂)
    """
    if flow_mode == "multi_ad" and adset_ids:
        # multi_ad 直接映射为单 target
        acc_id = fb_config.ad_account_id.replace("act_", "")
        target = {
            "account_id": acc_id,
            "account_name": f"Ad Acct {acc_id[-6:]}",
            "account_alias": acc_id[-6:],
            "page_id": fb_config.page_id or "",
            "pixel_id": fb_config.pixel_id or "",
            "campaign_id": campaign_id,
            "adset_id": adset_ids[0],
            "landing_url": landing_url,
            "cta": cta,
            "event": "SUBSCRIBE",
        }
        return create_multi_upload_task(
            chat_id=chat_id,
            token=fb_config.access_token,
            targets=[target],
            count=count,
        )

    # 兼容 multi_adset 老流程（保留旧 v1 结构）
    task_id = secrets.token_urlsafe(16)
    upload_tasks[task_id] = {
        "legacy": True,
        "chat_id": chat_id,
        "campaign_id": campaign_id,
        "adset_ids": adset_ids,
        "landing_url": landing_url,
        "cta": cta,
        "count": count,
        "fb_config": fb_config,
        "flow_mode": flow_mode,
        "slots_v1": [None] * count,
        "published": False,
    }
    return task_id


async def handle_upload_page(request: web.Request) -> web.Response:
    """GET /upload?task=xxx — 素材上传页面

    根据 task 类型分发:
    - v2 多账户任务 → upload2.html
    - v1 legacy 任务 → upload.html
    """
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.Response(text="Invalid or expired task link", status=403)

    file_name = "upload.html" if task.get("legacy") else "upload2.html"
    html_path = STATIC_DIR / file_name
    if not html_path.exists():
        return web.Response(text=f"{file_name} not found", status=404)
    return web.Response(text=html_path.read_text(encoding="utf-8"), content_type="text/html")


async def handle_upload_info(request: web.Request) -> web.Response:
    """GET /upload/info?task=xxx — 获取任务信息（v2 多账户结构）"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)

    # 老 v1 任务走老格式（/normal 的 multi_adset 回退路径）
    if task.get("legacy"):
        return web.json_response({
            "legacy": True,
            "count": task["count"],
            "flow_mode": task["flow_mode"],
            "campaign_id": task["campaign_id"],
            "landing_url": task["landing_url"],
            "slots": [
                {"filled": s is not None, "media_type": (s or {}).get("media_type", ""),
                 "text": (s or {}).get("text", ""), "title": (s or {}).get("title", "")}
                for s in task.get("slots_v1", [])
            ],
            "published": task["published"],
        })

    # v2 多账户
    return web.json_response({
        "version": 2,
        "count": task["count"],
        "base_name": task.get("base_name", ""),
        "targets": [
            {
                "account_id":    t["account_id"],
                "account_name":  t.get("account_name", ""),
                "account_alias": t.get("account_alias", ""),
                "page_id":       t.get("page_id", ""),
                "pixel_id":      t.get("pixel_id", ""),
                "landing_url":   t.get("landing_url", ""),
                "event":         t.get("event", ""),
                "cta":           t.get("cta", ""),
                "country":       t.get("country", ""),
                "device":        t.get("device", ""),
                "gender":        t.get("gender", 0),
                "age_min":       t.get("age_min", 18),
                "age_max":       t.get("age_max", 45),
                "budget":        t.get("budget", 20.0),
                "title":         t.get("title", ""),
                "text":          t.get("text", ""),
                "campaign_id":   t.get("campaign_id", ""),
                "adset_id":      t.get("adset_id", ""),
            }
            for t in task["targets"]
        ],
        "slots": [
            {
                aid: {
                    "status":     cell["status"],
                    "media_type": cell.get("media_type", ""),
                    "filename":   cell.get("filename", ""),
                    "error":      cell.get("error", ""),
                }
                for aid, cell in row.items()
            }
            for row in task["slots"]
        ],
        "published": task["published"],
        "results": task.get("results", []),
    })


async def handle_upload_config(request: web.Request) -> web.Response:
    """POST /upload/config?task=xxx — 保存投手在网页上填的投放配置

    body 结构: {
      "count": 10,
      "base_name": "bet7-...",
      "accounts": [
        {
          "account_id": "...",
          "event": "SUBSCRIBE",
          "landing_url": "https://...",
          "country": "BR",
          "device": "Android",
          "gender": 0,
          "age_min": 18, "age_max": 45,
          "budget": 20.0,
          "title": "...",
          "text": "..."
        }
      ]
    }
    """
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task or task.get("legacy"):
        return web.json_response({"error": "Invalid task"}, status=403)
    if task.get("published"):
        return web.json_response({"error": "Already published"}, status=400)

    body = await request.json()

    # 全局字段
    if "count" in body:
        try:
            new_count = int(body["count"])
            if not (1 <= new_count <= 50):
                raise ValueError
        except (ValueError, TypeError):
            return web.json_response({"error": "count must be 1-50"}, status=400)
        if new_count != task["count"]:
            # 调整 slots 尺寸
            old_count = task["count"]
            account_ids = [t["account_id"] for t in task["targets"]]
            if new_count > old_count:
                task["slots"].extend(
                    _empty_slot_row(account_ids) for _ in range(new_count - old_count)
                )
            else:
                # 释放被截掉的 slot 对应的临时文件
                for row in task["slots"][new_count:]:
                    for cell in row.values():
                        p = cell.get("media_path")
                        if p:
                            Path(p).unlink(missing_ok=True)
                task["slots"] = task["slots"][:new_count]
            task["count"] = new_count

    if "base_name" in body:
        task["base_name"] = str(body["base_name"] or "").strip()

    # 每账户字段
    by_id = {t["account_id"]: t for t in task["targets"]}
    accounts_data = body.get("accounts", []) or []
    allowed_fields = {
        "event", "landing_url", "country", "device", "gender",
        "age_min", "age_max", "budget", "title", "text",
    }
    cta_map = {"SUBSCRIBE": "SUBSCRIBE", "PURCHASE": "SHOP_NOW"}
    for item in accounts_data:
        aid = item.get("account_id", "")
        t = by_id.get(aid)
        if not t:
            continue
        for f in allowed_fields:
            if f in item:
                t[f] = item[f]
        # 同步 cta
        ev = t.get("event", "SUBSCRIBE")
        t["cta"] = cta_map.get(ev, "SUBSCRIBE")

    return web.json_response({"ok": True})


def _build_fb_for_target(token: str, target: dict):
    """给某个 target 临时构建 FBClient"""
    from fb import FBClient, FBConfig
    return FBClient(FBConfig(
        access_token=token,
        ad_account_id=target["account_id"],
        pixel_id=target.get("pixel_id", ""),
        page_id=target.get("page_id", ""),
    ))


async def handle_upload_file(request: web.Request) -> web.Response:
    """POST /upload/file?task=xxx&slot=N&account=YYY — 上传单个 cell 的素材

    每个 cell = 一个素材位 × 一个账户。文件会实际上传到该账户的 FB 素材库。
    """
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task.get("published"):
        return web.json_response({"error": "Already published"}, status=400)
    if task.get("legacy"):
        # 老 v1 流程（不带 account 参数）→ 走原始逻辑
        return await _handle_upload_file_legacy(request, task)

    try:
        slot_idx = int(request.query.get("slot", "-1"))
    except ValueError:
        return web.json_response({"error": "Invalid slot"}, status=400)
    if not (0 <= slot_idx < task["count"]):
        return web.json_response({"error": "Invalid slot index"}, status=400)

    account_id = request.query.get("account", "")
    target = next((t for t in task["targets"] if t["account_id"] == account_id), None)
    if not target:
        return web.json_response({"error": "Invalid account"}, status=400)

    reader = await request.multipart()
    field = await reader.next()
    if not field:
        return web.json_response({"error": "No file"}, status=400)

    filename = field.filename or "media"
    is_image = any(filename.lower().endswith(ext)
                   for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"])
    suffix = ".jpg" if is_image else ".mp4"

    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    while True:
        chunk = await field.read_chunk()
        if not chunk:
            break
        tmp.write(chunk)
    tmp.close()

    # 标记该 cell 为 uploading
    row = task["slots"][slot_idx]
    row[account_id]["status"] = "uploading"
    row[account_id]["filename"] = filename
    row[account_id]["error"] = ""

    # 上传到 FB（这个 cell 对应的账户）
    fb = _build_fb_for_target(task["token"], target)
    loop = asyncio.get_event_loop()
    try:
        if is_image:
            media_hash = await loop.run_in_executor(None, fb.upload_image, tmp.name)
            row[account_id].update({
                "status": "done",
                "media_type": "image",
                "media_id": "",
                "media_hash": media_hash,
                "media_path": tmp.name,
            })
            result_type = "image"
        else:
            video_id = await loop.run_in_executor(None, fb.upload_video, tmp.name, filename)
            row[account_id].update({
                "status": "done",
                "media_type": "video",
                "media_id": video_id,
                "media_hash": "",
                "media_path": tmp.name,
            })
            result_type = "video"
    except Exception as e:
        Path(tmp.name).unlink(missing_ok=True)
        row[account_id]["status"] = "failed"
        row[account_id]["error"] = str(e)
        return web.json_response({"error": str(e)}, status=500)

    return web.json_response({
        "ok": True,
        "slot": slot_idx,
        "account": account_id,
        "media_type": result_type,
    })


async def _handle_upload_file_legacy(request, task) -> web.Response:
    """老 v1 upload_file 处理逻辑（/normal multi_adset 的 fallback）"""
    try:
        slot_idx = int(request.query.get("slot", "0"))
    except ValueError:
        return web.json_response({"error": "Invalid slot"}, status=400)
    if slot_idx < 0 or slot_idx >= task["count"]:
        return web.json_response({"error": "Invalid slot"}, status=400)

    reader = await request.multipart()
    field = await reader.next()
    if not field:
        return web.json_response({"error": "No file"}, status=400)

    filename = field.filename or "media"
    is_image = any(filename.lower().endswith(ext)
                   for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"])
    suffix = ".jpg" if is_image else ".mp4"

    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    while True:
        chunk = await field.read_chunk()
        if not chunk:
            break
        tmp.write(chunk)
    tmp.close()

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

    old = task["slots_v1"][slot_idx]
    media_info["text"] = (old or {}).get("text", "")
    media_info["title"] = (old or {}).get("title", "")
    task["slots_v1"][slot_idx] = media_info
    return web.json_response({"ok": True, "slot": slot_idx, "media_type": media_info["media_type"]})


async def handle_upload_clear(request: web.Request) -> web.Response:
    """POST /upload/clear?task=xxx&slot=N&account=YYY — 清除单个 cell（v2）

    对老 v1 任务：只接受 slot 参数，清除整个 slot。
    """
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task.get("published"):
        return web.json_response({"error": "Already published"}, status=400)

    try:
        slot_idx = int(request.query.get("slot", "0"))
    except ValueError:
        return web.json_response({"error": "Invalid slot"}, status=400)
    if slot_idx < 0 or slot_idx >= task["count"]:
        return web.json_response({"error": "Invalid slot"}, status=400)

    if task.get("legacy"):
        old = task["slots_v1"][slot_idx]
        if old:
            p = old.get("media_path")
            if p:
                Path(p).unlink(missing_ok=True)
            task["slots_v1"][slot_idx] = None
        return web.json_response({"ok": True, "slot": slot_idx})

    account_id = request.query.get("account", "")
    row = task["slots"][slot_idx]
    if account_id:
        # 清单个 cell
        cell = row.get(account_id)
        if cell:
            p = cell.get("media_path")
            if p:
                Path(p).unlink(missing_ok=True)
            cell.update({
                "status": "idle", "media_type": "", "media_id": "",
                "media_hash": "", "media_path": "", "filename": "", "error": "",
            })
        return web.json_response({"ok": True, "slot": slot_idx, "account": account_id})
    else:
        # 清整行（所有账户）
        for aid, cell in row.items():
            p = cell.get("media_path")
            if p:
                Path(p).unlink(missing_ok=True)
            cell.update({
                "status": "idle", "media_type": "", "media_id": "",
                "media_hash": "", "media_path": "", "filename": "", "error": "",
            })
        return web.json_response({"ok": True, "slot": slot_idx, "cleared": "row"})


async def handle_upload_text(request: web.Request) -> web.Response:
    """POST /upload/text?task=xxx — 保存文案和标题

    v2 body: {"accounts": [{"account_id": "...", "title": "...", "text": "..."}, ...]}
    v1 body: {"slots": [{"slot": i, "title": "...", "text": "..."}, ...]}
    """
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)

    body = await request.json()

    if task.get("legacy"):
        slots_data = body.get("slots", [])
        for item in slots_data:
            idx = item.get("slot", 0)
            if 0 <= idx < task["count"] and task["slots_v1"][idx]:
                task["slots_v1"][idx]["text"] = item.get("text", "")
                task["slots_v1"][idx]["title"] = item.get("title", "")
        return web.json_response({"ok": True})

    accounts_data = body.get("accounts", [])
    by_id = {t["account_id"]: t for t in task["targets"]}
    for item in accounts_data:
        aid = item.get("account_id", "")
        if aid in by_id:
            by_id[aid]["title"] = item.get("title", "")
            by_id[aid]["text"] = item.get("text", "")
    return web.json_response({"ok": True})


async def handle_upload_publish(request: web.Request) -> web.Response:
    """POST /upload/publish?task=xxx — 一键发布所有广告（v2 多账户 fanout）"""
    task_id = request.query.get("task", "")
    task = upload_tasks.get(task_id)
    if not task:
        return web.json_response({"error": "Invalid task"}, status=403)
    if task.get("published"):
        return web.json_response({"error": "Already published"}, status=400)

    if task.get("legacy"):
        return await _handle_upload_publish_legacy(task)

    # v2 多账户 fanout —— 先创建 campaign/adset，再 fanout 发布
    from services.campaign import (
        TargetSpec, create_targets_parallel, publish_targets_parallel
    )

    # 为每个账户收集它的 slot 列表（按 row 顺序，仅 done 状态）
    slots_by_account: dict[str, list[dict]] = {}
    for t in task["targets"]:
        aid = t["account_id"]
        acc_slots = []
        for row in task["slots"]:
            cell = row.get(aid)
            if cell and cell.get("status") == "done":
                acc_slots.append({
                    "media_type": cell.get("media_type", "video"),
                    "media_id":   cell.get("media_id", ""),
                    "media_hash": cell.get("media_hash", ""),
                })
        slots_by_account[aid] = acc_slots

    # 过滤掉完全没素材的账户
    active_targets = [
        t for t in task["targets"]
        if slots_by_account.get(t["account_id"])
    ]
    if not active_targets:
        return web.json_response(
            {"error": "No account has any uploaded media"}, status=400)

    # 校验每账户的配置是否齐全
    missing = []
    for t in active_targets:
        if not t.get("landing_url"):
            missing.append(f"{t.get('account_name', t['account_id'])}: 缺 URL")
        if not t.get("country"):
            missing.append(f"{t.get('account_name', t['account_id'])}: 缺国家")
    if missing:
        return web.json_response(
            {"error": "配置未填齐: " + "; ".join(missing)}, status=400)

    base_name = task.get("base_name", "") or "campaign"
    token = task["token"]

    # 第一步：对还没有 campaign_id 的账户并发创建 campaign + adset
    specs_to_create = []
    specs_index: list[int] = []  # 与 active_targets 对应位置
    for i, t in enumerate(active_targets):
        if t.get("campaign_id") and t.get("adset_id"):
            continue  # 已经创建过（重试发布场景）
        alias = t.get("account_alias") or t["account_id"][-6:]
        specs_to_create.append(TargetSpec(
            account_id=t["account_id"],
            account_name=t.get("account_name", ""),
            token=token,
            page_id=t.get("page_id", ""),
            pixel_id=t.get("pixel_id", ""),
            campaign_name=f"{base_name}-{alias}",
            daily_budget_usd=float(t.get("budget", 20.0)),
            country=t.get("country", "BR"),
            device_os=t.get("device", "Android"),
            age_min=int(t.get("age_min", 18)),
            age_max=int(t.get("age_max", 45)),
            gender=int(t.get("gender", 0)),
            conversion_event=t.get("event", "SUBSCRIBE"),
            landing_url=t.get("landing_url", ""),
            cta=t.get("cta", "SUBSCRIBE"),
            count=task["count"],
        ))
        specs_index.append(i)

    if specs_to_create:
        created = await create_targets_parallel(specs_to_create)
        for spec, i in zip(created, specs_index):
            t = active_targets[i]
            t["campaign_id"] = spec.campaign_id
            t["adset_id"] = spec.adset_id
            if spec.error:
                t["error"] = spec.error

    # 只对成功创建了 campaign+adset 的账户继续发布
    ready_targets = [t for t in active_targets if t.get("campaign_id") and t.get("adset_id")]
    failed_create = [t for t in active_targets if not (t.get("campaign_id") and t.get("adset_id"))]

    # 每账户的 token 注入到 target dict（publish_target 需要）
    for t in ready_targets:
        t["token"] = token

    text_by_account = {t["account_id"]: t.get("text", "") for t in ready_targets}
    title_by_account = {t["account_id"]: t.get("title", "") for t in ready_targets}

    results = await publish_targets_parallel(
        targets=ready_targets,
        slots_by_account={aid: slots_by_account[aid] for aid in (t["account_id"] for t in ready_targets)},
        text_by_account=text_by_account,
        title_by_account=title_by_account,
    )

    # 合并创建失败的账户到结果
    for t in failed_create:
        results.append({
            "account_id": t["account_id"],
            "ok": False,
            "ad_ids": [],
            "error": "创建 campaign/adset 失败: " + (t.get("error") or "unknown"),
        })

    # 回写每个 target 的状态和 ad_ids
    for r in results:
        aid = r["account_id"]
        for t in task["targets"]:
            if t["account_id"] == aid:
                t["status"] = "done" if r["ok"] else "failed"
                t["error"] = r["error"]
                t["ad_ids"] = r["ad_ids"]
                break

    task["results"] = results
    task["published"] = all(r["ok"] for r in results)

    return web.json_response({
        "ok": task["published"],
        "results": [
            {
                "account_id":   r["account_id"],
                "account_name": next((t.get("account_name", "") for t in task["targets"]
                                      if t["account_id"] == r["account_id"]), ""),
                "ok":           r["ok"],
                "ad_count":     len(r["ad_ids"]),
                "error":        r["error"],
            }
            for r in results
        ],
    })


async def _handle_upload_publish_legacy(task) -> web.Response:
    """老 v1 发布逻辑（/normal multi_adset 的 fallback）"""
    filled = [s for s in task["slots_v1"] if s is not None]
    if not filled:
        return web.json_response({"error": "No media uploaded"}, status=400)

    from fb import FBClient
    fb = FBClient(task["fb_config"])

    results = []
    try:
        if task["flow_mode"] == "multi_ad":
            adset_id = task["adset_ids"][0]
            for i, slot in enumerate(task["slots_v1"]):
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

                ad_id = fb.create_ad(
                    adset_id=adset_id, creative_id=creative_id, name=f"ad-{i}")
                fb.set_ad_status(ad_id, "ACTIVE")
                results.append({"slot": i, "ad_id": ad_id, "creative_id": creative_id})

            fb.set_adset_status(adset_id, "ACTIVE")
            fb.set_campaign_status(task["campaign_id"], "ACTIVE")
        else:
            for i, adset_id in enumerate(task["adset_ids"]):
                slot = task["slots_v1"][i] if i < len(task["slots_v1"]) else task["slots_v1"][0]
                if not slot:
                    slot = filled[0]
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

                ad_id = fb.create_ad(
                    adset_id=adset_id, creative_id=creative_id, name=f"ad-{i}")
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
    app.router.add_post("/upload/config", handle_upload_config)
    app.router.add_post("/upload/publish", handle_upload_publish)
    return app


def get_upload_task(task_id: str) -> dict | None:
    """供外部调用：通过 task_id 取任务对象"""
    return upload_tasks.get(task_id)


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
