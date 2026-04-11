"""Report Bot — Web Dashboard 服务"""
import asyncio
import json
import logging
import os
import secrets
from pathlib import Path

import aiohttp
import requests as _req
from aiohttp import web
from datetime import datetime

logger = logging.getLogger(__name__)

GRAPH = "https://graph.facebook.com/v20.0"
STATIC_DIR = Path(__file__).resolve().parent

# {dashboard_token: chat_id}
dashboard_tokens: dict[str, int] = {}
# {chat_id: dashboard_token}
chat_id_tokens: dict[int, str] = {}
# {chat_id: WebSocketResponse set}
ws_connections: dict[int, set] = {}

ACCOUNT_STATUS_MAP = {
    1: "正常", 2: "已封禁", 3: "未结算", 7: "风控审核中",
    8: "待结算", 9: "宽限期", 100: "待关闭", 101: "已关闭",
}


def get_or_create_token(chat_id: int) -> str:
    token = chat_id_tokens.get(chat_id)
    if not token:
        token = secrets.token_urlsafe(16)
        chat_id_tokens[chat_id] = token
        dashboard_tokens[token] = chat_id
    return token


# ── FB 数据拉取 ───────────────────────────────────────────────

def _fb(path: str, token: str, params: dict = None) -> dict:
    p = {"access_token": token}
    if params:
        p.update(params)
    resp = _req.get(f"{GRAPH}/{path}", params=p, timeout=30)
    return resp.json()


def _parse_action(row: dict, action_type: str) -> float:
    for item in row.get("actions", []):
        if item["action_type"] == action_type:
            return float(item.get("value", 0))
    return 0.0


def _parse_value(row: dict, action_type: str) -> float:
    for item in row.get("action_values", []):
        if item["action_type"] == action_type:
            return float(item.get("value", 0))
    return 0.0


def _extract_metrics(row: dict) -> dict:
    spend       = float(row.get("spend", 0))
    impressions = int(row.get("impressions", 0))
    clicks      = int(row.get("clicks", 0))
    cpc         = float(row.get("cpc", 0))
    ctr         = float(row.get("ctr", 0))
    reach       = int(row.get("reach", 0))
    frequency   = float(row.get("frequency", 0))

    regs      = _parse_action(row, "offsite_conversion.fb_pixel_complete_registration")
    purchases = _parse_action(row, "offsite_conversion.fb_pixel_purchase")
    revenue   = _parse_value(row,  "offsite_conversion.fb_pixel_purchase")
    subs      = _parse_action(row, "offsite_conversion.fb_pixel_subscribe")
    trials    = _parse_action(row, "offsite_conversion.fb_pixel_start_trial")

    cpm      = (spend / impressions * 1000) if impressions > 0 else 0
    reg_rate = (regs / clicks * 100)        if clicks > 0 else 0
    sub_rate = (subs / clicks * 100)        if clicks > 0 else 0
    cpa      = (spend / purchases)          if purchases > 0 else 0
    roas     = (revenue / spend * 100)      if spend > 0 else 0

    return {
        "spend":       round(spend, 2),
        "impressions": impressions,
        "reach":       reach,
        "frequency":   round(frequency, 1),
        "clicks":      clicks,
        "cpc":         round(cpc, 2),
        "ctr":         round(ctr, 2),
        "cpm":         round(cpm, 2),
        "regs":        int(regs),
        "reg_rate":    round(reg_rate, 1),
        "subs":        int(subs),
        "sub_rate":    round(sub_rate, 1),
        "trials":      int(trials),
        "purchases":   int(purchases),
        "cpa":         round(cpa, 2),
        "revenue":     round(revenue, 2),
        "roas":        round(roas, 1),
    }


def _aggregate(metrics_list: list[dict]) -> dict:
    if not metrics_list:
        return {k: 0 for k in ["spend","impressions","reach","clicks","cpc","ctr","cpm",
                                 "regs","reg_rate","subs","sub_rate","trials","purchases","cpa","revenue","roas"]}
    spend       = sum(m["spend"]       for m in metrics_list)
    impressions = sum(m["impressions"] for m in metrics_list)
    clicks      = sum(m["clicks"]      for m in metrics_list)
    regs        = sum(m["regs"]        for m in metrics_list)
    subs        = sum(m["subs"]        for m in metrics_list)
    purchases   = sum(m["purchases"]   for m in metrics_list)
    revenue     = sum(m["revenue"]     for m in metrics_list)
    trials      = sum(m["trials"]      for m in metrics_list)
    reach       = sum(m["reach"]       for m in metrics_list)
    return {
        "spend":       round(spend, 2),
        "impressions": impressions,
        "reach":       reach,
        "clicks":      clicks,
        "cpc":         round((spend / clicks) if clicks > 0 else 0, 2),
        "ctr":         round((clicks / impressions * 100) if impressions > 0 else 0, 2),
        "cpm":         round((spend / impressions * 1000) if impressions > 0 else 0, 2),
        "regs":        int(regs),
        "reg_rate":    round((regs / clicks * 100) if clicks > 0 else 0, 1),
        "subs":        int(subs),
        "sub_rate":    round((subs / clicks * 100) if clicks > 0 else 0, 1),
        "trials":      int(trials),
        "purchases":   int(purchases),
        "cpa":         round((spend / purchases) if purchases > 0 else 0, 2),
        "revenue":     round(revenue, 2),
        "roas":        round((revenue / spend * 100) if spend > 0 else 0, 1),
    }


async def collect_report(chat_id: int, monitor_data: dict, date_preset: str = "today") -> dict:
    """在线程池中同步拉取所有 BM 下广告账户的完整报表数据"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _collect_report_sync, chat_id, monitor_data, date_preset)


def _collect_report_sync(chat_id: int, monitor_data: dict, date_preset: str) -> dict:
    info = monitor_data.get(chat_id, {})
    token = info.get("token", "")
    accounts_meta = info.get("accounts", [])

    now = datetime.now().isoformat()
    accounts_data = []

    for acc_meta in accounts_meta:
        acc_id = acc_meta.get("account_id", "")
        act_id = f"act_{acc_id}"

        # 1. 账户详情（余额、状态）
        try:
            acct_info = _fb(act_id, token, {
                "fields": "name,account_status,balance,amount_spent,spend_cap,currency,disable_reason"
            })
            name        = acct_info.get("name", acc_meta.get("name", act_id))
            acct_status = acct_info.get("account_status", 1)
            balance     = float(acct_info.get("balance", 0)) / 100
            amount_spent = float(acct_info.get("amount_spent", 0)) / 100
            spend_cap   = float(acct_info.get("spend_cap", 0)) / 100
            currency    = acct_info.get("currency", "USD")
        except Exception:
            name, acct_status, balance, amount_spent, spend_cap, currency = \
                acc_meta.get("name", act_id), -1, 0, 0, 0, "USD"

        # 2. 广告组级别 insights（按系列分组）
        try:
            fields = ",".join([
                "campaign_id", "campaign_name",
                "adset_id", "adset_name",
                "spend", "impressions", "reach", "frequency",
                "clicks", "cpc", "ctr",
                "actions", "action_values",
            ])
            resp = _fb(f"{act_id}/insights", token, {
                "fields": fields,
                "level": "adset",
                "date_preset": date_preset,
                "limit": "500",
            })
            rows = resp.get("data", [])
        except Exception:
            rows = []

        # 3. 系列列表（获取状态、预算）
        try:
            camp_list_data = _fb(f"{act_id}/campaigns", token, {
                "fields": "id,name,status,effective_status,daily_budget,lifetime_budget",
                "effective_status": json.dumps(["ACTIVE", "PAUSED", "PENDING_REVIEW",
                                                 "DISAPPROVED", "IN_PROCESS"]),
                "limit": "200",
            })
            camp_info = {c["id"]: c for c in camp_list_data.get("data", [])}
        except Exception:
            camp_info = {}

        # 4. 按 campaign_id 分组广告组 insights
        camp_adsets: dict[str, list] = {}
        for row in rows:
            cid = row.get("campaign_id", "UNKNOWN")
            camp_adsets.setdefault(cid, []).append(row)

        campaigns = []
        for cid, adset_rows in camp_adsets.items():
            meta = camp_info.get(cid, {})
            adsets = []
            adset_metrics = []
            for row in adset_rows:
                m = _extract_metrics(row)
                adset_metrics.append(m)
                adsets.append({
                    "adset_id":   row.get("adset_id", ""),
                    "adset_name": row.get("adset_name", "?"),
                    "metrics":    m,
                })
            adsets.sort(key=lambda a: a["metrics"]["spend"], reverse=True)
            totals = _aggregate(adset_metrics)
            campaigns.append({
                "campaign_id":   cid,
                "campaign_name": adset_rows[0].get("campaign_name", cid),
                "status":        meta.get("effective_status", meta.get("status", "UNKNOWN")),
                "daily_budget":  int(meta.get("daily_budget", 0)) / 100,
                "totals":        totals,
                "adsets":        adsets,
            })
        campaigns.sort(key=lambda c: c["totals"]["spend"], reverse=True)

        accounts_data.append({
            "account_id":   act_id,
            "name":         name,
            "status":       acct_status,
            "status_text":  ACCOUNT_STATUS_MAP.get(acct_status, "未知"),
            "balance":      round(balance, 2),
            "amount_spent": round(amount_spent, 2),
            "spend_cap":    round(spend_cap, 2),
            "currency":     currency,
            "bm_id":        acc_meta.get("_bm_id", ""),
            "campaigns":    campaigns,
        })

    return {
        "type":        "report_update",
        "timestamp":   now,
        "date_preset": date_preset,
        "accounts":    accounts_data,
    }


# ── HTTP 路由 ─────────────────────────────────────────────────

async def handle_report_page(request: web.Request) -> web.Response:
    key = request.query.get("key", "")
    if not key or key not in dashboard_tokens:
        return web.Response(text="Access denied. Use the link from /report.", status=403)
    html_path = STATIC_DIR / "report.html"
    if not html_path.exists():
        return web.Response(text="report.html not found", status=404)
    return web.Response(text=html_path.read_text(encoding="utf-8"), content_type="text/html")


async def handle_report_ws(request: web.Request) -> web.WebSocketResponse:
    """WebSocket：认证后按需拉取报表数据"""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    authed_chat_id: int | None = None
    # monitor_data 由 main.py 注入
    monitor_data: dict = request.app["monitor_data"]

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                if data.get("type") == "auth":
                    key = data.get("key", "")
                    chat_id = dashboard_tokens.get(key)
                    if chat_id is not None:
                        authed_chat_id = chat_id
                        if chat_id not in ws_connections:
                            ws_connections[chat_id] = set()
                        ws_connections[chat_id].add(ws)
                        await ws.send_json({"type": "auth_ok"})
                        logger.info(f"Report WS 已连接 [chat={chat_id}]")
                    else:
                        await ws.send_json({"type": "auth_error", "message": "Invalid key"})
                        await ws.close()
                        return ws

                elif data.get("type") == "fetch_report" and authed_chat_id is not None:
                    date_preset = data.get("date_preset", "today")
                    if date_preset not in ("today", "yesterday", "last_7d", "last_30d"):
                        date_preset = "today"
                    await ws.send_json({"type": "loading"})
                    try:
                        payload = await collect_report(authed_chat_id, monitor_data, date_preset)
                        await ws.send_str(json.dumps(payload, ensure_ascii=False))
                    except Exception as e:
                        logger.error(f"报表拉取失败: {e}")
                        await ws.send_json({"type": "error", "message": str(e)})

                elif data.get("type") == "pong":
                    pass

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    finally:
        if authed_chat_id is not None:
            ws_set = ws_connections.get(authed_chat_id)
            if ws_set:
                ws_set.discard(ws)
            logger.info(f"Report WS 已断开 [chat={authed_chat_id}]")

    return ws


def create_web_app(monitor_data: dict) -> web.Application:
    app = web.Application()
    app["monitor_data"] = monitor_data
    app.router.add_get("/report", handle_report_page)
    app.router.add_get("/report-ws", handle_report_ws)
    return app


async def start_web_server(monitor_data: dict, host: str = "0.0.0.0", port: int = 8081):
    app = create_web_app(monitor_data)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Report Dashboard 已启动: http://{host}:{port}/report")
    return runner
