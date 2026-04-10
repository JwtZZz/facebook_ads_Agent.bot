"""自动监控服务 — 实时数据推送到 Dashboard + 按规则关停效果差的广告组"""
import asyncio
import logging
from datetime import datetime
from fb import FBClient
from fb.insights import parse_action, parse_action_value
from store.state import monitor_chats, custom_rules

logger = logging.getLogger(__name__)

# 自动关停规则：按转化事件分类
# 格式：(消耗阈值USD, 检查指标, 最低要求数量, 说明)
RULES_BY_EVENT = {
    "SUBSCRIBE": [
        (3.0, "clicks",    0, "消耗$3无点击"),
        (5.0, "subs",      0, "消耗$5无订阅"),
    ],
    "PURCHASE": [
        (3.0, "clicks",    0, "消耗$3无点击"),
        (5.0, "regs",      0, "消耗$5无注册"),
        (9.0, "purchases", 0, "消耗$9无购买"),
    ],
    "COMPLETE_REGISTRATION": [
        (3.0, "clicks",    0, "消耗$3无点击"),
        (5.0, "regs",      0, "消耗$5无注册"),
    ],
}

INTERVAL_SECONDS = 120  # 2 分钟


def _extract_metrics(row: dict) -> dict:
    spend = float(row.get("spend", 0))
    clicks = int(row.get("clicks", 0))
    impressions = int(row.get("impressions", 0))
    reach = int(row.get("reach", 0))
    regs = parse_action(row, "offsite_conversion.fb_pixel_complete_registration")
    purchases = parse_action(row, "offsite_conversion.fb_pixel_purchase")
    revenue = parse_action_value(row, "offsite_conversion.fb_pixel_purchase")
    subs = parse_action(row, "offsite_conversion.fb_pixel_subscribe")
    trials = parse_action(row, "offsite_conversion.fb_pixel_start_trial")

    return {
        "spend":       spend,
        "clicks":      clicks,
        "impressions": impressions,
        "reach":       reach,
        "frequency":   float(row.get("frequency", 0)),
        "cpc":         float(row.get("cpc", 0)),
        "cpm":         (spend / impressions * 1000) if impressions > 0 else 0,
        "ctr":         float(row.get("ctr", 0)),
        "regs":        regs,
        "purchases":   purchases,
        "revenue":     revenue,
        "subs":        subs,
        "sub_cost":    (spend / subs) if subs > 0 else 0,
        "sub_rate":    (subs / clicks * 100) if clicks > 0 else 0,
        "trials":      trials,
        "trial_cost":  (spend / trials) if trials > 0 else 0,
        "reg_cost":    (spend / regs) if regs > 0 else 0,
        "cpa":         (spend / purchases) if purchases > 0 else 0,
        "conv_rate":   (purchases / regs * 100) if regs > 0 else 0,
        "roas":        (revenue / spend * 100) if spend > 0 else 0,
    }


def _collect_campaign_data(campaign_id: str, rows: list, fb: FBClient, camp_status: str = "ACTIVE") -> dict:
    """为单个系列收集结构化数据，同时执行关停规则"""
    campaign_name = campaign_id
    if rows:
        campaign_name = rows[0].get("campaign_name", campaign_id)

    totals = {"spend": 0, "clicks": 0, "impressions": 0, "reach": 0,
              "subs": 0, "trials": 0, "ctr": 0, "cpm": 0, "cpc": 0,
              "sub_cost": 0, "sub_rate": 0, "trial_cost": 0}
    adsets = []
    pause_events = []

    # 如果没有 insights 数据，用 list_adsets 拉广告组基本信息
    if not rows:
        try:
            raw_adsets = fb.list_adsets(campaign_id=campaign_id, status="ALL")
            for a in raw_adsets:
                zero_metrics = {k: 0 for k in ["spend","impressions","clicks","ctr","cpm","cpc","subs","sub_cost","sub_rate","trials","trial_cost","reach","frequency","regs","reg_cost","purchases","cpa","revenue","conv_rate","roas"]}
                fb_status = a.get("effective_status", a.get("status", "UNKNOWN"))
                if camp_status != "ACTIVE":
                    fb_status = camp_status
                # 从 promoted_object 获取转化事件类型
                po = a.get("promoted_object", {})
                conv_event = po.get("custom_event_type", "UNKNOWN")
                adsets.append({
                    "adset_id": a.get("id", ""),
                    "adset_name": a.get("name", "?"),
                    "status": fb_status,
                    "conversion_event": conv_event,
                    "metrics": zero_metrics,
                })
        except Exception:
            pass
        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "totals": totals,
            "adsets": adsets,
            "pause_events": [],
        }

    # 获取广告组的转化事件类型
    adset_events = {}
    try:
        all_adsets_info = fb.list_adsets(campaign_id=campaign_id, status="ALL")
        for ai in all_adsets_info:
            po = ai.get("promoted_object", {})
            adset_events[ai.get("id", "")] = po.get("custom_event_type", "UNKNOWN")
    except Exception:
        pass

    for row in rows:
        adset_id = row.get("adset_id")
        adset_name = row.get("adset_name", adset_id or "?")
        if not adset_id:
            continue

        m = _extract_metrics(row)
        totals["spend"] += m["spend"]
        totals["clicks"] += m["clicks"]
        totals["impressions"] += m["impressions"]
        totals["reach"] += m["reach"]
        totals["subs"] += m["subs"]
        totals["trials"] += m["trials"]

        status = "running"

        # 根据转化事件类型选择对应的关停规则（优先用自定义规则）
        conv_event = adset_events.get(adset_id, "UNKNOWN")
        campaign_custom = custom_rules.get(campaign_id)
        if campaign_custom:
            rules = [(r["threshold"], r["metric"], r["min_val"], r["label"]) for r in campaign_custom]
        else:
            rules = RULES_BY_EVENT.get(conv_event, RULES_BY_EVENT.get("SUBSCRIBE", []))

        # 检查关停规则
        if m["spend"] > 0:
            for threshold, metric, min_val, label in rules:
                if m["spend"] >= threshold and m[metric] <= min_val:
                    try:
                        fb.set_adset_status(adset_id, "PAUSED")
                        status = "auto_paused"
                        pause_events.append({
                            "adset_id": adset_id,
                            "adset_name": adset_name,
                            "reason": label,
                            "actual_rate": m[metric],
                        })
                    except Exception as e:
                        logger.error(f"关停失败 {adset_id}: {e}")
                    break

        adsets.append({
            "adset_id": adset_id,
            "adset_name": adset_name,
            "status": status,
            "conversion_event": adset_events.get(adset_id, "UNKNOWN"),
            "metrics": {
                "spend": round(m["spend"], 2),
                "impressions": m["impressions"],
                "reach": m["reach"],
                "frequency": round(m["frequency"], 1),
                "clicks": m["clicks"],
                "ctr": round(m["ctr"], 2),
                "cpm": round(m["cpm"], 2),
                "cpc": round(m["cpc"], 2),
                "subs": m["subs"],
                "sub_cost": round(m["sub_cost"], 2),
                "sub_rate": round(m["sub_rate"], 1),
                "trials": m["trials"],
                "trial_cost": round(m["trial_cost"], 2),
                "regs": m["regs"],
                "reg_cost": round(m["reg_cost"], 2),
                "purchases": m["purchases"],
                "cpa": round(m["cpa"], 2),
                "revenue": round(m["revenue"], 2),
                "conv_rate": round(m["conv_rate"], 1),
                "roas": round(m["roas"], 1),
            },
        })

    # 计算汇总的衍生指标
    ts = totals["spend"]
    tc = totals["clicks"]
    ti = totals["impressions"]
    totals["ctr"] = round((tc / ti * 100) if ti > 0 else 0, 2)
    totals["cpm"] = round((ts / ti * 1000) if ti > 0 else 0, 2)
    totals["cpc"] = round((ts / tc) if tc > 0 else 0, 2)
    totals["sub_cost"] = round((ts / totals["subs"]) if totals["subs"] > 0 else 0, 2)
    totals["sub_rate"] = round((totals["subs"] / tc * 100) if tc > 0 else 0, 1)
    totals["trial_cost"] = round((ts / totals["trials"]) if totals["trials"] > 0 else 0, 2)
    totals["spend"] = round(ts, 2)

    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "totals": totals,
        "adsets": adsets,
        "pause_events": pause_events,
    }


ACCOUNT_STATUS_MAP = {
    1: "正常", 2: "已封禁", 3: "未结算", 7: "风控审核中",
    8: "待结算", 9: "宽限期", 100: "待关闭", 101: "已关闭",
}


async def collect_all_campaigns() -> dict | None:
    """收集所有监控中的系列数据，返回 payload"""
    all_campaigns = []
    all_accounts = []
    seen_accounts = set()
    now = datetime.now().isoformat()

    for chat_id, info in list(monitor_chats.items()):
        if not isinstance(info, dict) or not info.get("enabled"):
            continue

        fb_config = info.get("fb_config")
        campaign_ids = info.get("campaign_ids", [])
        if not fb_config or not campaign_ids:
            continue

        fb = FBClient(fb_config)

        # 查账户状态（每个账户只查一次）
        acct_id = fb.cfg.account
        if acct_id not in seen_accounts:
            seen_accounts.add(acct_id)
            try:
                acct_info = fb._req("GET", acct_id, params={
                    "fields": "name,account_status,disable_reason",
                })
                all_accounts.append({
                    "account_id": acct_id,
                    "name": acct_info.get("name", acct_id),
                    "status": acct_info.get("account_status", 1),
                    "status_text": ACCOUNT_STATUS_MAP.get(acct_info.get("account_status", 1), "未知"),
                    "disable_reason": acct_info.get("disable_reason", 0),
                })
            except Exception:
                all_accounts.append({
                    "account_id": acct_id,
                    "name": acct_id,
                    "status": -1,
                    "status_text": "查询失败",
                    "disable_reason": 0,
                })

        try:
            for cid in campaign_ids:
                try:
                    info_data = fb._req("GET", cid, params={"fields": "name,effective_status"})
                    camp_name = info_data.get("name", cid)
                    camp_status = info_data.get("effective_status", "ACTIVE")
                except Exception:
                    camp_name = cid
                    camp_status = "ACTIVE"

                rows = fb.get_insights(cid, level="adset", date_preset="today")
                data = _collect_campaign_data(cid, rows, fb, camp_status)
                data["campaign_name"] = camp_name
                all_campaigns.append(data)
        except Exception as e:
            logger.error(f"收集数据出错 [chat={chat_id}]: {e}")

    if not all_campaigns and not all_accounts:
        return None

    return {
        "type": "monitor_update",
        "timestamp": now,
        "accounts": all_accounts,
        "campaigns": all_campaigns,
    }


async def run_once(app, chat_id: int):
    """对单个 chat 跑一次监控（兼容旧调用）"""
    payload = await collect_all_campaigns()
    if payload:
        from services.web import push_to_dashboard
        await push_to_dashboard(chat_id, payload)


async def monitor_loop(app, interval_seconds: int = INTERVAL_SECONDS):
    """后台循环，每 interval_seconds 秒拉数据推送到面板"""
    while True:
        await asyncio.sleep(interval_seconds)
        from services.web import push_all_data
        await push_all_data()
