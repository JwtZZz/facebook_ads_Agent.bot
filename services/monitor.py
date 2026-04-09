"""自动监控服务 — 实时数据推送到 Dashboard + 按规则关停效果差的广告组"""
import asyncio
import logging
from datetime import datetime
from fb import FBClient
from fb.insights import parse_action, parse_action_value
from store.state import monitor_chats

logger = logging.getLogger(__name__)

# 转化率关停规则：(样本指标, 样本门槛, 转化指标, 转化率阈值%, 说明)
RULES = [
    ("impressions", 1000, "clicks",  0.5, "展示≥1000 CTR<0.5%，素材不行"),
    ("clicks",       50,  "subs",    5.0, "点击≥50 订阅率<5%，落地页或定向有问题"),
]

INTERVAL_SECONDS = 600  # 10 分钟


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
                zero_metrics = {k: 0 for k in ["spend","impressions","clicks","ctr","cpm","cpc","subs","sub_cost","sub_rate","trials","trial_cost"]}
                fb_status = a.get("effective_status", a.get("status", "UNKNOWN"))
                # 如果系列本身不是 ACTIVE，用系列状态覆盖（准备中/审核中等）
                if camp_status != "ACTIVE":
                    fb_status = camp_status
                adsets.append({
                    "adset_id": a.get("id", ""),
                    "adset_name": a.get("name", "?"),
                    "status": fb_status,
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

        # 检查关停规则
        if m["spend"] > 0:
            for sample_key, sample_min, conv_key, rate_min, label in RULES:
                sample_val = m[sample_key]
                conv_val = m[conv_key]
                if sample_val >= sample_min:
                    rate = (conv_val / sample_val * 100) if sample_val > 0 else 0
                    if rate < rate_min:
                        try:
                            fb.set_adset_status(adset_id, "PAUSED")
                            status = "auto_paused"
                            pause_events.append({
                                "adset_id": adset_id,
                                "adset_name": adset_name,
                                "reason": label,
                                "actual_rate": round(rate, 2),
                            })
                        except Exception as e:
                            logger.error(f"关停失败 {adset_id}: {e}")
                        break

        adsets.append({
            "adset_id": adset_id,
            "adset_name": adset_name,
            "status": status,
            "metrics": {
                "spend": round(m["spend"], 2),
                "impressions": m["impressions"],
                "clicks": m["clicks"],
                "ctr": round(m["ctr"], 2),
                "cpm": round(m["cpm"], 2),
                "cpc": round(m["cpc"], 2),
                "subs": m["subs"],
                "sub_cost": round(m["sub_cost"], 2),
                "sub_rate": round(m["sub_rate"], 1),
                "trials": m["trials"],
                "trial_cost": round(m["trial_cost"], 2),
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


async def run_once(app, chat_id: int):
    """对单个 chat 跑一次监控：收集数据并推送到 Dashboard"""
    info = monitor_chats.get(chat_id, {})
    if not info.get("enabled"):
        return

    fb_config = info.get("fb_config")
    campaign_ids = info.get("campaign_ids", [])
    if not fb_config or not campaign_ids:
        return

    fb = FBClient(fb_config)
    now = datetime.now().isoformat()

    try:
        # 先批量获取系列名称和状态
        campaign_names = {}
        campaign_statuses = {}
        for cid in campaign_ids:
            try:
                info_data = fb._req("GET", cid, params={"fields": "name,effective_status"})
                campaign_names[cid] = info_data.get("name", cid)
                campaign_statuses[cid] = info_data.get("effective_status", "ACTIVE")
            except Exception:
                campaign_names[cid] = cid
                campaign_statuses[cid] = "ACTIVE"

        campaigns = []
        for cid in campaign_ids:
            rows = fb.get_insights(cid, level="adset", date_preset="today")
            camp_status = campaign_statuses.get(cid, "ACTIVE")
            data = _collect_campaign_data(cid, rows, fb, camp_status)
            data["campaign_name"] = campaign_names.get(cid, data["campaign_name"])
            campaigns.append(data)

        payload = {
            "type": "monitor_update",
            "timestamp": now,
            "campaigns": campaigns,
        }

        from services.web import push_to_dashboard
        await push_to_dashboard(chat_id, payload)

    except Exception as e:
        logger.error(f"监控出错 [chat={chat_id}]: {e}")


async def monitor_loop(app, interval_seconds: int = INTERVAL_SECONDS):
    """后台循环，每 interval_seconds 秒检查一次所有开启监控的 chat"""
    while True:
        await asyncio.sleep(interval_seconds)
        for chat_id, info in list(monitor_chats.items()):
            if isinstance(info, dict) and info.get("enabled"):
                await run_once(app, chat_id)
