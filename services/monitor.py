"""自动监控服务 — 按规则关停效果差的广告组"""
import asyncio
import logging
from fb import FBClient
from fb.insights import parse_action
from store.state import monitor_chats, get_fb

logger = logging.getLogger(__name__)

# 监控规则：(最低消耗阈值USD, 检查指标名, 友好说明)
RULES = [
    (3.0,  "clicks",    "无点击"),
    (5.0,  "regs",      "无注册"),
    (9.0,  "purchases", "无首充"),
]


def _extract_metrics(row: dict) -> dict:
    return {
        "spend":     float(row.get("spend", 0)),
        "clicks":    int(row.get("clicks", 0)),
        "regs":      parse_action(row, "offsite_conversion.fb_pixel_complete_registration"),
        "purchases": parse_action(row, "offsite_conversion.fb_pixel_purchase"),
    }


async def run_once(app, chat_id: int):
    """对单个 chat 跑一次监控检查"""
    fb = get_fb(chat_id)
    if not fb:
        return

    import os
    from fb import FBConfig
    cfg = fb.cfg

    try:
        rows = fb.get_insights(cfg.account, level="adset", date_preset="today")
        paused = []

        for row in rows:
            adset_id = row.get("adset_id")
            if not adset_id:
                continue

            m = _extract_metrics(row)
            for threshold, metric, label in RULES:
                if m["spend"] >= threshold and m[metric] == 0:
                    fb.set_adset_status(adset_id, "PAUSED")
                    paused.append((
                        row.get("adset_name", adset_id),
                        f"消耗 ${m['spend']:.2f} {label}",
                    ))
                    break  # 命中第一条规则就跳过后续

        if paused:
            lines = ["🤖 *自动监控 — 已暂停广告组：*\n"]
            for name, reason in paused:
                lines.append(f"⏸ {name}\n  原因: {reason}")
            await app.bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"监控出错 [chat={chat_id}]: {e}")


async def monitor_loop(app, interval_seconds: int = 1800):
    """后台循环，每 interval_seconds 秒检查一次所有开启监控的 chat"""
    while True:
        await asyncio.sleep(interval_seconds)
        for chat_id, enabled in list(monitor_chats.items()):
            if enabled:
                await run_once(app, chat_id)
