"""高层广告操作 — 一刀流、正常跑法等业务逻辑"""
import logging
from fb import FBClient

logger = logging.getLogger(__name__)


def one_dollar_flow(fb: FBClient, camp_name: str, count: int) -> tuple[str, list[str]]:
    """
    一刀流：创建 1 个系列 + N 个 $1/天广告组
    返回 (campaign_id, [adset_id, ...])
    """
    camp_id = fb.create_campaign(camp_name, use_campaign_budget=False)
    logger.info(f"一刀流 — 系列已创建: {camp_id}")

    adset_ids = []
    for i in range(1, count + 1):
        adset_id = fb.create_adset(
            campaign_id=camp_id,
            name=f"{camp_name}-{i:02d}",
            daily_budget_usd=1.0,
        )
        adset_ids.append(adset_id)
        logger.info(f"  广告组 {i}/{count}: {adset_id}")

    return camp_id, adset_ids


def bind_and_publish(fb: FBClient,
                     adset_ids: list[str],
                     video_id: str,
                     landing_url: str,
                     message: str,
                     title: str,
                     camp_id: str = None) -> str:
    """
    为所有广告组创建广告创意 + 广告，然后全部启动
    返回 creative_id
    """
    creative_id = fb.create_video_creative(
        name=f"creative-{video_id[:8]}",
        video_id=video_id,
        landing_url=landing_url,
        message=message,
        title=title,
        cta="DOWNLOAD",
    )

    for adset_id in adset_ids:
        fb.create_ad(
            adset_id=adset_id,
            creative_id=creative_id,
            name=f"ad-{adset_id[-6:]}",
        )

    fb.activate_all(adset_ids)
    if camp_id:
        fb.set_campaign_status(camp_id, "ACTIVE")

    return creative_id
