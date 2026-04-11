"""高层广告操作 — 广告创建、发布等业务逻辑"""
import logging
from fb import FBClient

logger = logging.getLogger(__name__)


def normal_flow(fb: FBClient, camp_name: str, count: int,
                daily_budget_usd: float = 20.0,
                country: str = "",
                device_os: str = "Android",
                age_min: int = 18,
                age_max: int = 65,
                gender: int = 0,
                conversion_event: str = "SUBSCRIBE") -> tuple[str, list[str]]:
    """
    正常跑法：创建 1 个系列（CBO 广告系列预算）+ N 个广告组
    daily_budget_usd: 广告系列级别的单日预算
    gender: 0=全部, 1=男, 2=女
    返回 (campaign_id, [adset_id, ...])
    """
    camp_id = fb.create_campaign(
        camp_name,
        daily_budget_usd=daily_budget_usd,
        use_campaign_budget=True,
        objective="OUTCOME_SALES",
    )
    logger.info(f"正常跑法 — 系列已创建: {camp_id} (${daily_budget_usd}/天)")

    adset_ids = []
    for i in range(1, count + 1):
        adset_id = fb.create_adset(
            campaign_id=camp_id,
            name=f"{camp_name}-{i:02d}",
            daily_budget_usd=0,  # CBO 模式下广告组不设预算
            optimization="OFFSITE_CONVERSIONS",
            conversion_event=conversion_event,
            mode="转化",
            country=country or None,
            device_os=device_os,
            age_min=age_min,
            age_max=age_max,
            gender=gender,
        )
        adset_ids.append(adset_id)
        logger.info(f"  广告组 {i}/{count}: {adset_id}")

    return camp_id, adset_ids


def bind_and_publish(fb: FBClient,
                     adset_ids: list[str],
                     landing_url: str,
                     message: str,
                     title: str,
                     camp_id: str = None,
                     video_id: str = "",
                     image_hash: str = "",
                     cta: str = "SUBSCRIBE") -> str:
    """
    为所有广告组创建广告创意 + 广告，然后全部启动
    支持视频或图片素材
    返回 creative_id
    """
    media_key = video_id or image_hash
    if image_hash:
        creative_id = fb.create_image_creative(
            name=f"creative-{media_key[:8]}",
            image_hash=image_hash,
            landing_url=landing_url,
            message=message,
            title=title,
            cta=cta,
        )
    else:
        creative_id = fb.create_video_creative(
            name=f"creative-{media_key[:8]}",
            video_id=video_id,
            landing_url=landing_url,
            message=message,
            title=title,
            cta=cta,
        )

    ad_ids = []
    for adset_id in adset_ids:
        ad_id = fb.create_ad(
            adset_id=adset_id,
            creative_id=creative_id,
            name=f"ad-{adset_id[-6:]}",
        )
        ad_ids.append(ad_id)

    fb.activate_all(adset_ids)
    fb.activate_all(ad_ids)
    if camp_id:
        fb.set_campaign_status(camp_id, "ACTIVE")

    return creative_id


def bind_and_publish_multi_ads(
    fb: FBClient,
    adset_id: str,
    landing_url: str,
    camp_id: str,
    ad_list: list[dict],
    cta: str = "SUBSCRIBE",
) -> list[str]:
    """
    在单个广告组里创建多条广告，每条广告有独立的文案和素材。
    ad_list: [{"media": {"type": "video"/"image", "id": str, "hash": str}, "text": str, "title": str}, ...]
    返回已创建的 ad_id 列表
    """
    ad_ids = []
    for i, ad_info in enumerate(ad_list):
        media = ad_info.get("media", {})
        text = ad_info.get("text", "")
        title = ad_info.get("title", "")
        media_type = media.get("type", "video")
        media_id = media.get("id", "")
        image_hash = media.get("hash", "")

        creative_name = f"creative-ad{i+1}-{media_id[:8] if media_id else image_hash[:8]}"

        if media_type == "image" or image_hash:
            creative_id = fb.create_image_creative(
                name=creative_name,
                image_hash=image_hash,
                landing_url=landing_url,
                message=text,
                title=title,
                cta=cta,
            )
        else:
            creative_id = fb.create_video_creative(
                name=creative_name,
                video_id=media_id,
                landing_url=landing_url,
                message=text,
                title=title,
                cta=cta,
            )

        ad_id = fb.create_ad(
            adset_id=adset_id,
            creative_id=creative_id,
            name=f"ad-{i+1}-{adset_id[-6:]}",
        )
        ad_ids.append(ad_id)

    fb.activate_all([adset_id])
    fb.activate_all(ad_ids)
    if camp_id:
        fb.set_campaign_status(camp_id, "ACTIVE")

    return ad_ids
