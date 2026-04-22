"""多账户 fanout 广告创建/发布 — /go 流程"""
import asyncio
import logging
from dataclasses import dataclass
from fb import FBClient, FBConfig

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════

@dataclass
class TargetSpec:
    """描述一个账户上要建的一套 campaign/adset 的规格"""
    account_id: str           # 不含 act_ 前缀
    account_name: str         # 用于命名、显示
    token: str                # 该账户用的 token（所有 targets 通常共用）
    page_id: str              # 全局 page 或该账户自己的
    pixel_id: str             # 全局 pixel 或该账户自己的
    campaign_name: str        # 完整 campaign 名字
    daily_budget_usd: float
    country: str
    device_os: str
    age_min: int
    age_max: int
    gender: int
    conversion_event: str     # SUBSCRIBE / COMPLETE_REGISTRATION / PURCHASE
    landing_url: str
    cta: str
    count: int                # 每账户广告数
    # 创建后回填
    campaign_id: str = ""
    adset_id: str = ""
    error: str = ""


def _build_fb(token: str, account_id: str, page_id: str, pixel_id: str) -> FBClient:
    """为某账户临时构建 FBClient"""
    return FBClient(FBConfig(
        access_token=token,
        ad_account_id=account_id,
        pixel_id=pixel_id,
        page_id=page_id,
    ))


def _create_one_target(spec: TargetSpec) -> TargetSpec:
    """为一个 target 创建 1 系列 + 1 广告组（单组多广告模式）

    不抛异常，失败写进 spec.error，调用方从字段判断。
    """
    try:
        fb = _build_fb(spec.token, spec.account_id, spec.page_id, spec.pixel_id)
        camp_id = fb.create_campaign(
            spec.campaign_name,
            daily_budget_usd=spec.daily_budget_usd,
            use_campaign_budget=True,
            objective="OUTCOME_SALES",
        )
        adset_id = fb.create_adset(
            campaign_id=camp_id,
            name=f"{spec.campaign_name}-01",
            daily_budget_usd=0,  # CBO
            optimization="OFFSITE_CONVERSIONS",
            conversion_event=spec.conversion_event,
            mode="转化",
            country=spec.country or None,
            device_os=spec.device_os,
            age_min=spec.age_min,
            age_max=spec.age_max,
            gender=spec.gender,
        )
        spec.campaign_id = camp_id
        spec.adset_id = adset_id
        logger.info(f"[target {spec.account_name}] ✓ campaign={camp_id} adset={adset_id}")
    except Exception as e:
        spec.error = str(e)
        logger.warning(f"[target {spec.account_name}] ✗ 创建失败: {e}")
    return spec


async def create_targets_parallel(specs: list[TargetSpec]) -> list[TargetSpec]:
    """并发给 M 个账户创建 campaign+adset

    每个账户独立，某个失败不影响其他。
    利用线程池执行同步的 FB SDK 调用，避免阻塞事件循环。
    """
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, _create_one_target, s) for s in specs]
    return list(await asyncio.gather(*tasks))


def publish_target(
    target: dict,
    slots_for_account: list[dict],
    title: str,
    text: str,
) -> dict:
    """为单个 target（账户）把所有素材建成 creative+ad 并激活

    target: {account_id, token, page_id, pixel_id, campaign_id, adset_id, landing_url, cta}
    slots_for_account: [{media_type, media_id, media_hash}, ...] — 该账户的所有素材
    title/text: 该账户的统一文案和标题

    返回: {ok: bool, ad_ids: [...], error: str}
    """
    try:
        fb = _build_fb(
            token=target["token"],
            account_id=target["account_id"],
            page_id=target["page_id"],
            pixel_id=target["pixel_id"],
        )
        adset_id = target["adset_id"]
        camp_id = target["campaign_id"]
        landing_url = target["landing_url"]
        cta = target.get("cta", "SUBSCRIBE")

        ad_ids = []
        for i, slot in enumerate(slots_for_account):
            if not slot:
                continue
            media_type = slot.get("media_type", "video")
            media_id = slot.get("media_id", "")
            image_hash = slot.get("media_hash", "")

            if media_type == "image" or image_hash:
                creative_id = fb.create_image_creative(
                    name=f"creative-{i+1}-{image_hash[:8]}",
                    image_hash=image_hash,
                    landing_url=landing_url,
                    message=text,
                    title=title,
                    cta=cta,
                )
            else:
                creative_id = fb.create_video_creative(
                    name=f"creative-{i+1}-{media_id[:8]}",
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

        # 激活
        for aid in ad_ids:
            fb.set_ad_status(aid, "ACTIVE")
        fb.set_adset_status(adset_id, "ACTIVE")
        fb.set_campaign_status(camp_id, "ACTIVE")
        return {"ok": True, "ad_ids": ad_ids, "error": ""}
    except Exception as e:
        logger.warning(f"发布 target 失败: {e}")
        return {"ok": False, "ad_ids": [], "error": str(e)}


async def publish_targets_parallel(
    targets: list[dict],
    slots_by_account: dict[str, list[dict]],
    text_by_account: dict[str, str],
    title_by_account: dict[str, str],
) -> list[dict]:
    """并发对 M 个账户发布（每账户内部串行创建 N 条广告）

    返回每账户的结果 [{account_id, ok, ad_ids, error}, ...]
    """
    loop = asyncio.get_event_loop()
    jobs = []
    for t in targets:
        aid = t["account_id"]
        slots = slots_by_account.get(aid, [])
        title = title_by_account.get(aid, "")
        text = text_by_account.get(aid, "")
        jobs.append(loop.run_in_executor(None, publish_target, t, slots, title, text))

    outcomes = await asyncio.gather(*jobs, return_exceptions=True)
    results = []
    for t, out in zip(targets, outcomes):
        if isinstance(out, Exception):
            results.append({
                "account_id": t["account_id"],
                "ok": False,
                "ad_ids": [],
                "error": str(out),
            })
        else:
            results.append({
                "account_id": t["account_id"],
                "ok": out["ok"],
                "ad_ids": out["ad_ids"],
                "error": out["error"],
            })
    return results

