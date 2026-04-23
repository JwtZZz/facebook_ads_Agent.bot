"""Campaign creation and publish helpers for the /go web flow."""

import asyncio
import logging
from dataclasses import dataclass

from fb import FBClient, FBConfig

logger = logging.getLogger(__name__)


@dataclass
class TargetSpec:
    """Describe one campaign/adset publish unit."""

    target_id: str
    account_id: str
    account_name: str
    series_suffix: str
    token: str
    page_id: str
    pixel_id: str
    campaign_name: str
    budget_scope: str
    budget_type: str
    budget_amount_usd: float
    country: str
    device_os: str
    age_min: int
    age_max: int
    gender: int
    conversion_event: str
    landing_url: str
    application_id: str
    object_store_url: str
    cta: str
    count: int
    campaign_id: str = ""
    adset_id: str = ""
    error: str = ""


def _build_fb(token: str, account_id: str, page_id: str, pixel_id: str) -> FBClient:
    return FBClient(
        FBConfig(
            access_token=token,
            ad_account_id=account_id,
            pixel_id=pixel_id,
            page_id=page_id,
        )
    )


def _is_app_install_target(spec: TargetSpec | dict) -> bool:
    event = spec.get("event") if isinstance(spec, dict) else spec.conversion_event
    return event == "APP_INSTALL"


def _create_one_target(spec: TargetSpec) -> TargetSpec:
    """Create one campaign and one adset for a target."""

    try:
        fb = _build_fb(spec.token, spec.account_id, spec.page_id, spec.pixel_id)
        is_app_install = _is_app_install_target(spec)
        is_campaign_budget = spec.budget_scope == "campaign"
        campaign_id = fb.create_campaign(
            spec.campaign_name,
            budget_amount_usd=spec.budget_amount_usd if is_campaign_budget else 0,
            budget_type=spec.budget_type,
            use_campaign_budget=is_campaign_budget,
            objective="OUTCOME_APP_PROMOTION" if is_app_install else "OUTCOME_SALES",
        )
        adset_id = fb.create_adset(
            campaign_id=campaign_id,
            name=f"{spec.campaign_name}-01",
            budget_amount_usd=spec.budget_amount_usd if not is_campaign_budget else 0,
            budget_type=spec.budget_type,
            optimization="APP_INSTALLS" if is_app_install else "OFFSITE_CONVERSIONS",
            conversion_event=spec.conversion_event,
            mode="转化",
            application_id=spec.application_id,
            object_store_url=spec.object_store_url,
            country=spec.country or None,
            device_os=spec.device_os,
            age_min=spec.age_min,
            age_max=spec.age_max,
            gender=spec.gender,
        )
        spec.campaign_id = campaign_id
        spec.adset_id = adset_id
        logger.info(
            "[target %s %s] created campaign=%s adset=%s",
            spec.account_name,
            spec.series_suffix,
            campaign_id,
            adset_id,
        )
    except Exception as exc:
        spec.error = str(exc)
        logger.warning(
            "[target %s %s] create failed: %s",
            spec.account_name,
            spec.series_suffix,
            exc,
        )
    return spec


async def create_targets_parallel(specs: list[TargetSpec]) -> list[TargetSpec]:
    """Create campaign/adset pairs in parallel."""

    loop = asyncio.get_running_loop()
    jobs = [loop.run_in_executor(None, _create_one_target, spec) for spec in specs]
    return list(await asyncio.gather(*jobs))


def publish_target(
    target: dict,
    slots_for_target: list[dict],
    title: str,
    text: str,
) -> dict:
    """Create creatives and ads for one target."""

    try:
        fb = _build_fb(
            token=target["token"],
            account_id=target["account_id"],
            page_id=target["page_id"],
            pixel_id=target["pixel_id"],
        )
        adset_id = target["adset_id"]
        campaign_id = target["campaign_id"]
        landing_url = target["landing_url"]
        application_id = target.get("application_id", "")
        object_store_url = target.get("object_store_url", "")
        cta = target.get("cta", "SUBSCRIBE")
        is_app_install = _is_app_install_target(target)

        ad_ids: list[str] = []
        for index, slot in enumerate(slots_for_target, start=1):
            if not slot:
                continue

            media_type = slot.get("media_type", "video")
            media_id = slot.get("media_id", "")
            image_hash = slot.get("media_hash", "")

            if media_type == "image" or image_hash:
                creative_id = fb.create_image_creative(
                    name=f"creative-{index}-{image_hash[:8]}",
                    image_hash=image_hash,
                    landing_url=landing_url,
                    message=text,
                    title=title,
                    cta=cta,
                    application_id=application_id if is_app_install else "",
                    object_store_url=object_store_url if is_app_install else "",
                )
            else:
                creative_id = fb.create_video_creative(
                    name=f"creative-{index}-{media_id[:8]}",
                    video_id=media_id,
                    landing_url=landing_url,
                    message=text,
                    title=title,
                    cta=cta,
                    application_id=application_id if is_app_install else "",
                    object_store_url=object_store_url if is_app_install else "",
                )

            ad_id = fb.create_ad(
                adset_id=adset_id,
                creative_id=creative_id,
                name=f"ad-{index}-{adset_id[-6:]}",
            )
            ad_ids.append(ad_id)

        for ad_id in ad_ids:
            fb.set_ad_status(ad_id, "ACTIVE")
        fb.set_adset_status(adset_id, "ACTIVE")
        fb.set_campaign_status(campaign_id, "ACTIVE")
        return {
            "ok": True,
            "ad_ids": ad_ids,
            "error": "",
            "campaign_name": target.get("campaign_name", ""),
        }
    except Exception as exc:
        logger.warning("publish target failed: %s", exc)
        return {
            "ok": False,
            "ad_ids": [],
            "error": str(exc),
            "campaign_name": target.get("campaign_name", ""),
        }


async def publish_targets_parallel(
    targets: list[dict],
    slots_by_target: dict[str, list[dict]],
    text_by_target: dict[str, str],
    title_by_target: dict[str, str],
) -> list[dict]:
    """Publish all targets in parallel."""

    loop = asyncio.get_running_loop()
    jobs = []
    for target in targets:
        target_id = target["target_id"]
        jobs.append(
            loop.run_in_executor(
                None,
                publish_target,
                target,
                slots_by_target.get(target_id, []),
                title_by_target.get(target_id, ""),
                text_by_target.get(target_id, ""),
            )
        )

    outcomes = await asyncio.gather(*jobs, return_exceptions=True)
    results = []
    for target, outcome in zip(targets, outcomes):
        base = {
            "target_id": target["target_id"],
            "account_id": target["account_id"],
            "campaign_name": target.get("campaign_name", ""),
        }
        if isinstance(outcome, Exception):
            results.append(
                {
                    **base,
                    "ok": False,
                    "ad_ids": [],
                    "error": str(outcome),
                }
            )
            continue

        results.append(
            {
                **base,
                "ok": outcome["ok"],
                "ad_ids": outcome["ad_ids"],
                "error": outcome["error"],
            }
        )
    return results
