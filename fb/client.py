"""Facebook Marketing API HTTP 客户端"""
import json
import logging
import requests

from .config import FBConfig, FBError

logger = logging.getLogger(__name__)
GRAPH = "https://graph.facebook.com/v20.0"


class FBClient:
    def __init__(self, cfg: FBConfig):
        self.cfg = cfg
        self._s = requests.Session()

    # ── 底层请求 ────────────────────────────────────────────────
    def _req(self, method: str, path: str,
             params: dict = None, body: dict = None, files=None) -> dict:
        url = f"{GRAPH}/{path}"
        p = {"access_token": self.cfg.access_token}
        if params:
            p.update(params)

        kwargs: dict = {"params": p}
        if files:
            kwargs["files"] = files
            if body:
                kwargs["data"] = body
        elif body:
            kwargs["json"] = body

        resp = self._s.request(method, url, **kwargs)
        try:
            data = resp.json()
        except Exception:
            raise FBError(f"非 JSON 响应: {resp.text[:200]}")

        if "error" in data:
            err = data["error"]
            sub = err.get("error_subcode", "")
            detail = err.get("error_user_msg", "") or err.get("error_user_title", "")
            msg = f"[{err.get('code')}/{sub}] {err.get('message', str(err))}"
            if detail:
                msg += f"\n{detail}"
            raise FBError(msg)
        return data

    # ── 广告系列 ────────────────────────────────────────────────
    def create_campaign(self, name: str,
                        daily_budget_usd: float = None,
                        use_campaign_budget: bool = True,
                        objective: str = "OUTCOME_SALES") -> str:
        body: dict = {
            "name": name,
            "objective": objective,
            "status": "PAUSED",
            "special_ad_categories": [],
            "is_adset_budget_sharing_enabled": False,
        }
        if daily_budget_usd and use_campaign_budget:
            body["daily_budget"] = int(daily_budget_usd * 100)
            body["bid_strategy"] = "LOWEST_COST_WITHOUT_CAP"
        return self._req("POST", f"{self.cfg.account}/campaigns", body=body)["id"]

    def list_campaigns(self, status: str = "ACTIVE") -> list:
        return self._req("GET", f"{self.cfg.account}/campaigns", params={
            "fields": "id,name,status,daily_budget,lifetime_budget",
            "effective_status": json.dumps([status]),
            "limit": 50,
        }).get("data", [])

    def update_campaign_budget(self, campaign_id: str, daily_budget_usd: float):
        return self._req("POST", campaign_id,
                         body={"daily_budget": int(daily_budget_usd * 100)})

    def set_campaign_status(self, campaign_id: str, status: str):
        return self._req("POST", campaign_id, body={"status": status})

    # ── 广告组 ──────────────────────────────────────────────────
    def create_adset(self, campaign_id: str, name: str,
                     daily_budget_usd: float,
                     optimization: str = "OFFSITE_CONVERSIONS",
                     conversion_event: str = "SUBSCRIBE",
                     country: str = None,
                     mode: str = "转化",
                     device_os: str = "Android",
                     age_min: int = 18,
                     age_max: int = 65,
                     gender: int = 0) -> str:
        """
        gender: 0=全部, 1=男, 2=女
        device_os: Android / iOS / All
        """
        country = country or self.cfg.country

        # 设备和平台
        targeting: dict = {
            "geo_locations": {"countries": [country]},
            "publisher_platforms": ["facebook", "instagram"],
            "age_min": age_min,
            "age_max": age_max,
            "targeting_automation": {"advantage_audience": 0},
        }
        if device_os == "All":
            targeting["device_platforms"] = ["mobile", "desktop"]
        else:
            targeting["device_platforms"] = ["mobile"]
            targeting["user_os"] = [device_os]

        if gender in (1, 2):
            targeting["genders"] = [gender]

        body: dict = {
            "name": name,
            "campaign_id": campaign_id,
            "optimization_goal": optimization,
            "billing_event": "IMPRESSIONS",
            "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
            "status": "PAUSED",
            "targeting": targeting,
        }
        # CBO 模式下广告组不设预算，由系列统一控制
        if daily_budget_usd > 0:
            body["daily_budget"] = int(daily_budget_usd * 100)
        # 互动模式不需要 pixel，转化模式需要
        if mode == "互动":
            body["promoted_object"] = {"page_id": self.cfg.page_id}
        else:
            body["promoted_object"] = {
                "pixel_id": self.cfg.pixel_id,
                "custom_event_type": conversion_event,
            }
        return self._req("POST", f"{self.cfg.account}/adsets", body=body)["id"]

    def list_adsets(self, campaign_id: str = None, status: str = "ACTIVE") -> list:
        endpoint = f"{campaign_id}/adsets" if campaign_id else f"{self.cfg.account}/adsets"
        params = {
            "fields": "id,name,status,effective_status,daily_budget,campaign_id",
            "limit": 100,
        }
        if status == "ALL":
            params["effective_status"] = json.dumps([
                "ACTIVE", "PAUSED", "PENDING_REVIEW", "DISAPPROVED",
                "PREAPPROVED", "PENDING_BILLING_INFO", "CAMPAIGN_PAUSED",
                "ADSET_PAUSED", "IN_PROCESS", "WITH_ISSUES", "ARCHIVED",
            ])
        else:
            params["effective_status"] = json.dumps([status])
        return self._req("GET", endpoint, params=params).get("data", [])

    def update_adset_budget(self, adset_id: str, daily_budget_usd: float):
        return self._req("POST", adset_id,
                         body={"daily_budget": int(daily_budget_usd * 100)})

    def set_adset_status(self, adset_id: str, status: str):
        return self._req("POST", adset_id, body={"status": status})

    # ── 素材 ────────────────────────────────────────────────────
    def upload_video(self, video_path: str, title: str = "") -> str:
        with open(video_path, "rb") as f:
            return self._req(
                "POST", f"{self.cfg.account}/advideos",
                body={"title": title or video_path},
                files={"source": f},
            )["id"]

    def upload_image(self, image_path: str) -> str:
        """上传图片，返回 image_hash"""
        with open(image_path, "rb") as f:
            data = self._req(
                "POST", f"{self.cfg.account}/adimages",
                files={"filename": f},
            )
            # 返回格式: {"images": {"filename": {"hash": "xxx", ...}}}
            images = data.get("images", {})
            for v in images.values():
                return v.get("hash", "")
            raise FBError("图片上传返回数据异常")

    def create_video_creative(self, name: str, video_id: str,
                              landing_url: str, message: str = "",
                              title: str = "", cta: str = "SUBSCRIBE") -> str:
        body = {
            "name": name,
            "object_story_spec": {
                "page_id": self.cfg.page_id,
                "video_data": {
                    "video_id": video_id,
                    "message": message,
                    "title": title,
                    "call_to_action": {
                        "type": cta,
                        "value": {"link": landing_url},
                    },
                },
            },
        }
        return self._req("POST", f"{self.cfg.account}/adcreatives", body=body)["id"]

    def create_image_creative(self, name: str, image_hash: str,
                              landing_url: str, message: str = "",
                              title: str = "", cta: str = "SUBSCRIBE") -> str:
        body = {
            "name": name,
            "object_story_spec": {
                "page_id": self.cfg.page_id,
                "link_data": {
                    "image_hash": image_hash,
                    "link": landing_url,
                    "message": message,
                    "name": title,
                    "call_to_action": {
                        "type": cta,
                        "value": {"link": landing_url},
                    },
                },
            },
        }
        return self._req("POST", f"{self.cfg.account}/adcreatives", body=body)["id"]

    # ── 广告 ────────────────────────────────────────────────────
    def create_ad(self, adset_id: str, creative_id: str, name: str) -> str:
        return self._req("POST", f"{self.cfg.account}/ads", body={
            "name": name,
            "adset_id": adset_id,
            "creative": {"creative_id": creative_id},
            "status": "PAUSED",
        })["id"]

    def set_ad_status(self, ad_id: str, status: str):
        return self._req("POST", ad_id, body={"status": status})

    # ── 数据洞察 ────────────────────────────────────────────────
    def get_insights(self, object_id: str,
                     level: str = "adset",
                     date_preset: str = "today") -> list:
        fields = [
            "campaign_name", "adset_name", "ad_name",
            "spend", "impressions", "reach", "frequency",
            "clicks", "cpc", "ctr",
            "unique_outbound_clicks",
            "actions", "action_values", "cost_per_action_type",
        ]
        return self._req("GET", f"{object_id}/insights", params={
            "fields": ",".join(fields),
            "level": level,
            "date_preset": date_preset,
        }).get("data", [])

    def activate_all(self, object_ids: list[str]):
        for oid in object_ids:
            self._req("POST", oid, body={"status": "ACTIVE"})
