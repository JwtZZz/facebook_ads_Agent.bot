"""通过 Token 拉取 BM 的 accounts / pages / pixels

把原本在 bot/handlers/fb_ads.py 里内联的查询逻辑抽出来，让 /go 和 /normal 都能复用。
"""
import logging
import requests

logger = logging.getLogger(__name__)

GRAPH = "https://graph.facebook.com/v20.0"
TIMEOUT = 15


def fetch_accounts(token: str) -> list[dict]:
    """拉取 Token 关联的所有广告账户

    返回元素形如 {id: 'act_xxx', account_id: 'xxx', name: '...', account_status: 1}
    """
    try:
        r = requests.get(f"{GRAPH}/me/adaccounts", params={
            "access_token": token,
            "fields": "id,account_id,name,account_status",
            "limit": 100,
        }, timeout=TIMEOUT)
        return r.json().get("data", [])
    except Exception as e:
        logger.warning(f"拉取广告账户失败: {e}")
        return []


def fetch_pixels(token: str, account_id: str) -> list[dict]:
    """拉取某广告账户下的所有像素"""
    try:
        r = requests.get(f"{GRAPH}/act_{account_id}/adspixels", params={
            "access_token": token,
            "fields": "id,name",
            "limit": 50,
        }, timeout=TIMEOUT)
        return r.json().get("data", [])
    except Exception as e:
        logger.warning(f"拉取像素失败 [{account_id}]: {e}")
        return []


def _pages_via_business(token: str, account_id: str) -> list[dict]:
    """方式 1：通过账户的 Business Manager owned_pages"""
    r = requests.get(f"{GRAPH}/act_{account_id}", params={
        "access_token": token, "fields": "business",
    }, timeout=TIMEOUT)
    biz_id = r.json().get("business", {}).get("id", "")
    if not biz_id:
        return []
    r = requests.get(f"{GRAPH}/{biz_id}/owned_pages", params={
        "access_token": token, "fields": "id,name", "limit": 100,
    }, timeout=TIMEOUT)
    return r.json().get("data", [])


def _pages_via_promote(token: str, account_id: str) -> list[dict]:
    """方式 2：账户关联的 promote_pages"""
    r = requests.get(f"{GRAPH}/act_{account_id}/promote_pages", params={
        "access_token": token, "fields": "id,name", "limit": 100,
    }, timeout=TIMEOUT)
    return r.json().get("data", [])


def _pages_via_me_accounts(token: str) -> list[dict]:
    """方式 3：me/accounts（个人用户 token 持有的主页）"""
    r = requests.get(f"{GRAPH}/me/accounts", params={
        "access_token": token, "fields": "id,name", "limit": 100,
    }, timeout=TIMEOUT)
    return r.json().get("data", [])


def _pages_via_assigned(token: str) -> list[dict]:
    """方式 4：系统用户 assigned_pages"""
    r = requests.get(f"{GRAPH}/me", params={
        "access_token": token, "fields": "id",
    }, timeout=TIMEOUT)
    me_id = r.json().get("id", "")
    if not me_id:
        return []
    r = requests.get(f"{GRAPH}/{me_id}/assigned_pages", params={
        "access_token": token, "fields": "id,name", "limit": 100,
    }, timeout=TIMEOUT)
    return r.json().get("data", [])


def fetch_pages(token: str, account_id: str) -> list[dict]:
    """拉取某广告账户可用的所有主页（4 重 fallback，第一个有数据的就返回）"""
    for fn, label in (
        (lambda: _pages_via_business(token, account_id), "BM owned_pages"),
        (lambda: _pages_via_promote(token, account_id), "promote_pages"),
        (lambda: _pages_via_me_accounts(token), "me/accounts"),
        (lambda: _pages_via_assigned(token), "assigned_pages"),
    ):
        try:
            pages = fn()
            if pages:
                return pages
        except Exception as e:
            logger.debug(f"拉取主页方式 {label} 失败 [{account_id}]: {e}")
    return []


def fetch_bm_all(token: str) -> tuple[list[dict], list[dict], list[dict]]:
    """一次性拉取 BM 的全部 accounts/pages/pixels

    策略：
    - 先拉所有账户
    - 再对每个账户查一次 pages 和 pixels，合并去重成 BM 级共享池
    - 返回 (accounts, unique_pages, unique_pixels)
    """
    accounts = fetch_accounts(token)
    if not accounts:
        return [], [], []

    pages_by_id: dict[str, dict] = {}
    pixels_by_id: dict[str, dict] = {}

    for acc in accounts:
        acc_id = acc.get("account_id") or acc.get("id", "").replace("act_", "")
        if not acc_id:
            continue
        for p in fetch_pages(token, acc_id):
            pid = p.get("id", "")
            if pid:
                pages_by_id.setdefault(pid, p)
        for px in fetch_pixels(token, acc_id):
            pxid = px.get("id", "")
            if pxid:
                pixels_by_id.setdefault(pxid, px)

    return accounts, list(pages_by_id.values()), list(pixels_by_id.values())
