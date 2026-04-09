"""Adspower 本地 API 封装"""
import json
import os
import logging
import requests

logger = logging.getLogger(__name__)

# Adspower 默认本地端口，可通过环境变量覆盖
ADS_BASE = os.getenv("ADSPOWER_API", "http://localhost:50325")


class AdspowerError(Exception):
    pass


def _get(path: str, params: dict = None) -> dict:
    try:
        resp = requests.get(f"{ADS_BASE}{path}", params=params or {}, timeout=10)
        data = resp.json()
    except requests.exceptions.ConnectionError:
        raise AdspowerError("无法连接 Adspower，请确认软件已启动")
    except Exception as e:
        raise AdspowerError(f"请求失败: {e}")

    if data.get("code") != 0:
        raise AdspowerError(data.get("msg", "未知错误"))
    return data


def _post(path: str, body: dict) -> dict:
    try:
        resp = requests.post(f"{ADS_BASE}{path}", json=body, timeout=15)
        data = resp.json()
    except requests.exceptions.ConnectionError:
        raise AdspowerError("无法连接 Adspower，请确认软件已启动")
    except Exception as e:
        raise AdspowerError(f"请求失败: {e}")

    if data.get("code") != 0:
        raise AdspowerError(data.get("msg", "未知错误"))
    return data


def convert_cookies_to_json(cookie_str: str) -> str:
    """将 'datr=xxx;sb=xxx;c_user=xxx' 格式转为 Adspower 需要的 JSON 数组"""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        cookies.append({
            "name": name.strip(),
            "value": value.strip(),
            "domain": ".facebook.com",
            "path": "/",
        })
    return json.dumps(cookies)


def create_profile(
    name: str,
    group_id: str = "0",
    username: str = "",
    password: str = "",
    fakey: str = "",
    cookie: str = "",
    domain_name: str = "facebook.com",
) -> dict:
    """
    创建指纹环境
    POST /api/v1/user/create
    cookie: 已经是 JSON 字符串格式
    返回: {"user_id": "xxx", "serial_number": "1234"}
    """
    body: dict = {
        "name": name,
        "group_id": group_id,
        "fingerprint_config": {"automatic_timezone": 1},
        "domain_name": domain_name,
    }
    if username:
        body["username"] = username
    if password:
        body["password"] = password
    if fakey:
        body["fakey"] = fakey
    if cookie:
        body["cookie"] = cookie

    data = _post("/api/v1/user/create", body)
    result = data.get("data", {})
    return {
        "user_id": result.get("id", ""),
        "serial_number": result.get("serial_number", ""),
    }


def list_profiles(page: int = 1, page_size: int = 50) -> list[dict]:
    """
    列出所有指纹环境
    返回: [{"user_id": "xxx", "serial_number": "3185", "name": "xxx", ...}, ...]
    """
    data = _get("/api/v1/user/list", {
        "page": page,
        "page_size": page_size,
    })
    return data.get("data", {}).get("list", [])


def start_profile(serial_number: str, timeout: int = 60) -> dict:
    """
    通过编号启动指纹环境
    返回: {"ws_puppeteer": "ws://...", "ws_selenium": "...", "debug_port": xxxx}
    """
    try:
        resp = requests.get(
            f"{ADS_BASE}/api/v1/browser/start",
            params={"serial_number": serial_number},
            timeout=timeout,
        )
        data = resp.json()
    except requests.exceptions.ConnectionError:
        raise AdspowerError("无法连接 Adspower，请确认软件已启动")
    except Exception as e:
        raise AdspowerError(f"请求失败: {e}")
    if data.get("code") != 0:
        raise AdspowerError(data.get("msg", "未知错误"))
    ws_data = data.get("data", {})
    return {
        "ws_puppeteer": ws_data.get("ws", {}).get("puppeteer", ""),
        "ws_selenium":  ws_data.get("ws", {}).get("selenium", ""),
        "debug_port":   ws_data.get("debug_port", ""),
        "webdriver":    ws_data.get("webdriver", ""),
    }


def stop_profile(serial_number: str):
    """通过编号关闭指纹环境"""
    _get("/api/v1/browser/stop", {"serial_number": serial_number})


def list_active() -> list[dict]:
    """列出当前已打开的环境"""
    data = _get("/api/v1/browser/local-active")
    return data.get("data", {}).get("list", [])


def get_profile_info(serial_number: str) -> dict | None:
    """通过编号获取环境详细信息"""
    profiles = list_profiles(page_size=100)
    for p in profiles:
        if p.get("serial_number") == serial_number:
            return p
    return None


def check_status(serial_number: str) -> bool:
    """检查某个环境是否已打开，返回 True/False"""
    data = _get("/api/v1/browser/active", {"serial_number": serial_number})
    return data.get("data", {}).get("status") == "Active"
