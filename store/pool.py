"""资源池 — 持久化 per-chat 元数据存储

设计原则:
- Token 不持久化（只存在内存 fb_configs 里）
- BM 元数据（accounts/pages/pixels 列表）落盘
- 粘 token 时重新拉取并刷新元数据
- 原子写入（临时文件 + rename）防半写入损坏
- 进程内锁防并发写冲突

数据结构:
{
  "version": 1,
  "per_chat": {
    "<chat_id>": {
      "bm": {
        "accounts": [{"id": "act_xxx", "account_id": "xxx", "name": "...", "account_status": 1}],
        "pages":    [{"id": "xxx", "name": "..."}],
        "pixels":   [{"id": "xxx", "name": "..."}],
        "last_synced": "2026-04-15T10:00:00"
      },
      "urls": ["https://...", ...],     # 最近 URL，最新在前
      "last_used": {                     # 上次 /go 选过什么，下次默认选中
        "account_ids": [...],
        "page_id": "...",
        "pixel_id": "...",
        "event": "SUBSCRIBE",
        "url": "...",
        "country": "BR",
        "device": "Android",
        "age_min": 18, "age_max": 45,
        "gender": 0,
        "budget": 20.0,
        "count": 10,
        "base_name": "bet7-DT-0415"
      }
    }
  }
}
"""
import json
import logging
import os
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

POOL_FILE = Path(__file__).resolve().parent / "pool.json"
_LOCK = threading.RLock()

POOL_VERSION = 1


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _default_chat() -> dict:
    return {
        "bm": {
            "accounts": [],
            "pages": [],
            "pixels": [],
            "last_synced": None,
        },
        "urls": [],
        "last_used": {},
    }


def _load_raw() -> dict:
    """读取 pool.json，不存在或损坏时返回空骨架"""
    if not POOL_FILE.exists():
        return {"version": POOL_VERSION, "per_chat": {}}
    try:
        data = json.loads(POOL_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "per_chat" not in data:
            raise ValueError("结构不对")
        return data
    except Exception as e:
        logger.warning(f"pool.json 解析失败，重建空池: {e}")
        return {"version": POOL_VERSION, "per_chat": {}}


def _save_raw(data: dict):
    """原子写入 pool.json（临时文件 → rename）"""
    POOL_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=".pool_", suffix=".json", dir=str(POOL_FILE.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, str(POOL_FILE))
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


# ── 公共 API ───────────────────────────────────────────────────

def get_chat_pool(chat_id: int) -> dict:
    """读取某 chat 的池数据（深拷贝，调用方可自由修改不影响内部）"""
    with _LOCK:
        data = _load_raw()
        chat = data.get("per_chat", {}).get(str(chat_id))
        if chat is None:
            return _default_chat()
        # 深拷贝，确保调用方修改不污染内部
        return json.loads(json.dumps(chat))


def update_chat_pool(chat_id: int, updater: Callable[[dict], None]):
    """函数式更新。updater 接受 chat_dict 并原地修改，框架负责加锁和保存"""
    with _LOCK:
        data = _load_raw()
        per_chat = data.setdefault("per_chat", {})
        chat_dict = per_chat.get(str(chat_id)) or _default_chat()
        updater(chat_dict)
        per_chat[str(chat_id)] = chat_dict
        _save_raw(data)


def save_bm_metadata(chat_id: int, accounts: list, pages: list, pixels: list):
    """粘 token 成功拉取后保存 BM 的元数据（不含 token）"""
    def _upd(chat):
        chat["bm"] = {
            "accounts": accounts,
            "pages": pages,
            "pixels": pixels,
            "last_synced": _now(),
        }
    update_chat_pool(chat_id, _upd)


def push_url(chat_id: int, url: str, max_len: int = 10):
    """把 URL 挤入最近使用列表头，去重，超出 max_len 截断"""
    if not url:
        return

    def _upd(chat):
        urls = chat.get("urls") or []
        if url in urls:
            urls.remove(url)
        urls.insert(0, url)
        chat["urls"] = urls[:max_len]
    update_chat_pool(chat_id, _upd)


def save_last_used(chat_id: int, **kwargs):
    """记录上次投放的配置，下次作为默认值"""
    if not kwargs:
        return

    def _upd(chat):
        lu = chat.get("last_used") or {}
        lu.update(kwargs)
        chat["last_used"] = lu
    update_chat_pool(chat_id, _upd)


def has_bm_metadata(chat_id: int) -> bool:
    """池里是否已经存过 BM 元数据"""
    pool = get_chat_pool(chat_id)
    accs = pool.get("bm", {}).get("accounts", [])
    return bool(accs)
