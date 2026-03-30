"""全局状态存储（内存，重启清空）"""
import os
from fb import FBClient, FBConfig

# 每个 chat_id 的 LLM 对话历史
chat_histories: dict[int, list[dict]] = {}

# 每个 chat_id 的 FB 账户配置
fb_configs: dict[int, FBConfig] = {}

# 自动监控开关
monitor_chats: dict[int, bool] = {}


def get_fb(chat_id: int) -> FBClient | None:
    """获取该 chat 的 FBClient，优先用动态配置，其次用环境变量默认值"""
    cfg = fb_configs.get(chat_id)
    if not cfg:
        token   = os.getenv("FB_ACCESS_TOKEN", "")
        account = os.getenv("FB_AD_ACCOUNT_ID", "")
        if token and account:
            cfg = FBConfig(
                access_token=token,
                ad_account_id=account,
                pixel_id=os.getenv("FB_PIXEL_ID", ""),
                page_id=os.getenv("FB_PAGE_ID", ""),
            )
    return FBClient(cfg) if cfg else None
