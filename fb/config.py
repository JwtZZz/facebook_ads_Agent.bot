from dataclasses import dataclass


class FBError(Exception):
    pass


@dataclass
class FBConfig:
    access_token: str
    ad_account_id: str  # 不含 act_ 前缀
    pixel_id: str = ""
    page_id: str = ""
    country: str = "BR"

    @property
    def account(self) -> str:
        return f"act_{self.ad_account_id.replace('act_', '')}"
