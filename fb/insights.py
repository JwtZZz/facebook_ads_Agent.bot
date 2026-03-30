"""Facebook Insights 数据解析与格式化"""


def parse_action(row: dict, action_type: str) -> float:
    for item in row.get("actions", []):
        if item["action_type"] == action_type:
            return float(item.get("value", 0))
    return 0.0


def parse_action_value(row: dict, action_type: str) -> float:
    for item in row.get("action_values", []):
        if item["action_type"] == action_type:
            return float(item.get("value", 0))
    return 0.0


def format_report_row(row: dict) -> str:
    name    = row.get("adset_name") or row.get("campaign_name") or row.get("ad_name", "?")
    spend   = float(row.get("spend", 0))
    impr    = int(row.get("impressions", 0))
    clicks  = int(row.get("clicks", 0))
    cpc     = float(row.get("cpc", 0))
    charges = parse_action(row, "offsite_conversion.fb_pixel_purchase")
    revenue = parse_action_value(row, "offsite_conversion.fb_pixel_purchase")
    subs    = parse_action(row, "offsite_conversion.fb_pixel_subscribe")
    regs    = parse_action(row, "offsite_conversion.fb_pixel_complete_registration")
    roas    = (revenue / spend * 100) if spend > 0 else 0

    return "\n".join([
        f"📊 {name}",
        f"  💰 消耗: ${spend:.2f}  展示: {impr}  点击: {clicks}  CPC: ${cpc:.2f}",
        f"  📝 注册: {regs:.0f}  首充: {charges:.0f}  订阅: {subs:.0f}",
        f"  💵 充值: ${revenue:.2f}  ROAS: {roas:.0f}%",
    ])
