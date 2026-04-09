# FB 广告主调度机器人 - 使用手册

基于 Telegram 的 FB 广告投放主调度机器人，集成 Adspower 指纹浏览器管理、FB 广告 API 操作、AI 对话等功能。

---

## 1. 快速开始

### 环境要求
- Python 3.13+
- Adspower 指纹浏览器（已启动）
- Telegram Bot Token
- FB Marketing API Token（从 BM 系统用户获取）

### 安装步骤
```bash
git clone <项目地址>
uv sync
# 配置 .env 文件（见下方配置说明）
uv run main.py
```

### .env 配置
```env
# Telegram Bot
BOT_TOKEN=你的Bot Token

# LLM 对话（通义千问）
LLM_API_KEY=你的API Key
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
LLM_MODEL=qwen-turbo

# 代理（可选，不用留空）
TELEGRAM_PROXY=

# Adspower（默认端口 50325）
ADSPOWER_API=http://localhost:50325

# Facebook Marketing API
FB_ACCESS_TOKEN=你的Token
FB_AD_ACCOUNT_ID=广告账户ID（不含act_前缀）
FB_PIXEL_ID=像素ID
FB_PAGE_ID=公共主页ID
```

---

## 2. 功能命令

### Adspower 环境管理

| 命令 | 说明 | 示例 |
|------|------|------|
| `/profiles` | 列出所有指纹环境 | `/profiles` |
| `/open <编号>` | 打开指纹浏览器 | `/open 3185` |
| `/close <编号>` | 关闭指纹浏览器 | `/close 3185` |
| `/active` | 查看已开启的环境 | `/active` |
| `/info <编号>` | 查看环境详情（账号/密码/IP/代理/2FA） | `/info 3165` |
| `/login <编号>` | 自动登录 FB（账密 + 2FA 验证码） | `/login 3185` |
| 直接发编号 | 自动打开环境 | `3185` |

### BM 管理

| 命令 | 说明 | 示例 |
|------|------|------|
| `/acceptbm <编号> <链接>` | 接受 BM 邀请 | `/acceptbm 3165 https://business.facebook.com/invitation/?token=xxx` |
| 直接发链接 + 编号 | 自动识别并接受 | `https://business.facebook.com/invitation/?token=xxx 3165` |
| 批量发多行 | 批量接受 BM | 每行一组：`链接 编号` |
| `/regdev <编号>` | 注册 FB 开发者 | `/regdev 3165` |

### FB 广告操作（需要配置 FB Token）

| 命令 | 说明 | 示例 |
|------|------|------|
| `/setfb` | 配置 FB 账户 | `/setfb <token> <account_id> [pixel_id] [page_id]` |
| `/campaigns` | 列出广告系列 | `/campaigns` |
| `/report` | 数据报表 | `/report today` 或 `/report yesterday` |
| `/onedollar` | 一刀流投放 | `/onedollar bet7-778-DT-0402 50 https://betxxx.com/app/index.html` |
| `/pause <ID>` | 暂停广告组 | `/pause 123456789` |
| `/resume <ID>` | 启动广告组 | `/resume 123456789` |
| `/addbudget` | 增加预算 | `/addbudget 123456 +50%` 或 `/addbudget 123456 100` |
| 发送视频文件 | 上传素材到 FB | 直接发送 mp4 视频 |
| `/publish_last` | 绑定素材并发布 | `/publish_last 正文\|标题` |
| `/automonitor` | 自动监控开关 | `/automonitor on` 或 `/automonitor off` |

### 其他

| 命令 | 说明 |
|------|------|
| `/status` | 查看当前状态 |
| `/clear` | 清除对话历史 |
| `/help` | 显示帮助 |
| 直接发文字 | AI 对话（流式输出） |

---

## 3. 投放工作流

### 一刀流投放流程

```
1. /onedollar bet7-778-DT-0402 50 https://betxxx.com/app/index.html?c=778
   → 创建1个系列 + 50个广告组（每组$1/天）

2. 发送视频素材给 Bot
   → 自动上传到 FB 广告账户

3. /publish_last Experimente os melhores caça-níqueis|Se você não quer ganhar, não clique!
   → 绑定视频创意 + 发布所有广告组

4. /automonitor on
   → 开启自动监控（每30分钟检查）

5. /report
   → 查看数据报表
```

### 自动监控规则

| 条件 | 动作 |
|------|------|
| 消耗 $3 无点击 | 自动暂停 |
| 消耗 $5 无注册 | 自动暂停 |
| 消耗 $9 无首充 | 自动暂停 |

每 30 分钟自动检查一次。

### 数据指标

| 指标 | 公式 |
|------|------|
| CPA（获客成本） | 花费 / 首充人数 |
| ROAS | 首充金额 / 消耗 x 100% |
| ARPPU | 首充金额 / 首充人数 |
| 充值转化率 | 充值人数 / 注册人数 |

- ROAS > 80% 属于稳定可持续跑
- 获客成本 8U 以下可增加预算
- ROI > 100% = 渠道盈利超过成本

---

## 4. 预算管理

### 加预算规则
- 实际消耗达预算 30% 以上即可加
- 每次加 30%-50%
- 两小时加一次
- 巴西高峰时期加
- 一天最多 3 次

### 加预算命令
```
/addbudget <campaign_id> +50%    # 按百分比加
/addbudget <campaign_id> 100     # 设为固定金额
```

---

## 5. 项目结构

```
main.bot/
├── main.py                 # 入口
├── .env                    # 环境变量配置
├── pyproject.toml          # 依赖管理
│
├── bot/                    # Telegram Bot
│   ├── app.py              # Bot 初始化 + Handler 注册
│   └── handlers/
│       ├── base.py         # /start /help /status /clear
│       ├── fb_ads.py       # FB 广告命令
│       ├── media.py        # 视频上传
│       └── adspower.py     # Adspower + BM 命令
│
├── services/               # 业务服务
│   ├── llm.py              # LLM 对话（通义千问，流式）
│   ├── campaign.py         # 广告操作（一刀流、发布）
│   ├── monitor.py          # 自动监控关停
│   ├── adspower.py         # Adspower API 封装
│   └── browser.py          # 浏览器自动化（Playwright）
│
├── fb/                     # Facebook API
│   ├── config.py           # FBConfig / FBError
│   ├── client.py           # FBClient（Graph API 封装）
│   └── insights.py         # 数据解析与格式化
│
└── store/
    └── state.py            # 全局状态（内存存储）
```

---

## 6. 注意事项

- Bot 启动前确保 Adspower 已打开
- FB Token 从代理 BM 的系统用户处获取，不会过期
- 一个公共主页最多跑 250 条广告
- 投放地区默认巴西（BR），仅投安卓移动端
- 平台保留 Facebook + Instagram，取消 Audience Network 和 Messenger
- 每次改动代码后运行 `uv sync` 同步依赖
- 全局状态存储在内存中，重启 Bot 会清空
