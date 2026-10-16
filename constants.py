"""插件级常量。

网络层常量（URL / UA 池 / 风控参数 / 游客身份节流）沿用同作者的
astrbot_plugin_weibo_forward_to_qqgroup 实测口径，README 已注明借鉴来源。
"""

from __future__ import annotations

import re
from datetime import timedelta, timezone
from re import Pattern
from urllib.parse import quote
from zoneinfo import ZoneInfo

PLUGIN_NAME = "astrbot_plugin_weibo_remind"
PLUGIN_VERSION = "v0.3.3"

# ---------------- 微博接口 ----------------
WEIBO_HOME_URL = "https://m.weibo.cn/"
WEIBO_INDEX_URL = "https://m.weibo.cn/api/container/getIndex"
WEIBO_VISITOR_HOST = "https://visitor.passport.weibo.cn"
WEIBO_GENVISITOR_URL = f"{WEIBO_VISITOR_HOST}/visitor/genvisitor"
WEIBO_INCARNATE_URL = f"{WEIBO_VISITOR_HOST}/visitor/visitor"
# 游客系统入口页（genvisitor / incarnate 的 Referer）
WEIBO_VISITOR_REFERER = (
    f"{WEIBO_VISITOR_HOST}/visitor/visitor?entry=sinawap&a=enter&url="
    f"{quote(WEIBO_HOME_URL, safe='')}&domain=.weibo.cn&sudaref="
    "&ua=php-sso_sdk_client-0.6.36&_rand=1791030900.1"
)

# 出网白名单：微博时间线接口 + 游客身份系统
WEIBO_ALLOWED_HOSTS = {"m.weibo.cn", "visitor.passport.weibo.cn"}

# UA 池：游客身份与 UA 绑定（一套身份一套指纹），全部为移动端 UA。
# 第一个为实测验证过的 UA。
UA_POOL = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36",
)
API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://m.weibo.cn/",
    "MWeibo-Pwa": "1",
    "X-Requested-With": "XMLHttpRequest",
}

# ---------------- 抓取节流与风控 ----------------
# 时间线翻页补漏上限（默认 3 页）
TIMELINE_MAX_PAGES = 3
# 单页条数：page>=2 对游客是登录墙（实测一律 ok=-100），所以「一轮能看到多少」
# 实际取决于这一页能取多少。count=100 → 98 条 / 601 KB，count=50 → 48 条 / 308 KB。
# 取 50 是体积与「停机后补看多少」的折中：本账号月均 6 条约能覆盖半年，
# 高频账号也能覆盖数天；配 custom_cookie 的部署仍可继续翻页。
TIMELINE_PAGE_COUNT = 50
# 内存 seen 集合上限（sqlite 是权威存储，这层只是省查库）
SEEN_IDS_LIMIT = 1000
# 游客身份最长复用 3 天，到期主动换新；续期节流：最小间隔 / 每小时上限 / 失败退避基数
VISITOR_MAX_AGE = 3 * 24 * 3600
RENEW_COOLDOWN = 600.0
RENEW_MAX_PER_HOUR = 3
RENEW_FAIL_BACKOFF = 600.0
# 相邻两次微博请求的最小随机间隔（秒）
REQUEST_GAP = (2.0, 5.0)
# 出网校验用 DNS 解析缓存时长（秒）
DNS_CACHE_TTL = 300.0
# HTTP 418/432 与「200 回 HTML」均属 IP 级风控：全局冷却梯度（秒）；
# 距上次触发超过 RISK_ESCALATION_RESET 后梯度重置
RISK_COOLDOWN_STEPS = (600, 1800, 3600)
RISK_ESCALATION_RESET = 2 * 3600

# ---------------- 轮询与调度 ----------------
POLL_MIN_INTERVAL = 30  # 轮询间隔下限（秒）
POLL_JITTER = (0.0, 5.0)  # 轮询间隔抖动（秒），避免整点齐步走
BACKOFF_MAX = 600.0  # 连续失败退避封顶（秒）
SCHEDULER_SCAN_INTERVAL = 20.0  # 到点任务扫描周期（秒）

# ---------------- 时间解析 ----------------
TZ_CN = ZoneInfo("Asia/Shanghai")
TZ_CN_FIXED = timezone(timedelta(hours=8))  # 解析微博 created_at 原始串用
# 候选时间的最小回看容忍（秒）：刚过点几秒内抓到仍允许建到点任务
PAST_TOLERANCE_SECONDS = 300
# 纯数字时间的前后文判定窗口（字符数）
NUMERIC_CONTEXT_WINDOW = 10
# 「1030 开播」类 → 时刻；「1030 上线/发售」类 → 日期
STREAM_CONTEXT_WORDS = ("开播", "直播", "开播时间", "连播")
RELEASE_CONTEXT_WORDS = ("上线", "发售", "开售", "开服", "预售", "上市", "上线时间", "发售时间")
# 纯数字扫描前剔除的噪音模式：话题、@提及、URL、序号/价格/楼层等明显非时间用法
HASHTAG_RE: Pattern[str] = re.compile(r"#([^#\n]{1,40})#")
MENTION_RE: Pattern[str] = re.compile(r"@[^\s@，。,.\u4e00-\u9fa5]+")
URL_RE: Pattern[str] = re.compile(r"(?:https?://|www\.)\S+|[\w.-]+\.(?:com|cn|net|org|cc|me)/?\S*")
ORDINAL_NOISE_RE: Pattern[str] = re.compile(
    r"第\s*\d+\s*(?:期|楼|名|章|回|话|弹|条|位|次)|\d+\s*(?:元|块|楼|期|折|w|万)\b"
)

DEFAULT_REMIND_TEMPLATE = "⏰ 微博提醒\n【{name}】{source}\n⏱ 事件时间：{event_time}\n⌛ 提前 {advance}\n📝 {weibo}\n🔗 {link}"
DEFAULT_PARSE_FAIL_TEMPLATE = (
    "📢 {name} 发了新微博，提到时间安排但未能自动识别，请自行注意确认\n📝 {weibo}\n🔗 {link}"
)

USAGE = (
    "微博定时提醒指令：\n"
    "微博提醒 绑定 - 把当前群绑定为提醒推送目标\n"
    "微博提醒 解绑 - 解除当前群推送\n"
    "微博提醒 状态 - 查看运行状态（管理员）\n"
    "微博提醒 列表 - 查看待触发任务（管理员）\n"
    "微博提醒 暂停 / 微博提醒 恢复 - 暂停或恢复轮询与调度（管理员）\n"
    "微博提醒 重抓 - 立即抓取一轮（管理员）\n"
    "微博提醒 测试 <时间文本> - 调试时间解析效果（管理员）\n"
    "微博提醒 覆盖 - 博文覆盖自检：各形态分布、覆盖率、待补样本（管理员）\n"
    "微博提醒 全体 开 / 关 - 切换提醒推送时是否 @全体成员（管理员）\n"
    "微博提醒 全体 状态 - 查看 @全体 配置与今日已用次数（管理员）\n"
    "说明：自动解析微博正文里的「今晚8点」「明天下午3点」「10月30日」等时间，"
    "按提前档位（默认提前60/10分钟与到点）推送提醒"
)
