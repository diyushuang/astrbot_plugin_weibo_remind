"""微博正文时间解析。

三层解析，按优先级合并（同级取更精确者）：

1. 结构化层（structural）：处理「日期 + 时刻」分行的排期体微博——
   `⏰10月17日 / 10:00 邀约试驾视频`、`【0945】开播预告`、`9:29 悬念海报`。
   jionlp 会把这类裸时刻判成「今天」（实测：10月17日那行的 10:00 被解析为
   2026-10-16 10:00），必须由本层按最近的日期上下文重新归属。
2. jionlp 层：处理自然语言表达（今晚8点 / 明天下午3点 / 每周五晚8点 / 双11上午10点）。
3. 纯数字层：处理「1030」这类无分隔数字（jionlp 直接报 can not parse），
   按前后文判断是时刻（开播/直播）还是日期（上线/发售），歧义则不建任务。

基准时间（base_dt）取**微博发布时间**而非当前时间：微博可能补发，用当前时间
做基准会把「今晚8点」这类表达误判为已过去。是否过期另用 now_ts 过滤。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

try:
    from .constants import (
        HASHTAG_RE,
        MENTION_RE,
        NUMERIC_CONTEXT_WINDOW,
        ORDINAL_NOISE_RE,
        PAST_TOLERANCE_SECONDS,
        RELEASE_CONTEXT_WORDS,
        STREAM_CONTEXT_WORDS,
        TZ_CN,
        URL_RE,
    )
except ImportError:  # 独立脚本 / 测试环境
    from constants import (  # type: ignore
        HASHTAG_RE,
        MENTION_RE,
        NUMERIC_CONTEXT_WINDOW,
        ORDINAL_NOISE_RE,
        PAST_TOLERANCE_SECONDS,
        RELEASE_CONTEXT_WORDS,
        STREAM_CONTEXT_WORDS,
        TZ_CN,
        URL_RE,
    )

# ---------------- 结构化层正则 ----------------
# 段落分隔：微博排期体用 / 或换行切分（也兼容全角斜杠与竖线）
_SEG_SPLIT_RE = re.compile(r"[/／|\n]+")
# 中文日期：10月17日 / 10月17号
_DATE_CN_RE = re.compile(r"(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]")
# 仅日：17日 / 17号（月份按基准月补全）
_DAY_ONLY_RE = re.compile(r"(?<![\d月])(\d{1,2})\s*[日号]")
# ---------------- 紧凑日期（排期体标题里的常见写法） ----------------
# 实拍样本：「0711重要发布节奏」「0811官宣」「【623发布节奏】」「12.03发布计划」
# 这些标题里的日期若不识别，后面紧跟的时刻清单就没有日期归属，
# 时刻会被归到发布当天（多数已过去）而整条漏提醒。
# 带分隔符的写法：12.03 / 12/03 / 12-03
_DATE_SEP_RE = re.compile(
    r"(?<![\d.])(\d{1,2})\s*[.\-/]\s*(\d{1,2})(?![\d万亿美元%折个人员星分秒])"
)
# 紧凑 4 位（0711）与 3 位（623）
_COMPACT_4_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
_COMPACT_3_RE = re.compile(r"(?<!\d)(\d{3})(?!\d)")
# 紧凑日期必须与这些「排期词」同段出现才认（否则可能只是编号/时刻）
_DATE_TITLE_WORDS = (
    "发布",
    "官宣",
    "节奏",
    "物料",
    "计划",
    "行程",
    "日程",
    "节目单",
    "上线",
    "预售",
    "开售",
    "发售",
    "开播时间",
)
# 括号内 3 位数字 + 排期词才按日期解（4 位括号内容一律是时刻，如【0945】【1700】）
_BRACKET_3_RE = re.compile(r"[【\[]\s*(\d{3})\s*[】\]]")
# 括号时刻：【0945】【09:45】【9：45】(1010)
_CLOCK_BRACKET_RE = re.compile(r"[【\[]\s*(\d{1,2})\s*[:：]?\s*(\d{2})\s*[】\]]")
# 冒号时刻：9:29 / 10：00
_CLOCK_COLON_RE = re.compile(r"(?<![\d:])(\d{1,2})\s*[:：]\s*(\d{2})(?![\d:])")
# 中文时刻：(下午)3点半 / 晚上8点20分
_CLOCK_CN_RE = re.compile(
    r"(凌晨|早上|早晨|上午|中午|下午|傍晚|晚上|夜里|今晚|明晚|今早|明早)?"
    r"\s*(\d{1,2})\s*[点時时]\s*(?:(\d{1,2})\s*分?|(半))?"
)
# 中文日期时间里的中文数字（今晚八点）交给 jionlp，本层只认阿拉伯数字
_PERIOD_HINTS = ("下午", "傍晚", "晚上", "夜里", "今晚", "明晚", "中午")
# 含相对日/周表达又不带明确日期的段落交回 jionlp：本层会把「明晚8点半」
# 当成「今天 20:30」（实测），而 jionlp 能正确算出「明天 20:30」
_RELATIVE_DAY_RE = re.compile(
    r"今天|今日|明天|明日|后天|今晚|明晚|今早|明早|昨晚|前天|次日|当天|"
    r"周[一二三四五六日天末]|星期|下周|本周|下月|本月|下个月"
)

_NUM_4_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
_NUM_8_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
# 纯数字扫描前先剔除的结构化时间（括号时刻已由结构化层处理，防重复计入歧义）
_BRACKET_TIME_STRIP_RE = re.compile(r"[【\[]\s*\d{1,2}\s*[:：]?\d{2}\s*[】\]]")
_YEAR_NOISE_RE = re.compile(r"\d{4}\s*年")
# 括号时刻的掩码（给 jionlp 用）：排期体里的【1700】会被 jionlp 当成公元 1700 年，
# 实测在 Windows 上换算时间戳直接抛 OSError 打断整轮解析，必须在喂给它之前挖掉
_BRACKET_MASK = "␣"
# 时间戳合理区间：早于 1971 或晚于 2100 一律视为解析噪声（也避开负时间戳的平台差异）
_MIN_YEAR = 1971
_MAX_YEAR = 2100

# 引擎优先级：结构化 > 纯数字 > jionlp（同一分钟冲突时取高优先级）
_ENGINE_PRIORITY = {"structural": 2, "numeric": 1, "jionlp": 0}
# 纯日期候选的「有信息量」标记：只有带这些字样的日期才值得按默认时刻提醒，
# 「今天」「本周」这类过于含糊的相对表达不建任务（否则「今天有直播」会在 9 点误报）；
# 紧凑日期（0711 / 623 / 12.03）本身即明确日期，同样算有信息量
_DATE_INFORMATIVE_RE = re.compile(
    r"月|日|号|明天|明日|后天|周[一二三四五六日天末]|星期|下月|下周|次年"
    r"|(?<!\d)\d{3,4}(?!\d)|\d{1,2}\s*[.\-/]\s*\d{1,2}"
)
# 「前夜发次日排期」的顺延依据：排期体常在晚上发次日的物料节奏（实测样本
# 「📢新商务官宣 / 7:28 发布骄阳似我中插广告」发布于 16:01），此时裸时刻指的
# 是次日同一时刻。只在这些排期词出现时才顺延，避免把「7:28 发布了花絮」
# 这类回顾性表述误判成明天的提醒。
_ROLL_WORDS = (
    "节奏",
    "预告",
    "计划",
    "物料",
    "海报",
    "花絮",
    "官宣",
    "上线",
    "开播",
    "直播",
    "节目单",
    "行程",
    "日程",
    "分集",
)
# 「疑似含时间安排」信号：用于区分「解析失败」与「压根没提时间」，
# 后者不该触发解析失败推送。刻意不含「今天」「今日」——「今天天气不错」不是时间安排
_TIME_SIGNAL_RE = re.compile(
    r"[点时]|月\s*\d|\d\s*[日月号]|周[一二三四五六日天末]|星期|今晚|明晚|明天|后天|"
    r"直播|开播|上线|发售|开售|开服|预售|发布|预告|日程|行程|安排|活动|【|\d{4}|\d{1,2}:\d{2}"
)


@dataclass
class TimeCandidate:
    """一个待建任务的候选时间点。"""

    trigger_ts: float
    source_text: str
    engine: str
    date_only: bool = False  # True 表示只识别到日期、时刻是按默认小时补的


@dataclass
class ParseResult:
    """解析结果。

    candidates：可用候选（已按时间升序、去重）
    raw_hits：检测到的时间表达总数（含被过滤/歧义的）——用于区分
        「压根没提到时间」（解析失败，可按配置推送原文）与
        「提到了但时间已过去」（静默跳过，不推送）
    ambiguous：纯数字歧义计数（既不建任务，也不触发解析失败推送，仅记日志）
    suspect：正文疑似含时间安排（有时间信号词）；解析失败推送需同时满足它，
        否则「今天天气不错」这类日常博文也会被当成解析失败推送出去
    past_rejected：识别到但因「已过去 / 超出最大未来天数」被丢弃的候选数——
        覆盖自检据此区分「因为时间已过而正确跳过」与「真的没解析出来」
    detail：调试明细
    """

    candidates: list[TimeCandidate] = field(default_factory=list)
    raw_hits: int = 0
    ambiguous: int = 0
    suspect: bool = False
    past_rejected: int = 0
    detail: list[str] = field(default_factory=list)

    @property
    def parse_failed(self) -> bool:
        """是否属于「疑似含时间安排但完全没识别出可用时间」。"""
        return self.suspect and self.raw_hits == 0 and self.ambiguous == 0


class WeiboTimeParser:
    """微博正文时间解析器。"""

    def __init__(self, config: dict[str, Any], log: Any):
        self.config = config
        self.log = log
        self.debug = bool(config.get("debug_parse", False))
        hour = config.get("date_only_default_hour")
        try:
            hour = int(hour)
        except (TypeError, ValueError):
            hour = 9
        self.date_only_hour = hour if 0 <= hour <= 23 else 9
        try:
            self.max_future_days = max(1, int(config.get("max_future_days") or 60))
        except (TypeError, ValueError):
            self.max_future_days = 60
        self._jionlp: Any = None
        self._jionlp_failed = False
        self._jionlp_warned = False

    # ---------------- 对外接口 ----------------

    @property
    def jionlp_ready(self) -> bool:
        """jionlp 是否可用（不可用时自然语言层降级，其余两层照常工作）。"""
        return self._ensure_jionlp() is not None

    def parse(self, text: str, base_dt: datetime, now_ts: float) -> ParseResult:
        """解析正文，返回窗口内的候选时间点。

        base_dt：解析基准（应为微博发布时间，带时区或本地壁钟时间均可）
        now_ts：当前时间戳，用于过滤已过去的时间（回看容忍 PAST_TOLERANCE_SECONDS）
        """
        base = self._as_wall_clock(base_dt)
        raw = text or ""
        result = ParseResult()
        result.suspect = bool(_TIME_SIGNAL_RE.search(self._strip_noise(raw)))

        # 三层各自独立容错：任一层出问题只丢该层结果，不能让整条微博解析失败
        structural, struct_dates = self._safe_engine(
            self._structural_candidates, raw, base, default=([], set())
        )
        numeric, ambiguous = self._safe_engine(self._numeric_candidates, raw, base, default=([], 0))
        jionlp = self._safe_engine(
            self._jionlp_candidates, raw, base, has_date_ctx=bool(struct_dates), default=[]
        )

        # 结构化层只认阿拉伯数字时刻，遇到「明天下午3点」「双11上午10点」这类
        # 带日期前缀的表达时，它给出的裸时刻会被 jionlp 的完整表达覆盖：
        # 前者的 source_text 是后者 source_text 的子串即可判定为同一次提及
        jionlp_srcs = [c.source_text for c in jionlp if c.source_text]
        structural = [
            c
            for c in structural
            if not any(
                c.source_text and c.source_text in js and js != c.source_text for js in jionlp_srcs
            )
        ]

        merged: dict[int, TimeCandidate] = {}
        for cand in [*structural, *numeric, *jionlp]:
            key = int(cand.trigger_ts // 60)
            old = merged.get(key)
            if old is None or _ENGINE_PRIORITY[cand.engine] > _ENGINE_PRIORITY[old.engine]:
                merged[key] = cand
        # 同一日期若已有精确时刻的候选，纯日期候选就是冗余（否则「明日10点」这类
        # 表达会同时产出「默认 9 点」与「10 点」两条），去掉没时刻的那条
        merged = self._drop_redundant_date_only(merged, structural)

        result.raw_hits = len(structural) + len(numeric) + len(jionlp) + ambiguous
        result.ambiguous = ambiguous
        now = now_ts
        future_limit = now + self.max_future_days * 86400
        for cand in sorted(merged.values(), key=lambda c: c.trigger_ts):
            if cand.trigger_ts < now - PAST_TOLERANCE_SECONDS:
                result.past_rejected += 1
                result.detail.append(f"已过：{cand.source_text} @ {self._fmt(cand.trigger_ts)}")
                continue
            if cand.trigger_ts > future_limit:
                result.past_rejected += 1
                result.detail.append(f"太远：{cand.source_text} @ {self._fmt(cand.trigger_ts)}")
                continue
            result.candidates.append(cand)
            result.detail.append(
                f"{cand.engine}：{cand.source_text} -> {self._fmt(cand.trigger_ts)}"
            )
        if self.debug and result.detail:
            self.log.info("时间解析明细：" + "；".join(result.detail))
        return result

    def _safe_engine(self, fn: Any, *args: Any, default: Any = None, **kwargs: Any) -> Any:
        """跑一层解析；该层异常时返回 default，不让整条微博解析失败。"""
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # pragma: no cover - 防御性兜底
            self.log.warning(f"时间解析层 {getattr(fn, '__name__', fn)} 异常，已跳过该层: {e!r}")
            return default

    # ---------------- 结构化层 ----------------

    def _structural_candidates(
        self, text: str, base: datetime
    ) -> tuple[list[TimeCandidate], set[str]]:
        """按段落扫描「日期 + 时刻」，返回（候选, 出现过的日期集合）。"""
        out: list[TimeCandidate] = []
        date_ctx: datetime | None = None
        date_ctx_src = ""
        dates: dict[str, str] = {}  # 日期 → 原文（纯日期候选的 source 用原文而非格式化日期）
        for seg in _SEG_SPLIT_RE.split(text):
            seg = seg.strip()
            if not seg:
                continue
            explicit_date = _DATE_CN_RE.search(seg)
            # 段落内先更新日期上下文（段落形如「⏰10月17日」或「10月17日 20:00 直播」）
            if explicit_date:
                resolved = self._resolve_date(
                    base, int(explicit_date.group(1)), int(explicit_date.group(2))
                )
                if resolved is not None:
                    date_ctx, date_ctx_src = resolved, explicit_date.group(0)
                    dates[resolved.strftime("%Y-%m-%d")] = explicit_date.group(0)
            else:
                m_day = _DAY_ONLY_RE.search(seg)
                if m_day and not _CLOCK_COLON_RE.search(seg):
                    resolved = self._resolve_date(base, None, int(m_day.group(1)))
                    if resolved is not None:
                        date_ctx, date_ctx_src = resolved, m_day.group(0)
                        dates[resolved.strftime("%Y-%m-%d")] = m_day.group(0)
                else:
                    # 紧凑日期（0711 / 623 / 12.03）——排期体标题里的常见写法
                    compact = self._find_compact_date(seg)
                    if compact is not None:
                        month, day, src = compact
                        resolved = self._resolve_date(base, month, day)
                        if resolved is not None:
                            date_ctx, date_ctx_src = resolved, src
                            dates[resolved.strftime("%Y-%m-%d")] = src
            # 相对日/周表达交给 jionlp（本层算不准这类日期的偏移），
            # 此时只保留括号时刻【0945】这种明确写法
            loose_ok = bool(explicit_date) or not _RELATIVE_DAY_RE.search(seg)
            clock = self._find_clock(seg, allow_loose=loose_ok)
            if clock is None:
                continue
            hh, mm, clock_src = clock
            day = date_ctx or base
            trigger = day.replace(hour=hh, minute=mm, second=0, microsecond=0)
            # 排期体的「前夜发次日排期」：没有日期上下文且裸时刻已过发布时刻，
            # 又有排期词时，按次日同一时刻理解
            if date_ctx is None and trigger <= base and any(w in text for w in _ROLL_WORDS):
                trigger += timedelta(days=1)
            src = f"{date_ctx_src} {clock_src}".strip() if date_ctx else clock_src
            out.append(self._cand(trigger, src, "structural"))
        # 只有日期没有时刻的段落：按 date_only_default_hour 补一个默认时刻
        if date_ctx_src and not any(c is not None and c.engine == "structural" for c in out):
            for ds, ds_src in sorted(dates.items()):
                dt = datetime.strptime(ds, "%Y-%m-%d").replace(
                    hour=self.date_only_hour, minute=0, second=0, microsecond=0
                )
                out.append(self._cand(dt, ds_src, "structural", date_only=True))
        return [c for c in out if c is not None], set(dates)

    @staticmethod
    def _find_compact_date(seg: str) -> tuple[int, int, str] | None:
        """识别排期体标题里的紧凑日期，返回（月, 日, 原文）。

        只在与「排期词」（发布/官宣/节奏/物料/计划/行程/上线…）同段出现时才认，
        否则四位数字更可能是时刻（如「1030开播」）或编号（如「第1030期」）。
        实拍样本：「0711重要发布节奏」「0811官宣」「【623发布节奏】」「12.03发布计划」。
        """
        if not any(w in seg for w in _DATE_TITLE_WORDS):
            return None
        # 带分隔符的 12.03 / 12/03 / 12-03 最明确，优先
        m = _DATE_SEP_RE.search(seg)
        if m:
            month, day = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return month, day, m.group(0)
        # 括号内 3 位（【623发布节奏】）：4 位括号内容一律留给时刻规则
        m = _BRACKET_3_RE.search(seg)
        if m:
            month, day = int(m.group(1)[0]), int(m.group(1)[1:])
            if 1 <= month <= 12 and 1 <= day <= 31:
                return month, day, m.group(0)
        # 紧凑 4 位：0711 → 07月11日
        m = _COMPACT_4_RE.search(seg)
        if m:
            month, day = int(m.group(1)[:2]), int(m.group(1)[2:])
            if 1 <= month <= 12 and 1 <= day <= 31:
                return month, day, m.group(0)
        # 紧凑 3 位：623 → 6月23日（须紧邻排期词，避免把 930 之类时刻当日期）
        m = _COMPACT_3_RE.search(seg)
        if m:
            tail = seg[m.end() : m.end() + 6]
            if any(tail.startswith(w) for w in ("发布", "官宣", "节奏", "物料", "计划", "行程")):
                month, day = int(m.group(1)[0]), int(m.group(1)[1:])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return month, day, m.group(0)
        return None

    @staticmethod
    def _find_clock(seg: str, allow_loose: bool = True) -> tuple[int, int, str] | None:
        """在段落里找一个时刻，返回（时, 分, 原文）。

        allow_loose=False 时只认括号时刻【0945】（明确的 HHMM 写法），
        不认裸时刻与中文时刻——那些在相对日语境下需要 jionlp 的日期推算。
        """
        m = _CLOCK_BRACKET_RE.search(seg)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            if hh <= 23 and mm <= 59:
                return hh, mm, m.group(0)
        if not allow_loose:
            return None
        m = _CLOCK_COLON_RE.search(seg)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            if hh <= 23 and mm <= 59:
                return hh, mm, m.group(0)
        m = _CLOCK_CN_RE.search(seg)
        if m:
            hint = m.group(1) or ""
            hh = int(m.group(2))
            mm = 30 if m.group(4) else int(m.group(3) or 0)
            if hh < 12 and hint in _PERIOD_HINTS:
                hh += 12
            if hh <= 23 and mm <= 59:
                return hh, mm, m.group(0).strip()
        return None

    def _resolve_date(self, base: datetime, month: int | None, day: int) -> datetime | None:
        """把（月, 日）解析为不低于基准日的具体日期。"""
        year = base.year
        if month is None:
            month = base.month
            if day < base.day:  # 本月已过，顺延到下月
                month += 1
                if month > 12:
                    month, year = 1, year + 1
        if not 1 <= month <= 12 or not 1 <= day <= 31:
            return None
        try:
            dt = datetime(year, month, day)
        except ValueError:
            return None
        if dt.date() < base.date() and month < base.month:
            try:
                dt = dt.replace(year=year + 1)
            except ValueError:  # pragma: no cover
                return None
        return dt

    @staticmethod
    def _drop_redundant_date_only(
        merged: dict[int, TimeCandidate], structural: list[TimeCandidate]
    ) -> dict[int, TimeCandidate]:
        """清掉两类多余的纯日期候选：

        1. 同一日期已有精确时刻的候选（任何一层给出的都算）——「明日10点」不该
           同时产出「默认 9 点」与「10 点」；
        2. 含糊的相对日期（「今天」「本周」）—— 无信息量，建了就是误报。
        """
        precise_dates = {
            datetime.fromtimestamp(c.trigger_ts, tz=TZ_CN).strftime("%Y-%m-%d")
            for c in merged.values()
            if not c.date_only
        }
        out: dict[int, TimeCandidate] = {}
        for key, cand in merged.items():
            if not cand.date_only:
                out[key] = cand
                continue
            if not _DATE_INFORMATIVE_RE.search(cand.source_text):
                continue
            day = datetime.fromtimestamp(cand.trigger_ts, tz=TZ_CN).strftime("%Y-%m-%d")
            if day in precise_dates:
                continue
            out[key] = cand
        return out

    # ---------------- jionlp 层 ----------------

    def _ensure_jionlp(self) -> Any:
        if self._jionlp is not None or self._jionlp_failed:
            return self._jionlp
        try:
            import jionlp

            self._jionlp = jionlp
        except Exception as e:  # pragma: no cover - 取决于运行环境
            self._jionlp_failed = True
            if not self._jionlp_warned:
                self._jionlp_warned = True
                self.log.warning(
                    f"jionlp 不可用（{e}），中文时间解析降级为结构化+纯数字两层；"
                    "如需完整能力请执行 pip install jionlp"
                )
        return self._jionlp

    def _jionlp_candidates(
        self, text: str, base: datetime, has_date_ctx: bool
    ) -> list[TimeCandidate]:
        """调 jionlp 抽取自然语言时间表达。

        喂进去之前先把括号时刻挖掉：排期体里的【1700】/【1730】会被 jionlp
        当成公元 1700 年（实测其返回 1700-01-01 ~ 1700-12-31 的 time_span），
        既产生错误候选，也会在 Windows 上因负时间戳抛 OSError。括号时刻由
        结构化层负责，这里只需要让它看不见。
        """
        jio = self._ensure_jionlp()
        if jio is None or not text.strip():
            return []
        masked = _BRACKET_TIME_STRIP_RE.sub(lambda m: _BRACKET_MASK * len(m.group(0)), text)
        try:
            found = jio.ner.extract_time(masked, time_base=base)
        except Exception as e:
            self.log.info(f"jionlp 抽取出错（本条跳过自然语言层）: {e}")
            return []
        out: list[TimeCandidate] = []
        for item in found or []:
            if not isinstance(item, dict):
                continue
            src = str(item.get("text") or "").strip()
            # 掩码残留或裸时刻交给结构化层（本层会把「10月17日」后的 10:00 误判成今天）
            if not src or _BRACKET_MASK in src or _CLOCK_COLON_RE.fullmatch(src):
                continue
            ts = self._extract_ts(item, base)
            if ts is None:
                continue
            dt = datetime.fromtimestamp(ts, tz=TZ_CN).replace(tzinfo=None)
            date_only = False
            # 全天日期候选（00:00-23:59）：按 date_only_default_hour 归一，
            # 避免凌晨提醒
            if dt.hour == 0 and dt.minute == 0:
                parts = (item.get("detail") or {}).get("time") or []
                if isinstance(parts, list) and len(parts) == 2:
                    try:
                        end = self._parse_std(str(parts[1]))
                    except ValueError:
                        end = None
                    if end is not None and end.hour == 23 and end.minute == 59:
                        dt = dt.replace(hour=self.date_only_hour)
                        date_only = True
            out.append(self._cand(dt, src, "jionlp", date_only=date_only))
        if has_date_ctx:
            # 已经由结构化层接管了这类文本的时刻归属；jionlp 可能把「悬念海报3 / 11:00」
            # 之类误判成 3 月 11 日，仅保留其中在未来的候选由主流程过滤兜底
            out = [c for c in out if c is not None and not c.source_text.startswith("/")]
        return [c for c in out if c is not None]

    def _extract_ts(self, item: dict[str, Any], base: datetime) -> float | None:
        """从 jionlp 结果里取触发时间戳（time_point/time_span 取起点，period 取最近点）。

        年份超出「基准年 -1 ~ +3" 的候选一律丢弃：排期体里的四位编号会被 jionlp
        解读成公元年份（1700/1730），这类噪声不能进入任务表。
        """
        detail = item.get("detail") or {}
        if not isinstance(detail, dict):
            return None
        kind = str(detail.get("type") or item.get("type") or "")
        slot = detail.get("time")
        if kind == "time_period":
            # 周期表达（每周五晚8点）：jionlp 已按基准时间算出最近一次触发点
            point = (slot or {}).get("point") if isinstance(slot, dict) else None
            slot = (point or {}).get("time") if isinstance(point, dict) else None
        if kind == "time_delta" or not isinstance(slot, list) or not slot:
            return None
        try:
            start = self._parse_std(str(slot[0]))
        except (ValueError, TypeError):
            return None
        if not base.year - 1 <= start.year <= base.year + 3:
            return None
        return self._to_ts(start)

    @staticmethod
    def _parse_std(raw: str) -> datetime:
        """解析 jionlp 的标准时间串 YYYY-MM-DD HH:MM:SS。"""
        return datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")

    # ---------------- 纯数字层 ----------------

    def _numeric_candidates(self, text: str, base: datetime) -> tuple[list[TimeCandidate], int]:
        """扫描 4 位（1030）与 8 位（20261030）纯数字时间，返回（候选, 歧义数）。"""
        cleaned = self._strip_noise(text)
        out: list[TimeCandidate] = []
        ambiguous = 0
        if not cleaned:
            return out, ambiguous
        for m in _NUM_8_RE.finditer(cleaned):
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                ambiguous += 1
                continue
            if dt.date() < base.date() and dt.month < base.month:
                dt = dt.replace(year=dt.year + 1)
            out.append(self._cand(dt.replace(hour=self.date_only_hour), m.group(0), "numeric"))
        span = NUMERIC_CONTEXT_WINDOW
        for m in _NUM_4_RE.finditer(cleaned):
            num = m.group(1)
            ctx = cleaned[max(0, m.start() - span) : m.end() + span]
            is_stream = any(w in ctx for w in STREAM_CONTEXT_WORDS)
            is_release = any(w in ctx for w in RELEASE_CONTEXT_WORDS)
            if is_stream and not is_release:
                hh, mm = int(num[:2]), int(num[2:])
                if hh <= 23 and mm <= 59:
                    dt = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    out.append(self._cand(dt, num, "numeric"))
                else:
                    ambiguous += 1
            elif is_release and not is_stream:
                month, day = int(num[:2]), int(num[2:])
                dt = self._resolve_date(base, month, day)
                if dt is None:
                    ambiguous += 1
                    continue
                out.append(self._cand(dt.replace(hour=self.date_only_hour), num, "numeric"))
            # 两类词都命中或都没命中：歧义过大（也可能是编号/价格），不建任务
            else:
                ambiguous += 1
        return [c for c in out if c is not None], ambiguous

    @staticmethod
    def _strip_noise(text: str) -> str:
        """剔除话题、@提及、链接、序号/价格等明显非时间用法与已处理的括号时刻。"""
        cleaned = _BRACKET_TIME_STRIP_RE.sub(" ", text)
        cleaned = HASHTAG_RE.sub(" ", cleaned)
        cleaned = MENTION_RE.sub(" ", cleaned)
        cleaned = URL_RE.sub(" ", cleaned)
        cleaned = ORDINAL_NOISE_RE.sub(" ", cleaned)
        cleaned = _YEAR_NOISE_RE.sub(" ", cleaned)
        return cleaned

    # ---------------- 工具 ----------------

    @staticmethod
    def _as_wall_clock(dt: datetime) -> datetime:
        """统一为上海时区的壁钟时间（jionlp 按本地壁钟解释基准时间）。"""
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(TZ_CN).replace(tzinfo=None)

    def _cand(
        self, dt: datetime, source_text: str, engine: str, date_only: bool = False
    ) -> TimeCandidate | None:
        """构造候选；时间不合理（年份越界等）时返回 None，由调用方跳过。"""
        ts = self._to_ts(dt)
        if ts is None:
            return None
        return TimeCandidate(
            trigger_ts=ts, source_text=source_text, engine=engine, date_only=date_only
        )

    @staticmethod
    def _to_ts(dt: datetime) -> float | None:
        """壁钟时间 → 时间戳（按上海时区解释）。

        年份越界或平台不支持的时间返回 None 而不是抛异常：Windows 上
        `timestamp()`/`fromtimestamp()` 对 1970 年前的时间抛 OSError
        （实测被 jionlp 把【1700】读成公元 1700 年触发），解析层绝不能因此崩。
        """
        if not _MIN_YEAR <= dt.year <= _MAX_YEAR:
            return None
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ_CN)
            return dt.timestamp()
        except (OSError, OverflowError, ValueError):
            return None

    @staticmethod
    def _fmt(ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=TZ_CN).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def human_time(ts: float) -> str:
        """推送文案里的可读时间。"""
        return datetime.fromtimestamp(ts, tz=TZ_CN).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def advance_label(minutes: int) -> str:
        """提前档位的中文标签（0 表示到点）。"""
        if minutes <= 0:
            return "到点"
        if minutes % 1440 == 0:
            return f"{minutes // 1440} 天"
        if minutes % 60 == 0:
            return f"{minutes // 60} 小时"
        return f"{minutes} 分钟"


# 供 main 复用的时间跨度工具
def in_future_window(ts: float, now_ts: float, max_days: int) -> bool:
    return (
        now_ts - PAST_TOLERANCE_SECONDS <= ts <= now_ts + timedelta(days=max_days).total_seconds()
    )
