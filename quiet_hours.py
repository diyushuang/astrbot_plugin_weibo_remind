"""免打扰时段解析与判定。

配置形如 `"23:00-08:00"`（跨午夜）或 `"12:00-14:00"`（同日内）。
空串/非法串一律视为「未开启」，不抛异常——配置写错不该让插件起不来。

为什么是「顺延」而不是「静默」或「照发」：
- 静默 = 漏提醒，违背插件初衷；
- 照发 = 深夜炸群，用户次日会关掉这个插件；
- 顺延到时段结束并标注「顺延提醒」，既不吵人也不丢信息。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time as dtime

_SPEC_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$")


@dataclass(frozen=True)
class QuietHours:
    """一个免打扰时段（左闭右开）。"""

    start: dtime
    end: dtime

    @property
    def spec(self) -> str:
        return f"{self.start.strftime('%H:%M')}-{self.end.strftime('%H:%M')}"

    @property
    def crosses_midnight(self) -> bool:
        return self.start > self.end

    @staticmethod
    def _clock(when: datetime) -> dtime:
        """只取时分秒（丢掉 tzinfo，避免 aware/naive time 比较 TypeError）。"""
        return when.timetz().replace(tzinfo=None)

    def contains(self, when: datetime) -> bool:
        """when 是否落在时段内。start == end 视为 0 长度（不生效）。"""
        if self.start == self.end:
            return False
        clock = self._clock(when)
        if self.crosses_midnight:
            # 例如 23:00-08:00：23:00 之后，或 08:00 之前
            return clock >= self.start or clock < self.end
        return self.start <= clock < self.end

    def end_after(self, when: datetime) -> datetime:
        """when 之后最近的一次时段结束时刻（把提醒顺延到这里）。"""
        end = when.replace(hour=self.end.hour, minute=self.end.minute, second=0, microsecond=0)
        if self.crosses_midnight and self._clock(when) >= self.start:
            # 已进入今晚的时段，结束时刻在明天
            return end + timedelta(days=1)
        return end


def parse_quiet_hours(spec: object) -> QuietHours | None:
    """把配置串解析为 QuietHours；空值或非法值返回 None（= 未开启）。"""
    raw = str(spec or "").strip()
    if not raw:
        return None
    m = _SPEC_RE.match(raw)
    if not m:
        return None
    hour1, minute1, hour2, minute2 = (int(m.group(i)) for i in (1, 2, 3, 4))
    if not (0 <= hour1 <= 23 and 0 <= hour2 <= 23):
        return None
    if not (0 <= minute1 <= 59 and 0 <= minute2 <= 59):
        return None
    return QuietHours(dtime(hour1, minute1), dtime(hour2, minute2))
