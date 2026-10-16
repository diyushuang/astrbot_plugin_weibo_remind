"""测试环境配置。

把插件目录加入 sys.path，使测试可以直接 import 各模块（插件在 AstrBot 里
以 data.plugins.<name> 包形式加载，走相对导入；独立测试时走顶层导入）。
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

PLUGIN_DIR = Path(__file__).resolve().parent.parent
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

# 插件全流程按上海时区解释正文时间（constants.TZ_CN）。用例构造时间也必须显式带
# 这个时区：裸 naive datetime 的 .timestamp() 跟随**运行机器的本地时区**，
# CI（UTC）与本地开发机（UTC+8）会差 8 小时，在北京时间深夜还会走到不同的分支。
CN = ZoneInfo("Asia/Shanghai")


def now_cn() -> datetime:
    """当前时刻（上海时区，带 tzinfo）。用例里不要用裸 datetime.now()。"""
    return datetime.now(tz=CN)


@pytest.fixture
def log() -> logging.Logger:
    logger = logging.getLogger("weibo_remind_test")
    logger.addHandler(logging.NullHandler())
    return logger


@pytest.fixture
def base_config() -> dict:
    return {
        "date_only_default_hour": 9,
        "max_future_days": 60,
        "debug_parse": False,
        "text_max_length": 500,
    }


def ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> float:
    """构造上海时区的时间戳，供用例断言用。"""
    from zoneinfo import ZoneInfo

    return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


def local(ts_value: float) -> str:
    """时间戳 → 上海时区的 MM-DD HH:MM 串。"""
    from zoneinfo import ZoneInfo

    return datetime.fromtimestamp(ts_value, tz=ZoneInfo("Asia/Shanghai")).strftime("%m-%d %H:%M")
