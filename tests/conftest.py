"""测试环境配置。

把插件目录加入 sys.path，使测试可以直接 import 各模块（插件在 AstrBot 里
以 data.plugins.<name> 包形式加载，走相对导入；独立测试时走顶层导入）。
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pytest

PLUGIN_DIR = Path(__file__).resolve().parent.parent
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

TZ = None  # 由被测模块内部统一使用 Asia/Shanghai


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
