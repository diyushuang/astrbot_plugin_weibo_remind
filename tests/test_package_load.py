"""按 AstrBot 的真实加载方式（data.plugins.<插件名> 包路径）跑通全链路。

存在的理由：有些缺陷只在包路径导入下才暴露，用扁平导入（把插件目录塞进
sys.path）测不出来。实测踩到的例子：`pusher.render_remind` 里曾用
`from time_parser import ...` 这种裸模块名做惰性导入——扁平导入下没问题，
AstrBot 包路径下抛 `ModuleNotFoundError: No module named 'time_parser'`，
表现是「有任务、有日志、但群里永远收不到提醒」。

所以关键那条用例跑在**子进程**里：只把 AstrBot 式根目录放进 sys.path，
插件目录本身不可作为顶层模块导入——这正是线上部署的真实情形。
"""

from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
PKG = "astrbot_plugin_weibo_remind"

# 子进程驱动脚本：按包路径加载插件 → 造假微博 → 建任务 → 调度 → 渲染推送文案
DRIVER = """
import asyncio, importlib, sys, time
from datetime import datetime
from pathlib import Path

root = sys.argv[1]
sys.path.insert(0, root)          # 只暴露 AstrBot 式根目录，插件目录不可顶层导入
mod = importlib.import_module("data.plugins.{pkg}.main")
mod._resolve_data_dir = lambda: Path(sys.argv[2])

class FakeContext:
    def __init__(self): self.sent = []; self.first_segment = ""
    async def send_message(self, session, chain):
        self.sent.append((session, "".join(getattr(s, "text", "") for s in chain.chain)))
        self.first_segment = type(chain.chain[0]).__name__
        return True

async def noop(*a, **k): return None

async def main():
    ctx = FakeContext()
    p = mod.WeiboRemindPlugin(context=ctx, config={{
        "uid": "6902546888", "check_window_minutes": 0,
        "remind_advance_minutes": [60, 0], "text_max_length": 300,
        "push_sessions": ["aiocqhttp:GroupMessage:123456"],
        "at_all": True, "at_all_advance_minutes": [], "at_all_daily_limit": 10,
    }})
    p._poll_loop = lambda: noop()
    mod.WeiboFetcher.start = noop
    mod.WeiboFetcher.warmup = noop
    # 关掉后台调度循环：下面手动驱动 scan_once，不能被后台抢跑
    async def noop_run(self): return None
    mod.RemindScheduler.run = noop_run
    await p.initialize()
    try:
        dt = datetime.fromtimestamp(time.time() + 5 * 86400)
        post = mod.WeiboPost(
            id="pkg-1", uid="6902546888", author="麦报的小行家",
            link="https://weibo.com/6902546888/pkg1",
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            created_ts=time.time(),
            text="📢商务物料\\n发布规划\\n⏰" + str(dt.month) + "月" + str(dt.day) + "日\\n10:00 邀约试驾视频",
            is_retweet=False, orig_text="",
        )
        created = await p._process_post(post)
        now = time.time()
        with p.store._lock, p.store._conn:
            p.store._conn.execute("UPDATE remind_tasks SET trigger_at = ?", (now - 1,))
        results = await p.scheduler.scan_once(now_ts=now)
        print("CREATED=" + str(created))
        print("STATUSES=" + ",".join(s for _, s in results))
        if ctx.sent:
            print("FIRST_SEGMENT=" + ctx.first_segment)
            print("PUSH_OK=" + ctx.sent[0][1].replace("\\n", " | "))
        else:
            print("PUSH_MISSING")
    finally:
        await p.terminate()

asyncio.run(main())
"""


class FakeContext:
    def __init__(self):
        self.sent: list[tuple[str, str]] = []

    async def send_message(self, session, chain):
        self.sent.append((session, "".join(getattr(s, "text", "") for s in chain.chain)))
        self.first_segment = type(chain.chain[0]).__name__
        return True


def _build_package(root: Path) -> Path:
    """把插件目录复制成 <root>/data/plugins/<name>/ 的 AstrBot 目录结构。"""
    plugins_dir = root / "data" / "plugins"
    pkg_dir = plugins_dir / PKG
    pkg_dir.mkdir(parents=True)
    for init in (
        root / "data" / "__init__.py",
        plugins_dir / "__init__.py",
        pkg_dir / "__init__.py",
    ):
        init.write_text("", encoding="utf-8")
    for src in PLUGIN_ROOT.iterdir():
        if src.is_file():
            shutil.copy(src, pkg_dir)
    return root


@pytest.fixture
def packaged(tmp_path, monkeypatch):
    """在当前进程里按包路径导入插件（用于轻量断言）。"""
    root = tmp_path / "as_astrbot"
    _build_package(root)

    monkeypatch.syspath_prepend(str(root))
    module_name = f"data.plugins.{PKG}.main"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    yield module
    for name in list(sys.modules):
        if name.startswith(f"data.plugins.{PKG}"):
            sys.modules.pop(name, None)


def test_package_import_registers_commands(packaged):
    """包路径导入后指令必须都注册上（AstrBot 就是按这个路径加载的）。"""
    cls = packaged.WeiboRemindPlugin
    assert len([n for n in dir(cls) if n.startswith("cmd_")]) >= 10
    assert hasattr(cls, "weibo_remind")


def test_package_import_both_constructor_paths(packaged):
    """star_manager 的两条构造路径都要能过（config 可省略）。"""
    from astrbot.api.star import Context

    ctx = Context.__new__(Context)
    assert packaged.WeiboRemindPlugin(context=ctx, config={})
    assert packaged.WeiboRemindPlugin(context=ctx)


def test_full_pipeline_renders_push_in_isolated_subprocess(tmp_path):
    """隔离子进程里跑通「建任务 → 调度 → 渲染推送文案」。

    子进程只把 AstrBot 式根目录放进 sys.path（插件目录本身不是顶层包），
    与线上部署一致——裸模块名导入、相对导入写错这类问题会在这里现形。
    """
    root = _build_package(tmp_path / "deploy")
    data_dir = tmp_path / "plugin_data"
    driver = tmp_path / "drive.py"
    driver.write_text(DRIVER.format(pkg=PKG), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, str(driver), str(root), str(data_dir)],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
    )
    out = proc.stdout
    assert proc.returncode == 0, f"子进程失败：\n{proc.stderr[-2000:]}"
    assert "PUSH_MISSING" not in out, f"未渲染出推送文案：\n{out}\n{proc.stderr[-1000:]}"
    assert "PUSH_OK=" in out, f"推送链路未跑通：\n{out}\n{proc.stderr[-1000:]}"
    assert "STATUSES=sent" in out.replace("STATUSES=sent,sent", "STATUSES=sent")
    assert "微博提醒" in out and "麦报的小行家" in out
    # @全体 段在真实加载路径下也要能构造出来（OneBot 的 at_all 段）
    assert "FIRST_SEGMENT=AtAll" in out, f"@全体 段未生效：\n{out}"
