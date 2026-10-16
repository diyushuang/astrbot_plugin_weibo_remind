# 贡献指南

感谢你愿意为「微博定时提醒」出力。本文件是本仓库的**唯一规范出处**：目录约定、代码约定、提交信息格式、分支命名与发版流程都以这里为准。README 讲怎么用，这里讲怎么改。

> **本项目完全由 AI 生成**（见 README 的 [AI 生成声明](README.md#ai-生成声明)）。因此这里对"证据"的要求高于一般开源项目：AI 会犯自信的错，规范的作用就是把每一条结论钉在可复查的证据上。

---

## 1. 开发环境

| 依赖 | 说明 |
| --- | --- |
| Python | **3.12+**。AstrBot ≥4.19.5 的 `requires_python` 就是 ≥3.12，用 3.10/3.11 会让 pip 一路回退到 4.14.6（低于本插件声明下限）而装不上。`ruff.toml` 的 `target-version` 保留 `py310` 只是限制语法特性，不代表能在 3.10 上运行 |
| ruff | 静态检查与格式化，配置见 `ruff.toml` |
| pytest / pytest-asyncio | 用例运行器（`pytest.ini` 里 `asyncio_mode = auto`） |
| jionlp | 运行期依赖，跑时间解析相关用例必须有 |
| AstrBot | 真机联调时需要（声明 `>=4.24,<5`；**下限 4.24.5 与最新版都已实测通过**，CI 也按这两个版本跑矩阵）。用例本身不依赖它——只有 `tests/test_package_load.py` 会真的 import `astrbot`，没有 AstrBot 环境时它会失败 |

```bash
pip install -r requirements.txt -r dev_requirements.txt
ruff check .            # 静态检查
ruff format --check .   # 格式校验（去掉 --check 直接改）
python -m pytest        # 全量用例
```

---

## 2. 目录与文件约定

```
main.py               插件主逻辑：装配各模块、轮询、指令（只做接线）
constants.py          跨模块共享的常量与正则（URL / 风控参数 / 指令文案）
fetcher.py            微博抓取：游客身份三步流、时间线、SSRF 校验、风控冷却
time_parser.py        三层时间解析（结构化 / 纯数字 / jionlp）
post_coverage.py      博文形态分类与覆盖自检
store.py              SQLite 持久化（已见微博、提醒任务、@全体 计数）
scheduler.py          到点扫描与重启恢复
pusher.py             推送渲染与发送（含 @全体 四道闸门与失败降级）
metadata.yaml         插件元信息（含版本号）
_conf_schema.json     WebUI 配置面板定义
requirements.txt      运行期依赖
dev_requirements.txt  开发与测试依赖
docs/                 使用说明、安装配置指南、规范符合性说明、博文覆盖分析报告
tests/                用例（corpus/ 下是真实博文夹具）
```

**硬约束**

1. **仓库根目录必须就是插件目录。** `metadata.yaml`、`main.py`、`_conf_schema.json` 都在顶层——AstrBot 按目录名加载插件，往下挪一层会直接装不上。
2. **跨模块导入一律写「双路」。** AstrBot 以 `data.plugins.<插件名>` 包路径加载，插件目录本身不是顶层包：

   ```python
   try:
       from .constants import TZ_CN
   except ImportError:  # 独立脚本 / 测试环境
       from constants import TZ_CN
   ```

   写成 `from constants import ...` 这种**裸模块名**在本地扁平导入下能过、线上抛 `ModuleNotFoundError`，
   表现为「任务建了、日志有、群里永远收不到提醒」——v0.2.1 就是这么翻车的，见 CHANGELOG。
3. **模块名不要撞 PyPI 上的常见包。** `post_coverage.py` 就是因此从 `coverage.py` 改名的
   （`coverage` 是 pytest-cov 的依赖，扁平导入下可能被抢先解析）。
4. **持久化只能落 AstrBot 的 data 目录**（`get_astrbot_data_path()/plugin_data/<插件名>/`），
   不能写插件自身目录——更新/重装插件会覆盖。
5. `tests/corpus/` 与 `docs/` **要入库**（前者是回归资产，后者是用户文档）；
   本地的 `.workbuddy/`、`.mimosa/` 等工具目录在 `.gitignore` 里。

---

## 3. 代码约定

- **日志**：业务日志走 `self.logger`（或注入进来的 logger）；不新起 `logging.getLogger`，
  也不 `import logging`（`astrbot.api.logger` 才能被 AstrBot 的日志体系接住）。
- **网络**：只用 `aiohttp`，禁用 `requests`（官方规范明确要求）。
- **常量归属**：跨模块共享的放 `constants.py`；只在单个模块用的就近定义在模块常量区，
  并写明"为什么是这个值"。
- **注释写"为什么"，不写"做了什么"**：被实测推翻过的直觉尤其要留下结论与取证方式
  （例如「`【1700】` 会被 jionlp 读成公元 1700 年」「裸模块名导入只在包路径下暴露」），
  避免下次又被猜回去。
- **异常处理**：任何一条微博、一次请求的失败都不能拖垮整轮——解析层各层独立容错
  （`_safe_engine`），推送失败降级重发，翻页失败保留本轮已得结果。
- **措辞必须与真实结果一致**：日志、推送文案、覆盖统计三者的口径不允许出现倒挂
  （例如把「时间已过、正确跳过」记成「未覆盖」）。

---

## 4. 测试约定

| 用例 | 覆盖 |
| --- | --- |
| `tests/test_time_parser.py` | 三层解析：相对时间、日期+时刻分行、括号时刻、纯数字歧义、噪音剔除 |
| `tests/test_corpus_coverage.py` | **真实博文夹具回归**（16 条实际微博，期望时间点人工核对） |
| `tests/test_post_coverage.py` | 博文形态分类、覆盖统计口径、待补样本池、持久化往返 |
| `tests/test_fetcher.py` | 帖子解析、cards 容错、风控/身份失效分支、翻页失败保留首屏 |
| `tests/test_store.py` | 任务去重/原子领取、重启恢复、@全体 计数 |
| `tests/test_scheduler.py` | 多档触发、补发、失败标记、中断复位 |
| `tests/test_pusher.py` | 渲染占位符、@全体 四道闸门、失败降级、限额 |
| `tests/test_integration.py` | 端到端：抓取→解析→建任务→推送，含指令与策略分支 |
| `tests/test_package_load.py` | **按 AstrBot 真实包路径加载**（子进程隔离 `sys.path`）跑通推送 |

**写用例的规矩**

1. **能真跑就别手写复刻**：优先加载真模块（`test_package_load.py` 会把插件目录复制成
   `data/plugins/<名>/` 再按包路径 import），复刻版本走不出导入路径这类问题。
2. **涉及导入方式的改动，必须跑 `test_package_load.py`。** 它跑在**子进程**里、只暴露
   AstrBot 式根目录；同进程用例因为 `conftest.py` 把插件目录塞进了 `sys.path`，
   抓不到裸模块名这类缺陷。
3. **新用例要用故障注入自证有效**：把缺陷还原回去，用例必须失败，再恢复。
   提交信息里写清"用什么缺陷验证的"。
4. **断言数写进 CHANGELOG**，方便回溯"这次改动有没有测试兜着"。

---

## 5. 提交信息规范

采用**中文语义化提交**：首行一句话说清"改了什么、结果如何"，正文用固定小节交代证据与约束。

```
<类型>(<范围>): <一句话结论>

症状：用户或线上看到的现象，附关键报错原文。
根因：确证的原因，并写明**怎么确证的**（实测 / 抓包 / 故障注入）。
修复：改了什么，为什么这样改。
已知约束：这次没解决什么、在什么条件下会退化。
测试：断言数与覆盖的用例文件。
版本：vX.Y.Z（含文档/配置类提交时可省略）
```

**类型**（取其一）：`feat` 新功能 / `fix` 修复 / `docs` 文档 / `refactor` 重构 / `perf` 性能 / `test` 测试 / `chore` 构建与杂项。

**范围**（可选，取最贴近的一个）：`poll` 抓取与轮询 / `parse` 时间解析 / `push` 推送链路 / `store` 持久化 / `kind` 博文分类 / `repo` 仓库与发布。纯文档提交省略范围，直接写 `docs: …`。

**示例**

```
fix(push): 推送不再因裸模块名导入而静默失败

症状：任务照建、日志照打，群里永远收不到提醒；日志只有一行
      ModuleNotFoundError("No module named 'time_parser'")，任务被标 failed。
根因：pusher 里用了 `from time_parser import ...` 这种裸模块名惰性导入。
      扁平导入（本地脚本/测试）下能解析；AstrBot 以 data.plugins.<名> 包路径加载时
      顶层没有该名字 ⇒ 渲染阶段直接抛错。用子进程只暴露包根目录复现确认。
修复：改为与其它模块一致的 try: from .xxx / except ImportError: from xxx 双路导入。
已知约束：无。
测试：189 项断言全过（新增 tests/test_package_load.py 子进程用例；故障注入验证有效）。
版本：v0.2.1
```

**要求**

- 一个版本一个提交；`feat` / `fix` 类提交必须写「症状」与「根因」，根因要能指向证据。
- 不要写"优化代码""修复 bug"这类无信息量的首行。
- **不要擅自 push**，也不要 force push。

---

## 6. 分支命名

| 分支 | 用途 |
| --- | --- |
| `main` | 唯一长期分支，同时是发布分支：**每个 tag 都打在 `main` 上的某个提交** |
| `feat/<简短描述>` | 新功能，例：`feat/at-all-in-group` |
| `fix/<简短描述>` | 修复，例：`fix/package-import` |
| `docs/<简短描述>` | 文档，例：`docs/repo-structure` |
| `chore/<简短描述>` | 构建、发布、依赖，例：`chore/issue-templates` |

- 描述用小写英文与连字符，不用中文、不用下划线、不堆日期。
- 分支合入 `main` 前必须：`ruff check` 与 `ruff format --check` 通过、`pytest` 全绿。

---

## 7. 发版流程

**版本号四处必须同步**（漏一处就会出现"状态指令显示旧版本、CHANGELOG 已发新版"）：

| 位置 | 说明 |
| --- | --- |
| `metadata.yaml` 的 `version` | 插件市场与 AstrBot 读取 |
| `constants.py` 的 `PLUGIN_VERSION` | `微博提醒 状态` 首行显示 |
| `CHANGELOG.md` 的 `## vX.Y.Z` 标题 | 更新日志 |
| `README.md` 顶部徽章 | 展示用 |

提交前用这条命令复核：

```bash
grep -rn "v[0-9]\+\.[0-9]\+\.[0-9]\+" metadata.yaml constants.py README.md | head
```

**CHANGELOG** 遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，中文条目，
按「新增 / 修复 / 变更 / 已知约束」组织；`fix` 类条目写清「症状 → 根因 → 修复」。

**发布步骤**

1. 改版本号四处，写 CHANGELOG 条目；
2. 跑全量用例与 `ruff`（`tests/test_package_load.py` 必须包含在内）；
3. 提交到 `main`（提交信息按第 5 节格式，正文写清测试结果）；
4. 打**标注 tag**：`git tag -a vX.Y.Z -m "<提交标题>" -m "<日期> · 变更详情见仓库根 CHANGELOG.md"`；
5. 推 `main` 与 tag（`git push origin main && git push origin --tags`）；
6. 在 GitHub 上以该 tag 建 Release，正文取 CHANGELOG 对应段落；
7. 需要压缩包时打 `astrbot_plugin_weibo_remind-vX.Y.Z.zip`，内部放一层同名文件夹（`.zip` 不进仓库）。

---

## 8. 报告问题

- **提 Issue**：用 [Issue 模板](.github/ISSUE_TEMPLATE/) 填写，务必带插件版本（`微博提醒 状态` 可查）、
  AstrBot 版本、协议端与版本，以及相关日志原文。
- **提 PR**：先说清"改了什么、怎么验证的"。只改文档也要跑一遍 `ruff`。
- **安全问题**：不要开公开 Issue，走 [SECURITY.md](SECURITY.md) 的私下上报渠道。
- **解析不准**：请附上微博原文与 `微博提醒 测试 <时间文本>` 的输出——解析是纯函数，
  有原文就能复现。
