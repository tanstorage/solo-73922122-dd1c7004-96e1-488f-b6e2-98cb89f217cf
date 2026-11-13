# 鸟声片段提取服务（Birdcall WAV Fragment Extractor）

从野外长录音库中，按 **录音 ID + 起止采样帧** 提取 16-bit PCM WAV 片段，供多位标注人员
核对同一声事件。使用 FastAPI + httpx 实现，源录音通过固定配置的 HTTP 地址访问，
全程用 HTTP Range 读取，不下载整份录音。

## 设计要点（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 按录音 ID 与起止采样帧提取 WAV | `GET /recordings/{id}/frames?start=&end=`，帧号按 PCM 帧（每帧 = 声道数×2 字节）换算字节偏移 |
| 源地址固定配置、httpx 访问 | `SOURCE_BASE_URL` 环境变量；`api/app/upstream.py` 统一封装 |
| 仅支持单 `data` 块、16-bit PCM | `api/app/wav.py` 用稀疏 Range 只读头部：拒绝多 `data` 块、非 PCM、非 16-bit（返回 415） |
| Range 读取头部和所需音频，不下载整份 | 头部检查只读 64B 起步并按需扩展；音频只请求目标字节区间 |
| 所有读取绑定同一强 ETag | 首个 Range 响应锁定强 ETag（拒绝弱 ETag/缺失），后续请求带 `If-Match`；换版→412→整体 409，断流→整体 502，绝不返回半份 WAV |
| 相同录音+区间并发共用一次提取 | 进程内 `JobManager` 按 `(id, start, end)` 合并；单运行者读一次，经 fan-out relay 分发给所有等待者，每人都拿到**完整**片段 |
| 一人断连不影响他人，最后一人退出才取消远端读取 | 双引用计数：`waiters`（连接中的客户端，覆盖等待+流式）与 `readers`（暂存文件引用）。仅当 `waiters=0` 且任务仍在运行时取消上游任务 |
| 等实际读取结束后再清理暂存 | 清理协程先等任务真正结束，再用 `Condition` 等所有 reader 释放，最后才 unlink |
| 收尾期间新请求不能加入已取消任务，旧收尾不得误删新文件 | 任务结束即从映射摘除；新请求建**新任务**，暂存文件名带 UUID；清理只在 `dict[key] is self` 时删除自身路径 |
| 完成后不缓存片段 | 响应结束且无 reader 后立即删除暂存；串行重复请求会重新读源（测试场景 7 验证） |
| 并发不同片段互不影响 | 每个 key 独立任务、独立暂存、独立 relay（测试场景 8 验证） |

### 并发与流式架构

```
 client A ─┐
 client B ─┼─► JobManager ─► 一个 Job（一个上游读取，绑定一个强 ETag）
 client C ─┘                     │
                                 ├─ 写入 UUID 暂存文件（失败/取消后清理）
                                 └─ Relay：每个 chunk 发布到各客户端独立队列
```

- 响应在**头部校验通过后**立即开始流式输出；校验期错误（404/409/415/422）以正常 HTTP
  状态码返回；开始流式后的换版/断流则安全中断连接，客户端永远收不到可解析的半份 WAV。
- relay 缓存已发布 chunk，晚加入的等待者也能按序补齐完整片段；慢消费者阻塞满队列时被
  摘除，不会拖慢唯一的上游读取。

## 目录结构

```
.
├── docker-compose.yml          # 一键启动 source + api
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # FastAPI 端点、错误映射、断连生命周期
│       ├── jobs.py             # 合并任务、引用计数、relay、暂存清理
│       ├── extractor.py        # 单次提取运行者（头部+音频）
│       ├── upstream.py         # httpx Range / 强 ETag / If-Match 封装
│       ├── wav.py              # 仅 Range 的 WAV 头部校验与头部生成
│       └── config.py
├── source/
│   ├── Dockerfile
│   ├── server.py               # 本地录音源：Range、强ETag、延迟、断流、换版
│   └── generate.py             # 无 PII 的合成 WAV 样例生成器
└── tests/
    ├── e2e_test.py             # 12 个端到端场景
    ├── cancel_test.py          # 最后一人断连取消上游读取
    └── coalesce_disconnect_test.py  # 多人中一人断连，其余仍拿完整片段
```

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger，可直接在浏览器演示）：<http://localhost:8000/docs>
- 录音源控制台：<http://localhost:8080/recordings>
- 健康检查：`GET http://localhost:8000/health`

### API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 存活检查 |
| GET | `/recordings/{id}/info` | 仅读头部，返回 ETag、采样率、声道、帧数等 |
| GET | `/recordings/{id}/frames?start={f0}&end={f1}` | 提取 `[start, end)` 采样帧，返回完整 WAV |
| GET | `/jobs` | 内部状态：当前任务、状态、引用计数（演示用） |

响应头：`X-Source-ETag`（本次绑定的强 ETag）、`X-Coalesced`（`1`=搭车合并任务）。

### 录音源控制接口（演示故障注入）

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/control/recordings/{id}/version` | body `{"version":"v2"}` 切换版本，ETag 变化；进行中的旧连接在下一块被切断 |
| POST | `/control/recordings/{id}/behavior` | body `{"delay_seconds":1,"cut_after_bytes":200,"chunk_delay_seconds":0.2}` 注入延迟/中途断流/慢速流 |
| GET | `/control/recordings/{id}` | 查看当前版本、ETag、行为配置 |
| GET | `/stats` | 源收到的请求数、Range 数、已发送字节、断流次数（证明合并只读一次） |
| POST | `/control/reset`、`/control/stats/reset` | 复位版本/行为/统计 |

## 在 /docs 里的演示流程

1. **普通提取**：调 `frames`，`recording_id=nightjar, start=48000, end=96000`，下载得到
   1 秒 48kHz 单声道 16-bit WAV。
2. **多人合并**：先
   `POST localhost:8080/control/recordings/nightjar/behavior`
   设 `{"delay_seconds":1.5}`，再同时开多个相同 `frames` 请求；查
   `GET localhost:8080/stats`，可见音频 Range 只有 1 次，响应头 `X-Coalesced` 仅首个为 `0`。
3. **多人请求取消**：设 `{"chunk_delay_seconds":1}`，发起多个请求，其中一个提前中断；
   其余请求仍收到完整 WAV；当所有客户端都断开时，`stats.bytes_served` 远小于完整大小，
   且暂存目录被清空。
4. **换版失败**：慢速流进行中调 version 切到 `v2`，进行中的提取整体失败、无半份 WAV；
   之后的新请求重新绑定新 ETag 并成功。
5. **中途断流**：设 `{"cut_after_bytes":200}`，提取请求整体失败（连接中断或 502）。
6. **不支持格式**：对 `float32`（32-bit float）或 `two_data_chunks`（两个 data 块）提取，
   得到 415。

## 本地（无 Docker）运行与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r api/requirements.txt

# 生成样例
mkdir -p /tmp/samples && OUT_DIR=/tmp/samples python source/generate.py
# 起源
SAMPLES_DIR=/tmp/samples PORT=8080 python source/server.py &
# 起 API
SOURCE_BASE_URL=http://127.0.0.1:8080 SPOOL_DIR=/tmp/spool \
  uvicorn app.main:app --host 127.0.0.1 --port 8000

# 端到端测试（三个套件）
API_URL=http://127.0.0.1:8000 SOURCE_URL=http://127.0.0.1:8080 SPOOL_DIR=/tmp/spool \
  python tests/e2e_test.py
python tests/cancel_test.py
python tests/coalesce_disconnect_test.py
```

## 样例数据（无个人信息）

全部由 `source/generate.py` 用确定性伪随机/合成音调生成，无野外录音、无元数据块、
无任何个人信息：

- `nightjar` 提供 `v1`/`v2` 两个版本（演示 ETag 变化）
- `warbler`、`rail`、`sparrow` 不同时长/采样率（sparrow 为立体声）
- `float32`：32-bit float，用于 415 演示
- `two_data_chunks`：含两个 data 块，用于 415 演示

## 限制与约束

- 仅接受单 `data` 块、PCM（format=1）、16-bit；RIFF 大小须与 Content-Length 一致。
- 单片段最大帧数由 `MAX_FRAMES` 限制（默认 480000，即 48kHz 下 10 秒）。
- 合并作用域为**单 API 进程内**；多进程/多副本部署需在前置层做会话粘性或外置协调。
