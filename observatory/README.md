# observatory — 望远镜导星漂移聚合服务

天文观测站值班系统：通过 HTTP 接收固定网关转发的**漂移读数**和**观测进度**，
按（望远镜 × UTC 整分钟窗口）聚合读数条数与最大漂移幅度，随水位推进发布
窗口版本，支持崩溃恢复与日志回放。Go + SQLite，无外部服务依赖。

## 快速开始

```bash
# 构建（依赖已 vendor，无需联网）
go build -o bin/obs-server ./cmd/server
go build -o bin/obs-gateway ./cmd/gateway

# 1. 启动服务：两路固定网关，接收容量 1000
./bin/obs-server -addr :8080 -db observatory.db -gateways gw-a,gw-b -capacity 1000

# 2. 另开两个终端，各起一路网关夹具（生成带迟到读数的流量并上报进度）
./bin/obs-gateway -server http://127.0.0.1:8080 -gateway gw-a -rate 10 -duration 60s
./bin/obs-gateway -server http://127.0.0.1:8080 -gateway gw-b -rate 10 -duration 60s

# 3. 查询
curl -s localhost:8080/v1/progress                 # 处理进度与水位
curl -s localhost:8080/v1/windows                  # 所有有读数的窗口
curl -s localhost:8080/v1/windows/T1/1735689600    # 某窗口的版本历史（含触发位置）
curl -s localhost:8080/v1/messages/42              # 接收日志条目及其触发的版本
curl -s -X POST localhost:8080/v1/replays          # 发起回放
curl -s localhost:8080/v1/replays/1                # 查看回放结果
```

启动参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `-addr` | `:8080` | HTTP 监听地址 |
| `-db` | `observatory.db` | SQLite 库文件；重启时复用同一文件即恢复进度 |
| `-gateways` | `gw-a,gw-b` | 固定网关名单（水位按此名单取最小值） |
| `-capacity` | `1000` | 未处理的已接收消息容量上限 |

## 消息格式

`POST /v1/messages`，JSON。每条消息在**所属网关内**有稳定标识 `msg_id`。

```json
// 漂移读数：telescope 在 ts（unix 秒）时刻的导星漂移幅度（角秒，>=0）
{"gateway":"gw-a","msg_id":"a-000001","type":"reading","telescope":"T1","ts":1735689605,"drift":0.37}

// 观测进度：本网关已完成 progress（unix 秒）时刻之前的观测
{"gateway":"gw-a","msg_id":"a-000002","type":"progress","progress":1735689660}
```

响应：

| 状态 | 含义 |
|---|---|
| `200 {"seq":N,"duplicate":false}` | 已落盘，`seq` 为接收位置（全局单调、从 1 连续编号） |
| `200 {"seq":N,"duplicate":true}` | 同一网关重试同一条消息，返回**原位置**，不重复落盘 |
| `503 {"error":...,"retryable":true}` | 未处理消息达到容量，可稍后重试（带 `Retry-After`） |
| `400 {"error":...}` | 格式非法（未知网关、未知字段、负漂移等） |

## 语义

**接收与容量。** 接收时先落盘再返回接收位置；同一 `(gateway, msg_id)` 重复
提交返回原位置（幂等）。当*已接收未处理*的消息数达到 `-capacity` 时，**新**
消息得到可重试的 503；已接收消息的重试仍返回原位置。

**处理顺序。** 后台处理器严格按接收位置（`seq`）顺序逐条处理，每条消息
的处理（聚合、发版、标记已处理）在一个 SQLite 事务内完成。

**窗口聚合。** 读数按 `(telescope, ts 所属 UTC 整分钟)` 归入窗口，累计条数
`count` 与最大漂移 `max_drift`。

**水位。** 每路网关的进度取其历次报告的**最大值**；全局水位取各路进度的
**最小值**；任何一路从未上报时水位不建立（不推进）。水位单调不减。

**版本发布。** 设窗口 `w` 的窗尾为 `end = w + 60`：

1. 水位首次到达 `end` 时，发布所有*有读数*窗口的**首版**（v1）；
2. 水位处于 `[end, end+60)` 期间到达的迟到读数，使该窗口**即时**发布
   首版（此前无读数）或修订版（v2、v3…），版本内容为当时的累计聚合；
3. 水位到达 `end + 60` 时窗口**定稿**；之后的迟到读数只进接收日志
   （`disposition = ignored_finalized`），不再改变聚合与版本。

每个版本记录触发它的接收位置 `trigger_seq`（进度消息或迟到读数），可通过
`GET /v1/messages/{seq}` 反查该位置的消息及其触发的所有版本。

**崩溃恢复。** 进程被 `kill -9` 后用同一 SQLite 库重启即可：已确认消息不丢
（接收即落盘），已发布版本不重不漏（处理与发版同事务，重启后从未处理位置
继续），查询结果与处理进度始终对应同一日志截面（多语句查询在只读事务内
执行，单连接串行化读写）。

**回放。** `POST /v1/replays` 以发起时刻已确认接收的日志**前缀**（含当时尚
未处理的消息）为输入，用一个全新的确定性状态机重放，结果存入独立的回放
表：线上接收、处理、查询不受任何影响。同一日志前缀必产生相同的逐窗版本
历史与触发位置——因此回放也是对线上处理的独立校验（测试中用它做基准
比对）。`GET /v1/replays/{id}` 查看逐窗版本与触发位置。

## HTTP 接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/messages` | 接收网关消息 |
| GET | `/v1/messages/{seq}` | 接收日志条目 + 它触发的版本 |
| GET | `/v1/progress` | 已收/已处理/待处理、容量、水位、各路进度 |
| GET | `/v1/windows?telescope=` | 所有有读数的窗口（含版本数、是否定稿） |
| GET | `/v1/windows/{telescope}/{window_start}` | 单窗口完整版本历史 |
| GET | `/v1/versions` | 线上全部已发布版本 |
| POST | `/v1/replays` | 发起回放（同步完成，返回摘要） |
| GET | `/v1/replays` / `/v1/replays/{id}` | 回放列表 / 回放结果 |

## 网关夹具

`cmd/gateway` 模拟一路固定网关，演示客户端可靠性规则：消息 id 稳定、
收到 503 稍后重试、网络错误用**同一 id** 重发（依赖接收幂等去重）。

```bash
./bin/obs-gateway -server http://127.0.0.1:8080 -gateway gw-a \
  -telescopes T1,T2,T3 -rate 10 -duration 60s -seed 1 \
  -late-fraction 0.15 -late-min 30s -late-max 5m \
  -progress-period 2s -progress-lag 5s
```

`-late-fraction` 控制迟到读数比例（可观察修订版与定稿后丢弃）；退出前
夹具会再报一次进度，把水位推过当前窗尾，让剩余窗口完成首版发布。

## 测试

```bash
./run_tests.sh
```

一条命令完成构建并跑完全部测试（单元 + 集成），输出可读结果：热缓存
约 6 秒、完全冷缓存约 21 秒，均远低于 80 秒预算。覆盖：

- 状态机：窗口聚合、水位=min(各路 max)、未齐不报不推进、首版/修订/
  迟到首版、定稿后忽略、空窗口不发版；
- 存储：接收幂等且位置连续、容量 503 且重试原位置仍受理、重开库后
  状态完整恢复、回放=线上逐版本一致、同前缀回放确定性、回放含未处理
  消息且不干扰线上；
- HTTP：端到端流程、参数校验、容量响应、异步处理器；
- 集成：真实子进程跑流量中途 `kill -9`，同库重启后消息不丢、版本不重
  不漏、回放与线上逐版本一致；网关夹具冒烟（双网关真实流量）。

## 设计要点

- **确定性状态机**（`internal/obs/machine.go`）：线上处理与回放共用同一
  `Machine`，喂入相同消息序列必产生相同版本与触发位置；回放只是换一台
  新机器重放日志前缀。
- **SQLite 单连接 + WAL**：所有读写经一个连接串行化；接收、单条消息处理、
  回放落库各自一个事务；多语句查询包在只读事务里，保证每个响应对应同一
  日志截面。崩溃恢复 = 重开库后从 `messages.processed=0` 的最小位置继续。
- **内存态即持久态的镜像**：处理器持有的 `Machine` 在启动时从
  `window_aggregates / window_versions / gateway_progress` 重建，每条消息
  的增量在同一事务内落盘；处理事务失败会重建内存态再重试。
- 目录结构：`cmd/server`（服务）、`cmd/gateway`（夹具）、
  `internal/obs`（核心库）、`tests/`（集成测试）。
