# Stellar Wholesale → Dujiao-Next eSIM Bridge

将 Stellar Wholesale eSIM 接入 Dujiao-Next：桥接服务提供独角上游协议，把符合筛选规则的套餐合并成一个商品、多档 SKU，并处理采购、交付通知和取消退款。

```text
Dujiao-Next ── HMAC API ──> Bridge ── Bearer API ──> Stellar Wholesale
     ↑                       ↑
     └── 持久化交付通知 ───────┘── Stellar Webhook
```

支持 eSIM 新购及供应商允许的取消；没有 VPN、Antivirus 或 eSIM Top-up 的独立销售接口。Stellar 随卡赠送的 VPN 不作为 eSIM 发货的必要条件。

## 项目结构

服务按职责拆分为小模块，`app/main.py` 只做入口和兼容性再导出（`uvicorn app.main:app`）。模块之间的调用一律写成 `模块.函数`，因此测试与排查时的 `patch` 目标就是定义该名字的模块。

| 文件 | 职责 |
|---|---|
| `app/main.py` | 入口模块：再导出公开接口，保持 `app.main:app` 与旧导入路径可用 |
| `app/api.py` | FastAPI 应用、Dujiao 鉴权、HTTP 路由、请求体上限、生命周期与后台任务登记 |
| `app/settings.py` | 环境变量解析与启动校验，集中全部可调参数 |
| `app/util.py` | 时间／JSON 辅助、SQLite 连接、建表与迁移 |
| `app/stellar_client.py` | 上游 HTTP 客户端（重试、限速、幂等键） |
| `app/wallet_balance.py` | 钱包余额快照与缓存 |
| `app/catalogue.py` | 套餐筛选（portfolio-v1）、库存换算、目录全量与增量同步、目录快照保护 |
| `app/presentation.py` | 商品／SKU 渲染、订单响应与交付结构 |
| `app/orders.py` | 采购、报价校验、交付轮询、逐卡取消与退款核对 |
| `app/queues.py` | 订单状态变更＋通知的原子写入、回调 outbox、Webhook 事件、重试上限、死信与清理 |
| `app/store.py` | `plans` / `orders` 表读取 |
| `app/logs.py` | 日志配置（text／json） |
| `app/security.py` | Dujiao 请求／通知签名、Stellar Webhook 验签、回调地址校验 |
| `tests/test_catalogue.py` | 目的地、线路、去重、预算、性价比与停售规则 |
| `tests/test_reliability.py` | 采购幂等、报价保护、完整交付、通知恢复、增量同步及部分退款 |
| `tests/test_security.py` | 签名验证 |
| `tests/test_hardening.py` | 目录快照保护、请求体上限、回调地址校验、队列死信／清理、订单路由 |
| `tests/test_schema_logs.py` | 旧库原地迁移、交付内容保留期、JSON 日志字段 |
| `ops/sku-sort/` | 指定独角商品的总价事务内自动排序及每 15 秒兜底 timer |
| `ops/minute-sync/` | 每分钟向独角原生队列投递价格／库存同步任务的 Go 工具和 systemd 配置 |
| `.env.example` | 配置模板，不含真实凭证 |
| `pyproject.toml` | 依赖声明、`requires-python`、ruff／mypy 配置 |
| `requirements.txt` | 由 `pyproject.toml` 生成的锁定依赖，供 Docker 与 CI 使用 |
| `.github/workflows/ci.yml` | Python 3.12 测试＋lint、依赖锁校验、Go 构建及镜像启动检查 |
| `docker-compose.yml` | 单实例服务、端口绑定、非 root 运行与持久化数据库卷 |

## 快速部署

需要 Docker Compose、Stellar Wholesale 已充值账户，以及可接收 HTTPS Webhook 的域名。

### 1. 上游凭证与 Webhook

在 Stellar 的 Developer 页面创建 API Key，授权 `plans:read`、`wallet:read`、`orders:create`、`orders:read`、`esims:read`。需要取消功能时增加 `esims:cancel`；通过 API 管理 Webhook 时增加 `webhooks:manage`。

在 Stellar 配置 Webhook，地址为 `https://你的桥接域名/webhooks/stellar`。订阅 `catalogue.updated`、`order.fulfilled`、`order.failed`、`esim.fulfilled`、`esim.failed`，保存创建时提供的 signing secret。

### 2. 启动桥接服务

```bash
cp .env.example .env
openssl rand -hex 32
```

编辑 `.env`，填写 `STELLAR_API_KEY`、`STELLAR_WEBHOOK_SECRET` 和独角连接使用的 `DUJIAO_API_KEY`、`DUJIAO_API_SECRET`。生成的随机字符串可用于独角 Secret。

```bash
docker compose up -d --build
curl --fail http://127.0.0.1:8091/health
docker compose logs --tail=100 -f
```

Compose 将 `127.0.0.1:8091` 转发到容器的 `8080`，用 Caddy／Nginx 将桥接域名反向代理至该地址，并启用 HTTPS。数据库位于命名卷 `bridge-data` 中的 `/data/bridge.db`。

服务启动时自动迁移数据库，立即提供缓存目录并在后台刷新。首次刷新成功前，以及目录过期时，库存为零。`/health` 仅表示服务运行正常，不表示上游余额或目录一定可用。

### 3. 独角连接

在独角后台的“对接管理 → 连接管理”新建连接：

| 字段 | 配置 |
|---|---|
| 站点地址 | 桥接服务的 HTTPS 域名 |
| 协议类型 | Dujiao OpenAPI |
| API Key / Secret | 与桥接 `.env` 的 Dujiao 凭证相同 |
| 回调地址 | `https://你的独角域名/api/v1/upstream/callback` |
| 自动同步价格 | 开启；按需设置汇率、加价和舍入规则 |

测试连接后，在商品映射中同步商品。桥接返回上游成本价，最终零售价由独角的连接规则决定。

## 配置项

| 环境变量 | 默认值 | 用途 |
|---|---|---|
| `STELLAR_BASE_URL` | `https://wholesale.stellarsecurity.com/api/v1` | 上游 API 地址 |
| `STELLAR_API_KEY` | 无 | 上游 Bearer 凭证 |
| `STELLAR_WEBHOOK_SECRET` | 无 | Webhook 验签 Secret |
| `DUJIAO_API_KEY` / `DUJIAO_API_SECRET` | 无 | 独角请求验证和回调签名 |
| `SITE_NAME` | `Stellar Wholesale eSIM` | 桥接站点名称 |
| `DB_PATH` | `/data/bridge.db` | SQLite 文件，父目录必须存在且可写 |
| `TOP_PLAN_LIMIT` | `10` | 入选套餐上限；大于零时限制数量，零或负数不限制 |
| `SYNC_INTERVAL_SECONDS` | `60` | 全量目录同步调度间隔，单位秒 |
| `STELLAR_PAGINATION_DELAY_SECONDS` | `1.5` | 分页之间的等待时间，单位秒 |
| `WALLET_CACHE_SECONDS` | `30` | 普通余额查询缓存时间，单位秒；采购和目录更新强制刷新 |
| `PRODUCT_WALLET_TIMEOUT_SECONDS` | `15` | 商品同步等待余额的最长时间，单位秒；临时失败返回 503，避免误写零库存 |
| `HTTP_TIMEOUT_SECONDS` | `30` | 上游请求超时，单位秒；通知超时最多 10 秒 |
| `CATALOGUE_MIN_ACTIVE_RATIO` | `0.5` | 目录保护阈值；新快照的可用套餐少于上次的该比例时拒绝应用，保留旧目录。设为 `0` 关闭 |
| `CATALOGUE_MAX_PAGES` | `50` | 单次全量拉取的最大页数，防止分页元数据异常导致无界循环 |
| `REQUIRE_DOWNSTREAM_ORDER_NO` | `1` | 下单必须携带 `downstream_order_no`；缺少时无法构造幂等键，重复请求会重复采购 |
| `MAX_REQUEST_BODY_BYTES` | `1048576` | 请求体上限，超限返回 413，避免超大请求在验签前占满内存 |
| `CALLBACK_MAX_ATTEMPTS` | `20` | 单条交付通知的最大重试次数；超过后写入 `failed_at` 停止重试并告警 |
| `WEBHOOK_MAX_ATTEMPTS` | `20` | 单条 Webhook 事件的最大处理次数；超过后写入 `failed_at` 并停止重试 |
| `QUEUE_RETENTION_DAYS` | `30` | 已送达／已处理／已放弃的队列记录保留天数，最小 1 |
| `QUEUE_PRUNE_INTERVAL_SECONDS` | `3600` | 清理已结束队列记录的间隔，单位秒，最小 60 |
| `ESIM_EVENT_SWEEP_LIMIT` | `20` | `esim.*` 事件无法定位订单时，回刷在途订单的上限 |
| `ESIM_EVENT_SWEEP_COOLDOWN_SECONDS` | `5` | 上述回刷的最小间隔，单位秒；设为 0 关闭限速 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `LOG_FORMAT` | `text` | `text` 或 `json`；JSON 每个日志一行并带 `order_id`／状态迁移等结构化字段 |
| `FULFILLMENT_RETENTION_DAYS` | `0` | 交付内容（二维码／激活码）保留天数；`0` 永久保留。设为正数后只清理已交付／已取消／已失败且超期的订单内容 |

应用从进程环境读取配置；Compose 自动加载 `.env`。本地直接运行时需要自行设置环境变量。采用单实例、单 Uvicorn worker：进程内锁负责采购、目录和队列串行化，不能直接扩展为多 worker／多副本。

## 商品筛选与展示

全量请求使用 `country_code=CN` 和 `destination=China` 拉取全部分页，再本地筛选。供应商的目的地过滤可能是模糊匹配，因此本地仍要求目的地去除空白后、不区分大小写地精确匹配 `China`，排除 `China mainland`。

筛选顺序：

1. 保留上游可用、仅覆盖 `CN`、EUR 计价且可计算有限每 GB 单价的套餐。计算使用 `1024 MB = 1 GB`。
2. 按“余额减去尚未确认扣款的订单预留”剔除买不起至少一张的套餐。
3. 按每 GB 单价排序；相同流量、有效期及完整服务条件只保留最便宜的一款。完整服务条件包括线路、出口、运营商网络、流量类型、速率、激活、充值、热点及限速条件。
4. 在完整服务条件相同的候选中，另一款流量不少、有效期不短，且流量或有效期更优、加价不超过 1% 时，优先淘汰较差项。容差由 `PLAN_UPGRADE_PRICE_TOLERANCE` 配置（默认 `0.01`）。
5. 固定选品模型 **portfolio-v1**：默认 10 档，先为香港、非香港出口各保留一档低总价短期套餐（≤7 天），再各保留一档低每 GB 单价长效套餐（≥30 天），最多占 4 个名额。若某线路缺少短期或长效套餐，仍尽量保留该线路。短期保底档允许保留，以免被近价长效档全部替代。
6. 其余名额按每 GB 单价优先补齐。每次只比较当前最低每 GB 单价的 **5%** 范围内候选，评分为 `每GB单价 / (1 + 0.15 × min(有效天数, 90) / 90)`，越低越优；超过 5% 的单价差不能靠时间权重反超。低价短期保底先限制在最低总价的 5% 内，再使用相同评分挑选。评分并列时依次按每 GB 单价、总价、更长天数、SKU 和 ID 决定，不依赖上游返回顺序。
7. 合格候选充足时选满 10 档；余额不足、线路缺货或去重后合格候选不足时少选，不虚构库存、不采购超预算套餐，也不为凑数重新加入较差重复项。`TOP_PLAN_LIMIT` 仍控制部署上限；设置为零或负数仅输出去除较差项后的全部候选，不执行十档组合。

线路优先使用上游 `coverage.breakout_ip_country_code`：`HK` 为香港，`SG` 为新加坡，其余出口标为非香港。只有缺少该字段时才回退到名称、SKU 或 slug 中的 `nonhkip` 标记（新加坡），否则回退为香港。这是上游声明或兼容性标注，并非实际出口 IP 检测。组合配额按香港／非香港两组计算，完整服务条件仍分别去重和比较。

**筛选按每 GB 单价，展示按套餐总价升序。** 桥接响应按总价排列；独角原生同步会保留已有 `sort_order`，不会自动以响应顺序重排。当前独角部署按 `sort_order` 从高到低展示。安装 [自动排序工具](ops/sku-sort/README.md) 后，在 SKU 新增、改价、上下架和删除的同一数据库事务内，按独角实际零售价立即重排指定商品；每 15 秒检查作为兜底；最便宜的一档获得最高排序值。

已发布的套餐不会直接从同步响应消失。停售、预算不足或不再入选时返回零库存和 `is_active=false`；独角应使用包含下架商品的同步结果更新 SKU。无可计算流量的无限套餐不会参与当前排名；若每日套餐符合筛选条件，则按其默认天数销售，独角下单接口不支持另选天数。

## 同步、余额与库存

桥接默认每分钟调度一次 China 目录全量更新；任务串行且合并，上一轮未完成时不会重叠执行，因此不是每 60 秒保证完成。`catalogue.updated` 先入库再应答，通过 `ids[]` 只拉取事件指定的 1–100 个套餐；指定 ID 在完整成功响应中缺失才视为停售，再用本地目录重新排名。全量刷新标记触发目录修复，周期全量扫描也修复漏事件。增量更新不延长全量目录的有效期。

库存按以下公式逐个 SKU 计算，各档共享同一个钱包，不能把各档库存相加作为总购买能力：

```text
可售数量 = floor(max(0, 可用余额 - 未确认扣款的订单预留) / 套餐成本)
```

已取得上游订单号的订单不再重复预占余额。目录更新无法取得有效 EUR 余额时保留上一份目录。商品余额查询最长等待 15 秒（可配置），超时或余额无效时返回 503，让独角保留上次库存并重试同步，不把未知余额当作零余额；真实余额不足仍返回零库存。只读 GET 请求遇到网络错误时等待 1 秒重试一次，采购 POST 不新增网络重试。下单仍强制查询实时余额，全量目录超过 `max(同步间隔 × 2 + 120 秒, 300 秒)` 未更新时仍返回零库存、暂停销售。默认目录有效期为 5 分钟。

全量刷新要求分页完整：`data` 必须是数组，`meta.last_page` 必须有效且不超过 `CATALOGUE_MAX_PAGES`，其中任何一页提前返回空都会被视为不完整而放弃本次更新。空响应、缺少 `data` 或可用套餐少于上次的 `CATALOGUE_MIN_ACTIVE_RATIO`（默认 50%）时同样停止应用并保留旧目录，避免上游故障或灰度瞬间把全部 SKU 清零。放弃更新不延长目录有效期，旧快照到期后仍按上面的规则返回零库存、暂停销售，不会拿不可信的目录继续卖。确认是真实大规模下架时，可临时下调该比例后重启。

桥接刷新与独角商品同步是两层任务。独角内置任务保持每 5 分钟运行；需要每分钟更新商店价格和库存时，按 [同步工具说明](ops/minute-sync/README.md) 安装辅助 timer。辅助工具只投递原生同步任务，不处理订单和退款。

## 订单、交付与退款

### 采购与交付

- 下游订单号幂等：重复请求返回同一订单；同一订单号更改 SKU 或数量会被拒绝，每次最多 10 张。因缺少单号就无法构造幂等键，默认要求 `downstream_order_no`（`REQUIRE_DOWNSTREAM_ORDER_NO`）。
- 订单回调地址必须为公网 HTTPS（拒绝明文、回环、私网、链路本地与 `.internal` 域名），否则下单返回 `invalid_callback_url`，避免把签名回调发向内网。
- 首次采购前读取 `/plans/{id}` 核验可用性、目的地、价格和规格，再强制刷新余额。价格高于下单时记录的成本、规格变化或余额不足时停止采购。
- 请求参数和 `Idempotency-Key` 在发起采购前持久保存。超时、临时 409 等不确定结果只重试原请求；明确的幂等冲突和永久错误进入失败状态。失败、已交付和已取消订单不会被强制刷新重新采购。
- 订单通过 Webhook 和轮询推进。普通轮询间隔随订单年龄为 5／15／30 秒，人工审核状态为 5 分钟；实际调度受请求耗时影响。
- `order.*` 事件按 `order_id` 直接回刷对应订单。`esim.*` 事件按 `order_id`／`sim_id` 反查订单；查不到时以 `ESIM_EVENT_SWEEP_LIMIT` 为上限、`ESIM_EVENT_SWEEP_COOLDOWN_SECONDS` 为最小间隔回刷在途订单，所以事件突发不会放大成上游请求风暴。
- 只有全部购买数量的 eSIM 已 fulfilled 且有 customer link、二维码或激活码时才确认交付。缺少安装信息会读取单卡详情补齐；`vpn_failed` 在 eSIM 完整可用时可以交付。
- 交付内容保存到 `fulfillment.payload` 和 `fulfillment.delivery_data.esims[]`。状态变更与待发送回调在同一事务中写入 SQLite；通知失败持续重试，间隔从 5 秒增加到最多 5 分钟，重启后恢复；超过 `CALLBACK_MAX_ATTEMPTS`（默认 20 次）后写入 `failed_at` 停止重试，保留记录供人工处理。网络应答丢失可能产生重复通知，接收端需幂等处理。回调结构为 `{event, order_id, order_no, downstream_order_no, status, fulfillment?, timestamp}`，其中 `status` 与查询接口一致（内部 `creating` 统一上报为 `fulfilling`），不会出现内部状态值。

### 取消与退款

取消使用独立流程，不会触发采购。尚未发起采购的订单可直接取消；已请求采购但尚无上游订单号时停止自动采购并进入人工核对。

已采购订单按 `sim_id` 逐张取消，要求供应商允许取消未使用 eSIM。逐张保存 `provider_cancelled`、`refunded` 和实际退款金额；重试跳过已确认取消并退款的卡。临时错误 60 秒后继续核对；明确拒绝进入人工审核。部分成功时保留累计退款，全部确认后才标记 canceled。桥接记录的是上游钱包退款，客户支付渠道退款仍由独角及其支付流程处理。

## HTTP 接口

除健康检查和 Stellar Webhook 外，接口均要求 Dujiao HMAC 认证（含 `/status`）。签名实现见 `app/security.py`，时间戳允许偏差 60 秒；Stellar Webhook 按原始请求体验签，允许偏差 300 秒。所有请求体上限为 `MAX_REQUEST_BODY_BYTES`（默认 1 MB），超限在验签前返回 413。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 进程健康检查 |
| POST | `/api/v1/upstream/ping` | 连接检查、余额与检查时间 |
| GET | `/api/v1/upstream/categories` | 分类 |
| GET | `/api/v1/upstream/status` | 运行状态：目录年龄与套餐数、订单积压、队列积压与死信、钱包缓存；不访问上游 |
| GET | `/api/v1/upstream/products` | 商品列表，支持 `include_inactive=true` |
| GET | `/api/v1/upstream/products/{product_id}` | 商品详情 |
| POST | `/api/v1/upstream/orders` | 创建／查询幂等订单；要求 `downstream_order_no` 与公网 HTTPS `callback_url` |
| GET | `/api/v1/upstream/orders/{order_id}` | 查询订单及交付 |
| POST | `/api/v1/upstream/orders/{order_id}/cancel` | 取消与上游退款核对 |
| POST | `/webhooks/stellar` | 验签、持久保存事件并应答 |

## 测试与开发

Python 3.12 与 Docker 镜像一致（`pyproject.toml` 声明 `requires-python >= 3.12`）。测试使用临时 SQLite 和模拟上游，不需要真实 API Key，也不会采购或退款。

```bash
# 创建 3.12 环境（也可用 uv：uv venv --python 3.12 .venv）
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e ".[dev]"    # ruff 等开发依赖
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/mypy
.venv/bin/python -m unittest discover -s tests -v
```

测试统一使用标准库 `unittest`。Go 同步工具的构建检查见其 README。

### 依赖锁定

`pyproject.toml` 是依赖的唯一来源，`requirements.txt` 是由它生成的锁定文件（Docker 与 CI 都只安装锁定版本）。修改依赖后重新生成并提交：

```bash
uv pip compile pyproject.toml -o requirements.txt --python-version 3.12 --universal --no-header
```

CI（`.github/workflows/ci.yml`）会在 Python 3.12 上重算锁定文件并 `diff`，同时跑 `ruff check`、`unittest`、Go 工具的 `build/vet/test`，并构建镜像验证容器能起来且以非 root 运行。

## 运维与故障排查

升级前备份 SQLite，然后执行 `docker compose up -d --build`。在线备份应使用 SQLite backup API，不要只复制正在写入的 `.db` 文件；还原时先停止服务。不要执行会删除数据库卷的 `docker compose down -v`。

镜像以非 root（uid／gid `10001`）运行，`/data` 在镜像内已授予该用户。**从旧版本升级时**，已有命名卷内部仍属 root，需要一次性改属主，否则启动会报数据库无法打开：

```bash
docker compose run --rm --user root --entrypoint chown stellar-dujiao-bridge -R 10001:10001 /data
```

镜像自带 `HEALTHCHECK`（30 秒间隔探测 `/health`），`docker compose ps` 可直接看到健康状态。

```bash
docker compose exec -T stellar-dujiao-bridge python - <<'PY'
import sqlite3
from datetime import datetime, timezone
from app.main import DB_PATH
backup = DB_PATH + '.backup-' + datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
with sqlite3.connect(DB_PATH) as source, sqlite3.connect(backup) as target:
    source.backup(target)
print(backup)
PY
```

备份文件仍位于同一个数据卷，应另行保存到卷外存储。回滚程序版本也要同时评估数据库迁移；完整恢复使用升级前的程序与数据库备份。

| 现象 | 排查方向 |
|---|---|
| 库存为零 | `/ping` 余额、`/status` 的目录年龄与钱包缓存、EUR 币种、订单预留和供应商请求错误 |
| 价格没有更新 | 桥接目录日志、独角自动同步价格设置、辅助 timer 和原生任务队列 |
| 已采购但没有发货 | 上游数量及安装信息是否完整、订单状态、`callback_outbox` 中 `delivered_at` 与 `failed_at` 为空的记录 |
| 交付通知一直未送达 | `callback_outbox.failed_at` 非空即为已放弃（默认 20 次后），结合 `last_error`、下游回调地址与证书排查后按需重发 |
| 取消停在人工审核 | `cancel_error`、单卡取消／退款记录、上游实际订单及钱包流水 |
| SKU 顺序错误 | 自动排序 timer、配置的商品 ID 与数据库路径，以及独角 `sort_order` 展示方向 |

数据库迁移按 `PRAGMA user_version` 顺序执行、每步幂等，旧库（`user_version=0`）启动时原地升级并回填缺失字段；新增字段只需在 `app/util.py` 追加一个迁移步骤。持久化表包括 `plans`、`orders`、`bridge_state`、`webhook_events` 和 `callback_outbox`，轮询、队列与排序热路径均有索引（`idx_orders_poll`、`idx_orders_stellar`、`idx_outbox_due`、`idx_webhook_due` 等），启动时幂等创建。日志保留供应商错误的 `X-Request-ID` 以便支持人员查询。`callback_outbox` 与 `webhook_events` 在有界重试后进入死信（`failed_at`），已结束记录按 `QUEUE_RETENTION_DAYS`（默认 30 天）由后台每小时清理一次，无需手工维护；死信记录同样按此期限删除，需长期留存的应在清理前导出。修改订单或删除幂等记录前必须先核对供应商账务。

收到 SIGTERM 时先取消后台任务（目录同步、订单轮询、队列与清理循环）并等待在途请求结束，再关闭上游连接；Compose 的默认停止宽限期足够，但不要用 `kill -9`。

`.env`、数据库、备份与安装信息都包含敏感数据，不要提交 Git。仅让反向代理访问服务端口，妥善保护独角 API 凭证和 `/ping` 的余额信息。

## 许可证

[MIT](LICENSE)
