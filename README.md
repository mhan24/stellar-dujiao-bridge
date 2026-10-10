# Stellar Wholesale → Dujiao-Next eSIM Bridge

将 Stellar Wholesale eSIM 接入 Dujiao-Next：桥接服务提供独角上游协议，把符合筛选规则的套餐合并成一个商品、多档 SKU，并处理采购、交付通知和取消退款。

```text
Dujiao-Next ── HMAC API ──> Bridge ── Bearer API ──> Stellar Wholesale
     ↑                       ↑
     └── 持久化交付通知 ───────┘── Stellar Webhook
```

支持 eSIM 新购及供应商允许的取消；没有 VPN、Antivirus 或 eSIM Top-up 的独立销售接口。Stellar 随卡赠送的 VPN 不作为 eSIM 发货的必要条件。

## 项目结构

| 文件 | 职责 |
|---|---|
| `app/main.py` | 配置、SQLite 迁移、Stellar 客户端、目录筛选、库存、订单、持久化队列和 HTTP 接口 |
| `app/security.py` | Dujiao 请求／通知签名和 Stellar Webhook 验签 |
| `tests/test_catalogue.py` | 目的地、线路、去重、预算、性价比与停售规则 |
| `tests/test_reliability.py` | 采购幂等、报价保护、完整交付、通知恢复、增量同步及部分退款 |
| `tests/test_security.py` | 签名验证 |
| `ops/minute-sync/` | 每分钟向独角原生队列投递价格／库存同步任务的 Go 工具和 systemd 配置 |
| `.env.example` | 配置模板，不含真实凭证 |
| `docker-compose.yml` | 单实例服务、端口绑定与持久化数据库卷 |

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
| `HTTP_TIMEOUT_SECONDS` | `30` | 上游请求超时，单位秒；通知超时最多 10 秒 |
| `LOG_LEVEL` | `INFO` | 日志级别 |

应用从进程环境读取配置；Compose 自动加载 `.env`。本地直接运行时需要自行设置环境变量。采用单实例、单 Uvicorn worker：进程内锁负责采购、目录和队列串行化，不能直接扩展为多 worker／多副本。

## 商品筛选与展示

全量请求使用 `country_code=CN` 和 `destination=China` 拉取全部分页，再本地筛选。供应商的目的地过滤可能是模糊匹配，因此本地仍要求目的地去除空白后、不区分大小写地精确匹配 `China`，排除 `China mainland`。

筛选顺序：

1. 保留上游可用、仅覆盖 `CN`、EUR 计价且可计算有限每 GB 单价的套餐。计算使用 `1024 MB = 1 GB`。
2. 按“余额减去尚未确认扣款的订单预留”剔除买不起至少一张的套餐。
3. 按每 GB 单价排序；相同流量、有效期和线路只保留最便宜的一款。
4. 同线路、同流量类型内，若另一款流量不少、有效期不短、价格不高，且至少一项更优，剔除被其替代的套餐。
5. 从剩余套餐中取每 GB 单价最低的前 `TOP_PLAN_LIMIT` 个。没有 €0.18/GB 等固定单价门槛。

名称、SKU 或 slug 含 `nonhkip` 的套餐标为“新加坡”，其余标为“香港”。两种线路分别去重和比较，共用前 10 个名额。此标注是业务规则，不代表检测了实际出口 IP。

**筛选按每 GB 单价，展示按套餐总价升序。** 桥接响应按总价排列；独角原生同步会保留已有 `sort_order`，不会自动以响应顺序重排。当前独角部署按 `sort_order` 从高到低展示，新增或调整 SKU 后需要在独角维护排序，最便宜的一档放在最前面。

已发布的套餐不会直接从同步响应消失。停售、预算不足或不再入选时返回零库存和 `is_active=false`；独角应使用包含下架商品的同步结果更新 SKU。无可计算流量的无限套餐不会参与当前排名；若每日套餐符合筛选条件，则按其默认天数销售，独角下单接口不支持另选天数。

## 同步、余额与库存

桥接默认每分钟调度一次 China 目录全量更新；任务串行且合并，上一轮未完成时不会重叠执行，因此不是每 60 秒保证完成。`catalogue.updated` 先入库再应答，通过 `ids[]` 只拉取事件指定的 1–100 个套餐；指定 ID 在完整成功响应中缺失才视为停售，再用本地目录重新排名。全量刷新标记触发目录修复，周期全量扫描也修复漏事件。增量更新不延长全量目录的有效期。

库存按以下公式逐个 SKU 计算，各档共享同一个钱包，不能把各档库存相加作为总购买能力：

```text
可售数量 = floor(max(0, 可用余额 - 未确认扣款的订单预留) / 套餐成本)
```

已取得上游订单号的订单不再重复预占余额。目录更新无法取得有效 EUR 余额时保留上一份目录；余额查询失败、商品余额查询超过 5 秒，或全量目录超过 `max(同步间隔 × 2 + 120 秒, 300 秒)` 未更新时暂停销售。默认目录有效期为 5 分钟。

桥接刷新与独角商品同步是两层任务。独角内置任务保持每 5 分钟运行；需要每分钟更新商店价格和库存时，按 [同步工具说明](ops/minute-sync/README.md) 安装辅助 timer。辅助工具只投递原生同步任务，不处理订单和退款。

## 订单、交付与退款

### 采购与交付

- 下游订单号幂等：重复请求返回同一订单；同一订单号更改 SKU 或数量会被拒绝，每次最多 10 张。
- 首次采购前读取 `/plans/{id}` 核验可用性、目的地、价格和规格，再强制刷新余额。价格高于下单时记录的成本、规格变化或余额不足时停止采购。
- 请求参数和 `Idempotency-Key` 在发起采购前持久保存。超时、临时 409 等不确定结果只重试原请求；明确的幂等冲突和永久错误进入失败状态。失败、已交付和已取消订单不会被强制刷新重新采购。
- 订单通过 Webhook 和轮询推进。普通轮询间隔随订单年龄为 5／15／30 秒，人工审核状态为 5 分钟；实际调度受请求耗时影响。
- 只有全部购买数量的 eSIM 已 fulfilled 且有 customer link、二维码或激活码时才确认交付。缺少安装信息会读取单卡详情补齐；`vpn_failed` 在 eSIM 完整可用时可以交付。
- 交付内容保存到 `fulfillment.payload` 和 `fulfillment.delivery_data.esims[]`。状态变更与待发送回调在同一事务中写入 SQLite；通知失败持续重试，间隔从 5 秒增加到最多 5 分钟，重启后恢复。网络应答丢失可能产生重复通知，接收端需幂等处理。

### 取消与退款

取消使用独立流程，不会触发采购。尚未发起采购的订单可直接取消；已请求采购但尚无上游订单号时停止自动采购并进入人工核对。

已采购订单按 `sim_id` 逐张取消，要求供应商允许取消未使用 eSIM。逐张保存 `provider_cancelled`、`refunded` 和实际退款金额；重试跳过已确认取消并退款的卡。临时错误 60 秒后继续核对；明确拒绝进入人工审核。部分成功时保留累计退款，全部确认后才标记 canceled。桥接记录的是上游钱包退款，客户支付渠道退款仍由独角及其支付流程处理。

## HTTP 接口

除健康检查和 Stellar Webhook 外，接口均要求 Dujiao HMAC 认证。签名实现见 `app/security.py`，时间戳允许偏差 60 秒；Stellar Webhook 按原始请求体验签，允许偏差 300 秒。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 进程健康检查 |
| POST | `/api/v1/upstream/ping` | 连接检查、余额与检查时间 |
| GET | `/api/v1/upstream/categories` | 分类 |
| GET | `/api/v1/upstream/products` | 商品列表，支持 `include_inactive=true` |
| GET | `/api/v1/upstream/products/{product_id}` | 商品详情 |
| POST | `/api/v1/upstream/orders` | 创建／查询幂等订单 |
| GET | `/api/v1/upstream/orders/{order_id}` | 查询订单及交付 |
| POST | `/api/v1/upstream/orders/{order_id}/cancel` | 取消与上游退款核对 |
| POST | `/webhooks/stellar` | 验签、持久保存事件并应答 |

## 测试与开发

Python 3.12 与 Docker 镜像一致。测试使用临时 SQLite 和模拟上游，不需要真实 API Key，也不会采购或退款。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

测试统一使用标准库 `unittest`。Go 同步工具的构建检查见其 README。

## 运维与故障排查

升级前备份 SQLite，然后执行 `docker compose up -d --build`。在线备份应使用 SQLite backup API，不要只复制正在写入的 `.db` 文件；还原时先停止服务。不要执行会删除数据库卷的 `docker compose down -v`。

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
| 库存为零 | `/ping` 余额、目录同步时间、EUR 币种、订单预留和供应商请求错误 |
| 价格没有更新 | 桥接目录日志、独角自动同步价格设置、辅助 timer 和原生任务队列 |
| 已采购但没有发货 | 上游数量及安装信息是否完整、订单状态、`callback_outbox` 中未送达记录 |
| 取消停在人工审核 | `cancel_error`、单卡取消／退款记录、上游实际订单及钱包流水 |
| SKU 顺序错误 | 独角 `sort_order`，不能只看桥接返回的数组顺序 |

持久化表包括 `plans`、`orders`、`bridge_state`、`webhook_events` 和 `callback_outbox`。日志保留供应商错误的 `X-Request-ID` 以便支持人员查询。Webhook 和通知记录目前不自动清理，长期运行需安排保留策略；修改订单或删除幂等记录前必须先核对供应商账务。

`.env`、数据库、备份与安装信息都包含敏感数据，不要提交 Git。仅让反向代理访问服务端口，妥善保护独角 API 凭证和 `/ping` 的余额信息。

## 许可证

[MIT](LICENSE)
