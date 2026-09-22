# Stellar Wholesale → Dujiao-Next eSIM Bridge

这是一个独立的 eSIM 桥接服务，只发布覆盖代码严格为 `CN` 的中国大陆套餐，并按价格/GB 只保留最便宜的 5 个套餐：

```text
Dujiao-Next ──Dujiao HMAC API──> 本服务 ──Bearer API──> Stellar Wholesale
                                      ^
                                      └── Stellar Webhook
```

只实现 eSIM，不实现 VPN、Antivirus 和 eSIM Top-up。

## 1. Stellar 准备

在 Stellar Wholesale：

1. `Developer → API keys → Create API key`
2. 至少选择：
   - `plans:read`
   - `wallet:read`
   - `orders:create`
   - `orders:read`
   - `esims:read`
   - `webhooks:manage`
3. 充值 Wholesale wallet。
   如果要让独角尝试取消未使用 eSIM，再额外选择 `esims:cancel`。
4. `Settings → Webhooks` 创建公网 HTTPS Endpoint：

   ```text
   https://你的桥接域名/webhooks/stellar
   ```

   建议事件：

   ```text
   catalogue.updated
   order.fulfilled
   order.failed
   esim.fulfilled
   esim.failed
   ```

   保留 `esim.*` 便于多张卡订单及时更新。

5. 保存 Webhook signing secret。它只在创建或轮换时显示。

## 2. 配置并启动

```bash
cp .env.example .env
openssl rand -hex 32
```

把 Stellar API Key、Webhook secret 和一组 Dujiao 凭证写入 `.env`：

```dotenv
STELLAR_API_KEY=...
STELLAR_WEBHOOK_SECRET=whsec_...
DUJIAO_API_KEY=bridge-esim-key
DUJIAO_API_SECRET=...
```

启动：

```bash
docker compose up -d --build
docker compose logs -f
```

健康检查：

```bash
curl http://127.0.0.1:8091/health
```

本部署包默认将宿主机端口绑定为 `127.0.0.1:8091`，因为独角通常已经占用 `8080`；通过域名访问时由 Caddy 反向代理。

生产环境请在前面配置 Nginx/Caddy，并使用 HTTPS。Stellar Webhook 要求公网 HTTPS。

## 3. 在独角中配置

在独角后台：`对接管理 → 连接管理 → 新增`：

| 字段 | 值 |
|---|---|
| 站点地址 | `https://你的桥接域名`，不要填 Stellar 地址 |
| 协议类型 | Dujiao OpenAPI |
| API Key | `.env` 中的 `DUJIAO_API_KEY` |
| API Secret | `.env` 中的 `DUJIAO_API_SECRET` |
| 回调地址 | 独角公网地址 + `/api/v1/upstream/callback` |

点击“测试连接”，成功后到“商品映射”同步商品。

## 4. 设计约束

- 5 个入选的 Stellar plan 合并为独角中的 1 个商品，并以 5 个 SKU 提供选择。
- 仅保留 `coverage.codes == ["CN"]` 的中国大陆套餐；港澳、亚洲及其他多区域套餐不会发布到独角。
- 在中国大陆套餐中按 Stellar 价格除以流量 GB 排序，只对外提供最低的 5 个；相同价格/GB 时保持 Stellar 返回顺序。
- Stellar 的 Daily Unlimited 套餐按默认天数销售，因为独角标准上游下单接口没有 `days` 字段。
- 商品价格按 Stellar 计划价格返回，独角连接中的汇率、加价和舍入规则负责最终售价。
- 商品交付类型固定为 `auto`。
- 交付内容会放在 `fulfillment.payload` 和 `fulfillment.delivery_data.esims[]`，包含 customer link、二维码地址、激活码、APN 和 `sim_id`。
- Stellar 订单是异步的：本服务同时使用 Webhook 和轮询；重复下单使用同一个 `Idempotency-Key`。
- Stellar 取消只适用于符合条件的未使用 eSIM；已使用 eSIM、尚未交付的订单和不满足供应商规则的订单会返回 `cancel_not_allowed`。

## 5. 批发余额与库存控制

- 服务定期读取 Stellar Wholesale 的 `/wallet` 余额，默认缓存 30 秒。
- 每个 SKU 的可售库存按“可用余额减去未完成订单预占金额”除以上游套餐成本计算，并向下取整。
- 余额读取失败时服务采取安全失败策略，将库存视为 0，避免独角继续售出无法履约的订单。
- 创建订单前会在锁内重新读取余额并再次校验库存；余额不足时返回 `409 insufficient_wholesale_balance`，不会创建上游订单。
- `/api/v1/upstream/ping` 返回当前余额、币种和检查时间，建议只在受保护的管理网络中访问，不要把余额信息直接展示给游客。

## 6. 安全

- Stellar API Key 只放在服务端 `.env`，不要提交 Git 或写进前端。
- Dujiao 的 API Secret 只用于桥接接口签名和回调签名。
- SQLite 数据库必须保存在 Docker volume 中，不能把 `/data` 指向临时目录。
- 建议将 `DUJIAO_API_SECRET` 设置为至少 32 字节随机字符串，并限制 8080 端口只允许反向代理访问。
