# 独角每分钟价格／库存同步

此工具向 Dujiao-Next 使用的 Asynq Redis 队列投递 `upstream:sync_stock`，由独角原生 worker 执行。依赖独角当前版本仍使用该任务名、`default` 队列和相应 Redis 数据库；升级独角后应核对兼容性。

任务去重时间 55 秒、超时 45 秒、失败最多重试 1 次，完成记录保留 2 分钟。timer 在每分钟第 15 秒运行；独角内置 5 分钟任务可以保留。此工具不刷新 Stellar 目录、不采购、不退款，也不维护 SKU 排序。

## 构建和安装

需要 Go 1.24 或兼容版本。以下示例假设独角部署在 `/opt/dujiao-next`，服务名为 `dujiao-next.service`；路径或服务名不同需同时修改 systemd 配置。

```bash
cd ops/minute-sync
go build -o /tmp/dujiao-minute-sync .
sudo install -m 0755 /tmp/dujiao-minute-sync /opt/dujiao-next/dujiao-minute-sync
sudo install -m 0644 dujiao-minute-sync.service /etc/systemd/system/
sudo install -m 0644 dujiao-minute-sync.timer /etc/systemd/system/
```

创建 `/opt/dujiao-next/minute-sync.env`，使用与独角 Asynq worker 完全相同的 Redis 地址、密码和数据库编号。下面只是示例，不能直接假定独角使用数据库 1：

```dotenv
REDIS_ADDR=127.0.0.1:6380
REDIS_DB=1
REDIS_PASSWORD=
```

```bash
sudo chmod 600 /opt/dujiao-next/minute-sync.env
sudo systemctl daemon-reload
sudo systemctl start dujiao-minute-sync.service
sudo journalctl -u dujiao-minute-sync.service -n 20 --no-pager
sudo systemctl enable --now dujiao-minute-sync.timer
sudo systemctl list-timers dujiao-minute-sync.timer
```

`price and inventory sync queued` 只表示成功入队；还需要检查独角 worker 日志和商店价格／库存，确认任务实际执行。`sync already queued` 表示去重生效，不是失败。

## 检查与停用

```bash
go test ./...
go vet ./...
sudo systemctl disable --now dujiao-minute-sync.timer
```

停用辅助 timer 后，独角内置同步任务继续按自身配置运行。
