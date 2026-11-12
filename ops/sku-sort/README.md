# 自动按总价排列独角 SKU

独角同步保留旧 SKU 的 `sort_order`，新增 SKU 默认为零。桥接响应顺序不能保证商店展示顺序，此工具通过 SQLite 触发器，在指定商品 SKU 新增、改价、上下架或删除的同一事务内，按独角实际零售价立即重排；每 15 秒检查作为兜底。独角当前按 `sort_order` 降序展示，所以最便宜的档位获得最大的排序值；同价按 SKU ID 排序。

仅修改指定商品的有效、未删除 SKU 的 `sort_order`。价格、库存、上下架状态不变；排序已经正确时不写入。使用 SQLite 事务与原生同步串行，同步数据提交时排序同步提交，避免等待 timer 的窗口。适用于当前使用 SQLite 且按排序值降序展示的独角部署；不是通用的独角数据库接口。

## 安装

先用 SQLite backup API 备份数据库。示例假设独角位于 `/opt/dujiao-next`：

```bash
sudo install -m 0644 sort_skus.py /opt/dujiao-next/sort_skus.py
sudo install -m 0644 dujiao-sku-sort.service dujiao-sku-sort.timer /etc/systemd/system/
```

创建 `/opt/dujiao-next/sku-sort.env`，填写实际数据库位置及商品 ID（107 仅为当前部署示例）：

```dotenv
DUJIAO_DB_PATH=/opt/dujiao-next/db/dujiao.db
SORT_PRODUCT_ID=107
```

若数据库目录不同，同步调整 service 的 `ReadWritePaths`；若独角服务名或安装目录不同，调整 `After`、`EnvironmentFile` 和 `ExecStart`。

```bash
sudo chmod 600 /opt/dujiao-next/sku-sort.env
sudo env DUJIAO_DB_PATH=/opt/dujiao-next/db/dujiao.db SORT_PRODUCT_ID=107 python3 /opt/dujiao-next/sort_skus.py --install-triggers
sudo systemctl daemon-reload
sudo systemctl start dujiao-sku-sort.service
sudo systemctl enable --now dujiao-sku-sort.timer
sudo journalctl -u dujiao-sku-sort.service -n 20 --no-pager
```

需要关闭自动排序时，先停止 timer，再删除对应商品的三个触发器（商品 ID 不同需修改触发器名称）。卸载前备份数据库：

```bash
sudo systemctl disable --now dujiao-sku-sort.timer
```

```sql
DROP TRIGGER IF EXISTS bridge_sku_price_sort_107_insert;
DROP TRIGGER IF EXISTS bridge_sku_price_sort_107_update;
DROP TRIGGER IF EXISTS bridge_sku_price_sort_107_delete;
```

触发器只监听价格、商品归属、上下架与删除字段，更新 `sort_order` 不会递归触发自身。只影响配置的商品；更换商品 ID 后需要显式清理旧商品的触发器。独角升级若重建 SKU 表可能移除触发器，应重新安装并验证页面顺序，timer 在此期间仍可兜底。
