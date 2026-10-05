import asyncio
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import main as bridge


class CatalogueLifecycleTests(unittest.TestCase):
    def test_country_filter_is_sent_on_every_page(self):
        async def run():
            client = bridge.StellarClient()
            with patch.object(client, "request", AsyncMock(return_value={})) as request:
                await client.list_plans(page=2, per_page=100)
                self.assertEqual(request.call_args.kwargs["params"],
                                 {"page": 2, "per_page": 100, "country_code": "CN"})
        asyncio.run(run())

    def test_lowest_five_without_price_ceiling(self):
        plans = [
            {"id": str(i), "available": True, "coverage": {"codes": ["CN"]},
             "data": {"megabytes": 1024}, "validity_days": i, "price": {"amount": str(i), "currency": "EUR"}}
            for i in range(1, 8)
        ]
        plans += [dict(plans[0], id="regional", coverage={"codes": ["CN", "HK"]}),
                  dict(plans[0], id="disabled", available=False),
                  dict(plans[0], id="expensive_duplicate", price={"amount": "9", "currency": "EUR"})]
        with patch.object(bridge, "TOP_PLAN_LIMIT", 5):
            self.assertEqual(bridge.best_mainland_plan_ids(plans), {"1", "2", "3", "4", "5"})

    def test_published_sku_survives_price_increase_and_removal(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            bridge, "DB_PATH", str(Path(directory) / "bridge.db")
        ):
            bridge.init_db()
            raw = {
                "id": "cn-plan", "sku": "CN-30", "available": True,
                "coverage": {"codes": ["CN"]},
                "data": {"megabytes": 30720, "label": "30GB"},
                "duration": {"default_days": 20}, "validity_days": 20,
                "price": {"amount": "5.37", "currency": "EUR"},
            }
            bridge.upsert_plans([raw])
            original_id = bridge.available_plan_rows()[0]["id"]
            with bridge.db_connect() as db:
                db.execute("INSERT INTO bridge_state VALUES ('catalogue_synced_at',?)", (bridge.iso(),))
            wallet = {"available_balance": Decimal("40"), "currency": "EUR"}
            self.assertEqual(bridge.wallet_capacity_for_plan(bridge.available_plan_rows()[0], wallet, Decimal(0)), 7)
            raw["price"]["amount"] = "7.19"
            raw["available"] = False
            bridge.upsert_plans([raw])
            rows = bridge.available_plan_rows()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], original_id)
            self.assertEqual(rows[0]["available"], 0)
            self.assertEqual(bridge.wallet_capacity_for_plan(rows[0], wallet, Decimal(0)), 0)
            bundle = bridge.product_bundle(rows, wallet)
            self.assertFalse(bundle["is_active"])
            self.assertFalse(bundle["skus"][0]["is_active"])
            self.assertEqual(bundle["skus"][0]["price_amount"], "7.19")
            self.assertEqual(bundle["skus"][0]["stock_quantity"], 0)
            async def responses():
                with patch.object(bridge, "product_wallet_snapshot", AsyncMock(return_value=wallet)):
                    public = await bridge.products(include_inactive=False)
                    sync = await bridge.products(include_inactive=True)
                    detail = await bridge.product(bridge.PRODUCT_BUNDLE_ID)
                self.assertEqual(public["items"], [])
                self.assertTrue(sync["includes_inactive"])
                self.assertEqual(sync["total"], 1)
                self.assertEqual(detail["product"], sync["items"][0])
            asyncio.run(responses())
            bridge.upsert_plans([])
            self.assertEqual(len(bridge.available_plan_rows()), 1)
            # When eligibility returns, preserve the original downstream mapping.
            raw["price"]["amount"] = "5.37"
            raw["available"] = True
            bridge.upsert_plans([raw])
            row = bridge.available_plan_rows()[0]
            self.assertEqual(row["id"], original_id)
            self.assertEqual(row["available"], 1)
            bridge.upsert_plans([])
            self.assertEqual(bridge.available_plan_rows()[0]["available"], 0)
            bridge.upsert_plans([raw])
            with bridge.db_connect() as db:
                db.execute("DELETE FROM bridge_state")
            self.assertEqual(bridge.wallet_capacity_for_plan(row, wallet, Decimal(0)), 0)


if __name__ == "__main__":
    unittest.main()
