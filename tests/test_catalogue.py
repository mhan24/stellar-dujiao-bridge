import asyncio
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import api, catalogue, presentation, settings, stellar_client, util


class CatalogueLifecycleTests(unittest.TestCase):
    @staticmethod
    def portfolio_plan(id, gb, days, price, route="SG"):
        return {
            "id": id,
            "sku": id,
            "name": "China",
            "destination": "China",
            "available": True,
            "coverage": {"codes": ["CN"], "breakout_ip_country_code": route},
            "validity_days": days,
            "data": {"megabytes": gb * 1024},
            "price": {"amount": str(price), "currency": "EUR"},
        }

    def test_fixed_portfolio_reserves_both_routes_and_short_long_options(self):
        make = self.portfolio_plan
        anchors = [
            make("hk-short", 1, 7, ".51", "HK"),
            make("sg-short", 1, 7, ".51"),
            make("hk-long", 50, 30, "14.22", "HK"),
            make("sg-long", 50, 30, "14.22"),
        ]
        pool = anchors + [make(f"value-{i}", i, 20, Decimal(i) * Decimal(".27")) for i in range(3, 40)]
        with patch.object(settings, "TOP_PLAN_LIMIT", 10):
            selected = catalogue.best_mainland_plan_ids(pool, Decimal("20"))
            self.assertEqual(len(selected), 10)
            self.assertTrue({p["id"] for p in anchors} <= selected)
            self.assertEqual(selected, catalogue.best_mainland_plan_ids(list(reversed(pool)), Decimal("20")))
            limited = catalogue.best_mainland_plan_ids(pool, Decimal(".51"))
            self.assertEqual(limited, {"hk-short", "sg-short"})

    def test_validity_bonus_only_breaks_close_unit_price_choices(self):
        make = self.portfolio_plan
        cheap = make("cheap", 1, 10, "1.00", "HK")
        longer = make("longer", 1, 29, "1.02", "HK")
        with patch.object(settings, "TOP_PLAN_LIMIT", 1):
            self.assertEqual(catalogue.best_mainland_plan_ids([cheap, longer]), {"longer"})
            costly = make("costly", 1, 29, "1.06", "HK")
            self.assertEqual(catalogue.best_mainland_plan_ids([cheap, costly]), {"cheap"})

    def test_explicit_exit_ip_takes_precedence_over_name_marker(self):
        p = self.portfolio_plan("p", 1, 7, ".51", "SG")
        self.assertEqual(catalogue.plan_route_label(p), "新加坡")
        p["name"] = "China nonhkip"
        p["coverage"]["breakout_ip_country_code"] = "HK"
        self.assertEqual(catalogue.plan_route_label(p), "香港")

    def test_small_premium_upgrades_apply_to_all_allowances_and_validities(self):
        def plan(id, gb, days, price, **fields):
            return dict(
                id=id,
                name="China",
                destination="China",
                available=True,
                coverage={"codes": ["CN"]},
                validity_days=days,
                data={"megabytes": gb * 1024},
                price={"currency": "EUR", "amount": price},
                **fields,
            )

        short = plan("short", 50, 15, "14.21")
        long = plan("long", 50, 30, "14.22")
        with patch.object(settings, "TOP_PLAN_LIMIT", 0):
            self.assertEqual(catalogue.best_mainland_plan_ids([short, long]), {"long"})
            # The better package must still be affordable.
            self.assertEqual(catalogue.best_mainland_plan_ids([short, long], Decimal("14.21")), {"short"})
            # Apply the same rule to a data upgrade, at the exact 1% boundary.
            self.assertEqual(
                catalogue.best_mainland_plan_ids([plan("small", 3, 30, "1.00"), plan("large", 5, 30, "1.01")]),
                {"large"},
            )
            # Meaningfully different prices and service conditions remain choices.
            self.assertEqual(
                catalogue.best_mainland_plan_ids([plan("small", 3, 30, "1.00"), plan("large", 5, 30, "1.02")]),
                {"small", "large"},
            )
            self.assertEqual(catalogue.best_mainland_plan_ids([short, dict(long, speed="5G")]), {"short", "long"})
            self.assertEqual(
                catalogue.best_mainland_plan_ids([short, dict(long, name="China nonhkip")]), {"short", "long"}
            )

    def test_affordability_and_dominated_plans(self):
        def plan(id, gb, days, price, singapore=False):
            return {
                "id": id,
                "name": "China nonhkip" if singapore else "China",
                "destination": "China",
                "available": True,
                "coverage": {"codes": ["CN"]},
                "validity_days": days,
                "data": {"megabytes": gb * 1024},
                "price": {"currency": "EUR", "amount": price},
            }

        plans = [
            plan("20-short", 20, 20, "5.22"),
            plan("20-long", 20, 30, "5.10"),
            plan("100-expensive", 100, 60, "27.59"),
            plan("50-short", 50, 15, "13.66", True),
            plan("50-long", 50, 30, "11.23", True),
            plan("sg-short", 20, 20, "5.22", True),
        ]
        self.assertEqual(catalogue.best_mainland_plan_ids(plans, Decimal("18.52")), {"20-long", "50-long", "sg-short"})
        self.assertEqual(catalogue.best_mainland_plan_ids(plans, Decimal("5.10")), {"20-long"})
        self.assertEqual(catalogue.best_mainland_plan_ids(plans, Decimal("0")), set())

    def test_exact_china_and_distinct_routes(self):
        plan = {
            "id": "hk",
            "name": "China 50GB 30Days",
            "destination": "China",
            "available": True,
            "coverage": {"codes": ["CN"]},
            "validity_days": 30,
            "data": {"megabytes": 51200},
            "price": {"currency": "EUR", "amount": "11.23"},
        }
        singapore = dict(plan, id="sg", name="China 50GB 30Days (nonhkip)")
        mainland = dict(plan, id="mainland", destination="China mainland")
        duplicate = dict(plan, id="hk-expensive", price={"currency": "EUR", "amount": "15"})
        self.assertEqual(catalogue.best_mainland_plan_ids([plan, singapore, mainland, duplicate]), {"hk", "sg"})

    def test_route_label(self):
        self.assertEqual(catalogue.plan_route_label({"name": "China 50GB 30Days (nonhkip)"}), "新加坡")
        self.assertEqual(catalogue.plan_route_label({"name": "China 50GB 30Days"}), "香港")
        self.assertEqual(catalogue.plan_route_label({"sku": "CN-NONHKIP"}, "en-US"), "Singapore")

    def test_lowest_ten_unique_specs(self):
        plans = [
            {
                "id": str(i),
                "available": True,
                "destination": "China",
                "coverage": {"codes": ["CN"]},
                "data": {"megabytes": 1024},
                "validity_days": i,
                "price": {"amount": str(i), "currency": "EUR"},
            }
            for i in range(1, 13)
        ]
        plans.append(dict(plans[0], id="duplicate", price={"amount": "50", "currency": "EUR"}))
        with patch.object(settings, "TOP_PLAN_LIMIT", 10):
            self.assertEqual(catalogue.best_mainland_plan_ids(plans), {str(i) for i in range(1, 11)})

    def test_country_filter_is_sent_on_every_page(self):
        async def run():
            client = stellar_client.StellarClient()
            with patch.object(client, "request", AsyncMock(return_value={})) as request:
                await client.list_plans(page=2, per_page=100)
                self.assertEqual(
                    request.call_args.kwargs["params"],
                    {"page": 2, "per_page": 100, "country_code": "CN", "destination": "China"},
                )

        asyncio.run(run())

    def test_lowest_five_without_price_ceiling(self):
        plans = [
            {
                "id": str(i),
                "available": True,
                "destination": "China",
                "coverage": {"codes": ["CN"]},
                "data": {"megabytes": 1024},
                "validity_days": i,
                "price": {"amount": str(i), "currency": "EUR"},
            }
            for i in range(1, 8)
        ]
        plans += [
            dict(plans[0], id="regional", coverage={"codes": ["CN", "HK"]}),
            dict(plans[0], id="disabled", available=False),
            dict(plans[0], id="expensive_duplicate", price={"amount": "9", "currency": "EUR"}),
        ]
        with patch.object(settings, "TOP_PLAN_LIMIT", 5):
            self.assertEqual(catalogue.best_mainland_plan_ids(plans), {"1", "2", "3", "4", "5"})

    def test_published_sku_survives_price_increase_and_removal(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(settings, "DB_PATH", str(Path(directory) / "bridge.db")),
        ):
            util.init_db()
            raw = {
                "id": "cn-plan",
                "sku": "CN-30",
                "available": True,
                "destination": "China",
                "coverage": {"codes": ["CN"]},
                "data": {"megabytes": 30720, "label": "30GB"},
                "duration": {"default_days": 20},
                "validity_days": 20,
                "price": {"amount": "5.37", "currency": "EUR"},
            }
            catalogue.upsert_plans([raw])
            original_id = presentation.available_plan_rows()[0]["id"]
            with util.db_connect() as db:
                db.execute("INSERT INTO bridge_state VALUES ('catalogue_synced_at',?)", (util.iso(),))
            wallet = {"available_balance": Decimal("40"), "currency": "EUR"}
            self.assertEqual(
                catalogue.wallet_capacity_for_plan(presentation.available_plan_rows()[0], wallet, Decimal(0)), 7
            )
            raw["price"]["amount"] = "7.19"
            raw["available"] = False
            catalogue.upsert_plans([raw])
            rows = presentation.available_plan_rows()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], original_id)
            self.assertEqual(rows[0]["available"], 0)
            self.assertEqual(catalogue.wallet_capacity_for_plan(rows[0], wallet, Decimal(0)), 0)
            bundle = presentation.product_bundle(rows, wallet)
            self.assertFalse(bundle["is_active"])
            self.assertFalse(bundle["skus"][0]["is_active"])
            self.assertEqual(bundle["skus"][0]["price_amount"], "7.19")
            self.assertEqual(bundle["skus"][0]["stock_quantity"], 0)

            async def responses():
                with patch.object(catalogue, "product_wallet_snapshot", AsyncMock(return_value=wallet)):
                    public = await api.products(include_inactive=False)
                    sync = await api.products(include_inactive=True)
                    detail = await api.product(settings.PRODUCT_BUNDLE_ID)
                self.assertEqual(public["items"], [])
                self.assertTrue(sync["includes_inactive"])
                self.assertEqual(sync["total"], 1)
                self.assertEqual(detail["product"], sync["items"][0])

            asyncio.run(responses())
            catalogue.upsert_plans([])
            self.assertEqual(len(presentation.available_plan_rows()), 1)
            # When eligibility returns, preserve the original downstream mapping.
            raw["price"]["amount"] = "5.37"
            raw["available"] = True
            catalogue.upsert_plans([raw])
            row = presentation.available_plan_rows()[0]
            self.assertEqual(row["id"], original_id)
            self.assertEqual(row["available"], 1)
            catalogue.upsert_plans([])
            self.assertEqual(presentation.available_plan_rows()[0]["available"], 0)
            catalogue.upsert_plans([raw])
            with util.db_connect() as db:
                db.execute("DELETE FROM bridge_state")
            self.assertEqual(catalogue.wallet_capacity_for_plan(row, wallet, Decimal(0)), 0)


if __name__ == "__main__":
    unittest.main()
