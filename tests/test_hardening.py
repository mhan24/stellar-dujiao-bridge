"""Guards that keep a bad downstream/upstream response from causing damage."""

import asyncio
import json
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from app import main as bridge


class UpstreamCatalogueGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "test.db"))
        patcher.start()
        self.addCleanup(patcher.stop)
        bridge.init_db()
        self.plans = [
            {
                "id": f"plan-{index}",
                "sku": f"CN-{index}",
                "name": f"China {index}GB 30Days",
                "destination": "China",
                "coverage": {"codes": ["CN"]},
                "available": True,
                "data": {"megabytes": index * 1024},
                "validity_days": 30,
                "price": {"amount": str(index), "currency": "EUR"},
            }
            for index in range(1, 5)
        ]
        bridge.upsert_plans(self.plans)

    def available(self):
        return [row["stellar_plan_id"] for row in bridge.available_plan_rows() if row["available"]]

    async def test_empty_successful_response_keeps_previous_catalogue(self):
        with patch.object(bridge.stellar, "list_plans", AsyncMock(return_value={"data": [], "meta": {"last_page": 1}})):
            with self.assertRaises(ValueError):
                await bridge.fetch_catalogue()
        self.assertEqual(len(self.available()), 4)

    async def test_missing_data_field_keeps_previous_catalogue(self):
        with patch.object(bridge.stellar, "list_plans", AsyncMock(return_value={"meta": {"last_page": 1}})):
            with self.assertRaises(ValueError):
                await bridge.fetch_catalogue()
        self.assertEqual(len(self.available()), 4)

    async def test_empty_page_before_last_page_is_incomplete(self):
        first = {"data": self.plans[:2], "meta": {"last_page": 3}}
        second = {"data": [], "meta": {"last_page": 3}}
        with patch.object(bridge.stellar, "list_plans", AsyncMock(side_effect=[first, second])):
            with self.assertRaises(ValueError):
                await bridge.fetch_catalogue()
        self.assertEqual(len(self.available()), 4)

    async def test_unbounded_pagination_is_rejected(self):
        response = {"data": self.plans[:1], "meta": {"last_page": 10_000}}
        with patch.object(bridge.stellar, "list_plans", AsyncMock(return_value=response)):
            with self.assertRaises(ValueError):
                await bridge.fetch_catalogue()
        self.assertEqual(len(self.available()), 4)

    async def test_suspicious_shrink_keeps_previous_catalogue(self):
        with patch.object(
            bridge.stellar, "list_plans", AsyncMock(return_value={"data": self.plans[:1], "meta": {"last_page": 1}})
        ):
            with self.assertRaises(ValueError):
                await bridge.fetch_catalogue()
        self.assertEqual(len(self.available()), 4)

    async def test_shrink_guard_can_be_relaxed_for_real_discontinuations(self):
        wallet = {"available_balance": Decimal("1000"), "currency": "EUR"}
        with (
            patch.object(bridge.settings, "CATALOGUE_MIN_ACTIVE_RATIO", Decimal("0")),
            patch.object(bridge.wallet_balance, "wallet_snapshot", AsyncMock(return_value=wallet)),
            patch.object(
                bridge.stellar, "list_plans", AsyncMock(return_value={"data": self.plans[:1], "meta": {"last_page": 1}})
            ),
        ):
            self.assertEqual(await bridge.fetch_catalogue(), 1)
        self.assertEqual(self.available(), ["plan-1"])
        self.assertTrue(bridge.catalogue_is_fresh())


class CatalogueDisplayTests(unittest.TestCase):
    def test_data_label_never_uses_scientific_notation(self):
        self.assertEqual(bridge.display_data_label({"data": {"megabytes": 102400}}), "100GB")
        self.assertEqual(bridge.display_data_label({"data": {"megabytes": 1024 * 1024}}), "1024GB")
        self.assertEqual(bridge.display_data_label({"data": {"megabytes": 1536}}), "1.5GB")
        self.assertEqual(bridge.display_data_label({"data": {"megabytes": 3072, "label": " 3gb "}}), "3GB")
        self.assertEqual(bridge.display_data_label({"data": {"megabytes": 0}}), "流量")


class OrderRequestTests(unittest.IsolatedAsyncioTestCase):
    class Request:
        def __init__(self, payload=None, error=None):
            self.payload, self.error = payload, error

        async def json(self):
            if self.error:
                raise self.error
            return self.payload

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "test.db"))
        patcher.start()
        self.addCleanup(patcher.stop)
        bridge.init_db()

    async def reject(self, request):
        with self.assertRaises(HTTPException) as caught:
            await bridge.create_order(request)
        self.assertEqual(caught.exception.status_code, 400)
        return caught.exception

    async def test_malformed_payloads_are_rejected_as_bad_requests(self):
        await self.reject(self.Request(error=ValueError("not json")))
        await self.reject(self.Request(payload=[{"sku_id": 1}]))
        await self.reject(self.Request(payload={"sku_id": "abc", "quantity": 1}))
        await self.reject(self.Request(payload={"sku_id": 1}))
        await self.reject(self.Request(payload={"sku_id": 0, "quantity": 1}))
        await self.reject(self.Request(payload={"sku_id": 1, "quantity": 11, "downstream_order_no": "down-1"}))

    async def test_missing_downstream_order_no_is_rejected(self):
        error = await self.reject(self.Request(payload={"sku_id": 1, "quantity": 1}))
        self.assertIn("downstream_order_no", error.detail)

    async def test_unusable_callback_urls_are_rejected_without_side_effects(self):
        for url in (
            "http://merchant.test/cb",
            "https://localhost/cb",
            "https://127.0.0.1/cb",
            "https://169.254.169.254/latest/meta-data",
            "https://metadata.google.internal/cb",
        ):
            with patch.object(bridge.wallet_balance, "wallet_snapshot", AsyncMock()) as wallet:
                response = await bridge.create_order(
                    self.Request(
                        payload={"sku_id": 1, "quantity": 1, "downstream_order_no": "down-1", "callback_url": url}
                    )
                )
            self.assertEqual(response.status_code, 400)
            self.assertEqual(json.loads(response.body)["error_code"], "invalid_callback_url")
            wallet.assert_not_awaited()

    async def test_quoted_cost_is_reported_instead_of_current_price(self):
        raw = {
            "id": "plan-a",
            "sku": "CN-10",
            "name": "China 10GB 30Days",
            "destination": "China",
            "coverage": {"codes": ["CN"]},
            "available": True,
            "data": {"megabytes": 10240},
            "validity_days": 30,
            "price": {"amount": "10.00", "currency": "EUR"},
        }
        bridge.upsert_plans([raw])
        plan = bridge.available_plan_rows()[0]
        with bridge.db_connect() as db:
            cursor = db.execute(
                """INSERT INTO orders (order_no,plan_id,quantity,idempotency_key,status,
                currency,created_at,updated_at,unit_cost) VALUES (?,?,?,?,?,?,?,?,?)""",
                ("ord-1", plan["id"], 1, "key-1", "delivered", "EUR", bridge.iso(), bridge.iso(), "10.00"),
            )
            order_id = cursor.lastrowid
        inflated = dict(raw, price={"amount": "19.99", "currency": "EUR"})
        bridge.upsert_plans([inflated])
        response = bridge.order_response(bridge.get_order(order_id))
        self.assertEqual(response["items"][0]["unit_price"], "10.00")


class CallbackUrlTests(unittest.TestCase):
    def test_public_https_callbacks_are_accepted(self):
        from app.security import callback_url_error

        self.assertIsNone(callback_url_error("https://dujiao.example.com/api/v1/upstream/callback"))
        self.assertIsNone(callback_url_error("https://8.8.8.8/callback"))

    def test_private_or_plaintext_callbacks_are_refused(self):
        from app.security import callback_url_error

        for url in (
            "http://dujiao.example.com/cb",
            "https://localhost/cb",
            "https://api.localhost/cb",
            "https://127.0.0.1/cb",
            "https://10.1.2.3/cb",
            "https://192.168.0.10/cb",
            "https://169.254.169.254/cb",
            "https://metadata.google.internal/cb",
            "https://[::1]/cb",
            "https:///cb",
            "not-a-url",
        ):
            self.assertIsNotNone(callback_url_error(url), url)


class BodyLimitTests(unittest.IsolatedAsyncioTestCase):
    class Inner:
        def __init__(self):
            self.body = None

        async def __call__(self, scope, receive, send):
            message = await receive()
            self.body = message.get("body")
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    async def post(self, middleware, body, headers=()):
        sent = []

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            sent.append(message)

        await middleware({"type": "http", "method": "POST", "path": "/x", "headers": list(headers)}, receive, send)
        return sent

    async def test_oversized_bodies_are_rejected_before_buffering(self):
        inner = self.Inner()
        middleware = bridge.BodyLimitMiddleware(inner, max_bytes=16)
        sent = await self.post(middleware, b"x" * 32)
        self.assertEqual(sent[0]["status"], 413)
        self.assertIsNone(inner.body)

    async def test_declared_length_is_rejected_without_reading(self):
        inner = self.Inner()
        middleware = bridge.BodyLimitMiddleware(inner, max_bytes=16)
        sent = await self.post(middleware, b"x", headers=[(b"content-length", b"9999")])
        self.assertEqual(sent[0]["status"], 413)
        self.assertIsNone(inner.body)

    async def test_accepted_bodies_are_replayed_to_the_app(self):
        inner = self.Inner()
        middleware = bridge.BodyLimitMiddleware(inner, max_bytes=16)
        sent = await self.post(middleware, b'{"sku_id":1}')
        self.assertEqual(sent[0]["status"], 200)
        self.assertEqual(inner.body, b'{"sku_id":1}')


class DurableQueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patches = [
            patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "test.db")),
            patch.object(bridge.orders, "order_locks", {}),
            patch.object(bridge.queues, "outbox_lock", asyncio.Lock()),
            patch.object(bridge.queues, "webhook_lock", asyncio.Lock()),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        bridge.init_db()
        raw = {
            "id": "11111111-1111-4111-8111-111111111111",
            "sku": "CN-10",
            "name": "China 10GB 30Days",
            "destination": "China",
            "coverage": {"codes": ["CN"]},
            "available": True,
            "data": {"megabytes": 10240},
            "validity_days": 30,
            "price": {"amount": "10.00", "currency": "EUR"},
        }
        bridge.upsert_plans([raw])
        self.plan = bridge.available_plan_rows()[0]
        with bridge.db_connect() as db:
            cursor = db.execute(
                """INSERT INTO orders (order_no,plan_id,quantity,idempotency_key,status,
                currency,created_at,updated_at,unit_cost,callback_url)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    "ord-1",
                    self.plan["id"],
                    1,
                    "key-1",
                    "fulfilling",
                    "EUR",
                    bridge.iso(),
                    bridge.iso(),
                    "10.00",
                    "https://merchant.test/callback",
                ),
            )
            self.order_id = cursor.lastrowid
        bridge.update_order(self.order_id, status="delivered")

    def outbox(self):
        with bridge.db_connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM callback_outbox")]

    async def deliver(self, status=503):
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status)))
        with patch.object(bridge.queues.httpx, "AsyncClient", return_value=client):
            await bridge.process_callback_outbox()

    def retry_now(self):
        with bridge.db_connect() as db:
            db.execute("UPDATE callback_outbox SET next_attempt_at=?", (bridge.iso(),))

    async def test_callback_is_abandoned_after_max_attempts(self):
        self.assertEqual(len(self.outbox()), 1)
        with patch.object(bridge.settings, "CALLBACK_MAX_ATTEMPTS", 2):
            await self.deliver()
            self.retry_now()
            await self.deliver()
            self.retry_now()
            await self.deliver()
        row = self.outbox()[0]
        self.assertEqual(row["attempts"], 2)
        self.assertIsNotNone(row["failed_at"])
        self.assertIsNone(row["delivered_at"])
        self.assertEqual(row["last_error"], "HTTP 503")

    async def test_delivered_callback_is_not_retried(self):
        await self.deliver(status=200)
        row = self.outbox()[0]
        self.assertIsNotNone(row["delivered_at"])
        self.retry_now()
        await self.deliver()
        self.assertEqual(self.outbox()[0]["attempts"], 1)

    async def test_prune_removes_only_expired_finished_rows(self):
        old = bridge.iso(bridge.now() - timedelta(days=40))
        recent = bridge.iso()
        with bridge.db_connect() as db:
            db.execute("DELETE FROM callback_outbox")
            db.execute("DELETE FROM webhook_events")
            db.executemany(
                """INSERT INTO callback_outbox
                (notification_key,order_id,callback_url,payload_json,next_attempt_at,delivered_at,failed_at)
                VALUES (?,?,?,?,?,?,?)""",
                [
                    ("old-delivered", self.order_id, "https://merchant.test/cb", "{}", old, old, None),
                    ("old-failed", self.order_id, "https://merchant.test/cb", "{}", old, None, old),
                    ("recent-delivered", self.order_id, "https://merchant.test/cb", "{}", recent, recent, None),
                    ("pending", self.order_id, "https://merchant.test/cb", "{}", old, None, None),
                ],
            )
            db.executemany(
                "INSERT INTO webhook_events (event_id,received_at,processed_at,failed_at) VALUES (?,?,?,?)",
                [("event-old", old, old, None), ("event-failed", old, None, old), ("event-pending", old, None, None)],
            )
        self.assertEqual(bridge.prune_durable_queues(), (2, 2, 0))
        with bridge.db_connect() as db:
            self.assertEqual(
                {row[0] for row in db.execute("SELECT notification_key FROM callback_outbox")},
                {"recent-delivered", "pending"},
            )
            self.assertEqual({row[0] for row in db.execute("SELECT event_id FROM webhook_events")}, {"event-pending"})

    async def test_order_lock_serializes_and_cleans_up(self):
        events = []

        async def worker(tag):
            async with bridge.order_lock(7):
                events.append(("start", tag))
                await asyncio.sleep(0.01)
                events.append(("end", tag))

        await asyncio.gather(worker("a"), worker("b"))
        self.assertEqual([kind for kind, _ in events], ["start", "end", "start", "end"])
        self.assertEqual(bridge.order_locks, {})

    async def test_db_connect_closes_its_connection(self):
        with bridge.db_connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM orders").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")

    async def test_lifespan_starts_and_stops_background_workers(self):
        async def forever():
            await asyncio.Event().wait()

        with (
            patch.object(bridge.util, "init_db"),
            patch.object(bridge.catalogue, "sync_catalogue", forever),
            patch.object(bridge.api, "worker_loop", forever),
            patch.object(bridge.queues, "maintenance_loop", forever),
            patch.object(bridge.stellar, "close", AsyncMock()) as close,
        ):
            async with bridge.lifespan(bridge.app):
                await asyncio.sleep(0)
                self.assertEqual(len(bridge.background_tasks), 3)
                self.assertEqual(
                    {task.get_name() for task in bridge.background_tasks},
                    {"catalogue-initial-sync", "order-worker", "durable-queues"},
                )
        self.assertEqual(bridge.background_tasks, set())
        close.assert_awaited_once()


class OrderRoutingTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end create_order through the HTTP handler, not just the internals."""

    class Request:
        def __init__(self, payload):
            self.payload = payload

        async def json(self):
            return self.payload

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patches = [
            patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "test.db")),
            patch.object(bridge.orders, "order_locks", {}),
            patch.object(bridge.orders, "order_create_lock", asyncio.Lock()),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        bridge.init_db()
        self.raw = {
            "id": "11111111-1111-4111-8111-111111111111",
            "sku": "CN-10",
            "name": "China 10GB 30Days",
            "destination": "China",
            "coverage": {"codes": ["CN"]},
            "available": True,
            "data": {"megabytes": 10240},
            "validity_days": 30,
            "price": {"amount": "10.00", "currency": "EUR"},
        }
        bridge.upsert_plans([self.raw])
        self.plan = bridge.available_plan_rows()[0]
        with bridge.db_connect() as db:
            db.execute("INSERT OR REPLACE INTO bridge_state VALUES (?,?)", ("catalogue_synced_at", bridge.iso()))
        self.wallet = {"available_balance": Decimal("100"), "currency": "EUR"}

    def payload(self, **overrides):
        body = {
            "sku_id": self.plan["id"],
            "quantity": 1,
            "downstream_order_no": "down-1",
            "callback_url": "https://merchant.test/callback",
        }
        body.update(overrides)
        return self.Request(body)

    async def test_order_is_created_once_and_locks_are_released(self):
        with (
            patch.object(bridge.wallet_balance, "wallet_snapshot", AsyncMock(return_value=self.wallet)),
            patch.object(bridge.stellar, "get_plan", AsyncMock(return_value={"data": self.raw})),
            patch.object(
                bridge.stellar, "create_order", AsyncMock(return_value={"data": {"id": "up-1", "total": "10.00"}})
            ) as create,
            patch.object(bridge.stellar, "get_order", AsyncMock(return_value={"data": {"status": "processing"}})),
        ):
            first = await bridge.create_order(self.payload())
            replay = await bridge.create_order(self.payload())
        self.assertEqual(first.status_code, 200)
        first_body, replay_body = json.loads(first.body), json.loads(replay.body)
        self.assertEqual(first_body["status"], "paid")
        self.assertEqual(replay_body["order_id"], first_body["order_id"])
        create.assert_awaited_once()
        self.assertEqual(bridge.orders.order_locks, {})
        with bridge.db_connect() as db:
            row = dict(db.execute("SELECT * FROM orders").fetchone())
        self.assertEqual(row["status"], "fulfilling")
        self.assertEqual(row["callback_url"], "https://merchant.test/callback")
        self.assertEqual(row["unit_cost"], "10.00")
        self.assertEqual(row["stellar_order_id"], "up-1")

    async def test_ping_reports_wallet_balance(self):
        with patch.object(
            bridge.wallet_balance,
            "wallet_snapshot",
            AsyncMock(
                return_value={
                    "available_balance": Decimal("12.34"),
                    "currency": "EUR",
                    "checked_at_iso": "now",
                    "error": None,
                }
            ),
        ):
            payload = await bridge.api.ping()
        self.assertEqual(payload["balance"], "12.34")
        self.assertEqual(payload["currency"], "EUR")


class ProtocolAndEventTests(unittest.IsolatedAsyncioTestCase):
    """Internal states must not leak and esim.* events must drive refreshes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patches = [
            patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "test.db")),
            patch.object(bridge.orders, "order_locks", {}),
            patch.object(bridge.queues, "webhook_lock", asyncio.Lock()),
            patch.object(bridge.queues, "_esim_sweep_at", 0.0),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        bridge.init_db()
        self.raw = {
            "id": "11111111-1111-4111-8111-111111111111",
            "sku": "CN-10",
            "name": "China 10GB 30Days",
            "destination": "China",
            "coverage": {"codes": ["CN"]},
            "available": True,
            "data": {"megabytes": 10240},
            "validity_days": 30,
            "price": {"amount": "10.00", "currency": "EUR"},
        }
        bridge.upsert_plans([self.raw])
        self.plan = bridge.available_plan_rows()[0]

    def order(self, status="fulfilling", stellar_id=None, fulfillment=None):
        with bridge.db_connect() as db:
            cursor = db.execute(
                """INSERT INTO orders (order_no,plan_id,quantity,idempotency_key,status,
                currency,created_at,updated_at,unit_cost,stellar_order_id,fulfillment_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    f"ord-{status}-{stellar_id}",
                    self.plan["id"],
                    1,
                    f"key-{status}-{stellar_id}",
                    status,
                    "EUR",
                    bridge.iso(),
                    bridge.iso(),
                    "10.00",
                    stellar_id,
                    json.dumps(fulfillment) if fulfillment else None,
                ),
            )
            return cursor.lastrowid

    def queue_event(self, event):
        with bridge.db_connect() as db:
            db.execute(
                "INSERT INTO webhook_events (event_id,received_at,payload_json) VALUES (?,?,?)",
                (event["id"], bridge.iso(), json.dumps(event)),
            )

    async def test_internal_creating_status_is_reported_as_fulfilling(self):
        order_id = self.order(status="creating")
        response = bridge.presentation.order_response(bridge.get_order(order_id))
        self.assertEqual(response["status"], "fulfilling")
        self.assertEqual(bridge.presentation.protocol_status("manual_review"), "manual_review")
        self.assertEqual(bridge.presentation.protocol_status("delivered"), "delivered")

    async def test_only_downstream_visible_status_changes_are_notified(self):
        order_id = self.order(status="creating", stellar_id="up-1")
        with bridge.db_connect() as db:
            db.execute("UPDATE orders SET callback_url = 'https://merchant.test/callback' WHERE id = ?", (order_id,))
        # creating -> fulfilling is a single state downstream, so nothing is sent.
        bridge.queues.update_order(order_id, status="fulfilling")
        with bridge.db_connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM callback_outbox").fetchone()[0], 0)
        # fulfilling -> delivered is visible and carries the delivery payload.
        fulfillment = {"delivery_data": {"esims": [{"sim_id": "SIM-1", "customer_link": "https://x"}]}}
        bridge.queues.update_order(order_id, status="delivered", fulfillment_json=json.dumps(fulfillment))
        with bridge.db_connect() as db:
            payloads = [json.loads(row[0]) for row in db.execute("SELECT payload_json FROM callback_outbox")]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["status"], "delivered")
        self.assertEqual(payloads[0]["event"], "order.fulfilled")
        self.assertEqual(payloads[0]["fulfillment"]["delivery_data"]["esims"][0]["sim_id"], "SIM-1")

    async def test_cancel_response_tolerates_a_missing_order(self):
        response = bridge.orders.cancel_response(None)
        self.assertEqual(response.status_code, 404)

    async def test_esim_event_refreshes_the_order_that_owns_the_sim(self):
        fulfillment = {"delivery_data": {"esims": [{"sim_id": "SIM-7", "customer_link": "https://x"}]}}
        order_id = self.order(status="delivered", stellar_id="up-1", fulfillment=fulfillment)
        self.assertEqual(bridge.store.find_order_by_sim_id("SIM-7")["id"], order_id)
        self.queue_event({"id": "e1", "type": "esim.fulfilled", "data": {"sim_id": "SIM-7"}})
        with patch.object(bridge.orders, "refresh_order", AsyncMock(return_value={})) as refresh:
            await bridge.queues.process_webhook_events()
        refresh.assert_awaited_once_with(order_id, force=True)
        with bridge.db_connect() as db:
            self.assertIsNotNone(db.execute("SELECT processed_at FROM webhook_events").fetchone()[0])

    async def test_esim_event_by_upstream_order_id_refreshes_immediately(self):
        order_id = self.order(stellar_id="up-9")
        self.queue_event({"id": "e2", "type": "esim.failed", "data": {"order_id": "up-9", "sim_id": "SIM-X"}})
        with (
            patch.object(bridge.orders, "refresh_order", AsyncMock(return_value={})) as refresh,
            patch.object(bridge.store, "in_flight_order_ids") as sweep,
        ):
            await bridge.queues.process_webhook_events()
        refresh.assert_awaited_once_with(order_id, force=True)
        sweep.assert_not_called()

    async def test_unmapped_esim_event_sweeps_in_flight_orders_once_per_cooldown(self):
        in_flight = [self.order(status="fulfilling", stellar_id="up-a")]
        self.queue_event({"id": "e3", "type": "esim.failed", "data": {"sim_id": "SIM-unknown"}})
        with (
            patch.object(bridge.orders, "refresh_order", AsyncMock(return_value={})) as refresh,
            patch.object(bridge.settings, "ESIM_EVENT_SWEEP_COOLDOWN_SECONDS", 3600),
        ):
            await bridge.queues.process_webhook_events()
            self.queue_event({"id": "e4", "type": "esim.failed", "data": {"sim_id": "SIM-unknown"}})
            await bridge.queues.process_webhook_events()
        self.assertEqual([call.args[0] for call in refresh.await_args_list], in_flight)

    async def test_order_events_still_refresh_by_order_id(self):
        order_id = self.order(stellar_id="up-3")
        self.queue_event({"id": "e5", "type": "order.fulfilled", "data": {"order_id": "up-3"}})
        with patch.object(bridge.orders, "refresh_order", AsyncMock(return_value={})) as refresh:
            await bridge.queues.process_webhook_events()
        refresh.assert_awaited_once_with(order_id, force=True)

    async def test_status_endpoint_reports_catalogue_orders_and_queues(self):
        self.order(status="creating")
        with bridge.db_connect() as db:
            db.execute("INSERT OR REPLACE INTO bridge_state VALUES (?,?)", ("catalogue_synced_at", bridge.iso()))
        payload = await bridge.api.status()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["catalogue"]["fresh"])
        self.assertEqual(payload["catalogue"]["plans_active"], 1)
        self.assertEqual(payload["orders"]["orders_in_flight"], 1)
        self.assertEqual(payload["queues"]["callbacks_pending"], 0)
        self.assertIn("wallet", payload)
        self.assertEqual(payload["background_tasks"], [])


if __name__ == "__main__":
    unittest.main()
