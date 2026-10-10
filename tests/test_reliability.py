import asyncio
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from app import main as bridge


class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patches = [patch.object(bridge, 'DB_PATH', str(Path(self.tmp.name) / 'test.db')),
                        patch.object(bridge, 'order_locks', {}),
                        patch.object(bridge, 'order_create_lock', asyncio.Lock()),
                        patch.object(bridge, 'catalogue_lock', asyncio.Lock()),
                        patch.object(bridge, 'outbox_lock', asyncio.Lock()),
                        patch.object(bridge, 'webhook_lock', asyncio.Lock())]
        for p in self.patches:
            p.start(); self.addCleanup(p.stop)
        bridge.init_db()
        self.raw = {'id': '11111111-1111-4111-8111-111111111111', 'sku': 'CN-10', 'name': 'China 10GB 30Days',
                    'destination': 'China', 'coverage': {'codes': ['CN']}, 'available': True,
                    'data': {'megabytes': 10240, 'type': 'fixed'}, 'validity_days': 30,
                    'price': {'amount': '10.00', 'currency': 'EUR'}}
        bridge.upsert_plans([self.raw])
        self.plan = bridge.available_plan_rows()[0]
        with bridge.db_connect() as db:
            db.execute('INSERT INTO bridge_state VALUES (?,?)', ('catalogue_synced_at', bridge.iso()))
        self.wallet = {'available_balance': Decimal('100'), 'currency': 'EUR'}

    def order(self, quantity=1, stellar_id=None, attempted=False, callback='https://merchant.test/callback'):
        with bridge.db_connect() as db:
            cursor = db.execute('''INSERT INTO orders
                (order_no,plan_id,quantity,idempotency_key,status,currency,created_at,updated_at,
                 unit_cost,plan_snapshot_json,stellar_order_id,purchase_attempted,callback_url)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                ('test-' + str(quantity) + str(stellar_id), self.plan['id'], quantity, 'original-key-' + str(quantity) + str(stellar_id),
                 'fulfilling' if stellar_id else 'creating', 'EUR', bridge.iso(), bridge.iso(),
                 '10.00', json.dumps(self.raw), stellar_id, int(attempted), callback))
            return cursor.lastrowid

    async def test_failed_order_is_never_repurchased_even_when_forced(self):
        order_id = self.order()
        bridge.update_order(order_id, status='failed')
        with patch.object(bridge.stellar, 'create_order', AsyncMock()) as create:
            result = await bridge.refresh_order(order_id, force=True)
        self.assertEqual(result['status'], 'failed')
        create.assert_not_awaited()

    async def test_price_increase_never_purchases(self):
        order_id = self.order()
        raw = dict(self.raw, price={'amount': '11.00', 'currency': 'EUR'})
        with patch.object(bridge.stellar, 'get_plan', AsyncMock(return_value={'data': raw})), \
             patch.object(bridge.stellar, 'create_order', AsyncMock()) as create:
            result = await bridge.refresh_order(order_id, force=True)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['purchase_attempted'], 0)
        create.assert_not_awaited()

    async def test_purchase_checks_balance_with_current_price(self):
        order_id = self.order(quantity=2)
        with patch.object(bridge.stellar, 'get_plan', AsyncMock(return_value={'data': self.raw})), \
             patch.object(bridge, 'wallet_snapshot', AsyncMock(return_value={'available_balance': Decimal('15'), 'currency': 'EUR'})), \
             patch.object(bridge.stellar, 'create_order', AsyncMock()) as create:
            result = await bridge.refresh_order(order_id, force=True)
        self.assertEqual(result['status'], 'failed')
        create.assert_not_awaited()

    async def test_transient_409_reuses_frozen_request_without_revalidation(self):
        order_id = self.order()
        conflict = bridge.StellarError(409, 'Processing', {'error': {'code': 'idempotency_in_progress'}})
        with patch.object(bridge.stellar, 'get_plan', AsyncMock(return_value={'data': self.raw})) as get_plan, \
             patch.object(bridge, 'wallet_snapshot', AsyncMock(return_value=self.wallet)), \
             patch.object(bridge.stellar, 'create_order', AsyncMock(side_effect=[conflict, {'data': {'id': 'upstream', 'total': '10.00'}}])) as create, \
             patch.object(bridge.stellar, 'get_order', AsyncMock(return_value={'data': {'status': 'processing'}})):
            first = await bridge.refresh_order(order_id, force=True)
            self.assertEqual(first['status'], 'creating')
            self.assertTrue(first['purchase_attempted'])
            # A changed catalogue must not alter recovery of the original request.
            second = await bridge.refresh_order(order_id, force=True)
        self.assertEqual(second['stellar_order_id'], 'upstream')
        self.assertEqual(get_plan.await_count, 1)
        self.assertEqual(create.await_args_list[0], create.await_args_list[1])

    async def test_permanent_idempotency_conflict_is_not_retried(self):
        order_id = self.order(attempted=True)
        conflict = bridge.StellarError(409, 'Different request', {'error': {'code': 'idempotency_conflict'}})
        with patch.object(bridge.stellar, 'create_order', AsyncMock(side_effect=conflict)) as create:
            result = await bridge.refresh_order(order_id, force=True)
            await bridge.refresh_order(order_id)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(create.await_count, 1)

    async def test_cancel_unsubmitted_order_does_not_purchase(self):
        order_id = self.order()
        with patch.object(bridge.stellar, 'create_order', AsyncMock()) as create, \
             patch.object(bridge.stellar, 'get_order', AsyncMock()) as get:
            response = await bridge.cancel_upstream_order(order_id)
            await bridge.refresh_order(order_id, force=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(bridge.get_order(order_id)['status'], 'canceled')
        create.assert_not_awaited(); get.assert_not_awaited()

    async def test_cancel_uncertain_purchase_requires_reconciliation_not_new_purchase(self):
        order_id = self.order(attempted=True)
        with patch.object(bridge.stellar, 'create_order', AsyncMock()) as create:
            response = await bridge.cancel_upstream_order(order_id)
            await bridge.refresh_order(order_id, force=True)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(bridge.get_order(order_id)['status'], 'manual_review')
        create.assert_not_awaited()

    async def test_partial_cancel_saves_refunds_and_skips_confirmed_units(self):
        order_id = self.order(quantity=2, stellar_id='upstream', attempted=True)
        detail = {'data': {'esims': [{'sim_id': '1', 'status': 'fulfilled'}, {'sim_id': '2', 'status': 'fulfilled'}]}}
        success = {'data': {'provider_cancelled': True, 'refunded': True, 'refund_amount_cents': 1000, 'currency': 'EUR'}}
        with patch.object(bridge.stellar, 'get_order', AsyncMock(return_value=detail)), \
             patch.object(bridge.stellar, 'cancel_esim', AsyncMock(side_effect=[success, bridge.StellarError(503, 'Pending'), success])) as cancel:
            first = await bridge.cancel_upstream_order(order_id)
            self.assertEqual(first.status_code, 503)
            self.assertEqual(bridge.get_order(order_id)['refunded_amount'], '10.00')
            second = await bridge.cancel_upstream_order(order_id)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(bridge.get_order(order_id)['refunded_amount'], '20.00')
        self.assertEqual([c.args[0] for c in cancel.await_args_list], ['1', '2', '2'])

    async def test_503_already_refunded_response_is_recorded_once(self):
        order_id = self.order(stellar_id='upstream', attempted=True)
        detail = {'data': {'esims': [{'sim_id': '1', 'status': 'fulfilled'}]}}
        error = bridge.StellarError(503, 'VPN pending', {'data': {'provider_cancelled': True, 'refunded': True,
                                                                          'refund_amount_cents': 1000, 'currency': 'EUR'}})
        with patch.object(bridge.stellar, 'get_order', AsyncMock(return_value=detail)), \
             patch.object(bridge.stellar, 'cancel_esim', AsyncMock(side_effect=error)) as cancel:
            response = await bridge.cancel_upstream_order(order_id)
            again = await bridge.cancel_upstream_order(order_id)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(again.status_code, 200)
        self.assertEqual(bridge.get_order(order_id)['refunded_amount'], '10.00')
        self.assertEqual(cancel.await_count, 1)

    async def test_order_transition_and_callback_survive_restart_and_retry(self):
        order_id = self.order(stellar_id='upstream')
        bridge.update_order(order_id, status='delivered', fulfillment_json=json.dumps({'payload': 'delivery'}))
        bridge.init_db()  # Queue survives startup/migration.
        await bridge.send_callback(bridge.get_order(order_id), 'delivered', {'payload': 'delivery'})
        with bridge.db_connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM callback_outbox').fetchone()[0], 1)
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(503 if len(calls) == 1 else 200)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(bridge.httpx, 'AsyncClient', return_value=client):
            await bridge.process_callback_outbox()
        with bridge.db_connect() as db:
            row = db.execute('SELECT * FROM callback_outbox').fetchone()
            self.assertIsNone(row['delivered_at']); self.assertEqual(row['attempts'], 1)
            db.execute('UPDATE callback_outbox SET next_attempt_at=?', (bridge.iso(),))
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(bridge.httpx, 'AsyncClient', return_value=client):
            await bridge.process_callback_outbox()
        with bridge.db_connect() as db:
            self.assertIsNotNone(db.execute('SELECT delivered_at FROM callback_outbox').fetchone()[0])
        self.assertEqual(json.loads(calls[-1].content)['fulfillment']['payload'], 'delivery')

    async def test_vpn_failure_can_deliver_complete_esim_without_waiting_for_vpn(self):
        order_id = self.order(stellar_id='upstream')
        detail = {'data': {'status': 'vpn_failed', 'quantity': 1,
                           'esims': [{'sim_id': '1', 'status': 'fulfilled'}]}}
        esim = {'data': {'customer_link': 'https://delivery.test/esim'}}
        with patch.object(bridge.stellar, 'get_order', AsyncMock(return_value=detail)), \
             patch.object(bridge.stellar, 'get_esim', AsyncMock(return_value=esim)):
            result = await bridge.refresh_order(order_id, force=True)
        self.assertEqual(result['status'], 'delivered')
        self.assertIn('https://delivery.test/esim', result['fulfillment_json'])

    async def test_partial_or_empty_delivery_is_not_marked_delivered(self):
        complete_one = {'quantity': 2, 'esims': [{'status': 'fulfilled', 'customer_link': 'https://delivery.test/one'}]}
        self.assertIsNone(bridge.fulfillment_from_stellar(complete_one, 2))
        self.assertIsNone(bridge.fulfillment_from_stellar({'quantity': 1, 'esims': [{'status': 'fulfilled'}]}, 1))

    async def test_confirmed_purchases_do_not_reserve_wallet_balance_twice(self):
        self.order(quantity=1)
        self.order(quantity=2, stellar_id='already-charged')
        self.assertEqual(bridge.reserved_wholesale_cost(), Decimal('10.00'))

    async def test_delta_only_deactivates_requested_missing_ids_and_reranks_cached_plans(self):
        second = dict(self.raw, id='22222222-2222-4222-8222-222222222222', name='China 3GB 1Days',
                      sku='CN-3', data={'megabytes': 3072, 'type': 'fixed'}, validity_days=1,
                      price={'amount': '1.00', 'currency': 'EUR'})
        bridge.upsert_plans([self.raw, second])
        with patch.object(bridge.stellar, 'plans_by_ids', AsyncMock(return_value=[])) as fetch, \
             patch.object(bridge, 'wallet_snapshot', AsyncMock(return_value=self.wallet)):
            await bridge.reconcile_catalogue_event({'data': {'plan_ids': [self.raw['id']]}}, bridge.iso())
        self.assertEqual({r['stellar_plan_id'] for r in bridge.available_plan_rows() if r['available']}, {second['id']})
        self.assertTrue(bridge.catalogue_is_fresh())
        fetch.assert_awaited_once_with([self.raw['id']])

    async def test_failed_delta_does_not_remove_products(self):
        with patch.object(bridge.stellar, 'plans_by_ids', AsyncMock(side_effect=bridge.StellarError(429, 'Limited'))):
            with self.assertRaises(bridge.StellarError):
                await bridge.reconcile_catalogue_event({'data': {'plan_ids': [self.raw['id']]}}, bridge.iso())
        self.assertEqual(bridge.available_plan_rows()[0]['available'], 1)

    async def test_full_scan_missing_plan_is_not_resurrected_by_unrelated_delta(self):
        other = dict(self.raw, id='22222222-2222-4222-8222-222222222222', sku='CN-OTHER')
        bridge.upsert_plans([other])
        with patch.object(bridge.stellar, 'plans_by_ids', AsyncMock(return_value=[other])), \
             patch.object(bridge, 'wallet_snapshot', AsyncMock(return_value=self.wallet)):
            await bridge.reconcile_catalogue_event({'data': {'plan_ids': [other['id']]}}, bridge.iso())
        self.assertEqual(bridge.get_plan(self.plan['id'])['upstream_active'], 0)
        self.assertEqual(bridge.get_plan(self.plan['id'])['available'], 0)

    async def test_ids_query_has_no_country_or_destination_filters_and_requires_complete_pages(self):
        client = bridge.StellarClient()
        try:
            with patch.object(client, 'request', AsyncMock(return_value={'data': [], 'meta': {'last_page': 1}})) as request:
                self.assertEqual(await client.plans_by_ids([self.raw['id']]), [])
                params = request.await_args.kwargs['params']
                self.assertIn(('ids[]', self.raw['id']), params)
                self.assertNotIn('country_code', dict(params))
                self.assertNotIn('destination', dict(params))
            with patch.object(client, 'request', AsyncMock(return_value={'data': []})):
                with self.assertRaises(ValueError):
                    await client.plans_by_ids([self.raw['id']])
        finally:
            await client.close()

    async def test_webhook_queue_survives_restart_and_duplicate_envelopes(self):
        event = {'id': 'event-1', 'type': 'catalogue.updated', 'data': {'plan_ids': [self.raw['id']]}}
        class Request:
            headers = {'Stellar-Signature': 'verified'}
            async def body(self): return json.dumps(event).encode()
        with patch.object(bridge, 'STELLAR_WEBHOOK_SECRET', 'test'), \
             patch.object(bridge, 'verify_stellar_signature', return_value=True):
            first = await bridge.stellar_webhook(Request())
            duplicate = await bridge.stellar_webhook(Request())
        self.assertFalse(first['duplicate']); self.assertTrue(duplicate['duplicate'])
        bridge.init_db()
        with patch.object(bridge.stellar, 'plans_by_ids', AsyncMock(return_value=[self.raw])), \
             patch.object(bridge, 'wallet_snapshot', AsyncMock(return_value=self.wallet)):
            await bridge.process_webhook_events()
        with bridge.db_connect() as db:
            self.assertIsNotNone(db.execute('SELECT processed_at FROM webhook_events').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
