"""Exercise failed-result retries through shared clients and encrypted history."""
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import Mock, patch

import requests

from exchange_binance_client import BinanceClient, ExchangeOperationFailedError, retry_failed_operation
from exchange_funds_recovery import FUNDS_WRITE_PATHS, _RECEIPT_KEYS
from operation_journal import OperationJournal
from secret_box import SecretBox


class ExchangeResultPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.box = SecretBox(self.root / '.test-key')
        self.journal = OperationJournal(self.root / 'operations.json', secret_box=self.box, fail_unconfirmed=True)

    def client(self, key='test-account', journal=None):
        with patch.object(BinanceClient, '_build_session', return_value=Mock()):
            client = BinanceClient(key, 'test-secret', operation_journal=journal or self.journal)
        client.DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 0
        self.addCleanup(client.close)
        return client

    def receipt(self, path):
        if path.endswith('/dust'):
            return {'transferResult': [{'tranId': 123}]}
        return {_RECEIPT_KEYS[path]: 123, 'orderStatus': 'SUCCESS'}

    def test_all_funds_paths_retry_missing_reply_once_and_allow_next_action(self):
        for path in sorted(FUNDS_WRITE_PATHS):
            with self.subTest(path=path):
                client = self.client()
                client._request_impl = Mock(side_effect=requests.ReadTimeout('lost reply'))
                params = {'coin': 'USDT', 'asset': 'USDT', 'amount': '20', 'quoteId': 'quote', 'address': 'target'}
                with self.assertRaises(ExchangeOperationFailedError):
                    client.request(client.spot, 'POST', path, params)
                self.assertEqual(client._request_impl.call_count, 2)
                self.assertEqual(self.journal.active_entries(), [])
                history = self.journal.recovery_snapshot()['entries'][-2:]
                self.assertEqual([entry['state'] for entry in history], ['failed', 'failed'])
                if path.endswith('/apply'):
                    self.assertNotEqual(history[0]['payload']['params']['withdrawOrderId'], history[1]['payload']['params']['withdrawOrderId'])
                restarted = self.client(journal=OperationJournal(self.journal.file_path, secret_box=self.box, fail_unconfirmed=True))
                restarted._request_impl = Mock(return_value=self.receipt(path))
                restarted.request(restarted.spot, 'POST', path, {**params, 'amount': '30', 'coin': 'USDC', 'asset': 'USDC', 'address': 'new-target'})
                restarted._request_impl.assert_called_once()
                self.assertEqual(self.journal.active_entries(), [])
        self.assertNotIn('new-target', self.journal.file_path.read_text())

    def test_all_funds_paths_second_attempt_can_succeed(self):
        for path in sorted(FUNDS_WRITE_PATHS):
            with self.subTest(path=path):
                client = self.client()
                client._request_impl = Mock(side_effect=[requests.ReadTimeout('lost'), self.receipt(path)])
                self.assertEqual(client.request(client.spot, 'POST', path, {'quoteId': 'quote'}), self.receipt(path))
                self.assertEqual(client._request_impl.call_count, 2)
                self.assertEqual([e['state'] for e in self.journal.recovery_snapshot()['entries'][-2:]], ['failed', 'completed'])

    def test_invalid_receipts_retry_once_without_success_or_pending_records(self):
        for receipt in ({}, {'tranId': True}, {'tranId': -1}, {'tranId': []}):
            client = self.client()
            client._request_impl = Mock(return_value=receipt)
            with self.assertRaises(ExchangeOperationFailedError):
                client.request(client.spot, 'POST', '/sapi/v1/asset/transfer', {'amount': '1'})
            self.assertEqual(client._request_impl.call_count, 2)
            self.assertEqual(self.journal.active_entries(), [])

    def test_order_timeout_retries_once_with_distinct_ids_and_no_recovery_blocker(self):
        for path in ('/api/v3/order', '/fapi/v1/order'):
            for order_type in ('MARKET', 'LIMIT'):
                with self.subTest(path=path, order_type=order_type):
                    client = self.client()
                    client.ORDER_SUBMISSION_CONFIRMATION_GRACE_SECONDS = 0
                    client._request_impl = Mock(side_effect=requests.ReadTimeout('lost'))
                    params = {'symbol': 'BTCUSDT', 'side': 'BUY', 'quantity': '2', 'type': order_type}
                    with self.assertRaises(ExchangeOperationFailedError):
                        client.request(client.spot if path.startswith('/api') else client.um_futures, 'POST', path, params)
                    posted = [call.args[3] for call in client._request_impl.call_args_list]
                    self.assertEqual(len(posted), 2)
                    self.assertNotEqual(posted[0]['newClientOrderId'], posted[1]['newClientOrderId'])
                    self.assertEqual(posted[0]['quantity'], posted[1]['quantity'])
                    self.assertEqual(client.pending_order_requests(), [])
                    self.assertEqual(self.journal.active_entries(), [])
                    client._request_impl = Mock(side_effect=lambda base, method, endpoint, values, **kw: {
                        'orderId': 10, 'clientOrderId': values['newClientOrderId'], 'symbol': values['symbol'], 'side': values['side'], 'status': 'FILLED'})
                    self.assertEqual(client.request(client.spot, 'POST', '/api/v3/order', {**params, 'symbol': 'ETHUSDC', 'side': 'SELL'})['status'], 'FILLED')
                    client._request_impl.assert_called_once()

    def test_confirmed_original_order_does_not_need_a_retry(self):
        client = self.client()
        original = {}
        def network(base, method, path, params, **kw):
            if method == 'POST':
                original.update(orderId=1, clientOrderId=params['newClientOrderId'], symbol=params['symbol'], side=params['side'], status='FILLED')
                raise requests.ReadTimeout('lost')
            return original
        client._request_impl = Mock(side_effect=network)
        self.assertEqual(client.request(client.spot, 'POST', '/api/v3/order', {'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'MARKET'})['status'], 'FILLED')
        self.assertEqual([c.args[1] for c in client._request_impl.call_args_list], ['POST', 'GET'])
        self.assertEqual(self.journal.active_entries(), [])

    def test_nested_convert_failures_share_one_retry_budget(self):
        client = self.client()
        client._validate_convert_from_amount = Mock(return_value=(1, {}))
        client.get_convert_quote = Mock(side_effect=[{'quoteId': 'first'}, {'quoteId': 'second'}])
        client._request_impl = Mock(side_effect=requests.ReadTimeout('lost'))
        with self.assertRaises(ExchangeOperationFailedError):
            client.convert_with_from_amount('BTC', 'USDT', 1)
        self.assertEqual(client._request_impl.call_count, 2)
        self.assertEqual([c.args[3]['quoteId'] for c in client._request_impl.call_args_list], ['first', 'second'])
        self.assertEqual(self.journal.active_entries(), [])

    def test_normal_convert_confirmation_keeps_quote_and_order_id_entrypoints(self):
        for by_order_id in (True, False):
            with self.subTest(by_order_id=by_order_id):
                client = self.client()
                client._request_impl = Mock(side_effect=[
                    {'orderId': 123, 'orderStatus': 'PROCESS'},
                    {'orderId': 123, 'quoteId': 'quote', 'orderStatus': 'SUCCESS'},
                ])
                client.accept_convert_quote('quote')
                args = {'order_id': 123} if by_order_id else {'quote_id': 'quote'}
                self.assertEqual(client.wait_convert_order_success(**args)['orderStatus'], 'SUCCESS')
                self.assertEqual(self.journal.active_entries(), [])

    def test_convert_confirmation_never_accepts_another_orders_receipt(self):
        client = self.client()
        client._request_impl = Mock(side_effect=[
            {'orderId': 123, 'orderStatus': 'PROCESS'},
            {'orderId': 456, 'quoteId': 'quote', 'orderStatus': 'SUCCESS'},
        ])
        client.accept_convert_quote('quote')
        with self.assertRaises(ExchangeOperationFailedError):
            client.get_convert_order_status(order_id=123)

    def test_one_account_failure_does_not_stop_next_account_or_share_retry_budget(self):
        first, second = self.client(), self.client('second-account')
        stop = threading.Event()
        first._request_impl = Mock(side_effect=requests.ReadTimeout('lost'))
        second._request_impl = Mock(return_value={'tranId': 1})
        outcomes = []
        for client in (first, second):
            try:
                client.request(client.spot, 'POST', '/sapi/v1/asset/transfer', {'amount': '1'})
            except ExchangeOperationFailedError:
                outcomes.append(False)
            else:
                outcomes.append(True)
        self.assertEqual(outcomes, [False, True])
        self.assertEqual(first._request_impl.call_count, 2)
        second._request_impl.assert_called_once()
        self.assertFalse(stop.is_set())

    def test_known_receipt_storage_failure_does_not_repost_it(self):
        client = self.client()
        client._request_impl = Mock(return_value={'tranId': 1})
        with patch.object(self.journal, 'update', side_effect=OSError('disk full')):
            with self.assertRaises(ExchangeOperationFailedError):
                client.request(client.spot, 'POST', '/sapi/v1/asset/transfer', {'amount': '1'})
        client._request_impl.assert_called_once()

    def test_restart_retires_only_exchange_uncertainty_and_keeps_other_checkpoints(self):
        old = OperationJournal(self.journal.file_path, secret_box=self.box)
        ids = {kind: old.begin(kind, 'scope', {'amount': '1'}) for kind in ('exchange-order', 'exchange-funds', 'onchain-transfer', 'exchange-batch')}
        restarted = OperationJournal(old.file_path, secret_box=self.box, fail_unconfirmed=True)
        data = {e['id']: e for e in restarted.recovery_snapshot()['entries']}
        self.assertEqual(data[ids['exchange-order']]['state'], 'failed')
        self.assertEqual(data[ids['exchange-funds']]['state'], 'failed')
        self.assertEqual(data[ids['onchain-transfer']]['state'], 'prepared')
        self.assertEqual(data[ids['exchange-batch']]['state'], 'prepared')

    def test_exchange_backup_restore_no_longer_requires_operator_review(self):
        old = OperationJournal(self.journal.file_path, secret_box=self.box)
        old.begin('exchange-funds', 'scope', {'amount': '1'})
        old.file_path.unlink()
        restored = OperationJournal(old.file_path, secret_box=self.box, fail_unconfirmed=True)
        self.assertEqual(restored.active_entries(), [])
        self.assertFalse(restored.recovery_snapshot().get('recovery_required'))

    def test_retry_budget_resets_after_failure(self):
        client = self.client()
        action = Mock(side_effect=ExchangeOperationFailedError('failed'))
        for _ in range(2):
            with self.assertRaises(ExchangeOperationFailedError):
                retry_failed_operation(client, action)
        self.assertEqual(action.call_count, 4)

    def test_concurrent_steps_on_one_client_keep_independent_retry_budgets(self):
        from concurrent.futures import ThreadPoolExecutor
        client = self.client()
        barrier = threading.Barrier(2)
        def run():
            count = 0
            def action():
                nonlocal count
                count += 1
                if count == 1:
                    barrier.wait(timeout=3)
                raise ExchangeOperationFailedError('missing result')
            with self.assertRaises(ExchangeOperationFailedError):
                retry_failed_operation(client, action)
            return count
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: run(), range(2)))
        self.assertEqual(results, [2, 2])

    def test_removed_review_api_and_desktop_entry_are_absent(self):
        from exchange_app_batch import ExchangeAppBatchMixin
        from linux_exchange.web import MUTATIONS
        self.assertFalse(hasattr(ExchangeAppBatchMixin, 'review_unconfirmed_operations'))
        self.assertFalse({'operation_review', 'operation_resolve'} & MUTATIONS)
        root = Path(__file__).resolve().parents[1]
        for name in ('exchange_app_base.py', 'linux_exchange/static/index.html', 'linux_exchange/static/app.js'):
            self.assertNotIn('核对未确认', (root / name).read_text(encoding='utf-8'))
