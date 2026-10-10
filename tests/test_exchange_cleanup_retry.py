"""Bounded cleanup through real strategies, signed requests and encrypted history."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import hmac
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import MethodType, SimpleNamespace
from urllib.parse import urlencode, urlsplit
import unittest
from unittest.mock import Mock, patch

import requests

from exchange_app_base import ExchangeAppBase
from exchange_binance_client import BinanceClient, ExchangeOperationFailedError, retry_cleanup, retry_failed_operation
from exchange_constants import TRADE_ACCOUNT_TYPE_FUTURES, TRADE_MODE_CONVERT, TRADE_MODE_LADDER
from exchange_stocks import StockSettings, StockStrategy, STOCK_FIELDS
from exchange_strategy import Strategy
from operation_journal import OperationJournal
from secret_box import SecretBox
import test_exchange_stocks as stocks


def signed_transport(client, dispatch):
    session = Mock()
    client.secret = b"test-secret"
    client.recv_window_ms = 60000
    client._sync_server_time_offset = Mock(return_value=0)
    def response(method, url, values):
        values = dict(values or {})
        if "signature" in values:
            signature = values.pop("signature")
            expected = hmac.new(client.secret, urlencode(values).encode(), hashlib.sha256).hexdigest()
            assert hmac.compare_digest(signature, expected)
        result = dispatch(method, urlsplit(url).path, values)
        return SimpleNamespace(status_code=200, json=lambda: result, raise_for_status=lambda: None)
    session.get.side_effect = lambda url, params=None, **_: response("GET", url, params)
    session.request.side_effect = lambda method, url, data=None, **_: response(method, url, data)
    client.session = session
    client._build_session = lambda: session
    client._request_impl = MethodType(BinanceClient._request_impl, client)
    return session


class CleanupRetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.box = SecretBox(root / ".test-key")
        self.journal = OperationJournal(root / "history.json", secret_box=self.box, fail_unconfirmed=True)

    def market_task(self, key, fail_rounds=()):
        with patch.object(BinanceClient, "_build_session", return_value=Mock()):
            client = BinanceClient(key, "test-secret", operation_journal=self.journal)
        self.addCleanup(client.close)
        balances = {"BTC": Decimal(0), "USDT": Decimal(1000)}
        orders, buys, cleanup_calls = [], [0], Counter()
        def network(method, path, params):
            if path == "/api/v3/account":
                if getattr(client, "_cleanup_attempt_depth", 0) and buys[0] in fail_rounds:
                    cleanup_calls[buys[0]] += 1
                    raise requests.ReadTimeout("cleanup balance unavailable")
                return {"balances": [{"asset": asset, "free": str(amount), "locked": "0"} for asset, amount in balances.items()]}
            if path == "/api/v3/exchangeInfo":
                return {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT", "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.000001", "minQty": "0.000001", "maxQty": "100000"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"}]}]}
            if path == "/api/v3/ticker/price":
                return {"symbol": "BTCUSDT", "price": "100"}
            if path == "/api/v3/ticker/bookTicker":
                return {"symbol": "BTCUSDT", "bidPrice": "99", "askPrice": "100"}
            if path == "/api/v3/order" and method == "POST":
                side = params["side"]
                if side == "BUY":
                    buys[0] += 1
                    quantity = Decimal(params["quoteOrderQty"]) / 100
                else:
                    quantity = Decimal(params["quantity"])
                sign = 1 if side == "BUY" else -1
                balances["BTC"] += sign * quantity
                balances["USDT"] -= sign * quantity * 100
                order = {**params, "orderId": len(orders) + 1, "clientOrderId": params["newClientOrderId"],
                         "status": "FILLED", "executedQty": str(quantity), "cummulativeQuoteQty": str(quantity * 100)}
                orders.append(order)
                return order
            raise AssertionError((method, path))
        signed_transport(client, network)
        client.collect_asset_to_spot = lambda *_args, **_kwargs: 0
        task = Strategy(client, spot_rounds=6, withdraw_coin="USDT", withdraw_address="", withdraw_network="",
                        withdraw_fee_buffer=0, spot_symbol="BTCUSDT", sleep_fn=lambda: None,
                        enable_withdraw=False, operation_journal=self.journal, operation_scope=key)
        return task, orders, cleanup_calls

    def test_round_three_and_six_fail_after_five_retries_then_same_worker_runs_next_account(self):
        first, first_orders, calls = self.market_task("first", (3, 6))
        second, second_orders, _ = self.market_task("second")
        stop = threading.Event()
        thread_ids = []
        def run(task):
            thread_ids.append(threading.get_ident())
            return task.run(stop)
        with ThreadPoolExecutor(max_workers=1) as pool:
            results = list(pool.map(run, (first, second)))
        self.assertEqual(calls, {3: 6, 6: 6})
        self.assertEqual([row["side"] for row in first_orders], ["BUY", "SELL"] * 6)
        self.assertEqual([row["side"] for row in second_orders], ["BUY", "SELL"] * 6)
        self.assertEqual(len({row["clientOrderId"] for row in first_orders + second_orders}), 24)
        self.assertEqual(results[0]["market_round_failed_rounds"], [3, 6])
        self.assertEqual(results[0]["trade_error_stage"], "清理失败")
        self.assertEqual(results[0]["market_round_completed"], 6)
        self.assertFalse(results[1]["trade_failed"])
        self.assertEqual(thread_ids[0], thread_ids[1])
        self.assertFalse(stop.is_set())
        restarted = OperationJournal(self.journal.file_path, secret_box=self.box, fail_unconfirmed=True)
        failures = [row for row in restarted.recovery_snapshot()["entries"] if row["kind"] == "exchange-cleanup"]
        self.assertEqual([row["state"] for row in failures], ["failed", "failed"])
        self.assertNotIn("cleanup balance unavailable", self.journal.file_path.read_text(encoding="utf-8"))

    def test_last_retry_can_succeed_without_nested_retry_multiplication(self):
        client = SimpleNamespace()
        calls = [0]
        def write():
            calls[0] += 1
            if calls[0] < 6:
                raise ExchangeOperationFailedError("lost result")
            return "done"
        self.assertEqual(retry_cleanup(client, lambda: retry_failed_operation(client, write)), "done")
        self.assertEqual(calls[0], 6)

    def test_cleanup_never_reposts_an_acknowledged_order_after_receipt_storage_failure(self):
        task, orders, _ = self.market_task("storage-failure")
        client = task.c
        params = {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY", "quoteOrderQty": "100"}
        with patch.object(self.journal, "update", side_effect=OSError("receipt storage unavailable")):
            with self.assertRaises(ExchangeOperationFailedError):
                retry_cleanup(client, lambda: client.request(client.spot, "POST", "/api/v3/order", params))
        self.assertEqual(len(orders), 1)
        self.assertEqual(len(client.pending_order_requests()), 1)
        restarted = OperationJournal(self.journal.file_path, secret_box=self.box)
        self.assertEqual(len(restarted.active_entries()), 1)

    def test_ladder_cleanup_keeps_acknowledged_sale_when_journal_and_balance_are_unavailable(self):
        for reason in ("manual", "bnb"):
            with self.subTest(reason=reason):
                task, orders, _ = self.market_task("storage-" + reason)
                client = task.c
                client.request(client.spot, "POST", "/api/v3/order", {
                    "symbol": "BTCUSDT", "type": "MARKET", "side": "BUY", "quoteOrderQty": "100"})
                original_get = client.session.get.side_effect
                def stale_balance(url, **kwargs):
                    response = original_get(url, **kwargs)
                    if urlsplit(url).path == "/api/v3/account":
                        response.json = lambda: {"balances": [{"asset": "BTC", "free": "1", "locked": "0"}]}
                    return response
                client.session.get.side_effect = stale_balance
                with patch.object(self.journal, "update", side_effect=OSError("receipt storage unavailable")):
                    if reason == "manual":
                        task._ladder_cleanup_after_manual_stop({}, "BTC")
                    else:
                        task._ladder_cleanup_with_retries(lambda: task._ladder_cleanup_after_bnb_stop_once({}, "BTC"))
                self.assertEqual([order["side"] for order in orders], ["BUY", "SELL"])
                self.assertTrue(task.cleanup_exhausted)
                self.assertFalse(task.cleanup_confirmed)

    def test_exhausted_cleanup_does_not_block_pending_tasks_or_shutdown(self):
        task, _, _ = self.market_task("first", (3, 6))
        app = SimpleNamespace(_cleanup_state_lock=threading.Lock(), _pending_strategy_cleanups={},
                              _closing=False, _dispatch_ui=Mock(), _retry_pending_cleanups=Mock())
        task.cleanup_callback = lambda value: ExchangeAppBase._record_strategy_cleanup(app, value)
        task.run(threading.Event())
        self.assertTrue(task.cleanup_exhausted)
        self.assertEqual(app._pending_strategy_cleanups, {})
        app._dispatch_ui.assert_not_called()

    def test_manual_stop_still_stops_trading_and_cleanup_exits_after_five_retries(self):
        task, orders, calls = self.market_task("stopped", (0,))
        stop = threading.Event()
        stop.set()
        result = task.run(stop)
        self.assertEqual(calls, {0: 6})
        self.assertEqual(orders, [])
        self.assertTrue(result["cleanup_exhausted"])
        self.assertTrue(stop.is_set())

    def test_futures_and_convert_cleanup_use_the_same_bounded_budget(self):
        for mode in ("futures", "convert"):
            with self.subTest(mode=mode):
                task, _, _ = self.market_task(mode)
                attempts = []
                def unavailable(method, path, params):
                    attempts.append((method, path))
                    raise requests.ReadTimeout("cleanup unavailable")
                signed_transport(task.c, unavailable)
                if mode == "futures":
                    task.trade_account_type = TRADE_ACCOUNT_TYPE_FUTURES
                    self.assertFalse(task.ensure_futures_position_closed())
                else:
                    task.trade_mode = TRADE_MODE_CONVERT
                    self.assertFalse(task.ensure_base_sold())
                self.assertEqual(len(attempts), 6)
                self.assertTrue(task.cleanup_exhausted)

    def test_ladder_bnb_stop_cleanup_exhausts_once_and_does_not_block_next_account(self):
        task, orders, calls = self.market_task("ladder", (0,))
        task.trade_mode = TRADE_MODE_LADDER
        task.ladder_min_pool_notional = Decimal(50)
        task.bnb_fee_stop_value = Decimal("0.01")
        result = task.run(threading.Event())
        self.assertEqual(calls, {0: 6})
        self.assertEqual(orders, [])
        self.assertTrue(result["cleanup_exhausted"])
        self.assertTrue(result["cleanup_error"])
        next_task, next_orders, _ = self.market_task("next")
        self.assertFalse(next_task.run(threading.Event())["trade_failed"])
        self.assertEqual(len(next_orders), 12)

    def test_ladder_cleanup_success_on_last_retry_clears_temporary_failure(self):
        task, _, _ = self.market_task("ladder-recovered")
        task.trade_mode = TRADE_MODE_LADDER
        calls = [0]
        def unavailable(method, path, params):
            self.assertEqual(path, "/api/v3/account")
            calls[0] += 1
            if calls[0] < 6:
                raise requests.ReadTimeout("cleanup unavailable")
            return {"balances": []}
        signed_transport(task.c, unavailable)
        task._ladder_cleanup_after_manual_stop({}, "BTC")
        self.assertEqual(calls[0], 7)  # The successful attempt also confirms the balance.
        self.assertTrue(task.cleanup_confirmed)
        self.assertTrue(task.ladder_cleanup_confirmed)
        self.assertFalse(task.cleanup_exhausted)
        self.assertEqual(task.trade_error, "")
        self.assertEqual(task.ladder_cleanup_error, "")

    def test_stock_failed_cleanup_keeps_original_inventory_and_continues_later_rounds(self):
        client = stocks.Client()
        client.operation_journal = self.journal
        raw = client._request_impl
        buys, cleanup_calls = [0], Counter()
        failed_order_ids = set()
        def network(method, path, params):
            if path.endswith("place"):
                if params["side"] == "BUY":
                    buys[0] += 1
                elif buys[0] in (3, 6):
                    failed_order_ids.add(params["clientOrderId"])
            if path.endswith("detail") and params["clientOrderId"] in failed_order_ids:
                row = client.orders[params["clientOrderId"]]
                row.update(status="PARTIALLY_FILLED", filledQty=str(Decimal(row["quantity"]) - Decimal("0.001")),
                           avgFilledPrice=row["price"], fee="0.35")
                return raw(client.spot, method, path, params)
            if path.endswith("cancel"):
                order = next(row for row in client.orders.values() if row["orderId"] == params["orderId"])
                if order["clientOrderId"] in failed_order_ids:
                    cleanup_calls[buys[0]] += 1
                    return {"status": "S"}
            return raw(client.spot, method, path, params)
        signed_transport(client, network)
        stop = stocks.Stop()
        settings = StockSettings.parse({**STOCK_FIELDS, "rounds": "6", "amount": "100", "poll": "1", "reprice": "2", "timeout": "4"})
        task = StockStrategy(client, settings, now=lambda: stocks.NOW, clock=lambda: stop.time)
        result = task.run(stop)
        self.assertFalse(result["success"], result)
        self.assertEqual(result["processed"], 6)
        self.assertEqual(result["completed"], 4)
        self.assertEqual([row["round"] for row in result["failed_rounds"]], [3, 6])
        self.assertEqual(cleanup_calls, {3: 6, 6: 6})
        self.assertEqual([row["side"] for row in client.posts], ["BUY", "SELL"] * 6)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(self.journal.active_entries(), [])
        failures = [row for row in self.journal.recovery_snapshot()["entries"] if row["state"] == "failed"]
        self.assertTrue(all(row["payload"]["active"]["clientOrderId"] in failed_order_ids for row in failures))
        self.assertEqual(client.holdings - Decimal(70), Decimal(result["inventory"]))


if __name__ == "__main__":
    unittest.main()
