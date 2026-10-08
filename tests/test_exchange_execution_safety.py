from decimal import Decimal
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import Mock, patch

import requests

from exchange_app_base import ExchangeAppBase
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceAPIError, BinanceClient, RequestStoppedError, SharedRateLimitGate, UnconfirmedOperationError, UnconfirmedOrderError
from exchange_constants import TRADE_ACCOUNT_TYPE_FUTURES, MARKET_STOP_MODE_BNB
from exchange_order_recovery import reconcile_exchange_orders
from exchange_strategy import Strategy
from operation_journal import OperationJournal
from page_onchain_base import OnchainTransferPageBase
from retry_utils import RetryPolicy


class _Journal:
    def __init__(self):
        self.entries = {}

    def begin(self, kind, scope, payload):
        entry_id = str(len(self.entries) + 1)
        self.entries[entry_id] = {"id": entry_id, "kind": kind, "scope": scope, "state": "prepared", "payload": dict(payload)}
        return entry_id

    def update(self, entry_id, state, **changes):
        self.entries[entry_id]["state"] = state
        self.entries[entry_id]["payload"].update(changes)

    def active_entries(self, **filters):
        return [entry for entry in self.entries.values() if entry["state"] not in {"completed", "cancelled", "failed"} and all(entry[key] == value for key, value in filters.items())]


class _Client(BinanceClient):
    DEFAULT_REQUEST_RETRY_COUNT = 2
    DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 0

    def __init__(self, *, lose_reply=False, market="spot"):
        self.journal = _Journal()
        self.stop = threading.Event()
        super().__init__("test-key", "test-secret", request_gate=SharedRateLimitGate(0), request_stop_event=self.stop, operation_journal=self.journal)
        self.lose_reply = lose_reply
        self.market = market
        self.requests = []
        self.orders = {}
        self.cancel_succeeds = True
        self.lookup_missing = False
        self.sale_calls = []
        self.has_base = True

    def _build_session(self):
        return Mock()

    def _request_impl(self, base, method, path, params=None, **kwargs):
        params = dict(params or {})
        self.requests.append((method, path, params))
        if path in self.ORDER_PATHS:
            if method == "POST":
                active = self.journal.active_entries()
                assert any(entry["payload"].get("client_order_id") == params["newClientOrderId"] for entry in active), "POST must follow durable intent"
                order = {"symbol": params["symbol"], "orderId": len(self.orders)+1, "clientOrderId": params["newClientOrderId"], "side": params.get("side", "BUY"), "status": "FILLED" if params.get("type") == "MARKET" else "NEW"}
                self.orders[params["newClientOrderId"]] = order
                if self.lose_reply:
                    raise requests.Timeout("accepted, reply lost")
                return dict(order)
            if self.lookup_missing:
                raise BinanceAPIError(-2013, "Order does not exist")
            order = next((value for key, value in self.orders.items() if key == params.get("origClientOrderId") or str(value["orderId"]) == str(params.get("orderId"))), None)
            if order is None:
                raise BinanceAPIError(-2013, "Order does not exist")
            if method == "DELETE" and self.cancel_succeeds:
                order["status"] = "CANCELED"
            return dict(order)
        raise requests.Timeout("financial write reply lost")

    def spot_sell_all_base(self, symbol, **kwargs):
        self._wait_for_request_gate()
        assert self._cleanup_request_depth > 0
        self.sale_calls.append(symbol)
        self.has_base = False
        return True

    def spot_market_sellable_base_quantity(self, symbol):
        return Decimal(1 if self.has_base else 0)


def _strategy(client, **kwargs):
    return Strategy(client, spot_rounds=1, withdraw_coin="USDT", withdraw_address="", withdraw_network="", withdraw_fee_buffer=0, spot_symbol=kwargs.pop("spot_symbol", "BTCUSDT"), sleep_fn=lambda: None, enable_withdraw=False, operation_journal=client.journal, **kwargs)


class _SubmissionOutage:
    def __init__(self, client, *, lost_attempts=1, create_original=True, query_times=(10, 10, 10)):
        self.client = client
        self.original_request = client._request_impl
        self.lost_attempts = lost_attempts
        self.create_original = create_original
        self.query_times = query_times
        self.now = 0.0
        self.posts = []
        self.queries = []
        self.query_count = {}
        client._request_impl = self.request

    def request(self, base, method, path, params=None, **kwargs):
        params = dict(params or {})
        if method == "POST" and path in self.client.ORDER_PATHS:
            self.posts.append((self.now, dict(params)))
            self.client.lose_reply = len(self.posts) <= self.lost_attempts
            if self.client.lose_reply and not self.create_original:
                raise requests.ReadTimeout("request never reached the exchange")
        client_id = params.get("origClientOrderId")
        lost_ids = [item[1]["newClientOrderId"] for item in self.posts[:self.lost_attempts]]
        if method == "GET" and client_id in lost_ids and not self.client._cleanup_request_depth:
            count = self.query_count.get(client_id, 0)
            self.query_count[client_id] = count + 1
            self.queries.append((self.now, kwargs["timeout"], client_id))
            if count < len(self.query_times):
                self.now += self.query_times[count]
                raise requests.ReadTimeout("status unavailable")
        return self.original_request(base, method, path, params, **kwargs)

    def clock(self):
        return self.now


class ExchangeExecutionSafetyTests(unittest.TestCase):
    def test_cleanup_query_recovers_beyond_ten_failures_then_withdraws_once(self):
        client = _Client()
        original_request = client._request_impl
        cleanup_queries = 0
        def request(base, method, path, params=None, **kwargs):
            nonlocal cleanup_queries
            if method == "GET" and client._cleanup_request_depth:
                cleanup_queries += 1
                if cleanup_queries <= 12:
                    raise requests.ReadTimeout("cleanup network unavailable")
            return original_request(base, method, path, params, **kwargs)
        client._request_impl = request
        outage = _SubmissionOutage(client)
        notifications = Mock()
        callback = Mock()
        strategy = _strategy(client, cleanup_callback=notifications, withdraw_callback=callback)
        strategy.withdraw_address = "destination"
        strategy.enable_withdraw = True
        strategy._run_bnb_topup_if_needed = Mock()
        def trading(*_args, **_kwargs):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
            strategy.market_round_total = strategy.market_round_completed = 1
        strategy._run_market_mode = Mock(side_effect=trading)
        def withdraw(**_kwargs):
            self.assertFalse(client.has_base)
            self.assertEqual(client.journal.active_entries(), [])
            return 10.0
        client.withdraw_all_coin = Mock(side_effect=withdraw)
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock), patch.object(strategy, "_pause_with_stop", return_value=False):
            result = strategy.run(client.stop)
        self.assertEqual(cleanup_queries, 13)
        self.assertEqual(result["market_round_completed"], 1)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertTrue(result["withdraw_attempted"])
        self.assertEqual(result["withdraw_amount"], 10.0)
        strategy._run_market_mode.assert_called_once()
        client.withdraw_all_coin.assert_called_once()
        callback.assert_called_once_with(10.0)
        notifications.assert_called_once_with(strategy)
        self.assertEqual(len(outage.posts), 1)
        self.assertEqual(client.pending_order_requests(), [])

    def test_cleanup_balance_recovers_after_old_order_has_already_been_resolved(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy.enable_withdraw = True
        strategy._run_bnb_topup_if_needed = Mock()
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
            client.orders[order["clientOrderId"]]["status"] = "FILLED"
        strategy._run_market_mode = Mock(side_effect=trading)
        original_sell = client.spot_sell_all_base
        sales = 0
        def sell(*args, **kwargs):
            nonlocal sales
            sales += 1
            self.assertEqual(client.pending_order_requests(), [])
            if sales == 1:
                raise requests.ReadTimeout("balance query failed after old order resolved")
            return original_sell(*args, **kwargs)
        client.spot_sell_all_base = sell
        client.withdraw_all_coin = Mock(return_value=10.0)
        with patch.object(strategy, "_pause_with_stop", return_value=False):
            result = strategy.run(client.stop)
        self.assertEqual(sales, 2)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertEqual(result["withdraw_amount"], 10.0)
        client.withdraw_all_coin.assert_called_once()
        strategy._run_market_mode.assert_called_once()

    def test_stop_during_completion_cleanup_wait_skips_withdrawal_and_still_cleans(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy.enable_withdraw = True
        strategy._run_bnb_topup_if_needed = Mock()
        original_query = client.get_order_by_client_id
        query_count = 0
        def query(*args, **kwargs):
            nonlocal query_count
            query_count += 1
            if query_count == 1:
                raise requests.ReadTimeout("temporary cleanup query failure")
            return original_query(*args, **kwargs)
        client.get_order_by_client_id = query
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy._run_market_mode = trading
        client.withdraw_all_coin = Mock(return_value=10.0)
        def stop_wait(_stop, _seconds):
            client.stop.set()
            return True
        with patch.object(strategy, "_pause_with_stop", side_effect=stop_wait):
            result = strategy.run(client.stop)
        self.assertTrue(client.stop.is_set())
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertFalse(result["withdraw_attempted"])
        client.withdraw_all_coin.assert_not_called()
        self.assertEqual(client.journal.active_entries(), [])
        self.assertFalse(client.has_base)

    def test_cleanup_recovery_preserves_the_original_trade_failure(self):
        for futures in (False, True):
            with self.subTest(futures=futures):
                client = _Client()
                strategy = _strategy(client, **({"trade_account_type": TRADE_ACCOUNT_TYPE_FUTURES} if futures else {}))
                strategy.withdraw_address = "destination"
                strategy.enable_withdraw = True
                strategy._run_bnb_topup_if_needed = Mock()
                client.get_order_by_client_id = Mock(side_effect=[requests.ReadTimeout("temporary cleanup outage"),
                                                                  {"symbol": "BTCUSDT", "orderId": 1, "status": "FILLED"}])
                def trading(*_args, **_kwargs):
                    path = "/fapi/v1/order" if futures else "/api/v3/order"
                    base = client.um_futures if futures else client.spot
                    order = client.request(base, "POST", path, {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
                    client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
                    strategy.trade_error = "definite trade rejection"
                    strategy.trade_error_stage = "开仓下单" if futures else "下单"
                trading_handler = Mock(side_effect=trading)
                if futures:
                    strategy._run_futures_mode = trading_handler
                    client.close_all_um_futures_positions_market = Mock(return_value=[])
                    client.get_um_futures_positions = Mock(return_value=[])
                else:
                    strategy._run_market_mode = trading_handler
                client.withdraw_all_coin = Mock(return_value=10.0)
                with patch.object(strategy, "_pause_with_stop", return_value=False):
                    result = strategy.run(client.stop)
                self.assertEqual(client.get_order_by_client_id.call_count, 2)
                self.assertTrue(result["cleanup_confirmed"])
                self.assertTrue(result["trade_failed"])
                self.assertEqual(result["trade_error"], "definite trade rejection")
                self.assertEqual(result["trade_error_stage"], "开仓下单" if futures else "下单")
                trading_handler.assert_called_once()
                if futures:
                    client.withdraw_all_coin.assert_called_once()
                    self.assertTrue(client.withdraw_all_coin.call_args.kwargs["auto_collect_to_spot"])
                else:
                    client.withdraw_all_coin.assert_not_called()

    def test_stop_after_cleanup_recovers_but_before_withdrawal_skips_withdrawal(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy.enable_withdraw = True
        strategy.sleep_fn = client.stop.set
        strategy._run_bnb_topup_if_needed = Mock()
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy._run_market_mode = Mock(side_effect=trading)
        client.get_order_by_client_id = Mock(side_effect=[requests.ReadTimeout("temporary cleanup outage"),
                                                          {"symbol": "BTCUSDT", "orderId": 1, "status": "FILLED"}])
        client.withdraw_all_coin = Mock(return_value=10.0)
        with patch.object(strategy, "_pause_with_stop", return_value=False):
            result = strategy.run(client.stop)
        self.assertTrue(client.stop.is_set())
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertFalse(result["withdraw_attempted"])
        client.withdraw_all_coin.assert_not_called()

    def test_cleanup_recovers_without_submitting_withdrawal_when_unchecked(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy._run_bnb_topup_if_needed = Mock()
        client.spot_asset_balance_decimal = Mock(return_value=Decimal("10"))
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy._run_market_mode = Mock(side_effect=trading)
        client.get_order_by_client_id = Mock(side_effect=[None, {"symbol": "BTCUSDT", "orderId": 1, "status": "FILLED"}])
        with patch.object(strategy, "_pause_with_stop", return_value=False):
            result = strategy.run(client.stop)
        self.assertEqual(client.get_order_by_client_id.call_count, 2)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertEqual(result["withdraw_amount"], 0.0)
        self.assertFalse(any(method == "POST" and "withdraw/apply" in path for method, path, _params in client.requests))
        strategy._run_market_mode.assert_called_once()

    def test_malformed_cleanup_reply_cannot_confirm_exit_or_clear_an_unresolved_order(self):
        for reply in (None, {}, {"status": "FILLED"}, {"orderId": 1, "status": "FILLED", "clientOrderId": "other-order"}):
            with self.subTest(reply=reply):
                client = _Client()
                client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
                client.get_order_by_client_id = Mock(return_value=reply)
                strategy = _strategy(client)
                strategy._cleanup_after_stop()
                self.assertFalse(strategy.cleanup_confirmed)
                self.assertEqual(client.sale_calls, [])
                self.assertEqual(len(client.journal.active_entries()), 1)
                self.assertEqual(len(client.pending_order_requests()), 1)

    def test_incomplete_order_reply_cannot_close_an_unconfirmed_journal_entry(self):
        client = _Client()
        entry_id = client.journal.begin("exchange-order", client._order_scope(), {"symbol": "BTCUSDT", "client_order_id": "original"})
        client._pending_order_requests["original"] = {"symbol": "BTCUSDT", "client_order_id": "original", "market": "spot", "entry_id": entry_id}
        with self.assertRaises(ValueError):
            client.record_order_state("/api/v3/order", {"symbol": "BTCUSDT", "status": "FILLED"})
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertEqual(len(client.pending_order_requests()), 1)

    def test_withdrawal_uncertainty_preserves_attempted_stage_and_round_result(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy._run_bnb_topup_if_needed = Mock()
        strategy._run_market_mode = Mock()
        client.withdraw_all_coin = Mock(side_effect=UnconfirmedOperationError("withdrawal reply unconfirmed"))
        result = strategy.run(client.stop)
        self.assertTrue(result["withdraw_attempted"])
        self.assertIn("unconfirmed", result["withdraw_error"])
        self.assertFalse(result["trade_failed"])
        client.withdraw_all_coin.assert_called_once()

    def test_cleanup_notification_failure_still_restores_client_request_state(self):
        client = _Client()
        strategy = _strategy(client, cleanup_callback=Mock(side_effect=RuntimeError("notification failed")))
        strategy._run_steps = lambda *_args, **_kwargs: strategy._strategy_result()
        client.stop.set()
        with self.assertRaisesRegex(RuntimeError, "notification failed"):
            strategy.run(client.stop)
        self.assertIs(client._request_stop_event, client.stop)
        self.assertFalse(client._continuous_trading)
        self.assertIsNone(client._recovery_check)
        self.assertIsNone(strategy._running_stop_event)

    def test_timed_out_old_order_is_cleaned_before_final_withdrawal(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy._run_bnb_topup_if_needed = Mock()
        events = []
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy._run_market_mode = trading
        original_sell = client.spot_sell_all_base
        def sell(*args, **kwargs):
            events.append("clear")
            return original_sell(*args, **kwargs)
        client.spot_sell_all_base = sell
        def withdraw(**_kwargs):
            events.append("withdraw")
            self.assertFalse(client.has_base)
            self.assertEqual(client.journal.active_entries(), [])
            return 10.0
        client.withdraw_all_coin = withdraw
        result = strategy.run(client.stop)
        self.assertEqual(events, ["clear", "withdraw"])
        self.assertEqual(result["withdraw_amount"], 10.0)
        self.assertTrue(result["cleanup_confirmed"])

    def test_unconfirmed_old_order_cleanup_blocks_final_withdrawal(self):
        client = _Client()
        client.cancel_succeeds = False
        strategy = _strategy(client)
        strategy.withdraw_address = "destination"
        strategy._run_bnb_topup_if_needed = Mock()
        def trading(*_args, **_kwargs):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy._run_market_mode = trading
        client.withdraw_all_coin = Mock(return_value=10.0)
        def stop_wait(_stop, _seconds):
            client.withdraw_all_coin.assert_not_called()
            client.stop.set()
            return True
        with patch.object(strategy, "_pause_with_stop", side_effect=stop_wait):
            result = strategy.run(client.stop)
        client.withdraw_all_coin.assert_not_called()
        self.assertFalse(result["withdraw_attempted"])
        self.assertFalse(result["cleanup_confirmed"])
        self.assertEqual(client.sale_calls, [])
        self.assertEqual(len(client.journal.active_entries()), 1)

    def test_old_order_reconciliation_invalidates_previous_ladder_clearance(self):
        client = _Client()
        order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
        client._pending_order_requests[order["clientOrderId"]]["confirmation_timed_out"] = True
        strategy = _strategy(client)
        strategy.ladder_cleanup_confirmed = True
        strategy._cleanup_after_stop()
        self.assertTrue(strategy.cleanup_confirmed)
        self.assertFalse(client.has_base)
        self.assertEqual(client.sale_calls, ["BTCUSDT"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_timeout_resubmission_keeps_round_count_and_sells_actual_spot_exposure(self):
        client = _Client()
        position = Decimal(0)
        original = client._request_impl
        def execution(base, method, path, params=None, **kwargs):
            nonlocal position
            try:
                return original(base, method, path, params, **kwargs)
            finally:
                if method == "POST":
                    position += Decimal(params["quantity"]) * (1 if params["side"] == "BUY" else -1)
        client._request_impl = execution
        outage = _SubmissionOutage(client)
        client.get_spot_base_asset = Mock(return_value="BTC")
        client.get_spot_quote_asset = Mock(return_value="USDT")
        client.collect_asset_to_spot = Mock(return_value=0)
        client.spot_market_sellable_base_quantity = lambda _symbol: position
        client.spot_buy_all_usdt = lambda **kwargs: client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY", "quantity": "1"})
        def sell(*args, **kwargs):
            if position <= 0:
                return False
            return client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "SELL", "quantity": str(position)})
        client.spot_sell_all_base = sell
        strategy = _strategy(client)
        def steps(stop, **kwargs):
            strategy._run_market_mode(stop)
            return strategy._strategy_result()
        strategy._run_steps = steps
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            result = strategy.run(client.stop)
        self.assertEqual(result["market_round_completed"], 1)
        self.assertEqual(result["market_round_failed_count"], 0)
        self.assertEqual([item[1]["side"] for item in outage.posts], ["BUY", "SELL"])
        self.assertEqual(Decimal(outage.posts[-1][1]["quantity"]), 1)
        self.assertEqual(position, 0)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_futures_timeout_resubmission_cleanup_closes_actual_total_position(self):
        client = _Client()
        position = Decimal(0)
        original = client._request_impl
        def execution(base, method, path, params=None, **kwargs):
            nonlocal position
            try:
                return original(base, method, path, params, **kwargs)
            finally:
                if method == "POST":
                    position += Decimal(params["quantity"]) * (1 if params["side"] == "BUY" else -1)
        client._request_impl = execution
        outage = _SubmissionOutage(client)
        client.get_um_futures_positions = lambda _symbol: [{"positionAmt": str(position)}]
        def close(symbol):
            if position == 0:
                return []
            return [client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": symbol, "type": "MARKET", "side": "SELL", "quantity": str(position), "reduceOnly": "true"})]
        client.close_all_um_futures_positions_market = close
        strategy = _strategy(client, trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES)
        def steps(*args, **kwargs):
            client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY", "quantity": "1"})
            return strategy._strategy_result()
        strategy._run_steps = steps
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            result = strategy.run(client.stop)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertEqual([item[1]["side"] for item in outage.posts], ["BUY", "SELL"])
        self.assertEqual(Decimal(outage.posts[-1][1]["quantity"]), 1)
        self.assertEqual(outage.posts[-1][1]["reduceOnly"], "true")
        self.assertEqual(position, 0)
        self.assertEqual(client.journal.active_entries(), [])

    def test_bnb_threshold_at_timeout_boundary_prevents_resubmission_and_cleans_up(self):
        client = _Client()
        outage = _SubmissionOutage(client)
        original = client._request_impl
        def request(base, method, path, params=None, **kwargs):
            if path == "/api/v3/account":
                return {"balances": [{"asset": "BNB", "free": "1" if outage.now < 30 else "0.001"}]}
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        strategy = _strategy(client, market_stop_mode=MARKET_STOP_MODE_BNB, bnb_fee_stop_value=Decimal("0.01"))
        strategy._recovery_bnb_enabled = True
        strategy._run_steps = lambda *_args, **_kwargs: client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            result = strategy.run(client.stop)
        self.assertEqual(len(outage.posts), 1)
        self.assertTrue(strategy._recovery_stop_event.is_set())
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertEqual(client.sale_calls, ["BTCUSDT"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_unconfirmed_spot_limit_and_futures_submissions_query_original_after_thirty_seconds(self):
        for path, order_type in (("/api/v3/order", "MARKET"), ("/api/v3/order", "LIMIT"), ("/fapi/v1/order", "MARKET")):
            with self.subTest(path=path, order_type=order_type):
                client = _Client()
                outage = _SubmissionOutage(client)
                params = {"symbol": "BTCUSDT", "type": order_type, "side": "BUY", "quantity": "1"}
                with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
                    order = client.request(client.spot if path == "/api/v3/order" else client.um_futures, "POST", path, params)
                self.assertEqual([item[0] for item in outage.posts], [0])
                original_id = outage.posts[0][1]["newClientOrderId"]
                self.assertEqual(order["clientOrderId"], original_id)
                self.assertEqual(len(client.journal.entries), 1)
                self.assertEqual(client.journal.entries["1"]["state"], "completed" if order_type == "MARKET" else "open")

    def test_confirmation_recovers_before_thirty_seconds_without_another_post(self):
        client = _Client()
        outage = _SubmissionOutage(client, query_times=(29,))
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual(order["status"], "FILLED")
        self.assertEqual(len(outage.posts), 1)
        self.assertEqual(outage.queries[-1][1], 1)
        self.assertEqual(client.journal.active_entries(), [])

    def test_repeated_timeouts_restart_grace_per_submission_with_distinct_identifiers(self):
        client = _Client()
        outage = _SubmissionOutage(client, lost_attempts=2, create_original=False)
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual([item[0] for item in outage.posts], [0, 30, 60])
        self.assertEqual(len({item[1]["newClientOrderId"] for item in outage.posts}), 3)
        self.assertEqual(order["orderId"], 1)
        self.assertEqual([entry["state"] for entry in client.journal.entries.values()], ["failed", "failed", "completed"])

    def test_query_timeout_is_limited_to_remaining_confirmation_grace(self):
        client = _Client()
        outage = _SubmissionOutage(client, query_times=(26, 4))
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual([item[1] for item in outage.queries], [6, 4])
        self.assertEqual(outage.now, 30)

    def test_stop_at_grace_boundary_prevents_resubmission(self):
        client = _Client()
        outage = _SubmissionOutage(client)
        def check_stop(**kwargs):
            if outage.now >= 30:
                client.stop.set()
        client._recovery_check = check_stop
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock), self.assertRaises(RequestStoppedError):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual(len(outage.posts), 1)
        self.assertEqual(client.journal.entries["1"]["state"], "unconfirmed")

    def test_automatic_ladder_rechecks_market_guard_before_timeout_resubmission(self):
        client = _Client()
        outage = _SubmissionOutage(client)
        guard = Mock(side_effect=[True, False])
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock), self.assertRaisesRegex(RuntimeError, "行情不再达标"):
            client._place_limit_order_with_recovery("BTCUSDT", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"}, guard)
        self.assertEqual(len(outage.posts), 1)
        self.assertEqual(guard.call_count, 2)
        self.assertEqual(len(client.journal.entries), 1)
        self.assertEqual(client.journal.entries["1"]["state"], "unconfirmed")
        self.assertEqual(len(client.pending_order_requests()), 1)

    def test_task_completion_automatically_reconciles_old_attempt_and_clears_exposure(self):
        for create_original in (True, False):
            with self.subTest(create_original=create_original):
                client = _Client()
                outage = _SubmissionOutage(client, create_original=create_original)
                strategy = _strategy(client)
                def steps(*args, **kwargs):
                    client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
                    return strategy._strategy_result()
                strategy._run_steps = steps
                with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
                    result = strategy.run(client.stop)
                self.assertTrue(result["cleanup_confirmed"])
                self.assertFalse(result["trade_failed"])
                self.assertEqual(len(outage.posts), 1 if create_original else 2)
                self.assertEqual(client.journal.entries["1"]["state"], "completed" if create_original else "failed")
                self.assertEqual(client.journal.active_entries(), [])
                self.assertEqual(client.pending_order_requests(), [])
                self.assertEqual(client.sale_calls, ["BTCUSDT"])
                if not create_original:
                    self.assertTrue(any(method == "DELETE" and params.get("origClientOrderId") == outage.posts[0][1]["newClientOrderId"] for method, _, params in client.requests))

    def test_stop_cancels_both_old_and_replacement_limit_orders_before_clearance(self):
        client = _Client()
        outage = _SubmissionOutage(client)
        strategy = _strategy(client)
        def steps(*args, **kwargs):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client.stop.set()
            return strategy._strategy_result()
        strategy._run_steps = steps
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            result = strategy.run(client.stop)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertEqual([order["status"] for order in client.orders.values()], ["CANCELED"])
        self.assertEqual(client.journal.active_entries(), [])
        self.assertEqual(client.pending_order_requests(), [])
        self.assertEqual(client.sale_calls, ["BTCUSDT"])

    def test_unavailable_network_during_old_attempt_cleanup_retains_recoverable_state(self):
        client = _Client()
        outage = _SubmissionOutage(client)
        strategy = _strategy(client)
        def steps(*args, **kwargs):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT"})
            client.lookup_missing = True
            original = client._request_impl
            def request(*args, **kwargs):
                if args[1] == "DELETE":
                    raise requests.ReadTimeout("cancel status unavailable")
                return original(*args, **kwargs)
            client._request_impl = request
            return strategy._strategy_result()
        strategy._run_steps = steps
        with patch("exchange_binance_client.time.monotonic", side_effect=outage.clock):
            result = strategy.run(client.stop)
        self.assertFalse(result["cleanup_confirmed"])
        self.assertTrue(strategy.stop_cleanup_requested)
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertEqual(client.journal.entries["1"]["state"], "open")
        self.assertEqual(client.sale_calls, [])
        client.lookup_missing = False
        client._request_impl = outage.original_request
        self.assertTrue(strategy.retry_stop_cleanup())
        self.assertEqual(client.journal.active_entries(), [])

    def test_recovery_bnb_stop_is_enabled_only_after_topup_and_during_trade_stage(self):
        client = _Client()
        strategy = _strategy(client, market_stop_mode=MARKET_STOP_MODE_BNB, bnb_fee_stop_value=Decimal("0.01"))
        client._request_impl = Mock(return_value={"balances": [{"asset": "BNB", "free": "0.001"}]})
        def topup(stop):
            strategy._check_recovery_stop()
            self.assertFalse(strategy._recovery_stop_event.is_set())
            client._request_impl.assert_not_called()
        strategy._run_bnb_topup_if_needed = topup
        def trade(*args, **kwargs):
            strategy._check_recovery_stop()
            self.assertTrue(strategy._recovery_stop_event.is_set())
        strategy._run_market_mode = trade
        result = strategy.run(client.stop)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(strategy._recovery_bnb_enabled)
        self.assertFalse(result["trade_failed"])

    def test_incomplete_market_query_reply_is_retried_without_losing_order_or_journal(self):
        client = _Client()
        original = client._request_impl
        query_replies = [{"status": "NEW"}, {"orderId": 999, "status": "FILLED"}]
        def request(base, method, path, params, **kwargs):
            if method == "POST":
                return {**original(base, method, path, params, **kwargs), "status": "NEW"}
            if query_replies:
                return query_replies.pop(0)
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual(order["orderId"], 1)
        self.assertEqual(order["status"], "FILLED")
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
        self.assertEqual(client.journal.active_entries(), [])
        self.assertEqual(client.pending_order_requests(), [])

    def test_journal_update_failure_after_acceptance_cannot_repeat_order(self):
        client = _Client()
        strategy = _strategy(client)
        client.journal.update = Mock(side_effect=OSError("disk write failed"))
        with self.assertRaises(UnconfirmedOperationError):
            strategy._run_trade_action_with_retry(client.stop, "buy", lambda: client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"}))
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertEqual(client._order_submission_serial, 1)

    def test_market_rounds_finish_in_order_after_long_lost_reply_outage(self):
        client = _Client(lose_reply=True)
        client.has_base = False
        strategy = _strategy(client)
        strategy.spot_rounds = 2
        client.get_spot_base_asset = Mock(return_value="BTC")
        client.get_spot_quote_asset = Mock(return_value="USDT")
        client.collect_asset_to_spot = Mock(return_value=0)
        original = client._request_impl
        query_counts = {}
        def request(base, method, path, params=None, **kwargs):
            if method == "POST":
                client.has_base = params.get("side") == "BUY"
            if method == "GET":
                client_id = params["origClientOrderId"]
                query_counts[client_id] = query_counts.get(client_id, 0) + 1
                if query_counts[client_id] <= 15:
                    raise requests.ReadTimeout("response unavailable")
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        def buy(**kwargs):
            return client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
        def sell(**kwargs):
            if not client.has_base:
                return False
            return client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "SELL"})
        client.spot_buy_all_usdt = buy
        client.spot_sell_all_base = sell
        def steps(stop, **kwargs):
            strategy._run_market_mode(stop)
            return strategy._strategy_result()
        strategy._run_steps = steps
        result = strategy.run(client.stop)
        self.assertEqual(result["market_round_completed"], 2)
        self.assertEqual(result["market_round_failed_count"], 0)
        self.assertFalse(result["trade_failed"])
        self.assertFalse(client.has_base)
        self.assertEqual([params["side"] for method, _, params in client.requests if method == "POST"], ["BUY", "SELL", "BUY", "SELL"])
        self.assertEqual(list(query_counts.values()), [16] * 4)
        self.assertEqual(client.journal.active_entries(), [])

    def test_futures_open_and_close_finish_all_rounds_after_lost_reply_outage(self):
        client = _Client(lose_reply=True)
        strategy = _strategy(client, trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES, futures_rounds=2)
        strategy.ensure_futures_position_closed = Mock()
        strategy._log_futures_symbol_settings = Mock()
        strategy._ensure_futures_available_balance = Mock()
        strategy._prepare_futures_symbol_settings = Mock()
        client.calculate_um_futures_order_quantity = Mock(return_value=Decimal(1))
        original = client._request_impl
        query_counts = {}
        def request(base, method, path, params=None, **kwargs):
            if method == "GET":
                client_id = params["origClientOrderId"]
                query_counts[client_id] = query_counts.get(client_id, 0) + 1
                if query_counts[client_id] <= 15:
                    raise requests.ReadTimeout("query unavailable")
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        client.place_um_futures_market_order = lambda symbol, side, quantity: client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": symbol, "type": "MARKET", "side": side, "quantity": str(quantity)})
        client.close_um_futures_position_market = lambda symbol: client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": symbol, "type": "MARKET", "side": "SELL", "reduceOnly": "true"})
        progress = Mock()
        def steps(stop, **kwargs):
            strategy._run_futures_mode(stop, progress_cb=progress)
            return strategy._strategy_result()
        strategy._run_steps = steps
        result = strategy.run(client.stop)
        self.assertFalse(result["trade_failed"])
        self.assertEqual(progress.call_args.args[0], 2)
        self.assertEqual([params["side"] for method, _, params in client.requests if method == "POST"], ["BUY", "SELL", "BUY", "SELL"])
        self.assertEqual(list(query_counts.values()), [16] * 4)
        self.assertEqual(client.journal.active_entries(), [])

    def test_stop_before_delivery_retries_does_not_leave_unknown_intent(self):
        client = _Client()
        def request(*args, **kwargs):
            client.stop.set()
            raise requests.ConnectTimeout("not delivered")
        client._request_impl = request
        with self.assertRaises(RequestStoppedError):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual(client.journal.active_entries(), [])
        self.assertEqual(client.pending_order_requests(), [])

    def test_futures_explicit_503_failure_retries_but_unknown_503_queries(self):
        for message, expected_posts in (("Service Unavailable.", 2), ("Unknown error, please check your request or try again later.", 1)):
            with self.subTest(message=message):
                client = _Client()
                original = client._request_impl
                posts = []
                def request(base, method, path, params, **kwargs):
                    if method == "POST":
                        posts.append(params["newClientOrderId"])
                        if len(posts) == 1:
                            if message.startswith("Unknown"):
                                original(base, method, path, params, **kwargs)
                            raise BinanceAPIError(-1000, message, status_code=503)
                    return original(base, method, path, params, **kwargs)
                client._request_impl = request
                order = client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": "BTCUSDT", "type": "MARKET"})
                self.assertEqual(order["status"], "FILLED")
                self.assertEqual(len(posts), expected_posts)
                self.assertEqual(len(set(posts)), 1)

    def test_connection_timeout_before_delivery_resubmits_same_intent_until_accepted(self):
        for market, path in (("spot", "/api/v3/order"), ("um_futures", "/fapi/v1/order")):
            with self.subTest(market=market):
                client = _Client()
                original = client._request_impl
                posts = []
                def request(base, method, order_path, params, **kwargs):
                    if method == "POST":
                        posts.append(params["newClientOrderId"])
                        if len(posts) <= 15:
                            raise requests.ConnectTimeout("connection was never established")
                    return original(base, method, order_path, params, **kwargs)
                client._request_impl = request
                result = client.request(client.spot if market == "spot" else client.um_futures, "POST", path, {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
                self.assertEqual(result["status"], "FILLED")
                self.assertEqual(len(posts), 16)
                self.assertEqual(len(set(posts)), 1)
                self.assertEqual(len(client.journal.entries), 1)
                self.assertEqual(client.journal.active_entries(), [])

    def test_reply_loss_and_long_query_outage_recover_original_spot_and_futures_order(self):
        for market, path in (("spot", "/api/v3/order"), ("um_futures", "/fapi/v1/order")):
            with self.subTest(market=market):
                client = _Client(lose_reply=True)
                original = client._request_impl
                queries = 0
                def request(base, method, order_path, params, **kwargs):
                    nonlocal queries
                    if method == "GET":
                        queries += 1
                        if queries <= 15:
                            raise requests.ReadTimeout("status reply temporarily unavailable")
                    return original(base, method, order_path, params, **kwargs)
                client._request_impl = request
                result = client.request(client.spot if market == "spot" else client.um_futures, "POST", path, {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
                self.assertEqual(result["status"], "FILLED")
                self.assertEqual(queries, 16)
                self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
                self.assertEqual(client.pending_order_requests(), [])

    def test_market_ack_waits_through_query_outage_for_terminal_result(self):
        for path in ("/api/v3/order", "/fapi/v1/order"):
            with self.subTest(path=path):
                client = _Client()
                original = client._request_impl
                queries = 0
                def request(base, method, order_path, params, **kwargs):
                    nonlocal queries
                    if method == "POST":
                        result = original(base, method, order_path, params, **kwargs)
                        client.orders[params["newClientOrderId"]]["status"] = "NEW"
                        return {**result, "status": "NEW"}
                    queries += 1
                    if queries <= 15:
                        raise requests.ReadTimeout("query offline")
                    if queries >= 18:
                        next(iter(client.orders.values()))["status"] = "FILLED"
                    return original(base, method, order_path, params, **kwargs)
                client._request_impl = request
                result = client.request(client.spot, "POST", path, {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
                self.assertEqual(result["status"], "FILLED")
                self.assertEqual(queries, 18)
                self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
                self.assertEqual(client.journal.active_entries(), [])

    def test_manual_stop_during_confirmation_finishes_cleanup_without_trade_failure(self):
        client = _Client(lose_reply=True)
        strategy = _strategy(client)
        original = client._request_impl
        queries = 0
        def request(base, method, path, params, **kwargs):
            nonlocal queries
            if method == "GET" and not client._cleanup_request_depth:
                queries += 1
                if queries == 15:
                    client.stop.set()
                raise requests.ReadTimeout("status unavailable")
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        def steps(*args, **kwargs):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
            self.fail("stop must not execute the next step")
        strategy._run_steps = steps
        result = strategy.run(client.stop)
        self.assertFalse(result["trade_failed"])
        self.assertTrue(result["cleanup_confirmed"])
        self.assertEqual(client.sale_calls, ["BTCUSDT"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_bnb_threshold_during_confirmation_cleans_up_and_ends_task(self):
        client = _Client(lose_reply=True)
        strategy = _strategy(client, market_stop_mode=MARKET_STOP_MODE_BNB, bnb_fee_stop_value=Decimal("0.01"))
        strategy._recovery_bnb_enabled = True
        original = client._request_impl
        def request(base, method, path, params=None, **kwargs):
            if path == "/api/v3/account":
                return {"balances": [{"asset": "BNB", "free": "0.001"}]}
            if method == "GET" and not client._cleanup_request_depth:
                raise requests.ReadTimeout("confirmation offline")
            return original(base, method, path, params, **kwargs)
        client._request_impl = request
        strategy._run_steps = lambda *_args, **_kwargs: client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        result = strategy.run(client.stop)
        self.assertFalse(result["trade_failed"])
        self.assertTrue(result["cleanup_confirmed"])
        self.assertTrue(strategy._recovery_stop_event.is_set())
        self.assertEqual(client.sale_calls, ["BTCUSDT"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_running_read_recovers_beyond_ten_errors_and_stop_remains_interruptible(self):
        client = _Client()
        client._continuous_trading = True
        client._request_impl = Mock(side_effect=[requests.ReadTimeout("offline")] * 15 + [{"balances": []}])
        self.assertEqual(client.request(client.spot, "GET", "/api/v3/account"), {"balances": []})
        self.assertEqual(client._request_impl.call_count, 16)
        def stop_during_read(*args, **kwargs):
            client.stop.set()
            raise requests.ReadTimeout("offline")
        client._request_impl = stop_during_read
        with self.assertRaises(RequestStoppedError):
            client.request(client.spot, "GET", "/api/v3/account")

    def test_dual_order_transient_sell_failure_recovers_only_sell_leg(self):
        client = _Client()
        strategy = _strategy(client)
        strategy.TRADE_OPERATION_RETRY_POLICY = RetryPolicy(2, 0)
        strategy._limit_order_plan = lambda _book: dict(quote_asset="USDT", base_asset="BTC", quote_balance=Decimal(10), base_balance=Decimal(1), base_notional=Decimal(10), bid_price=Decimal(10), ask_price=Decimal(11))
        client.spot_limit_buy_quote_amount = lambda symbol, _price, _amount: client.request(client.spot, "POST", "/api/v3/order", {"symbol": symbol, "type": "LIMIT", "side": "BUY"})
        failures = 0
        def sell(symbol, _price, _amount):
            nonlocal failures
            failures += 1
            if failures <= 15:
                raise requests.ConnectTimeout("not sent")
            return client.request(client.spot, "POST", "/api/v3/order", {"symbol": symbol, "type": "LIMIT", "side": "SELL"})
        client.spot_limit_sell_quantity = sell
        buy, sell_order = strategy._place_limit_orders("挂单", {})
        self.assertEqual((buy["side"], sell_order["side"]), ("BUY", "SELL"))
        self.assertEqual(failures, 16)
        self.assertEqual([params["side"] for method, _, params in client.requests if method == "POST"], ["BUY", "SELL"])

    def test_spot_and_futures_lost_reply_query_original_without_another_post(self):
        for market, path in (("spot", "/api/v3/order"), ("um_futures", "/fapi/v1/order")):
            with self.subTest(market=market):
                client = _Client(lose_reply=True, market=market)
                base = client.spot if market == "spot" else client.um_futures
                result = client.request(base, "POST", path, {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
                self.assertEqual(result["status"], "FILLED")
                self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
                self.assertEqual(client.journal.entries["1"]["state"], "completed")
                self.assertEqual(client.pending_order_requests(), [])

    def test_stop_during_confirmation_keeps_intent_and_blocks_another_post(self):
        client = _Client(lose_reply=True)
        client.lookup_missing = True
        original = client.get_order_by_client_id
        def query(*args, **kwargs):
            client.stop.set()
            return original(*args, **kwargs)
        client.get_order_by_client_id = query
        with self.assertRaises(RequestStoppedError):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        client.stop.clear()
        with self.assertRaises(UnconfirmedOrderError):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET"})
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
        self.assertEqual(len(client.journal.active_entries()), 1)

    def test_rejected_order_does_not_leave_recovery_blocker(self):
        client = _Client()
        client._request_impl = Mock(side_effect=BinanceAPIError(-2010, "rejected"))
        with self.assertRaises(BinanceAPIError):
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT"})
        self.assertEqual(client.journal.active_entries(), [])
        self.assertEqual(client.pending_order_requests(), [])

    def test_withdraw_and_transfer_uncertainty_never_replay(self):
        for path in ("/sapi/v1/capital/withdraw/apply", "/sapi/v1/asset/transfer", "/sapi/v1/simple-earn/flexible/subscribe", "/sapi/v1/simple-earn/flexible/redeem"):
            with self.subTest(path=path):
                client = _Client()
                with self.assertRaises(UnconfirmedOperationError):
                    client.request(client.spot, "POST", path, {"amount": "1"})
                with self.assertRaises(UnconfirmedOperationError):
                    client.request(client.spot, "POST", path, {"amount": "1"})
                self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)

    def test_read_only_post_keeps_transport_retry(self):
        client = _Client()
        client._request_impl = Mock(side_effect=[requests.Timeout("temporary"), {"ok": True}])
        self.assertEqual(client.request(client.spot, "POST", "/sapi/v3/asset/getUserAsset", {}), {"ok": True})
        self.assertEqual(client._request_impl.call_count, 2)

    def test_transfer_outer_retry_does_not_replay_uncertain_write(self):
        client = _Client()
        with self.assertRaises(UnconfirmedOperationError):
            client._universal_transfer_with_retry("MAIN_UMFUTURE", "USDT", 1, context="test", retry_delay=0)
        self.assertEqual(len(client.requests), 1)

    def test_transfer_preserves_retry_for_definite_temporary_rejection(self):
        client = _Client()
        client.universal_transfer = Mock(side_effect=[BinanceAPIError(32020, "temporarily suspended")] * 3 + [True])
        self.assertTrue(client._universal_transfer_with_retry("MAIN_UMFUTURE", "USDT", 1, context="test", retry_delay=0))
        self.assertEqual(client.universal_transfer.call_count, 4)

    def test_convert_accept_lost_reply_resolves_original_quote(self):
        client = _Client()
        client._request_impl = Mock(side_effect=[requests.Timeout("accepted"), {"orderStatus": "SUCCESS", "quoteId": "original", "orderId": 123}])
        result = client.accept_convert_quote("original")
        self.assertEqual(result["orderStatus"], "SUCCESS")
        self.assertEqual(client._request_impl.call_count, 2)
        calls = client._request_impl.call_args_list
        self.assertEqual([call.args[1] for call in calls], ["POST", "GET"])
        self.assertEqual(calls[1].args[3]["quoteId"], "original")
        self.assertIsNone(client._uncertain_write_error)
        self.assertEqual(client.journal.active_entries(kind="exchange-funds"), [])

    def test_convert_accepted_then_query_failed_does_not_start_another_conversion(self):
        client = _Client()
        strategy = _strategy(client)
        client._validate_convert_from_amount = Mock(return_value=(Decimal(1), {}))
        client.get_convert_quote = Mock(return_value={"quoteId": "original"})
        client.accept_convert_quote = Mock(return_value={"orderStatus": "PROCESS"})
        client.wait_convert_order_success = Mock(side_effect=requests.Timeout("query failed"))
        with self.assertRaises(UnconfirmedOperationError):
            strategy._run_trade_action_with_retry(client.stop, "convert", lambda: client.convert_with_from_amount("BTC", "USDT", 1))
        client.accept_convert_quote.assert_called_once_with("original")
        client.get_convert_quote.assert_called_once()

    def test_market_acknowledged_then_query_failed_does_not_post_again(self):
        client = _Client()
        strategy = _strategy(client)
        client.wait_order_filled = Mock(side_effect=requests.Timeout("query failed"))
        def market_action():
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            return client._confirm_spot_market_order_filled("BTCUSDT", order)
        with self.assertRaises(UnconfirmedOrderError):
            strategy._run_trade_action_with_retry(client.stop, "market", market_action)
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
        self.assertEqual(len(client.journal.active_entries()), 1)

    def test_clearance_never_treats_missing_price_as_dust(self):
        client = _Client()
        client.spot_market_sellable_base_quantity = BinanceClient.spot_market_sellable_base_quantity.__get__(client)
        client.spot_balance_snapshot = Mock(return_value={"BTC": {"total": Decimal(1)}})
        client.get_symbol_trade_rules = Mock(return_value={"status": "TRADING", "stepSize": Decimal("0.01"), "minQty": Decimal("0.01"), "maxQty": Decimal(100), "minNotional": Decimal(10)})
        client.get_symbol_price = Mock(return_value=None)
        with self.assertRaisesRegex(RuntimeError, "行情不可用"):
            client.spot_market_sellable_base_quantity("BTCUSDT")

    def test_strategy_does_not_retry_uncertain_or_permission_error(self):
        strategy = _strategy(_Client())
        for error in (UnconfirmedOperationError("unknown"), BinanceAPIError(-2015, "invalid permission")):
            action = Mock(side_effect=error)
            with self.assertRaises(type(error)):
                strategy._run_trade_action_with_retry(threading.Event(), "trade", action)
            self.assertEqual(action.call_count, 1)

    def test_composite_action_does_not_replay_after_acknowledged_order(self):
        client = _Client()
        strategy = _strategy(client)
        def submit_then_query_failed():
            client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            raise requests.Timeout("follow-up query failed")
        with self.assertRaises(UnconfirmedOperationError):
            strategy._run_trade_action_with_retry(client.stop, "premium", submit_then_query_failed)
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)

    def test_limit_round_recovers_monitoring_beyond_retry_batch_without_reposting(self):
        client = _Client()
        strategy = _strategy(client)
        client.get_book_ticker = Mock(return_value={"bidPrice": Decimal(10), "askPrice": Decimal(11)})
        strategy._place_limit_orders = Mock(side_effect=lambda *_args, **_kwargs: (client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"}), None))
        strategy.TRADE_OPERATION_RETRY_POLICY = RetryPolicy(2, 0)
        strategy._should_stop_for_bnb_fee = Mock(side_effect=[False, True])
        strategy._monitor_limit_orders = Mock(side_effect=[requests.Timeout("query failed")] * 12 + [("filled", {"filled_orders": []})])
        with patch.object(strategy, "_pause_with_stop", return_value=False):
            strategy._run_limit_mode(client.stop)
        strategy._place_limit_orders.assert_called_once()
        self.assertEqual(strategy._monitor_limit_orders.call_count, 13)

    def test_stop_liquidates_all_supported_spot_pair_bases_after_gate_stop(self):
        for symbol in ("BTCUSDT", "ALLOUSDC", "BTCETH"):
            with self.subTest(symbol=symbol):
                client = _Client()
                strategy = _strategy(client, spot_symbol=symbol)
                strategy._run_steps = lambda *_args, **_kwargs: strategy._strategy_result()
                client.stop.set()
                result = strategy.run(client.stop)
                self.assertTrue(result["cleanup_confirmed"])
                self.assertEqual(client.sale_calls, [symbol])
                self.assertEqual(result["cleanup_error"], "")

    def test_stop_confirms_cancel_before_liquidation_and_preserves_failed_cancel(self):
        client = _Client()
        strategy = _strategy(client)
        client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
        client.stop.set()
        client.cancel_succeeds = False
        strategy._run_steps = lambda *_args, **_kwargs: strategy._strategy_result()
        result = strategy.run(client.stop)
        self.assertFalse(result["cleanup_confirmed"])
        self.assertTrue(result["trade_error"])
        self.assertEqual(client.sale_calls, [])
        self.assertEqual(len(client.journal.active_entries()), 1)
        client.cancel_succeeds = True
        self.assertTrue(strategy.retry_stop_cleanup())
        self.assertEqual(client.sale_calls, ["BTCUSDT"])
        self.assertEqual(client.journal.active_entries(), [])

    def test_legacy_ladder_does_not_sell_until_cancellation_is_confirmed(self):
        client = _Client()
        strategy = _strategy(client)
        client.cancel_succeeds = False
        order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
        client.stop.set()
        strategy._ladder_cleanup_after_manual_stop({"1": {"order_id": order["orderId"], "side": "BUY"}}, "BTC")
        self.assertFalse(strategy.ladder_cleanup_confirmed)
        self.assertEqual(client.sale_calls, [])

    def test_premium_stop_keeps_order_recoverable_when_cancel_is_unconfirmed(self):
        client = _Client()
        strategy = _strategy(client)
        client.cancel_succeeds = False
        order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
        client.stop.set()
        with self.assertRaisesRegex(RuntimeError, "停止信号"):
            strategy._wait_order_filled_or_reprice(order["orderId"], "BUY", Decimal(10), client.stop, "溢价")
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertEqual(strategy._active_spot_orders, {"1": "BUY"})
        self.assertTrue(strategy.ladder_cleanup_error)

    def test_zero_round_stop_keeps_inverse_input_direction_and_skips_dust(self):
        for balance in (Decimal(1), Decimal(0)):
            with self.subTest(balance=balance):
                client = _Client()
                client.spot_market_conversion_sellable_source_amount = Mock(side_effect=[balance, Decimal(0)])
                client.spot_market_convert_base_to_quote_all = Mock(return_value=True)
                strategy = _strategy(client, spot_symbol="USDTUSDC")
                strategy.spot_rounds = 0
                strategy._run_steps = lambda *_args, **_kwargs: strategy._strategy_result()
                client.stop.set()
                self.assertTrue(strategy.run(client.stop)["cleanup_confirmed"])
                if balance:
                    client.spot_market_convert_base_to_quote_all.assert_called_once_with("USDTUSDC")
                else:
                    client.spot_market_convert_base_to_quote_all.assert_not_called()

    def test_limit_mode_stops_on_permanent_permission_rejection(self):
        client = _Client()
        client.get_book_ticker = Mock(side_effect=BinanceAPIError(-2015, "permission"))
        strategy = _strategy(client)
        strategy._run_limit_mode(client.stop)
        client.get_book_ticker.assert_called_once()
        self.assertIn("permission", strategy.trade_error)

    def test_stop_futures_cleanup_passes_gate_and_confirms_zero_position(self):
        client = _Client()
        def close_positions(_symbol):
            client._wait_for_request_gate()
            return []
        client.close_all_um_futures_positions_market = Mock(side_effect=close_positions)
        client.get_um_futures_positions = Mock(return_value=[])
        strategy = _strategy(client, trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES)
        strategy._run_steps = lambda *_args, **_kwargs: strategy._strategy_result()
        client.stop.set()
        result = strategy.run(client.stop)
        self.assertTrue(result["cleanup_confirmed"])
        client.close_all_um_futures_positions_market.assert_called_once()

    def test_stop_during_futures_settings_is_not_reported_as_trade_failure(self):
        client = _Client()
        client.close_all_um_futures_positions_market = Mock(return_value=[])
        client.get_um_futures_positions = Mock(return_value=[])
        strategy = _strategy(client, trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES)
        strategy._run_steps = Mock(side_effect=RequestStoppedError("stopped before setting request"))
        client.stop.set()
        result = strategy.run(client.stop)
        self.assertTrue(result["cleanup_confirmed"])
        self.assertFalse(result["trade_failed"])
        self.assertEqual(ExchangeAppBatchMixin._strategy_failure_text(result, stopped=True), "")

    def test_dual_order_failure_keeps_first_order_and_does_not_repeat_pair(self):
        client = _Client()
        strategy = _strategy(client)
        strategy._limit_order_plan = lambda _book: dict(quote_asset="USDT", base_asset="BTC", quote_balance=Decimal(10), base_balance=Decimal(1), base_notional=Decimal(10), bid_price=Decimal(10), ask_price=Decimal(11))
        client.spot_limit_buy_quote_amount = lambda symbol, _price, _amount: client.request(client.spot, "POST", "/api/v3/order", {"symbol": symbol, "type": "LIMIT", "side": "BUY"})
        client.spot_limit_sell_quantity = Mock(side_effect=BinanceAPIError(-2010, "sell rejected"))
        with self.assertRaises(UnconfirmedOperationError):
            strategy._run_trade_action_with_retry(client.stop, "dual", lambda: strategy._place_limit_orders("挂单", {}))
        self.assertEqual(sum(method == "POST" for method, _, _ in client.requests), 1)
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertEqual(strategy._active_spot_orders, {"1": "BUY"})

    def test_observed_terminals_bound_journal_and_client_indexes(self):
        client = _Client()
        for _ in range(520):
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "LIMIT", "side": "BUY"})
            client.request(client.spot, "DELETE", "/api/v3/order", {"symbol": "BTCUSDT", "orderId": order["orderId"]})
        self.assertEqual(client.journal.active_entries(), [])
        self.assertEqual(client._order_journal_entries, {})
        self.assertEqual(client.pending_order_requests(), [])
        self.assertEqual(len(OperationJournal._compact_entries(list(client.journal.entries.values()))), 500)

    def test_restart_reconciles_futures_by_original_client_id(self):
        client = _Client(lose_reply=True)
        client.lookup_missing = True
        original = client.get_order_by_client_id
        def query(*args, **kwargs):
            client.stop.set()
            return original(*args, **kwargs)
        client.get_order_by_client_id = query
        with self.assertRaises(RequestStoppedError):
            client.request(client.um_futures, "POST", "/fapi/v1/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "BUY"})
        client.stop.clear()
        client.get_order_by_client_id = original
        client.lookup_missing = False
        reconcile_exchange_orders(client.journal, client, client.key)
        self.assertEqual(client.journal.active_entries(), [])

    def test_single_account_common_result_reports_futures_and_withdraw_errors(self):
        self.assertIn("开仓失败", ExchangeAppBatchMixin._strategy_failure_text({"trade_error": "开仓失败", "trade_error_stage": "合约"}))
        self.assertIn("提现失败", ExchangeAppBatchMixin._strategy_failure_text({"withdraw_error": "permission", "withdraw_amount": 0}))
        self.assertIn("停止清理失败", ExchangeAppBatchMixin._strategy_failure_text({"cleanup_error": "position remains"}, stopped=True))


class CloseCleanupTests(unittest.TestCase):
    def _probe(self, *, running=False, pending=False):
        return SimpleNamespace(_close_wait_after_token=None,
            _retry_pending_cleanups=Mock(), _pending_cleanup_snapshot=lambda: [SimpleNamespace(cleanup_error="cancel unconfirmed")] if pending else [],
            _background_shutdown_pending=lambda: running, status_var=Mock(), after=Mock(return_value="after-id"),
            _complete_close_when_idle=Mock(), _finalize_close=Mock())

    def test_close_waits_for_worker_even_after_old_deadline(self):
        owner = self._probe(running=True)
        ExchangeAppBase._complete_close_when_idle(owner)
        owner._finalize_close.assert_not_called()
        owner.after.assert_called_once()

    def test_close_waits_for_unconfirmed_cleanup_after_worker_finishes(self):
        owner = self._probe(pending=True)
        ExchangeAppBase._complete_close_when_idle(owner)
        owner._finalize_close.assert_not_called()
        owner.status_var.set.assert_called_once()

    def test_close_exits_only_after_worker_and_cleanup_finish(self):
        owner = self._probe()
        ExchangeAppBase._complete_close_when_idle(owner)
        owner._finalize_close.assert_called_once()

    def test_final_exit_itself_cannot_bypass_cleanup_gate(self):
        owner = self._probe(pending=True)
        owner._close_finalized = False
        ExchangeAppBase._finalize_close(owner)
        owner._complete_close_when_idle.assert_called_once()
        self.assertFalse(owner._close_finalized)

    def test_final_exit_waits_when_runtime_state_cannot_be_saved(self):
        owner = self._probe()
        owner._close_finalized = False
        owner._flush_runtime_sensitive_stores = Mock(side_effect=OSError("disk full"))
        ExchangeAppBase._finalize_close(owner)
        self.assertFalse(owner._close_finalized)
        owner.after.assert_called_once_with(1000, owner._complete_close_when_idle)

    def test_shutdown_waits_for_rpc_worker_after_controller_has_finished(self):
        release = threading.Event()
        owner = SimpleNamespace(is_running=False, _managed_threads_lock=threading.Lock(), _managed_threads=set(),
                                _confirmation_executor_lock=threading.Lock(), _confirmation_futures=set(), client=Mock())
        owner._start_managed_thread = lambda target, **kwargs: OnchainTransferPageBase._start_managed_thread(owner, target, **kwargs)
        owner._managed_threads_snapshot = lambda: OnchainTransferPageBase._managed_threads_snapshot(owner)
        worker = OnchainTransferPageBase._start_rpc_worker_thread(owner, lambda: release.wait(5))
        try:
            self.assertTrue(OnchainTransferPageBase.shutdown_pending(owner))
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertFalse(OnchainTransferPageBase.shutdown_pending(owner))
        owner.client.release_current_thread_rpc_session.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
