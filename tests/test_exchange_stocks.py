from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from exchange_binance_client import BinanceAPIError, BinanceClient, UnconfirmedOperationError
from exchange_order_recovery import reconcile_exchange_orders
from exchange_stocks import STOCK_FIELDS, StockSettings, StockStrategy, platform_fee, trading_session


RULES = {"symbol": "AAPL", "tradability": "BUY_SELL", "stepSize": "0.000000001",
         "minQty": "0.000000001", "maxQty": "100000", "minNotional": "5",
         "maxNotional": "1000000", "fractionable": True, "fractionableEh": True,
         "extendedSession": True, "overnightSupported": True}
NOW = datetime(2026, 10, 2, 15, tzinfo=timezone.utc)


class Journal:
    def __init__(self):
        self.entries = {}

    def begin(self, kind, scope, payload):
        entry_id = str(len(self.entries) + 1)
        self.entries[entry_id] = {"id": entry_id, "kind": kind, "scope": scope, "state": "prepared", "payload": deepcopy(payload)}
        return entry_id

    def update(self, entry_id, state, **values):
        self.entries[entry_id]["state"] = state
        self.entries[entry_id]["payload"].update(deepcopy(values))

    def active_entries(self, **filters):
        return [deepcopy(e) for e in self.entries.values() if e["state"] not in {"completed", "cancelled", "failed"} and all(e[key] == value for key, value in filters.items())]


class Stop:
    def __init__(self):
        self.time = 0
        self.stopped = False

    def wait(self, seconds):
        self.time += seconds
        return self.stopped

    def is_set(self):
        return self.stopped


class Client(BinanceClient):
    spot = "https://api.binance.com"
    key = "test-stock-key"
    _is_uncertain_write_error = staticmethod(BinanceClient._is_uncertain_write_error)
    is_temporary_request_error = staticmethod(BinanceClient.is_temporary_request_error)

    def __init__(self):
        self.operation_journal = Journal()
        self.orders, self.posts, self.calls = {}, [], []
        self.quote_values = [(Decimal("100"), Decimal("100.02"))]
        self.balances = {"MAIN": Decimal("1000"), "CARD": Decimal("1000")}
        self.holdings = Decimal("70")  # Existing holdings must never be liquidated.
        self.lost_reply = False
        self.partial_then_cancel = False
        self.pending_cancel = False
        self.quote_count = 0
        self.cleanup_depth = 0
        self.request_stop = None
        self.detail_failures = 0
        self.session = object()
        self.rules = dict(RULES)
        self.transfers, self.transfer_rows = [], []
        self.transfer_error = None
        self.lost_transfer_reply = False
        self.transfer_history_delay = 0
        self.transfer_credit_delay = False

    def current_server_time_ms(self, base=None):
        return int(NOW.timestamp() * 1000)

    def _reset_session_after_transport_error(self):
        self.session = object()

    def _public_get_impl(self, base, path, params=None, **kwargs):
        assert path.startswith("/sapi/v1/equity/market/")
        return self._request_impl(base, "GET", path, params or {}, **kwargs)

    @contextmanager
    def cleanup_requests_after_stop(self):
        self.cleanup_depth += 1
        try:
            yield
        finally:
            self.cleanup_depth -= 1

    def _run_request_with_retry(self, fn, **kwargs):
        if self.request_stop and self.request_stop.is_set() and not self.cleanup_depth:
            raise RuntimeError("requests stopped")
        if kwargs["path"].endswith(("place", "cancel")) or kwargs["path"] == "/sapi/v1/asset/transfer":
            assert kwargs["max_retries"] == 1
            if not kwargs["replay_safe"]:
                self.assert_no_write_replay = True
        return fn()

    def spot_asset_balance_decimal(self, asset):
        return self.balances["MAIN"]

    def funding_asset_balance(self, asset):
        return self.balances["CARD"]

    def _request_impl(self, base, method, path, params, **kwargs):
        self.calls.append((method, path, dict(params)))
        if path == "/sapi/v1/asset/transfer":
            if method == "GET":
                if self.transfer_history_delay:
                    self.transfer_history_delay -= 1
                    return {"total": 0, "rows": []}
                if self.transfer_credit_delay and self.transfer_rows:
                    self.balances["CARD"] += Decimal(self.transfer_rows[-1]["amount"])
                    self.transfer_credit_delay = False
                return {"total": len(self.transfer_rows), "rows": deepcopy(self.transfer_rows)}
            pending = self.operation_journal.active_entries(kind="stock-task")[0]["payload"]["funding_transfer"]
            assert pending["amount"] == params["amount"], "durable transfer intent must precede submission"
            self.transfers.append(dict(params))
            if self.transfer_error:
                raise self.transfer_error
            amount = Decimal(params["amount"])
            self.balances["MAIN"] -= amount
            if not self.transfer_credit_delay:
                self.balances["CARD"] += amount
            self.transfer_rows.append({**params, "tranId": str(len(self.transfers)), "status": "CONFIRMED"})
            if self.lost_transfer_reply:
                self.lost_transfer_reply = False
                raise UnconfirmedOperationError("transfer response lost")
            return {"tranId": len(self.transfers)}
        if path.endswith("exchangeInfo"):
            return {"symbols": [dict(self.rules)]}
        if path.endswith("quote"):
            bid, ask = self.quote_values[min(self.quote_count, len(self.quote_values) - 1)]
            self.quote_count += 1
            return {"symbol": "AAPL", "bidPrice": str(bid), "askPrice": str(ask)}
        if path.endswith("place"):
            active = self.operation_journal.active_entries(kind="stock-task")[0]["payload"]["active"]
            assert active["clientOrderId"] == params["clientOrderId"], "durable intent must precede submission"
            self.posts.append(dict(params))
            self.orders[params["clientOrderId"]] = {**params, "orderId": str(len(self.posts)), "status": "NEW", "filledQty": "0", "avgFilledPrice": None, "fee": "0"}
            if self.lost_reply:
                self.lost_reply = False
                raise UnconfirmedOperationError("response lost")
            return {"status": "S", "orderId": str(len(self.posts))}
        if path.endswith("cancel"):
            row = next(row for row in self.orders.values() if row["orderId"] == params["orderId"])
            if not self.pending_cancel:
                # A final fill races with cancellation: it must be counted before replacement.
                if self.partial_then_cancel and row["side"] == "BUY":
                    row.update(filledQty=row["quantity"], avgFilledPrice=row["price"])
                row["status"] = "CANCELED"
            return {"status": "S"}
        if path.endswith("detail"):
            if self.detail_failures:
                self.detail_failures -= 1
                raise ConnectionError("temporary query disconnect")
            row = self.orders[params["clientOrderId"]]
            if row["status"] == "NEW":
                if self.partial_then_cancel and row["side"] == "BUY":
                    row.update(status="PARTIALLY_FILLED", filledQty=str((Decimal(row["quantity"]) / 2).quantize(Decimal("0.000000001"))), avgFilledPrice=row["price"])
                else:
                    row.update(status="FILLED", filledQty=row["quantity"], avgFilledPrice=row["price"], fee="0.35")
            # Simulate the actual account ledger from cumulative fills, independently of strategy bookkeeping.
            qty, old = Decimal(row["filledQty"]), Decimal(row.get("appliedQty", "0"))
            fee, old_fee = Decimal(row["fee"]), Decimal(row.get("appliedFee", "0"))
            side = 1 if row["side"] == "BUY" else -1
            wallet = row["walletType"] if side == 1 else "CARD"
            self.holdings += side * (qty - old)
            self.balances[wallet] -= side * (qty - old) * Decimal(row["price"]) + fee - old_fee
            row.update(appliedQty=str(qty), appliedFee=str(fee))
            return deepcopy(row)
        raise AssertionError(path)


class StockTests(unittest.TestCase):
    def strategy(self, client=None, **changes):
        client = client or Client()
        settings = StockSettings.parse({**STOCK_FIELDS, **changes})
        stop = Stop()
        task = StockStrategy(client, settings, now=lambda: NOW, clock=lambda: stop.time)
        return client, task, stop

    def test_settings_reject_invalid_and_nonfinite_values(self):
        for changes in ({"symbol": "BTC/USDT"}, {"rounds": "0"}, {"poll": "NaN"}, {"amount": "Infinity"}, {"tick": "0.005"}, {"spread": "0"}):
            with self.subTest(changes=changes), self.assertRaises((ValueError, RuntimeError)):
                StockSettings.parse(changes)
        settings = StockSettings.parse(STOCK_FIELDS)
        self.assertEqual(StockSettings.parse(settings.payload()), settings)

    def test_rounds_use_crossing_prices_and_only_task_inventory(self):
        client, task, stop = self.strategy(rounds="3")
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(result["completed"], 3)
        self.assertEqual(len(client.posts), 6)
        self.assertEqual(client.holdings, Decimal("70"))
        self.assertEqual(Decimal(result["inventory"]), 0)
        self.assertEqual(Decimal(result["fees"]), Decimal("2.10"))
        self.assertEqual([p["price"] for p in client.posts], ["100.03", "99.99"] * 3)
        self.assertTrue(all(p["tokenize"] == "false" and p["quoteAsset"] == "USDC" for p in client.posts))
        self.assertEqual(client.operation_journal.active_entries(), [])

    def test_quote_trailing_zeros_do_not_exceed_limit_price_precision(self):
        client, task, stop = self.strategy()
        client.quote_values = [(Decimal("100.000000"), Decimal("100.020000"))]
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual([p["price"] for p in client.posts], ["100.03", "99.99"])

    def test_all_spot_usdc_is_collected_and_every_order_uses_funding(self):
        client, task, stop = self.strategy(rounds="2", amount="100")
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(client.transfers, [{"type": "MAIN_FUNDING", "asset": "USDC", "amount": "1000"}])
        self.assertEqual(client.balances["MAIN"], 0)
        self.assertEqual([p["walletType"] for p in client.posts], ["CARD"] * 4)
        self.assertEqual(client.holdings, Decimal("70"))

    def test_funding_only_balance_needs_no_transfer(self):
        client, task, stop = self.strategy()
        client.balances["MAIN"] = Decimal(0)
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(client.transfers, [])

    def test_buy_waits_for_transfer_history_and_funding_credit(self):
        client, task, stop = self.strategy()
        client.transfer_history_delay = 2
        client.transfer_credit_delay = True
        client.balances["CARD"] = Decimal(0)
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertGreaterEqual(stop.time, 2)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual([p["walletType"] for p in client.posts], ["CARD"] * 2)

    def test_lost_transfer_reply_never_trades_or_repeats_on_resume(self):
        client, task, stop = self.strategy()
        client.lost_transfer_reply = True
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(client.posts, [])
        client.balances["MAIN"] = Decimal(500)  # A later deposit must not trigger another transfer.
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertFalse(result["success"])
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.posts, [])
        self.assertTrue(client.operation_journal.active_entries()[0]["payload"]["funding_transfer"])

    def test_acknowledged_transfer_recovers_without_sweeping_later_deposits(self):
        client, task, stop = self.strategy(timeout="2", reprice="1")
        client.transfer_history_delay = 100
        client.transfer_credit_delay = True
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(client.posts, [])
        client.balances["MAIN"] = Decimal(500)
        client.balances["CARD"] += Decimal(300)  # Funding deposits cannot expand the saved budget either.
        client.transfer_history_delay = 0
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.balances["MAIN"], 500)
        self.assertLess(Decimal(client.posts[0]["quantity"]) * Decimal(client.posts[0]["price"]), 2000)

    def test_transfer_permission_rejection_uses_existing_funding_balance(self):
        for code in (-2015, -1002):
            for amount in ("", "100"):
                with self.subTest(code=code, amount=amount):
                    client, task, stop = self.strategy(rounds="2", amount=amount)
                    client.transfer_error = BinanceAPIError(code, "universal transfer permission disabled")
                    result = task.run(stop)
                    self.assertTrue(result["success"], result)
                    self.assertEqual(len(client.transfers), 1)
                    self.assertEqual(client.balances["MAIN"], 1000)
                    self.assertEqual([p["walletType"] for p in client.posts], ["CARD"] * 4)
                    self.assertTrue(task.state["funding_permission_skipped"])
                    self.assertLess(Decimal(client.posts[0]["quantity"]) * Decimal(client.posts[0]["price"]), 1000)
                    self.assertEqual(client.operation_journal.active_entries(), [])

    def test_transfer_permission_rejection_and_insufficient_funding_stops(self):
        for amount, balance in (("", "0"), ("", "5"), ("100", "99")):
            with self.subTest(amount=amount, balance=balance):
                client, task, stop = self.strategy(amount=amount)
                client.balances["CARD"] = Decimal(balance)
                client.transfer_error = BinanceAPIError(-2015, "permission disabled")
                result = task.run(stop)
                self.assertFalse(result["success"])
                self.assertIn("无划转权限且资金账户USDC不足", result["error"])
                self.assertEqual(client.posts, [])
                self.assertTrue(task.cleanup_confirmed)

    def test_permission_shortage_can_retry_after_manual_funding_deposit(self):
        client, task, stop = self.strategy(amount="100")
        client.balances["CARD"] = Decimal(0)
        client.transfer_error = BinanceAPIError(-2015, "permission disabled")
        self.assertFalse(task.run(stop)["success"])
        self.assertEqual(client.operation_journal.active_entries(), [])
        client.balances["CARD"] = Decimal(200)
        restarted = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = restarted.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(client.balances["MAIN"], 1000)
        self.assertTrue(all(p["walletType"] == "CARD" for p in client.posts))

    def test_shortage_after_confirmed_transfer_can_retry_without_repeating_it(self):
        client, task, stop = self.strategy(amount="3000")
        self.assertFalse(task.run(stop)["success"])
        self.assertEqual(client.operation_journal.active_entries(), [])
        client.balances["CARD"] += Decimal(2000)
        restarted = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = restarted.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(len(client.transfers), 1)

    def test_other_transfer_rejections_and_unknown_results_never_fall_back(self):
        for error, uncertain in ((BinanceAPIError(-2010, "rejected"), False),
                                 (BinanceAPIError(-5012, "transfer pending"), True),
                                 (BinanceAPIError(-1007, "unknown status"), True),
                                 (ConnectionError("connection lost"), True)):
            with self.subTest(error=error):
                client, task, stop = self.strategy()
                client.transfer_error = error
                result = task.run(stop)
                self.assertFalse(result["success"])
                self.assertEqual(client.posts, [])
                self.assertEqual(task.cleanup_confirmed, not uncertain)

    def test_stop_before_funding_preparation_makes_no_transfer_or_order(self):
        client, task, stop = self.strategy()
        stop.stopped = True
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertEqual(client.transfers, [])
        self.assertEqual(client.posts, [])

    def test_pending_transfer_blocks_other_market_tasks_and_changed_stock(self):
        client, task, stop = self.strategy()
        client.lost_transfer_reply = True
        task.run(stop)
        with self.assertRaisesRegex(RuntimeError, "股票划转"):
            reconcile_exchange_orders(client.operation_journal, client, client.key)
        changed = StockStrategy(client, StockSettings.parse({**STOCK_FIELDS, "symbol": "TSLA"}), now=lambda: NOW)
        self.assertFalse(changed.run(stop)["success"])
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.posts, [])

    def test_legacy_wallet_setting_is_ignored_and_flat_spot_task_is_migrated(self):
        client, task, stop = self.strategy(wallet="MAIN")
        self.assertNotIn("wallet", task.settings.payload())
        task.load()
        task.state.update(wallet="MAIN", cash="1000", phase="READY", settings={**task.settings.payload(), "wallet": "MAIN"})
        task.save()
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(len(client.transfers), 1)
        self.assertTrue(all(p["walletType"] == "CARD" for p in client.posts))

    def test_legacy_spot_order_is_cleaned_before_funding_only_trading(self):
        client, task, stop = self.strategy()
        task.load()
        task.rules = dict(RULES)
        task.state.update(wallet="MAIN", cash="1000", phase="BUY", target="2", settings={**task.settings.payload(), "wallet": "MAIN"})
        task.place("BUY", Decimal(2), Decimal("100.03"), "RTH")
        client.partial_then_cancel = True
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            data = original(base, method, path, params, **kwargs)
            if path.endswith("cancel"):
                client.partial_then_cancel = False
            return data
        client._request_impl = request
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(client.posts[1]["side"], "SELL")
        self.assertTrue(all(p["walletType"] == "CARD" for p in client.posts[1:]))
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.balances["MAIN"], 0)
        self.assertEqual(client.holdings, 70)

    def test_wrong_transfer_history_cannot_unlock_buying(self):
        client, task, stop = self.strategy()
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            data = original(base, method, path, params, **kwargs)
            if path == "/sapi/v1/asset/transfer" and method == "GET":
                data["rows"][0]["asset"] = "USDT"
            return data
        client._request_impl = request
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.posts, [])

    def test_invalid_transfer_acknowledgement_cannot_repeat_or_buy(self):
        client, task, stop = self.strategy()
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            data = original(base, method, path, params, **kwargs)
            return {} if path == "/sapi/v1/asset/transfer" and method == "POST" else data
        client._request_impl = request
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.posts, [])

    def test_stop_during_transfer_confirmation_only_reconciles_the_transfer(self):
        client, task, stop = self.strategy()
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            data = original(base, method, path, params, **kwargs)
            if path == "/sapi/v1/asset/transfer" and method == "POST":
                stop.stopped = True
            return data
        client._request_impl = request
        client.request_stop = stop
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertTrue(task.cleanup_confirmed, result)
        self.assertEqual(len(client.transfers), 1)
        self.assertEqual(client.posts, [])
        self.assertEqual(client.operation_journal.active_entries(), [])

    def test_lost_submission_reply_queries_original_without_duplicate(self):
        client, task, stop = self.strategy()
        client.lost_reply = True
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(len(client.posts), 2)

    def test_transient_order_query_does_not_repeat_submission(self):
        client, task, stop = self.strategy()
        client.detail_failures = 2
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(len(client.posts), 2)
        self.assertEqual(client.holdings, Decimal("70"))

    def test_stop_after_partial_fill_sells_only_task_inventory(self):
        client, task, stop = self.strategy()
        client.partial_then_cancel = True
        def progress(*_):
            stop.stopped = len(client.posts) > 0
        result = task.run(stop, progress)
        self.assertFalse(result["success"])
        self.assertTrue(task.cleanup_confirmed, result)
        self.assertEqual(result["completed"], 0)
        self.assertEqual([p["side"] for p in client.posts], ["BUY", "SELL"])
        self.assertEqual(client.holdings, Decimal("70"))

    def test_reprice_accounts_for_fill_racing_with_cancel(self):
        client, task, stop = self.strategy(reprice="1")
        client.partial_then_cancel = True
        client.quote_values = [(Decimal("100"), Decimal("100.02"))] * 2 + [(Decimal("100.01"), Decimal("100.03"))]
        result = task.run(stop)
        self.assertTrue(result["success"], result)
        buys = [p for p in client.posts if p["side"] == "BUY"]
        self.assertEqual(len(buys), 1)
        self.assertEqual(client.holdings, Decimal("70"))
        self.assertTrue(any(path.endswith("cancel") for _, path, _ in client.calls))

    def test_cancel_acknowledgement_does_not_allow_replacement(self):
        client, task, stop = self.strategy(reprice="1", timeout="4")
        client.partial_then_cancel = client.pending_cancel = True
        client.quote_values = [(Decimal("100"), Decimal("100.02"))] * 2 + [(Decimal("100.01"), Decimal("100.03"))]
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(len(client.posts), 1)
        self.assertEqual(result["completed"], 0)
        self.assertTrue(client.operation_journal.active_entries())

    def test_resume_cleanup_does_not_buy_before_old_order_is_terminal(self):
        client, task, stop = self.strategy(reprice="1", timeout="4")
        client.partial_then_cancel = client.pending_cancel = True
        client.quote_values = [(Decimal("100"), Decimal("100.02"))] * 2 + [(Decimal("100.01"), Decimal("100.03"))]
        task.run(stop)
        client.pending_cancel = client.partial_then_cancel = False
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(client.posts[1]["side"], "SELL")
        self.assertEqual(client.holdings, Decimal("70"))

    def test_stop_cleanup_rule_queries_bypass_stopped_request_gate(self):
        client, task, stop = self.strategy(reprice="1", timeout="4")
        client.partial_then_cancel = client.pending_cancel = True
        client.quote_values = [(Decimal("100"), Decimal("100.02"))] * 2 + [(Decimal("100.01"), Decimal("100.03"))]
        task.run(stop)
        stop.stopped = task.stop_cleanup_requested = True
        client.request_stop = stop
        client.pending_cancel = client.partial_then_cancel = False
        client.session = None  # Account workers close clients before deferred cleanup.
        task.retry_stop_cleanup()
        self.assertTrue(task.cleanup_confirmed)
        self.assertIsNotNone(client.session)
        self.assertEqual(client.holdings, Decimal("70"))
        self.assertEqual(client.operation_journal.active_entries(), [])

    def test_halted_stock_still_cancels_owned_order_before_waiting_to_sell(self):
        client, task, stop = self.strategy(reprice="1", timeout="4")
        client.partial_then_cancel = client.pending_cancel = True
        client.quote_values = [(Decimal("100"), Decimal("100.02"))] * 2 + [(Decimal("100.01"), Decimal("100.03"))]
        task.run(stop)
        client.pending_cancel = client.partial_then_cancel = False
        client.rules["tradability"] = "NONE"
        with self.assertRaisesRegex(RuntimeError, "不可卖出"):
            task.retry_stop_cleanup()
        self.assertIsNone(task.state["active"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertGreater(Decimal(task.state["inventory"]), 0)
        self.assertEqual(len(client.posts), 1)
        client.rules["tradability"] = "BUY_SELL"
        task.retry_stop_cleanup()
        self.assertTrue(task.cleanup_confirmed)
        self.assertEqual(client.holdings, Decimal("70"))

    def test_resume_counts_saved_completed_rounds(self):
        client, task, stop = self.strategy(rounds="3")
        task.load()
        task.state.update(completed=1, phase="READY", cash="990")
        task.save()
        resumed = StockStrategy(client, task.settings, now=lambda: NOW, clock=lambda: stop.time)
        result = resumed.run(stop)
        self.assertTrue(result["success"], result)
        self.assertEqual(result["completed"], 3)
        self.assertEqual(len(client.posts), 4)

    def test_changed_symbol_cannot_cleanup_original_inventory(self):
        client, task, stop = self.strategy()
        task.load()
        task.state.update(phase="SELL", inventory="2", cash="0")
        task.save()
        changed = StockStrategy(client, StockSettings.parse({**task.settings.payload(), "symbol": "TSLA"}), now=lambda: NOW)
        result = changed.run(stop)
        self.assertFalse(result["success"])
        self.assertEqual(client.calls, [])
        self.assertEqual(client.posts, [])

    def test_other_market_recovery_blocks_pending_equity_orders(self):
        client, task, stop = self.strategy()
        task.load()
        task.state.update(inventory="1")
        task.save()
        with self.assertRaisesRegex(RuntimeError, "股票"):
            reconcile_exchange_orders(client.operation_journal, client, client.key)

    def test_invalid_quote_and_wide_spread_do_not_submit(self):
        for quote in ((Decimal("100"), Decimal("102")), (Decimal("0"), Decimal("1")), (Decimal("100.005"), Decimal("100.025"))):
            client, task, stop = self.strategy()
            client.quote_values = [quote]
            result = task.run(stop)
            self.assertFalse(result["success"])
            self.assertEqual(client.posts, [])

    def test_closed_market_and_holidays_do_not_submit(self):
        for now in (datetime(2026, 10, 3, 15, tzinfo=timezone.utc), datetime(2026, 12, 25, 15, tzinfo=timezone.utc), datetime(2026, 4, 3, 15, tzinfo=timezone.utc)):
            client, task, stop = self.strategy()
            task.now = lambda: now
            result = task.run(stop)
            self.assertFalse(result["success"])
            self.assertEqual(client.posts, [])

    def test_sessions_dst_and_overnight_whole_shares(self):
        self.assertEqual(trading_session(NOW, RULES), ("RTH", True))
        self.assertEqual(trading_session(datetime(2026, 10, 2, 12, tzinfo=timezone.utc), RULES), ("EXTENDED", True))
        self.assertEqual(trading_session(datetime(2026, 10, 5, 1, tzinfo=timezone.utc), RULES), ("24H", False))
        self.assertEqual(trading_session(datetime(2026, 12, 1, 14, 45, tzinfo=timezone.utc), RULES), ("RTH", True))
        with self.assertRaises(RuntimeError):
            trading_session(datetime(2026, 10, 2, 12, tzinfo=timezone.utc), {**RULES, "extendedSession": False})

    def test_usdt_is_rejected_without_automatic_conversion(self):
        with self.assertRaisesRegex(ValueError, "不自动兑换USDT"):
            StockSettings.parse({**STOCK_FIELDS, "asset": "USDT"})

    def test_usdc_shortage_never_converts_other_assets(self):
        client, task, stop = self.strategy()
        client.balances["CARD"] = Decimal(0)
        client.balances["MAIN"] = Decimal(0)
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertIn("USDC", result["error"])
        self.assertEqual(client.posts, [])
        self.assertTrue(all("convert" not in path for _, path, _ in client.calls))

    def test_legacy_uncertain_conversion_is_preserved_without_any_request(self):
        client, task, stop = self.strategy()
        task.load()
        task.state["conversion"] = {"quoteId": "old-conversion"}
        task.save()
        resumed = StockStrategy(client, task.settings, now=lambda: NOW)
        result = resumed.run(stop)
        self.assertFalse(result["success"])
        self.assertEqual(client.calls, [])
        self.assertEqual(client.operation_journal.active_entries()[0]["payload"]["conversion"]["quoteId"], "old-conversion")

    def test_inconsistent_terminal_fill_does_not_submit_replacement(self):
        client, task, stop = self.strategy()
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            row = original(base, method, path, params, **kwargs)
            if path.endswith("detail"):
                row["filledQty"] = "0"
            return row
        client._request_impl = request
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(len(client.posts), 1)
        self.assertTrue(client.operation_journal.active_entries())

    def test_unknown_order_state_never_allows_another_order(self):
        client, task, stop = self.strategy()
        original = client._request_impl
        def request(base, method, path, params, **kwargs):
            row = original(base, method, path, params, **kwargs)
            if path.endswith("detail"):
                row["status"] = "UNRECOGNIZED"
            return row
        client._request_impl = request
        result = task.run(stop)
        self.assertFalse(result["success"])
        self.assertFalse(task.cleanup_confirmed)
        self.assertEqual(len(client.posts), 1)

    def test_fee_threshold_rounding(self):
        self.assertEqual(platform_fee(Decimal(350)), Decimal("0.35"))
        self.assertEqual(platform_fee(Decimal("350.01")), Decimal("0.36"))


class StockRoutingTests(unittest.TestCase):
    def test_old_offline_gui_stubs_can_collect_default_stock_settings(self):
        from exchange_app import App
        from exchange_app_stocks import stock_payload
        self.assertEqual(stock_payload(App.__new__(App)), STOCK_FIELDS)

    def owner(self):
        return SimpleNamespace(worker_thread=None,
            trade_account_type_var=SimpleNamespace(get=lambda: "股票"),
            withdraw_buffer_var=SimpleNamespace(get=lambda: "-1"),
            start_stock_tasks=Mock())

    def test_single_start_routes_to_stock_executor(self):
        from exchange_app_batch import ExchangeAppBatchMixin
        owner = self.owner()
        ExchangeAppBatchMixin.start_bot(owner)
        owner.start_stock_tasks.assert_called_once_with()

    def test_batch_retry_preserves_stock_settings(self):
        from exchange_app_batch import ExchangeAppBatchMixin
        owner, accounts = self.owner(), [{"api_key": "key", "api_secret": "secret"}]
        raw = {**STOCK_FIELDS, "symbol": "MSFT", "rounds": "3"}
        ExchangeAppBatchMixin.run_selected_accounts(owner, accounts,
            trade_settings_override={"trade_account_type": "股票", "stock_settings": raw}, require_confirm=False)
        owner.start_stock_tasks.assert_called_once_with(accounts, raw=raw, require_confirm=False)

    def test_existing_batch_actions_do_not_route_to_stocks(self):
        from exchange_app_batch import ExchangeAppBatchMixin
        for options in ({"batch_total_asset_only": True}, {"batch_collect_bnb_mode": True},
                        {"trade_settings_override": {"trade_account_type": "理财"}}):
            owner = self.owner()
            with self.subTest(options=options), patch("exchange_app_batch.messagebox.showerror"):
                ExchangeAppBatchMixin.run_selected_accounts(owner, [{"api_key": "key"}], **options)
                owner.start_stock_tasks.assert_not_called()


if __name__ == "__main__":
    unittest.main()
