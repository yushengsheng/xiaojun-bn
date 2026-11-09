"""Run futures completion and withdrawals through signed requests and encrypted history."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import Mock, patch

import requests

from exchange_binance_client import BinanceAPIError, BinanceClient
from exchange_constants import TRADE_ACCOUNT_TYPE_FUTURES
from exchange_strategy import Strategy
from operation_journal import OperationJournal
from secret_box import SecretBox
from test_exchange_cleanup_retry import signed_transport


class _Exchange:
    def __init__(self):
        self.balances = {
            "MAIN": {"USDT": Decimal("20"), "USDC": Decimal("20"), "BNB": Decimal("2")},
            "UMFUTURE": {"USDT": Decimal("12"), "USDC": Decimal("0"), "BNB": Decimal("0")},
            "CMFUTURE": {"USDT": Decimal("5"), "USDC": Decimal("5"), "BNB": Decimal("0.5")},
            "FUNDING": {"USDT": Decimal("4"), "USDC": Decimal("4"), "BNB": Decimal("0.4")},
        }
        self.positions = {}
        self.orders = []
        self.events = []
        self.fail_preclear = False
        self.fail_open = False
        self.lose_close_reply = False
        self.fail_transfer_type = ""

    def request(self, method, path, params):
        if path == "/fapi/v1/exchangeInfo":
            return {"symbols": [{"symbol": "BTC" + coin, "status": "TRADING", "baseAsset": "BTC",
                                  "quoteAsset": coin, "marginAsset": coin, "quantityPrecision": 3,
                                  "filters": [{"filterType": "LOT_SIZE", "stepSize": "0.001",
                                               "minQty": "0.001", "maxQty": "1000"}]}
                                 for coin in ("USDT", "USDC")]}
        if path == "/fapi/v1/ticker/bookTicker":
            return {"bidPrice": "100", "askPrice": "100"}
        if path == "/fapi/v1/ticker/price":
            return {"price": "100"}
        if path == "/fapi/v1/positionSide/dual":
            return {"dualSidePosition": False}
        if path == "/fapi/v1/symbolConfig":
            return [{"symbol": params["symbol"], "marginType": "CROSSED", "leverage": 10}]
        if path == "/fapi/v3/account":
            return {"assets": [{"asset": coin, **{field: str(value) for field in (
                "walletBalance", "availableBalance", "marginBalance", "crossWalletBalance", "maxWithdrawAmount")}}
                               for coin, value in self.balances["UMFUTURE"].items()]}
        if path == "/fapi/v3/positionRisk":
            return [{"symbol": params["symbol"], "positionAmt": str(self.positions.get(params["symbol"], 0)),
                     "positionSide": "BOTH"}]
        if path == "/fapi/v1/order":
            if method == "GET":
                return next(order for order in self.orders if order["clientOrderId"] == params["origClientOrderId"])
            symbol = params["symbol"]
            if self.fail_open and params.get("reduceOnly") != "true":
                raise BinanceAPIError(-2015, "test trading permission rejected")
            quantity = Decimal(params["quantity"])
            self.positions[symbol] = self.positions.get(symbol, 0) + (quantity if params["side"] == "BUY" else -quantity)
            order = {**params, "orderId": len(self.orders) + 1, "clientOrderId": params["newClientOrderId"],
                     "status": "FILLED", "executedQty": str(quantity), "avgPrice": "100"}
            self.orders.append(order)
            self.events.append(("order", dict(params)))
            if params.get("reduceOnly") == "true" and self.lose_close_reply:
                self.lose_close_reply = False
                raise requests.ReadTimeout("test close response lost")
            return order
        if path == "/api/v3/account":
            return {"balances": [{"asset": coin, "free": str(amount), "locked": "0"}
                                 for coin, amount in self.balances["MAIN"].items()]}
        if path in {"/fapi/v2/balance", "/dapi/v1/balance", "/sapi/v1/asset/get-funding-asset"}:
            wallet, field = {"/fapi/v2/balance": ("UMFUTURE", "maxWithdrawAmount"),
                             "/dapi/v1/balance": ("CMFUTURE", "withdrawAvailable"),
                             "/sapi/v1/asset/get-funding-asset": ("FUNDING", "free")}[path]
            return [{"asset": coin, field: str(amount)} for coin, amount in self.balances[wallet].items()]
        if path == "/sapi/v1/asset/transfer":
            transfer_type = params["type"]
            if self.fail_preclear and transfer_type == "UMFUTURE_MAIN":
                raise BinanceAPIError(-2015, "test transfer permission rejected")
            if transfer_type == self.fail_transfer_type:
                raise BinanceAPIError(-2015, "test transfer permission rejected")
            source, target = transfer_type.split("_")
            coin, amount = params["asset"], Decimal(params["amount"])
            assert self.balances[source][coin] >= amount
            self.balances[source][coin] -= amount
            self.balances[target][coin] += amount
            self.events.append(("transfer", dict(params)))
            return {"tranId": len(self.events)}
        if path == "/sapi/v1/capital/config/getall":
            return [{"coin": coin, "networkList": [{"network": "BSC", "withdrawEnable": True,
                                                      "withdrawMin": "0.0001", "withdrawFee": "0"}]}
                    for coin in ("USDT", "USDC", "BNB")]
        if path == "/sapi/v1/capital/withdraw/apply":
            self.balances["MAIN"][params["coin"]] -= Decimal(params["amount"])
            self.events.append(("withdraw", dict(params)))
            return {"id": "test-withdrawal-" + str(len(self.events))}
        raise AssertionError((method, path))


class FuturesCompletionTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.box = SecretBox(self.root / ".test-key")
        self.journal = OperationJournal(self.root / "operations.json", secret_box=self.box, fail_unconfirmed=True)
        self.sleep = patch("exchange_binance_client.time.sleep", return_value=None)
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def client(self, exchange, key="test-account"):
        with patch.object(BinanceClient, "_build_session", return_value=Mock()):
            client = BinanceClient(key, "test-secret", operation_journal=self.journal)
        signed_transport(client, exchange.request)
        self.addCleanup(client.close)
        return client

    def strategy(self, client, **options):
        settings = dict(spot_rounds=0, withdraw_coin="USDT", withdraw_address="test-target",
                        withdraw_network="BSC", withdraw_fee_buffer=0, enable_withdraw=False, spot_symbol="BTCUSDT",
                        trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES, futures_symbol="BTCUSDT",
                        futures_rounds=1, futures_amount=Decimal("100"), futures_leverage=10,
                        futures_margin_type="CROSSED", sleep_fn=lambda: None,
                        operation_journal=self.journal, operation_scope=client._order_scope())
        return Strategy(client, **(settings | options))

    def run_strategy(self, client, **options):
        progress = []
        result = self.strategy(client, **options).run(threading.Event(),
            progress_cb=lambda step, total, text: progress.append((step, total, text)))
        return result, progress

    def test_completed_rounds_keep_funds_with_and_without_withdrawal_address(self):
        for address in ("test-target", ""):
            for rounds in (1, 3):
                with self.subTest(address=address, rounds=rounds):
                    exchange = _Exchange()
                    client = self.client(exchange)
                    result, progress = self.run_strategy(client, withdraw_address=address, futures_rounds=rounds)
                    self.assertFalse(result["trade_failed"])
                    self.assertEqual(progress, [(i, rounds, f"合约轮 {i}/{rounds} 完成") for i in range(1, rounds + 1)])
                    self.assertEqual([order["side"] for order in exchange.orders], ["BUY", "SELL"] * rounds)
                    self.assertEqual(len({order["clientOrderId"] for order in exchange.orders}), rounds * 2)
                    last_order = max(i for i, event in enumerate(exchange.events) if event[0] == "order")
                    self.assertEqual(exchange.events[last_order + 1:], [])
                    self.assertEqual(exchange.balances["UMFUTURE"]["USDT"], Decimal("36"))
                    self.assertEqual(exchange.balances["MAIN"]["USDT"], 0)
                    self.assertEqual(exchange.positions["BTCUSDT"], 0)
                    self.assertEqual(exchange.events[0][1]["type"], "UMFUTURE_MAIN")  # Pretrade check remains.
                    self.assertEqual(self.journal.active_entries(), [])

    def test_auto_withdraw_collects_only_selected_coin_after_closing(self):
        for coin in ("USDT", "USDC", "BNB"):
            with self.subTest(coin=coin):
                exchange = _Exchange()
                result, progress = self.run_strategy(self.client(exchange), enable_withdraw=True, withdraw_coin=coin)
                self.assertFalse(result["trade_failed"])
                self.assertGreater(result["withdraw_amount"], 0)
                last_order = max(i for i, event in enumerate(exchange.events) if event[0] == "order")
                after = exchange.events[last_order + 1:]
                self.assertEqual(after[-1][0], "withdraw")
                self.assertEqual(after[-1][1]["coin"], coin)
                self.assertTrue(all(params["asset"] == coin for kind, params in after if kind == "transfer"))
                self.assertTrue(all(wallet[coin] == 0 for wallet in exchange.balances.values()))
                if coin != "USDT":
                    self.assertEqual(exchange.balances["UMFUTURE"]["USDT"], Decimal("36"))
                self.assertEqual(progress[-1][2], "合约轮 1/1 完成")

    def test_restart_then_manual_withdraw_switches_coin_amount_address_and_account(self):
        first, second = _Exchange(), _Exchange()
        self.run_strategy(self.client(first))
        self.journal = OperationJournal(self.journal.file_path, secret_box=self.box, fail_unconfirmed=True)
        client = self.client(first)
        other = self.client(second, "other-test-account")
        untouched = deepcopy(second.balances)
        ids = []
        for i, coin in enumerate(("USDC", "BNB", "USDT")):
            before = deepcopy(first.balances)
            first.balances["UMFUTURE"][coin] += Decimal(i + 1)
            total = sum(wallet[coin] for wallet in first.balances.values())
            amount = client.withdraw_all_coin(coin, f"test-target-{i}", "BSC", fee_buffer=0,
                                              auto_collect_to_spot=True)
            self.assertEqual(Decimal(str(amount)), total)
            params = first.events[-1][1]
            self.assertEqual(params["address"], f"test-target-{i}")
            ids.append(params["withdrawOrderId"])
            self.assertTrue(all(wallet[coin] == 0 for wallet in first.balances.values()))
            for wallet in first.balances:
                for other_coin in before[wallet]:
                    if other_coin != coin:
                        self.assertEqual(first.balances[wallet][other_coin], before[wallet][other_coin])
            self.assertEqual(second.balances, untouched)
        self.assertEqual(len(set(ids)), 3)
        self.assertGreater(other.withdraw_all_coin("BNB", "other-target", "BSC", fee_buffer=0,
                                                 auto_collect_to_spot=True), 0)
        self.assertEqual(first.balances["MAIN"]["BNB"], 0)
        self.assertNotIn("test-target", self.journal.file_path.read_text())
        self.assertEqual(self.journal.active_entries(), [])

    def test_pretrade_transfer_rejection_still_blocks_opening(self):
        exchange = _Exchange()
        exchange.fail_preclear = True
        result, progress = self.run_strategy(self.client(exchange))
        self.assertTrue(result["trade_failed"])
        self.assertEqual(result["trade_error_stage"], "合约有问题")
        self.assertEqual(exchange.orders, [])
        self.assertEqual(progress[-1][2], "合约有问题")
        self.assertEqual(exchange.balances["UMFUTURE"]["USDT"], 12)

    def test_lost_close_reply_is_confirmed_without_reopening_or_collecting(self):
        exchange = _Exchange()
        exchange.lose_close_reply = True
        result, progress = self.run_strategy(self.client(exchange))
        self.assertFalse(result["trade_failed"])
        self.assertEqual(len(exchange.orders), 2)
        self.assertEqual(exchange.positions["BTCUSDT"], 0)
        self.assertEqual(progress[-1], (1, 1, "合约轮 1/1 完成"))
        self.assertEqual(exchange.balances["UMFUTURE"]["USDT"], 36)
        self.assertEqual(self.journal.active_entries(), [])

    def test_failed_or_stopped_trade_does_not_report_round_completion(self):
        for stopped in (False, True):
            with self.subTest(stopped=stopped):
                exchange = _Exchange()
                client = self.client(exchange)
                stop = threading.Event()
                progress = []
                if stopped:
                    stop.set()
                else:
                    exchange.fail_open = True
                result = self.strategy(client).run(stop, progress_cb=lambda *args: progress.append(args))
                self.assertFalse(any("完成" in item[2] for item in progress))
                self.assertEqual(exchange.orders, [])
                self.assertFalse(any(kind == "withdraw" for kind, _ in exchange.events))
                self.assertEqual(result["trade_failed"], not stopped)

    def test_reused_client_switches_futures_margin_asset_without_end_collection(self):
        exchange = _Exchange()
        client = self.client(exchange)
        for symbol, amount in (("BTCUSDT", Decimal("100")), ("BTCUSDC", Decimal("200"))):
            result, progress = self.run_strategy(client, futures_symbol=symbol, futures_amount=amount)
            self.assertFalse(result["trade_failed"])
            self.assertEqual(progress[-1][2], "合约轮 1/1 完成")
            self.assertEqual(exchange.events[-1][0], "order")
        self.assertEqual(exchange.balances["UMFUTURE"]["USDC"], 24)
        self.assertEqual(exchange.positions, {"BTCUSDT": Decimal(0), "BTCUSDC": Decimal(0)})
        self.assertEqual([Decimal(row["quantity"]) for row in exchange.orders], [Decimal(1)] * 2 + [Decimal(2)] * 2)

    def test_partial_collection_keeps_rejected_wallet_and_other_assets(self):
        exchange = _Exchange()
        exchange.fail_transfer_type = "CMFUTURE_MAIN"
        before = deepcopy(exchange.balances)
        client = self.client(exchange)
        amount = client.withdraw_all_coin("USDC", "test-partial-target", "BSC", fee_buffer=0,
                                          auto_collect_to_spot=True)
        self.assertEqual(amount, 24)
        self.assertEqual(exchange.balances["CMFUTURE"]["USDC"], before["CMFUTURE"]["USDC"])
        for wallet in before:
            self.assertEqual(exchange.balances[wallet]["USDT"], before[wallet]["USDT"])
            self.assertEqual(exchange.balances[wallet]["BNB"], before[wallet]["BNB"])

    def test_withdrawal_journal_write_failure_does_not_submit_funds(self):
        exchange = _Exchange()
        client = self.client(exchange)
        with patch.object(self.journal, "begin", side_effect=OSError("test disk full")):
            with self.assertRaisesRegex(OSError, "test disk full"):
                client.withdraw_all_coin("USDT", "test-target", "BSC", fee_buffer=0, auto_collect_to_spot=True)
        self.assertEqual(exchange.events, [])
        self.assertEqual(exchange.balances["UMFUTURE"]["USDT"], 12)


if __name__ == "__main__":
    unittest.main()
