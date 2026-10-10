import unittest
from exchange_order_recovery import exchange_operation_scope, validate_order_identity


class ExchangeOrderIdentityTests(unittest.TestCase):
    def test_scope_never_contains_api_key(self):
        self.assertNotIn("plain-api-key", exchange_operation_scope("plain-api-key"))

    def test_order_identity_rejects_unrelated_replies(self):
        for changes in ({"orderId": "102"}, {"symbol": "ETHUSDT"}, {"clientOrderId": "other"}, {"side": "SELL"}):
            row = {"orderId": "101", "symbol": "BTCUSDT", "clientOrderId": "original", "side": "BUY", **changes}
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                validate_order_identity(row, symbol="BTCUSDT", order_id="101", client_order_id="original", side="BUY")
