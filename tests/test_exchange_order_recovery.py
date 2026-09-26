#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from exchange_order_recovery import exchange_operation_scope, reconcile_exchange_orders


class _Journal:
    def __init__(self, entries):
        self.entries = entries
        self.updates = []

    def active_entries(self, **_kwargs):
        return list(self.entries)

    def update(self, entry_id, state, **changes):
        self.updates.append((entry_id, state, changes))


class _Client:
    def __init__(self, status):
        self.status = status

    def get_order(self, _symbol, _order_id):
        return {"status": self.status}

    def get_order_by_client_id(self, _symbol, _client_order_id):
        return {"status": self.status}


class ExchangeOrderRecoveryTests(unittest.TestCase):
    def _entry(self):
        return {"id": "entry-1", "payload": {"symbol": "BTCUSDT", "order_id": "101"}}

    def test_scope_never_contains_api_key(self) -> None:
        scope = exchange_operation_scope("plain-api-key")
        self.assertNotIn("plain-api-key", scope)
        self.assertTrue(scope.startswith("account:"))

    def test_filled_order_is_terminal_after_reconciliation(self) -> None:
        journal = _Journal([self._entry()])
        reconcile_exchange_orders(journal, _Client("FILLED"), "api-key")
        self.assertEqual(journal.updates, [("entry-1", "completed", {"exchange_status": "FILLED"})])

    def test_open_order_blocks_new_execution(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "未完成交易所挂单"):
            reconcile_exchange_orders(_Journal([self._entry()]), _Client("NEW"), "api-key")

    def test_unconfirmed_client_order_id_blocks_until_exchange_confirms_terminal_state(self) -> None:
        entry = {"id": "entry-2", "payload": {"symbol": "BTCUSDT", "client_order_id": "xja123"}}
        with self.assertRaisesRegex(RuntimeError, "未完成交易所挂单"):
            reconcile_exchange_orders(_Journal([entry]), _Client("NEW"), "api-key")
        journal = _Journal([entry])
        reconcile_exchange_orders(journal, _Client("FILLED"), "api-key")
        self.assertEqual(journal.updates, [("entry-2", "completed", {"exchange_status": "FILLED"})])


if __name__ == "__main__":
    unittest.main()
