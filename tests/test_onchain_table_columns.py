#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from page_onchain_layout import OnchainLayoutMixin


class _FakeTree:
    def __init__(self, width: int, rows: list[tuple[str, ...]]):
        self._width = width
        self._rows = rows
        self.columns: dict[str, dict[str, object]] = {}

    def winfo_width(self) -> int:
        return self._width

    def get_children(self, _item: str = "") -> list[str]:
        return [f"row-{index}" for index in range(len(self._rows))]

    def item(self, row_id: str, option: str | None = None):
        index = int(str(row_id).split("-", 1)[1])
        if option == "values":
            return self._rows[index]
        return {"values": self._rows[index]}

    def column(self, column: str, **kwargs):
        self.columns[column] = dict(kwargs)


class _FakeWrap:
    @staticmethod
    def winfo_width() -> int:
        return 920


class _TableColumnOwner(OnchainLayoutMixin):
    TREE_COL_MIN_WIDTHS = {
        "checked": 42,
        "idx": 42,
        "source": 78,
        "target": 96,
        "status": 86,
        "recovery": 82,
        "balance": 82,
    }
    TREE_COL_MAX_WIDTHS = {
        "checked": 46,
        "idx": 64,
        "source": 260,
        "target": 360,
        "status": 300,
        "recovery": 190,
        "balance": 210,
    }

    def __init__(self, tree: _FakeTree):
        self.tree = tree
        self.table_wrap = _FakeWrap()
        self._tree_column_ids = ("checked", "idx", "source", "target", "status", "recovery", "balance")
        self._tree_heading_base_texts = {
            "checked": "勾选",
            "idx": "编号",
            "source": "转出凭证",
            "target": "接收地址",
            "status": "执行状态",
            "recovery": "回收状态",
            "balance": "余额",
        }


class OnchainTableColumnSizingTests(unittest.TestCase):
    def test_columns_shrink_to_current_content_instead_of_fixed_large_widths(self) -> None:
        tree = _FakeTree(
            920,
            [
                ("✓", "1", "a1...b2", "0x9999999999999999999999999999999999999999", "❌代币余额不足", "-", "BNB:0"),
                ("✓", "2", "c3...d4", "0x8888888888888888888888888888888888888888", "✅0.001bnb", "-", "BNB:0.001"),
            ],
        )
        owner = _TableColumnOwner(tree)

        owner._resize_tree_columns()

        widths = {column: int(options["width"]) for column, options in tree.columns.items()}
        self.assertLess(widths["source"], 160)
        self.assertGreaterEqual(widths["target"], 300)
        self.assertLessEqual(widths["target"], owner.TREE_COL_MAX_WIDTHS["target"])
        self.assertGreater(widths["status"], owner.TREE_COL_MIN_WIDTHS["status"])
        self.assertLessEqual(sum(widths.values()), 920)
        self.assertTrue(all(options.get("stretch") is False for options in tree.columns.values()))

    def test_narrow_table_respects_min_widths_for_all_columns(self) -> None:
        tree = _FakeTree(360, [("✓", "12", "a", "0x1", "❌网络异常：Read timed out", "待处理", "BNB:0")])
        owner = _TableColumnOwner(tree)

        owner._resize_tree_columns()

        widths = {column: int(options["width"]) for column, options in tree.columns.items()}
        for column, min_width in owner.TREE_COL_MIN_WIDTHS.items():
            self.assertGreaterEqual(widths[column], min_width)


if __name__ == "__main__":
    unittest.main()
