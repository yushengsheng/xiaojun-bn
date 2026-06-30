#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientAccountMixin(object):
    def spot_balance(self, asset: str) -> float:
        data = self.request(self.spot, "GET", "/api/v3/account")
        for b in data.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", 0))
        return 0.0
    def spot_all_balances(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        data = requester(self.spot, "GET", "/api/v3/account", {}, timeout=6, max_retries=1) if fast else requester(self.spot, "GET", "/api/v3/account")
        result = []
        for b in data.get("balances", []):
            free_amt = float(b.get("free", 0) or 0)
            locked_amt = float(b.get("locked", 0) or 0)
            total = free_amt + locked_amt
            if total > 0:
                result.append({
                    "asset": b.get("asset"),
                    "free": free_amt,
                    "locked": locked_amt,
                    "total": total,
                })
        return result
    def query_total_wallet_balance(self, quote_asset="USDT", *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 8, "max_retries": 2} if fast else {}
        data = requester(
            self.spot,
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": quote_asset},
            **request_kwargs,
        )
        total = Decimal("0")
        rows = []
        for item in data:
            bal = Decimal(str(item.get("balance", "0")))
            wallet_name = item.get("walletName", "Unknown")
            active = bool(item.get("activate", False))
            rows.append({
                "walletName": wallet_name,
                "balance": bal,
                "activate": active,
            })
            total += bal
        return total, rows
    def query_asset_balances_breakdown(self, *, fast: bool = False) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}

        def add_amount(asset: str, amount) -> None:
            asset_u = str(asset or "").strip().upper()
            if not asset_u:
                return
            amount_dec = Decimal(str(amount or "0"))
            if amount_dec <= 0:
                return
            totals[asset_u] = totals.get(asset_u, Decimal("0")) + amount_dec

        try:
            for item in self.spot_all_balances(fast=fast):
                add_amount(item.get("asset", ""), item.get("total", 0))
        except Exception as e:
            logger.warning("查询现货资产明细失败: %s", e)

        try:
            for item in self.funding_positive_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("free", 0))
        except Exception as e:
            logger.warning("查询资金账户资产明细失败: %s", e)

        try:
            for item in self.um_futures_transferable_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            logger.warning("查询 U本位资产明细失败: %s", e)

        try:
            for item in self.cm_futures_transferable_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            logger.warning("查询 币本位资产明细失败: %s", e)

        return totals

    # -------- 工具：从 symbol 推断现货基础币种 --------


__all__ = ["BinanceClientAccountMixin"]
