#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientEarnMixin(object):
    def get_simple_earn_flexible_positions(
        self,
        *,
        asset: str | None = None,
        size: int = 100,
    ) -> list[dict[str, Any]]:
        asset_u = str(asset or "").strip().upper()
        page_size = max(1, min(100, int(size or 100)))
        current = 1
        positions: list[dict[str, Any]] = []

        while True:
            params: dict[str, Any] = {
                "current": current,
                "size": page_size,
            }
            if asset_u:
                params["asset"] = asset_u

            data = self.request(
                self.spot,
                "GET",
                "/sapi/v1/simple-earn/flexible/position",
                params,
            )
            rows = []
            total = 0
            if isinstance(data, dict):
                rows = data.get("rows") or []
                try:
                    total = max(0, int(data.get("total", 0) or 0))
                except Exception:
                    total = 0
            elif isinstance(data, list):
                rows = data

            normalized_rows: list[dict[str, Any]] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                product_id = str(item.get("productId") or "").strip()
                row_asset = str(item.get("asset") or "").strip().upper()
                if asset_u and row_asset != asset_u:
                    continue
                total_amount = self._decimal_from_str(item.get("totalAmount", "0"), "0")
                redeeming_amount = self._decimal_from_str(item.get("redeemingAmount", "0"), "0")
                normalized_rows.append(
                    {
                        "productId": product_id,
                        "asset": row_asset,
                        "totalAmount": total_amount,
                        "redeemingAmount": redeeming_amount,
                        "canRedeem": self._bool_from_value(item.get("canRedeem")),
                        "autoSubscribe": self._bool_from_value(item.get("autoSubscribe")),
                    }
                )
            positions.extend(normalized_rows)

            if total > 0 and len(positions) >= total:
                break
            if len(rows) < page_size and total <= 0:
                break
            if not rows:
                break
            current += 1
            if current > 100:
                logger.warning("活期理财持仓分页超过 100 页，提前停止拉取")
                break

        return positions
    def redeem_simple_earn_flexible_product(
        self,
        product_id: str,
        *,
        redeem_all: bool = True,
        amount: Decimal | str | float | int | None = None,
        dest_account: str = "SPOT",
    ) -> dict[str, Any]:
        product_id_text = str(product_id or "").strip()
        if not product_id_text:
            raise RuntimeError("活期理财 productId 为空")

        params: dict[str, Any] = {
            "productId": product_id_text,
            "redeemAll": "true" if redeem_all else "false",
        }
        dest_account_text = str(dest_account or "").strip().upper()
        if dest_account_text:
            params["destAccount"] = dest_account_text

        if not redeem_all:
            amount_dec = self._decimal_from_str(amount, "0")
            if amount_dec <= 0:
                raise RuntimeError("活期理财部分赎回 amount 必须大于 0")
            params["amount"] = self._format_decimal(amount_dec)

        return self.request(
            self.spot,
            "POST",
            "/sapi/v1/simple-earn/flexible/redeem",
            params,
        )
    def set_simple_earn_flexible_auto_subscribe(
        self,
        product_id: str,
        auto_subscribe: bool,
    ) -> dict[str, Any]:
        product_id_text = str(product_id or "").strip()
        if not product_id_text:
            raise RuntimeError("活期理财 productId 为空")
        return self.request(
            self.spot,
            "POST",
            "/sapi/v1/simple-earn/flexible/setAutoSubscribe",
            {
                "productId": product_id_text,
                "autoSubscribe": "true" if bool(auto_subscribe) else "false",
            },
        )


__all__ = ["BinanceClientEarnMixin"]
