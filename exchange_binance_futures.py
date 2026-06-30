#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientFuturesMixin(object):
    def get_um_futures_exchange_info(self, symbol: str):
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        if symbol_u in self._um_futures_exchange_info_cache:
            return self._um_futures_exchange_info_cache[symbol_u]
        data = self.public_get(self.um_futures, "/fapi/v1/exchangeInfo", {})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = None
        for item in symbols:
            if str(item.get("symbol") or "").strip().upper() == symbol_u:
                info = item
                break
        if info is None:
            return None
        self._um_futures_exchange_info_cache[symbol_u] = info
        return info
    def ensure_um_futures_symbol_supported(self, symbol: str) -> dict[str, Any]:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            raise RuntimeError("合约交易对不能为空")
        info = self.get_um_futures_exchange_info(symbol_u)
        if not info:
            raise RuntimeError(f"找不到合约交易对：{symbol_u}")
        if str(info.get("status") or "").strip().upper() != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")
        return info
    def get_um_futures_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        try:
            data = self.public_get(self.um_futures, "/fapi/v1/ticker/price", {"symbol": symbol_u})
            price = Decimal(str(data.get("price")))
            self._um_futures_price_cache[symbol_u] = price
            return price
        except Exception:
            return None
    def get_um_futures_trade_rules(self, symbol: str):
        info = self.get_um_futures_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        market_lot = self._extract_filter(info, "MARKET_LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")
        price_filter = self._extract_filter(info, "PRICE_FILTER")

        market_lot = market_lot or lot or {}
        lot = lot or market_lot or {}
        price_filter = price_filter or {}

        step_size = self._decimal_from_str((market_lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((market_lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((market_lot or {}).get("maxQty", "999999999"), "999999999")
        tick_size = self._decimal_from_str((price_filter or {}).get("tickSize", "0.00000001"), "0.00000001")
        min_price = self._decimal_from_str((price_filter or {}).get("minPrice", "0"), "0")
        max_price = self._decimal_from_str((price_filter or {}).get("maxPrice", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("notional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "tickSize": tick_size,
            "minPrice": min_price,
            "maxPrice": max_price,
            "minNotional": min_notional_val,
            "quantityPrecision": max(0, int(info.get("quantityPrecision", 8) or 8)),
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
            "marginAsset": info.get("marginAsset"),
            "marketTakeBound": self._decimal_from_str(info.get("marketTakeBound", "0"), "0"),
        }
    def get_um_futures_margin_asset(self, symbol: str) -> str:
        symbol_u = self.normalize_trade_symbol(symbol)
        info = self.ensure_um_futures_symbol_supported(symbol_u)
        margin_asset = str(info.get("marginAsset") or info.get("quoteAsset") or "").strip().upper()
        if not margin_asset:
            raise RuntimeError(f"合约交易对保证金币种无效：{symbol_u}")
        return margin_asset
    def get_um_futures_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get(self.um_futures, "/fapi/v1/ticker/bookTicker", {"symbol": symbol_u})
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"读取合约盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }
    def get_um_futures_symbol_config(self, symbol: str) -> dict | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v1/symbolConfig",
            {"symbol": symbol_u},
        )
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict):
            return data
        return None
    def get_um_futures_position_mode(self) -> bool:
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v1/positionSide/dual",
            {},
        )
        return bool(data.get("dualSidePosition"))
    def ensure_um_futures_one_way_mode(self) -> bool:
        dual_mode = self.get_um_futures_position_mode()
        if not dual_mode:
            return False
        try:
            self.request(
                self.um_futures,
                "POST",
                "/fapi/v1/positionSide/dual",
                {"dualSidePosition": "false"},
            )
            logger.info("U本位合约已切换为单向仓模式")
            return True
        except Exception as exc:
            if self._error_text_contains(exc, "no need to change position side"):
                return False
            raise
    def ensure_um_futures_margin_type(self, symbol: str, margin_type: str) -> bool:
        symbol_u = self.normalize_trade_symbol(symbol)
        target_margin_type = str(margin_type or FUTURES_MARGIN_TYPE_DEFAULT).strip().upper()
        config = self.get_um_futures_symbol_config(symbol_u)
        current_margin_type = str((config or {}).get("marginType") or "").strip().upper()
        if current_margin_type == target_margin_type:
            return False
        try:
            self.request(
                self.um_futures,
                "POST",
                "/fapi/v1/marginType",
                {
                    "symbol": symbol_u,
                    "marginType": target_margin_type,
                },
            )
            logger.info("U本位合约 %s 保证金模式已设置为 %s", symbol_u, target_margin_type)
            return True
        except Exception as exc:
            if self._error_text_contains(exc, "no need to change margin type"):
                return False
            raise
    def ensure_um_futures_leverage(self, symbol: str, leverage: int) -> int:
        symbol_u = self.normalize_trade_symbol(symbol)
        leverage_i = int(leverage)
        if leverage_i < 1 or leverage_i > 125:
            raise RuntimeError("合约杠杆必须在 1-125 之间")
        config = self.get_um_futures_symbol_config(symbol_u)
        try:
            current_leverage = int((config or {}).get("leverage"))
        except Exception:
            current_leverage = 0
        if current_leverage == leverage_i:
            return leverage_i
        data = self.request(
            self.um_futures,
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol_u,
                "leverage": leverage_i,
            },
        )
        final_leverage = leverage_i
        try:
            final_leverage = int(data.get("leverage", leverage_i))
        except Exception:
            pass
        logger.info("U本位合约 %s 杠杆已设置为 %s", symbol_u, final_leverage)
        return final_leverage
    def um_futures_account_info(self) -> dict:
        return self.request(
            self.um_futures,
            "GET",
            "/fapi/v3/account",
            {},
        )
    def um_futures_asset_balance(self, asset: str) -> dict[str, Decimal]:
        asset_u = str(asset or "").strip().upper()
        info = self.um_futures_account_info()
        for item in info.get("assets", []):
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            return {
                "walletBalance": self._decimal_from_str(item.get("walletBalance", "0"), "0"),
                "availableBalance": self._decimal_from_str(item.get("availableBalance", "0"), "0"),
                "marginBalance": self._decimal_from_str(item.get("marginBalance", "0"), "0"),
                "crossWalletBalance": self._decimal_from_str(item.get("crossWalletBalance", "0"), "0"),
                "maxWithdrawAmount": self._decimal_from_str(item.get("maxWithdrawAmount", "0"), "0"),
            }
        return {
            "walletBalance": Decimal("0"),
            "availableBalance": Decimal("0"),
            "marginBalance": Decimal("0"),
            "crossWalletBalance": Decimal("0"),
            "maxWithdrawAmount": Decimal("0"),
        }
    @staticmethod
    def _normalize_um_futures_position_side(position_side: str | None) -> str:
        value = str(position_side or "").strip().upper()
        return value if value in {"BOTH", "LONG", "SHORT"} else ""
    def get_um_futures_positions(self, symbol: str) -> list[dict]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v3/positionRisk",
            {"symbol": symbol_u},
        )
        items = data if isinstance(data, list) else [data]
        return [
            item
            for item in items
            if str(item.get("symbol") or "").strip().upper() == symbol_u
        ]
    def get_um_futures_position(self, symbol: str) -> dict | None:
        items = self.get_um_futures_positions(symbol)
        selected = None
        for item in items:
            position_side = self._normalize_um_futures_position_side(item.get("positionSide"))
            position_amt = self._decimal_from_str(item.get("positionAmt", "0"), "0")
            if position_side == "BOTH":
                return item
            if selected is None or position_amt != 0:
                selected = item
        return selected
    def calculate_um_futures_order_quantity(self, symbol: str, notional_amount, direction_side: str) -> Decimal:
        symbol_u = self.normalize_trade_symbol(symbol)
        amount = Decimal(str(notional_amount))
        if amount <= 0:
            raise RuntimeError("合约下单金额必须大于 0")

        rules = self.get_um_futures_trade_rules(symbol_u)
        if not rules:
            raise RuntimeError(f"找不到合约交易对规则：{symbol_u}")
        if rules["status"] != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")

        book_ticker = self.get_um_futures_book_ticker(symbol_u)
        side_u = str(direction_side or "").strip().upper()
        reference_price = Decimal(str(book_ticker["askPrice"])) if side_u == "BUY" else Decimal(str(book_ticker["bidPrice"]))
        if reference_price <= 0:
            raise RuntimeError(f"合约参考价格无效：{symbol_u}")

        effective_min_qty = rules["minQty"]
        if rules["minNotional"] > 0:
            effective_min_qty = max(
                effective_min_qty,
                self._ceil_to_step(rules["minNotional"] / reference_price, rules["stepSize"]),
            )
        effective_min_notional = effective_min_qty * reference_price

        qty = self._floor_to_step(amount / reference_price, rules["stepSize"])
        quantity_precision = int(rules.get("quantityPrecision", 8) or 8)
        if quantity_precision >= 0:
            qty = qty.quantize(Decimal("1").scaleb(-quantity_precision), rounding=ROUND_DOWN)
        if qty <= 0 or qty < rules["minQty"]:
            raise RuntimeError(
                f"{symbol_u} 合约下单数量过小：{self._format_decimal(qty)} < 最小数量 {self._format_decimal(rules['minQty'])}"
            )
        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if rules["minNotional"] > 0 and (qty * reference_price) < rules["minNotional"]:
            margin_asset = str(rules.get("marginAsset") or rules.get("quoteAsset") or "USDT").strip().upper()
            raise RuntimeError(
                f"{symbol_u} 当前市价下最小可下金额约为 {self._format_decimal(effective_min_notional)} {margin_asset}，"
                f"你填写的是 {self._format_decimal(amount)} {margin_asset}"
            )
        return qty
    def place_um_futures_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal | str | float | int,
        *,
        reduce_only: bool = False,
        position_side: str | None = None,
    ):
        symbol_u = self.normalize_trade_symbol(symbol)
        side_u = str(side or "").strip().upper()
        position_side_u = self._normalize_um_futures_position_side(position_side)
        rules = self.get_um_futures_trade_rules(symbol_u)
        if not rules:
            raise RuntimeError(f"找不到合约交易对规则：{symbol_u}")
        if rules["status"] != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")

        qty_raw = Decimal(str(quantity))
        qty = self._floor_to_step(qty_raw, rules["stepSize"])
        quantity_precision = int(rules.get("quantityPrecision", 8) or 8)
        if quantity_precision >= 0:
            qty = qty.quantize(Decimal("1").scaleb(-quantity_precision), rounding=ROUND_DOWN)
        if qty <= 0 or qty < rules["minQty"]:
            raise RuntimeError(
                f"{symbol_u} 合约下单数量过小：{self._format_decimal(qty)} < 最小数量 {self._format_decimal(rules['minQty'])}"
            )
        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if not reduce_only:
            ref_price = self.get_um_futures_symbol_price(symbol_u)
            if ref_price and rules["minNotional"] > 0 and (qty * ref_price) < rules["minNotional"]:
                raise RuntimeError(
                    f"{symbol_u} 合约下单金额过小：{self._format_decimal(qty * ref_price)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
                )

        params = {
            "symbol": symbol_u,
            "side": side_u,
            "type": "MARKET",
            "quantity": self._format_decimal(qty),
            "newOrderRespType": "RESULT",
        }
        if position_side_u and position_side_u != "BOTH":
            params["positionSide"] = position_side_u
        if reduce_only:
            if position_side_u and position_side_u != "BOTH":
                raise RuntimeError("对冲模式平仓请使用 positionSide 定向平仓，不支持同时传 reduceOnly")
            params["reduceOnly"] = "true"

        data = self.request(
            self.um_futures,
            "POST",
            "/fapi/v1/order",
            params,
        )
        logger.info(
            "U本位合约市价下单 %s %s，数量=%s，positionSide=%s，reduceOnly=%s",
            symbol_u,
            side_u,
            params["quantity"],
            params.get("positionSide", "BOTH"),
            "true" if reduce_only else "false",
        )
        return data
    def close_all_um_futures_positions_market(self, symbol: str) -> list[dict]:
        close_orders = []
        for position in self.get_um_futures_positions(symbol):
            position_amt = self._decimal_from_str(position.get("positionAmt", "0"), "0")
            if position_amt == 0:
                continue

            position_side = self._normalize_um_futures_position_side(position.get("positionSide"))
            if position_side == "LONG":
                side = "SELL"
            elif position_side == "SHORT":
                side = "BUY"
            else:
                side = "SELL" if position_amt > 0 else "BUY"
                position_side = "BOTH"

            close_orders.append(
                self.place_um_futures_market_order(
                    symbol,
                    side,
                    abs(position_amt),
                    reduce_only=(position_side == "BOTH"),
                    position_side=position_side,
                )
            )
        return close_orders
    def close_um_futures_position_market(self, symbol: str):
        close_orders = self.close_all_um_futures_positions_market(symbol)
        return close_orders[0] if close_orders else None


__all__ = ["BinanceClientFuturesMixin"]
