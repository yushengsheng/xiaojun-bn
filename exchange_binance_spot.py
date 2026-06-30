#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientSpotMixin(object):
    @staticmethod
    def normalize_trade_symbol(symbol: str) -> str:
        symbol_u = str(symbol or "").strip().upper()
        if "/" not in symbol_u:
            return symbol_u
        parts = [part.strip() for part in symbol_u.split("/") if part.strip()]
        if len(parts) == 2 and all(re.fullmatch(r"[A-Z0-9]+", part) for part in parts):
            return "".join(parts)
        return symbol_u
    @classmethod
    def split_spot_symbol(cls, symbol: str) -> tuple[str, str]:
        symbol_u = cls.normalize_trade_symbol(symbol)
        for suffix in SUPPORTED_QUOTE_ASSET_SUFFIXES:
            if symbol_u.endswith(suffix) and len(symbol_u) > len(suffix):
                return symbol_u[:-len(suffix)], suffix
        return "", ""
    @classmethod
    def _validated_spot_symbol_parts(cls, symbol: str) -> tuple[str, str, str]:
        symbol_u = cls.normalize_trade_symbol(symbol)
        if not symbol_u:
            raise RuntimeError("现货交易对不能为空")
        base_asset, quote_asset = cls.split_spot_symbol(symbol_u)
        if not base_asset or not quote_asset:
            raise RuntimeError(f"现货交易对格式不正确：{symbol_u}")
        return symbol_u, base_asset, quote_asset
    @classmethod
    def get_spot_base_asset(cls, symbol: str) -> str:
        _symbol_u, base_asset, _quote_asset = cls._validated_spot_symbol_parts(symbol)
        return base_asset
    @classmethod
    def get_spot_quote_asset(cls, symbol: str) -> str:
        _symbol_u, _base_asset, quote_asset = cls._validated_spot_symbol_parts(symbol)
        return quote_asset
    def ensure_spot_symbol_supported(self, symbol: str) -> tuple[str, str]:
        symbol_u, base_asset, quote_asset = self._validated_spot_symbol_parts(symbol)
        try:
            info = self.get_exchange_info(symbol_u)
        except Exception as exc:
            raise RuntimeError(f"现货交易对校验失败：{symbol_u}") from exc
        if not info:
            raise RuntimeError(f"找不到现货交易对：{symbol_u}")
        if str(info.get("status") or "").strip().upper() != "TRADING":
            raise RuntimeError(f"现货交易对不可交易：{symbol_u}")
        return base_asset, quote_asset

    # -------- 公共行情 / 交易规则 --------
    def get_exchange_info(self, symbol: str):
        symbol = self.normalize_trade_symbol(symbol)
        if symbol in self._exchange_info_cache:
            return self._exchange_info_cache[symbol]
        data = self.public_get(self.spot, "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = symbols[0]
        self._exchange_info_cache[symbol] = info
        return info
    def get_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol = self.normalize_trade_symbol(symbol)
        try:
            data = self.public_get(self.spot, "/api/v3/ticker/price", {"symbol": symbol})
            price = Decimal(str(data.get("price")))
            self._price_cache[symbol] = price
            return price
        except Exception:
            return None
    @staticmethod
    def _extract_filter(symbol_info: dict, filter_type: str):
        for f in symbol_info.get("filters", []):
            if f.get("filterType") == filter_type:
                return f
        return None
    @staticmethod
    def _decimal_from_str(v, default="0"):
        try:
            return Decimal(str(v))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(default)
    @staticmethod
    def _floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return qty
        return (qty // step) * step
    @staticmethod
    def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        if value <= 0:
            return Decimal("0")
        units = (value / step).to_integral_value(rounding=ROUND_UP)
        return units * step
    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        text = format(Decimal(str(value)).normalize(), "f")
        if text in {"-0", "-0.0"}:
            return "0"
        return text
    @staticmethod
    def _positive_int(value, default: int = 8) -> int:
        try:
            parsed = int(value)
        except Exception:
            return int(default)
        return parsed if parsed >= 0 else int(default)
    @staticmethod
    def _bool_from_value(value: object) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y"}
    def get_symbol_trade_rules(self, symbol: str):
        info = self.get_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")
        price_filter = self._extract_filter(info, "PRICE_FILTER")

        step_size = self._decimal_from_str((lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((lot or {}).get("maxQty", "999999999"), "999999999")
        tick_size = self._decimal_from_str((price_filter or {}).get("tickSize", "0.00000001"), "0.00000001")
        min_price = self._decimal_from_str((price_filter or {}).get("minPrice", "0"), "0")
        max_price = self._decimal_from_str((price_filter or {}).get("maxPrice", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("minNotional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        quote_precision_candidates = []
        for key in ("quoteAssetPrecision", "quotePrecision"):
            raw_value = info.get(key)
            try:
                parsed = int(raw_value)
            except Exception:
                continue
            if parsed >= 0:
                quote_precision_candidates.append(parsed)
        quote_order_qty_fraction = min(quote_precision_candidates) if quote_precision_candidates else 8

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "tickSize": tick_size,
            "minPrice": min_price,
            "maxPrice": max_price,
            "minNotional": min_notional_val,
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
            "quoteOrderQtyFraction": self._positive_int(quote_order_qty_fraction, 8),
        }
    def get_symbol_tick_size(self, symbol: str) -> Decimal:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return Decimal("0")
        tick_size = self._decimal_from_str(rules.get("tickSize", "0"), "0")
        return tick_size if tick_size > 0 else Decimal("0")
    def normalize_price_delta(self, symbol: str, delta: Decimal | str | float | int, *, min_one_tick: bool = True) -> Decimal:
        value = self._decimal_from_str(delta, "0")
        if value < 0:
            value = Decimal("0")
        tick_size = self.get_symbol_tick_size(symbol)
        if tick_size <= 0:
            return value
        if min_one_tick and value <= 0:
            return tick_size
        normalized = self._ceil_to_step(value, tick_size)
        if min_one_tick and normalized < tick_size:
            normalized = tick_size
        return normalized
    def get_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get(self.spot, "/api/v3/ticker/bookTicker", {"symbol": symbol_u})
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"读取盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }
    def place_limit_order(self, symbol: str, side: str, quantity: Decimal, price: Decimal):
        symbol_u = self.normalize_trade_symbol(symbol)
        side_u = side.upper()
        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过挂单 %s", symbol_u)
            return None
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过挂单", symbol_u)
            return None

        qty = self._floor_to_step(Decimal(str(quantity)), rules["stepSize"])
        order_price = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0 and self._floor_to_step(order_price, tick_size) != order_price:
            raise RuntimeError(f"挂单价格不符合最小价格精度：{order_price}")

        if qty <= 0 or qty < rules["minQty"]:
            logger.info("交易对 %s 挂单数量过小，跳过", symbol_u)
            return None

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if rules["minNotional"] > 0 and (qty * order_price) < rules["minNotional"]:
            logger.info(
                "交易对 %s 挂单金额 %s 小于最小下单额 %s，跳过",
                symbol_u,
                self._format_decimal(qty * order_price),
                self._format_decimal(rules["minNotional"]),
            )
            return None

        params = {
            "symbol": symbol_u,
            "side": side_u,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self._format_decimal(qty),
            "price": self._format_decimal(order_price),
        }
        data = self.request(self.spot, "POST", "/api/v3/order", params)
        logger.info(
            "限价挂单 %s %s：数量=%s 价格=%s",
            symbol_u,
            side_u,
            params["quantity"],
            params["price"],
        )
        return data
    def get_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "GET",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
        )
    def cancel_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "DELETE",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
        )
    def wait_order_filled(self, symbol: str, order_id: int | str, stop_event=None, poll_interval: float = 1.0):
        symbol_u = self.normalize_trade_symbol(symbol)
        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")
            time.sleep(max(0.2, float(poll_interval)))
    def get_order_average_price(self, order_data: dict) -> Decimal | None:
        if not isinstance(order_data, dict):
            return None
        try:
            executed_qty = Decimal(str(order_data.get("executedQty", "0")))
        except Exception:
            executed_qty = Decimal("0")
        try:
            quote_qty = Decimal(str(order_data.get("cummulativeQuoteQty", "0")))
        except Exception:
            quote_qty = Decimal("0")
        if executed_qty > 0 and quote_qty > 0:
            try:
                return quote_qty / executed_qty
            except Exception:
                pass
        try:
            price = Decimal(str(order_data.get("price", "0")))
        except Exception:
            price = Decimal("0")
        return price if price > 0 else None
    def spot_limit_buy_all_usdt(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        quote_asset = self.get_spot_quote_asset(symbol)
        self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = Decimal(str(self.spot_balance(quote_asset)))
        if quote_balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单买入", quote_asset)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单买入价格无效，跳过")
            return None

        amount_quote = quote_balance * Decimal(str(reserve_ratio))
        qty = self._floor_to_step(amount_quote / price_dec, rules["stepSize"])
        return self.place_limit_order(symbol, "BUY", qty, price_dec)
    def spot_limit_buy_quote_amount(self, symbol: str, price: Decimal, quote_amount: Decimal | str | float | int):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        amount_quote = Decimal(str(quote_amount))
        if amount_quote <= 0:
            logger.info("挂单买入金额 <= 0，跳过 %s", symbol)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单买入价格无效，跳过")
            return None

        qty = self._floor_to_step(amount_quote / price_dec, rules["stepSize"])
        return self.place_limit_order(symbol, "BUY", qty, price_dec)
    def spot_limit_sell_quantity(self, symbol: str, price: Decimal, quantity: Decimal | str | float | int):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        qty = Decimal(str(quantity))
        if qty <= 0:
            logger.info("挂单卖出数量 <= 0，跳过 %s", symbol)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单卖出价格无效，跳过")
            return None

        qty = self._floor_to_step(qty, rules["stepSize"])
        return self.place_limit_order(symbol, "SELL", qty, price_dec)
    def spot_buy_quote_amount(self, symbol: str, quote_amount: Decimal | str | float | int):
        symbol_u = self.normalize_trade_symbol(symbol)
        amount = Decimal(str(quote_amount))
        if amount <= 0:
            logger.info("买入金额 <= 0，跳过买入 %s", symbol_u)
            return False

        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过买入 %s", symbol_u)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过买入", symbol_u)
            return False

        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        quote_order_qty_fraction = int(rules.get("quoteOrderQtyFraction", 8) or 8)
        amount = self._quantize_to_fraction(amount, quote_order_qty_fraction)
        quote_balance = self._quantize_to_fraction(quote_balance, quote_order_qty_fraction)
        if quote_balance < amount:
            raise RuntimeError(
                f"现货 {quote_asset} 余额不足：需要 {self._format_decimal(amount)}，当前 {self._format_decimal(quote_balance)}"
            )
        if amount <= 0:
            logger.info("买入金额按精度截断后 <= 0，跳过买入 %s", symbol_u)
            return False

        if rules["minNotional"] > 0 and amount < rules["minNotional"]:
            raise RuntimeError(
                f"{symbol_u} 买入金额过小：{self._format_decimal(amount)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
            )

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol_u,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": self._format_decimal(amount),
            },
        )
        logger.info("现货市价买入 %s，使用 %s 金额 %s", symbol_u, quote_asset, self._format_decimal(amount))
        return True
    def buy_bnb_with_quote_amount(
        self,
        quote_asset: str,
        amount: Decimal | str | float | int,
        *,
        assume_spot_funds_ready: bool = False,
        spot_balance_snapshot: Decimal | str | float | int | None = None,
        return_order: bool = False,
    ):
        quote_asset_u = str(quote_asset or "").strip().upper()
        if not quote_asset_u:
            raise RuntimeError("后置币种为空，无法预买 BNB")
        if quote_asset_u == "BNB":
            logger.info("后置币种为 BNB，无需预买 BNB")
            return None if return_order else False

        amount_dec = Decimal(str(amount or "0"))
        if amount_dec <= 0:
            logger.info("预买 BNB 金额 <= 0，跳过")
            return None if return_order else False

        if not assume_spot_funds_ready:
            self.collect_asset_to_spot(quote_asset_u, context="交易前")
        if spot_balance_snapshot is None:
            quote_balance = self.spot_asset_balance_decimal(quote_asset_u)
        else:
            quote_balance = Decimal(str(spot_balance_snapshot or "0"))
        if quote_balance < amount_dec:
            raise RuntimeError(
                f"现货 {quote_asset_u} 余额不足：需要 {self._format_decimal(amount_dec)}，当前 {self._format_decimal(quote_balance)}"
            )

        order = self.convert_with_from_amount(quote_asset_u, "BNB", amount_dec)
        if not order:
            logger.info(
                "闪兑预买 BNB 未执行：%s 金额 %s",
                quote_asset_u,
                self._format_decimal(amount_dec),
            )
            return None if return_order else False

        logger.info(
            "已通过闪兑预买 BNB：使用 %s 金额 %s",
            quote_asset_u,
            self._format_decimal(amount_dec),
        )
        return order if return_order else True
    def spot_limit_sell_all_base(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        base = self.get_spot_base_asset(symbol)
        balance = Decimal(str(self.spot_balance(base)))
        if balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单卖出", base)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单卖出价格无效，跳过")
            return None

        qty = self._floor_to_step(balance * Decimal(str(reserve_ratio)), rules["stepSize"])
        return self.place_limit_order(symbol, "SELL", qty, price_dec)
    def adjust_price_to_valid_tick(self, symbol: str, target_price: Decimal, *, round_up: bool = False) -> Decimal:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return Decimal(str(target_price))
        price = Decimal(str(target_price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size <= 0:
            return price
        normalized = self._floor_to_step(price, tick_size)
        if normalized == price:
            return price
        if round_up:
            return self._ceil_to_step(price, tick_size)
        return normalized
    def sell_asset_market(
        self,
        symbol: str,
        free_balance: Decimal | str | float | int,
        reserve_ratio=Decimal("0.999"),
        *,
        small_qty_log_text: str | None = None,
    ) -> bool:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            logger.info("找不到交易对规则，跳过卖出 %s", symbol)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过", symbol)
            return False

        qty = Decimal(str(free_balance)) * reserve_ratio
        qty = self._floor_to_step(qty, rules["stepSize"])

        if qty <= 0 or qty < rules["minQty"]:
            if small_qty_log_text:
                logger.info("%s", small_qty_log_text)
            else:
                logger.info("交易对 %s 数量过小，跳过卖出", symbol)
            return False

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol)
        if price:
            notional = qty * price
            if rules["minNotional"] > 0 and notional < rules["minNotional"]:
                logger.info(
                    "交易对 %s 名义价值 %s 小于最小下单额 %s，跳过",
                    symbol,
                    self._format_decimal(notional),
                    self._format_decimal(rules["minNotional"]),
                )
                return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty.normalize(), "f"),
            },
        )
        logger.info("现货市价卖出 %s，数量 %s", symbol, format(qty.normalize(), "f"))
        return True
    def find_usdt_symbol_for_asset(self, asset: str) -> Optional[str]:
        asset = asset.upper()
        if asset in ("USDT", "BNB"):
            return None
        symbol = f"{asset}USDT"
        info = self.get_exchange_info(symbol)
        if info and info.get("status") == "TRADING":
            return symbol
        return None
    def sell_large_spot_assets_to_usdt(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        sold_assets = []
        balances = self.spot_all_balances()
        for item in balances:
            asset = (item.get("asset") or "").upper()
            free_amt = float(item.get("free", 0) or 0)

            if asset in skip_assets or free_amt <= 0:
                continue

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                logger.info("未找到 %sUSDT 交易对，跳过 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    sold_assets.append(asset)
                    time.sleep(0.3)
            except Exception as e:
                logger.warning("卖出大额币 %s 失败: %s", asset, e)

        logger.info("大额币卖出为 USDT 完成，成功处理 %d 个币种", len(sold_assets))
        return sold_assets
    def convert_large_spot_assets_to_bnb(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        direct_converted: list[str] = []
        usdt_fallback_assets: list[str] = []
        residual_assets: list[str] = []
        balances = self.spot_all_balances()

        for item in balances:
            asset = str(item.get("asset") or "").strip().upper()
            free_amt = self._decimal_from_str(item.get("free", "0"), "0")

            if asset in skip_assets or free_amt <= 0:
                continue

            direct_supported = bool(self.get_convert_pair_info(asset, "BNB"))
            if direct_supported:
                try:
                    order = self.convert_with_from_amount(asset, "BNB", free_amt)
                    if order:
                        direct_converted.append(asset)
                        logger.info("大额资产已直接闪兑为 BNB：%s", asset)
                        time.sleep(0.3)
                        continue
                    logger.info("大额资产 %s 直兑 BNB 未执行，回退到 USDT 路线", asset)
                except Exception as e:
                    logger.warning("大额资产 %s 直兑 BNB 失败，回退到 USDT 路线: %s", asset, e)
            else:
                logger.info("闪兑不支持 %s -> BNB，回退到 USDT 路线", asset)

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                residual_assets.append(asset)
                logger.warning("未找到 %sUSDT 交易对，暂时无法处理 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    usdt_fallback_assets.append(asset)
                    time.sleep(0.3)
                else:
                    residual_assets.append(asset)
                    logger.warning("大额资产 %s 回退卖出 USDT 未执行，该资产仍未处理", asset)
            except Exception as e:
                residual_assets.append(asset)
                logger.warning("大额资产 %s 回退卖出 USDT 失败: %s", asset, e)

        logger.info(
            "大额资产处理完成：直接闪兑BNB=%d，回退卖USDT=%d，残留=%d",
            len(direct_converted),
            len(usdt_fallback_assets),
            len(residual_assets),
        )
        return {
            "direct_bnb": direct_converted,
            "fallback_usdt": usdt_fallback_assets,
            "residual_assets": residual_assets,
        }

    # -------- 现货买入（全部 USDT） --------
    def spot_buy_all_usdt(self, buffer=0.2, symbol="BTCUSDT"):
        symbol_u = self.normalize_trade_symbol(symbol)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        buffer_dec = Decimal(str(buffer or "0"))
        if quote_balance <= buffer_dec:
            logger.info(
                "现货 %s %s <= buffer %s，跳过买入",
                quote_asset,
                self._format_decimal(quote_balance),
                self._format_decimal(buffer_dec),
            )
            return False

        amount = (quote_balance - buffer_dec) * Decimal("0.999")
        if amount <= 0:
            logger.info("可用 %s 金额太小，跳过买入", quote_asset)
            return False

        return self.spot_buy_quote_amount(symbol_u, amount)

    # -------- 现货卖出（全部基础币） --------
    def spot_sell_all_base(self, symbol: str, *, small_qty_log_text: str | None = None):
        base = self.get_spot_base_asset(symbol)
        balance = self.spot_asset_balance_decimal(base)

        if balance <= 0:
            return False

        # 现货市价卖出按交易所 stepSize 取整，避免高价币因手填精度或额外预留被截到低于最小下单额。
        return self.sell_asset_market(
            symbol,
            balance,
            reserve_ratio=Decimal("1"),
            small_qty_log_text=small_qty_log_text,
        )

    # -------- 提现 --------
    def spot_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v3/asset/getUserAsset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- 资金账户正余额资产 --------
    def spot_asset_balance_decimal(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        data = self.request(self.spot, "GET", "/api/v3/account")
        for item in data.get("balances", []):
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            return self._decimal_from_str(item.get("free", "0"), "0")
        return Decimal("0")
    def get_spot_dust_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust-btc",
            {"accountType": "SPOT"},
        )

        assets = []
        for item in data.get("details", []):
            asset = item.get("asset")
            amount_free = float(item.get("amountFree", 0) or 0)
            if asset and amount_free > 0 and asset != "BNB":
                assets.append(asset)
        return assets

    # -------- 小额兑换：现货可兑换资产 -> BNB --------
    def convert_spot_dust_to_bnb(self):
        assets = self.get_spot_dust_assets()
        if not assets:
            logger.info("没有可进行小额兑换的现货资产")
            return []

        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust",
            {
                "asset": assets,
                "accountType": "SPOT",
            },
        )
        result = data.get("transferResult", [])
        logger.info("小额兑换完成，兑换资产数: %d", len(result))
        return result


__all__ = ["BinanceClientSpotMixin"]
