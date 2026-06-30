#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientConvertMixin(object):
    @staticmethod
    def _quantize_to_fraction(value: Decimal | str | float | int, fraction: int) -> Decimal:
        fraction_i = max(0, int(fraction or 0))
        step = Decimal("1").scaleb(-fraction_i)
        return Decimal(str(value)).quantize(step, rounding=ROUND_DOWN)
    def get_convert_pairs(self, from_asset: str | None = None, to_asset: str | None = None) -> list[dict[str, Any]]:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        cache_key = (from_asset_u, to_asset_u)
        params: dict[str, str] = {}
        if from_asset_u:
            params["fromAsset"] = from_asset_u
        if to_asset_u:
            params["toAsset"] = to_asset_u

        with self._shared_convert_cache_lock:
            if cache_key in self._shared_convert_exchange_info_cache:
                return list(self._shared_convert_exchange_info_cache[cache_key])
            data = self.public_get(self.spot, "/sapi/v1/convert/exchangeInfo", params)
            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])

            normalized: list[dict[str, Any]] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                normalized.append(
                    {
                        "fromAsset": str(item.get("fromAsset") or "").strip().upper(),
                        "toAsset": str(item.get("toAsset") or "").strip().upper(),
                        "fromAssetMinAmount": self._decimal_from_str(item.get("fromAssetMinAmount", "0"), "0"),
                        "fromAssetMaxAmount": self._decimal_from_str(item.get("fromAssetMaxAmount", "0"), "0"),
                        "toAssetMinAmount": self._decimal_from_str(item.get("toAssetMinAmount", "0"), "0"),
                        "toAssetMaxAmount": self._decimal_from_str(item.get("toAssetMaxAmount", "0"), "0"),
                    }
                )
            self._shared_convert_exchange_info_cache[cache_key] = list(normalized)
            return list(normalized)
    def get_convert_pair_info(self, from_asset: str, to_asset: str) -> dict[str, Any] | None:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        if not from_asset_u or not to_asset_u:
            return None
        for item in self.get_convert_pairs(from_asset_u, to_asset_u):
            if item.get("fromAsset") == from_asset_u and item.get("toAsset") == to_asset_u:
                return item
        return None
    def ensure_convert_symbol_supported(self, symbol: str) -> tuple[str, str]:
        _symbol_u, base_asset, quote_asset = self._validated_spot_symbol_parts(symbol)
        if not self.get_convert_pair_info(quote_asset, base_asset):
            raise RuntimeError(f"闪兑不支持 {quote_asset} -> {base_asset}")
        if not self.get_convert_pair_info(base_asset, quote_asset):
            raise RuntimeError(f"闪兑不支持 {base_asset} -> {quote_asset}")
        return base_asset, quote_asset
    def get_convert_asset_info(self) -> dict[str, int]:
        with self._shared_convert_cache_lock:
            cached = self._shared_convert_asset_info_cache
            if isinstance(cached, dict) and cached:
                return dict(cached)
            data = self.request(self.spot, "GET", "/sapi/v1/convert/assetInfo", {})
            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            result: dict[str, int] = {}
            for item in rows:
                if not isinstance(item, dict):
                    continue
                asset = str(item.get("asset") or "").strip().upper()
                if not asset:
                    continue
                try:
                    result[asset] = max(0, int(item.get("fraction", 8) or 8))
                except Exception:
                    result[asset] = 8
            self._shared_convert_asset_info_cache = dict(result)
            return dict(result)
    def get_convert_asset_fraction(self, asset: str) -> int:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return 8
        return int(self.get_convert_asset_info().get(asset_u, 8) or 8)
    def _normalize_convert_from_amount(self, from_asset: str, amount: Decimal | str | float | int) -> Decimal:
        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return Decimal("0")
        fraction = self.get_convert_asset_fraction(from_asset)
        return self._quantize_to_fraction(amount_dec, fraction)
    def _validate_convert_from_amount(
        self,
        from_asset: str,
        to_asset: str,
        amount: Decimal | str | float | int,
    ) -> tuple[Decimal, dict[str, Any]]:
        pair_info = self.get_convert_pair_info(from_asset, to_asset)
        if not pair_info:
            raise RuntimeError(f"闪兑不支持 {str(from_asset or '').upper()} -> {str(to_asset or '').upper()}")

        amount_dec = self._normalize_convert_from_amount(from_asset, amount)
        min_amount = Decimal(str(pair_info.get("fromAssetMinAmount", "0") or "0"))
        max_amount = Decimal(str(pair_info.get("fromAssetMaxAmount", "0") or "0"))

        if amount_dec <= 0:
            return Decimal("0"), pair_info
        if min_amount > 0 and amount_dec < min_amount:
            return Decimal("0"), pair_info
        if max_amount > 0 and amount_dec > max_amount:
            raise RuntimeError(
                f"闪兑 {str(from_asset or '').upper()} -> {str(to_asset or '').upper()} 金额过大："
                f"{self._format_decimal(amount_dec)} > 最大 {self._format_decimal(max_amount)}"
            )
        return amount_dec, pair_info
    def get_convert_quote(
        self,
        from_asset: str,
        to_asset: str,
        *,
        from_amount: Decimal | str | float | int | None = None,
        to_amount: Decimal | str | float | int | None = None,
        wallet_type: str = "SPOT",
        valid_time: str = "10s",
    ) -> dict[str, Any]:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        params: dict[str, Any] = {
            "fromAsset": from_asset_u,
            "toAsset": to_asset_u,
            "walletType": str(wallet_type or "SPOT").strip().upper(),
            "validTime": str(valid_time or "10s").strip(),
        }
        if from_amount is not None:
            params["fromAmount"] = self._format_decimal(Decimal(str(from_amount)))
        elif to_amount is not None:
            params["toAmount"] = self._format_decimal(Decimal(str(to_amount)))
        else:
            raise RuntimeError("闪兑报价必须指定 fromAmount 或 toAmount")
        return self.request(self.spot, "POST", "/sapi/v1/convert/getQuote", params)
    def accept_convert_quote(self, quote_id: str) -> dict[str, Any]:
        quote_id_text = str(quote_id or "").strip()
        if not quote_id_text:
            raise RuntimeError("闪兑 quoteId 为空")
        return self.request(
            self.spot,
            "POST",
            "/sapi/v1/convert/acceptQuote",
            {"quoteId": quote_id_text},
        )
    def get_convert_order_status(self, *, order_id: str | int | None = None, quote_id: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if order_id is not None and str(order_id).strip():
            params["orderId"] = str(order_id).strip()
        if quote_id is not None and str(quote_id).strip():
            params["quoteId"] = str(quote_id).strip()
        if not params:
            raise RuntimeError("闪兑订单状态查询必须提供 orderId 或 quoteId")
        return self.request(self.spot, "GET", "/sapi/v1/convert/orderStatus", params)
    def wait_convert_order_success(
        self,
        *,
        order_id: str | int | None = None,
        quote_id: str | None = None,
        timeout_sec: float = 15.0,
        poll_interval: float = 0.4,
    ) -> dict[str, Any]:
        deadline = time.time() + max(1.0, float(timeout_sec))
        last_status = ""
        while time.time() < deadline:
            data = self.get_convert_order_status(order_id=order_id, quote_id=quote_id)
            status = str(data.get("orderStatus") or "").strip().upper()
            if status == "SUCCESS":
                return data
            if status in {"FAIL", "FAILED", "EXPIRED", "REJECTED", "CANCELED", "CANCELLED"}:
                raise RuntimeError(f"闪兑订单失败，状态={status}")
            last_status = status
            time.sleep(max(0.2, float(poll_interval)))
        if last_status:
            raise RuntimeError(f"闪兑订单等待超时，当前状态={last_status}")
        raise RuntimeError("闪兑订单等待超时")
    def convert_with_from_amount(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal | str | float | int,
        *,
        wallet_type: str = "SPOT",
        valid_time: str = "10s",
    ) -> dict[str, Any] | None:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        amount_dec, pair_info = self._validate_convert_from_amount(from_asset_u, to_asset_u, from_amount)
        if amount_dec <= 0:
            min_amount = Decimal(str(pair_info.get("fromAssetMinAmount", "0") or "0"))
            logger.info(
                "闪兑 %s -> %s 金额过小，跳过：%s < 最小 %s",
                from_asset_u,
                to_asset_u,
                self._format_decimal(self._normalize_convert_from_amount(from_asset_u, from_amount)),
                self._format_decimal(min_amount),
            )
            return None

        quote = self.get_convert_quote(
            from_asset_u,
            to_asset_u,
            from_amount=amount_dec,
            wallet_type=wallet_type,
            valid_time=valid_time,
        )
        quote_id = str(quote.get("quoteId") or "").strip()
        if not quote_id:
            raise RuntimeError(f"闪兑报价返回异常：{quote}")

        self.accept_convert_quote(quote_id)
        order = self.wait_convert_order_success(quote_id=quote_id)
        logger.info(
            "闪兑完成 %s -> %s：支出=%s 到账=%s",
            from_asset_u,
            to_asset_u,
            self._format_decimal(self._decimal_from_str(order.get("fromAmount", amount_dec), "0")),
            self._format_decimal(self._decimal_from_str(order.get("toAmount", quote.get("toAmount", "0")), "0")),
        )
        return order
    def convert_quote_to_base_all(self, symbol: str, buffer=0.2) -> dict[str, Any] | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        base_asset = self.get_spot_base_asset(symbol_u)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.ensure_convert_symbol_supported(symbol_u)
        self.collect_asset_to_spot(quote_asset, context="交易前")

        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        buffer_dec = Decimal(str(buffer or "0"))
        amount = quote_balance - buffer_dec
        if amount <= 0:
            logger.info(
                "闪兑买入跳过：现货 %s 余额 %s <= 预留 %s",
                quote_asset,
                self._format_decimal(quote_balance),
                self._format_decimal(buffer_dec),
            )
            return None
        return self.convert_with_from_amount(quote_asset, base_asset, amount)
    def convert_base_to_quote_all(self, symbol: str) -> dict[str, Any] | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        base_asset = self.get_spot_base_asset(symbol_u)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.ensure_convert_symbol_supported(symbol_u)
        self.collect_asset_to_spot(base_asset, context="交易前")

        base_balance = self.spot_asset_balance_decimal(base_asset)
        if base_balance <= 0:
            logger.info("闪兑卖出跳过：现货 %s 余额为 0", base_asset)
            return None
        return self.convert_with_from_amount(base_asset, quote_asset, base_balance)


__all__ = ["BinanceClientConvertMixin"]
