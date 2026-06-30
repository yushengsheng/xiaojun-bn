#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *


class BinanceClientFundingMixin(object):
    def withdraw_all_coin(
        self,
        coin: str,
        address: str,
        network: str,
        fee_buffer: float = WITHDRAW_FEE_BUFFER_DEFAULT,
        enable_withdraw: bool = True,
        auto_collect_to_spot: bool = False,
    ) -> float:
        coin = str(coin or "").strip().upper()
        if auto_collect_to_spot and coin:
            try:
                moved_count = self.collect_asset_to_spot(coin, context="提现前")
                if moved_count > 0:
                    # Give Binance a brief moment to reflect the transfer in spot balance.
                    time.sleep(0.5)
            except Exception as e:
                logger.warning("提现前归集 %s 到现货失败: %s", coin, e)
        balance = self.spot_asset_balance_decimal(coin)
        fee_buffer_dec = Decimal(str(fee_buffer or "0"))
        amount = balance - fee_buffer_dec
        if amount <= 0:
            logger.info(
                "%s 余额 %s 不足以提现（需 > buffer %s）",
                coin,
                self._format_decimal(balance),
                self._format_decimal(fee_buffer_dec),
            )
            return 0.0

        params = {
            "coin": coin,
            "address": address,
            "network": network,
            "amount": self._format_decimal(amount),
        }

        if enable_withdraw:
            self.request(
                self.spot,
                "POST",
                "/sapi/v1/capital/withdraw/apply",
                params,
            )
            logger.info(
                "已提交提现 %s %s → %s (%s)",
                self._format_decimal(amount),
                coin,
                address,
                network,
            )
            return float(amount)
        else:
            logger.info("提现开关关闭，仅打印参数: %s", params)
            return 0.0

    # -------- 现货正余额资产 --------
    def funding_positive_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
            **request_kwargs,
        )
        result = []
        for item in data:
            free_amt = self._decimal_from_str(item.get("free", "0"), "0")
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- U本位合约可转出余额 --------
    def funding_asset_balance(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
        )
        total = Decimal("0")
        for item in data:
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            total += self._decimal_from_str(item.get("free", "0"), "0")
        return total
    def collect_funding_asset_to_spot(
        self,
        asset: str,
        amount: Decimal | str | float | int | None = None,
    ) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        try:
            available_amount = self.funding_asset_balance(asset_u)
        except Exception as e:
            logger.warning("查询资金账户 %s 余额失败: %s", asset_u, e)
            return Decimal("0")
        if available_amount <= 0:
            return Decimal("0")
        transfer_amount = available_amount
        if amount is not None:
            requested_amount = self._decimal_from_str(amount, "0")
            if requested_amount <= 0:
                return Decimal("0")
            transfer_amount = min(available_amount, requested_amount)
        if transfer_amount <= 0:
            return Decimal("0")
        try:
            self._universal_transfer_with_retry("FUNDING_MAIN", asset_u, transfer_amount, context="资金账户划转到现货")
            logger.info("检测到资金账户 %s 余额，已自动划转到现货：%s", asset_u, self._format_decimal(transfer_amount))
            time.sleep(0.5)
            return transfer_amount
        except Exception as e:
            logger.warning("资金账户划转到现货失败 %s %s: %s", asset_u, self._format_decimal(transfer_amount), e)
            return Decimal("0")
    def ensure_bnb_fee_ready_in_spot(
        self,
        *,
        min_spot_balance: Decimal | str | float | int | None = None,
        max_transfer_amount: Decimal | str | float | int | None = None,
        timeout_seconds: float = 6.0,
    ) -> Decimal:
        asset_u = "BNB"
        deadline = time.monotonic() + max(0.5, float(timeout_seconds or 0))
        required_spot = self._decimal_from_str(min_spot_balance, "0") if min_spot_balance is not None else Decimal("0")
        if required_spot < 0:
            required_spot = Decimal("0")
        remaining_transfer_cap = (
            self._decimal_from_str(max_transfer_amount, "0") if max_transfer_amount is not None else None
        )
        if remaining_transfer_cap is not None and remaining_transfer_cap < 0:
            remaining_transfer_cap = Decimal("0")
        last_spot = Decimal("0")
        last_funding = Decimal("0")

        while True:
            last_spot = self.spot_asset_balance_decimal(asset_u)
            last_funding = self.funding_asset_balance(asset_u)
            if required_spot > 0 and last_spot >= required_spot:
                return last_spot
            if required_spot <= 0 and last_funding <= 0:
                return last_spot
            if last_funding <= 0:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "预买BNB后现货 BNB 余额仍不足："
                        f"现货={self._format_decimal(last_spot)}，"
                        f"目标={self._format_decimal(required_spot)}"
                    )
                time.sleep(0.4)
                continue

            transfer_amount = last_funding
            if required_spot > 0:
                missing_amount = required_spot - last_spot
                if missing_amount > 0:
                    transfer_amount = min(transfer_amount, missing_amount)
            if remaining_transfer_cap is not None:
                if remaining_transfer_cap <= 0:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            "预买BNB后检测到 BNB 仍停留在资金账户，但本次可划转额度已用尽："
                            f"现货={self._format_decimal(last_spot)}，"
                            f"资金={self._format_decimal(last_funding)}，"
                            f"目标={self._format_decimal(required_spot)}"
                        )
                    time.sleep(0.4)
                    continue
                transfer_amount = min(transfer_amount, remaining_transfer_cap)
            if transfer_amount <= 0:
                return last_spot

            moved_amount = self.collect_funding_asset_to_spot(asset_u, amount=transfer_amount)
            if remaining_transfer_cap is not None and moved_amount > 0:
                remaining_transfer_cap = max(Decimal("0"), remaining_transfer_cap - moved_amount)
            last_spot = self.spot_asset_balance_decimal(asset_u)
            last_funding = self.funding_asset_balance(asset_u)
            if required_spot > 0 and last_spot >= required_spot:
                logger.info(
                    "预买BNB后已确认手续费BNB位于现货账户：现货=%s，资金=%s，目标=%s",
                    self._format_decimal(last_spot),
                    self._format_decimal(last_funding),
                    self._format_decimal(required_spot),
                )
                return last_spot
            if required_spot <= 0 and last_funding <= 0:
                logger.info(
                    "预买BNB后已确认手续费BNB位于现货账户：现货=%s，资金=%s",
                    self._format_decimal(last_spot),
                    self._format_decimal(last_funding),
                )
                return last_spot

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "预买BNB后检测到 BNB 仍停留在资金账户，自动划转到现货失败："
                    f"现货={self._format_decimal(last_spot)}，"
                    f"资金={self._format_decimal(last_funding)}，"
                    f"目标={self._format_decimal(required_spot)}，"
                    f"本次划转={self._format_decimal(moved_amount)}"
                )
            time.sleep(0.4)
    def transfer_spot_asset_to_um_futures(
        self,
        asset: str,
        amount: Decimal | str | float | int | None = None,
    ) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")

        # Reuse the existing funding->spot helper so futures mode can consume资金账户中的保证金币种。
        self.collect_funding_asset_to_spot(asset_u)

        try:
            spot_balance = self.spot_asset_balance_decimal(asset_u)
        except Exception as e:
            logger.warning("查询现货 %s 余额失败: %s", asset_u, e)
            return Decimal("0")
        if spot_balance <= 0:
            return Decimal("0")

        # 用户要求：现货中该保证金币种一旦存在，直接整笔划转到 U 本位，
        # 避免按差额补划时产生极小精度尾差，导致交易所拒绝划转。
        transfer_amount = Decimal(str(spot_balance))
        if transfer_amount <= 0:
            return Decimal("0")

        try:
            self._universal_transfer_with_retry("MAIN_UMFUTURE", asset_u, transfer_amount, context="现货划转到 U本位")
            logger.info("检测到现货 %s 余额，已自动划转到 U本位：%s", asset_u, self._format_decimal(transfer_amount))
            time.sleep(0.5)
            return transfer_amount
        except Exception as e:
            logger.warning("现货划转到 U本位失败 %s %s: %s", asset_u, self._format_decimal(transfer_amount), e)
            return Decimal("0")
    def um_futures_transferable_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.um_futures,
            "GET",
            "/fapi/v2/balance",
            {},
            **request_kwargs,
        )
        result = []
        for item in data:
            amt = self._decimal_from_str(item.get("maxWithdrawAmount", "0"), "0")
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 币本位合约可转出余额 --------
    def cm_futures_transferable_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.cm_futures,
            "GET",
            "/dapi/v1/balance",
            {},
            **request_kwargs,
        )
        result = []
        for item in data:
            amt = self._decimal_from_str(item.get("withdrawAvailable", "0"), "0")
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 通用划转 --------
    def universal_transfer(self, transfer_type: str, asset: str, amount: Decimal | str | float | int):
        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return False

        amt_str = format(amount_dec.normalize(), "f")
        if amt_str == "0":
            return False

        self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/transfer",
            {
                "type": transfer_type,
                "asset": asset,
                "amount": amt_str,
            },
        )
        logger.info("划转成功: %s %s %s", transfer_type, asset, amt_str)
        return True
    def _universal_transfer_with_retry(
        self,
        transfer_type: str,
        asset: str,
        amount: Decimal | str | float | int,
        *,
        context: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        retries = max(0, int(max_retries or 0))
        for attempt in range(retries + 1):
            try:
                return self.universal_transfer(transfer_type, asset, amount)
            except Exception as e:
                if attempt >= retries:
                    raise
                logger.warning(
                    "%s失败 %s %s（重试 %d/%d）: %s",
                    context,
                    str(asset or "").strip().upper(),
                    self._format_decimal(Decimal(str(amount))),
                    attempt + 1,
                    retries,
                    e,
                )
                time.sleep(max(0.0, float(retry_delay or 0)))
        return False
    def collect_asset_to_spot(self, asset: str, *, context: str = "提现前") -> int:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return 0
        context_text = str(context or "提现前").strip() or "提现前"

        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("UMFUTURE_MAIN", asset_u, amount, context=f"{context_text} U本位划转")
                    total_count += 1
                except Exception as e:
                    logger.warning(
                        "%s U本位划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            logger.warning("%s查询 U本位 %s 余额失败: %s", context_text, asset_u, e)

        try:
            items = self.cm_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("CMFUTURE_MAIN", asset_u, amount, context=f"{context_text} 币本位划转")
                    total_count += 1
                except Exception as e:
                    logger.warning(
                        "%s 币本位划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            logger.warning("%s查询 币本位 %s 余额失败: %s", context_text, asset_u, e)

        try:
            items = self.funding_positive_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("free", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("FUNDING_MAIN", asset_u, amount, context=f"{context_text} 资金账户划转")
                    total_count += 1
                except Exception as e:
                    logger.warning(
                        "%s 资金账户划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            logger.warning("%s查询 资金账户 %s 余额失败: %s", context_text, asset_u, e)

        if total_count > 0:
            logger.info("%s归集 %s 到现货完成，共处理 %d 项", context_text, asset_u, total_count)
        return total_count

    # -------- 归集合约/资金到现货 --------
    def collect_all_to_spot(self):
        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            if items:
                logger.info("检测到 U本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("U本位划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            logger.warning("查询 U本位合约余额失败: %s", e)

        try:
            items = self.cm_futures_transferable_assets()
            if items:
                logger.info("检测到 币本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("币本位划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            logger.warning("查询 币本位合约余额失败: %s", e)

        try:
            items = self.funding_positive_assets()
            if items:
                logger.info("检测到 资金账户可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("free", "0"), "0")
                try:
                    self.universal_transfer("FUNDING_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("资金账户划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            logger.warning("查询资金账户余额失败: %s", e)

        logger.info("归集到现货完成，共处理 %d 项资产", total_count)
        return total_count

    # -------- 查询现货可小额兑换资产 --------


__all__ = ["BinanceClientFundingMixin"]
