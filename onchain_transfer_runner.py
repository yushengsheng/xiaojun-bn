#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from onchain_relay_runner import run_relay_batch, run_relay_fee_recovery
from operation_recovery import build_onchain_recovery_plan
from page_onchain_base import (
    Decimal,
    InvalidOperation,
    WithdrawRuntimeParams,
    messagebox,
    queue,
    random_decimal_between,
    set_ui_batch_size,
    threading,
    time,
)


class OnchainTransferRunnerMixin(object):
    def _transfer_stop_requested(self) -> bool:
        stop_event = getattr(self, "stop_requested", None)
        return bool(stop_event is not None and stop_event.is_set())

    def _raise_if_transfer_stopped(self) -> None:
        if self._transfer_stop_requested():
            raise RuntimeError("任务已停止")

    def _prepare_onchain_recovery_batch(
        self,
        params: WithdrawRuntimeParams,
        jobs_data: list[tuple[str, str, str]],
    ):
        """Create or resume one immutable direct-transfer batch.

        Only public source addresses and task fingerprints are persisted. Source
        credentials are used transiently to derive the address and never enter the
        recovery ledger.
        """
        journal = getattr(self, "operation_journal", None)
        if journal is None:
            return "", None, {}, False
        resolved_jobs: list[tuple[str, str, str, str]] = []
        source_address_by_credential: dict[str, str] = {}
        for row_key, source, target in jobs_data:
            self._raise_if_transfer_stopped()
            source_key = str(source or "").strip()
            source_address = source_address_by_credential.get(source_key)
            if not source_address:
                _private_key, source_address = self._resolve_wallet(source)
                source_address_by_credential[source_key] = source_address
            target_address = self._validate_recipient_address(target, "接收地址")
            resolved_jobs.append((row_key, source, target_address, source_address))
        plan = build_onchain_recovery_plan(
            network=params.network,
            mode=self._mode(),
            coin=params.coin,
            amount=params.amount,
            random_enabled=params.random_enabled,
            random_min=self._decimal_to_text(params.random_min) if params.random_min is not None else "",
            random_max=self._decimal_to_text(params.random_max) if params.random_max is not None else "",
            token_contract=params.token_contract,
            token_decimals=params.token_decimals,
            token_is_native=params.token_is_native,
            jobs=resolved_jobs,
        )
        batch_id, resumed = journal.resume_or_begin_batch(
            "onchain-transfer-batch",
            str(params.network or "").upper(),
            fingerprint=plan.fingerprint,
            task_keys=[task.task_key for task in plan.tasks],
        )
        states = journal.batch_task_states(batch_id, kind="onchain-transfer")
        completed = {
            task_key: entry
            for task_key, entry in states.items()
            if str(entry.get("state") or "") in {"completed", "reconciled"}
        }
        return batch_id, plan, completed, resumed

    def _reconcile_pending_onchain_operations(self, network: str, jobs_data: list[tuple[str, str, str]]) -> bool:
        journal = getattr(self, "operation_journal", None)
        if journal is None:
            return True
        source_addresses: set[str] = set()
        source_address_by_credential: dict[str, str] = {}
        for _row_key, source, _target in jobs_data:
            self._raise_if_transfer_stopped()
            try:
                source_key = str(source or "").strip()
                source_address = source_address_by_credential.get(source_key)
                if not source_address:
                    _private_key, source_address = self._resolve_wallet(source)
                    source_address_by_credential[source_key] = source_address
                source_addresses.add(source_address.lower())
            except Exception:
                continue
        pending: list[str] = []
        for entry in journal.active_entries(kind="onchain-transfer"):
            self._raise_if_transfer_stopped()
            payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
            if str(payload.get("network") or "").upper() != str(network or "").upper():
                continue
            source_addr = str(payload.get("source_address") or "").lower()
            if source_addresses and source_addr not in source_addresses:
                continue
            entry_id = str(entry.get("id") or "")
            txid = str(payload.get("txid") or "").strip()
            try:
                try:
                    receipt = self.client.get_transaction_receipt(
                        network,
                        txid,
                        read_timeout=3.0,
                        max_attempts=1,
                    ) if txid else None
                except TypeError:
                    receipt = self.client.get_transaction_receipt(network, txid) if txid else None
            except Exception:
                receipt = None
            if receipt is not None:
                status_value = receipt.get("status")
                if status_value is not None and self.client._int_from_hex(status_value) != 1:
                    journal.update(entry_id, "failed", reconciliation="receipt-failed")
                else:
                    journal.update(entry_id, "completed", reconciliation="receipt-success")
                continue
            try:
                nonce = int(payload.get("nonce"))
                pending_nonce = self.client.get_nonce(network, source_addr)
            except Exception:
                pending.append(entry_id)
                continue
            if pending_nonce > nonce:
                journal.update(entry_id, "submitted_unknown", reconciliation="nonce-advanced")
            pending.append(entry_id)
        if pending:
            message = f"检测到 {len(pending)} 笔未完成链上交易，请先等待确认或核对 txid 后再重试，已阻止重复转账。"
            self.log(message)
            self._dispatch_ui(lambda text=message: messagebox.showwarning("待恢复交易", text))
            return False
        return True

    @classmethod
    def _random_decimal_between(cls, low: Decimal, high: Decimal, token_decimals: int) -> Decimal:
        return random_decimal_between(low, high, cls._random_amount_unit(token_decimals))
    def _validate_transfer_params(self) -> WithdrawRuntimeParams | None:
        network = self.network_var.get().strip().upper()
        amount_mode = self._amount_mode()
        amount_raw = self.amount_var.get().strip()
        random_enabled = amount_mode == self.AMOUNT_MODE_RANDOM

        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "未设置网络，无法转账")
            return None

        token = self._selected_token(with_message=True)
        if not token:
            return None
        if (not token.is_native) and (not self.client.is_address(token.contract)):
            messagebox.showerror("参数错误", "代币合约地址格式错误")
            return None
        decimals = int(token.decimals)
        if decimals < 0 or decimals > self.MAX_TOKEN_DECIMALS:
            messagebox.showerror("参数错误", f"代币精度无效：{decimals}")
            return None
        coin = token.symbol.strip().upper()
        if not coin:
            messagebox.showerror("参数错误", "币种无效")
            return None

        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        else:
            if amount_mode == self.AMOUNT_MODE_FIXED and not amount_raw:
                messagebox.showerror("参数错误", "转账数量不能为空")
                return None
            try:
                if amount_mode == self.AMOUNT_MODE_FIXED:
                    v = Decimal(amount_raw)
                    if v <= 0:
                        raise InvalidOperation
                    amount = self._decimal_to_text(v)
                else:
                    amount = amount_raw or "0"
            except Exception:
                messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
                return None

        random_min: Decimal | None = None
        random_max: Decimal | None = None
        if random_enabled:
            min_raw = self.random_min_var.get().strip()
            max_raw = self.random_max_var.get().strip()
            try:
                random_min = Decimal(min_raw)
                random_max = Decimal(max_raw)
            except Exception:
                messagebox.showerror("参数错误", "随机金额最小值/最大值格式错误")
                return None
            if random_min <= 0 or random_max <= 0:
                messagebox.showerror("参数错误", "随机金额最小值和最大值必须大于 0")
                return None
            if random_max < random_min:
                messagebox.showerror("参数错误", "随机金额最大值必须大于或等于最小值")
                return None
            try:
                random_decimal_between(random_min, random_max, self._random_amount_unit(decimals))
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return None

        try:
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return None
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
        except Exception:
            messagebox.showerror("参数错误", "执行线程数格式错误")
            return None
        try:
            confirm_timeout_seconds = float(str(self.confirm_timeout_var.get()).strip())
        except Exception:
            messagebox.showerror("参数错误", "确认超时格式错误")
            return None
        if confirm_timeout_seconds <= 0:
            messagebox.showerror("参数错误", "确认超时必须大于 0")
            return None
        try:
            confirm_poll_interval_seconds = self._parse_confirm_poll_interval_seconds(
                self.confirm_poll_interval_var.get()
            )
        except ValueError as exc:
            messagebox.showerror("参数错误", str(exc))
            return None

        if hasattr(self, "mode_var") and self._is_mode_m1():
            target = self.target_address_var.get().strip()
            if not target:
                messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return None
            try:
                safe_target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return None
            self.target_address_var.set(safe_target)

        relay_enabled = False
        relay_fee_reserve: Decimal | None = None
        if hasattr(self, "mode_var") and (self._is_mode_1m() or self._is_mode_m1()) and getattr(self, "_relay_enabled", None):
            relay_enabled = bool(self._relay_enabled())
        if relay_enabled:
            if amount_mode == self.AMOUNT_MODE_ALL and not self._is_mode_m1():
                messagebox.showerror("参数错误", "启用中转时仅支持固定数量或随机数量，暂不支持“全部”")
                return None
            reserve_raw = self.relay_fee_reserve_var.get().strip()
            if not reserve_raw:
                messagebox.showerror("参数错误", "启用中转后必须填写预留原生币手续费")
                return None
            try:
                relay_fee_reserve = Decimal(reserve_raw)
            except Exception:
                messagebox.showerror("参数错误", "预留原生币手续费格式错误")
                return None
            if relay_fee_reserve < 0:
                messagebox.showerror("参数错误", "预留原生币手续费不能小于 0")
                return None
            if relay_fee_reserve == 0 and not self._is_mode_m1():
                messagebox.showerror("参数错误", "预留原生币手续费必须大于 0")
                return None

        return WithdrawRuntimeParams(
            coin=coin,
            amount=amount,
            network=network,
            delay=delay,
            threads=threads,
            confirm_timeout_seconds=confirm_timeout_seconds,
            confirm_poll_interval_seconds=confirm_poll_interval_seconds,
            random_enabled=random_enabled,
            random_min=random_min,
            random_max=random_max,
            token_contract=token.contract if not token.is_native else "",
            token_decimals=decimals,
            token_is_native=bool(token.is_native),
            relay_enabled=relay_enabled,
            relay_fee_reserve=relay_fee_reserve,
            relay_sweep_enabled=False,
            relay_sweep_target="",
        )
    def _resolve_wallet(self, source: str) -> tuple[str, str]:
        source_key = source.strip()
        if not source_key:
            raise RuntimeError("转出钱包私钥/助记词不能为空")
        if self.client.is_address(source_key):
            raise RuntimeError("当前数据仅包含钱包地址，无法签名转账。真实转账请提供私钥或助记词。")
        with self.wallet_cache_lock:
            cached_pk = self.source_private_key_cache.get(source_key)
            cached_addr = self.source_address_cache.get(source_key)
            if cached_pk and cached_addr:
                return cached_pk, cached_addr
        private_key = self.client.credential_to_private_key(source_key)
        address = self.client.address_from_private_key(private_key)
        with self.wallet_cache_lock:
            self.source_private_key_cache[source_key] = private_key
            self.source_address_cache[source_key] = address
        return private_key, address
    def _relay_confirm_recovery_text(self) -> str:
        if hasattr(self, "mode_var") and self._is_mode_m1():
            return "手续费回收：主流程结束后手动触发，默认回收到最终目标地址"
        return "手续费回收：主流程结束后手动触发，默认回收到对应源钱包"
    def _relay_confirm_reserve_label_text(self) -> str:
        if hasattr(self, "mode_var") and self._is_mode_m1():
            return "源钱包手续费预留"
        return "中转手续费预留"
    def _resolve_source_address(self, source: str) -> str:
        source_key = source.strip()
        if not source_key:
            raise RuntimeError("转出钱包不能为空")
        with self.wallet_cache_lock:
            cached_addr = self.source_address_cache.get(source_key)
            if cached_addr:
                return cached_addr
        if self.client.is_address(source_key):
            address = self.client.validate_evm_address(source_key, "转出钱包地址")
            with self.wallet_cache_lock:
                self.source_address_cache[source_key] = address
            return address
        _private_key, address = self._resolve_wallet(source_key)
        return address
    def _ensure_signing_sources(self, sources: list[str]) -> None:
        address_only = [s.strip() for s in sources if self.client.is_address(s.strip())]
        if not address_only:
            return
        count = len(address_only)
        sample = address_only[0]
        raise RuntimeError(
            f"当前选择的数据里有 {count} 条仅包含钱包地址，无法真实转账。请把左列改为私钥或助记词后再执行。示例：{sample}"
        )
    def start_transfer_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            if self._warn_if_draft_selected():
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            row_key, source, target = job
            mode = self._mode()
            if mode == self.MODE_1M and not source:
                messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return
            if mode in {self.MODE_M2M, self.MODE_1M}:
                try:
                    target = self._validate_recipient_address(target, "接收地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            if mode == self.MODE_M1:
                if not target:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                    return
                try:
                    target = self._validate_recipient_address(target, "收款地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
                self.target_address_var.set(target)

            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
                try:
                    self._ensure_signing_sources([source])
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = self._random_amount_range_text(params.random_min, params.random_max)
                relay_text = ""
                if params.relay_enabled:
                    relay_text = (
                        f"中转：已启用\n"
                        f"{self._relay_confirm_reserve_label_text()}：{self._decimal_to_text(params.relay_fee_reserve or Decimal('0'))} {self._network_fee_symbol(params.network)}\n"
                        f"{self._relay_confirm_recovery_text()}\n"
                        f"中转钱包文件：{self.relay_wallet_store.file_path.name}\n"
                    )
                text = (
                    f"即将执行链上当前行真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：1\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n"
                    f"确认超时：{params.confirm_timeout_seconds:g} 秒\n"
                    f"确认轮询：{params.confirm_poll_interval_seconds:g} 秒\n"
                    f"{relay_text}\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了当前行真实链上转账")
                    return

            row_prefix = self._row_log_prefix(row_key, 1, 1, self._row_number_by_key_snapshot())
            self.log(f"{row_prefix} 已启动当前行转账任务：mode={self._mode()}，币种={token_desc}")
            self.stop_requested.clear()
            self.is_running = True
            self._start_managed_thread(
                self._run_batch_transfer,
                args=([(row_key, source, target)], params, dry_run),
                name="onchain-transfer-current-row",
            )
        except Exception as exc:
            self.log(f"当前行转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def _resolve_amount_and_gas(self, params: WithdrawRuntimeParams, source_addr: str, target_addr: str) -> tuple[int, int, int, str]:
        target_addr = self._validate_recipient_address(target_addr, "接收地址")
        gas_price = self.client.get_gas_price_wei(params.network)

        if params.token_is_native:
            gas_limit = self.client.NATIVE_GAS_LIMIT
            gas_cost = gas_price * gas_limit

            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_dec = self._random_decimal_between(params.random_min, params.random_max, params.token_decimals)
                if amount_dec <= 0:
                    raise RuntimeError("随机金额生成失败：结果必须大于 0")
                value_units = self._amount_to_units(amount_dec, params.token_decimals)
                amount_text = self._decimal_to_text(amount_dec)
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                if balance_units < value_units + gas_cost:
                    raise RuntimeError("余额不足（随机金额 + gas）")
                return value_units, gas_price, gas_limit, amount_text

            if params.amount == self.AMOUNT_ALL_LABEL:
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                value_units = balance_units - gas_cost
                if value_units <= 0:
                    raise RuntimeError("余额不足以覆盖 gas")
                amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
                return value_units, gas_price, gas_limit, amount_text

            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            if value_units <= 0:
                raise RuntimeError("转账数量过小")
            balance_units = self.client.get_balance_wei(params.network, source_addr)
            if balance_units < value_units + gas_cost:
                raise RuntimeError("余额不足（固定金额 + gas）")
            return value_units, gas_price, gas_limit, self._decimal_to_text(amount_dec)

        token_contract = params.token_contract.strip()
        if not token_contract:
            raise RuntimeError("代币合约未设置")
        token_balance_units = self.client.get_erc20_balance(params.network, token_contract, source_addr)
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_dec = self._random_decimal_between(params.random_min, params.random_max, params.token_decimals)
            if amount_dec <= 0:
                raise RuntimeError("随机金额生成失败：结果必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = self._decimal_to_text(amount_dec)
        elif params.amount == self.AMOUNT_ALL_LABEL:
            value_units = token_balance_units
            amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
        else:
            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = self._decimal_to_text(amount_dec)
        if value_units <= 0:
            raise RuntimeError("代币余额为 0 或转账数量过小")
        if token_balance_units < value_units:
            raise RuntimeError("代币余额不足")

        gas_limit = self.client.estimate_erc20_transfer_gas(
            params.network,
            source_addr,
            token_contract,
            target_addr,
            value_units,
        )
        gas_cost = gas_price * gas_limit
        native_balance_units = self.client.get_balance_wei(params.network, source_addr)
        if native_balance_units < gas_cost:
            raise RuntimeError("原生币余额不足，无法支付 gas")
        return value_units, gas_price, gas_limit, amount_text
    def _launch_retry_jobs(
        self,
        jobs: list[tuple[str, str, str]],
        params: WithdrawRuntimeParams,
        dry_run: bool,
        *,
        confirm: bool,
    ) -> bool:
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return False
        if not jobs:
            return False
        if not dry_run:
            try:
                self.client._ensure_eth_account()
            except Exception as exc:
                messagebox.showerror("依赖缺失", str(exc))
                return False
            try:
                self._ensure_signing_sources([source for _row_key, source, _target in jobs])
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return False
        if params.amount == self.AMOUNT_ALL_LABEL:
            source_counter: dict[str, int] = {}
            for _k, source, _target in jobs:
                source_counter[source] = source_counter.get(source, 0) + 1
            duplicated = [s for s, c in source_counter.items() if c > 1]
            if duplicated:
                messagebox.showerror("参数错误", "重试模式下，数量为“全部”时同一个转出钱包只能处理 1 条失败任务")
                return False
        if confirm and not dry_run:
            token_desc = self._token_desc_from_params(params)
            amount_text = params.amount
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_text = self._random_amount_range_text(params.random_min, params.random_max)
            text = (
                f"即将重试链上失败转账：\n"
                f"模式：{self._mode()}\n"
                f"失败任务数：{len(jobs)}\n"
                f"网络：{params.network}\n"
                f"币种：{token_desc}\n"
                f"数量：{amount_text}\n"
                f"执行间隔：{params.delay} 秒\n"
                f"执行线程数：{params.threads}\n\n"
                f"确认超时：{params.confirm_timeout_seconds:g} 秒\n\n"
                f"确认轮询：{params.confirm_poll_interval_seconds:g} 秒\n\n"
                "确认继续？"
            )
            if not messagebox.askyesno("重试确认", text):
                self.log("用户取消了失败重试")
                return False
        self.log(f"开始失败重试：{len(jobs)}")
        self.stop_requested.clear()
        self.is_running = True
        self._start_managed_thread(
            self._run_batch_transfer,
            args=(jobs, params, dry_run),
            name="onchain-transfer-retry-failed",
        )
        return True
    def _launch_relay_fee_recovery(
        self,
        *,
        dry_run: bool,
        batch_ids: set[str] | None = None,
        confirm: bool,
        show_dialog: bool = True,
        run_label: str = "manual",
    ) -> bool:
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return False
        records = self.relay_wallet_store.load_records()
        batch_filter = {str(value or "").strip() for value in (batch_ids or set()) if str(value or "").strip()}
        if batch_filter:
            records = [record for record in records if str(getattr(record, "batch_id", "") or "").strip() in batch_filter]
        if not records:
            messagebox.showinfo("提示", "中转钱包.txt 中没有可处理记录")
            return False
        if not dry_run:
            try:
                self.client._ensure_eth_account()
            except Exception as exc:
                messagebox.showerror("依赖缺失", str(exc))
                return False
        worker_threads = max(1, int(self._runtime_worker_threads()))
        confirm_poll_interval = self._runtime_confirmation_poll_interval_seconds()
        if confirm:
            scope_text = f"本次批量 {len(batch_filter)} 个批次" if batch_filter else "全部历史记录"
            text = (
                f"即将扫描 {scope_text} 中的 {len(records)} 条中转记录，\n"
                "检测并回收中转钱包中的 BNB / USDT / USDC 余额。\n"
                "第二跳未确认成功的记录会自动跳过，避免误操作。\n\n"
                f"当前模式：{'模拟执行' if dry_run else '真实执行'}\n"
                f"回收线程数：{worker_threads}\n\n"
                f"确认轮询：{confirm_poll_interval:g} 秒\n\n"
                "确认继续？"
            )
            if not messagebox.askyesno("中转手续费回收确认", text):
                self.log("用户取消了中转手续费回收")
                return False
        self.log(
            f"开始中转手续费回收：扫描 {len(records)} 条记录，线程数={worker_threads}，"
            f"确认轮询={confirm_poll_interval:g} 秒"
        )
        self.stop_requested.clear()
        self.is_running = True
        self._start_managed_thread(
            run_relay_fee_recovery,
            args=(self, dry_run),
            kwargs={
                "batch_ids": batch_filter if batch_filter else None,
                "show_dialog": show_dialog,
                "run_label": run_label,
            },
            name="onchain-relay-fee-recovery",
        )
        return True
    def start_batch_relay_fee_recovery(self, batch_ids: set[str]) -> bool:
        return self._launch_relay_fee_recovery(
            dry_run=bool(self.dry_run_var.get()),
            batch_ids=batch_ids,
            confirm=False,
            show_dialog=True,
            run_label="batch-dialog",
        )
    def start_batch_transfer(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
                try:
                    self._ensure_signing_sources([source for _row_key, source, _target in jobs])
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            if params.amount == self.AMOUNT_ALL_LABEL:
                source_counter: dict[str, int] = {}
                for _k, source, _target in jobs:
                    source_counter[source] = source_counter.get(source, 0) + 1
                duplicated = [s for s, c in source_counter.items() if c > 1]
                if duplicated:
                    messagebox.showerror("参数错误", "数量为“全部”时，同一个转出钱包只能执行 1 条任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = self._random_amount_range_text(params.random_min, params.random_max)
                relay_text = ""
                if params.relay_enabled:
                    relay_text = (
                        f"中转：已启用\n"
                        f"{self._relay_confirm_reserve_label_text()}：{self._decimal_to_text(params.relay_fee_reserve or Decimal('0'))} {self._network_fee_symbol(params.network)}\n"
                        f"{self._relay_confirm_recovery_text()}\n"
                        f"中转钱包文件：{self.relay_wallet_store.file_path.name}\n"
                    )
                text = (
                    f"即将执行链上真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n"
                    f"确认超时：{params.confirm_timeout_seconds:g} 秒\n"
                    f"确认轮询：{params.confirm_poll_interval_seconds:g} 秒\n"
                    f"{relay_text}\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了真实链上转账")
                    return

            self.log(f"已启动批量转账任务准备：任务={len(jobs)}，币种={token_desc}")
            self.stop_requested.clear()
            self.is_running = True
            self._start_managed_thread(
                self._run_batch_transfer,
                args=(jobs, params, dry_run),
                name="onchain-transfer-batch",
            )
        except Exception as exc:
            self.log(f"批量转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def start_retry_failed(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_failed_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            self._launch_retry_jobs(jobs, params, dry_run, confirm=True)
        except Exception as exc:
            self.log(f"失败重试启动异常：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def start_relay_fee_recovery(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            dry_run = bool(self.dry_run_var.get())
            self._launch_relay_fee_recovery(
                dry_run=dry_run,
                confirm=True,
                show_dialog=True,
                run_label="manual",
            )
        except Exception as exc:
            self.log(f"中转手续费回收启动异常：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def _run_batch_transfer(self, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
        recovery_batch_id = ""
        recovery_batch_finalized = False
        success = 0
        failed = 0
        if params.relay_enabled:
            run_relay_batch(self, jobs_data, params, dry_run)
            return
        dispatch_ui = self._dispatch_ui
        initial_row_number_by_key = self._row_number_by_key_snapshot()
        initial_progress_keys = self._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
        initial_context_by_row_key = {
            row_key: self._row_context_for_values(row_key, source, target)
            for row_key, source, target in jobs_data
        }
        token_desc = self._token_desc_from_params(params)
        amount_view = params.amount
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_view = f"random({self._decimal_to_text(params.random_min)}~{self._decimal_to_text(params.random_max)})"
        track_amount_total = not (dry_run and params.amount == self.AMOUNT_ALL_LABEL)
        set_ui_batch_size(self, params.threads)
        dispatch_ui(lambda keys=initial_progress_keys: self._begin_progress("transfer", keys))
        dispatch_ui(
            lambda a=self._token_amount_text(params.coin, Decimal("0")), g=("-" if dry_run else self._estimated_gas_fee_text(params.network, 0)), amt_known=track_amount_total: self._set_progress_metrics(
                amount_text=(a if amt_known else "-"),
                gas_text=g,
            ),
        )
        dispatch_ui(
            lambda: self.log(
                f"开始批量链上转账准备：mode={self._mode()}，任务={len(jobs_data)}，network={params.network}，"
                f"coin={token_desc}，amount={amount_view}，delay={params.delay}，threads={params.threads}，dry_run={dry_run}"
            ),
        )
        for row_key, _source, _target in jobs_data:
            self._mark_row_status_context(row_key, initial_context_by_row_key.get(row_key, ""))
            dispatch_ui(lambda k=row_key: self._set_status(k, "running", "准备中"))
        try:
            preflight_started_at = time.monotonic()
            if self._transfer_stop_requested():
                raise RuntimeError("任务已停止")
            if not dry_run:
                reconcile_started_at = time.monotonic()
                self.log(f"链上转账准备：开始账本对账，任务={len(jobs_data)}")
                if not self._reconcile_pending_onchain_operations(params.network, jobs_data):
                    self.log(f"链上转账准备：账本对账阻止执行，用时 {time.monotonic() - reconcile_started_at:.2f}s")
                    failed = len(initial_progress_keys)
                    for row_key in initial_progress_keys:
                        self._mark_row_status_context(row_key, initial_context_by_row_key.get(row_key, ""))
                        dispatch_ui(lambda k=row_key: self._set_status(k, "failed", "待恢复"))
                    dispatch_ui(lambda s=0, f=failed: self._finish_progress("transfer", s, f))
                    return
                self.log(f"链上转账准备：账本对账完成，用时 {time.monotonic() - reconcile_started_at:.2f}s")
            completed_recoveries: dict[str, dict[str, object]] = {}
            completed_by_row_key: dict[str, dict[str, object]] = {}
            task_key_by_row_key: dict[str, str] = {}
            if not dry_run:
                recovery_plan_started_at = time.monotonic()
                self.log(f"链上转账准备：开始恢复计划检查，任务={len(jobs_data)}")
                recovery_batch_id, recovery_plan, completed_recoveries, resumed = self._prepare_onchain_recovery_batch(params, jobs_data)
                if recovery_plan is not None:
                    task_key_by_row_key = {task.row_key: task.task_key for task in recovery_plan.tasks}
                    completed_by_row_key = {
                        task.row_key: completed_recoveries[task.task_key]
                        for task in recovery_plan.tasks
                        if task.task_key in completed_recoveries
                    }
                    jobs_data = [
                        item
                        for item in jobs_data
                        if task_key_by_row_key.get(item[0], "") not in completed_recoveries
                    ]
                    if resumed:
                        self.log(
                            f"检测到未结束链上批次，已恢复账本：跳过 {len(completed_recoveries)} 笔已确认交易，继续处理 {len(jobs_data)} 笔。"
                        )
                self.log(
                    f"链上转账准备：恢复计划检查完成，用时 {time.monotonic() - recovery_plan_started_at:.2f}s，"
                    f"跳过={len(completed_recoveries)}，继续={len(jobs_data)}"
                )
            self.log(f"链上转账准备完成，用时 {time.monotonic() - preflight_started_at:.2f}s")
            row_number_by_key = initial_row_number_by_key

            def job_prefix(index: int, row_key: str = "") -> str:
                return self._row_log_prefix(row_key, index, len(jobs_data), row_number_by_key)

            progress_keys = self._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
            context_by_row_key = {
                row_key: self._row_context_for_values(row_key, source, target)
                for row_key, source, target in jobs_data
            }
            dispatch_ui(lambda keys=progress_keys: self._begin_progress("transfer", keys))
            dispatch_ui(
                lambda a=self._token_amount_text(params.coin, Decimal("0")), g=("-" if dry_run else self._estimated_gas_fee_text(params.network, 0)), amt_known=track_amount_total: self._set_progress_metrics(
                    amount_text=(a if amt_known else "-"),
                    gas_text=g,
                ),
            )
            dispatch_ui(
                lambda: self.log(
                    f"开始批量链上转账：mode={self._mode()}，任务={len(jobs_data)}，network={params.network}，"
                    f"coin={token_desc}，amount={amount_view}，delay={params.delay}，threads={params.threads}，dry_run={dry_run}"
                ),
            )
            fallback_prefixes: dict[str, str] = {}
            for row_key, entry in completed_by_row_key.items() if not dry_run else ():
                payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                recovered_amount = str(payload.get("amount") or "")
                self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(
                    lambda k=row_key, amount=recovered_amount: self._set_status(
                        k,
                        "success",
                        self._success_status_text(params.coin, amount),
                    )
                )
            for row_key, _source, _target in jobs_data:
                fallback_prefixes[row_key] = job_prefix(len(fallback_prefixes) + 1, row_key)
                self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(lambda k=row_key: self._set_status(k, "waiting"))

            success = len(completed_recoveries)
            failed = 0
            resolved = 0
            total_amount = Decimal("0")
            if track_amount_total:
                for entry in completed_recoveries.values():
                    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                    try:
                        total_amount += Decimal(str(payload.get("amount") or "0"))
                    except Exception:
                        pass
            total_gas_fee_wei = 0
            lock = threading.Lock()
            resolved_event = threading.Event()
            resolved_row_keys: set[str] = set()
            journal_entry_ids: dict[str, str] = {}
            nonce_lock_map: dict[str, threading.Lock] = {}
            nonce_next_map: dict[str, int] = {}
            nonce_guard = threading.Lock()

            jobs_q: queue.Queue[tuple[int, str, str, str]] = queue.Queue()
            for i, item in enumerate(jobs_data, start=1):
                row_key, source, target = item
                jobs_q.put((i, row_key, source, target))

            def alloc_nonce(source_addr: str) -> int:
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    nonce = nonce_next_map.get(source_addr)
                    if nonce is None:
                        nonce = self.client.get_nonce(params.network, source_addr)
                    nonce_next_map[source_addr] = nonce + 1
                    return nonce

            def rollback_nonce(source_addr: str, used_nonce: int):
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    cached_next = nonce_next_map.get(source_addr)
                    # 仅在当前缓存恰好对应本次分配的 nonce 时回滚，避免覆盖其他线程进度。
                    if cached_next == used_nonce + 1:
                        nonce_next_map.pop(source_addr, None)

            def finalize_job(
                row_key: str,
                result_status: str,
                msg: str,
                *,
                amount_text: str = "",
                gas_fee_wei: int = 0,
                journal_state: str = "",
            ):
                nonlocal success, failed, resolved, total_amount, total_gas_fee_wei
                context_sig = context_by_row_key.get(row_key, "")
                row_status_text = ""
                with lock:
                    if row_key in resolved_row_keys:
                        return
                    resolved_row_keys.add(row_key)
                    if result_status == "success":
                        success += 1
                        row_status_text = self._success_status_text(params.coin, amount_text)
                        if track_amount_total:
                            try:
                                total_amount += Decimal(amount_text)
                            except Exception:
                                pass
                    else:
                        failed += 1
                    total_gas_fee_wei += gas_fee_wei
                    amount_total_text = self._token_amount_text(params.coin, total_amount) if track_amount_total else "-"
                    gas_total_text = "-" if dry_run else self._estimated_gas_fee_text(params.network, total_gas_fee_wei)
                    resolved += 1
                    done = resolved >= len(jobs_data)
                journal_entry_id = journal_entry_ids.get(row_key, "")
                if journal_entry_id and not dry_run:
                    if journal_state:
                        self.operation_journal.update(journal_entry_id, journal_state)
                    elif result_status == "success":
                        self.operation_journal.update(journal_entry_id, "completed")
                    elif result_status == "failed":
                        self.operation_journal.update(journal_entry_id, "failed")
                if done:
                    resolved_event.set()
                dispatch_ui(lambda m=msg: self.log(m))
                self._mark_row_status_context(row_key, context_sig)
                dispatch_ui(lambda k=row_key, s=result_status, t=row_status_text: self._set_status(k, s, t))
                dispatch_ui(lambda a=amount_total_text, g=gas_total_text: self._set_progress_metrics(amount_text=a, gas_text=g))

            def schedule_submitted_timeout(row_key: str, prefix: str, submitted_timeout_seconds: float):
                timeout_msg = f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，自动判定失败"
                timeout_deadline = time.monotonic() + max(0.0, float(submitted_timeout_seconds))

                def timeout_worker():
                    remaining = max(0.0, timeout_deadline - time.monotonic())
                    if remaining > 0 and self.stop_requested.wait(remaining):
                        return
                    if self.stop_requested.is_set() or self._closing or bool(getattr(self.root, "_closing", False)):
                        return
                    finalize_job(row_key, "failed", timeout_msg, journal_state="submitted")

                if submitted_timeout_seconds > 0:
                    timeout_name = f"onchain-submitted-timeout-{str(row_key or '')[-8:] or 'job'}"
                    self._submit_confirmation_task(timeout_worker, name=timeout_name)
                else:
                    timeout_worker()

            def target_balance_units(target_address: str) -> int:
                if params.token_is_native:
                    return self.client.get_balance_wei(params.network, target_address)
                return self.client.get_erc20_balance(params.network, params.token_contract, target_address)

            def schedule_submitted_confirmation(
                row_key: str,
                prefix: str,
                submitted_timeout_seconds: float,
                txid: str,
                amount_text: str,
                gas_fee_wei: int,
                *,
                source_addr: str,
                target_addr: str,
                used_nonce: int | None,
                value_units: int,
                target_balance_before_units: int | None,
            ) -> bool:
                tx_hash = str(txid or "").strip()
                if not tx_hash:
                    schedule_submitted_timeout(row_key, prefix, submitted_timeout_seconds)
                    return True
                confirm_deadline = time.time() + max(0.1, float(submitted_timeout_seconds))
                source_addr_norm = str(source_addr or "").strip().lower()
                target_addr_norm = str(target_addr or "").strip().lower()
                balance_confirmation_enabled = (
                    target_balance_before_units is not None
                    and int(value_units) > 0
                    and source_addr_norm
                    and target_addr_norm
                    and source_addr_norm != target_addr_norm
                )
                receipt_read_timeout = min(5.0, max(1.0, float(params.confirm_poll_interval_seconds)))

                def confirm_worker():
                    last_error = ""
                    while True:
                        if self.stop_requested.is_set() or self._closing or bool(getattr(self.root, "_closing", False)):
                            return
                        try:
                            try:
                                receipt = self.client.get_transaction_receipt(
                                    params.network,
                                    tx_hash,
                                    read_timeout=receipt_read_timeout,
                                    max_attempts=1,
                                )
                            except TypeError:
                                receipt = self.client.get_transaction_receipt(params.network, tx_hash)
                            if receipt is not None:
                                status_raw = receipt.get("status")
                                if status_raw is not None and self.client._int_from_hex(status_raw) != 1:
                                    finalize_job(
                                        row_key,
                                        "failed",
                                        f"{prefix} 交易确认失败：链上执行状态异常，txid={tx_hash}",
                                        journal_state="failed",
                                    )
                                else:
                                    finalize_job(
                                        row_key,
                                        "success",
                                        f"{prefix} 转账确认完成：txid={tx_hash}",
                                        amount_text=amount_text,
                                        gas_fee_wei=gas_fee_wei,
                                        journal_state="completed",
                                    )
                                return
                        except Exception as exc:
                            last_error = str(exc)
                        if balance_confirmation_enabled:
                            try:
                                target_after_units = target_balance_units(target_addr)
                                nonce_after = self.client.get_nonce(params.network, source_addr)
                                if (
                                    int(nonce_after) > int(used_nonce if used_nonce is not None else -1)
                                    and int(target_after_units) >= int(target_balance_before_units) + int(value_units)
                                ):
                                    finalize_job(
                                        row_key,
                                        "success",
                                        f"{prefix} 转账确认完成：余额已到账，txid={tx_hash}",
                                        amount_text=amount_text,
                                        gas_fee_wei=gas_fee_wei,
                                        journal_state="completed",
                                    )
                                    return
                            except Exception as exc:
                                last_error = str(exc)
                        if time.time() >= confirm_deadline:
                            detail = f"，最后错误：{last_error}" if last_error else ""
                            finalize_job(
                                row_key,
                                "failed",
                                f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，txid={tx_hash}{detail}",
                                journal_state="submitted",
                            )
                            return
                        if self.stop_requested.wait(params.confirm_poll_interval_seconds):
                            return

                timeout_name = f"onchain-submitted-confirm-{str(row_key or '')[-8:] or 'job'}"
                future = self._submit_confirmation_task(confirm_worker, name=timeout_name)
                if future is None:
                    finalize_job(
                        row_key,
                        "failed",
                        f"{prefix} 确认任务提交失败，交易可能已广播，txid={tx_hash}",
                        journal_state="submitted",
                    )
                    return False
                return True

            def worker():
                total = len(jobs_data)
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, row_key, source, target = jobs_q.get_nowait()
                    except queue.Empty:
                        return

                    result_status = "failed"
                    msg = ""
                    amount_text = ""
                    used_nonce: int | None = None
                    source_addr = ""
                    gas_fee_wei = 0
                    tx_sent = False
                    txid = ""
                    journal_entry_id = ""
                    journal_terminal_state = ""
                    value_units = 0
                    target_balance_before_units: int | None = None
                    self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                    dispatch_ui(lambda k=row_key: self._set_status(k, "running"))
                    submitted_timeout_seconds = max(0.0, float(params.confirm_timeout_seconds))
                    prefix = job_prefix(i, row_key)
                    try:
                        if dry_run:
                            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                                amount_text = self._decimal_to_text(
                                    self._random_decimal_between(
                                        params.random_min,
                                        params.random_max,
                                        params.token_decimals,
                                    )
                                )
                            elif params.amount == self.AMOUNT_ALL_LABEL:
                                amount_text = f"{self.AMOUNT_ALL_LABEL}(模拟)"
                            else:
                                amount_text = params.amount
                            prefix = f"{job_prefix(i, row_key)}[{self._mask_credential(source)}]"
                            msg = (
                                f"{prefix} 模拟成功 -> {params.coin} {amount_text} "
                                f"到 {self._mask(target, head=8, tail=6)}"
                            )
                            result_status = "success"
                        else:
                            private_key, source_addr = self._resolve_wallet(source)
                            value_units, gas_price_wei, gas_limit, amount_text = self._resolve_amount_and_gas(
                                params, source_addr, target
                            )
                            try:
                                target_balance_before_units = target_balance_units(target)
                            except Exception:
                                target_balance_before_units = None
                            nonce = alloc_nonce(source_addr)
                            used_nonce = nonce
                            journal = getattr(self, "operation_journal", None)
                            if journal is not None:
                                journal_entry_id = journal.begin(
                                    "onchain-transfer",
                                    f"{params.network}:{source_addr.lower()}",
                                    {
                                        "network": params.network,
                                        "source_address": source_addr,
                                        "target_address": target,
                                        "nonce": nonce,
                                        "coin": params.coin,
                                        "amount": amount_text,
                                        "batch_id": recovery_batch_id,
                                        "task_key": task_key_by_row_key.get(row_key, ""),
                                    },
                                )
                                journal_entry_ids[row_key] = journal_entry_id
                                journal.update(journal_entry_id, "broadcasting")
                            recovery_timeout_seconds = min(max(6.0, float(params.confirm_timeout_seconds) / 3.0), 20.0)
                            if params.token_is_native:
                                submission = self.client.submit_native_transfer_reliably(
                                    network=params.network,
                                    private_key=private_key,
                                    to_address=target,
                                    value_wei=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                    source_address=source_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            else:
                                submission = self.client.submit_erc20_transfer_reliably(
                                    network=params.network,
                                    private_key=private_key,
                                    token_contract=params.token_contract,
                                    to_address=target,
                                    amount_units=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                    source_address=source_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            txid = submission.tx_hash
                            tx_sent = True
                            if journal_entry_id:
                                self.operation_journal.update(
                                    journal_entry_id,
                                    "submitted",
                                    txid=txid,
                                    recovered=bool(submission.recovered),
                                    recovery_reason=str(submission.recovery_reason or ""),
                                )
                            if txid:
                                gas_fee_wei = max(0, int(gas_limit)) * max(0, int(gas_price_wei))
                                gas_text = self._gas_fee_amount_text(params.network, gas_fee_wei)
                                if submission.recovered and submission.recovery_reason == "receipt":
                                    msg = (
                                        f"{prefix} 金额={params.coin} {amount_text}，预估gas={gas_text}，"
                                        f"from={source_addr}，to={target}，txid={txid}，广播异常但链上已确认"
                                    )
                                    result_status = "success"
                                    journal_terminal_state = "completed"
                                elif submission.recovered:
                                    recover_reason = {
                                        "tx-hash": "已按 tx hash 找到交易",
                                        "source-nonce": "已按源地址 nonce 发现交易占用",
                                    }.get(submission.recovery_reason, "已进入链上确认")
                                    msg = (
                                        f"{prefix} 转账广播结果待确认：金额={params.coin} {amount_text}，预估gas={gas_text}，"
                                        f"from={source_addr}，to={target}，txid={txid}，{recover_reason}"
                                    )
                                    result_status = "submitted"
                                else:
                                    msg = (
                                        f"{prefix} 转账广播完成，等待确认：金额={params.coin} {amount_text}，预估gas={gas_text}，"
                                        f"from={source_addr}，to={target}，txid={txid}"
                                    )
                                    result_status = "submitted"
                            else:
                                msg = (
                                    f"{prefix} 转账确认中：未拿到 txid，"
                                    f"from={source_addr}，to={target}"
                                )
                                result_status = "submitted"
                    except Exception as exc:
                        if used_nonce is not None and source_addr and not tx_sent:
                            rollback_nonce(source_addr, used_nonce)
                        if journal_entry_id and not tx_sent:
                            self.operation_journal.update(journal_entry_id, "failed", error=str(exc))
                        msg = f"{prefix} 转账失败：{exc}"
                    finally:
                        jobs_q.task_done()

                    if result_status == "submitted":
                        dispatch_ui(lambda m=msg: self.log(m))
                        self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                        dispatch_ui(lambda k=row_key: self._set_status(k, "submitted"))
                        schedule_submitted_confirmation(
                            row_key,
                            prefix,
                            submitted_timeout_seconds,
                            txid,
                            amount_text,
                            gas_fee_wei,
                            source_addr=source_addr,
                            target_addr=target,
                            used_nonce=used_nonce,
                            value_units=value_units,
                            target_balance_before_units=target_balance_before_units,
                        )
                    else:
                        finalize_job(
                            row_key,
                            result_status,
                            msg,
                            amount_text=amount_text,
                            gas_fee_wei=gas_fee_wei,
                            journal_state=(journal_terminal_state or ("failed" if result_status == "failed" else "")),
                        )
                    if params.delay > 0:
                        if self.stop_requested.wait(params.delay):
                            return

            workers: list[threading.Thread] = []
            worker_count = max(1, min(params.threads, len(jobs_data)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                with lock:
                    pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
                for row_key in pending_row_keys:
                    prefix = fallback_prefixes.get(row_key, "")
                    stop_msg = f"{prefix} 已停止" if prefix else "任务已停止"
                    finalize_job(row_key, "failed", stop_msg, journal_state="interrupted")
                resolved_event.set()
                dispatch_ui(lambda: self.log("链上转账任务已停止"))
            batch_finalize_timeout_seconds = max(
                0.2,
                max(0.0, float(params.confirm_timeout_seconds)) + 1.0,
            )
            if len(jobs_data) == 0:
                resolved_event.set()
            if not resolved_event.wait(batch_finalize_timeout_seconds):
                with lock:
                    pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
                for row_key in pending_row_keys:
                    prefix = fallback_prefixes.get(row_key, "")
                    timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                    finalize_job(row_key, "failed", timeout_msg, journal_state="submitted")

            summary = f"链上转账任务结束：成功 {success}，失败 {failed}"
            if not dry_run:
                summary = (
                    f"{summary}，转账总额={self._token_amount_text(params.coin, total_amount) if track_amount_total else '-'}，"
                    f"预估gas合计={self._gas_fee_amount_text(params.network, total_gas_fee_wei)}"
                )
            else:
                summary = f"{summary}，转账总额={self._token_amount_text(params.coin, total_amount) if track_amount_total else '-'}，预估gas合计=-"
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(
                lambda s=success, f=failed, detail=summary: self._show_result_summary_dialog(
                    title="执行完成",
                    summary_title="链上批量转账完成",
                    success=s,
                    failed=f,
                    detail_text=detail,
                )
            )
            dispatch_ui(lambda s=success, f=failed: self._finish_progress("transfer", s, f))
            if recovery_batch_id:
                task_states = self.operation_journal.batch_task_states(recovery_batch_id, kind="onchain-transfer")
                has_pending_receipt = any(
                    str(entry.get("state") or "") not in {"completed", "cancelled", "failed", "reconciled"}
                    for entry in task_states.values()
                )
                self.operation_journal.finish_batch(
                    recovery_batch_id,
                    success=success,
                    failed=failed,
                    interrupted=bool(self.stop_requested.is_set()) or has_pending_receipt,
                )
                recovery_batch_finalized = True
        except Exception as exc:
            err_text = str(exc)
            stopped = self._transfer_stop_requested() or err_text == "任务已停止"
            if stopped:
                failed = max(failed, len(initial_progress_keys) - success)
                for row_key in initial_progress_keys:
                    if str(getattr(self, "row_status", {}).get(row_key, "") or "") in {"success", "failed"}:
                        continue
                    self._mark_row_status_context(row_key, initial_context_by_row_key.get(row_key, ""))
                    dispatch_ui(lambda k=row_key: self._set_status(k, "failed", "已停止"))
                dispatch_ui(lambda: self.log("链上转账任务已停止"))
            else:
                dispatch_ui(lambda m=f"链上转账任务异常终止：{err_text}": self.log(m))
                dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(
                lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("transfer", s, f),
            )
        finally:
            if recovery_batch_id and not recovery_batch_finalized:
                try:
                    self.operation_journal.finish_batch(
                        recovery_batch_id,
                        success=success,
                        failed=max(1, failed),
                        interrupted=True,
                    )
                except Exception:
                    pass
            self._clear_source_private_key_cache()
            self.is_running = False
