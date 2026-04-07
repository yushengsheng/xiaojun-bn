#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from page_onchain_base import *  # noqa: F401,F403


class OnchainTransferRunnerMixin(object):
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

        return WithdrawRuntimeParams(
            coin=coin,
            amount=amount,
            network=network,
            delay=delay,
            threads=threads,
            random_enabled=random_enabled,
            random_min=random_min,
            random_max=random_max,
            token_contract=token.contract if not token.is_native else "",
            token_decimals=decimals,
            token_is_native=bool(token.is_native),
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
                text = (
                    f"即将执行链上当前行真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：1\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了当前行真实链上转账")
                    return

            self.log(f"已启动当前行转账任务：mode={self._mode()}，币种={token_desc}")
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
                text = (
                    f"即将执行链上真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
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
                    messagebox.showerror("参数错误", "重试模式下，数量为“全部”时同一个转出钱包只能处理 1 条失败任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
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
                    "确认继续？"
                )
                if not messagebox.askyesno("重试确认", text):
                    self.log("用户取消了失败重试")
                    return

            self.log(f"开始重试失败任务：{len(jobs)}")
            self.stop_requested.clear()
            self.is_running = True
            self._start_managed_thread(
                self._run_batch_transfer,
                args=(jobs, params, dry_run),
                name="onchain-transfer-retry-failed",
            )
        except Exception as exc:
            self.log(f"失败重试启动异常：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def _run_batch_transfer(self, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
        dispatch_ui = self._dispatch_ui
        try:
            set_ui_batch_size(self, params.threads)
            def job_prefix(index: int) -> str:
                return f"[{index}/{len(jobs_data)}]"

            amount_view = params.amount
            token_desc = self._token_desc_from_params(params)
            progress_keys = self._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
            track_amount_total = not (dry_run and params.amount == self.AMOUNT_ALL_LABEL)
            context_by_row_key = {
                row_key: self._row_context_for_values(row_key, source, target)
                for row_key, source, target in jobs_data
            }
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_view = f"random({self._decimal_to_text(params.random_min)}~{self._decimal_to_text(params.random_max)})"
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
            for row_key, _source, _target in jobs_data:
                fallback_prefixes[row_key] = job_prefix(len(fallback_prefixes) + 1)
                self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(lambda k=row_key: self._set_status(k, "waiting"))

            success = 0
            failed = 0
            resolved = 0
            total_amount = Decimal("0")
            total_gas_fee_wei = 0
            lock = threading.Lock()
            resolved_event = threading.Event()
            resolved_row_keys: set[str] = set()
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

            def finalize_job(row_key: str, result_status: str, msg: str, *, amount_text: str = "", gas_fee_wei: int = 0):
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
                if done:
                    resolved_event.set()
                dispatch_ui(lambda m=msg: self.log(m))
                self._mark_row_status_context(row_key, context_sig)
                dispatch_ui(lambda k=row_key, s=result_status, t=row_status_text: self._set_status(k, s, t))
                dispatch_ui(lambda a=amount_total_text, g=gas_total_text: self._set_progress_metrics(amount_text=a, gas_text=g))

            def schedule_submitted_timeout(row_key: str, prefix: str, submitted_timeout_seconds: float):
                timeout_msg = f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，自动判定失败"

                def timeout_worker():
                    if submitted_timeout_seconds > 0 and self.stop_requested.wait(submitted_timeout_seconds):
                        return
                    if self.stop_requested.is_set() or self._closing or bool(getattr(self.root, "_closing", False)):
                        return
                    finalize_job(row_key, "failed", timeout_msg)

                if submitted_timeout_seconds > 0:
                    timeout_name = f"onchain-submitted-timeout-{str(row_key or '')[-8:] or 'job'}"
                    self._start_managed_thread(timeout_worker, name=timeout_name)
                else:
                    timeout_worker()

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
                    self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                    dispatch_ui(lambda k=row_key: self._set_status(k, "running"))
                    submitted_timeout_seconds = max(0.0, float(getattr(self, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS)))
                    prefix = job_prefix(i)
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
                            prefix = f"[{i}/{total}][{self._mask_credential(source)}]"
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
                            nonce = alloc_nonce(source_addr)
                            used_nonce = nonce
                            if params.token_is_native:
                                txid = self.client.send_native_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    to_address=target,
                                    value_wei=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            else:
                                txid = self.client.send_erc20_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    token_contract=params.token_contract,
                                    to_address=target,
                                    amount_units=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            tx_sent = True
                            if txid:
                                gas_fee_wei = max(0, int(gas_limit)) * max(0, int(gas_price_wei))
                                gas_text = self._gas_fee_amount_text(params.network, gas_fee_wei)
                                msg = (
                                    f"{prefix} 金额={params.coin} {amount_text}，预估gas={gas_text}，"
                                    f"from={source_addr}，to={target}，txid={txid}"
                                )
                                result_status = "success"
                            else:
                                msg = (
                                    f"{prefix} 转账确认中：未拿到 txid，"
                                    f"from={source_addr}，to={target}"
                                )
                                result_status = "submitted"
                    except Exception as exc:
                        if used_nonce is not None and source_addr and not tx_sent:
                            rollback_nonce(source_addr, used_nonce)
                        msg = f"[{i}/{total}] 转账失败：{exc}"
                    finally:
                        jobs_q.task_done()

                    if result_status == "submitted":
                        dispatch_ui(lambda m=msg: self.log(m))
                        self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                        dispatch_ui(lambda k=row_key: self._set_status(k, "submitted"))
                        schedule_submitted_timeout(row_key, prefix, submitted_timeout_seconds)
                    else:
                        finalize_job(row_key, result_status, msg, amount_text=amount_text, gas_fee_wei=gas_fee_wei)
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
                    finalize_job(row_key, "failed", stop_msg)
                resolved_event.set()
                dispatch_ui(lambda: self.log("链上转账任务已停止"))
            batch_finalize_timeout_seconds = max(
                0.2,
                max(0.0, float(getattr(self, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS))) + 1.0,
            )
            if len(jobs_data) == 0:
                resolved_event.set()
            if not resolved_event.wait(batch_finalize_timeout_seconds):
                with lock:
                    pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
                for row_key in pending_row_keys:
                    prefix = fallback_prefixes.get(row_key, "")
                    timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                    finalize_job(row_key, "failed", timeout_msg)

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
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"链上转账任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(
                lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("transfer", s, f),
            )
        finally:
            self.is_running = False
