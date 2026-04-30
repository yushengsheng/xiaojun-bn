#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from page_onchain_base import (
    Decimal,
    EvmToken,
    WithdrawRuntimeParams,
    messagebox,
    queue,
    set_ui_batch_size,
    threading,
)


class OnchainQueryMixin(object):
    def _prepare_async_task(self) -> bool:
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return False
        self.stop_requested.clear()
        self.is_running = True
        return True
    def stop_current_tasks(self):
        if not self.is_running:
            self.log("当前没有运行中的链上任务")
            return
        self.stop_requested.set()
        self.log("已收到停止请求，正在停止当前链上任务...")
    def query_current_source_balance(self):
        try:
            if not self._is_mode_1m():
                return
            source = self.source_credential_var.get().strip()
            if not source:
                messagebox.showerror("参数错误", "请先填写转出钱包私钥/助记词")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            if not self._prepare_async_task():
                return
            self._start_managed_thread(
                self._run_query_balance_one_to_many,
                args=(network, token, source, []),
                name="onchain-query-current-source",
            )
        except Exception as exc:
            self.log(f"转出钱包余额查询启动失败：{exc}")
            self.is_running = False
            messagebox.showerror("执行异常", str(exc))
    def query_current_target_balance(self):
        try:
            if not self._is_mode_m1():
                return
            target = self.target_address_var.get().strip()
            if not target:
                messagebox.showerror("参数错误", "请先填写收款地址")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            target = self._validate_recipient_address(target, "收款地址")
            self.target_address_var.set(target)
            if not self._prepare_async_task():
                return
            self._start_managed_thread(
                self._run_query_balance_many_to_one,
                args=(network, token, target, []),
                name="onchain-query-current-target",
            )
        except Exception as exc:
            self.log(f"收款地址余额查询启动失败：{exc}")
            self.is_running = False
            messagebox.showerror("执行异常", str(exc))
    def _update_balance_heading(self):
        symbol = self.symbol_var.get().strip() or "-"
        text = f"余额({symbol})"
        self._tree_heading_base_texts["balance"] = text
        self.tree.heading("balance", text=text)
        self._apply_import_target_view()
    def _build_token_options(self, network: str, prefer_symbol: str = "", prefer_contract: str = ""):
        net = network.strip().upper()
        defaults = self.client.get_default_tokens(net)
        custom = list((self.custom_tokens_by_network.get(net) or {}).values())
        all_tokens = defaults + custom

        mapping: dict[str, EvmToken] = {}
        used: set[str] = set()
        for token in all_tokens:
            label = self._token_display(token)
            if label in used:
                if token.contract:
                    label = f"{token.symbol}({self._short_contract(token.contract)})"
                else:
                    label = f"{token.symbol}(原生)"
            idx = 2
            while label in used:
                label = f"{label}#{idx}"
                idx += 1
            used.add(label)
            mapping[label] = token

        self.current_tokens = mapping
        values = list(mapping.keys())
        self.coin_box.configure(values=values)
        if not values:
            self.coin_var.set("")
            self.symbol_var.set("-")
            return

        current_label = self.coin_var.get().strip()
        selected = ""
        prefer_contract_l = prefer_contract.strip().lower()
        prefer_symbol_u = prefer_symbol.strip().upper()

        if prefer_contract_l:
            for label, token in mapping.items():
                if token.contract.strip().lower() == prefer_contract_l:
                    selected = label
                    break
        if not selected and prefer_symbol_u:
            for label, token in mapping.items():
                if token.symbol.strip().upper() == prefer_symbol_u:
                    selected = label
                    break
        if not selected and current_label in mapping:
            selected = current_label
        if not selected:
            selected = values[0]
        self.coin_var.set(selected)
    def _selected_token(self, with_message: bool = False) -> EvmToken | None:
        label = self.coin_var.get().strip() if hasattr(self, "coin_var") else ""
        token_map = self.current_tokens if hasattr(self, "current_tokens") else {}
        token = token_map.get(label)
        if token is not None:
            return token

        if token_map:
            first_label = next(iter(token_map))
            if hasattr(self, "coin_var"):
                self.coin_var.set(first_label)
            return token_map[first_label]

        network = self.network_var.get().strip().upper()
        if network in {"ETH", "BSC"}:
            symbol = self.client.get_symbol(network)
            if symbol:
                return EvmToken(symbol=symbol, contract="", decimals=18, is_native=True)

        if with_message:
            messagebox.showerror("参数错误", "币种未设置，无法执行")
        return None
    def _token_desc(self, token: EvmToken) -> str:
        if token.is_native:
            return token.symbol
        return f"{token.symbol}({self._short_contract(token.contract)})"
    def _token_desc_from_params(self, params: WithdrawRuntimeParams) -> str:
        if params.token_is_native:
            return params.coin
        return f"{params.coin}({self._short_contract(params.token_contract)})"
    def search_contract_token(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        network = self.network_var.get().strip().upper()
        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "请先设置网络后再搜索合约")
            return
        contract = self.contract_search_var.get().strip()
        if not self.client.is_address(contract):
            messagebox.showerror("参数错误", "合约地址格式错误")
            return
        self.log(f"开始搜索代币合约：network={network}, contract={contract}")
        self._start_managed_thread(
            self._run_search_contract_token,
            args=(network, contract),
            name="onchain-search-contract",
        )
    def _run_search_contract_token(self, network: str, contract: str):
        try:
            token = self.client.get_erc20_token_info(network, contract)
            self.custom_tokens_by_network.setdefault(network, {})[token.contract.lower()] = token
            self._dispatch_ui(lambda n=network, t=token: self._apply_found_token(n, t))
        except Exception as exc:
            err_text = str(exc)
            self._dispatch_ui(lambda m=f"合约搜索失败：{err_text}": self.log(m))
            self._dispatch_ui(lambda e=err_text: messagebox.showerror("合约搜索失败", e))
    def _apply_found_token(self, network: str, token: EvmToken):
        self.log(
            f"合约识别成功：network={network}, symbol={token.symbol}, decimals={token.decimals}, contract={token.contract}"
        )
        if self.network_var.get().strip().upper() == network:
            self._build_token_options(network=network, prefer_symbol=token.symbol, prefer_contract=token.contract)
    def _collect_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if key in self.checked_row_keys:
                    try:
                        target = self._validate_recipient_address(item.target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, item.source, target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if key in self.checked_row_keys:
                    try:
                        safe_target = self._validate_recipient_address(target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, source, safe_target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            try:
                target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                if with_message:
                    messagebox.showerror("参数错误", str(exc))
                return []
            self.target_address_var.set(target)
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if key in self.checked_row_keys:
                    jobs.append((key, source, target))

        if with_message and not jobs:
            messagebox.showwarning("提示", "请先勾选至少一条数据")
        return jobs
    def _collect_failed_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if self._display_status(key) == "failed":
                    try:
                        target = self._validate_recipient_address(item.target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, item.source, target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if self._display_status(key) == "failed":
                    try:
                        safe_target = self._validate_recipient_address(target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, source, safe_target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            try:
                target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                if with_message:
                    messagebox.showerror("参数错误", str(exc))
                return []
            self.target_address_var.set(target)
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if self._display_status(key) == "failed":
                    jobs.append((key, source, target))

        if with_message and not jobs:
            tip = "当前没有失败记录可重试"
            if self._has_submitted_jobs():
                tip = "当前没有失败记录可重试，存在确认中的记录，请等待自动确认完成"
            messagebox.showwarning("提示", tip)
        return jobs
    def _has_submitted_jobs(self) -> bool:
        return any(self._display_status(row_key) == "submitted" for row_key in self._active_row_keys())
    def start_query_balance(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            mode = self._mode()

            if mode == self.MODE_1M:
                source = self.source_credential_var.get().strip()
                selected_targets = [t for t in self.store.one_to_many_addresses if self._one_to_many_key(t) in self.checked_row_keys]
                targets: list[str] = []
                seen_t: set[str] = set()
                for target in selected_targets:
                    t = target.strip()
                    if not t or t in seen_t:
                        continue
                    seen_t.add(t)
                    targets.append(t)
                if not targets:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                self._mark_query_status_contexts([self._one_to_many_key(t) for t in targets], source)
                self._set_query_statuses([self._one_to_many_key(t) for t in targets], "waiting")
                self.stop_requested.clear()
                self.is_running = True
                self._start_managed_thread(
                    self._run_query_balance_one_to_many,
                    args=(network, token, source, targets),
                    name="onchain-query-one-to-many",
                )
                return

            if mode == self.MODE_M1:
                target = self.target_address_var.get().strip()
                if target:
                    try:
                        target = self._validate_recipient_address(target, "收款地址")
                        self.target_address_var.set(target)
                    except Exception as exc:
                        self.log(f"{exc}，已跳过收款地址余额查询")
                        target = ""
                selected_sources = [s for s in self.store.many_to_one_sources if self._many_to_one_key(s) in self.checked_row_keys]
                sources: list[str] = []
                seen_s: set[str] = set()
                for source in selected_sources:
                    s = source.strip()
                    if not s or s in seen_s:
                        continue
                    seen_s.add(s)
                    sources.append(s)
                if not sources:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                target_context = self._normalize_context_target(target) if target else ""
                self._mark_query_status_contexts([self._many_to_one_key(s) for s in sources], target_context)
                self._set_query_statuses([self._many_to_one_key(s) for s in sources], "waiting")
                self.stop_requested.clear()
                self.is_running = True
                self._start_managed_thread(
                    self._run_query_balance_many_to_one,
                    args=(network, token, target, sources),
                    name="onchain-query-many-to-one",
                )
                return

            selected_pairs = [x for x in self.store.multi_to_multi_pairs if self._m2m_key(x) in self.checked_row_keys]
            sources: list[str] = []
            seen: set[str] = set()
            row_keys_by_source: dict[str, list[str]] = {}
            for item in selected_pairs:
                s = item.source.strip()
                if not s or s in seen:
                    if s:
                        row_keys_by_source.setdefault(s, []).append(self._m2m_key(item))
                    continue
                seen.add(s)
                sources.append(s)
                row_keys_by_source[s] = [self._m2m_key(item)]
            if not sources:
                messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                return

            self._query_row_keys_by_source = {k: list(v) for k, v in row_keys_by_source.items()}
            self._set_query_statuses([key for keys in row_keys_by_source.values() for key in keys], "waiting")
            self.stop_requested.clear()
            self.is_running = True
            self._start_managed_thread(
                self._run_query_balance_for_sources,
                args=(network, token, sources),
                name="onchain-query-multi-to-multi",
            )
        except Exception as exc:
            self.log(f"余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def start_query_balance_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            if self._warn_if_draft_selected():
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            _row_key, source, target = job
            mode = self._mode()
            if mode == self.MODE_1M:
                try:
                    target = self._validate_recipient_address(target, "接收地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
                self.stop_requested.clear()
                self.is_running = True
                self._mark_query_status_context(self._one_to_many_key(target), source)
                self._set_query_status(self._one_to_many_key(target), "waiting")
                self._start_managed_thread(
                    self._run_query_balance_one_to_many,
                    args=(network, token, source, [target]),
                    name="onchain-query-row-one-to-many",
                )
                return
            if mode == self.MODE_M1:
                target_addr = target
                if target_addr:
                    try:
                        target_addr = self._validate_recipient_address(target_addr, "收款地址")
                        self.target_address_var.set(target_addr)
                    except Exception as exc:
                        self.log(f"{exc}，已跳过收款地址余额查询")
                        target_addr = ""
                self.stop_requested.clear()
                self.is_running = True
                self._mark_query_status_context(self._many_to_one_key(source), self._normalize_context_target(target_addr))
                self._set_query_status(self._many_to_one_key(source), "waiting")
                self._start_managed_thread(
                    self._run_query_balance_many_to_one,
                    args=(network, token, target_addr, [source]),
                    name="onchain-query-row-many-to-one",
                )
                return
            self._query_row_keys_by_source = {source: [_row_key]}
            self._set_query_status(_row_key, "waiting")
            self.stop_requested.clear()
            self.is_running = True
            self._start_managed_thread(
                self._run_query_balance_for_sources,
                args=(network, token, [source]),
                name="onchain-query-row-multi-to-multi",
            )
        except Exception as exc:
            self.log(f"当前行余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))
    def _run_query_balance_for_sources(self, network: str, token: EvmToken, sources: list[str]):
        dispatch_ui = self._dispatch_ui
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            symbol = token.symbol
            token_desc = symbol if token.is_native else f"{symbol}({self._short_contract(token.contract)})"
            row_keys_by_source = {k: list(v) for k, v in getattr(self, "_query_row_keys_by_source", {}).items()}
            if not row_keys_by_source:
                row_keys_by_source = {}
                for item in getattr(self.store, "multi_to_multi_pairs", []):
                    src = item.source.strip()
                    if src in sources:
                        row_keys_by_source.setdefault(src, []).append(self._m2m_key(item))
            progress_keys = self._unique_row_keys([key for src in sources for key in row_keys_by_source.get(src, [])])
            dispatch_ui(lambda keys=progress_keys: self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=[key for src in sources for key in row_keys_by_source.get(src, [])]: self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：网络={network}，币种={token_desc}，钱包数={len(sources)}"))
            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, source in enumerate(sources, start=1):
                jobs.put((i, source))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, source = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_keys = list(row_keys_by_source.get(source, []))
                    prefix = f"[{i}/{len(sources)}]"
                    dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "running"))
                    try:
                        addr = self._resolve_source_address(source)
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.source_balance_cache[source] = amount
                            total += amount
                            ok += 1
                            done_row_keys.update(row_keys)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            done_row_keys.update(row_keys)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = max(1, min(self._runtime_worker_threads(), len(sources)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in progress_keys if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))
            summary = f"余额查询结束：成功 {ok}，失败 {failed}，{symbol} 合计={self._decimal_to_text(total)}"
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self._query_row_keys_by_source = {}
            self.is_running = False
    def _run_query_balance_one_to_many(self, network: str, token: EvmToken, source: str, targets: list[str]):
        dispatch_ui = self._dispatch_ui
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            symbol = token.symbol
            token_desc = self._token_desc(token)
            src_count = 1 if source else 0
            context_sig = source.strip()
            row_keys_by_target = {target: self._one_to_many_key(target) for target in targets}
            self._mark_query_status_contexts(list(row_keys_by_target.values()), context_sig)
            dispatch_ui(lambda keys=self._unique_row_keys(list(row_keys_by_target.values())): self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=list(row_keys_by_target.values()): self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：模式=1对多，网络={network}，币种={token_desc}，源钱包数={src_count}，目标地址数={len(targets)}"))

            # 先查询转出钱包余额，展示在私钥输入框旁
            source_addr = ""
            source_query_state = "skip"
            if source:
                try:
                    source_addr = self._resolve_source_address(source)
                    if token.is_native:
                        src_units = self.client.get_balance_wei(network, source_addr)
                    else:
                        src_units = self.client.get_erc20_balance(network, token.contract, source_addr)
                    src_amount = self._units_to_amount(src_units, token.decimals)
                    self.source_balance_cache[source] = src_amount
                    src_text = f"{symbol}:{self._decimal_to_text(src_amount)}"
                    dispatch_ui(lambda c=context_sig, t=src_text: self._apply_source_balance_summary(c, t))
                    dispatch_ui(lambda m=f"[SRC][{self._mask(source_addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(src_amount)}": self.log(m))
                    source_query_state = "ok"
                except Exception as exc:
                    dispatch_ui(lambda c=context_sig: self._clear_source_balance_summary(c))
                    dispatch_ui(lambda m=f"[SRC] 查询失败：{exc}": self.log(m))
                    source_query_state = "failed"
            else:
                dispatch_ui(lambda: self.source_balance_var.set("-"))
                dispatch_ui(lambda: self.log("[SRC] 未提供转出钱包，已跳过源钱包余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, addr in enumerate(targets, start=1):
                jobs.put((i, addr))
            if not targets:
                dispatch_ui(lambda: self.log("[DST] 未提供目标地址，已跳过目标地址余额查询"))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, addr = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_key = row_keys_by_target.get(addr, "")
                    prefix = f"[{i}/{len(targets)}]"
                    if row_key:
                        self._mark_query_status_context(row_key, context_sig)
                        dispatch_ui(lambda k=row_key: self._set_query_status(k, "running"))
                    try:
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.target_balance_cache[addr] = amount
                            total += amount
                            ok += 1
                            if row_key:
                                done_row_keys.add(row_key)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            if row_key:
                                done_row_keys.add(row_key)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = min(self._runtime_worker_threads(), len(targets))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in self._unique_row_keys(list(row_keys_by_target.values())) if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))

            src_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(source_query_state, source_query_state)
            summary = (
                f"余额查询结束：源钱包查询={src_status_text}，目标地址成功 {ok}，失败 {failed}，"
                f"目标地址{symbol}合计={self._decimal_to_text(total)}"
            )
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self.is_running = False
    def _run_query_balance_many_to_one(self, network: str, token: EvmToken, target: str, sources: list[str]):
        dispatch_ui = self._dispatch_ui
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            symbol = token.symbol
            token_desc = self._token_desc(token)
            target_count = 1 if target else 0
            context_sig = self._normalize_context_target(target)
            row_keys_by_source = {source: self._many_to_one_key(source) for source in sources}
            self._mark_query_status_contexts(list(row_keys_by_source.values()), context_sig)
            dispatch_ui(lambda keys=self._unique_row_keys(list(row_keys_by_source.values())): self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=list(row_keys_by_source.values()): self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：模式=多对1，网络={network}，币种={token_desc}，收款地址数={target_count}，源钱包数={len(sources)}"))

            target_query_state = "skip"
            if target:
                try:
                    if token.is_native:
                        target_units = self.client.get_balance_wei(network, target)
                    else:
                        target_units = self.client.get_erc20_balance(network, token.contract, target)
                    target_amount = self._units_to_amount(target_units, token.decimals)
                    self.target_balance_cache[target] = target_amount
                    target_text = f"{symbol}:{self._decimal_to_text(target_amount)}"
                    dispatch_ui(lambda c=context_sig, t=target_text: self._apply_target_balance_summary(c, t))
                    dispatch_ui(lambda m=f"[DST][{self._mask(target, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(target_amount)}": self.log(m))
                    target_query_state = "ok"
                except Exception as exc:
                    dispatch_ui(lambda c=context_sig: self._clear_target_balance_summary(c))
                    dispatch_ui(lambda m=f"[DST] 查询失败：{exc}": self.log(m))
                    target_query_state = "failed"
            else:
                dispatch_ui(lambda: self.target_balance_var.set("-"))
                dispatch_ui(lambda: self.log("[DST] 未提供收款地址，已跳过收款地址余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, source in enumerate(sources, start=1):
                jobs.put((i, source))
            if not sources:
                dispatch_ui(lambda: self.log("[SRC] 未提供源钱包，已跳过源钱包余额查询"))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, source = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_key = row_keys_by_source.get(source, "")
                    prefix = f"[{i}/{len(sources)}]"
                    if row_key:
                        self._mark_query_status_context(row_key, context_sig)
                        dispatch_ui(lambda k=row_key: self._set_query_status(k, "running"))
                    try:
                        addr = self._resolve_source_address(source)
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.source_balance_cache[source] = amount
                            total += amount
                            ok += 1
                            if row_key:
                                done_row_keys.add(row_key)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            if row_key:
                                done_row_keys.add(row_key)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = min(self._runtime_worker_threads(), len(sources))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in self._unique_row_keys(list(row_keys_by_source.values())) if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))

            dst_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(target_query_state, target_query_state)
            summary = (
                f"余额查询结束：收款地址查询={dst_status_text}，源钱包成功 {ok}，失败 {failed}，"
                f"源钱包{symbol}合计={self._decimal_to_text(total)}"
            )
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self.is_running = False
