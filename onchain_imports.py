#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from page_onchain_base import *  # noqa: F401,F403


class OnchainImportMixin(object):
    def _parse_m2m_lines(self, lines: list[str]) -> list[OnchainPairEntry]:
        result: list[OnchainPairEntry] = []
        seen: set[tuple[str, str]] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if any(x in s for x in ("\t", ",", ";")):
                arr = [x.strip() for x in re.split(r"[\t,;]+", s) if x.strip()]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            else:
                arr = [x for x in s.split() if x]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            if not source:
                raise RuntimeError(f"第 {i} 行格式错误：私钥/助记词不能为空")
            target = self._validate_recipient_address(target, f"第 {i} 行接收地址")
            key = (source, target)
            if key in seen:
                continue
            seen.add(key)
            result.append(OnchainPairEntry(source=source, target=target))
        return result
    def _parse_address_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            s = self._validate_recipient_address(s, f"第 {i} 行接收地址")
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result
    def _parse_source_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result
    def _persist_import_rows(self) -> bool:
        try:
            self.store.multi_to_multi_drafts = [
                {
                    "source": str(item.get("source", "") or "").strip(),
                    "target": str(item.get("target", "") or "").strip(),
                }
                for item in self.m2m_import_drafts
                if str(item.get("source", "") or "").strip() or str(item.get("target", "") or "").strip()
            ]
            self.store.save_transfer_lists_only()
            return True
        except Exception as exc:
            self.log(f"导入数据自动保存失败：{exc}")
            messagebox.showerror("保存失败", f"导入数据自动保存失败：{exc}")
            return False
    def _import_rows(self, rows: list[OnchainPairEntry] | list[str], source_name: str):
        if not rows:
            messagebox.showwarning("提示", "没有可导入的数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M:
            created = self.store.upsert_multi_to_multi(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self._persist_import_rows()
            self.log(f"{source_name}导入完成：新增 {created} 条，已自动全选 {len(self.store.multi_to_multi_pairs)} 条")
            return
        if mode == self.MODE_1M:
            created = self.store.upsert_one_to_many_addresses(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self._persist_import_rows()
            self.log(f"{source_name}导入完成：新增 {created} 条地址，已自动全选 {len(self.store.one_to_many_addresses)} 条")
            return
        created = self.store.upsert_many_to_one_sources(rows)  # type: ignore[arg-type]
        self.checked_row_keys = set(self._active_row_keys())
        self._refresh_tree()
        self._persist_import_rows()
        self.log(f"{source_name}导入完成：新增 {created} 条钱包，已自动全选 {len(self.store.many_to_one_sources)} 条")
    def import_from_paste(self):
        self.import_from_clipboard()
    def import_from_clipboard(self):
        try:
            text = self.root.clipboard_get()
        except Exception:
            messagebox.showwarning("提示", "剪贴板为空或不可读取")
            return
        try:
            lines = text.splitlines()
            mode = self._mode()
            target = self._current_import_target()
            if mode == self.MODE_M2M and target != "full":
                self._import_m2m_column(target, lines, "剪贴板")
                return
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "剪贴板")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))
    def import_txt(self):
        from tkinter import filedialog

        path = filedialog.askopenfilename(title="选择 TXT", filetypes=[("Text", "*.txt"), ("All Files", "*.*")])
        if not path:
            return
        try:
            content = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = Path(path).read_text(encoding="utf-8-sig")
        try:
            lines = content.splitlines()
            mode = self._mode()
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "TXT")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))
    def export_txt(self):
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(title="导出 TXT", defaultextension=".txt", filetypes=[("Text", "*.txt")])
        if not path:
            return
        try:
            mode = self._mode()
            if mode == self.MODE_M2M:
                lines = [f"{x.source} {x.target}" for x in self.store.multi_to_multi_pairs]
            elif mode == self.MODE_1M:
                lines = list(self.store.one_to_many_addresses)
            else:
                lines = list(self.store.many_to_one_sources)
            Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            self.log(f"TXT 导出完成：{path}")
            messagebox.showinfo("导出完成", f"已导出 {len(lines)} 条")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc))
    def toggle_check_all(self):
        keys = set(self._active_row_keys())
        draft_rows = set(range(len(self.m2m_import_drafts))) if self._is_mode_m2m() else set()
        checked_draft_rows = self._ensure_checked_m2m_draft_rows() if self._is_mode_m2m() else set()
        if not keys and not draft_rows:
            messagebox.showwarning("提示", "当前模式暂无可操作数据")
            return
        if self.checked_row_keys == keys and checked_draft_rows == draft_rows:
            self.checked_row_keys.clear()
            checked_draft_rows.clear()
            self.log("已取消全选")
        else:
            self.checked_row_keys = set(keys)
            checked_draft_rows.clear()
            checked_draft_rows.update(draft_rows)
            self.log(f"已全选 {len(keys) + len(draft_rows)} 条")
        self._refresh_tree()
    def delete_selected(self):
        mode = self._mode()
        if mode == self.MODE_M2M:
            idxs = [i for i, item in enumerate(self.store.multi_to_multi_pairs) if self._m2m_key(item) in self.checked_row_keys]
            draft_idxs = sorted(self._ensure_checked_m2m_draft_rows())
        elif mode == self.MODE_1M:
            idxs = [i for i, target in enumerate(self.store.one_to_many_addresses) if self._one_to_many_key(target) in self.checked_row_keys]
            draft_idxs = []
        else:
            idxs = [i for i, source in enumerate(self.store.many_to_one_sources) if self._many_to_one_key(source) in self.checked_row_keys]
            draft_idxs = []
        if not idxs and not draft_idxs:
            messagebox.showwarning("提示", "请先勾选要删除的数据")
            return
        total = len(idxs) + len(draft_idxs)
        if not messagebox.askyesno("确认删除", f"确认删除 {total} 条数据吗？"):
            return

        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices(idxs)
            for draft_idx in sorted(draft_idxs, reverse=True):
                if 0 <= draft_idx < len(self.m2m_import_drafts):
                    self.m2m_import_drafts.pop(draft_idx)
            self._ensure_checked_m2m_draft_rows().clear()
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices(idxs)
        else:
            self.store.delete_many_to_one_by_indices(idxs)
        self._refresh_tree()
        if idxs or draft_idxs:
            self._persist_import_rows()
        self.log(f"已删除 {total} 条")
    def delete_current_row(self):
        draft_idx = self._selected_draft_index()
        if draft_idx is not None and self._is_mode_m2m():
            if not (0 <= draft_idx < len(self.m2m_import_drafts)):
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            if not messagebox.askyesno("确认删除", "确认删除当前待补齐数据吗？"):
                return
            self.m2m_import_drafts.pop(draft_idx)
            self._ensure_checked_m2m_draft_rows().clear()
            self._refresh_tree()
            self._persist_import_rows()
            self.log("已删除当前待补齐数据")
            return
        idx = self._single_selected_index()
        if idx is None:
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M and not (0 <= idx < len(self.store.multi_to_multi_pairs)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_1M and not (0 <= idx < len(self.store.one_to_many_addresses)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_M1 and not (0 <= idx < len(self.store.many_to_one_sources)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if not messagebox.askyesno("确认删除", "确认删除当前数据吗？"):
            return
        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices([idx])
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices([idx])
        else:
            self.store.delete_many_to_one_by_indices([idx])
        self._refresh_tree()
        self._persist_import_rows()
        self.log("已删除当前数据")
    def _apply_settings_to_store(self) -> bool:
        mode = self._mode()
        network = self.network_var.get().strip().upper()
        token = self._selected_token(with_message=False)
        amount_mode = self._amount_mode()
        amount = self.amount_var.get().strip()
        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        elif amount_mode == self.AMOUNT_MODE_FIXED:
            if not amount:
                messagebox.showerror("参数错误", "转账数量不能为空")
                return False
            try:
                amount_value = Decimal(amount)
                if amount_value <= 0:
                    raise InvalidOperation
                amount = self._decimal_to_text(amount_value)
            except Exception:
                messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
                return False
        random_min = self.random_min_var.get().strip()
        random_max = self.random_max_var.get().strip()
        if amount_mode == self.AMOUNT_MODE_RANDOM:
            try:
                random_min_value = Decimal(random_min)
                random_max_value = Decimal(random_max)
            except Exception:
                messagebox.showerror("参数错误", "随机金额最小值/最大值格式错误")
                return False
            if random_min_value <= 0 or random_max_value <= 0:
                messagebox.showerror("参数错误", "随机金额最小值和最大值必须大于 0")
                return False
            if random_max_value < random_min_value:
                messagebox.showerror("参数错误", "随机金额最大值必须大于或等于最小值")
                return False
            random_min = self._decimal_to_text(random_min_value)
            random_max = self._decimal_to_text(random_max_value)
        try:
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return False
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
        except Exception:
            messagebox.showerror("参数错误", "执行线程数格式错误")
            return False
        try:
            confirm_timeout_seconds = float(str(self.confirm_timeout_var.get()).strip())
            if confirm_timeout_seconds <= 0:
                raise ValueError
        except Exception:
            messagebox.showerror("参数错误", "确认超时必须是大于 0 的数字")
            return False
        proxy_text = self.onchain_proxy_var.get().strip()
        if self.use_config_proxy_var.get():
            try:
                proxy_text = self._normalize_onchain_proxy(proxy_text)
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return False
        self.onchain_proxy_var.set(proxy_text)
        relay_enabled = bool(self.relay_enabled_var.get()) if mode in {self.MODE_1M, self.MODE_M1} else False
        relay_fee_reserve = self.relay_fee_reserve_var.get().strip()
        if relay_enabled and amount_mode == self.AMOUNT_MODE_ALL and mode != self.MODE_M1:
            messagebox.showerror("参数错误", "启用中转时仅支持固定数量或随机数量，暂不支持“全部”")
            return False
        if relay_fee_reserve:
            try:
                relay_fee_reserve_value = Decimal(relay_fee_reserve)
                if relay_fee_reserve_value < 0:
                    raise InvalidOperation
                relay_fee_reserve = self._decimal_to_text(relay_fee_reserve_value)
            except Exception:
                messagebox.showerror("参数错误", "预留原生币手续费必须是大于等于 0 的数字")
                return False
            if relay_enabled and mode != self.MODE_M1 and relay_fee_reserve_value == 0:
                messagebox.showerror("参数错误", "预留原生币手续费必须是大于 0 的数字")
                return False
        elif relay_enabled:
            messagebox.showerror("参数错误", "启用中转后必须填写预留原生币手续费")
            return False
        current_mode_amount_config = {
            "amount_mode": amount_mode,
            "amount": amount,
            "random_min": random_min,
            "random_max": random_max,
        }
        current_mode_relay_config = {
            "relay_enabled": relay_enabled,
            "relay_fee_reserve": relay_fee_reserve,
        }
        self._store_mode_amount_config(mode, current_mode_amount_config)
        self._store_mode_relay_config(mode, current_mode_relay_config)
        normalized_mode_amount_config = self._normalize_mode_amount_config(mode, current_mode_amount_config)
        normalized_mode_relay_config = self._normalize_mode_relay_config(mode, current_mode_relay_config)
        existing_mode_amounts = getattr(getattr(self.store, "settings", None), "mode_amounts", {}) or {}
        existing_mode_relay_configs = getattr(getattr(self.store, "settings", None), "mode_relay_configs", {}) or {}
        self.store.settings = OnchainSettings(
            mode=mode,
            network=network,
            token_symbol=(token.symbol if token else ""),
            token_contract=(token.contract if token else ""),
            amount_mode=normalized_mode_amount_config["amount_mode"],
            amount=normalized_mode_amount_config["amount"],
            random_min=normalized_mode_amount_config["random_min"],
            random_max=normalized_mode_amount_config["random_max"],
            mode_amounts=self._mode_amounts_payload(
                existing_mode_amounts=existing_mode_amounts if isinstance(existing_mode_amounts, dict) else None,
                current_mode=mode,
                current_config=normalized_mode_amount_config,
            ),
            delay_seconds=delay,
            worker_threads=threads,
            confirm_timeout_seconds=confirm_timeout_seconds,
            dry_run=bool(self.dry_run_var.get()),
            use_config_proxy=bool(self.use_config_proxy_var.get()),
            proxy_url=proxy_text,
            one_to_many_source=self.source_credential_var.get().strip(),
            many_to_one_target="",
            relay_enabled=bool(normalized_mode_relay_config.get("relay_enabled")) if mode in self.MODE_RELAY_STORAGE_KEYS else False,
            relay_fee_reserve=normalized_mode_relay_config["relay_fee_reserve"] if mode in self.MODE_RELAY_STORAGE_KEYS else str(getattr(getattr(self.store, "settings", None), "relay_fee_reserve", "") or "").strip(),
            mode_relay_configs=self._mode_relay_configs_payload(
                existing_mode_relay_configs=existing_mode_relay_configs if isinstance(existing_mode_relay_configs, dict) else None,
                current_mode=mode,
                current_config=normalized_mode_relay_config if mode in self.MODE_RELAY_STORAGE_KEYS else None,
            ),
            relay_sweep_enabled=False,
            relay_sweep_target="",
        )
        target = self.target_address_var.get().strip()
        if target:
            try:
                safe_target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return False
            self.target_address_var.set(safe_target)
            self.store.settings.many_to_one_target = safe_target
        return True
    def save_all(self):
        if not self._apply_settings_to_store():
            return
        try:
            self.store.save_settings_only()
            self.log("链上批量转账配置已保存")
            messagebox.showinfo("成功", f"批量转账配置已保存到：{ONCHAIN_DATA_FILE}")
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))
    def _load_data(self):
        try:
            self.store.load()
            notice = str(getattr(self.store, "last_load_notice", "") or "").strip()
            st = self.store.settings
            self.m2m_import_drafts = [
                {
                    "source": str(item.get("source", "") or "").strip(),
                    "target": str(item.get("target", "") or "").strip(),
                }
                for item in getattr(self.store, "multi_to_multi_drafts", []) or []
                if str(item.get("source", "") or "").strip() or str(item.get("target", "") or "").strip()
            ]
            loaded_mode = st.mode if st.mode in {self.MODE_M2M, self.MODE_1M, self.MODE_M1} else self.MODE_M2M
            self._mode_amount_config_ready = False
            self._mode_relay_config_ready = False
            self._load_mode_amount_configs_from_settings(st)
            self._load_mode_relay_configs_from_settings(st)
            self.mode_var.set(loaded_mode)
            net = st.network if st.network in {"ETH", "BSC"} else ""
            self.network_var.set(net)
            if net:
                self._build_token_options(network=net, prefer_symbol=st.token_symbol, prefer_contract=st.token_contract)
            else:
                self.current_tokens = {}
                self.coin_box.configure(values=[])
                self.coin_var.set("")
            self.delay_var.set(st.delay_seconds)
            self.threads_var.set(str(max(1, int(st.worker_threads or 1))))
            self.confirm_timeout_var.set(self._decimal_to_text(Decimal(str(st.confirm_timeout_seconds or 180.0))))
            self.dry_run_var.set(st.dry_run)
            self.use_config_proxy_var.set(bool(st.use_config_proxy))
            self.onchain_proxy_var.set(st.proxy_url or "")
            self.source_credential_var.set(st.one_to_many_source or "")
            loaded_target = st.many_to_one_target or ""
            try:
                loaded_target = self._try_validate_recipient_address(loaded_target, "收款地址") or loaded_target
            except Exception:
                pass
            self.target_address_var.set(loaded_target)
            self.contract_search_var.set(st.token_contract or "")
            self.source_balance_var.set("-")
            self.target_balance_var.set("-")
            self.checked_row_keys = set(self._active_row_keys())
            self._mode_amount_config_ready = True
            self._mode_relay_config_ready = True
            self._apply_mode_amount_config(loaded_mode)
            self._apply_mode_relay_config(loaded_mode)
            self._last_mode_for_amounts = loaded_mode
            self._last_mode_for_relay = loaded_mode
            self._on_coin_changed()
            self._on_mode_changed()
            if notice:
                self.log(notice)
            self.log("链上配置加载完成")
        except Exception as exc:
            messagebox.showerror("加载失败", str(exc))
