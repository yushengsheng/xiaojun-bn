#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import (
    AccountEntry,
    DATA_FILE,
    Decimal,
    EXCHANGE_OPERATION_JOURNAL_FILE,
    STRATEGY_CONFIG_FILE,
    TOTAL_ASSET_RESULT_FILE,
    WITHDRAW_NETWORK_DEFAULT,
    WITHDRAW_SUCCESS_FILE,
    csv,
    clear_exchange_log_queue,
    config_or_backup_exists,
    filedialog,
    logger,
    messagebox,
    random,
    re,
    purge_exchange_file_logs,
    schedule_ui_callback,
    sys,
    threading,
    time,
    tk,
)
from exchange_account_destruction import (
    destroy_exchange_account_artifacts,
    scrub_exchange_strategy_account_credentials,
)
from sensitive_data import mask_api_key


class ExchangeAppAccountsMixin(object):
    def _setup_account_list_mousewheel_bindings(self):
        # 账号列表区域内，鼠标滚轮可直接滚动，无需命中右侧滚动条
        self.bind_all("<MouseWheel>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-4>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-5>", self._on_account_list_mousewheel, add="+")
    def _pointer_in_account_list(self):
        frame = getattr(self, "frame_list_canvas", None)
        if frame is None or not frame.winfo_exists():
            return False
        try:
            x = self.winfo_pointerx()
            y = self.winfo_pointery()
            left = frame.winfo_rootx()
            top = frame.winfo_rooty()
            right = left + frame.winfo_width()
            bottom = top + frame.winfo_height()
            return left <= x <= right and top <= y <= bottom
        except Exception:
            return False
    def _on_account_list_mousewheel(self, event=None):
        if not self._pointer_in_account_list():
            return None
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return None

        units = 0
        num = getattr(event, "num", None)
        if num == 4:
            units = -1
        elif num == 5:
            units = 1
        else:
            delta = int(getattr(event, "delta", 0) or 0)
            if delta != 0:
                if sys.platform == "darwin":
                    units = -1 if delta > 0 else 1
                else:
                    units = -int(delta / 120) if abs(delta) >= 120 else (-1 if delta > 0 else 1)

        if units == 0:
            return None

        try:
            tree.yview_scroll(units, "units")
            return "break"
        except Exception:
            return None
    def _focus_account_list_for_paste(self, _event=None):
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return None
        try:
            tree.focus_set()
        except Exception:
            return None
        return None
    def _refresh_account_list_hint(self):
        hint = getattr(self, "account_list_hint", None)
        if hint is None:
            return
        if self.accounts:
            hint.place_forget()
            return
        hint.place(relx=0.5, rely=0.5, anchor="center")
    def _account_matches_search(self, acc: dict, query: str) -> bool:
        needle = str(query or "").strip().lower()
        if not needle:
            return False
        api_key = str(acc.get("api_key") or "").strip()
        address = str(acc.get("address") or "").strip()
        fields = (
            api_key,
            address,
            self._mask_key(api_key),
            self._mask_addr(address),
        )
        return any(needle in str(value or "").lower() for value in fields)
    def _selected_account_index(self) -> int | None:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return None
        try:
            selected = tree.selection()
        except Exception:
            selected = ()
        if not selected:
            return None
        acc = self._account_from_tree_row_id(selected[0])
        if acc not in self.accounts:
            return None
        try:
            return self.accounts.index(acc)
        except ValueError:
            return None
    def _focus_account_row(self, acc: dict) -> bool:
        tree = getattr(self, "account_tree", None)
        if tree is None or acc not in self.accounts:
            return False
        tree_id = str(acc.get("tree_id") or "").strip()
        if not tree_id:
            return False
        try:
            tree.selection_set(tree_id)
            tree.focus(tree_id)
            tree.see(tree_id)
            tree.focus_set()
            return True
        except Exception:
            return False
    def search_account_list(self, _event=None):
        query = str(getattr(self, "account_search_var", tk.StringVar()).get() or "").strip()
        result_var = getattr(self, "account_search_result_var", None)
        if not query:
            if result_var is not None:
                result_var.set("请输入 API KEY 或提现地址")
            entry = getattr(self, "account_search_entry", None)
            if entry is not None:
                try:
                    entry.focus_set()
                except Exception:
                    pass
            return "break"
        matches = [
            (idx, acc)
            for idx, acc in enumerate(self.accounts)
            if self._account_matches_search(acc, query)
        ]
        if not matches:
            if result_var is not None:
                result_var.set("未找到匹配账号")
            logger.info("账号搜索未找到匹配项")
            return "break"

        selected_index = self._selected_account_index()
        last_query = str(getattr(self, "_account_search_last_query", "") or "")
        start_index = 0
        if last_query == query and selected_index is not None:
            start_index = selected_index + 1
        target_pos, target_acc = next(((idx, acc) for idx, acc in matches if idx >= start_index), matches[0])
        self._account_search_last_query = query
        self._focus_account_row(target_acc)

        match_number = 1
        for pos, (idx, _acc) in enumerate(matches, start=1):
            if idx == target_pos:
                match_number = pos
                break
        try:
            account_no = str(target_acc.get("index_var").get() or "").strip()
        except Exception:
            account_no = str(target_pos + 1)
        if result_var is not None:
            result_var.set(f"找到 {match_number}/{len(matches)}：账号 #{account_no or target_pos + 1}")
        logger.info("账号搜索命中：账号 #%s（%d/%d）", account_no or target_pos + 1, match_number, len(matches))
        return "break"
    def clear_account_search(self):
        var = getattr(self, "account_search_var", None)
        if var is not None:
            var.set("")
        result_var = getattr(self, "account_search_result_var", None)
        if result_var is not None:
            result_var.set("")
        self._account_search_last_query = ""
        entry = getattr(self, "account_search_entry", None)
        if entry is not None:
            try:
                entry.focus_set()
            except Exception:
                pass
    def _clear_exchange_account_ui_state(self) -> None:
        self._cancel_after_token("_accounts_save_after_token")
        self._loading_accounts = True
        try:
            tree = getattr(self, "account_tree", None)
            if tree is not None:
                try:
                    tree.delete(*tree.get_children())
                except Exception:
                    pass
            self.accounts.clear()
            self._account_tree_row_to_account.clear()
            set_context_account = getattr(self, "_set_context_account", None)
            if callable(set_context_account):
                set_context_account(None)
            store = getattr(self, "account_store", None)
            if store is not None:
                store.accounts = []
                store.one_to_many_addresses = []
                store.one_to_many_source_api_key = ""
                store.one_to_many_source_api_secret = ""
            self.total_asset_results.clear()
            self._current_batch_summary = None
            self._last_batch_retry = None
            summary_var = getattr(self, "exchange_batch_summary_var", None)
            if summary_var is not None:
                summary_var.set("结果汇总：成功0 | 失败0 | 提现总额=- | 余额总额=-")
            for name in (
                "api_key_var",
                "api_secret_var",
                "withdraw_addr_var",
                "acc_api_key_var",
                "acc_api_secret_var",
                "acc_withdraw_addr_var",
            ):
                value_var = getattr(self, name, None)
                if value_var is not None:
                    value_var.set("")
            self.clear_account_search()
            try:
                self.clipboard_clear()
            except tk.TclError:
                pass
        finally:
            self._loading_accounts = False
        self._reindex_accounts()
        self._update_toggle_select_button_text()
        self._refresh_account_list_hint()

    def _clear_exchange_log_widgets(self) -> None:
        for name in ("text_log", "text_error_log"):
            widget = getattr(self, name, None)
            if widget is None:
                continue
            try:
                widget.configure(state="normal")
                widget.delete("1.0", "end")
            except Exception:
                pass
            finally:
                try:
                    widget.configure(state="disabled")
                except Exception:
                    pass

    def destroy_all_exchange_accounts(self) -> None:
        if self._batch_task_active or (self.worker_thread and self.worker_thread.is_alive()):
            messagebox.showwarning("无法销毁", "请先停止当前交易所任务后再销毁账号信息", parent=self)
            return
        warning = (
            "将直接销毁本机交易所账号、1对多来源账号、提现/资产结果、交易恢复账本、"
            "交易所运行日志及运行时代理临时文件。\n\n"
            "共享策略的交易参数和代理设置会保留，但其中 API Key、Secret 和提现地址会清空。\n\n"
            "此操作不会使用回收站，程序无法恢复。已导出的账号文件、系统备份、云同步副本不在本次范围。"
        )
        if not messagebox.askyesno("确认销毁所有", warning, parent=self):
            return
        try:
            scrub_exchange_strategy_account_credentials(STRATEGY_CONFIG_FILE)
        except Exception as exc:
            logger.error("销毁前清空策略账号凭据失败: %s", exc)
            messagebox.showerror("销毁失败", f"共享策略中的账号凭据无法安全清空，未执行销毁：{exc}", parent=self)
            return

        try:
            runtime = getattr(self, "exchange_proxy_runtime", None)
            if runtime is not None:
                runtime.stop()
            clear_current_client = getattr(self, "_clear_current_binance_client", None)
            if callable(clear_current_client):
                clear_current_client()
            report = destroy_exchange_account_artifacts(
                accounts_file=DATA_FILE,
                operation_journal_file=EXCHANGE_OPERATION_JOURNAL_FILE,
                withdraw_result_file=WITHDRAW_SUCCESS_FILE,
                total_asset_result_file=TOTAL_ASSET_RESULT_FILE,
                runtime_dir=DATA_FILE.parent,
            )
            report.merge(purge_exchange_file_logs())
            clear_exchange_log_queue()
            self._clear_exchange_account_ui_state()
            self._clear_exchange_log_widgets()
        except Exception as exc:
            logger.exception("销毁交易所账号信息时发生异常")
            messagebox.showerror("销毁异常", f"销毁过程中发生异常，请重启程序后检查：{exc}", parent=self)
            return

        if report.ok:
            logger.info("交易所账号信息已销毁；共享策略参数和代理设置已保留")
            messagebox.showinfo("销毁完成", "交易所账号相关信息已销毁；共享策略参数和代理设置已保留。", parent=self)
            return
        failed_names = "、".join(path.name for path in report.failed)
        logger.error("部分交易所账号文件未能销毁：%s", failed_names)
        messagebox.showwarning(
            "部分销毁失败",
            f"账号列表已从程序中清空，但以下文件未能确认销毁：{failed_names}\n请关闭程序后检查文件权限。",
            parent=self,
        )
    def _copy_account_field(self, acc: dict | None, field_name: str, field_label: str) -> None:
        if acc is None:
            messagebox.showinfo("提示", "请先右键选择一个账号")
            return
        value = str(acc.get(field_name) or "").strip()
        if not value:
            messagebox.showinfo("提示", f"当前账号没有可复制的{field_label}")
            return
        try:
            self.clipboard_clear()
            self.clipboard_append(value)
        except tk.TclError as exc:
            logger.error("复制%s失败: %s", field_label, exc)
            messagebox.showerror("错误", f"复制{field_label}失败：{exc}")
            return
        try:
            idx_text = str(acc.get("index_var").get() or "").strip()
        except Exception:
            idx_text = "?"
        logger.info("已复制账号 #%s 的%s到剪贴板", idx_text or "?", field_label)
    def random_sleep(self, min_ms, max_ms):
        if max_ms < min_ms:
            min_ms, max_ms = max_ms, min_ms
        delay_ms = random.randint(min_ms, max_ms)
        time.sleep(delay_ms / 1000.0)
    def _current_random_delay_seconds(self) -> float:
        try:
            min_ms = int(self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f"))
            max_ms = int(self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927"))
        except Exception:
            min_ms = 1000
            max_ms = 3000
        if max_ms < min_ms:
            min_ms, max_ms = max_ms, min_ms
        return max(0.0, random.randint(min_ms, max_ms) / 1000.0)
    def _mask_key(self, key, prefix=6, suffix=4):
        if len(key) <= prefix + suffix:
            return key
        return key[:prefix] + "..." + key[-suffix:]
    def _mask_addr(self, addr, prefix=6, suffix=4):
        if len(addr) <= prefix + suffix:
            return addr
        return addr[:prefix] + "..." + addr[-suffix:]
    def _should_handle_account_paste_shortcut(self, focus=None) -> bool:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return False
        try:
            if not tree.winfo_viewable():
                return False
        except Exception:
            return False
        if focus is tree:
            return True
        return self._pointer_in_account_list()
    def _on_paste_shortcut(self, event=None):
        focus = self.focus_get()
        if focus is not None:
            widget_class = str(focus.winfo_class())
            if widget_class in {"Entry", "TEntry", "Text", "TCombobox", "Spinbox", "TSpinbox"}:
                return None
        if not self._should_handle_account_paste_shortcut(focus):
            return None
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            return None

        parsed, err_msg = self._parse_accounts_from_text(raw)
        if parsed:
            self._import_accounts_from_text(raw, "剪贴板")
            return "break"

        if err_msg:
            messagebox.showerror("错误", f"剪贴板导入失败：{err_msg}")
            return "break"
        return None
    def _parse_accounts_from_text(self, raw_text):
        if raw_text is None:
            return [], "内容为空"

        normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
        accounts: list[tuple[str, str, str, str]] = []
        tokens: list[str] = []

        for raw_line in normalized.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    network = parts[3] if len(parts) >= 4 else ""
                    accounts.append((parts[0], parts[1], parts[2], network))
                    continue

            # 支持“行尾备注/中文说明”，只提取长 token（API KEY/SECRET/地址）
            found = re.findall(r"0x[a-fA-F0-9]{40}|[A-Za-z0-9]{24,}", line)
            if found:
                tokens.extend(found)

        if not accounts and not tokens:
            return [], "没有识别到账号数据"

        if tokens and len(tokens) % 3 != 0:
            return [], f"识别到 {len(tokens)} 条有效字段，必须按 3 条一组：APIKEY / APISECRET / 提现地址"

        for i in range(0, len(tokens), 3):
            key, secret, addr = tokens[i], tokens[i + 1], tokens[i + 2]
            accounts.append((key, secret, addr, ""))
        return accounts, ""
    def _import_accounts_from_text(self, raw_text, source_name):
        parsed, err_msg = self._parse_accounts_from_text(raw_text)
        if err_msg:
            messagebox.showerror("错误", f"{source_name}导入失败：{err_msg}")
            return 0

        existing_api_keys = {
            str(acc.get("api_key", "") or "").strip()
            for acc in self.accounts
            if str(acc.get("api_key", "") or "").strip()
        }
        seen_api_keys = set(existing_api_keys)
        deduped_accounts = []
        duplicate_count = 0
        for key, secret, addr, network in parsed:
            api_key = str(key or "").strip()
            if api_key in seen_api_keys:
                duplicate_count += 1
                continue
            seen_api_keys.add(api_key)
            deduped_accounts.append((key, secret, addr, network))

        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for key, secret, addr, network in deduped_accounts:
            self._append_account_row(key, secret, addr, network or net)

        self._reindex_accounts()
        self._focus_account_list_for_paste()
        logger.info("从%s导入账号数量：%d，重复 API 数量：%d", source_name, len(deduped_accounts), duplicate_count)
        if deduped_accounts:
            msg = f"从{source_name}导入账号数量：{len(deduped_accounts)}"
            if duplicate_count:
                msg += f"\n已跳过重复 API：{duplicate_count}"
            messagebox.showinfo("成功", msg)
        else:
            messagebox.showinfo("提示", f"{source_name}没有新增账号，全部为重复 API（重复 {duplicate_count} 个）")
        return len(deduped_accounts)
    def record_withdraw(self, index, api_key, address, amount):
        api_key_masked = mask_api_key(api_key)
        line = f"{index}+{api_key_masked}+{address}+{amount:.8f}\n"
        try:
            WITHDRAW_SUCCESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._result_file_lock:
                with open(WITHDRAW_SUCCESS_FILE, "a", encoding="utf-8") as f:
                    f.write(line)
            logger.info("已记录提现到 %s：%s", WITHDRAW_SUCCESS_FILE, line.strip())
        except Exception as e:
            logger.error("写入提现记录文件失败: %s", e)
    def record_total_asset(self, index, api_key, address, network, total_usdt):
        total_dec = Decimal(str(total_usdt))
        api_key_masked = mask_api_key(api_key)
        line = f"{index}+{api_key_masked}+{total_dec:.8f}\n"

        try:
            TOTAL_ASSET_RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._result_file_lock:
                with open(TOTAL_ASSET_RESULT_FILE, "a", encoding="utf-8") as f:
                    f.write(line)
            logger.info("已记录总资产到 %s：%s", TOTAL_ASSET_RESULT_FILE, line.strip())
        except Exception as e:
            logger.error("写入总资产记录文件失败: %s", e)
        with self._result_file_lock:
            self.total_asset_results[index] = {
                "index": index,
                "api_key": api_key_masked,
                "address": address,
                "network": network,
                "total_usdt": total_dec,
            }
    def export_total_asset_csv(self):
        if not self.total_asset_results:
            messagebox.showinfo("提示", "当前没有可导出的总资产结果，请先运行一次“批量查询总资产”")
            return

        path = filedialog.asksaveasfilename(
            title="导出总资产 CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            rows = sorted(self.total_asset_results.values(), key=lambda x: x["index"])

            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "API_KEY", "提现地址", "网络", "总资产(USDT)"])
                for row in rows:
                    writer.writerow([
                        row["index"],
                        row["api_key"],
                        row["address"],
                        row["network"],
                        f'{row["total_usdt"]:.8f}',
                    ])

            logger.info("总资产 CSV 已导出到：%s", path)
            messagebox.showinfo("成功", f"总资产 CSV 已导出到：\n{path}")
        except Exception as e:
            logger.error("导出总资产 CSV 失败: %s", e)
            messagebox.showerror("错误", "导出总资产 CSV 失败: %s" % e)
    @staticmethod
    def _normalize_account_network_text(value: str, *, fallback: str = WITHDRAW_NETWORK_DEFAULT) -> str:
        text = str(value or "").strip().upper()
        return text or str(fallback or WITHDRAW_NETWORK_DEFAULT).strip().upper() or WITHDRAW_NETWORK_DEFAULT
    def _account_network_value(self, acc: dict, *, fallback: str | None = None) -> str:
        network_var = acc.get("network_var")
        if network_var is not None:
            try:
                raw = network_var.get()
            except Exception:
                raw = acc.get("network")
        else:
            raw = acc.get("network")
        if fallback is None:
            fallback = self.acc_network_var.get() if hasattr(self, "acc_network_var") else WITHDRAW_NETWORK_DEFAULT
        return self._normalize_account_network_text(raw, fallback=fallback)
    def _sync_account_network_var(self, acc: dict) -> None:
        network_var = acc.get("network_var")
        if network_var is None:
            acc["network"] = self._account_network_value(acc)
            return
        try:
            current_value = str(network_var.get() or "").strip().upper()
        except Exception:
            current_value = str(acc.get("network") or "").strip().upper()
        normalized = self._normalize_account_network_text(current_value)
        if current_value != normalized:
            try:
                network_var.set(normalized)
            except Exception:
                acc["network"] = normalized
            return
        if str(acc.get("network") or "").strip().upper() != normalized:
            acc["network"] = normalized
        self._refresh_account_tree_row(acc)
        if not self._loading_accounts:
            self._schedule_accounts_save()
    def _account_store_entries(self) -> list[AccountEntry]:
        entries: list[AccountEntry] = []
        for acc in self.accounts:
            api_key = str(acc.get("api_key") or "").strip()
            api_secret = str(acc.get("api_secret") or "").strip()
            address = str(acc.get("address") or "").strip()
            if not api_key or not api_secret or not address:
                continue
            entries.append(
                AccountEntry(
                    api_key=api_key,
                    api_secret=api_secret,
                    address=address,
                    network=self._account_network_value(acc),
                )
            )
        return entries
    def _save_accounts_silently(self) -> bool:
        self._cancel_after_token("_accounts_save_after_token")
        store = getattr(self, "account_store", None)
        if store is None:
            return False
        try:
            entries = self._account_store_entries()
            if not entries and not config_or_backup_exists(store.file_path):
                return False
            store.accounts = entries
            store.settings.network = self._normalize_account_network_text(self.acc_network_var.get())
            store.save()
            return True
        except Exception:
            logger.exception("保存交易所账号列表失败")
            return False
    def _schedule_accounts_save(self) -> None:
        if self._loading_accounts:
            return
        if not self.accounts and not config_or_backup_exists(self.account_store.file_path):
            return
        if self._closing:
            self._save_accounts_silently()
            return
        if self._accounts_save_after_token is not None:
            return
        try:
            self._accounts_save_after_token = self.after(150, self._save_accounts_silently)
        except Exception:
            self._accounts_save_after_token = None
            self._save_accounts_silently()
    def _load_accounts(self) -> None:
        store = getattr(self, "account_store", None)
        if store is None:
            return
        self._loading_accounts = True
        try:
            store.load()
            if store.last_load_notice:
                logger.warning(store.last_load_notice)
            loaded_any = False
            fallback_network = self.acc_network_var.get()
            for item in store.accounts:
                self._append_account_row(
                    item.api_key,
                    item.api_secret,
                    item.address,
                    self._normalize_account_network_text(item.network, fallback=fallback_network),
                    selected=True,
                )
                loaded_any = True
            if loaded_any:
                self._reindex_accounts()
                self._focus_account_list_for_paste()
                logger.info("已加载交易所账号列表：%s（共 %d 个）", store.file_path, len(store.accounts))
        except Exception as exc:
            logger.error("加载交易所账号列表失败: %s", exc)
            messagebox.showwarning("提示", f"交易所账号列表加载失败：{exc}")
        finally:
            self._loading_accounts = False
    def _reindex_accounts(self):
        for i, acc in enumerate(self.accounts, start=1):
            acc["index_var"].set(str(i))
            self._refresh_account_tree_row(acc)
        self._refresh_account_list_hint()
    def _on_global_network_changed(self, *_):
        self.apply_network_to_all_accounts()
    def apply_network_to_all_accounts(self):
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for acc in self.accounts:
            acc["network"] = net
            if "network_var" in acc:
                acc["network_var"].set(net)
            self._refresh_account_tree_row(acc)
        if self.accounts or config_or_backup_exists(self.account_store.file_path):
            self._schedule_accounts_save()
    @staticmethod
    def _account_row_color_by_status(status_text: str) -> str:
        s = str(status_text or "").strip()
        if not s or s == "就绪":
            return "#f2f2f2"
        if "未到账" in s:
            return "#ffe3b8"
        if any(k in s for k in ("已停止", "已请求停止")):
            return "#ffe3b8"
        if s.startswith("×") or any(k in s for k in ("失败", "异常")):
            return "#f8c7c7"
        if s.startswith("✔") or any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "#cfeecf"
        return "#cfe3ff"
    def _is_context_account(self, acc: dict) -> bool:
        return bool(acc is not None and acc is getattr(self, "_context_account", None))
    def _account_row_style_tag(self, acc: dict) -> str:
        if self._is_context_account(acc):
            return "acc_context"
        s = str(acc.get("status_var").get() if acc.get("status_var") is not None else "").strip()
        if not s or s == "就绪":
            return "acc_ready"
        if "未到账" in s or any(k in s for k in ("已停止", "已请求停止")):
            return "acc_warn"
        if s.startswith("×") or any(k in s for k in ("失败", "异常")):
            return "acc_failed"
        if s.startswith("✔") or any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "acc_success"
        return "acc_running"
    def _account_tree_values(self, acc: dict) -> tuple[str, str, str, str, str, str]:
        checked = "✓" if bool(acc.get("selected_var").get()) else ""
        index_text = str(acc.get("index_var").get() or "")
        api_key = self._mask_key(str(acc.get("api_key") or ""))
        address = self._mask_addr(str(acc.get("address") or ""))
        network = self._account_network_value(acc)
        status = str(acc.get("status_var").get() or "")
        return checked, index_text, api_key, address, network, status
    def _refresh_account_tree_row(self, acc: dict) -> None:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return
        tree_id = str(acc.get("tree_id") or "").strip()
        if not tree_id:
            return
        try:
            tree.item(tree_id, values=self._account_tree_values(acc), tags=(self._account_row_style_tag(acc),))
        except Exception:
            pass
    def _insert_account_tree_row(self, acc: dict) -> None:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return
        tree_id = tree.insert("", "end", values=self._account_tree_values(acc), tags=(self._account_row_style_tag(acc),))
        acc["tree_id"] = tree_id
        self._account_tree_row_to_account[tree_id] = acc
    def _account_from_tree_row_id(self, row_id: str) -> dict | None:
        return self._account_tree_row_to_account.get(str(row_id or "").strip())
    def _apply_account_row_style(self, acc: dict):
        self._refresh_account_tree_row(acc)
    def _set_account_status(self, acc: dict, text: str):
        status_text = str(text)
        status_var = acc.get("status_var")
        if status_var is None:
            return
        try:
            current = str(status_var.get() or "")
        except Exception:
            current = ""
        if current == status_text:
            return
        status_var.set(status_text)
        self._apply_account_row_style(acc)
    def _schedule_account_status(self, acc: dict, text: str) -> None:
        if self._closing:
            return
        schedule_ui_callback(
            self,
            f"account-status:{id(acc)}",
            lambda acc_ref=acc, status_text=str(text): self._set_account_status(acc_ref, status_text),
            root=self,
        )
    def _append_account_row(self, key, secret, addr, net, selected=True):
        net = self._normalize_account_network_text(net)
        index_var = tk.StringVar(value=str(len(self.accounts) + 1))
        selected_var = tk.BooleanVar(value=selected)
        network_var = tk.StringVar(value=net)
        status_var = tk.StringVar(value="就绪")
        acc = {
            "index_var": index_var,
            "selected_var": selected_var,
            "network_var": network_var,
            "status_var": status_var,
            "stop_event": threading.Event(),
            "batch_active": False,
            "api_key": key,
            "api_secret": secret,
            "address": addr,
            "network": net,
            "tree_id": "",
        }
        selected_var.trace_add("write", lambda *_args, a=acc: (self._update_toggle_select_button_text(), self._refresh_account_tree_row(a)))
        network_var.trace_add("write", lambda *_args, a=acc: self._sync_account_network_var(a))
        self.accounts.append(acc)
        self._insert_account_tree_row(acc)
        self._update_toggle_select_button_text()
        self._refresh_account_list_hint()
        if not self._loading_accounts:
            self._schedule_accounts_save()
        return acc
    def add_account_to_list(self):
        key = self.acc_api_key_var.get().strip()
        secret = self.acc_api_secret_var.get().strip()
        addr = self.acc_withdraw_addr_var.get().strip()
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT

        if not key or not secret or not addr:
            messagebox.showerror("错误", "请完整填写：API KEY / SECRET / 提现地址")
            return

        self._append_account_row(key, secret, addr, net)
        self._reindex_accounts()
        self._update_toggle_select_button_text()

        self.acc_api_key_var.set("")
        self.acc_api_secret_var.set("")
        self.acc_withdraw_addr_var.set("")
    def delete_selected_accounts(self):
        selected_accounts = [acc for acc in self.accounts if acc["selected_var"].get()]
        selected_count = len(selected_accounts)
        logger.info("当前选中账号 %d 个，准备删除", selected_count)
        if selected_count <= 0:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        removed_count = self._delete_accounts(
            selected_accounts,
            require_confirm=True,
            confirm_text=f"确认删除已勾选的 {selected_count} 个账号吗？",
        )
        if removed_count <= 0:
            return
        logger.info("已删除选中账号 %d 个，当前剩余 %d 个", removed_count, len(self.accounts))
    def _delete_accounts(
        self,
        accounts_to_delete: list[dict],
        *,
        require_confirm: bool = True,
        confirm_title: str = "确认删除",
        confirm_text: str = "",
    ) -> int:
        targets = [acc for acc in accounts_to_delete if acc in self.accounts]
        if not targets:
            return 0
        if require_confirm:
            text = str(confirm_text or "").strip() or f"确认删除 {len(targets)} 个账号吗？"
            if not messagebox.askyesno(confirm_title, text):
                return 0
        current_context = getattr(self, "_context_account", None)
        target_ids = {id(acc) for acc in targets}
        keep: list[dict] = []
        removed_count = 0
        for acc in self.accounts:
            if id(acc) in target_ids:
                tree_id = str(acc.get("tree_id") or "").strip()
                if tree_id:
                    self._account_tree_row_to_account.pop(tree_id, None)
                    try:
                        self.account_tree.delete(tree_id)
                    except Exception:
                        pass
                removed_count += 1
            else:
                keep.append(acc)
        self.accounts = keep
        if current_context is not None and current_context not in keep:
            self._set_context_account(None)
        self._reindex_accounts()
        self._update_toggle_select_button_text()
        self._schedule_accounts_save()
        return removed_count
    def select_all_accounts(self):
        total_count = len(self.accounts)
        for acc in self.accounts:
            acc["selected_var"].set(True)
        self._update_toggle_select_button_text()
        logger.info("已选中账号 %d 个", total_count)
    def deselect_all_accounts(self):
        selected_count = sum(1 for acc in self.accounts if acc["selected_var"].get())
        for acc in self.accounts:
            acc["selected_var"].set(False)
        self._update_toggle_select_button_text()
        logger.info("已取消选中账号 %d 个", selected_count)
    def toggle_select_all_accounts(self):
        if self.accounts and all(acc["selected_var"].get() for acc in self.accounts):
            self.deselect_all_accounts()
        else:
            self.select_all_accounts()
    def _update_toggle_select_button_text(self):
        btn = getattr(self, "btn_toggle_select_accounts", None)
        if btn is None:
            return
        all_selected = bool(self.accounts) and all(acc["selected_var"].get() for acc in self.accounts)
        btn.config(text="取消全选" if all_selected else "全选")
    def export_accounts(self):
        if not self.accounts:
            messagebox.showinfo("提示", "当前账号列表为空，无需导出")
            return

        path = filedialog.asksaveasfilename(
            title="导出账号列表到文件",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            with open(path, "w", encoding="utf-8") as f:
                for acc in self.accounts:
                    line = "|".join([
                        acc["api_key"],
                        acc["api_secret"],
                        acc["address"],
                        self._account_network_value(acc),
                    ])
                    f.write(line + "\n")
            logger.info("账号列表已导出到文件：%s", path)
            messagebox.showinfo("成功", f"账号列表已导出到：\n{path}")
        except Exception as e:
            logger.error("导出账号列表失败: %s", e)
            messagebox.showerror("错误", "导出账号列表失败: %s" % e)
    def import_accounts(self):
        path = filedialog.askopenfilename(
            title="导入账号列表文件",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            try:
                with open(path, "r", encoding="utf-8-sig") as f:
                    raw = f.read()
            except UnicodeDecodeError:
                with open(path, "r", encoding="gb18030", errors="ignore") as f:
                    raw = f.read()

            self._import_accounts_from_text(raw, "文件")
        except Exception as e:
            logger.error("导入账号列表失败: %s", e)
            messagebox.showerror("错误", "导入账号列表失败: %s" % e)
    def import_accounts_from_clipboard(self):
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            messagebox.showerror("错误", "剪贴板内容不可用或为空")
            return

        self._import_accounts_from_text(raw, "剪贴板")
    def _get_selected_accounts(self):
        return [acc for acc in self.accounts if acc["selected_var"].get()]
