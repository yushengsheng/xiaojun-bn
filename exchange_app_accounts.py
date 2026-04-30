#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import (
    AccountEntry,
    Decimal,
    TOTAL_ASSET_RESULT_FILE,
    WITHDRAW_NETWORK_DEFAULT,
    WITHDRAW_SUCCESS_FILE,
    csv,
    filedialog,
    logger,
    messagebox,
    random,
    re,
    schedule_ui_callback,
    sys,
    threading,
    time,
    tk,
)


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
        line = f"{index}+{api_key}+{address}+{amount:.8f}\n"
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
        line = f"{index}+{api_key}+{total_dec:.8f}\n"

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
                "api_key": api_key,
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
        self._accounts_save_after_token = None
        store = getattr(self, "account_store", None)
        if store is None:
            return False
        try:
            entries = self._account_store_entries()
            if not entries and not store.file_path.exists():
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
        if not self.accounts and not self.account_store.file_path.exists():
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
        if self.accounts or self.account_store.file_path.exists():
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
        selected_count = sum(1 for acc in self.accounts if acc["selected_var"].get())
        logger.info("当前选中账号 %d 个，准备删除", selected_count)
        if selected_count <= 0:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        if not messagebox.askyesno("确认删除", f"确认删除已勾选的 {selected_count} 个账号吗？"):
            return
        current_context = getattr(self, "_context_account", None)
        keep = []
        for acc in self.accounts:
            if acc["selected_var"].get():
                tree_id = str(acc.get("tree_id") or "").strip()
                if tree_id:
                    self._account_tree_row_to_account.pop(tree_id, None)
                    try:
                        self.account_tree.delete(tree_id)
                    except Exception:
                        pass
            else:
                keep.append(acc)
        self.accounts = keep
        if current_context is not None and current_context not in keep:
            self._set_context_account(None)
        self._reindex_accounts()
        self._update_toggle_select_button_text()
        self._schedule_accounts_save()
        logger.info("已删除选中账号 %d 个，当前剩余 %d 个", selected_count, len(self.accounts))
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
