#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Isolated reconciliation for exchange orders stored in the recovery journal."""

from __future__ import annotations

import hashlib
from decimal import Decimal


def exchange_operation_scope(api_key: str) -> str:
    digest = hashlib.sha256(str(api_key or "").encode("utf-8")).hexdigest()
    return f"account:{digest[:16]}"


def reconcile_exchange_orders(journal, client, api_key: str, *, allow_stock_tasks: bool = False) -> None:
    if journal is None:
        return
    if not allow_stock_tasks:
        for entry in journal.active_entries(kind="stock-task", scope=exchange_operation_scope(api_key)):
            payload = entry.get("payload", {})
            if payload.get("active") or payload.get("conversion") or payload.get("funding_transfer") or Decimal(str(payload.get("inventory", "0"))) != 0:
                raise RuntimeError("存在待核对股票划转、订单或持仓，请先恢复股票任务或停止清理")
    pending: list[str] = []
    for entry in journal.active_entries(kind="exchange-order", scope=exchange_operation_scope(api_key)):
        payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
        symbol = str(payload.get("symbol") or "").strip().upper()
        order_id = payload.get("order_id")
        client_order_id = str(payload.get("client_order_id") or "").strip()
        market = str(payload.get("market") or "spot")
        entry_id = str(entry.get("id") or "")
        if not symbol or (order_id in (None, "") and not client_order_id):
            pending.append(entry_id)
            continue
        try:
            if market == "um_futures":
                params = {"symbol": symbol}
                params["orderId" if order_id not in (None, "") else "origClientOrderId"] = order_id if order_id not in (None, "") else client_order_id
                order = client.request(client.um_futures, "GET", "/fapi/v1/order", params)
            else:
                order = client.get_order(symbol, order_id) if order_id not in (None, "") else client.get_order_by_client_id(symbol, client_order_id)
            status = str(order.get("status") or "").upper()
        except Exception as exc:
            # A crash can leave an intent that never reached the exchange. Use
            # the same query + cancel confirmation as live timeout cleanup.
            if getattr(exc, "code", None) == -2013 and client_order_id:
                cleanup = getattr(client, "get_pending_order_for_cleanup", None)
                if callable(cleanup):
                    try:
                        order = cleanup({**payload, "entry_id": entry_id, "confirmation_timed_out": True})
                        if order is None:
                            continue
                        status = str(order.get("status") or "").upper()
                    except Exception:
                        pending.append(entry_id)
                        continue
                else:
                    pending.append(entry_id)
                    continue
            else:
                pending.append(entry_id)
                continue
        if status == "FILLED":
            journal.update(entry_id, "completed", exchange_status=status)
        elif status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
            journal.update(entry_id, "cancelled", exchange_status=status)
        else:
            pending.append(entry_id)
    if pending:
        raise RuntimeError(f"检测到 {len(pending)} 个未完成交易所挂单，请先在交易所确认或撤销后再启动新任务")
