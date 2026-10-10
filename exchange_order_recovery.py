#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Exchange account scope and order reply identity validation."""

from __future__ import annotations

import hashlib


def exchange_operation_scope(api_key: str) -> str:
    digest = hashlib.sha256(str(api_key or "").encode("utf-8")).hexdigest()
    return f"account:{digest[:16]}"


def validate_order_identity(order, *, symbol, order_id=None, client_order_id="", side=""):
    """An unrelated terminal order must never release another order's guard."""
    if not isinstance(order, dict) or not order.get("orderId"):
        raise ValueError("订单回复缺少 orderId，按失败处理")
    returned_client_id = str(order.get("origClientOrderId") or order.get("clientOrderId") or "")
    if (str(order.get("symbol", "")).upper() != str(symbol).upper()
            or order_id not in (None, "") and str(order.get("orderId")) != str(order_id)
            or client_order_id and (returned_client_id or order_id in (None, "")) and returned_client_id != str(client_order_id)
            or order.get("side") and side and str(order["side"]).upper() != str(side).upper()):
        raise RuntimeError("订单回复的交易对、订单编号、客户端标识或方向与原请求不匹配，按失败处理")
