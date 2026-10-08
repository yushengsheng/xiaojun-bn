"""Durable uncertainty guard for non-idempotent exchange money movements."""
from __future__ import annotations

import time
import uuid
import re
from decimal import Decimal

from exchange_order_recovery import exchange_operation_scope


FUNDS_WRITE_PATHS = frozenset({
    "/sapi/v1/capital/withdraw/apply", "/sapi/v1/asset/transfer",
    "/sapi/v1/futures/transfer", "/sapi/v1/simple-earn/flexible/subscribe",
    "/sapi/v1/simple-earn/flexible/redeem", "/sapi/v1/convert/acceptQuote",
    "/sapi/v1/asset/dust",
})
_RECEIPT_KEYS = {
    "/sapi/v1/capital/withdraw/apply": "id",
    "/sapi/v1/asset/transfer": "tranId",
    "/sapi/v1/futures/transfer": "tranId",
    "/sapi/v1/simple-earn/flexible/subscribe": "purchaseId",
    "/sapi/v1/simple-earn/flexible/redeem": "redeemId",
    "/sapi/v1/convert/acceptQuote": "orderId",
}


def _receipt_id(value):
    return (type(value) is int and value > 0
            or isinstance(value, str) and value.strip() not in {"", "0"})


def _same_address(left, right):
    left, right = str(left), str(right)
    if re.fullmatch(r"0x[0-9a-fA-F]{40}", left) and re.fullmatch(r"0x[0-9a-fA-F]{40}", right):
        return left.lower() == right.lower()
    return left == right


def pending_funds(journal, api_key):
    if journal is None:
        return []
    return journal.active_entries(kind="exchange-funds", scope=exchange_operation_scope(api_key))


def record_convert_receipt(journal, entry, row):
    params = entry.get("payload", {}).get("params", {})
    status = str(row.get("orderStatus", "")).upper() if isinstance(row, dict) else ""
    if isinstance(row, dict) and row.get("quoteId") and str(row["quoteId"]) != str(params.get("quoteId")):
        raise RuntimeError("闪兑核对记录与原请求不符")
    original_id = entry.get("payload", {}).get("receipt", {}).get("orderId")
    if _receipt_id(original_id) and isinstance(row, dict) and str(row.get("orderId")) != str(original_id):
        raise RuntimeError("闪兑核对回执编号与原订单不符")
    if status not in {"SUCCESS", "FAIL", "FAILED"} or not _receipt_id(row.get("orderId")):
        return False
    journal.update(entry["id"], "completed" if status == "SUCCESS" else "failed", receipt_id=row["orderId"], reconciled_by="convert-status")
    return True


def reconcile_funds_entry(journal, client, entry):
    """Only an identifiable exchange receipt can automatically clear a guard."""
    payload = entry.get("payload", {})
    params = payload.get("params", {})
    path = payload.get("path", "")
    if path == "/sapi/v1/capital/withdraw/apply" and params.get("withdrawOrderId"):
        rows = client.request_query(client.spot, "GET", "/sapi/v1/capital/withdraw/history",
                                    {"withdrawOrderId": params["withdrawOrderId"], "startTime": payload["started_ms"] - 1000},
                                    timeout=6, max_retries=1)
        matches = [row for row in rows if isinstance(row, dict) and row.get("withdrawOrderId") == params["withdrawOrderId"]] if isinstance(rows, list) else []
        if len(matches) == 1:
            row = matches[0]
            if (str(row.get("coin", "")).upper() != str(params.get("coin", "")).upper()
                    or not _same_address(row.get("address", ""), params.get("address", ""))
                    or Decimal(str(row.get("amount"))) != Decimal(str(params.get("amount")))
                    or params.get("network") and row.get("network") != params["network"]):
                raise RuntimeError("提现核对记录与原请求不符")
            status = row.get("status")
            if type(status) is not int or status not in {0, 2, 3, 4, 6} or not _receipt_id(row.get("id")):
                return False
            journal.update(entry["id"], "failed" if status in {2, 3} else "completed", receipt_id=row["id"], reconciled_by="withdraw-history", withdrawal_status=status)
            return True
    elif path == "/sapi/v1/convert/acceptQuote" and params.get("quoteId"):
        return record_convert_receipt(journal, entry, client.get_convert_order_status(quote_id=params["quoteId"]))
    return False


def assert_funds_clear(journal, client):
    from exchange_binance_client import UnconfirmedOperationError
    remaining = []
    for entry in pending_funds(journal, client.key):
        try:
            if reconcile_funds_entry(journal, client, entry):
                continue
        except Exception:
            pass
        remaining.append(entry)
    if remaining:
        raise UnconfirmedOperationError(f"存在 {len(remaining)} 笔资金操作待核对，已阻止重复提交；请使用“核对未确认”处理")
    if getattr(client, "_funds_uncertainty_entry_id", ""):
        client._uncertain_write_error = None
        client._funds_uncertainty_entry_id = ""


def run_funds_request(client, base, path, params, submit):
    from exchange_binance_client import BinanceAPIError, RequestStoppedError, UnconfirmedOperationError
    journal = client.operation_journal
    assert_funds_clear(journal, client)
    if getattr(client, "_uncertain_write_error", None) is not None:
        raise client._uncertain_write_error
    values = dict(params or {})
    if path == "/sapi/v1/capital/withdraw/apply":
        values.setdefault("withdrawOrderId", "xjf" + uuid.uuid4().hex)
    entry_id = journal.begin("exchange-funds", exchange_operation_scope(client.key),
                             {"path": path, "params": values, "started_ms": int(time.time() * 1000)})
    try:
        result = submit(values)
    except (BinanceAPIError, RequestStoppedError) as exc:
        unknown = client._is_uncertain_write_error(exc)
        if unknown:
            client._funds_uncertainty_entry_id = entry_id
        journal.update(entry_id, "unconfirmed" if unknown else "failed", reason=str(exc))
        raise
    except Exception as exc:
        client._funds_uncertainty_entry_id = entry_id
        journal.update(entry_id, "unconfirmed", reason=str(exc))
        raise UnconfirmedOperationError(f"{path} 结果未确认，已保存核对记录，禁止重复提交") from exc
    if isinstance(result, dict) and result.get("success") is False:
        journal.update(entry_id, "failed", reason="exchange-rejected")
        raise RuntimeError("资金操作被交易所拒绝")
    try:
        valid = isinstance(result, dict) and _receipt_id(result.get(_RECEIPT_KEYS.get(path, "")))
        if path == "/sapi/v1/asset/dust" and isinstance(result, dict):
            rows = result.get("transferResult")
            valid = isinstance(rows, list) and bool(rows) and all(isinstance(row, dict) and _receipt_id(row.get("tranId")) for row in rows)
        if not valid:
            journal.update(entry_id, "unconfirmed", reason="invalid-receipt")
            raise UnconfirmedOperationError("资金操作未返回有效回执，已保留核对记录")
        state = "completed"
        if path == "/sapi/v1/convert/acceptQuote":
            status = str(result.get("orderStatus", "")).upper()
            state = "completed" if status == "SUCCESS" else "failed" if status in {"FAIL", "FAILED"} else "submitted"
        journal.update(entry_id, state, receipt={key: result[key] for key in ("id", "tranId", "purchaseId", "redeemId", "orderId", "orderStatus", "transferResult") if key in result})
    except Exception as exc:
        client._funds_uncertainty_entry_id = entry_id
        client._uncertain_write_error = UnconfirmedOperationError("资金操作结果未确认或回执未能完整保存，须核对后继续")
        raise client._uncertain_write_error from exc
    return result
