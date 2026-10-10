"""Record exchange money movements; retry a missing result only once."""
from __future__ import annotations

import time
import uuid

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


def run_funds_request(client, base, path, params, submit):
    from exchange_binance_client import retry_failed_operation
    with client.operation_journal.operation_lock("exchange-funds", exchange_operation_scope(client.key)):
        return retry_failed_operation(client, lambda: _run_funds_request_locked(client, base, path, params, submit))


def _run_funds_request_locked(client, base, path, params, submit):
    from exchange_binance_client import BinanceAPIError, RequestStoppedError, ExchangeOperationFailedError
    journal = client.operation_journal
    values = dict(params or {})
    if path == "/sapi/v1/capital/withdraw/apply":
        values.setdefault("withdrawOrderId", "xjf" + uuid.uuid4().hex)
    entry_id = journal.begin("exchange-funds", exchange_operation_scope(client.key),
                             {"path": path, "params": values, "started_ms": int(time.time() * 1000)})
    try:
        result = submit(values)
    except (BinanceAPIError, RequestStoppedError) as exc:
        journal.update(entry_id, "failed", reason=str(exc))
        raise
    except Exception as exc:
        journal.update(entry_id, "failed", reason=str(exc))
        if isinstance(exc, ExchangeOperationFailedError):
            raise
        raise ExchangeOperationFailedError(f"{path} 无法确认结果，按失败处理") from exc
    if isinstance(result, dict) and result.get("success") is False:
        journal.update(entry_id, "failed", reason="exchange-rejected")
        raise RuntimeError("资金操作被交易所拒绝")
    valid = isinstance(result, dict) and _receipt_id(result.get(_RECEIPT_KEYS.get(path, "")))
    if path == "/sapi/v1/asset/dust" and isinstance(result, dict):
        rows = result.get("transferResult")
        valid = isinstance(rows, list) and bool(rows) and all(isinstance(row, dict) and _receipt_id(row.get("tranId")) for row in rows)
    if not valid:
        journal.update(entry_id, "failed", reason="invalid-receipt")
        raise ExchangeOperationFailedError("资金操作未返回有效回执，按失败处理")
    state = "completed"
    if path == "/sapi/v1/convert/acceptQuote":
        status = str(result.get("orderStatus", "")).upper()
        state = "completed" if status == "SUCCESS" else "failed" if status in {"FAIL", "FAILED"} else "submitted"
    try:
        journal.update(entry_id, state, receipt={key: result[key] for key in ("id", "tranId", "purchaseId", "redeemId", "orderId", "orderStatus", "transferResult") if key in result})
    except Exception as exc:
        # A usable reply was received: a local storage failure is not a missing-result retry.
        raise ExchangeOperationFailedError("资金操作回执保存失败", retryable=False) from exc
    if state == "submitted":
        if not hasattr(client, "_convert_journal_entries"):
            client._convert_journal_entries = {}
        client._convert_journal_entries[str(values.get("quoteId", ""))] = entry_id
        if not hasattr(client, "_convert_expected_receipts"):
            client._convert_expected_receipts = {}
        client._convert_expected_receipts[str(values.get("quoteId", ""))] = str(result["orderId"])
    return result
