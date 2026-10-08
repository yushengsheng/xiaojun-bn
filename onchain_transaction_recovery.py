"""Persist signed transactions before broadcast; recovery always reuses the bytes."""
from __future__ import annotations

from contextlib import nullcontext
import re
import time


class UnconfirmedTransactionError(RuntimeError):
    def __init__(self, message, tx_hash, nonce=None, entry_id="", *, own_transaction=True):
        super().__init__(message)
        self.tx_hash = tx_hash
        self.nonce = nonce
        self.entry_id = entry_id
        self.own_transaction = own_transaction


def transaction_context(client, **metadata):
    manager = getattr(client, "transaction_context", None)
    return manager(**metadata) if callable(manager) else nullcontext()


def scope(network, source):
    return f"{network.upper()}:{source.lower()}"


def _transfer_intent(tx):
    """Nonce and fee quotes can change on retry; payment content cannot."""
    return (
        str(tx.get("to") or "").lower(), int(tx.get("value", 0)),
        str(tx.get("data") or tx.get("input") or "0x").lower(),
        tx.get("chainId"),
    )


def verify_transfer_receipt(receipt, tx, source_address, *, tx_hash):
    """Require the actual recipient/asset/amount, not just a successful call."""
    if (not isinstance(receipt, dict)
            or str(receipt.get("transactionHash", "")).lower() != str(tx_hash).lower()
            or str(receipt.get("status")) not in {"0x1", "1"}
            or str(receipt.get("from", "")).lower() != str(source_address).lower()
            or str(receipt.get("to", "")).lower() != str(tx.get("to", "")).lower()):
        raise RuntimeError("交易回执与本次转账的哈希、转出地址或收款地址不符，须核对原交易")
    data = str(tx.get("data") or tx.get("input") or "0x").lower()
    if data in {"0x", ""}:
        return
    if not re.fullmatch(r"0xa9059cbb[0-9a-f]{128}", data):
        raise RuntimeError("无法核验转账调用内容，须核对原交易")
    target = "0x" + data[34:74]
    amount = int(data[74:], 16)
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    expected_topics = [transfer_topic, "0x" + str(source_address)[2:].lower().rjust(64, "0"),
                       "0x" + target[2:].rjust(64, "0")]
    for log in receipt.get("logs") or []:
        if (isinstance(log, dict) and not log.get("removed")
                and str(log.get("address", "")).lower() == str(tx["to"]).lower()
                and [str(topic).lower() for topic in log.get("topics", [])] == expected_topics
                and re.fullmatch(r"0x[0-9a-fA-F]{64}", str(log.get("data", "")))
                and int(log["data"], 16) == amount):
            return
    raise RuntimeError("交易回执未包含本次代币、收款地址和金额的 Transfer 记录，禁止判定转账成功")


def resume_entry(client, entry, *, recovery_timeout_seconds=8):
    from api_clients import PreparedSignedTransaction
    p = entry["payload"]
    prepared = PreparedSignedTransaction(p["tx"], p["raw_transaction"], p["tx_hash"])
    metadata = dict(getattr(getattr(client, "_transaction_context", None), "metadata", {}))
    metadata.update({k: p[k] for k in ("batch_id", "task_key", "amount_text", "coin") if k in p})
    with transaction_context(client, **metadata):
        return client.submit_prepared_transaction(p["network"], prepared, source_address=p["source"],
                                                  nonce=p["tx"]["nonce"], recovery_timeout_seconds=recovery_timeout_seconds)


def submit(client, network, prepared, source_address, nonce, recovery_timeout_seconds):
    journal = getattr(client, "operation_journal", None)
    lock = journal.operation_lock("onchain-transfer", scope(network, source_address)) if journal is not None else nullcontext()
    with lock:
        return _submit_locked(client, network, prepared, source_address, nonce, recovery_timeout_seconds)


def _submit_locked(client, network, prepared, source_address, nonce, recovery_timeout_seconds):
    from api_clients import PreparedSignedTransaction, SubmittedTransaction, RpcTransactionRejected
    journal = getattr(client, "operation_journal", None)
    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", str(prepared.tx_hash)):
        raise ValueError("本地签名交易哈希无效")
    metadata = getattr(getattr(client, "_transaction_context", None), "metadata", {})
    current_scope = scope(network, source_address)
    entry_id = ""
    matching = None
    if journal is not None:
        entries = journal.active_entries(kind="onchain-transfer", scope=current_scope)
        for entry in entries:
            p = entry["payload"]
            same_task = metadata.get("batch_id") and p.get("batch_id") == metadata["batch_id"] and p.get("task_key") == metadata.get("task_key")
            same_legacy_replay = (not metadata.get("batch_id") and not p.get("batch_id")
                                  and str(p["tx_hash"]).lower() == str(prepared.tx_hash).lower()
                                  and int(p["tx"]["nonce"]) == int(prepared.tx["nonce"]))
            if not same_task and not same_legacy_replay and (entry.get("state") in {"prepared", "unconfirmed"} or
                                  metadata.get("batch_id") and p.get("batch_id") != metadata["batch_id"]):
                raise UnconfirmedTransactionError("该钱包仍有待核对交易，请先恢复原任务，禁止另起任务重复付款", p["tx_hash"], p["tx"]["nonce"], entry["id"], own_transaction=False)
        if metadata.get("batch_id") and metadata.get("task_key"):
            saved = journal.batch_task_states(metadata["batch_id"], kind="onchain-transfer").get(metadata["task_key"])
            entries = [saved] if saved and saved.get("state") != "failed" else []
        matching = None
        for entry in entries:
            p = entry["payload"]
            same_task = metadata.get("batch_id") and p.get("batch_id") == metadata["batch_id"] and p.get("task_key") == metadata.get("task_key")
            if same_task or int(p["tx"]["nonce"]) == int(prepared.tx["nonce"]):
                if same_task and (
                    p.get("network") != network.upper()
                    or str(p.get("source", "")).lower() != str(source_address).lower()
                    or _transfer_intent(p["tx"]) != _transfer_intent(prepared.tx)
                ):
                    raise UnconfirmedTransactionError(
                        "同一任务标识对应不同的转账内容，禁止复用旧交易或重新付款",
                        p["tx_hash"], p["tx"]["nonce"], entry["id"], own_transaction=False,
                    )
                if not same_task and str(p["tx_hash"]).lower() != str(prepared.tx_hash).lower():
                    raise UnconfirmedTransactionError("该 nonce 已有待核对交易，禁止改换转账内容", p["tx_hash"], p["tx"]["nonce"], entry["id"], own_transaction=False)
                matching = entry
                prepared = PreparedSignedTransaction(p["tx"], p["raw_transaction"], p["tx_hash"])
                nonce = int(p["tx"]["nonce"])
                entry_id = entry["id"]
                if entry.get("state") == "completed":
                    return SubmittedTransaction(prepared.tx_hash, recovered=True, recovery_reason="saved-receipt", nonce=nonce)
                break
        if matching is None:
            payload = {"network": network.upper(), "source": source_address, "tx": prepared.tx,
                       "raw_transaction": prepared.raw_transaction, "tx_hash": prepared.tx_hash}
            payload.update({k: metadata[k] for k in ("batch_id", "task_key", "amount_text", "coin") if k in metadata})
            entry_id = journal.begin("onchain-transfer", current_scope, payload)
    expected = str(prepared.tx_hash).lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", expected):
        raise ValueError("本地签名交易哈希无效")
    stop = metadata.get("stop_event")
    if stop is not None and stop.is_set():
        raise UnconfirmedTransactionError("任务已停止，签名交易已保留待核对", expected, nonce, entry_id)
    try:
        returned = str(client.send_raw_transaction(network, prepared.raw_transaction)).lower()
        if returned != expected:
            raise RuntimeError("RPC 返回的交易哈希为空或与本地签名不符")
        if journal is not None:
            journal.update(entry_id, "submitted")
        return SubmittedTransaction(expected, recovery_reason="rpc-ack", nonce=nonce)
    except Exception as error:
        # Only a fresh intent explicitly rejected by EVERY node is safe to
        # replace with a smaller sweep. A recovered/lost reply stays guarded.
        if isinstance(error, RpcTransactionRejected) and matching is None:
            if journal is not None:
                journal.update(entry_id, "failed", reason=str(error), rejected_before_acceptance=True)
            raise
        deadline = time.monotonic() + max(0.0, float(recovery_timeout_seconds))
        for read in ("receipt", "transaction"):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or stop is not None and stop.is_set():
                break
            try:
                if read == "receipt":
                    row = client.get_transaction_receipt(network, expected, timeout_seconds=remaining / 2, stop_event=stop)
                else:
                    row = client.get_transaction_by_hash(network, expected, timeout_seconds=remaining, stop_event=stop)
                if not row:
                    continue
                if read == "receipt" and str(row.get("status")) not in {"0x0", "0x1", "0", "1"}:
                    continue
                reported_hash = row.get("transactionHash" if read == "receipt" else "hash")
                if reported_hash and str(reported_hash).lower() != expected:
                    continue
                if journal is not None:
                    state = "submitted"
                    if read == "receipt":
                        status = int(str(row.get("status", "-1")), 0)
                        state = "completed" if status == 1 else "failed" if status == 0 else "submitted"
                    journal.update(entry_id, state, recovered=True)
                return SubmittedTransaction(expected, acknowledged=False, recovered=True, recovery_reason=read, nonce=nonce)
            except Exception:
                continue
        if journal is not None:
            try:
                journal.update(entry_id, "unconfirmed", reason=str(error))
            except Exception as storage_error:
                raise UnconfirmedTransactionError(f"交易状态保存失败，须核对原交易：{expected}；{storage_error}", expected, nonce, entry_id) from storage_error
        raise UnconfirmedTransactionError(f"交易回复未确认，保留原交易待核对：{expected}；{error}", expected, nonce, entry_id) from error
