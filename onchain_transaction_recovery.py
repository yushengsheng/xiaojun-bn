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
    from api_clients import PreparedSignedTransaction, SubmittedTransaction
    journal = getattr(client, "operation_journal", None)
    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", str(prepared.tx_hash)):
        raise ValueError("本地签名交易哈希无效")
    metadata = getattr(getattr(client, "_transaction_context", None), "metadata", {})
    current_scope = scope(network, source_address)
    entry_id = ""
    if journal is not None:
        entries = journal.active_entries(kind="onchain-transfer", scope=current_scope)
        for entry in entries:
            p = entry["payload"]
            same_task = metadata.get("batch_id") and p.get("batch_id") == metadata["batch_id"] and p.get("task_key") == metadata.get("task_key")
            if not same_task and (entry.get("state") in {"prepared", "unconfirmed"} or
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
