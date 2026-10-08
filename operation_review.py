"""Minimal operator review shared by desktop and Linux management UI."""
from operation_journal import TERMINAL_STATES


def snapshot(journal):
    data = journal.recovery_snapshot()
    entries = []
    for entry in data.get("entries", []):
        if entry.get("state") in TERMINAL_STATES or entry.get("kind") not in {"exchange-funds", "exchange-order", "onchain-transfer"}:
            continue
        p = entry.get("payload", {})
        params = p.get("params", {})
        entries.append({"id": entry["id"], "kind": entry["kind"], "state": entry["state"],
                        "time": entry.get("created_at", ""), "action": p.get("path", p.get("symbol", p.get("network", ""))),
                        "coin": params.get("coin", params.get("asset", p.get("coin", ""))),
                        "amount": str(params.get("amount", p.get("amount_text", ""))),
                        "reference": str(params.get("withdrawOrderId", p.get("client_order_id", p.get("tx_hash", "")))),
                        "address": str(params.get("address", p.get("source", "")))})
    return {"recovery_required": bool(data.get("recovery_required")), "entries": entries}


def resolve(journal, entry_id, outcome, confirmation, evidence=""):
    if outcome == "restore":
        journal.acknowledge_recovery(confirmation)
        return
    if confirmation != "已在交易所核对" or outcome not in {"completed", "failed"} or not str(evidence).strip():
        raise ValueError("请填写核对依据，并输入“已在交易所核对”")
    entry = next((e for e in journal.active_entries(kind="exchange-funds") if e["id"] == entry_id), None)
    if entry is None:
        raise ValueError("记录不存在或已处理；订单及链上交易须通过原单/原交易核对")
    journal.update(entry_id, outcome, reconciled_by="operator", review_evidence=str(evidence).strip()[:1000])


def desktop_review(parent, journal):
    from tkinter import messagebox, simpledialog
    try:
        data = snapshot(journal)
        lines = [f"{e['id']}\n{e['time']} {e['action']} {e['coin']} {e['amount']}\n{e['address']} {e['reference']} [{e['state']}]" for e in data["entries"]]
        text = "\n\n".join(lines) or "没有待核对操作。"
        if data["recovery_required"]:
            messagebox.showinfo("账本恢复保护", "账本从备份恢复，可能缺少最近记录。请先逐笔核对交易所及链上历史。\n\n" + text, parent=parent)
            answer = simpledialog.askstring("解除恢复保护", "核对全部历史后输入：已核对全部操作\n解除后，现有待核对记录仍会保留。", parent=parent)
            if answer is not None:
                resolve(journal, "", "restore", answer)
            return
        funds = [e for e in data["entries"] if e["kind"] == "exchange-funds"]
        if not funds:
            messagebox.showinfo("核对未确认", text + "\n\n订单会在再次执行时查询原单；链上任务使用失败重试核对原交易。", parent=parent)
            return
        ident = simpledialog.askstring("核对未确认", text + "\n\n仅在交易所核对历史后，填写要处理的资金记录编号：", parent=parent)
        if ident is None:
            return
        outcome = simpledialog.askstring("核对结果", "原操作已执行输入 completed；确认未执行输入 failed。\n不能确定时取消。", parent=parent)
        if outcome is None:
            return
        evidence = simpledialog.askstring("核对依据", "填写交易所回执编号或查询结果及时间：", parent=parent)
        if evidence is None:
            return
        answer = simpledialog.askstring("确认核对", "输入：已在交易所核对", parent=parent)
        if answer is not None:
            resolve(journal, ident.strip(), outcome.strip(), answer, evidence)
    except Exception as exc:
        messagebox.showerror("核对失败", str(exc), parent=parent)
