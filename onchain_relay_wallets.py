#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app_paths import RELAY_WALLET_FILE
from core_models import EvmToken, GeneratedWalletEntry

RELAY_RETENTION_HOURS = 72
RELAY_TRACKED_BALANCE_SYMBOLS = {"USDT", "USDC"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime | None = None) -> str:
    current = value or _utc_now()
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc_text(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decimal_text(value: Decimal | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value != value.to_integral() else format(value, "f")
    return str(value or "").strip()


def _atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".relay-wallet.", suffix=".tmp", dir=str(path.parent))
    try:
        try:
            os.chmod(tmp_name, 0o600)
        except Exception:
            pass
        with os.fdopen(fd, "w", encoding=encoding) as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_name, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


@dataclass
class RelayWalletRecord:
    created_at: str
    expires_at: str
    network: str
    source: str
    target: str
    relay_address: str
    private_key: str
    status: str
    batch_id: str
    token_symbol: str = ""
    token_contract: str = ""
    token_decimals: int = 18
    token_is_native: bool = True
    relay_fee_reserve: str = ""
    transfer_amount: str = ""
    transfer_units: str = ""
    sweep_enabled: bool = False
    sweep_target: str = ""
    fee_funded_txid: str = ""
    fee_funded_nonce: str = ""
    token_funded_txid: str = ""
    token_funded_nonce: str = ""
    token_forward_txid: str = ""
    token_forward_nonce: str = ""
    gas_sweep_txid: str = ""
    gas_sweep_nonce: str = ""
    sweep_resolution: str = ""
    last_error: str = ""
    completed_at: str = ""

    @classmethod
    def from_raw(cls, raw: dict[str, object]) -> RelayWalletRecord:
        return cls(
            created_at=str(raw.get("created_at") or "").strip(),
            expires_at=str(raw.get("expires_at") or "").strip(),
            network=str(raw.get("network") or "").strip().upper(),
            source=str(raw.get("source") or "").strip(),
            target=str(raw.get("target") or "").strip(),
            relay_address=str(raw.get("relay_address") or "").strip(),
            private_key=str(raw.get("private_key") or "").strip(),
            status=str(raw.get("status") or "").strip(),
            batch_id=str(raw.get("batch_id") or "").strip(),
            token_symbol=str(raw.get("token_symbol") or "").strip().upper(),
            token_contract=str(raw.get("token_contract") or "").strip(),
            token_decimals=max(0, int(raw.get("token_decimals", 18) or 18)),
            token_is_native=bool(raw.get("token_is_native", True)),
            relay_fee_reserve=str(raw.get("relay_fee_reserve") or "").strip(),
            transfer_amount=str(raw.get("transfer_amount") or "").strip(),
            transfer_units=str(raw.get("transfer_units") or "").strip(),
            sweep_enabled=bool(raw.get("sweep_enabled", False)),
            sweep_target=str(raw.get("sweep_target") or "").strip(),
            fee_funded_txid=str(raw.get("fee_funded_txid") or "").strip(),
            fee_funded_nonce=str(raw.get("fee_funded_nonce") or "").strip(),
            token_funded_txid=str(raw.get("token_funded_txid") or "").strip(),
            token_funded_nonce=str(raw.get("token_funded_nonce") or "").strip(),
            token_forward_txid=str(raw.get("token_forward_txid") or "").strip(),
            token_forward_nonce=str(raw.get("token_forward_nonce") or "").strip(),
            gas_sweep_txid=str(raw.get("gas_sweep_txid") or "").strip(),
            gas_sweep_nonce=str(raw.get("gas_sweep_nonce") or "").strip(),
            sweep_resolution=str(raw.get("sweep_resolution") or "").strip(),
            last_error=str(raw.get("last_error") or "").strip(),
            completed_at=str(raw.get("completed_at") or "").strip(),
        )


class RelayWalletFileStore:
    def __init__(self, file_path: Path | None = None):
        self.file_path = Path(file_path or RELAY_WALLET_FILE)
        self._lock = threading.RLock()
        self.ensure_file()

    def ensure_file(self) -> None:
        with self._lock:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            if self.file_path.exists():
                return
            _atomic_write_text(self.file_path, "", encoding="utf-8")

    def _load_records_locked(self) -> list[RelayWalletRecord]:
        self.ensure_file()
        text = self.file_path.read_text(encoding="utf-8")
        records: list[RelayWalletRecord] = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            raw_line = str(line or "").strip()
            if not raw_line or raw_line.startswith("#"):
                continue
            try:
                raw = json.loads(raw_line)
            except Exception as exc:
                raise RuntimeError(f"中转钱包.txt 第 {line_no} 行 JSON 解析失败：{exc}") from exc
            if not isinstance(raw, dict):
                raise RuntimeError(f"中转钱包.txt 第 {line_no} 行格式无效")
            record = RelayWalletRecord.from_raw(raw)
            if not record.relay_address or not record.private_key:
                raise RuntimeError(f"中转钱包.txt 第 {line_no} 行缺少中转钱包地址或私钥")
            records.append(record)
        return records

    def _save_records_locked(self, records: list[RelayWalletRecord]) -> None:
        lines = [json.dumps(asdict(record), ensure_ascii=False, separators=(",", ":")) for record in records]
        content = "\n".join(lines)
        if content:
            content += "\n"
        _atomic_write_text(self.file_path, content, encoding="utf-8")

    def load_records(self) -> list[RelayWalletRecord]:
        with self._lock:
            return self._load_records_locked()

    def append_records(self, records: list[RelayWalletRecord]) -> None:
        if not records:
            return
        with self._lock:
            current = self._load_records_locked()
            current.extend(records)
            self._save_records_locked(current)

    def update_record(self, relay_address: str, *, batch_id: str = "", **changes: object) -> RelayWalletRecord:
        relay_key = str(relay_address or "").strip()
        batch_key = str(batch_id or "").strip()
        if not relay_key:
            raise RuntimeError("中转钱包地址不能为空")
        with self._lock:
            records = self._load_records_locked()
            for idx, record in enumerate(records):
                if record.relay_address != relay_key:
                    continue
                if batch_key and record.batch_id != batch_key:
                    continue
                updated_raw = asdict(record)
                updated_raw.update(changes)
                updated = RelayWalletRecord.from_raw(updated_raw)
                records[idx] = updated
                self._save_records_locked(records)
                return updated
        raise RuntimeError(f"未找到中转钱包记录：{relay_key}")

    def build_record(
        self,
        *,
        batch_id: str,
        network: str,
        source_address: str,
        target_address: str,
        relay_wallet: GeneratedWalletEntry,
        token: EvmToken,
        relay_fee_reserve: Decimal | str | None,
        sweep_enabled: bool,
        sweep_target: str,
    ) -> RelayWalletRecord:
        created_at = _utc_now()
        return RelayWalletRecord(
            created_at=_utc_text(created_at),
            expires_at=_utc_text(created_at + timedelta(hours=RELAY_RETENTION_HOURS)),
            network=str(network or "").strip().upper(),
            source=str(source_address or "").strip(),
            target=str(target_address or "").strip(),
            relay_address=str(relay_wallet.address or "").strip(),
            private_key=str(relay_wallet.private_key or "").strip(),
            status="created",
            batch_id=str(batch_id or "").strip(),
            token_symbol=str(token.symbol or "").strip().upper(),
            token_contract=str(token.contract or "").strip(),
            token_decimals=max(0, int(token.decimals)),
            token_is_native=bool(token.is_native),
            relay_fee_reserve=_decimal_text(relay_fee_reserve),
            sweep_enabled=bool(sweep_enabled),
            sweep_target=str(sweep_target or "").strip(),
        )

    def cleanup_expired_empty_records(self, client, *, log=None, worker_threads: int = 1) -> tuple[int, int, int]:
        removed = 0
        kept = 0
        checked = 0
        now = _utc_now()
        with self._lock:
            records = self._load_records_locked()
            remain: list[RelayWalletRecord] = []
            expired_records: list[RelayWalletRecord] = []
            for record in records:
                expires_at = _parse_utc_text(record.expires_at)
                if expires_at is None or expires_at > now:
                    remain.append(record)
                    continue
                expired_records.append(record)

            checked = len(expired_records)
            if not expired_records:
                return checked, removed, kept

            tracked_contracts_by_network: dict[str, list[str]] = {}
            for record in expired_records:
                network = str(record.network or "").strip().upper()
                if network in tracked_contracts_by_network:
                    continue
                contracts: list[str] = []
                for token in client.get_default_tokens(network):
                    if bool(getattr(token, "is_native", False)):
                        continue
                    symbol = str(getattr(token, "symbol", "") or "").strip().upper()
                    if symbol not in RELAY_TRACKED_BALANCE_SYMBOLS:
                        continue
                    contract = str(getattr(token, "contract", "") or "").strip()
                    if contract:
                        contracts.append(contract)
                tracked_contracts_by_network[network] = contracts

            def inspect_record(record: RelayWalletRecord) -> tuple[bool, list[int] | Exception]:
                try:
                    balances: list[int] = [int(client.get_balance_wei(record.network, record.relay_address))]
                    for contract in tracked_contracts_by_network.get(str(record.network or "").strip().upper(), []):
                        balances.append(int(client.get_erc20_balance(record.network, contract, record.relay_address)))
                    return True, balances
                except Exception as exc:
                    return False, exc

            result_by_key: dict[tuple[str, str], tuple[bool, list[int] | Exception]] = {}
            worker_count = max(1, min(int(worker_threads or 1), len(expired_records)))
            if worker_count == 1:
                for record in expired_records:
                    record_key = (str(record.batch_id or "").strip(), str(record.relay_address or "").strip())
                    result_by_key[record_key] = inspect_record(record)
            else:
                with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="relay-retention") as executor:
                    future_map = {
                        executor.submit(inspect_record, record): (
                            str(record.batch_id or "").strip(),
                            str(record.relay_address or "").strip(),
                        )
                        for record in expired_records
                    }
                    for future in as_completed(future_map):
                        result_by_key[future_map[future]] = future.result()

            for record in expired_records:
                record_key = (str(record.batch_id or "").strip(), str(record.relay_address or "").strip())
                ok, payload = result_by_key.get(record_key, (False, RuntimeError("cleanup result missing")))
                if not ok:
                    kept += 1
                    remain.append(record)
                    if callable(log):
                        log(f"中转钱包过期清理跳过：{record.relay_address} 查询失败：{payload}")
                    continue

                balances = payload
                if all(int(value) == 0 for value in balances):
                    removed += 1
                    if callable(log):
                        log(f"中转钱包已清空并超过 72 小时，已移除：{record.relay_address}")
                    continue

                kept += 1
                remain.append(record)
                if callable(log):
                    log(f"中转钱包超过 72 小时但余额未清空，继续保留：{record.relay_address}")
            if removed > 0:
                self._save_records_locked(remain)
        return checked, removed, kept
