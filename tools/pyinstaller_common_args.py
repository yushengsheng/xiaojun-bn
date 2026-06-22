#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE_NAME = "version.txt"

HIDDEN_IMPORTS = (
    "page_onchain",
    "api_clients",
    "app_defaults",
    "exchange_binance_client",
    "app_paths",
    "app_lock",
    "app_version",
    "core_models",
    "shared_utils",
    "stores",
    "secret_box",
    "secure_config",
    "operation_journal",
    "operation_recovery",
    "exchange_order_recovery",
    "exchange_account_destruction",
    "secure_delete",
    "selftest_basic_checks",
    "selftest_gui_checks",
    "selftest_online_checks",
    "table_import_utils",
    "task_progress",
)

COLLECT_ALL = (
    "requests",
    "urllib3",
    "certifi",
    "charset_normalizer",
    "idna",
    "socks",
    "cryptography",
    "cffi",
    "pycparser",
    "eth_account",
    "eth_utils",
    "eth_abi",
    "eth_keyfile",
    "eth_keys",
    "eth_rlp",
    "eth_hash",
    "hexbytes",
    "rlp",
    "parsimonious",
    "cytoolz",
    "bitarray",
    "ckzg",
)


def common_pyinstaller_args(*, version_file: Path | None = None) -> list[str]:
    """Return portable build arguments without requiring a local release tag file."""
    args = [
        "--noconfirm",
        "--clean",
        "--windowed",
        "--noupx",
    ]
    for module_name in HIDDEN_IMPORTS:
        args.extend(("--hidden-import", module_name))
    for package_name in COLLECT_ALL:
        args.extend(("--collect-all", package_name))
    candidate = Path(version_file) if version_file is not None else ROOT / VERSION_FILE_NAME
    if candidate.is_file():
        args.extend(("--add-data", f"{candidate}{os.pathsep}."))
    return args


def main() -> int:
    for item in common_pyinstaller_args():
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
