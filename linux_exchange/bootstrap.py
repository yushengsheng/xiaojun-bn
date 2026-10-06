"""Redirect paths inside the dedicated worker before importing desktop modules."""
from pathlib import Path


def configure_data_directory(directory):
    import app_paths
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    app_paths.APP_DIR = root
    app_paths.DATA_DIR = root
    files = {
        "DATA_FILE": "accounts.json", "STRATEGY_CONFIG_FILE": "exchange_strategy_settings.json",
        "EXCHANGE_PROXY_CONFIG_FILE": "exchange_proxy_settings.json",
        "EXCHANGE_OPERATION_JOURNAL_FILE": "exchange_operations.json",
        "SECRET_KEY_FILE": ".secret.key", "APP_LOCK_FILE": "app_lock.json",
        "APP_LOCK_BACKUP_FILE": "app_lock.json.bak", "LOG_FILE_PATH": "bot_log.txt",
        "WITHDRAW_SUCCESS_FILE": "withdraw_success.txt", "TOTAL_ASSET_RESULT_FILE": "total_asset_result.txt",
    }
    for name, filename in files.items():
        setattr(app_paths, name, root / filename)
    app_paths.LOG_DIR = root / "logs"
    return root
