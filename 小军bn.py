#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys

from app_paths import APP_DIR, DATA_DIR


if __name__ == '__main__':
    try:
        os.chdir(str(APP_DIR))
    except Exception as e:
        print(f'路径设置失败: {e}')
    selftest_online = '--selftest-online' in sys.argv or os.environ.get('XIAOJUN_SELFTEST_ONLINE', '').strip() == '1'
    selftest_gui = '--selftest-gui' in sys.argv or os.environ.get('XIAOJUN_SELFTEST_GUI', '').strip() == '1'
    if '--selftest' in sys.argv or '--selftest-online' in sys.argv or '--selftest-gui' in sys.argv:
        from exchange_selftest import run_selftest
        os.environ.setdefault('XIAOJUN_APP_LOCK_BYPASS', '1')
        raise SystemExit(run_selftest(include_online_checks=selftest_online, include_gui_checks=selftest_gui))

    from process_lock import AlreadyRunningError, ProcessLease
    try:
        lease = ProcessLease(DATA_DIR / '.desktop.lock').acquire()
    except AlreadyRunningError as exc:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo('客户端已运行', str(exc), parent=root)
        root.destroy()
        raise SystemExit(0)
    try:
        from exchange_app import App
        app = App()
        app.mainloop()
    finally:
        lease.close()
