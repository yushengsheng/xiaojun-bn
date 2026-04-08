#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys

from app_paths import APP_DIR
from exchange_app import App
from exchange_selftest import run_selftest


if __name__ == '__main__':
    try:
        os.chdir(str(APP_DIR))
    except Exception as e:
        print(f'路径设置失败: {e}')
    selftest_online = '--selftest-online' in sys.argv or os.environ.get('XIAOJUN_SELFTEST_ONLINE', '').strip() == '1'
    selftest_gui = '--selftest-gui' in sys.argv or os.environ.get('XIAOJUN_SELFTEST_GUI', '').strip() == '1'
    if '--selftest' in sys.argv or '--selftest-online' in sys.argv or '--selftest-gui' in sys.argv:
        raise SystemExit(run_selftest(include_online_checks=selftest_online, include_gui_checks=selftest_gui))

    app = App()
    app.mainloop()
