#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import onchain_relay_batch as _relay_batch_module
import onchain_relay_helpers as _relay_helpers_module
import onchain_relay_recovery as _relay_recovery_module

from onchain_relay_helpers import *
from onchain_relay_recovery import *


def _sync_compat_overrides(target_module) -> None:
    for name in getattr(_relay_helpers_module, "__all__", ()):
        if name in globals():
            setattr(target_module, name, globals()[name])
    for name in ("_schedule_background_relay_sweep_recovery",):
        if name in globals():
            setattr(target_module, name, globals()[name])


def run_relay_fee_recovery(*args, **kwargs):
    _sync_compat_overrides(_relay_recovery_module)
    return _relay_recovery_module.run_relay_fee_recovery(*args, **kwargs)


def run_relay_batch(*args, **kwargs):
    _sync_compat_overrides(_relay_batch_module)
    return _relay_batch_module.run_relay_batch(*args, **kwargs)


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("__")]


__all__ = _build_module_star_exports()
