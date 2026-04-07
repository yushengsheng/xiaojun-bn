#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_accounts import ExchangeAppAccountsMixin
from exchange_app_base import ExchangeAppBase
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_app_config import ExchangeAppConfigMixin
from exchange_app_log_view import ExchangeAppLogViewMixin


class App(
    ExchangeAppBatchMixin,
    ExchangeAppAccountsMixin,
    ExchangeAppConfigMixin,
    ExchangeAppLogViewMixin,
    ExchangeAppBase,
):
    pass
