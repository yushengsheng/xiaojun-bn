#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from onchain_imports import OnchainImportMixin
from onchain_query import OnchainQueryMixin
from onchain_transfer_runner import OnchainTransferRunnerMixin
from onchain_wallets import OnchainWalletMixin
from page_onchain_base import OnchainTransferPageBase


class OnchainTransferPage(
    OnchainTransferRunnerMixin,
    OnchainQueryMixin,
    OnchainWalletMixin,
    OnchainImportMixin,
    OnchainTransferPageBase,
):
    pass
