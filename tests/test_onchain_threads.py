"""Real signing checks for restored concurrency; RPC boundaries never access the network."""
import threading
import unittest
from dataclasses import replace
from unittest.mock import Mock
from eth_account import Account
from eth_account._utils.legacy_transactions import Transaction
from api_clients import EvmClient
from core_models import WithdrawRuntimeParams
from onchain_transfer_runner import OnchainTransferRunnerMixin
from onchain_relay_runner import _relay_source_stage, run_relay_fee_recovery
from tests.test_onchain_relay_pipeline import _FakeOwner, _item, _params, _RECORDS, _close_sweep_log_after
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

class _DirectOwner(OnchainTransferRunnerMixin, _FakeOwner):
    MODE_1M = "1对多"

    def __init__(self, client):
        super().__init__()
        self.client = client
        self.wallet_cache_lock = threading.Lock()
        self.source_private_key_cache, self.source_address_cache = {}, {}

    def _start_rpc_worker_thread(self, target, **_kwargs):
        thread = threading.Thread(target=target)
        thread.start()
        return thread

    def _row_number_by_key_snapshot(self):
        return {}

    def _token_desc_from_params(self, params):
        return params.coin

    def _row_context_for_values(self, key, source, target):
        return f"{key}:{source}:{target}"

    def _mark_row_status_context(self, *_args):
        pass

    def _begin_progress(self, *_args):
        pass

    def _set_progress_metrics(self, **_kwargs):
        pass

    def _finish_progress(self, *_args):
        pass

    def _estimated_gas_fee_text(self, network, amount):
        return self._gas_fee_amount_text(network, amount)

    def _success_status_text(self, coin, amount):
        return f"{amount} {coin}"

class RestoredOnchainThreadsTests(unittest.TestCase):
    def setUp(self):
        _RECORDS.clear()
        self.client = EvmClient()
        self.addCleanup(self.client.close)
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(3)
        self.pending_nonce = 0
        self.active = self.peak = 0
        self.signed = []
        self.client._rpc_call = Mock(side_effect=self.rpc)

    def rpc(self, network, method, args, **_kwargs):
        if method == 'eth_getTransactionCount':
            return hex(self.pending_nonce)
        if method == 'eth_getBalance':
            return hex(10 * 10**18)
        if method == 'eth_call':
            return hex(10000 * 10**18)
        if method == 'eth_gasPrice':
            return hex(10**9)
        if method == 'eth_blockNumber':
            return '0x1'
        if method == 'eth_getBlockByNumber':
            return {'transactions': []}
        if method == 'eth_estimateGas':
            return hex(65000)
        if method == 'eth_getTransactionReceipt':
            return {'status': '0x1'}
        if method != 'eth_sendRawTransaction':
            raise AssertionError('Unmocked RPC: ' + method)
        tx = Transaction.from_bytes(bytes.fromhex(args[0][2:])).as_dict()
        self.assertEqual(Account.recover_transaction(args[0]).lower(), self.client.address_from_private_key('1'*64).lower())
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.pending_nonce = max(self.pending_nonce, tx['nonce'] + 1)
            self.signed.append(tx)
        try:
            self.barrier.wait(timeout=5)
        finally:
            with self.lock:
                self.active -= 1
        return '0x' + bytes(self.client._ensure_eth_utils().keccak(hexstr=args[0])).hex()

    def test_direct_one_to_many_broadcasts_concurrently_and_switches_asset(self):
        owner = _DirectOwner(self.client)
        jobs = [(f'row{i}', '1'*64, '0x'+str(i+2)*40) for i in range(3)]
        for native, amount in ((True, '0.001'), (False, '20')):
            params = WithdrawRuntimeParams(coin='BNB' if native else 'USDT', amount=amount, network='BSC',
                delay=0, threads=20, one_to_many_source_threads=3, mode='1对多', token_is_native=native,
                token_contract='' if native else '0x55d398326f99059ff775485246999027b3197955')
            owner._run_batch_transfer(jobs, params, False)
            self.assertEqual(owner.result_dialogs[-1]['success'], 3, owner.logs[-3:])
        self.assertEqual(self.peak, 3)
        self.assertEqual(sorted(tx['nonce'] for tx in self.signed), list(range(6)))
        self.assertEqual(sum(tx['value'] for tx in self.signed), 3 * 10**15)
        self.assertEqual(sorted(int(tx['data'].hex()[72:], 16) for tx in self.signed if tx['data']), [20 * 10**18] * 3)

    def test_relay_source_stage_broadcasts_three_real_signatures_in_parallel(self):
        owner = _FakeOwner(mode_1m=True)
        owner.client = self.client
        items = [_item(i) for i in range(1,4)]
        address = self.client.address_from_private_key('1'*64)
        for item in items:
            item.source_addr = address
            item.source_private_key = '1'*64
        def send(item, nonce):
            return self.client.submit_native_transfer_reliably(network='BSC', private_key='1'*64,
                to_address=item.relay_addr, value_wei=10**15, nonce=nonce, gas_price_wei=10**9,
                source_address=address).tx_hash
        params = replace(_params(coin='BNB', token_is_native=True), threads=20,
            one_to_many_source_threads=3, nonce_window=3, delay=0, mode='1对多')
        _relay_source_stage(owner, params, items, stage_label='资金第一跳', tx_attr='source_fund_txid',
            nonce_attr='source_fund_nonce', submitted_status='source_fund_submitted', done_status='token_funded',
            done_statuses={'token_funded'}, submit_one=send, status_text='第一跳')
        self.assertEqual(self.peak, 3)
        self.assertEqual(sorted(tx['nonce'] for tx in self.signed), [0,1,2])
        self.assertTrue(all(not item.failed for item in items))

    def test_recovery_uses_six_configured_workers_without_a_four_worker_cap(self):
        owner = _FakeOwner()
        owner._runtime_worker_threads = lambda: 6
        barrier = threading.Barrier(6)
        for i in range(1,7):
            _item(i)
        def balance(*_args):
            barrier.wait(timeout=5)
            return 0
        owner.client.get_balance_wei = Mock(side_effect=balance)
        owner.client.erc20_balance_units = 0
        with TemporaryDirectory() as directory, _close_sweep_log_after(), patch(
            'onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE', Path(directory)/'manual.txt'
        ), patch('onchain_relay_runner.RELAY_SWEEP_LOG_FILE', Path(directory)/'sweep.jsonl'):
            run_relay_fee_recovery(owner, False, use_progress=False, manage_running_state=False)
        self.assertEqual(owner.client.get_balance_wei.call_count, 6)
        self.assertEqual(_RECORDS, {})
        self.assertTrue(any('threads=6' in line for line in owner.logs))
