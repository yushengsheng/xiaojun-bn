"""Exercise balance-based recovery with real signing and an encrypted wallet store."""
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import requests
from eth_account import Account
from eth_account._utils.legacy_transactions import Transaction
from api_clients import EvmClient
from onchain_relay_runner import (
    _RelayBatchItem, _build_resume_candidates, _pick_or_create_relay_records, _relay_submit_auto_sweep,
    run_relay_fee_recovery,
)
from onchain_relay_wallets import RelayWalletFileStore, RelayWalletRecord
from secret_box import SecretBox
from tests.test_onchain_relay_pipeline import _FakeOwner, _close_sweep_log_after, _params


class RecoveryRpc:
    def __init__(self, client, store):
        self.client, self.store = client, store
        self.native, self.tokens, self.nonces, self.receipts = {}, {}, {}, {}
        self.sent = []
        self.receipt_queries = []
        self.lock = threading.Lock()
        self.lose_reply = False
        self.always_lose_reply = False
        self.broadcasts = []
        self.pending_receipt = False
        self.corrupt_receipt_hash = False
        self.fail_contract = None
        self.old_hash = '0x' + 'f' * 64

    def __call__(self, network, method, args, **kwargs):
        if method == 'eth_getTransactionCount':
            return hex(self.nonces.get(args[0].lower(), 0))
        if method == 'eth_getBalance':
            return hex(self.native.get(args[0].lower(), 0))
        if method == 'eth_call':
            address = '0x' + args[0]['data'][-40:]
            return hex(self.tokens.get((address.lower(), args[0]['to'].lower()), 0))
        if method == 'eth_gasPrice':
            return hex(10**9)
        if method == 'eth_blockNumber':
            return '0x1'
        if method == 'eth_getBlockByNumber':
            return {'transactions': []}
        if method == 'eth_estimateGas':
            return hex(65000)
        if method == 'eth_getTransactionReceipt':
            self.receipt_queries.append(args[0])
            if args[0] == self.old_hash:
                raise AssertionError('Old second hop must not gate recovery')
            receipt = None if self.pending_receipt else self.receipts.get(args[0])
            if receipt and self.corrupt_receipt_hash:
                return {**receipt,'transactionHash':'0x'+'b'*64}
            return receipt
        if method != 'eth_sendRawTransaction':
            raise AssertionError('Unexpected RPC: ' + method)
        raw = args[0]
        tx = Transaction.from_bytes(bytes.fromhex(raw[2:])).as_dict()
        sender = Account.recover_transaction(raw).lower()
        txid = '0x' + bytes(self.client._ensure_eth_utils().keccak(hexstr=raw)).hex()
        recipient = '0x' + tx['to'].hex()
        # The encrypted primary and backup must contain this exact identity before broadcast.
        saved = RelayWalletFileStore(self.store.file_path).load_records()
        record = next(row for row in saved if row.relay_address.lower() == sender and row.gas_sweep_txid == txid)
        assert record.sweep_resolution == 'recovery_pending'
        assert self.store.file_path.read_bytes() == Path(str(self.store.file_path) + '.bak').read_bytes()
        with self.lock:
            self.broadcasts.append(raw)
            if txid in self.receipts:
                if self.always_lose_reply:
                    raise requests.ReadTimeout('RPC reply lost after acceptance')
                return txid
            fee = tx['gas'] * tx['gasPrice']
            assert tx['nonce'] == self.nonces.get(sender, 0)
            assert self.native.get(sender, 0) >= tx['value'] + fee
            self.nonces[sender] = tx['nonce'] + 1
            self.native[sender] -= fee
            failed = recipient.lower() == self.fail_contract
            if tx['data']:
                data = tx['data'].hex()
                assert data[:8] == 'a9059cbb'
                target = '0x' + data[32:72]
                units = int(data[72:], 16)
                if not failed:
                    source_key, target_key = (sender, recipient.lower()), (target.lower(), recipient.lower())
                    assert self.tokens.get(source_key, 0) >= units
                    self.tokens[source_key] -= units
                    self.tokens[target_key] = self.tokens.get(target_key, 0) + units
                asset = recipient.lower()
            elif not failed:
                target, units, asset = recipient, tx['value'], 'BNB'
                self.native[sender] -= units
                self.native[target.lower()] = self.native.get(target.lower(), 0) + units
            self.sent.append({'sender':sender,'target':target.lower(),'asset':asset,'units':units,'nonce':tx['nonce'],'hash':txid})
            self.receipts[txid] = {'status':'0x0' if failed else '0x1','transactionHash':txid,'from':sender,'to':recipient}
        if self.lose_reply or self.always_lose_reply:
            self.lose_reply = False
            raise requests.ReadTimeout('RPC reply lost after acceptance')
        return txid


class BalanceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.box = SecretBox(self.root / '.test-key')
        self.secret_patch = patch('onchain_relay_wallets.SECRET_BOX', self.box)
        self.secret_patch.start()
        self.addCleanup(self.secret_patch.stop)
        self.store = RelayWalletFileStore(self.root / 'wallets.txt')
        self.client = EvmClient()
        self.addCleanup(self.client.close)
        self.rpc = RecoveryRpc(self.client, self.store)
        self.client._rpc_call = self.rpc
        self.owner = _FakeOwner()
        self.owner.client, self.owner.relay_wallet_store = self.client, self.store
        self.usdt, self.usdc = self.client.get_default_tokens('BSC')[1:]
        self.target = '0x' + '9' * 40
        self.key = '1' * 64
        self.address = self.client.address_from_private_key(self.key)
        self.clock = 0.0

    def record(self, *, key=None, batch='batch', old_hash=True):
        key = key or self.key
        address = self.client.address_from_private_key(key)
        row = RelayWalletRecord(created_at='',expires_at='',network='BSC',
            source='0x' + '2' * 40,target='0x' + '3' * 40,relay_address=address,private_key=key,
            status='failed',batch_id=batch,token_is_native=False,token_symbol='USDT',
            token_contract=self.usdt.contract,token_forward_txid=self.rpc.old_hash if old_hash else '')
        self.store.append_records([row])
        return row

    def fund(self, record, *, bnb=10**15, usdt=0, usdc=0):
        address = record.relay_address.lower()
        self.rpc.native[address] = bnb
        self.rpc.tokens[(address,self.usdt.contract.lower())] = usdt
        self.rpc.tokens[(address,self.usdc.contract.lower())] = usdc

    def run_recovery(self, *, dry=False, batch=None, target=None):
        def wait(seconds):
            self.clock += seconds
            return False
        with patch('onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE',self.root/'manual.txt'), \
             patch('onchain_relay_runner.RELAY_SWEEP_LOG_FILE',self.root/'sweep.jsonl'), \
             patch('onchain_relay_runner.time.monotonic',side_effect=lambda:self.clock), \
             patch.object(self.owner.stop_requested,'wait',side_effect=wait), _close_sweep_log_after():
            run_relay_fee_recovery(self.owner,dry,batch_ids={batch} if batch else None,
                use_progress=False,manage_running_state=False,sweep_target_override=target or self.target)

    def run_auto_sweep(self, row):
        item = _RelayBatchItem(
            index=1, row_key='auto-row', source_raw='', target=row.target,
            source_private_key='', source_addr=self.target, relay_record=row,
            relay_private_key=self.key, prefix='auto', relay_name='B1',
            target_name='C1', source_label='source', amount_text='20', value_units=20*10**18,
        )
        def wait(seconds):
            self.clock += seconds
            return self.owner.stop_requested.is_set()
        with patch('onchain_relay_runner.time.monotonic',side_effect=lambda:self.clock), \
             patch.object(self.owner.stop_requested,'wait',side_effect=wait), self.store.defer_writes():
            _relay_submit_auto_sweep(self.owner,_params(),item,self.target)
        return item

    def test_auto_sweep_lost_replies_preserve_signed_identity_across_restart(self):
        row = self.record()
        self.fund(row)
        self.rpc.always_lose_reply = True
        item = self.run_auto_sweep(row)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(len(set(self.rpc.broadcasts)),1)
        saved = RelayWalletFileStore(self.store.file_path).load_records()[0]
        self.assertEqual(saved.gas_sweep_txid,self.rpc.sent[0]['hash'])
        self.assertEqual(saved.token_forward_txid,self.rpc.old_hash)
        self.assertEqual(saved.sweep_resolution,'recovery_pending')
        self.assertEqual(getattr(item,'_auto_sweep_value_wei'),0)
        self.assertNotIn(self.key,self.store.file_path.read_text())
        self.rpc.always_lose_reply = False
        self.rpc.pending_receipt = True
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(len(self.rpc.sent),1)
        self.rpc.pending_receipt = False
        self.run_recovery()
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(len(self.rpc.sent),1)

    def test_auto_sweep_confirmation_timeout_keeps_protection_for_manual_recovery(self):
        row = self.record()
        self.fund(row)
        self.rpc.pending_receipt = True
        self.run_auto_sweep(row)
        self.assertEqual(self.store.load_records()[0].sweep_resolution,'recovery_pending')
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(len(self.rpc.sent),1)

    def test_auto_sweep_write_failure_prevents_broadcast(self):
        row = self.record()
        self.fund(row)
        with patch.object(self.store,'flush',side_effect=OSError('disk failure')):
            with self.assertRaisesRegex(OSError,'disk failure'):
                self.run_auto_sweep(row)
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(self.rpc.native[self.address.lower()],10**15)

    def test_unconfirmed_auto_sweep_cannot_be_erased_by_wallet_reuse(self):
        row = self.record()
        self.fund(row)
        self.rpc.pending_receipt = True
        self.run_auto_sweep(row)
        saved = RelayWalletFileStore(self.store.file_path).load_records()[0]
        for status in ('completed','failed'):
            with self.subTest(status=status), self.assertRaisesRegex(RuntimeError,'待确认的回收交易'):
                _pick_or_create_relay_records(self.owner,[('row','source',saved.target)],_params(),
                    {'row':(self.key,saved.source)},
                    {(saved.source.lower(),saved.target.lower()):[replace(saved,status=status)]})
        self.assertEqual(self.store.load_records()[0].gas_sweep_txid,saved.gas_sweep_txid)
        self.assertEqual(self.store.load_records()[0].sweep_resolution,'recovery_pending')

    def test_auto_sweep_rejects_receipt_for_another_transaction(self):
        row = self.record()
        self.fund(row)
        self.rpc.corrupt_receipt_hash = True
        item = self.run_auto_sweep(row)
        self.assertEqual(getattr(item,'_auto_sweep_value_wei'),0)
        self.assertEqual(self.store.load_records()[0].sweep_resolution,'recovery_pending')
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(len(self.rpc.sent),1)

    def test_completed_legacy_wallet_is_reused_only_on_its_original_network(self):
        row = replace(self.record(),status='completed',reuse_key='')
        jobs, sources = [('row','source',row.target)], {'row':(self.key,row.source)}
        candidates = _build_resume_candidates(self.owner,[row],jobs,_params(),sources)
        self.assertEqual(sum(map(len,candidates.values())),1)
        candidates = _build_resume_candidates(self.owner,[replace(row,network='ETH')],jobs,_params(),sources)
        self.assertEqual(candidates,{})

    def test_legacy_auto_sweep_submission_is_checked_before_dust_cleanup(self):
        row = self.record()
        self.fund(row,bnb=0)
        self.store.update_record(row.relay_address,batch_id=row.batch_id,status='completed',
            gas_sweep_txid='0x'+'a'*64,sweep_resolution='submitted')
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(self.rpc.sent,[])

    def test_native_balance_recovers_to_override_without_consulting_second_hop(self):
        for old_hash in (False,True):
            with self.subTest(old_hash=old_hash):
                row = self.record(batch=str(old_hash),old_hash=old_hash)
                self.fund(row)
                self.run_recovery()
                self.assertEqual(self.store.load_records(),[])
                self.assertEqual(self.rpc.sent[-1]['target'],self.target.lower())
                self.assertEqual(self.rpc.sent[-1]['units'],10**15-21000*10**9)
        self.assertNotIn(self.rpc.old_hash,self.rpc.receipt_queries)
        self.assertTrue(all(tx['sender']==self.address.lower() for tx in self.rpc.sent))

    def test_tokens_recover_one_by_one_then_native_currency(self):
        row = self.record()
        self.fund(row,usdt=20*10**18,usdc=30*10**18)
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],
                         [self.usdc.contract.lower(),self.usdt.contract.lower(),'BNB'])
        self.assertEqual([tx['nonce'] for tx in self.rpc.sent],[0,1,2])
        self.assertTrue(all(tx['target']==self.target.lower() for tx in self.rpc.sent))
        self.assertEqual(self.store.load_records(),[])
        self.assertIn('20 USDT',self.owner.result_dialogs[-1]['detail_text'])
        self.assertIn('30 USDC',self.owner.result_dialogs[-1]['detail_text'])
        self.assertNotIn(self.rpc.old_hash,self.rpc.receipt_queries)

    def test_individual_token_dust_is_skipped_before_native_sweep(self):
        row = self.record()
        self.fund(row,usdt=499_999_999_999_999,usdc=500_000_000_000_000)
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],[self.usdc.contract.lower(),'BNB'])
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(self.rpc.tokens[(self.address.lower(),self.usdt.contract.lower())],499_999_999_999_999)

    def test_only_token_dust_recovers_bnb_directly(self):
        row = self.record()
        self.fund(row,usdt=499_999_999_999_999,usdc=400_000_000_000_000)
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],['BNB'])
        self.assertEqual(self.store.load_records(),[])

    def test_insufficient_fee_goes_to_manual_with_no_submission(self):
        for bnb,usdt in ((0,20*10**18),(10_000_000_000_000,0),(65_000_000_000_000,20*10**18)):
            with self.subTest(bnb=bnb,usdt=usdt):
                row = self.record(batch=str(bnb))
                # Two material tokens require two fees before the first token is sent.
                self.fund(row,bnb=bnb,usdt=usdt,usdc=20*10**18 if bnb==65_000_000_000_000 else 0)
                self.run_recovery(batch=row.batch_id)
                self.assertEqual(self.rpc.sent,[])
                self.assertIn(row.relay_address,[item.relay_address for item in self.store.load_records()])
                self.assertIn('待人工 1',self.owner.result_dialogs[-1]['detail_text'])
                self.assertTrue(any('回收手续费不够' in line for line in self.owner.logs))
                self.assertIn(row.relay_address,(self.root/'manual.txt').read_text())

    def test_lost_reply_keeps_identity_and_restart_does_not_repay(self):
        row = self.record()
        self.fund(row)
        self.rpc.lose_reply = True
        self.run_recovery()
        saved = RelayWalletFileStore(self.store.file_path).load_records()[0]
        self.assertEqual(saved.token_forward_txid,self.rpc.old_hash)
        self.assertEqual(saved.gas_sweep_txid,self.rpc.sent[0]['hash'])
        self.assertEqual(saved.sweep_resolution,'recovery_pending')
        self.run_recovery()
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])

    def test_unconfirmed_recovery_preserves_wallet_until_own_receipt_arrives(self):
        row = self.record()
        self.fund(row)
        self.rpc.pending_receipt = True
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.run_recovery()
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(len(self.store.load_records()),1)
        self.rpc.pending_receipt = False
        self.run_recovery()
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])

    def test_partial_token_failure_is_manual_and_preserves_remaining_funds(self):
        row = self.record()
        self.fund(row,usdt=20*10**18,usdc=30*10**18)
        self.rpc.fail_contract = self.usdt.contract.lower()
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],[self.usdc.contract.lower(),self.usdt.contract.lower()])
        saved = self.store.load_records()[0]
        self.assertEqual(saved.token_forward_txid,self.rpc.old_hash)
        self.assertEqual(self.rpc.tokens[(self.address.lower(),self.usdt.contract.lower())],20*10**18)
        self.assertEqual(self.rpc.tokens[(self.target.lower(),self.usdc.contract.lower())],30*10**18)
        self.assertIn('待人工 1',self.owner.result_dialogs[-1]['detail_text'])

    def test_multiple_accounts_and_selected_batch_stay_isolated(self):
        first,second = self.record(batch='first'),self.record(key='2'*64,batch='second')
        self.fund(first,usdt=20*10**18)
        self.fund(second,usdc=30*10**18)
        self.run_recovery(batch='first')
        self.assertEqual([row.batch_id for row in self.store.load_records()],['second'])
        self.assertTrue(all(tx['sender']==first.relay_address.lower() for tx in self.rpc.sent))
        other_target = '0x'+'8'*40
        self.run_recovery(target=other_target)
        self.assertEqual(self.store.load_records(),[])
        self.assertTrue(all(tx['target']==other_target.lower() for tx in self.rpc.sent if tx['sender']==second.relay_address.lower()))

    def test_duplicate_records_do_not_send_twice_from_the_same_wallet(self):
        first = self.record(batch='first')
        self.record(batch='second')
        self.fund(first)
        self.run_recovery()
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])

    def test_duplicate_wallet_waits_for_submitted_token_before_sweeping_bnb(self):
        first = self.record(batch='first')
        self.record(batch='second')
        self.fund(first,usdt=20*10**18,usdc=30*10**18)
        self.rpc.pending_receipt = True
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],[self.usdc.contract.lower()])
        self.assertEqual(len(self.store.load_records()),2)
        # A selected batch must also respect the same wallet's actual pending recovery.
        self.run_recovery(batch='second')
        self.assertEqual(len(self.rpc.sent),1)
        self.rpc.pending_receipt = False
        self.run_recovery()
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],
                         [self.usdc.contract.lower(),self.usdt.contract.lower(),'BNB'])
        self.assertEqual(self.store.load_records(),[])

    def test_restart_rejects_receipt_for_a_different_recovery_transaction(self):
        row = self.record()
        self.fund(row)
        self.rpc.pending_receipt = True
        self.run_recovery()
        expected = self.rpc.sent[0]['hash']
        self.rpc.receipts[expected]['transactionHash'] = '0x' + 'a'*64
        self.rpc.pending_receipt = False
        self.run_recovery()
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(self.store.load_records()[0].sweep_resolution,'recovery_pending')
        self.assertEqual(len(self.rpc.sent),1)

    def test_dry_run_and_stop_do_not_broadcast_or_delete(self):
        row = self.record()
        self.fund(row,usdt=20*10**18)
        self.run_recovery(dry=True)
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(len(self.store.load_records()),1)
        self.owner.stop_requested.set()
        self.run_recovery()
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(len(self.store.load_records()),1)

    def test_wallet_write_failure_prevents_broadcast(self):
        row = self.record()
        self.fund(row)
        with patch.object(self.store,'flush',side_effect=OSError('disk failure')), \
             patch('onchain_relay_runner.messagebox.showerror'):
            self.run_recovery()
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(self.rpc.native[self.address.lower()],10**15)
        self.assertNotIn(self.key,self.store.file_path.read_text())


if __name__ == '__main__':
    unittest.main()
