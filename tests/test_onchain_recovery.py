"""Exercise balance-based recovery with real signing and an encrypted wallet store."""
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import requests
from eth_account import Account
from eth_account._utils.legacy_transactions import Transaction
from api_clients import EvmClient
from onchain_relay_runner import (
    _RelayBatchItem, _build_resume_candidates, _pick_or_create_relay_records, _relay_submit_auto_sweep,
    _relay_recovery_records, _relay_write_failed_export_file, run_relay_fee_recovery,
)
from onchain_transfer_runner import OnchainTransferRunnerMixin
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
            self.sent.append({'sender':sender,'target':target.lower(),'asset':asset,'units':units,'nonce':tx['nonce'],'hash':txid,
                              'network':network,'chain_id':(tx['v']-35)//2})
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
        self.failed_path = self.root / 'failed.txt'
        failed_patch = patch('onchain_relay_runner.RELAY_FAILED_EXPORT_FILE', self.failed_path)
        failed_patch.start()
        self.addCleanup(failed_patch.stop)
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

    def record(self, *, key=None, batch='batch', old_hash=True, persist=True):
        key = key or self.key
        address = self.client.address_from_private_key(key)
        row = RelayWalletRecord(created_at='',expires_at='',network='BSC',
            source='0x' + '2' * 40,target='0x' + '3' * 40,relay_address=address,private_key=key,
            status='failed',batch_id=batch,token_is_native=False,token_symbol='USDT',
            token_contract=self.usdt.contract,token_forward_txid=self.rpc.old_hash if old_hash else '')
        if persist:
            self.store.append_records([row])
        return row

    def fund(self, record, *, bnb=10**15, usdt=0, usdc=0):
        address = record.relay_address.lower()
        self.rpc.native[address] = bnb
        self.rpc.tokens[(address,self.usdt.contract.lower())] = usdt
        self.rpc.tokens[(address,self.usdc.contract.lower())] = usdc

    def run_recovery(self, *, dry=False, batch=None, target=None, failed_only=False, through_button=False,
                     network='BSC', before_worker=None):
        def wait(seconds):
            self.clock += seconds
            return False
        with patch('onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE',self.root/'manual.txt'), \
             patch('onchain_relay_runner.RELAY_SWEEP_LOG_FILE',self.root/'sweep.jsonl'), \
             patch('onchain_relay_runner.time.monotonic',side_effect=lambda:self.clock), \
             patch.object(self.owner.stop_requested,'wait',side_effect=wait), _close_sweep_log_after():
            if through_button:
                self.owner.dry_run_var = Mock(get=Mock(return_value=dry))
                self.owner.network_var = Mock(get=Mock(return_value=network))
                self.owner._ask_relay_fee_recovery_confirm = Mock(return_value=target if target is not None else self.target)
                self.owner._launch_relay_fee_recovery = lambda **kwargs: OnchainTransferRunnerMixin._launch_relay_fee_recovery(self.owner, **kwargs)
                def start_worker(fn, args, kwargs, **_):
                    if before_worker is not None: before_worker()
                    return fn(*args, **kwargs, use_progress=False)
                self.owner._start_managed_thread = Mock(side_effect=start_worker)
                OnchainTransferRunnerMixin.start_failed_relay_fee_recovery(self.owner)
            else:
                run_relay_fee_recovery(self.owner,dry,batch_ids={batch} if batch else None,
                    use_progress=False,manage_running_state=False,sweep_target_override=target if target is not None else self.target,
                    failed_only=failed_only,failed_network=network)

    def export_failed(self, *rows):
        _relay_write_failed_export_file(list(rows))

    def restart_store(self):
        self.store = RelayWalletFileStore(self.store.file_path)
        self.owner.relay_wallet_store = self.rpc.store = self.store

    def test_failed_recovery_button_uses_original_records_and_only_selected_wallets(self):
        row = self.record()
        other = self.record(key='2'*64, batch='other')
        self.store.update_record(other.relay_address,batch_id=other.batch_id,status='completed')
        self.fund(row,usdt=20*10**18)
        self.fund(other,usdc=30*10**18)
        self.export_failed(row)
        self.store.update_record(row.relay_address,batch_id=row.batch_id,status='completed')
        # The export is a selector, not a replacement signing key.
        self.failed_path.write_text(f'{row.relay_address.upper()} - ignored-export-key\n')
        manual_path = self.root/'manual.txt'
        manual_path.write_text(f'{row.relay_address} - {row.private_key}\n{other.relay_address} - {other.private_key}\n')
        self.run_recovery(through_button=True)
        self.assertEqual([r.batch_id for r in self.store.load_records()],['other'])
        self.assertTrue(all(tx['sender']==row.relay_address.lower() for tx in self.rpc.sent))
        self.assertTrue(all(tx['target']==self.target.lower() for tx in self.rpc.sent))
        self.assertFalse(self.failed_path.exists())
        self.assertNotIn(row.relay_address,manual_path.read_text())
        self.assertIn(other.relay_address,manual_path.read_text())
        self.assertNotIn(self.rpc.old_hash,self.rpc.receipt_queries)
        self.assertNotIn(row.private_key,self.store.file_path.read_text())

    def test_failed_recovery_can_retry_after_rpc_failure_and_restart(self):
        row = self.record()
        self.fund(row)
        self.export_failed(row)
        with patch.object(self.client,'_rpc_call',side_effect=requests.ConnectionError('RPC unavailable')):
            self.run_recovery(failed_only=True)
        self.assertEqual(self.rpc.sent,[])
        self.assertIn(row.relay_address,self.failed_path.read_text())
        self.assertEqual(len(self.store.load_records()),1)
        self.restart_store()
        other_target='0x'+'8'*40
        self.run_recovery(failed_only=True,target=other_target)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.rpc.sent[0]['sender'],row.relay_address.lower())
        self.assertEqual(self.rpc.sent[0]['target'],other_target.lower())
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())

    def test_failed_partial_token_recovery_retries_only_remaining_balance(self):
        row = self.record()
        self.fund(row,usdt=20*10**18,usdc=30*10**18)
        self.export_failed(row)
        self.rpc.fail_contract = self.usdt.contract.lower()
        self.run_recovery(failed_only=True)
        self.assertIn(row.relay_address,self.failed_path.read_text())
        self.assertIn(row.relay_address,(self.root/'manual.txt').read_text())
        self.restart_store()
        self.rpc.fail_contract = None
        self.run_recovery(failed_only=True)
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],
                         [self.usdc.contract.lower(),self.usdt.contract.lower(),self.usdt.contract.lower(),'BNB'])
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())
        self.assertFalse((self.root/'manual.txt').exists())

    def test_failed_pending_recovery_preserves_documents_and_never_submits_twice(self):
        row = self.record()
        self.fund(row)
        self.export_failed(row)
        self.rpc.lose_reply = True
        self.rpc.pending_receipt = True
        self.run_recovery(failed_only=True)
        self.assertIn(row.relay_address,self.failed_path.read_text())
        self.restart_store()
        self.run_recovery(failed_only=True)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertTrue(self.failed_path.exists())
        self.rpc.pending_receipt = False
        self.run_recovery(failed_only=True)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())

    def test_failed_export_cleanup_obeys_zero_dust_and_exact_margin_boundaries(self):
        for index,(bnb,usdt,cleared) in enumerate(((0,0,True),(9_999_999_999_999,499_999_999_999_999,True),
                                                  (10_000_000_000_000,0,False),(0,500_000_000_000_000,False))):
            with self.subTest(index=index):
                row = self.record(key=f'{index+3:064x}',batch=str(index))
                self.fund(row,bnb=bnb,usdt=usdt)
                self.export_failed(row)
                self.run_recovery(batch=row.batch_id,failed_only=True)
                self.assertEqual(row.relay_address not in [r.relay_address for r in self.store.load_records()],cleared)
                text=self.failed_path.read_text() if self.failed_path.exists() else ''
                self.assertEqual(row.relay_address not in text,cleared)
        self.assertEqual(self.rpc.sent,[])

    def test_failed_exports_survive_dry_run_stop_and_wallet_write_failure(self):
        row = self.record()
        self.fund(row,bnb=0)
        self.export_failed(row)
        manual=self.root/'manual.txt'
        manual.write_bytes(self.failed_path.read_bytes())
        before=(self.failed_path.read_bytes(),manual.read_bytes())
        self.run_recovery(dry=True,failed_only=True)
        self.owner.stop_requested.set()
        self.run_recovery(failed_only=True)
        self.owner.stop_requested.clear()
        with patch.object(self.store,'remove_records',side_effect=OSError('disk failure')), \
             patch('onchain_relay_runner.messagebox.showerror'):
            self.run_recovery(failed_only=True)
        self.assertEqual((self.failed_path.read_bytes(),manual.read_bytes()),before)
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(self.rpc.sent,[])

    def test_failed_document_keeps_other_batches_same_wallet_until_all_are_checked(self):
        row=self.record(batch='first')
        self.record(batch='second')
        self.fund(row,bnb=0)
        self.export_failed(row)
        self.run_recovery(batch='first',failed_only=True)
        self.assertTrue(self.failed_path.exists())
        self.assertEqual([r.batch_id for r in self.store.load_records()],['second'])
        self.run_recovery(failed_only=True)
        self.assertFalse(self.failed_path.exists())

    def test_stop_during_failed_recovery_does_not_delete_wallet_or_document_entries(self):
        row=self.record()
        self.fund(row,bnb=0)
        self.export_failed(row)
        before=self.failed_path.read_bytes()
        def rpc(network,method,args,**kwargs):
            if method=='eth_getBalance': self.owner.stop_requested.set()
            return self.rpc(network,method,args,**kwargs)
        with patch.object(self.client,'_rpc_call',side_effect=rpc):
            self.run_recovery(failed_only=True)
        self.assertEqual(len(self.store.load_records()),1)
        self.assertEqual(self.failed_path.read_bytes(),before)
        self.assertEqual(self.rpc.sent,[])

    def test_failed_status_remains_retryable_when_export_document_was_missing(self):
        row=self.record()
        self.fund(row)
        self.run_recovery(failed_only=True)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])

    def test_invalid_document_wallet_is_not_imported_and_requires_an_explicit_network(self):
        self.failed_path.write_text('0x'+'4'*40+' - not-a-wallet-key\n')
        before=self.failed_path.read_bytes()
        with self.assertRaisesRegex(RuntimeError,'选择当前回收网络'):
            _relay_recovery_records(self.owner,failed_only=True)
        with patch('onchain_relay_runner.messagebox.showerror'):
            self.run_recovery(failed_only=True)
        self.assertEqual(self.failed_path.read_bytes(),before)
        self.assertEqual(self.rpc.sent,[])

    def test_document_only_wallet_is_imported_encrypted_and_recovered_through_button(self):
        row=self.record(persist=False)
        self.fund(row,usdt=20*10**18,usdc=30*10**18)
        self.export_failed(row)
        self.run_recovery(through_button=True)
        self.assertTrue(self.owner._ask_relay_fee_recovery_confirm.call_args.kwargs['require_target'])
        self.assertEqual([tx['asset'] for tx in self.rpc.sent],[self.usdc.contract.lower(),self.usdt.contract.lower(),'BNB'])
        self.assertTrue(all(tx['sender']==row.relay_address.lower() for tx in self.rpc.sent))
        self.assertTrue(all(tx['target']==self.target.lower() for tx in self.rpc.sent))
        self.assertFalse(self.failed_path.exists())
        self.assertEqual(self.store.load_records(),[])
        self.assertNotIn(row.private_key,self.store.file_path.read_text())

    def test_all_41_document_only_wallets_can_be_processed_without_original_records(self):
        rows=[self.record(key=f'{i:064x}',batch=str(i),persist=False) for i in range(1,42)]
        self.export_failed(*rows)
        self.run_recovery(through_button=True)
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())
        self.assertIn('已清空 41',self.owner.result_dialogs[-1]['detail_text'])

    def test_document_import_requires_explicit_destination_before_writing_or_broadcasting(self):
        row=self.record(persist=False)
        self.fund(row)
        self.export_failed(row)
        before=self.failed_path.read_bytes()
        with patch('onchain_transfer_runner.messagebox.showerror') as error, \
             patch.object(self.client,'_rpc_call',wraps=self.rpc) as rpc:
            self.run_recovery(through_button=True,target='')
        error.assert_called_once()
        rpc.assert_not_called()
        self.owner._start_managed_thread.assert_not_called()
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(self.failed_path.read_bytes(),before)

    def test_document_import_lost_reply_reuses_persisted_identity_after_restart(self):
        row=self.record(persist=False)
        self.fund(row)
        self.export_failed(row)
        self.rpc.lose_reply=True
        self.rpc.pending_receipt=True
        self.run_recovery(failed_only=True)
        saved=self.store.load_records()[0]
        self.assertEqual(saved.network,'BSC')
        self.assertEqual(saved.source,'')
        self.assertEqual(saved.sweep_target,self.target)
        self.assertEqual(saved.gas_sweep_txid,self.rpc.sent[0]['hash'])
        self.assertEqual(saved.sweep_resolution,'recovery_pending')
        self.restart_store()
        self.run_recovery(failed_only=True)
        self.assertEqual(self.store.load_records()[0].batch_id,saved.batch_id)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertTrue(self.failed_path.exists())
        self.rpc.pending_receipt=False
        self.run_recovery(failed_only=True)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertFalse(self.failed_path.exists())

    def test_document_wallet_with_unknown_pending_transaction_is_not_sent_or_deleted(self):
        row=self.record(persist=False)
        self.fund(row,bnb=0)
        self.export_failed(row)
        def pending(network,method,args,**kwargs):
            if method=='eth_getTransactionCount' and args[1]=='pending': return '0x1'
            return self.rpc(network,method,args,**kwargs)
        with patch.object(self.client,'_rpc_call',side_effect=pending):
            self.run_recovery(failed_only=True)
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(len(self.store.load_records()),1)
        self.assertTrue(self.failed_path.exists())
        self.assertTrue(any('未确认交易' in line for line in self.owner.logs))
        self.restart_store()
        self.run_recovery(failed_only=True)
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())

    def test_document_import_write_failure_prevents_all_network_operations(self):
        row=self.record(persist=False)
        self.fund(row)
        self.export_failed(row)
        before=self.failed_path.read_bytes()
        with patch.object(self.store,'append_records',side_effect=OSError('disk failure')), \
             patch('onchain_relay_runner.messagebox.showerror'), \
             patch.object(self.client,'_rpc_call',wraps=self.rpc) as rpc:
            self.run_recovery(failed_only=True)
        rpc.assert_not_called()
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(self.failed_path.read_bytes(),before)

    def test_document_preview_freezes_network_wallets_and_selected_custom_token(self):
        row=self.record(persist=False)
        other=self.record(key='2'*64,batch='other',persist=False)
        self.fund(row)
        self.fund(other)
        custom=replace(self.usdt,symbol='CUSTOM',contract='0x'+'7'*40)
        self.rpc.tokens[(row.relay_address.lower(),custom.contract.lower())]=10**18
        self.owner._selected_token=Mock(return_value=custom)
        self.export_failed(row)
        networks=[]
        def rpc(network,method,args,**kwargs):
            networks.append(network)
            return self.rpc(network,method,args,**kwargs)
        def change_after_confirmation():
            self.owner.network_var.get.return_value='ETH'
            self.export_failed(other)
        with patch.object(self.client,'_rpc_call',side_effect=rpc):
            self.run_recovery(through_button=True,before_worker=change_after_confirmation)
        self.assertEqual(set(networks),{'BSC'})
        self.assertTrue(all(tx['sender']==row.relay_address.lower() for tx in self.rpc.sent))
        self.assertEqual(self.rpc.sent[0]['asset'],custom.contract.lower())
        self.assertIn(other.relay_address,self.failed_path.read_text())
        self.assertNotIn(row.relay_address,self.failed_path.read_text())

    def test_document_only_dry_run_and_stop_preserve_original_files(self):
        row=self.record(persist=False)
        self.export_failed(row)
        before=self.failed_path.read_bytes()
        self.run_recovery(dry=True,failed_only=True)
        self.owner.stop_requested.set()
        self.run_recovery(failed_only=True)
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(self.failed_path.read_bytes(),before)
        self.assertEqual(self.rpc.sent,[])

    def test_document_bom_and_duplicate_rows_do_not_duplicate_wallet_recovery(self):
        row=self.record(persist=False)
        self.fund(row)
        line=f'{row.relay_address} - {row.private_key}\n'
        self.failed_path.write_text('\ufeff'+line+line, encoding='utf-8')
        self.run_recovery(failed_only=True)
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.store.load_records(),[])
        self.assertFalse(self.failed_path.exists())

    def test_document_import_uses_selected_ethereum_network_and_signing_chain(self):
        row=self.record(persist=False)
        self.fund(row)
        self.export_failed(row)
        self.run_recovery(through_button=True,network='ETH')
        self.assertEqual(len(self.rpc.sent),1)
        self.assertEqual(self.rpc.sent[0]['network'],'ETH')
        self.assertEqual(self.rpc.sent[0]['chain_id'],1)
        self.assertEqual(self.rpc.sent[0]['target'],self.target.lower())
        self.assertFalse(self.failed_path.exists())

    def test_document_address_key_mismatch_does_not_import_or_expose_credentials(self):
        row=self.record(persist=False)
        wrong_key='2'*64
        self.failed_path.write_text(f'{row.relay_address} - {wrong_key}\n')
        before=self.failed_path.read_bytes()
        with patch('onchain_relay_runner.messagebox.showerror') as error:
            self.run_recovery(failed_only=True)
        self.assertNotIn(wrong_key,str(error.call_args))
        self.assertEqual(self.store.load_records(),[])
        self.assertEqual(self.rpc.sent,[])
        self.assertEqual(self.failed_path.read_bytes(),before)

    def test_new_failures_preserve_previous_failed_wallets(self):
        first=self.record(batch='first')
        second=self.record(key='2'*64,batch='second')
        self.export_failed(first)
        self.export_failed(second)
        self.export_failed(second)
        lines=self.failed_path.read_text().splitlines()
        self.assertEqual(len(lines),2)
        self.assertTrue(any(first.relay_address in line for line in lines))
        self.assertTrue(any(second.relay_address in line for line in lines))

    def test_export_read_or_write_failure_preserves_existing_failed_document(self):
        row=self.record()
        self.export_failed(row)
        before=self.failed_path.read_bytes()
        with patch('onchain_relay_runner._atomic_write_text',side_effect=OSError('disk failure')):
            with self.assertRaises(OSError): self.export_failed(row)
        with patch.object(Path,'read_text',side_effect=OSError('read failure')):
            with self.assertRaises(OSError): self.export_failed(row)
        self.assertEqual(self.failed_path.read_bytes(),before)

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
