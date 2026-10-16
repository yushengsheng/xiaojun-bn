"""Real password wrapping and encrypted data, with only service/RPC transport replaced."""
from pathlib import Path
import logging
import io
import shutil
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from linux_exchange import deploy, install
from linux_exchange.worker import Service


def main():
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary).resolve()
        app, data, config, units = (root / name for name in ('app', 'data', 'config', 'units'))
        for path in (app, data, config, units):
            path.mkdir()
        (config/'server.env').write_text('XIAOJUN_PUBLIC_ORIGIN=https://panel.example.com:26483\n')
        shutil.copyfile(Path(__file__).resolve().parents[1]/'linux_exchange/deploy'/deploy.WEB, units/deploy.WEB)
        setup = data / 'setup-token'; setup.write_text('disposable-initialization-token')
        holder = [Service(data, setup)]
        from operation_journal import OperationJournal
        commands = []
        def rpc(method, payload=None, token='', csrf='', **_kwargs):
            return holder[0].dispatch(dict(method=method, payload=payload or {}, token=token, csrf=csrf))
        def systemctl(*args):
            commands.append(args)
            if args[0] == 'stop' and deploy.WORKER in args:
                runtime = holder[0].runtime
                if runtime:
                    runtime.close_idle()
            if args[0] == 'start' and deploy.WORKER in args:
                holder[0] = Service(data, setup)
            return ''
        with patch.object(install, 'ROOT', app), patch.object(install, 'DATA', data), \
                patch.object(install, 'CONFIG', config), patch.object(install, 'UNITS', units), \
                patch.object(deploy, 'call', side_effect=rpc), patch.object(deploy, 'systemctl', side_effect=systemctl), \
                patch.object(deploy.urllib.request, 'urlopen', side_effect=lambda *_a, **_kw: io.BytesIO(b'{"configured":true}')):
            first = 'first-custom-password'
            args = SimpleNamespace(new_password=first, old_password='')
            deploy.apply_password(args)
            assert deploy.health() == dict(configured=True, unlocked=False, busy=False)
            assert not setup.exists()
            session = deploy.password_login(first)
            deploy.password_rpc('accounts', {'accounts': [dict(api_key='fixture-key-for-password-test',
                api_secret='fixture-secret-for-password-test', selected=True)]}, session)
            journal_path = data / 'password-test-operations.json'
            journal = OperationJournal(journal_path, secret_box=holder[0].lock.secret_box)
            entry = journal.begin('exchange-order', 'fixture-account', {'order_id': 'fixture-order'})
            protected = {p: p.read_bytes() for p in data.glob('*.json*') if not p.name.startswith('app_lock.json')}
            assert b'fixture-secret-for-password-test' not in (data / 'accounts.json').read_bytes()
            deploy.password_rpc('lock', {'password':first, 'confirmed':True}, session)
            systemctl('stop', deploy.WORKER); systemctl('start', deploy.WORKER)
            second = 'second-custom-password'
            args = SimpleNamespace(new_password=second, old_password='')
            with patch.object(deploy.getpass, 'getpass', return_value=first) as prompt:
                deploy.validate_current_password(args)
                prompt.assert_called_once()
            assert deploy.health()['unlocked'] is False
            deploy.apply_password(args)
            assert all(p.read_bytes() == content for p, content in protected.items())
            systemctl('stop', deploy.WORKER); systemctl('start', deploy.WORKER)
            try:
                deploy.password_login(first)
                raise AssertionError('old password still works')
            except RuntimeError as exc:
                assert getattr(exc, 'code', None) == 401
            session = deploy.password_login(second)
            records = holder[0].runtime.accounts
            assert records[0]['api_secret'] == 'fixture-secret-for-password-test'
            assert OperationJournal(journal_path, secret_box=holder[0].lock.secret_box).active_entries()[0]['id'] == entry
            deploy.password_rpc('lock', {'password':second, 'confirmed':True}, session)
            before = {p:p.read_bytes() for p in data.glob('*.json*')}
            commands.clear()
            deploy.apply_password(SimpleNamespace(new_password='', old_password=''))
            assert commands == [] and all(p.read_bytes() == content for p, content in before.items())
            # The server commits a new password but its reply is lost. Restore the
            # original encrypted wrapper and prove the original data still opens.
            def lost_reply(method, *args, **kwargs):
                result = rpc(method, *args, **kwargs)
                if method == 'change_password':
                    raise OSError('simulated lost reply after password write')
                return result
            with patch.object(deploy, 'call', side_effect=lost_reply):
                try:
                    deploy.apply_password(SimpleNamespace(new_password='third-attempted-password', old_password=second))
                    raise AssertionError('injected failure accepted')
                except ValueError as exc:
                    assert '原有配置已保留' in str(exc)
            assert not (app / '.password-incomplete.json').exists()
            assert all(p.read_bytes() == content for p, content in before.items())
            session = deploy.password_login(second)
            assert holder[0].runtime.accounts[0]['api_secret'] == 'fixture-secret-for-password-test'
            assert OperationJournal(journal_path, secret_box=holder[0].lock.secret_box).active_entries()[0]['id'] == entry
            deploy.password_rpc('lock', {'password':second, 'confirmed':True}, session)
            import app_lock
            write = app_lock._atomic_write_json_file
            def failed_password_backup(path, payload):
                if str(path).endswith('.bak'):
                    raise OSError('simulated password backup write failure')
                return write(path, payload)
            with patch.object(app_lock, '_atomic_write_json_file', side_effect=failed_password_backup):
                try:
                    deploy.apply_password(SimpleNamespace(new_password='fourth-attempted-password', old_password=second))
                    raise AssertionError('Incomplete password backup accepted')
                except ValueError as exc:
                    assert '主文件与备份未完整保存' in str(exc)
            assert all(p.read_bytes() == content for p, content in before.items())
            session = deploy.password_login(second)
            assert holder[0].runtime.accounts[0]['api_secret'] == 'fixture-secret-for-password-test'
            deploy.password_rpc('lock', {'password':second, 'confirmed':True}, session)
        logging.shutdown()
        print('LINUX PASSWORD TRANSACTION OK: initialization, change, blank retention, reply loss/backup failure rollback, encrypted accounts and journal')


if __name__ == '__main__':
    main()
