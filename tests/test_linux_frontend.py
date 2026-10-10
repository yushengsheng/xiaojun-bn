"""Linux panel DOM contract and offline real-browser interaction smoke; no financial API calls."""
from __future__ import annotations

import json
import os
import signal
from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "linux_exchange" / "static"


class Ids(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = []

    def handle_starttag(self, _tag, attrs):
        self.ids.extend(value for name, value in attrs if name == "id")


def browser_binary():
    candidates = [
        shutil.which("google-chrome"), shutil.which("google-chrome-stable"),
        shutil.which("chromium"), shutil.which("chromium-browser"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" if sys.platform == "darwin" else None,
    ]
    return next((str(p) for p in candidates if p and Path(p).is_file()), None)


class LinuxFrontendTests(unittest.TestCase):
    def test_static_and_generated_controls_have_matching_references(self):
        html = (STATIC / "index.html").read_text(encoding="utf-8")
        js = (STATIC / "app.js").read_text(encoding="utf-8")
        parser = Ids()
        parser.feed(html)
        ids = set(parser.ids)
        self.assertEqual(len(parser.ids), len(ids), "重复的 HTML id")
        generated = set(re.findall(r"\.id='([^']+)'", js))
        from linux_exchange.runtime import field_schema
        generated = {value for value in generated if not value.endswith("-")}
        generated.update("field-" + field["name"] for field in field_schema())
        from exchange_auto_ladder import AutoLadderSettings
        generated.update("auto-" + name for name in AutoLadderSettings.__dataclass_fields__)
        referenced = set(re.findall(r"\$\('([^']+)'\)", js))
        # All fixed-id loop lists also dereference each element via $(id).
        for array in re.findall(r"for\(const id of \[([^\]]+)\]\)", js):
            referenced.update(re.findall(r"'([^']+)'", array))
        self.assertFalse(referenced - ids - generated,
                         "脚本引用了未定义的 HTML/动态控件: " + repr(referenced - ids - generated))
        self.assertNotIn("operation-review", js)
        for target in re.findall(r'data-close="([^"]+)"', html):
            self.assertIn(target, ids)

    def test_browser_renders_idle_and_busy_and_opens_dialogs(self):
        chrome = browser_binary()
        if not chrome:
            if os.environ.get("GITHUB_ACTIONS") == "true" and sys.platform.startswith("linux"):
                self.fail("Linux 发布 runner 缺少 Chrome，无法完成网页按钮交互验收")
            self.skipTest("本机无 Chrome；静态 DOM 契约测试仍执行")
        from linux_exchange.runtime import field_schema
        from exchange_auto_ladder import AutoLadderSettings
        schema = field_schema()
        settings = {field["name"]: field["default"] for field in schema}
        fixture = {
            "server_time": 1, "boot": "smoke", "seq": 0, "logs": [],
            "status": "状态：就绪", "summary": "", "progress": {"value": 0, "maximum": 1},
            "interrupted": False, "settings": settings,
            "auto_ladder": AutoLadderSettings().as_dict(),
            "accounts": [{"id": "fixture-id", "index": 1, "api_key": "test-public-key",
                          "address": "", "network": "BSC", "selected": True, "bnb_topup": True,
                          "status": "就绪", "active": False}],
            "assets": {}, "busy": False, "can_retry": False,
            "proxy_configured": False, "maintenance": {"ready": True, "free_bytes": 1073741824},
        }
        mock = """<script>
const fakeState = JSON.parse(document.getElementById('fixture-state').textContent);
const fakeSchema = JSON.parse(document.getElementById('fixture-schema').textContent);
const errors = [];
window.addEventListener('error', e => errors.push(e.message));
window.addEventListener('unhandledrejection', e => errors.push(String(e.reason)));
window.fetch = async (url, options) => {
 if(options && options.method === 'POST') throw Error('smoke forbids mutations');
 const path=String(url);
 const value=path.endsWith('/health')?{configured:true}:
   path.endsWith('/session')?{csrf:'test',schema:fakeSchema}:fakeState;
 return {ok:true,json:async()=>value};
};
window.EventSource = class { addEventListener() {} close() {} };
function finish(result){let marker=document.createElement('meta');marker.id='smoke-result';marker.dataset.result=result;document.head.append(marker);}
document.addEventListener('DOMContentLoaded',()=>setTimeout(()=>{
 try {
  if(!document.getElementById('field-trade_mode'))throw Error('settings not rendered');
  if(document.getElementById('console').hidden)throw Error('console still hidden');
  if(document.getElementById('accounts').children.length!==1)throw Error('account missing');
  const type=document.getElementById('field-trade_account_type');
  const mode=document.getElementById('field-trade_mode');
  const stopMode=document.getElementById('field-market_stop_mode');
  for(const option of type.options){
   type.value=option.value;
   for(const choice of (option.value==='现货'?mode.options:[{value:mode.value}])){
    mode.value=choice.value;
    for(const stop of (choice.value==='市价'?stopMode.options:[{value:stopMode.value}])){
     stopMode.value=stop.value;layoutSettings();
     if(!document.getElementById('settings-row2').children.length)throw Error('empty '+type.value+'/'+mode.value);
     if(type.value==='现货'&&mode.value==='自动分仓挂单'&&document.getElementById('auto-open').parentElement.id!=='settings-row2')throw Error('auto settings not visible');
     if(type.value==='股票'&&document.getElementById('stock-open').parentElement.id!=='settings-row2')throw Error('stock settings not visible');
    }
   }
  }
  for(const id of ['auto-open','stock-open','volume-open','server-details','show-assets']){
   if(id==='auto-open'){type.value='现货';mode.value='自动分仓挂单';layoutSettings();}
   if(id==='stock-open'){type.value='股票';layoutSettings();}
   const button=document.getElementById(id);button.click();
   const target=({'auto-open':'auto-dialog','stock-open':'stock-dialog','volume-open':'volume-dialog',
                  'server-details':'server-dialog','show-assets':'assets-dialog'})[id];
   if(!document.getElementById(target).open)throw Error(id+' did not open');
   document.getElementById(target).close();
  }
  document.querySelector('.row-menu-button').click();
  if(document.getElementById('account-menu').hidden)throw Error('account menu did not open');
  render({...fakeState,server_time:2,busy:true});
  if(!document.getElementById('lock').disabled)throw Error('busy lock not disabled');
  if(document.getElementById('auto-open').disabled)throw Error('hot settings unexpectedly disabled');
  render({...fakeState,server_time:3,busy:false});
  if(document.getElementById('lock').disabled)throw Error('idle lock still disabled');
  if(errors.length)throw Error(errors.join(' / '));
  finish('ok');
 }catch(e){finish('error: '+e.message)}
},100));
</script>"""
        with tempfile.TemporaryDirectory(prefix="linux-frontend-") as tmp:
            root = Path(tmp)
            script = (STATIC / "app.js").read_bytes()
            (root / "app.js").write_bytes(script)
            (root / "style.css").write_bytes((STATIC / "style.css").read_bytes())
            html = (STATIC / "index.html").read_text(encoding="utf-8")
            data = (f'<script id="fixture-state" type="application/json">{json.dumps(fixture, ensure_ascii=False)}</script>'
                    f'<script id="fixture-schema" type="application/json">{json.dumps(schema, ensure_ascii=False)}</script>')
            html = html.replace('<link rel="stylesheet" href="/style.css">',
                                '<link rel="stylesheet" href="style.css">')
            html = html.replace('<script defer src="/app.js"></script>', data + mock + '<script defer src="app.js"></script>')
            page = root / "index.html"
            page.write_text(html, encoding="utf-8")
            command = [chrome, "--headless=new", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
                       "--no-first-run", "--disable-background-networking", f"--user-data-dir={root / 'profile'}",
                       "--virtual-time-budget=3000", "--dump-dom", page.as_uri()]
            stdout, stderr = root / "chrome.stdout", root / "chrome.stderr"
            with stdout.open("w+") as out, stderr.open("w+") as err:
                process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
                try:
                    for _ in range(120):
                        if 'id="smoke-result"' in stdout.read_text(encoding="utf-8", errors="replace") or process.poll() is not None:
                            break
                        time.sleep(0.1)
                    else:
                        self.fail("Chrome 未完成网页渲染；" + stderr.read_text(errors="replace")[-800:])
                    exited = process.poll() is not None
                finally:
                    # Some macOS Chrome builds keep the --dump-dom process alive
                    # after printing; terminate only this isolated browser group.
                    if process.poll() is None:
                        if os.name == "posix":
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass  # Browser exited between poll and signal.
                        else:
                            process.kill()
                    process.wait(timeout=5)
                out.seek(0)
                err.seek(0)
                document, diagnostics = out.read(), err.read()
            if exited:
                self.assertEqual(process.returncode, 0, diagnostics[-800:])
            match = re.search(r'<meta id="smoke-result" data-result="([^"]+)"', document)
            self.assertIsNotNone(match, document[-1600:] + diagnostics[-500:])
            self.assertEqual(match.group(1), "ok", diagnostics[-500:])


if __name__ == "__main__":
    unittest.main()
