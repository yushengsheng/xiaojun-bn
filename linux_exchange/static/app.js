'use strict';
const autoFields=[["volume_minutes", "成交额窗口（分钟）"], ["start_volume", "开始挂单成交额（计价币）"], ["recent_minutes", "近期有成交窗口（分钟）"], ["entry_confirm_seconds", "入场连续确认（秒）"], ["no_trade_seconds", "无成交暂停（秒）"], ["price_minutes", "入场价格窗口（分钟）"], ["price_ticks", "入场最大振幅（tick）"], ["pause_price_ticks", "运行中价格暂停幅度（tick）"], ["dry_minutes", "缩量观察窗口（分钟）"], ["dry_volume", "缩量暂停成交额（计价币）"], ["dry_confirm_minutes", "缩量连续确认（分钟）"], ["turn_seconds", "反向折返窗口（秒）"], ["turn_ticks", "最小反向幅度（tick）"], ["turn_count", "暂停折返次数"], ["stable_seconds", "恢复平稳时长（秒）"]];
const autoHelp={"volume_minutes": "入场看当前正在走的 1 分钟，加上前面已收盘的分钟，合计这么多分钟的成交额。例：填 5，就是当前分钟已发生的成交额＋前 4 个完整分钟；不必等当前分钟收盘。", "start_volume": "只有上述时间段的总成交额达到这个数，才允许首次入场或暂停后恢复。单位是交易对右边的计价币；例如 BTCUSDT 填 100000，就是至少成交 10 万 USDT。", "recent_minutes": "最近这几分钟里至少要有一笔成交，包含当前正在走的分钟。填 1 就要求当前分钟已有成交；这里只检查成交额大于 0，不要求达到启动门槛。", "entry_confirm_seconds": "首次入场前，成交额、价格、盘口与最近成交都必须连续达标这么久，才开始挂单。有一项不达标就重新计时；短暂获取失败会先重试，重试期间暂停计时。填 0 可关闭首次入场等待；暂停后恢复仍由“恢复平稳时长”控制。", "no_trade_seconds": "交易所公布的最近一笔成交距现在达到这么久，就暂时观察并尝试撤销挂单，不等几分钟的缩量窗口。再次出现成交后，还要满足其他条件才能入场或恢复。", "price_minutes": "入场看当前正在走的分钟和前面的完整分钟，取这段时间的最高价减最低价，与“入场最大振幅”比较。填 5 就是当前分钟＋前 4 分钟。", "price_ticks": "入场时允许的价格上下波动上限。tick 是该交易对最小的报价变化单位；如 1 tick=0.001，填 5 就是最近价格最高与最低最多相差 0.005。数值越小越难入场。", "pause_price_ticks": "运行中用这条较宽的线防突发价格风险：最近价格振幅、当前买卖价差，或实时盘口偏离近期价格区间超过这个幅度时，暂停新挂单并尝试撤单。价格重新稳定后仍需满足成交额、近期成交与恢复平稳时长才能再挂单。应不小于入场最大振幅。", "dry_minutes": "运行中用最近这些已经收盘的完整分钟的总成交额判断是否缩量，避免刚进入新分钟时因当前分钟成交尚少而误判。", "dry_volume": "滚动窗口的总成交额低于此值，算一次“缩量”。低于启动门槛但还没低于这里时，已启动的挂单不会仅因成交额下降而暂停。单位同样是计价币。", "dry_confirm_minutes": "需要连续多少次、每次相隔 1 分钟的缩量窗口都低于门槛，才暂停挂单。例：观察 5 分钟、确认 2 分钟，要连续两个重叠的 5 分钟窗口都缩量。", "turn_seconds": "观察最近多少秒的盘口中间价；中间价是买一价与卖一价的平均值。用于识别短时间来回折返。", "turn_ticks": "价格先朝一个方向走，再反向走至少多少 tick，才记作一次折返。小于这个幅度的抖动不计数；单向上涨或下跌也不算折返。", "turn_count": "在上述秒数内，折返次数达到此值就暂停，并尝试撤销当前挂单。数值越小越容易因来回波动暂停。", "stable_seconds": "暂停后，成交额和价格条件持续达标、且没有达到折返次数，保持这么久才重新挂单。短暂行情读取失败会冻结计时，恢复后接着算；连续失联达到 30 秒或条件不达标才重新计时。首次入场由“入场连续确认”控制。"};
const $=id=>document.getElementById(id);
const tasks={run:'批量执行',query_assets:'查询全部总资产',query_volume:'查询成交额',collect_bnb:'归集BNB并提现',withdraw:'批量提现',redeem_earn:'批量赎回理财',earn_auto_subscribe:'自动申购',retry_failed:'失败重试'};
let csrf='',schema=[],state=null,events=null,settingsDirty=false,initialized=false,seq=0,boot='',logs=[];
let highlighted='',contextId='',currentId='',singleReady=false,lastSearch='',lastSummary='',pendingTask=null;
let layoutKey='';
let autoSnapshot=null,stockSnapshot=null,resolveQuestion=null,questionMode='',resolvePassword=null;
const fields=new Map();
const labels={trade_mode:'交易模式',market_stop_mode:'停止方式',bnb_fee_stop:'剩余bnb手续费',bnb_topup_amount:'预购买BNB金额',premium_delta:'溢价',premium_order_count:'笔数',premium_append_threshold:'追加挂单',premium_partial_fill:'部分成交循环',reprice_threshold:'重挂阈值',ladder_layout:'档位模式',ladder_min_pool_notional:'最小分仓成交额',ladder_sell_premium_enabled:'溢价挂卖',ladder_sell_premium:'溢价金额',futures_side:'开仓方向',futures_amount:'下单金额',futures_leverage:'杠杆',earn_amount:'存入数量',earn_amount_all:'全部',earn_auto_subscribe:'自动申购',enable_withdraw:'自动提现',max_threads:'线程数',usdt_timeout:'到账超时(秒)',rate_limit_wait:'限流等待(秒)',acc_network:'网络',exchange_proxy:'交易所代理',use_exchange_config_proxy:'使用配置代理',stock_rounds:'轮次',stock_amount:'每轮金额(留空全部)'};
const setup=new URLSearchParams(location.hash.slice(1)).get('setup')||'';
if(setup){$('setup-token').value=setup;history.replaceState(null,'',location.pathname);}
function node(tag,text,className){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(className)e.className=className;return e;}
function feedback(text){$('feedback').textContent=text||'';$('feedback').hidden=!text;}
function notice(text,title='提示'){$('notice-title').textContent=title;$('notice-message').textContent=text;if(!$('notice-dialog').open)$('notice-dialog').showModal();}
function guard(fn){return async(...args)=>{try{await fn(...args);}catch(e){feedback(e.message);notice(e.message,'错误');}};}
async function api(method,payload){
 const options={credentials:'same-origin',cache:'no-store'};
 if(payload!==undefined){options.method='POST';options.headers={'Content-Type':'application/json','X-CSRF-Token':csrf};options.body=JSON.stringify(payload);}
 let response;
 try{response=await fetch('/api/'+method,options);}catch(e){const error=Error('服务器连接中断，请刷新状态确认；不要重复创建同一交易指令。');error.transport=true;throw error;}
 const value=await response.json();
 if(!response.ok){if(response.status===401&&method!=='login'&&!['export_accounts','destroy_accounts','lock','change_password'].includes(method))signedOut();const error=Error(value.error||'请求失败');error.transport=response.status>=500;throw error;}
 return value;
}
function closeMenu(){$('account-menu').hidden=true;}
function signedOut(){
 if(events)events.close();events=null;csrf='';state=null;initialized=false;settingsDirty=false;fields.clear();layoutKey='';highlighted=contextId=currentId='';singleReady=false;pendingTask=null;logs=[];seq=0;boot='';
 closeMenu();if(resolveQuestion)finishQuestion(false);if(resolvePassword)finishPassword(null);
 for(const dialog of document.querySelectorAll('dialog[open]'))dialog.close();
 for(const id of ['password','import-text','edit-secret','edit-key','single-key','single-secret','single-address'])$(id).value='';
 $('single-key').placeholder='';$('accounts').replaceChildren();$('logs').textContent=$('error-logs').textContent='';$('console').hidden=$('top-controls').hidden=true;$('login').hidden=false;setConnection('等待解锁');
}
function setConnection(text,online=false){$('connection').textContent=text;$('connection').classList.toggle('online',online);}
async function connected(){
 const session=await api('session');csrf=session.csrf;schema=session.schema;
 $('console').hidden=$('top-controls').hidden=false;$('login').hidden=true;$('password').value=$('setup-token').value='';
 await refresh();if(events)events.close();events=new EventSource('/api/events?since='+seq);
 events.addEventListener('state',e=>{render(JSON.parse(e.data));setConnection('实时连接',true);});
 events.addEventListener('problem',e=>{const p=JSON.parse(e.data);feedback(p.error);if(p.code===401)signedOut();else setConnection('执行服务暂不可用');});
 events.onerror=()=>setConnection('连接中断，正在重连');
}
async function refresh(){render(await api('state?since='+seq));}
function inputValue(name){const input=$('field-'+name);return input.type==='checkbox'?input.checked:input.value;}
function dirty(){settingsDirty=true;$('settings-hint').textContent='配置已修改';}
function buildSettings(){
 fields.clear();layoutKey='';for(const id of ['settings-row1','settings-row2','settings-row3','settings-cache','threads-field','proxy-field','stock-settings'])$(id).replaceChildren();
 for(const f of schema){
  const label=node('label',undefined,'field'+(f.kind==='boolean'?' boolean':'')),text=node('span',labels[f.name]||f.label,'field-text');let input;
  if(f.options){input=node('select');for(const v of f.options){const option=node('option',f.name==='futures_margin_type'?({CROSSED:'全仓',ISOLATED:'逐仓'}[v]||v):v);option.value=v;input.append(option);}}
  else{input=node('input');input.type=f.kind==='boolean'?'checkbox':f.name==='exchange_proxy'?'password':'text';input.autocomplete='off';}
  input.id='field-'+f.name;input.setAttribute('form','settings-form');input.addEventListener('input',()=>{if(!f.name.startsWith('stock_')||!$('stock-dialog').open)dirty();if(['trade_account_type','trade_mode','market_stop_mode','earn_amount_all','ladder_sell_premium_enabled'].includes(f.name))layoutSettings();});
  if(f.kind==='boolean')label.append(input,text);else label.append(text,input);
  if(f.name.endsWith('symbol')||f.name==='stock_symbol')label.classList.add('field-symbol','accent-text');
  if(f.name==='enable_withdraw')label.classList.add('danger-text');
  label.dataset.field=f.name;fields.set(f.name,label);$('settings-cache').append(label);
 }
 $('field-min_delay').placeholder='最小';$('field-max_delay').placeholder='最大';
 for(const name of ['max_threads','spot_rounds','futures_rounds','futures_leverage','premium_order_count']){$('field-'+name).type='number';$('field-'+name).min=name==='spot_rounds'||name==='premium_order_count'?'0':'1';}
 $('field-max_threads').max='50';$('threads-field').append(fields.get('max_threads'));$('proxy-field').append(fields.get('exchange_proxy'));
 const delay=node('div',undefined,'delay-pair');delay.append(node('span','随机延迟(毫秒):'),fields.get('min_delay'),fields.get('max_delay'));$('settings-row1').append(delay);
 for(const name of ['withdraw_coin','acc_network','usdt_timeout','use_exchange_config_proxy'])$('settings-row1').append(fields.get(name));
 const save=node('button','保存配置');save.type='submit';save.setAttribute('form','settings-form');const hint=node('span');hint.id='settings-hint';$('settings-row1').append(save,hint);
 const autoButton=node('button','策略设置');autoButton.id='auto-open';autoButton.type='button';autoButton.onclick=openAuto;
 const stockButton=node('button','交易设置');stockButton.id='stock-open';stockButton.type='button';stockButton.onclick=openStock;
 $('settings-cache').append(autoButton,stockButton);const stockHint=node('span','支付：USDC','muted');stockHint.id='stock-payment';$('settings-cache').append(stockHint);
 const stockInfo=node('span','自动限价：买入卖一+1步长，卖出买一-1步长；开始前现货USDC归集至资金账户，全程使用资金账户','muted');stockInfo.id='stock-info';$('settings-cache').append(stockInfo);
 for(const name of ['stock_tick','stock_poll','stock_reprice','stock_timeout','stock_deviation','stock_spread','stock_interval'])$('stock-settings').append(fields.get(name));
 $('auto-settings').replaceChildren();for(const [name,text] of autoFields){const label=node('label',undefined,'field');label.title=autoHelp[name];label.append(node('span',text));const input=node('input');input.id='auto-'+name;input.autocomplete='off';input.title=autoHelp[name];label.append(input);$('auto-settings').append(label);}
 initialized=true;
}
function layoutSettings(){
 if(!initialized)return;
 $('field-earn_amount').disabled=inputValue('earn_amount_all');$('field-ladder_sell_premium').disabled=!inputValue('ladder_sell_premium_enabled');
 const key=[inputValue('trade_account_type'),inputValue('trade_mode'),inputValue('market_stop_mode')].join('|');
 if(layoutKey===key)return;layoutKey=key;
 for(const id of ['settings-row2','settings-row3'])for(const child of [...$(id).children])$('settings-cache').append(child);
 const add=(row,names)=>names.forEach(name=>$(row).append(fields.get(name)));
 const type=inputValue('trade_account_type'),mode=inputValue('trade_mode');add('settings-row2',['trade_account_type']);
 if(type==='现货'){
  add('settings-row2',['trade_mode','spot_symbol']);
  if(mode==='市价'){add('settings-row2',['market_stop_mode',inputValue('market_stop_mode')==='剩余BNB'?'bnb_fee_stop':'spot_rounds']);add('settings-row3',['market_stagger_enabled']);}
  else{add('settings-row2',['spot_rounds']);}
  add('settings-row2',['bnb_topup_amount']);if(mode==='自动分仓挂单')$('settings-row2').append($('auto-open'));
  if(mode==='挂单')add('settings-row3',['bnb_fee_stop','reprice_threshold']);
  if(mode==='溢价单')add('settings-row3',['bnb_fee_stop','premium_delta','premium_order_count','premium_append_threshold','reprice_threshold','premium_partial_fill']);
  if(['分仓挂单','自动分仓挂单'].includes(mode))add('settings-row3',['bnb_fee_stop','ladder_layout','ladder_min_pool_notional','reprice_threshold','ladder_sell_premium_enabled','ladder_sell_premium']);
  add('settings-row3',['rate_limit_wait','withdraw_buffer','enable_withdraw']);
 }else if(type==='合约'){
  add('settings-row2',['futures_symbol','futures_rounds','futures_side','futures_amount']);add('settings-row3',['futures_margin_type','futures_leverage','rate_limit_wait','withdraw_buffer','enable_withdraw']);
 }else if(type==='理财'){
  add('settings-row2',['earn_asset','earn_amount','earn_amount_all','earn_auto_subscribe']);add('settings-row3',['rate_limit_wait']);
 }else{
  add('settings-row2',['stock_symbol','stock_rounds','stock_amount']);$('settings-row2').append($('stock-payment'),$('stock-open'));$('settings-row3').append($('stock-info'));add('settings-row3',['rate_limit_wait']);
 }
 $('field-earn_amount').disabled=inputValue('earn_amount_all');$('field-ladder_sell_premium').disabled=!inputValue('ladder_sell_premium_enabled');
}
function collectSettings(){const settings={};for(const f of schema){const value=inputValue(f.name);if(f.name==='exchange_proxy'&&!value)continue;settings[f.name]=value;}return settings;}
async function saveSettings(forQuery=false){let settings=collectSettings();if(forQuery){const names=['min_delay','max_delay','max_threads','spot_symbol','acc_network','exchange_proxy','use_exchange_config_proxy','rate_limit_wait','usdt_timeout'];settings=Object.fromEntries(Object.entries(settings).filter(([name])=>names.includes(name)));}const result=await api('settings',{settings});if(!forQuery)settingsDirty=false;$('settings-hint').textContent=result.staged?'已保存，下次任务生效':'已保存';await refresh();}
function render(current){
 if(state&&current.server_time<state.server_time)return;
 if(boot!==current.boot){boot=current.boot;seq=0;logs=[];pendingTask=null;}
 state=current;const additions=current.logs.filter(entry=>entry.seq>seq);seq=Math.max(seq,current.seq);logs.push(...additions);if(logs.length>1500)logs.splice(0,logs.length-1500);renderLogs();
 $('status').textContent=current.status;$('summary').textContent=current.summary||'结果汇总：成功0 | 失败0 | 提现总额=- | 余额总额=-';$('progress').max=Math.max(1,Number(current.progress.maximum)||1);$('progress').value=Number(current.progress.value)||0;
 $('interrupted').hidden=!current.interrupted;$('interrupted').textContent=current.interrupted?'上次执行服务中断，请核对交易所订单和余额；不会自动继续轮次或重复提交指令。':'';
 if(!initialized)buildSettings();
 if(!settingsDirty){for(const f of schema){const input=$('field-'+f.name);if(f.name.startsWith('stock_')&&$('stock-dialog').open)continue;if(f.name==='exchange_proxy'){input.value='';input.placeholder=current.proxy_configured?'已加密保存，留空保留':'留空直连';}else if(f.kind==='boolean')input.checked=!!current.settings[f.name];else input.value=current.settings[f.name];}}
 if(!$('auto-dialog').open)for(const [name] of autoFields)$('auto-'+name).value=current.auto_ladder[name];
 layoutSettings();
 for(const b of document.querySelectorAll('[data-action]'))b.disabled=current.busy||(b.dataset.action==='retry_failed'&&!current.can_retry);
 for(const id of ['lock','change-password','destroy-accounts','add-account','import-file','paste-accounts','delete-selected','edit-highlighted','single-run','single-query','single-withdraw','stock-open','use-current','topup-all','volume-open','operation-review'])$(id).disabled=current.busy;
 renderAccounts();renderAssets();
 $('proxy-status').textContent=current.proxy_configured&&current.settings.use_exchange_config_proxy?'已启用':'未启用';
 for(const entry of current.logs){const match=entry.text.match(/连接出口 ([\d.:a-f]+)，耗时 (\d+) ms/i);if(match){$('exit-ip').textContent=match[1];$('proxy-test').textContent='延迟 '+match[2]+' ms';}}
 const m=current.maintenance||{};$('maintenance').textContent=m.error||('磁盘可用 '+((m.free_bytes||0)/1073741824).toFixed(1)+' GB · '+(m.ready?'空间正常':'空间不足，暂停新任务'));$('server-maintenance').textContent=$('maintenance').textContent;
 if(pendingTask&&!current.busy&&current.summary){const task=pendingTask;pendingTask=null;notice((tasks[task.action]||task.action)+'\n'+current.summary+(current.can_retry?'\n失败账号可点击“失败重试”继续。':''),'执行完成');}
 lastSummary=current.summary;
}
const problemPattern=/\[(WARNING|ERROR|CRITICAL)\]|失败|异常|错误|报错|Traceback|Exception|Client Error|API error|APIError/i;
function updateLogPane(id,entries){
 const pane=$(id),follow=pane.scrollTop+pane.clientHeight>=pane.scrollHeight-25,text=entries.map(x=>x.text).join('\n');
 if(pane.textContent!==text){
  const fragment=document.createDocumentFragment();
  entries.forEach((entry,index)=>{
   if(index)fragment.append(document.createTextNode('\n'));
   const chars=Array.from(entry.text);let cursor=0;
   for(const [start,end] of entry.amount_spans||[]){
    if(!Number.isInteger(start)||!Number.isInteger(end)||start<cursor||end<=start||end>chars.length)continue;
    fragment.append(document.createTextNode(chars.slice(cursor,start).join('')),node('span',chars.slice(start,end).join(''),'log-amount'));cursor=end;
   }
   fragment.append(document.createTextNode(chars.slice(cursor).join('')));
  });
  pane.replaceChildren(fragment);
 }
 if(follow)pane.scrollTop=pane.scrollHeight;
}
function renderLogs(){const query=$('log-search').value.toLowerCase(),shown=logs.filter(x=>x.text.toLowerCase().includes(query)),errors=shown.filter(x=>problemPattern.test(x.text));updateLogPane('logs',shown);updateLogPane('error-logs',errors);$('error-count').textContent=errors.length?'报错 / 警告 '+errors.length+' 条':'暂无报错';}
function preservedRows(){return state.accounts.map(a=>({...a,api_key:'',api_secret:''}));}
async function selection(rows){await api('selection',{accounts:rows.map(a=>({id:a.id,selected:a.selected,bnb_topup:a.bnb_topup}))});await refresh();}
function rowClass(a){if(a.active)return'running';if(/失败|错误|异常/.test(a.status))return'failed';if(/警告|暂停|停止/.test(a.status))return'warn';if(/✔|成功|完成|总资产|已赎回/.test(a.status))return'success';return'';}
function highlight(id){highlighted=id;for(const row of $('accounts').children)row.classList.toggle('highlighted',row.dataset.id===id);}
function renderAccounts(){
 const table=$('accounts'),previous=table.parentElement.scrollTop,signature=JSON.stringify([state.accounts,state.busy]);
 if(table.dataset.signature!==signature){
  table.dataset.signature=signature;const existing=new Map([...table.children].map(row=>[row.dataset.id,row]));let position=0;
  for(const a of state.accounts){
   const rowSignature=JSON.stringify([a,state.busy]),old=existing.get(a.id);existing.delete(a.id);
   if(old&&old.dataset.signature===rowSignature){old.classList.toggle('highlighted',highlighted===a.id);if(table.children[position]!==old)table.insertBefore(old,table.children[position]||null);position++;continue;}
   const row=node('tr',undefined,rowClass(a));row.dataset.signature=rowSignature;row.dataset.id=a.id;row.classList.toggle('highlighted',highlighted===a.id);
   const selected=node('input');selected.type='checkbox';selected.checked=a.selected;selected.setAttribute('aria-label','勾选账号 '+a.index);selected.onclick=e=>e.stopPropagation();selected.onchange=guard(async()=>{selected.disabled=true;try{await selection([{...a,selected:selected.checked}]);}finally{selected.disabled=false;selected.checked=(state?.accounts.find(x=>x.id===a.id)||a).selected;}});
   const selectedCell=node('td');selectedCell.append(selected);row.append(selectedCell,node('td',a.index),node('td',a.api_key),node('td',a.address||''),node('td',a.network));row.children[3].title=a.address;
   const topup=node('input');topup.type='checkbox';topup.checked=a.bnb_topup;topup.disabled=state.busy;topup.setAttribute('aria-label','账号 '+a.index+' 预买BNB');topup.onclick=e=>e.stopPropagation();topup.onchange=guard(async()=>{try{await selection([{...a,bnb_topup:topup.checked}]);}finally{topup.checked=(state?.accounts.find(x=>x.id===a.id)||a).bnb_topup;}});const topupCell=node('td');topupCell.append(topup);row.append(topupCell,node('td',a.status));
   const more=node('button','⋯','row-menu-button');more.setAttribute('aria-label','账号 '+a.index+' 菜单');more.onclick=e=>{e.stopPropagation();highlight(a.id);showMenu(a,e.clientX,e.clientY,more);};const moreCell=node('td');moreCell.append(more);row.append(moreCell);
   row.onclick=()=>highlight(a.id);row.ondblclick=guard(e=>{if(e.target.closest('input,button')||e.target.closest('td')?.cellIndex===5)return;return selection([{...a,selected:!a.selected}]);});row.oncontextmenu=e=>{e.preventDefault();highlight(a.id);showMenu(a,e.clientX,e.clientY);};if(old)old.replaceWith(row);if(table.children[position]!==row)table.insertBefore(row,table.children[position]||null);position++;
  }
  for(const row of existing.values())row.remove();
  table.parentElement.scrollTop=previous;
 }
 if(highlighted&&!state.accounts.some(a=>a.id===highlighted))highlighted='';
 if(currentId&&!state.accounts.some(a=>a.id===currentId)){currentId='';singleReady=false;$('single-key').placeholder='';}
 $('empty-accounts').hidden=!!state.accounts.length;$('account-count').textContent='共 '+state.accounts.length+' 个 · 勾选 '+state.accounts.filter(a=>a.selected).length+' 个';
 $('select-all').textContent=state.accounts.length&&state.accounts.every(a=>a.selected)?'取消全选':'全选';
}
async function copyText(text,message='已复制'){try{await navigator.clipboard.writeText(text);feedback(message);return true;}catch(e){await textInput('浏览器无法写入剪贴板，请选择内容后复制：',text,'复制内容');return false;}}
function showMenu(a,x,y,anchor){
 contextId=a.id;const menu=$('account-menu');menu.replaceChildren();
 const items=[['复制 API KEY',async()=>copyText((await api('account_key',{id:a.id})).text)],['复制提现地址',()=>copyText(a.address)],null,['查询',()=>command('query_assets',[a.id])],['查询成交额',()=>command('query_volume',[a.id])],['执行',()=>command('run',[a.id])],['停止',()=>command('stop',[a.id]),!a.active],['提现',()=>command('withdraw',[a.id])],['自动申购',()=>command('earn_auto_subscribe',[a.id])],['赎回理财',()=>command('redeem_earn',[a.id])],['归集BNB并提现',()=>command('collect_bnb',[a.id])],null,['删除该行',()=>deleteAccounts([a.id])]];
 for(const item of items){if(!item){menu.append(node('hr'));continue;}const [text,fn,disabled]=item,b=node('button',text);b.setAttribute('role','menuitem');b.disabled=!!disabled||(state.busy&&!['复制 API KEY','复制提现地址','停止'].includes(text));b.onclick=guard(async()=>{closeMenu();await fn();});menu.append(b);}
 menu.hidden=false;if(anchor){const rect=anchor.getBoundingClientRect();x=rect.right;y=rect.bottom;}
 menu.style.left=Math.max(4,Math.min(x,innerWidth-menu.offsetWidth-5))+'px';menu.style.top=Math.max(4,Math.min(y,innerHeight-menu.offsetHeight-5))+'px';menu.querySelector('button:not(:disabled)').focus();
}
async function searchAccounts(){const query=$('search').value.trim();if(!query){$('search-result').textContent='请输入 API KEY 或提现地址';return;}const {ids}=await api('search_accounts',{query});if(!ids.length){$('search-result').textContent='未找到匹配账号';return;}const previous=lastSearch===query?ids.indexOf(highlighted):-1,id=ids[(previous+1)%ids.length];lastSearch=query;highlight(id);const row=[...$('accounts').children].find(e=>e.dataset.id===id);row?.scrollIntoView({block:'nearest'});const a=state.accounts.find(a=>a.id===id);$('search-result').textContent='找到 '+(ids.indexOf(id)+1)+'/'+ids.length+'：账号 #'+a.index;}
async function deleteAccounts(ids){if(!ids.length)throw Error('请至少勾选一个账号');const a=state.accounts.find(a=>a.id===ids[0]);const message=ids.length===1?'确认删除账号 #'+a.index+'（'+a.api_key+'）吗？':'确认删除已勾选的 '+ids.length+' 个账号吗？';if(!await confirmInput(message,'确认删除'))return;await api('accounts',{accounts:preservedRows().filter(a=>!ids.includes(a.id))});await refresh();}
function executionSummary(ids,autoOnly=false){const s=state.settings,t=s.trade_account_type,lines=['批量账号数：'+ids.length,'交易类型：'+(autoOnly?'理财':t)];
 if(autoOnly)lines.push('理财币种：'+s.earn_asset,'执行内容：仅开启自动申购','自动申购：开启');
 else if(t==='理财')lines.push('理财币种：'+s.earn_asset,'存入数量：'+(s.earn_amount_all?'全部':s.earn_amount),'自动申购：'+(s.earn_auto_subscribe?'开启':'关闭'));
 else if(t==='合约')lines.push('交易对：'+s.futures_symbol,'轮次：'+s.futures_rounds);
 else if(t==='股票')lines.push('股票代码：'+s.stock_symbol,'自动限价轮次：'+s.stock_rounds,'买入=卖一+1步长；卖出=买一-1步长。','开始前现货USDC全部划转到资金账户，全程使用资金账户。','每轮金额：'+(s.stock_amount||'全部任务可用资金'),'不兑换USDT，不自动提现。');
 else{lines.push('交易模式：'+s.trade_mode,'交易对：'+s.spot_symbol,s.trade_mode==='市价'&&s.market_stop_mode==='剩余BNB'?'停止方式：剩余BNB低于 '+s.bnb_fee_stop:'轮次：'+s.spot_rounds);if(s.trade_mode==='市价')lines.push('市价交错执行：'+(s.market_stagger_enabled?'开启':'关闭'));if(s.trade_mode.includes('分仓'))lines.push('档位模式：'+s.ladder_layout,'最小分仓成交额：'+s.ladder_min_pool_notional);lines.push('预购买BNB金额：'+s.bnb_topup_amount);}
 lines.push('随机延迟：'+(s.min_delay||1000)+' - '+(s.max_delay||3000)+' 毫秒');if(!autoOnly&&!['理财','股票'].includes(t))lines.push('提现币种：'+s.withdraw_coin,'提现网络：'+s.acc_network,'自动提现：'+(s.enable_withdraw?'开启':'关闭'));return lines.join('\n');
}
async function command(action,ids,{single=false}={}){
 if(action!=='stop'&&settingsDirty)await saveSettings(['query_assets','query_volume'].includes(action));
 const selected=ids||state.accounts.filter(a=>a.selected).map(a=>a.id);
 if(action!=='stop'&&action!=='retry_failed'&&!selected.length)throw Error('请至少勾选一个账号');
 let options={};
 if(action==='run'&&!single){const t=state.settings.trade_account_type;const confirmed=await questionInput(executionSummary(selected),{title:t==='股票'?'执行股票交易':'确认批量执行',accept:'开始执行',execution:!['理财','股票'].includes(t),ids:selected});if(!confirmed)return;if(typeof confirmed==='object')options=confirmed;}
 else if(action==='earn_auto_subscribe'){if(!await confirmInput(executionSummary(selected,true),'确认批量执行','开始执行'))return;}
 else if(action==='withdraw'&&!single){if(!await confirmInput('确认对 '+selected.length+' 个账号执行批量提现吗？','确认执行'))return;}
 else if(action==='collect_bnb'){if(!await confirmInput('即将对 '+selected.length+' 个账号执行“归集BNB并提现”。\n流程包含：归集合约/资金到现货、小额币兑换BNB、大额资产优先直接闪兑BNB，失败时回退卖USDT再换BNB、最后自动提现BNB。\n该模式固定提取 BNB，不受“自动提现”勾选和提现币种设置影响。\n\n确认继续吗？','确认执行'))return;}
 else if(action==='redeem_earn'){if(!await confirmInput('即将对 '+selected.length+' 个账号执行“批量赎回理财”。\n查询所有活期理财持仓，发现可赎回资产后逐个赎回到现货，不限制币种。\n赎回流程复用共享策略中的随机延迟，并遵守单账号连续赎回的接口节流。\n\n确认继续吗？','确认执行'))return;}
 else if(action==='retry_failed'){if(!await confirmInput('确认对上一次任务中的失败账号重试吗？','确认重试'))return;}
 const payload={action,accounts:action==='stop'?(ids||null):action==='retry_failed'?null:selected,confirmed:true,command_id:crypto.randomUUID(),issued_at:state.server_time};if(Object.keys(options).length)payload.options=options;
 let result;try{result=await api('command',payload);}catch(e){if(!e.transport)throw e;if(await confirmInput(e.message+'\n使用同一操作编号查询 / 重试此请求？','连接确认'))result=await api('command',payload);else throw e;}
 if(!result.accepted)throw Error(result.error||'操作未启动');
 if(action!=='stop')pendingTask={action,summary:state.summary};feedback(action==='stop'?'已请求停止，等待订单清理确认。':'');await refresh();
}
function normalizeVolumeDate(value){
 const text=value.trim();if(!text)return '';
 let parts=text.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$/),year,month,day;
 if(parts){[,year,month,day]=parts.map(Number);}
 else if(/^\d{6,8}$/.test(text)){year=Number(text.slice(0,4));const rest=text.slice(4),width=rest.length===4||(rest.length===3&&Number(rest.slice(0,2))>=1&&Number(rest.slice(0,2))<=12)?2:1;month=Number(rest.slice(0,width));day=Number(rest.slice(width));}
 else throw Error('日期可输入 YYYYMMDD 或 YYYY-M-D，例如 20260902、2026-9-2');
 const date=new Date(Date.UTC(year,month-1,day));if(date.getUTCFullYear()!==year||date.getUTCMonth()+1!==month||date.getUTCDate()!==day)throw Error('查询日期无效，请检查年月日');
 return String(year).padStart(4,'0')+'-'+String(month).padStart(2,'0')+'-'+String(day).padStart(2,'0');
}
function openVolumeSettings(){const saved=state.volume_query||{};$('volume-start').value=saved.start_date||'';$('volume-end').value=saved.end_date||'';$('volume-error').hidden=true;$('volume-dialog').showModal();}
function questionInput(message,{title='确认执行',accept='确认',textMode=false,value='',execution=false,ids=[]}={}){
 if(resolveQuestion)throw Error('请先完成当前确认操作');
 return new Promise(resolve=>{resolveQuestion=resolve;questionMode=textMode?'text':execution?'execution':'confirm';$('question-title').textContent=title;$('question-message').textContent=message;$('question-input-label').hidden=!textMode;$('question-input').value=value;$('question-accept').textContent=accept;$('execution-options').hidden=!execution;
 if(execution){const s=state.settings,topup=s.trade_account_type==='现货'&&['市价','挂单','溢价单','分仓挂单','自动分仓挂单'].includes(s.trade_mode)&&((s.trade_mode==='市价'&&s.market_stop_mode==='剩余BNB')||Number(s.spot_rounds)!==0);$('confirm-bnb-label').hidden=$('confirm-bnb-count').hidden=!topup;$('confirm-bnb').checked=topup&&Number(s.bnb_topup_amount)>0;$('confirm-withdraw').checked=!!s.enable_withdraw;const count=state.accounts.filter(a=>ids.includes(a.id)&&a.bnb_topup).length;$('confirm-bnb-count').textContent='本次共 '+ids.length+' 个账号，其中 '+count+' 个账号勾选预买BNB。';}
 $('question-dialog').showModal();if(textMode)$('question-input').focus();});
}
function finishQuestion(accepted){let value=null;if(accepted){value=questionMode==='text'?$('question-input').value:questionMode==='execution'?{enable_withdraw:$('confirm-withdraw').checked,enable_bnb_topup:!$('confirm-bnb-label').hidden&&$('confirm-bnb').checked}:true;}$('question-input').value='';$('question-dialog').close();const resolve=resolveQuestion;resolveQuestion=null;if(resolve)resolve(value);}
const confirmInput=(message,title,accept)=>questionInput(message,{title,accept});
const textInput=(message,value='',title='导入参数')=>questionInput(message,{title,textMode:true,value});
function passwordInput(change=false){return new Promise(resolve=>{resolvePassword=resolve;$('new-password-label').hidden=$('repeat-password-label').hidden=!change;$('new-password').required=$('repeat-password').required=change;$('password-title').textContent=change?'修改程序密码':'再次验证密码';$('password-dialog').showModal();$('verify-password').focus();});}
function finishPassword(value){for(const id of ['verify-password','new-password','repeat-password'])$(id).value='';$('password-dialog').close();const resolve=resolvePassword;resolvePassword=null;if(resolve)resolve(value);}
async function sensitive(method,warning){if(warning&&!await confirmInput(warning,method==='destroy_accounts'?'确认销毁所有':'确认操作'))return;const passwords=await passwordInput(method==='change_password');if(!passwords)return;const result=await api(method,{...passwords,confirmed:true});if(result.text!==undefined)download(result.text,result.filename);if(['lock','change_password'].includes(method))signedOut();else await refresh();if(method==='destroy_accounts'){$('single-key').value=$('single-secret').value=$('single-address').value='';currentId='';singleReady=false;notice('账号相关信息已销毁；共享策略参数和代理设置已保留。','销毁完成');}else feedback('操作完成。');}
async function reviewOperations(){
 const data=await api('operation_review',{}),text=data.entries.map(e=>[e.id,e.time+' '+e.action+' '+e.coin+' '+e.amount,e.address+' '+e.reference+' ['+e.state+']'].join('\n')).join('\n\n')||'没有待核对操作。';
 let id='',outcome='',confirmation='',evidence='';
 if(data.recovery_required){outcome='restore';confirmation=await textInput('账本从备份恢复，可能缺少最近记录。核对全部交易所历史后输入：已核对全部操作\n\n'+text,'','解除恢复保护');}
 else if(data.entries.some(e=>e.kind==='exchange-funds')){
  id=await textInput(text+'\n\n在交易所核对历史后，填写要处理的资金记录编号：','','核对未确认');if(id===null)return;
  outcome=await textInput('原操作已执行输入 completed；确认未执行输入 failed。不能确定时取消。','','核对结果');if(outcome===null)return;
  evidence=await textInput('填写交易所回执编号或查询结果及时间：','','核对依据');if(evidence===null)return;
  confirmation=await textInput('输入：已在交易所核对','','确认核对');
 }else{notice(text+'\n\n订单会在再次执行时查询原单。','核对未确认');return;}
 if(confirmation===null)return;const passwords=await passwordInput();if(!passwords)return;
 await api('operation_resolve',{id:id.trim(),outcome:outcome.trim(),confirmation,evidence,...passwords,confirmed:true});await refresh();notice('核对结果已保存。');
}
function download(text,filename){const blob=new Blob([text],{type:'text/plain;charset=utf-8'}),url=URL.createObjectURL(blob),a=node('a');a.href=url;a.download=filename;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
function editAccount(a){$('edit-id').value=a?.id||'';$('edit-key').value=$('edit-secret').value='';$('edit-key').placeholder=a?.api_key||'';$('edit-address').value=a?.address||'';$('edit-network').value=a?.network||state.settings.acc_network;$('account-dialog').showModal();}
function closeAccount(){for(const id of ['edit-key','edit-secret'])$(id).value='';$('account-dialog').close();}
async function importText(text,source='剪贴板'){if(!text.trim())throw Error('内容为空');const result=await api('import_accounts',{text});$('import-text').value='';$('import-dialog').close();await refresh();notice(result.added?'从'+source+'导入账号数量：'+result.added+(result.skipped?'\n已跳过重复 API：'+result.skipped:''):source+'没有新增账号，全部为重复 API（重复 '+result.skipped+' 个）','成功');}
async function pasteImport(){try{const text=await navigator.clipboard.readText();if(text.trim()){await importText(text);return;}}catch(e){if(e.name!=='NotAllowedError'&&e.name!=='SecurityError')throw e;}$('import-dialog').showModal();$('import-text').focus();}
async function readAccountFile(file){if(!file)return;if(file.size>1000000)throw Error('导入文件不能超过 1 MB');const bytes=await file.arrayBuffer();let text;try{text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);}catch(e){text=new TextDecoder('gb18030').decode(bytes);}await importText(text,'文件');}
function openAuto(){autoSnapshot=Object.fromEntries(autoFields.map(([name])=>[name,$('auto-'+name).value]));$('share-status').textContent='';$('auto-dialog').showModal();}
function cancelAuto(){if(autoSnapshot)for(const [name,value]of Object.entries(autoSnapshot))$('auto-'+name).value=value;autoSnapshot=null;}
function openStock(){stockSnapshot=Object.fromEntries([...fields.keys()].filter(n=>n.startsWith('stock_')).map(n=>[n,inputValue(n)]));$('stock-dialog').showModal();}
function cancelStock(){if(stockSnapshot)for(const [name,value]of Object.entries(stockSnapshot))$('field-'+name).value=value;stockSnapshot=null;}
function useCurrent(){const a=state.accounts.find(a=>a.id===highlighted);if(!a)throw Error('请先选中一行账号');currentId=a.id;singleReady=false;$('single-key').value=$('single-secret').value='';$('single-key').placeholder=a.api_key+' · 留空保留';$('single-address').value=a.address;feedback('当前 API：账号 #'+a.index+'，凭据保留在服务器。');}
async function singleAccount(){
 const key=$('single-key').value.trim(),secret=$('single-secret').value.trim(),address=$('single-address').value.trim();let id=currentId;
 if(key){const hash=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(key));id=[...new Uint8Array(hash)].map(n=>n.toString(16).padStart(2,'0')).join('').slice(0,24);}
 const existing=state.accounts.find(a=>a.id===id);if(!existing&&(!key||!secret))throw Error('请先填写 API KEY 和 SECRET');
 let rows=preservedRows();const row={id,api_key:key,api_secret:secret,address,network:inputValue('acc_network'),selected:existing?.selected||false,bnb_topup:existing?.bnb_topup??true};
 if(existing)Object.assign(rows.find(a=>a.id===id),row);else rows.push(row);
 await api('accounts',{accounts:rows});await refresh();currentId=id;$('single-key').value=$('single-secret').value='';$('single-key').placeholder=state.accounts.find(a=>a.id===id).api_key+' · 留空保留';return id;
}
async function singleAction(action){if(action==='withdraw'&&!singleReady)throw Error('请先使用当前 API 创建连接（点击一次开始或刷新余额）');const id=await singleAccount();await command(action,[id],{single:true});singleReady=true;}
function renderAssets(){const body=$('assets');body.replaceChildren();const values=Object.values(state.assets);for(const a of values){const row=node('tr');for(const key of ['index','api_key','address','network','total_usdt'])row.append(node('td',a[key]??''));body.append(row);}if(!values.length){const row=node('tr'),cell=node('td','暂无查询结果');cell.colSpan=5;row.append(cell);body.append(row);}}
$('login-form').addEventListener('submit',guard(async e=>{e.preventDefault();const p=$('password').value;$('password').value='';await api('login',{password:p,setup_token:$('setup-token').value});await connected();feedback('');}));
$('settings-form').addEventListener('submit',guard(async e=>{e.preventDefault();await saveSettings();}));
$('account-form').addEventListener('submit',guard(async e=>{e.preventDefault();const id=$('edit-id').value,row={id,api_key:$('edit-key').value,api_secret:$('edit-secret').value,address:$('edit-address').value,network:$('edit-network').value,selected:true,bnb_topup:true},rows=preservedRows(),existing=rows.find(a=>a.id===id);if(existing)Object.assign(existing,row,{selected:existing.selected,bnb_topup:existing.bnb_topup});else rows.push(row);await api('accounts',{accounts:rows});closeAccount();await refresh();}));
$('question-form').addEventListener('submit',e=>{e.preventDefault();finishQuestion(true);});$('question-cancel').onclick=()=>finishQuestion(false);$('question-dialog').addEventListener('cancel',e=>{e.preventDefault();finishQuestion(false);});
$('password-form').addEventListener('submit',e=>{e.preventDefault();if(!$('new-password-label').hidden&&$('new-password').value!==$('repeat-password').value){notice('两次新密码不同','错误');return;}finishPassword({password:$('verify-password').value,new_password:$('new-password').value});});$('password-cancel').onclick=()=>finishPassword(null);$('password-dialog').addEventListener('cancel',e=>{e.preventDefault();finishPassword(null);});
for(const b of document.querySelectorAll('[data-close]'))b.onclick=()=>{const id=b.dataset.close;if(id==='auto-dialog')cancelAuto();if(id==='stock-dialog')cancelStock();if(id==='import-dialog')$('import-text').value='';$(id).close();};
$('auto-dialog').addEventListener('cancel',cancelAuto);$('stock-dialog').addEventListener('cancel',cancelStock);$('import-dialog').addEventListener('cancel',()=>{$('import-text').value='';});
for(const b of document.querySelectorAll('[data-action]'))b.onclick=guard(()=>command(b.dataset.action));
$('volume-open').onclick=openVolumeSettings;
for(const id of ['volume-start','volume-end']){const input=$(id);const format=()=>{try{input.value=normalizeVolumeDate(input.value);}catch(e){}};input.oninput=()=>{if(/^\d{8}$/.test(input.value))format();$('volume-error').hidden=true;};input.onblur=format;}
$('volume-form').onsubmit=async e=>{e.preventDefault();const button=$('volume-save');button.disabled=true;try{const dates={start_date:normalizeVolumeDate($('volume-start').value),end_date:normalizeVolumeDate($('volume-end').value)};if(dates.start_date&&dates.end_date&&dates.start_date>dates.end_date)throw Error('开始日期不能晚于结束日期');const result=await api('volume_settings',dates);state.volume_query=result.dates;$('volume-dialog').close();await refresh();feedback('查询条件已保存，点击“查询成交额”即可使用。');}catch(e){$('volume-error').textContent=e.message;$('volume-error').hidden=false;}finally{button.disabled=false;}};
$('volume-cancel').onclick=()=>$('volume-dialog').close();
$('stop-all').onclick=guard(async()=>{await api('emergency_stop',{});feedback('已请求停止，等待订单清理确认。');await refresh();});
$('select-all').onclick=guard(()=>selection(state.accounts.map(a=>({...a,selected:!state.accounts.every(x=>x.selected)}))));$('topup-all').onclick=guard(()=>selection(state.accounts.map(a=>({...a,bnb_topup:true}))));
$('delete-selected').onclick=guard(()=>deleteAccounts(state.accounts.filter(a=>a.selected).map(a=>a.id)));$('add-account').onclick=()=>editAccount(null);$('edit-highlighted').onclick=guard(()=>{const a=state.accounts.find(a=>a.id===highlighted);if(!a)throw Error('请先选中一行账号');editAccount(a);});$('cancel-account').onclick=closeAccount;$('account-dialog').addEventListener('cancel',closeAccount);
$('search-accounts').onclick=guard(searchAccounts);$('search').onkeydown=guard(e=>{if(e.key==='Enter'){e.preventDefault();return searchAccounts();}});$('clear-search').onclick=()=>{$('search').value='';$('search-result').textContent='';lastSearch='';$('search').focus();};
$('account-list').onclick=()=>{$('account-list').focus({preventScroll:true});};$('account-list').addEventListener('paste',guard(async e=>{e.preventDefault();if(state.busy)throw Error('请先停止任务再导入账号');await importText(e.clipboardData.getData('text'));}));
$('account-list').onkeydown=guard(e=>{if(e.key==='F2'){e.preventDefault();const a=state.accounts.find(a=>a.id===highlighted);if(a)editAccount(a);}if((e.key==='ContextMenu'||e.key==='F10'&&e.shiftKey)&&highlighted){e.preventDefault();const a=state.accounts.find(a=>a.id===highlighted),row=[...$('accounts').children].find(r=>r.dataset.id===highlighted),rect=row.getBoundingClientRect();showMenu(a,rect.x+120,rect.bottom);}if(e.key===' '&&highlighted){e.preventDefault();const a=state.accounts.find(a=>a.id===highlighted);return selection([{...a,selected:!a.selected}]);}});
$('paste-accounts').onclick=guard(pasteImport);$('import-file').onclick=()=>$('account-file').click();$('account-file').onchange=guard(async()=>{const file=$('account-file').files[0];$('account-file').value='';await readAccountFile(file);});$('import-form').onsubmit=guard(async e=>{e.preventDefault();await importText($('import-text').value);});
$('use-current').onclick=guard(useCurrent);$('single-run').onclick=guard(()=>singleAction('run'));$('single-query').onclick=guard(()=>singleAction('query_assets'));$('single-withdraw').onclick=guard(()=>singleAction('withdraw'));$('single-key').oninput=()=>{currentId='';singleReady=false;};
$('log-search').oninput=renderLogs;$('clear-view').onclick=()=>{logs=[];renderLogs();};
$('export-accounts').onclick=guard(()=>{if(!state.accounts.length){notice('当前账号列表为空，无需导出');return;}return sensitive('export_accounts',null);});
$('destroy-accounts').onclick=guard(()=>sensitive('destroy_accounts','将直接销毁服务器交易所账号、提现/资产结果、交易恢复账本、交易所运行日志及运行时代理临时文件。\n\n共享策略参数和代理设置会保留。\n\n此操作不会使用回收站，程序无法恢复。已导出的账号文件不在本次范围。'));
$('asset-export').onclick=guard(async()=>{const r=await api('asset_export',{});download(r.text,r.filename);});$('lock').onclick=guard(()=>sensitive('lock',null));$('change-password').onclick=guard(()=>sensitive('change_password',null));$('logout').onclick=guard(async()=>{await api('logout',{});signedOut();});
$('operation-review').onclick=guard(reviewOperations);
$('proxy-test').onclick=guard(async()=>{if(settingsDirty)await saveSettings();await api('proxy_test',{});$('proxy-test').textContent='测试中…';});$('proxy-clear').onclick=guard(async()=>{if(await confirmInput('清除服务器的加密代理配置？')){await api('settings',{settings:{exchange_proxy:'',use_exchange_config_proxy:false}});await refresh();}});
$('auto-save').onclick=guard(async()=>{const auto_ladder=Object.fromEntries(autoFields.map(([name])=>[name,$('auto-'+name).value]));await api('settings',{settings:{},auto_ladder});autoSnapshot=null;$('auto-dialog').close();await refresh();feedback('自动分仓判断参数已保存并生效。');});
$('stock-save').onclick=()=>{stockSnapshot=null;dirty();$('stock-dialog').close();};
$('share-export').onclick=guard(async()=>{const settings=Object.fromEntries(autoFields.map(([name])=>[name,$('auto-'+name).value]));const copied=await copyText(JSON.stringify({format:'xiaojun-auto-ladder-settings',version:1,settings}));$('share-status').textContent=copied?'已复制当前表单参数，可发送到另一台机器。':'请从复制内容窗口手动复制参数。';});
$('share-import').onclick=guard(async()=>{let text;try{text=await navigator.clipboard.readText();}catch(e){text=await textInput('粘贴桌面版或服务器分享的策略参数：');}if(!text)return;const raw=JSON.parse(text);if(raw.format!=='xiaojun-auto-ladder-settings'||raw.version!==1||!raw.settings||autoFields.some(([name])=>typeof raw.settings[name]!=='string')||Object.keys(raw.settings).length!==autoFields.length)throw Error('分享策略参数不完整或版本不兼容');for(const [name]of autoFields)$('auto-'+name).value=raw.settings[name];$('share-status').textContent='已填入导入参数；检查后点击“保存并运行”生效。';});
$('settings-export').onclick=()=>download(JSON.stringify({format:'xiaojun-linux-exchange-settings',version:1,settings:state.settings,auto_ladder:state.auto_ladder},null,2),'exchange-settings.json');$('settings-import').onclick=guard(async()=>{const text=await textInput('粘贴服务器导出的交易设置 JSON：');if(!text)return;const raw=JSON.parse(text);if(raw.format!=='xiaojun-linux-exchange-settings'||raw.version!==1)throw Error('交易设置格式不正确');await api('settings',{settings:raw.settings,auto_ladder:raw.auto_ladder});settingsDirty=false;await refresh();feedback('交易设置已导入并保存。');});
$('server-details').onclick=()=>$('server-dialog').showModal();$('show-assets').onclick=()=>$('assets-dialog').showModal();
document.addEventListener('click',e=>{if(!e.target.closest('#account-menu,.row-menu-button'))closeMenu();});document.addEventListener('keydown',e=>{if(e.key==='Escape')closeMenu();});window.addEventListener('resize',closeMenu);$('account-list').addEventListener('scroll',closeMenu);
$('account-menu').addEventListener('keydown',e=>{if(!['ArrowDown','ArrowUp','Home','End'].includes(e.key))return;e.preventDefault();const buttons=[...$('account-menu').querySelectorAll('button:not(:disabled)')],index=buttons.indexOf(document.activeElement);if(!buttons.length)return;const next=e.key==='Home'?0:e.key==='End'?buttons.length-1:(index+(e.key==='ArrowDown'?1:-1)+buttons.length)%buttons.length;buttons[next].focus();});
guard(async()=>{const health=await api('health');$('setup-label').hidden=health.configured;if(!health.configured)$('login-hint').textContent='首次设置程序密码，需要服务器初始化码。';try{await connected();}catch(e){if(!String(e.message).includes('会话')&&!String(e.message).includes('登录'))feedback(e.message);}})();
