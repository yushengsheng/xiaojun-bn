import sys,shutil,time
from pathlib import Path
from decimal import Decimal
from contextlib import ExitStack
from unittest.mock import Mock,patch
root=Path(sys.argv[1]);sys.path[:0]=[str(root),str(root/'tests')]
import linux_exchange_cases as cases
from exchange_constants import TRADE_MODE_OPTIONS
s,call,_=cases.fixture();r=s.runtime
client=Mock();client.key=r.accounts[0]['api_key']
client.spot_asset_balance_decimal.return_value=Decimal('100')
client.find_simple_earn_flexible_product.return_value={'productId':'TEST-PRODUCT'}
client.subscribe_simple_earn_flexible_product.return_value={'purchaseId':'TEST-PURCHASE'}
client.get_simple_earn_flexible_positions.return_value=[{'asset':'USDT','productId':'TEST-PRODUCT','totalAmount':'10','canRedeem':True,'autoSubscribe':True}]
client.redeem_simple_earn_flexible_product.return_value={'redeemId':'TEST-REDEEM'}
client.withdraw_all_coin.return_value=Decimal('12.34')
client.spot_positive_assets.return_value=[{'asset':'BNB','free':'10'}]
client.convert_spot_dust_to_bnb.return_value=[]
client.convert_large_spot_assets_to_bnb.return_value={'direct_bnb':[],'fallback_usdt':[],'residual_assets':[]}
client.query_asset_balances_breakdown.return_value={'BNB':Decimal('10'),'USDT':Decimal('100')}
client.normalize_price_delta.side_effect=lambda _symbol,value,**kw:Decimal(str(value))
client.pending_order_requests.return_value=[]
client.get_spot_symbol_catalog.return_value=[{'symbol':'BNBUSDT','baseAsset':'BNB','quoteAsset':'USDT'}]
client.get_spot_symbols_for_base_asset.return_value=('BNB',client.get_spot_symbol_catalog.return_value)
client.get_historical_usdt_hourly_rates.return_value={}
summary={k:Decimal('0') for k in ('quoteVolume','buyQuoteVolume','sellQuoteVolume','orderPnl','usdtQuoteVolume','usdtBuyQuoteVolume','usdtSellQuoteVolume','usdtOrderPnl','usdtCommission')}
summary.update(symbol='BNBUSDT',quoteAsset='USDT',commissionByAsset={},unvaluedQuoteVolumeByAsset={},unvaluedCommissionByAsset={},unvaluedHourlyAmounts={},tradeCount=0,orderCount=0)
client.spot_trade_volume_summary.return_value=summary
rows=r.account_records();rows[0].update(api_key='',api_secret='',address='TEST-ONLY-ADDRESS');rows[1].update(api_key='',api_secret='');r.replace_accounts(rows)
try:
 with ExitStack() as stack:
  stack.enter_context(patch('requests.sessions.Session.request',side_effect=AssertionError('LIVE HTTP FORBIDDEN')))
  stack.enter_context(patch.object(r,'_create_binance_client',return_value=client))
  stack.enter_context(patch.object(r,'_ensure_trade_symbol_supported',return_value='USDT'))
  stack.enter_context(patch.object(r,'wait_for_usdt',return_value=True))
  stack.enter_context(patch.object(r,'_reconcile_exchange_orders'))
  stack.enter_context(patch.object(r,'_run_strategy_with_spot_statistics',return_value=({'withdraw_amount':0,'ladder_cleanup_confirmed':True,'market_round_failed_count':0},'')))
  stack.enter_context(patch('exchange_app_batch.Strategy'))
  stack.enter_context(patch('exchange_app_batch.AutoLadderMarketObserver'))
  for mode in TRADE_MODE_OPTIONS:
   r.update_settings({'trade_account_type':'现货','trade_mode':mode,'spot_rounds':1,'premium_delta':'.01','bnb_fee_stop':'.001','min_delay':'0','max_delay':'0'})
   r.execute('run');cases.wait_idle(s)
   assert '成功1' in r.exchange_batch_summary_var.get(),(mode,r.accounts[0]['status_var'].get(),r.exchange_batch_summary_var.get())
   print('CONTROLLER OK',mode,flush=True)
  r.update_settings({'trade_account_type':'合约'});r.execute('run');cases.wait_idle(s)
  assert '成功1' in r.exchange_batch_summary_var.get(),r.accounts[0]['status_var'].get()
  print('CONTROLLER OK futures',flush=True)
  r.update_settings({'trade_account_type':'理财','earn_amount':'1'})
  for action in ('run','earn_auto_subscribe','redeem_earn','withdraw','collect_bnb','query_volume'):
   r.execute(action);cases.wait_idle(s)
   assert '失败' not in r.accounts[0]['status_var'].get(),(action,r.accounts[0]['status_var'].get())
   print('CONTROLLER OK',action,r.accounts[0]['status_var'].get(),flush=True)
 r.close_idle()
finally:shutil.rmtree(cases.WORK)
