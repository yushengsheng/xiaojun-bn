"""Binance underlying-equity trading, isolated from spot/futures execution."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import logging
import re
import time
import uuid

from exchange_binance_client import BinanceAPIError, BinanceClient, ExchangeOperationFailedError, retry_failed_operation, retry_cleanup
from exchange_order_recovery import exchange_operation_scope
from exchange_constants import TRADE_ACCOUNT_TYPE_STOCK as STOCK_TYPE
from input_validation import bounded_int, finite_decimal, finite_float

logger = logging.getLogger("bot")
STOCK_FIELDS = {
    "symbol": "AAPL", "rounds": "1", "asset": "USDC",
    "amount": "", "tick": "0.01", "poll": "1", "reprice": "5",
    "timeout": "60", "deviation": "1", "spread": "0.5", "interval": "0",
}
TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


def number(value) -> Decimal:
    return finite_decimal(value, "股票接口数值", minimum=0)


def text_number(value) -> str:
    return format(Decimal(value), "f")


@dataclass(frozen=True)
class StockSettings:
    symbol: str
    rounds: int
    asset: str
    amount: Decimal | None
    tick: Decimal
    poll: float
    reprice: float
    timeout: float
    deviation: Decimal
    spread: Decimal
    interval: float

    @classmethod
    def parse(cls, raw):
        values = {**STOCK_FIELDS, **raw}
        symbol = str(values["symbol"]).strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", symbol):
            raise ValueError("请填写美股代码，例如 AAPL、TSLA；不要填写现货交易对")
        asset = str(values["asset"])
        if asset != "USDC":
            raise ValueError("股票仅使用账户已有USDC支付，不自动兑换USDT")
        amount = finite_decimal(values["amount"], "每轮金额", minimum=0) if str(values["amount"]).strip() else None
        tick = finite_decimal(values["tick"], "股票价格步长", minimum=0)
        if (amount is not None and amount <= 0) or tick <= 0 or tick % Decimal("0.01"):
            raise ValueError("金额必须大于0；股票价格步长须为0.01的正整数倍（当前API价格精度）")
        deviation = finite_decimal(values["deviation"], "最大价格偏离", minimum=0)
        spread = finite_decimal(values["spread"], "最大盘口价差", minimum=0)
        if not 0 < deviation <= 10 or not 0 < spread <= 10:
            raise ValueError("价格偏离和盘口价差必须在0到10%之间")
        poll = finite_float(values["poll"], "成交检查间隔", positive=True)
        reprice = finite_float(values["reprice"], "改价间隔", positive=True)
        timeout = finite_float(values["timeout"], "等待上限", positive=True)
        if not 0.2 <= poll <= reprice <= timeout:
            raise ValueError("成交检查须至少0.2秒，且不超过改价间隔和等待上限")
        return cls(symbol, bounded_int(values["rounds"], "股票轮次", maximum=100000), asset,
                   amount, tick, poll, reprice, timeout, deviation / 100,
                   spread / 100, finite_float(values["interval"], "轮次间隔"))

    def payload(self):
        result = {key: str(value) for key, value in asdict(self).items()}
        result["amount"] = "" if self.amount is None else str(self.amount)
        result["deviation"], result["spread"] = str(self.deviation * 100), str(self.spread * 100)
        return result


def eastern_time(now: datetime) -> datetime:
    """Modern US DST rules; works in packaged Windows without a tzdata dependency."""
    now = now.astimezone(timezone.utc)
    march, november = date(now.year, 3, 8), date(now.year, 11, 1)
    start = march + timedelta(days=(6 - march.weekday()) % 7)
    end = november + timedelta(days=(6 - november.weekday()) % 7)
    daylight = datetime.combine(start, datetime.min.time(), timezone.utc) + timedelta(hours=7) <= now < datetime.combine(end, datetime.min.time(), timezone.utc) + timedelta(hours=6)
    return now.astimezone(timezone(timedelta(hours=-4 if daylight else -5)))


def market_holidays(year: int) -> set[date]:
    def observed(day):
        return day + timedelta(days=1 if day.weekday() == 6 else -1 if day.weekday() == 5 else 0)
    def nth(month, weekday, occurrence):
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (occurrence - 1))
    # Gregorian Easter (Good Friday is an equity-market holiday).
    a, b, c = year % 19, year // 100, year % 100
    d, e, f = b // 4, b % 4, (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    easter = date(year, (h + l - 7 * m + 114) // 31, (h + l - 7 * m + 114) % 31 + 1)
    # NYSE does not observe New Year's Day on the preceding Friday when Jan 1 is Saturday.
    new_year = date(year, 1, 1)
    result = {new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year,
              nth(1, 0, 3), nth(2, 0, 3), easter - timedelta(days=2),
              date(year, 5, 31) - timedelta(days=date(year, 5, 31).weekday()),
              observed(date(year, 7, 4)), nth(9, 0, 1), nth(11, 3, 4), observed(date(year, 12, 25))}
    if year >= 2022:
        result.add(observed(date(year, 6, 19)))
    return result


def trading_session(now: datetime, rules: dict) -> tuple[str, bool]:
    local = eastern_time(now)
    day, minute = local.date(), local.hour * 60 + local.minute
    trading_day = day + timedelta(days=1) if minute >= 1200 else day
    if trading_day.weekday() >= 5 or trading_day in market_holidays(trading_day.year):
        raise RuntimeError("美股休市，股票轮次暂停；请在开放交易时段恢复")
    thanksgiving = next(d for d in market_holidays(day.year) if d.month == 11)
    early = day == thanksgiving + timedelta(days=1) or (day.month, day.day) in {(7, 3), (12, 24)}
    close = 780 if early else 960
    if 570 <= minute < close:
        return "RTH", bool(rules.get("fractionable"))
    if 240 <= minute < 1200:
        if early and minute >= 1020:
            raise RuntimeError("美股提前收市，等待下一个交易时段")
        if not rules.get("extendedSession"):
            raise RuntimeError("该股票不支持盘前盘后交易")
        return "EXTENDED", bool(rules.get("fractionableEh"))
    if not rules.get("overnightSupported"):
        raise RuntimeError("该股票不支持隔夜交易")
    # Overnight fractional eligibility is not separately documented: use whole shares.
    return "24H", False


def platform_fee(notional: Decimal) -> Decimal:
    return Decimal("0.35") if notional <= 350 else (notional * Decimal("0.001")).quantize(Decimal("0.01"), rounding=ROUND_CEILING)


class StockAPI:
    def __init__(self, client):
        self.c = client

    def call(self, method, path, params=None):
        endpoint = "/sapi/v1/equity/" + path
        if method == "GET" and path.startswith("market/"):
            # MARKET_DATA uses the session API-key header without signed parameters.
            request = lambda: self.c._public_get_impl(self.c.spot, endpoint, params or {}, timeout=6)
        else:
            request = lambda: self.c._request_impl(self.c.spot, method, endpoint, params or {}, timeout=6)
        # Equity submissions share the enclosing step's five-retry budget.
        submit = lambda: self.c._run_request_with_retry(
            request, max_retries=1, delay=0, base=self.c.spot, path=endpoint,
            replay_safe=method == "GET")
        return retry_failed_operation(self.c, submit) if method != "GET" else submit()

    def rules(self, symbol):
        data = self.call("GET", "market/exchangeInfo", {"symbol": symbol})
        rows = data.get("symbols", []) if isinstance(data, dict) else []
        matches = [row for row in rows if row.get("symbol") == symbol]
        if len(matches) != 1:
            raise RuntimeError(f"币安股票列表中未找到 {symbol}")
        row = matches[0]
        if row.get("tradability") not in {"BUY_SELL", "BUY", "SELL", "NONE"}:
            raise RuntimeError(f"{symbol} 股票交易状态未知")
        for key in ("stepSize", "minQty", "maxQty", "minNotional", "maxNotional"):
            if key not in row or number(row[key]) <= 0:
                raise RuntimeError(f"{symbol} 股票规则缺少有效 {key}")
        return row

    def quote(self, symbol):
        row = self.call("GET", "market/quote", {"symbol": symbol})
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            raise RuntimeError("股票无有效报价，可能休市或停牌")
        bid, ask = number(row.get("bidPrice", "0")), number(row.get("askPrice", "0"))
        if not 0 < bid <= ask:
            raise RuntimeError("股票盘口报价无效")
        return bid, ask

    def balance(self, wallet, asset="USDC"):
        return number(self.c.spot_asset_balance_decimal(asset) if wallet == "MAIN" else self.c.funding_asset_balance(asset))

    def funding_transfer(self, method, params):
        endpoint = "/sapi/v1/asset/transfer"
        submit = lambda: self.c._run_request_with_retry(
            lambda: self.c._request_impl(self.c.spot, method, endpoint, params, timeout=6),
            max_retries=1, delay=0, base=self.c.spot, path=endpoint,
            replay_safe=method == "GET")
        return retry_failed_operation(self.c, submit) if method != "GET" else submit()


class StockStrategy:
    """One durable task per account; inventory comes exclusively from its own fills."""
    def __init__(self, client, settings: StockSettings, *, cleanup_callback=None, now=None, clock=None):
        self.c, self.api, self.settings = client, StockAPI(client), settings
        self.journal = client.operation_journal
        self.scope = exchange_operation_scope(client.key)
        self.cleanup_callback = cleanup_callback
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.clock = clock or time.monotonic
        self.state, self.entry_id = {}, ""
        self.cleanup_confirmed, self.cleanup_error, self.stop_cleanup_requested = True, "", False
        self.cleanup_exhausted, self.cleanup_failure = False, ""
        self.rules = {}

    def save(self, status="running"):
        self.journal.update(self.entry_id, status, **self.state)

    def load(self):
        if self.journal is None:
            raise RuntimeError("股票交易必须启用任务记录")
        entries = self.journal.active_entries(kind="stock-task", scope=self.scope)
        if len(entries) > 1:
            raise RuntimeError("存在多个未完成股票任务，请先完成停止清理")
        if entries:
            state = dict(entries[0]["payload"])
            if state.get("conversion"):
                state["conversion"] = None
                if state.get("active") or number(state.get("inventory", "0")):
                    state["phase"] = "CLEANUP"
                    self.journal.update(entries[0]["id"], "interrupted", **state)
                else:
                    self.journal.update(entries[0]["id"], "failed", reason="legacy-conversion-no-result")
                return self.load()
            original_settings = dict(state.get("settings", {}))
            original_settings.pop("wallet", None)  # Legacy UI selection no longer controls new orders.
            if original_settings.get("asset") == "USDT":
                original_settings["asset"] = "USDC"
            state["settings"] = original_settings
            if state.get("settings") != self.settings.payload():
                if state.get("funding_transfer") or state.get("active") or number(state.get("inventory", "0")):
                    raise RuntimeError("存在未完成股票任务，请恢复原股票设置后继续或点击停止清理")
                # Explicitly starting different settings retires only an already flat task.
                self.journal.update(entries[0]["id"], "cancelled", reason="settings-changed-after-flat")
                return self.load()
            self.entry_id, self.state = entries[0]["id"], state
            if state.get("failed_rounds"):
                self.cleanup_failure = str(state.get("cleanup_error") or "清理失败")
            self.state["settings"] = self.settings.payload()
            if self.state.get("wallet") == "MAIN":
                if self.state.get("active") or number(self.state.get("inventory", "0")):
                    # Finish only the old task's exposure before adopting funding-only trading.
                    self.state.update(phase="CLEANUP", legacy_spot_cleanup=True)
                else:
                    self.state.update(phase="INIT", cash="0", wallet="CARD")
                self.save()
        else:
            self.state = {"settings": self.settings.payload(), "completed": 0, "phase": "INIT",
                          "inventory": "0", "cash": "0", "wallet": "CARD",
                          "active": None, "buy_total": "0", "sell_total": "0", "fees": "0"}
            self.entry_id = self.journal.begin("stock-task", self.scope, dict(self.state))

    def confirm_funding_transfer(self):
        transfer = self.state.get("funding_transfer")
        if not transfer:
            return True
        # Universal transfers have no client idempotency key. An unknown reply must never be replayed.
        if not transfer.get("tranId"):
            raise ExchangeOperationFailedError("USDC划转未返回有效编号，按失败处理")
        data = self.api.funding_transfer("GET", {"type": "MAIN_FUNDING", "startTime": transfer["started_ms"], "size": 100})
        if not isinstance(data, dict) or not isinstance(data.get("rows"), list):
            raise RuntimeError("USDC划转记录无效，保留恢复记录")
        matches = [row for row in data["rows"] if isinstance(row, dict) and str(row.get("tranId")) == transfer["tranId"]]
        if not matches:
            return False
        if len(matches) != 1:
            raise RuntimeError("USDC划转记录重复，保留恢复记录")
        row = matches[0]
        if row.get("type") != "MAIN_FUNDING" or row.get("asset") != "USDC" or number(row.get("amount")) != number(transfer["amount"]):
            raise RuntimeError("USDC划转记录与本任务不符，保留恢复记录")
        if row.get("status") != "CONFIRMED" or self.api.balance("CARD") < number(transfer["before"]) + number(transfer["amount"]):
            return False
        self.state.update(funding_transfer=None, funding_collected=True)
        self.save()
        logger.info("股票USDC归集已确认：%s USDC，划转编号%s", transfer["amount"], transfer["tranId"])
        return True

    def setup_funds(self, stop):
        self._funding_retry_request = None
        def attempt():
            try:
                return self._setup_funds_once(stop)
            except Exception as exc:
                transfer = self.state.get("funding_transfer")
                if transfer and not (stop.is_set() and transfer.get("tranId")):
                    self._funding_retry_request = dict(transfer)
                    self.state["funding_transfer"] = None
                    self.save()
                if not stop.is_set() and (self.c.is_temporary_request_error(exc)
                    or isinstance(exc, BinanceAPIError) and (self.c._is_uncertain_write_error(exc) or exc.code in {-1000, -3000, -5012})
                    or transfer and isinstance(exc, RuntimeError) and not isinstance(exc, BinanceAPIError)):
                    raise ExchangeOperationFailedError("USDC划转确认失败，按失败处理") from exc
                raise
        try:
            return retry_failed_operation(self.c, attempt)
        finally:
            self._funding_retry_request = None

    def _setup_funds_once(self, stop):
        s = self.settings
        if stop.is_set():
            raise RuntimeError("已请求停止股票任务")
        if not self.state.get("funding_collected") and not self.state.get("funding_transfer"):
            retry = getattr(self, "_funding_retry_request", None)
            spot, funding = (number(retry["amount"]), number(retry["before"])) if retry else (self.api.balance("MAIN"), self.api.balance("CARD"))
            if not retry:
                self.state["funding_budget"] = text_number(spot + funding)
            if spot:
                self.state["funding_transfer"] = {"amount": text_number(spot), "before": text_number(funding),
                                                  "started_ms": self.c.current_server_time_ms(self.c.spot)}
                self.save()  # Persist the intent before the non-idempotent write.
                try:
                    reply = self.api.funding_transfer("POST", {"type": "MAIN_FUNDING", "asset": "USDC", "amount": text_number(spot)})
                except BinanceAPIError as exc:
                    if self.c._is_uncertain_write_error(exc) or exc.code in {-1000, -3000, -5012}:
                        raise
                    self.state["funding_transfer"] = None
                    if exc.code in {-1002, -2015}:
                        self.state.update(funding_collected=True, funding_budget=text_number(funding), funding_permission_skipped=True)
                        logger.warning("股票USDC归集因API划转权限被拒绝，改用资金账户已有USDC：%s；%s", funding, exc)
                    self.save()
                    if not self.state.get("funding_permission_skipped"):
                        raise
                    reply = None
                if self.state.get("funding_transfer"):
                    transfer_id = str(reply.get("tranId", "")) if isinstance(reply, dict) else ""
                    if not transfer_id.isdigit() or int(transfer_id) <= 0:
                        raise ExchangeOperationFailedError("USDC划转未返回有效编号，按失败处理")
                    self.state["funding_transfer"]["tranId"] = transfer_id
                    self.save()
            else:
                self.state["funding_collected"] = True
                self.save()
        deadline = self.clock() + s.timeout
        while self.state.get("funding_transfer"):
            if stop.is_set():
                raise RuntimeError("已请求停止股票任务")
            if self.confirm_funding_transfer():
                break
            if self.clock() >= deadline:
                raise ExchangeOperationFailedError("USDC划转确认超时，按失败处理")
            stop.wait(s.poll)
        available = min(self.api.balance("CARD"), number(self.state["funding_budget"]))
        minimum = s.amount if s.amount is not None else number(self.rules["minNotional"]) + platform_fee(number(self.rules["minNotional"])) * 2
        if self.state.get("funding_permission_skipped") and available < minimum:
            raise RuntimeError("API无划转权限且资金账户USDC不足，请自行划转USDC到资金账户或开通划转权限")
        if available <= 0 or (s.amount is not None and available < s.amount):
            raise RuntimeError("USDC 可用余额不足，请先自行准备USDC")
        # Allocate only the configured task budget; deposits during a task cannot enlarge it.
        allocation = available if s.amount is None else min(available, s.amount + platform_fee(s.amount) * 2 * s.rounds)
        self.state["cash"] = text_number(allocation)
        self.state["wallet"] = "CARD"
        self.state["phase"] = "READY"
        self.save()

    def price(self, side, anchor=None):
        session, fractional = trading_session(self.now(), self.rules)
        if side == "BUY" and self.rules.get("tradability") != "BUY_SELL":
            raise RuntimeError("股票当前不允许完整买卖，停止新买单")
        if side == "SELL" and self.rules.get("tradability") not in {"BUY_SELL", "SELL"}:
            raise RuntimeError("股票当前不可卖出，保留本任务持仓等待恢复")
        bid, ask = self.api.quote(self.settings.symbol)
        tick = self.settings.tick
        if bid % tick or ask % tick:
            raise RuntimeError("报价不符合配置价格步长，请核对币安股票交易参数")
        if side == "BUY" and (ask - bid) / bid > self.settings.spread:
            raise RuntimeError("盘口价差超过设置上限，暂停新一轮买入")
        price = (ask + tick if side == "BUY" else bid - tick).quantize(Decimal("0.01"))
        reference = ask if side == "BUY" else bid
        if price <= 0 or (anchor is not None and abs(price / anchor - 1) > self.settings.deviation):
            raise RuntimeError("股票限价超过本阶段最大价格偏离")
        return price, reference, session, fractional

    def quantity(self, quantity, price, fractional):
        step = number(self.rules["stepSize"]) if fractional else max(number(self.rules["stepSize"]), Decimal(1))
        qty = (quantity / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if not number(self.rules["minQty"]) <= qty <= number(self.rules["maxQty"]) or not number(self.rules["minNotional"]) <= qty * price <= number(self.rules["maxNotional"]):
            raise RuntimeError("股票股数或订单金额不满足交易规则（盘外可能仅支持整股）")
        return qty

    def reconcile(self):
        def attempt():
            try:
                return self._read_order_once()
            except Exception as exc:
                if isinstance(exc, ExchangeOperationFailedError) or self.c.is_temporary_request_error(exc) or isinstance(exc, BinanceAPIError) and exc.code == -2013 or isinstance(exc, (ValueError, RuntimeError)) and not isinstance(exc, BinanceAPIError):
                    raise ExchangeOperationFailedError(f"股票订单确认失败，按失败处理：{exc}") from exc
                raise
        return retry_failed_operation(self.c, attempt)

    def _read_order_once(self):
        active = self.state.get("active")
        if not active:
            return None
        row = self.api.call("GET", "order/detail", {"clientOrderId": active["clientOrderId"]})
        if not isinstance(row, dict) or row.get("clientOrderId") != active["clientOrderId"] or row.get("symbol") != self.settings.symbol or row.get("side") != active["side"]:
            raise RuntimeError("股票订单查询返回不完整或标识不匹配，按失败处理")
        if active.get("orderId") and str(row.get("orderId")) != str(active["orderId"]):
            raise RuntimeError("股票订单编号与原订单不匹配，按失败处理")
        status = row.get("status")
        if status not in TERMINAL | {"NEW", "ACCEPTED", "PARTIALLY_FILLED"}:
            raise RuntimeError("股票订单状态未知，禁止重挂")
        if status not in TERMINAL and not row.get("orderId"):
            raise RuntimeError("股票原订单标识未确认，禁止撤单或重挂")
        qty = number(row["filledQty"])
        old = number(active.get("seenQty", "0"))
        if qty < old or qty > number(active["quantity"]):
            raise RuntimeError("股票成交股数异常")
        if status == "FILLED" and qty != number(active["quantity"]):
            raise RuntimeError("股票完成状态与成交股数不一致，按失败处理")
        total = qty * number(row.get("avgFilledPrice") or "0")
        fee = number(row.get("fee", "0"))
        if qty and total <= 0:
            raise RuntimeError("股票成交价格未确认")
        sign = 1 if active["side"] == "BUY" else -1
        self.state["inventory"] = text_number(number(self.state["inventory"]) + sign * (qty - old))
        cash = number(self.state["cash"]) - sign * (total - number(active.get("seenTotal", "0"))) - (fee - number(active.get("seenFee", "0")))
        self.state["cash"] = text_number(max(cash, Decimal(0)))
        field = "buy_total" if sign == 1 else "sell_total"
        self.state[field] = text_number(number(self.state[field]) + total - number(active.get("seenTotal", "0")))
        self.state["fees"] = text_number(number(self.state["fees"]) + fee - number(active.get("seenFee", "0")))
        active.update(seenQty=text_number(qty), seenTotal=text_number(total), seenFee=text_number(fee), orderId=row.get("orderId"))
        if status in TERMINAL:
            self.state["active"] = None
        self.save()
        return row

    def cancel(self):
        active = self.state.get("active")
        if not active:
            return
        if not active.get("orderId"):
            self.reconcile()
        active = self.state.get("active")
        if active:
            # Mark before cancel; a lost cancel response must not cause a new order.
            active["cancelRequested"] = True
            self.save()
            self.api.call("POST", "order/cancel", {"orderId": active["orderId"]})
            self.reconcile()

    def place(self, side, qty, price, session):
        def attempt():
            params = {"symbol": self.settings.symbol, "side": side, "orderType": "LIMIT",
                      "quantity": text_number(qty), "price": text_number(price), "quoteAsset": "USDC",
                      "walletType": self.state["wallet"], "tradingSession": session,
                      "timeInForce": "DAY", "tokenize": "false", "clientOrderId": uuid.uuid4().hex}
            self.state["active"] = dict(params)
            self.save()
            try:
                result = self.api.call("POST", "order/place", params)
            except Exception:
                self.state["active"] = None
                self.save()
                raise
            if not isinstance(result, dict) or result.get("status") != "S" or not result.get("orderId"):
                self.state["active"] = None
                self.save()
                if isinstance(result, dict) and result.get("status") == "F":
                    raise RuntimeError("股票下单被拒绝")
                raise ExchangeOperationFailedError("股票下单无法确认结果，按失败处理")
            self.state["active"]["orderId"] = result["orderId"]
            try:
                self.save()
            except Exception as exc:
                raise ExchangeOperationFailedError("股票订单回执保存失败", retryable=False) from exc
        return retry_failed_operation(self.c, attempt)

    def execute_side(self, side, stop, progress):
        started, repriced = self.clock(), self.clock()
        if "anchor" not in self.state:
            _, anchor, _, _ = self.price(side)
            self.state["anchor"] = text_number(anchor)
            self.save()
        while True:
            if stop.is_set():
                raise RuntimeError("已请求停止股票任务")
            if self.clock() - started >= self.settings.timeout:
                raise RuntimeError(f"股票{side}等待超过上限")
            if self.state.get("active"):
                try:
                    row = self.reconcile()
                except Exception as exc:
                    if self.c.is_temporary_request_error(exc) or isinstance(exc, BinanceAPIError) and exc.code == -2013:
                        progress(self.state["completed"], self.settings.rounds, "股票原订单待确认，暂停重挂")
                        stop.wait(self.settings.poll)
                        continue
                    raise
                if row and row.get("status") == "REJECTED":
                    raise RuntimeError("股票订单被拒绝，请检查交易资格、资金或可卖股数")
            remaining = (number(self.state["target"]) - number(self.state["inventory"])) if side == "BUY" else number(self.state["inventory"])
            if remaining <= 0 and not self.state.get("active"):
                return
            active = self.state.get("active")
            if active and active.get("cancelRequested"):
                # Acknowledged cancellation is not final; wait for the terminal order state.
                stop.wait(self.settings.poll)
                continue
            if not active or self.clock() - repriced >= self.settings.reprice:
                price, _, session, fractional = self.price(side, number(self.state["anchor"]))
                if active and number(active["price"]) != price:
                    self.cancel()
                    repriced = self.clock()
                    continue
                if not active:
                    qty = self.quantity(remaining, price, fractional)
                    if side == "BUY" and qty * price + platform_fee(qty * price) > min(number(self.state["cash"]), self.api.balance(self.state["wallet"])):
                        raise RuntimeError("重挂所需资金超过任务预算，停止追加买入")
                    self.place(side, qty, price, session)
                repriced = self.clock()
            processed = self.state["completed"] + len(self.state.get("failed_rounds", []))
            progress(processed, self.settings.rounds,
                     f"股票第{processed + 1}轮 {side}，剩余{remaining}股")
            stop.wait(self.settings.poll)

    def cleanup(self):
        self.state["phase"] = "CLEANUP"
        self.save()
        with self.c.cleanup_requests_after_stop():
            if not self.confirm_funding_transfer():
                raise RuntimeError("USDC划转尚未确认到账，保留恢复记录")
            if self.state.get("active"):
                self.reconcile()
                self.cancel()
            if self.state.get("active"):
                raise RuntimeError("股票撤单尚未确认")
            if number(self.state.get("inventory", "0")) > 0:
                anchor = number(self.state["cleanup_anchor"]) if self.state.get("cleanup_anchor") else None
                price, reference, session, fractional = self.price("SELL", anchor)
                if anchor is None:
                    self.state["cleanup_anchor"] = text_number(reference)
                qty = self.quantity(number(self.state["inventory"]), price, fractional)
                if qty != number(self.state["inventory"]):
                    raise RuntimeError("本任务残余股票在当前时段不可全部卖出")
                if self.state["wallet"] == "MAIN":
                    self.state["cash"] = "0"
                self.state["wallet"] = "CARD"
                self.place("SELL", qty, price, session)
                self.reconcile()
            if self.state.get("active") or number(self.state.get("inventory", "0")):
                raise RuntimeError("股票清理尚未成交，保留持仓等待重试")
        self.cleanup_confirmed, self.cleanup_error = True, ""
        self.state["phase"] = "READY" if number(self.state["cash"]) else "INIT"
        self.state.pop("anchor", None)
        self.state.pop("cleanup_anchor", None)
        self.save("cancelled" if self.stop_cleanup_requested else "interrupted")

    def retry_stop_cleanup(self):
        if isinstance(self.c, BinanceClient) and self.c.session is None:
            self.c._reset_session_after_transport_error()
        self.cleanup_exhausted = False
        return self._cleanup_with_retries(refresh_rules=True)

    def _cleanup_with_retries(self, *, refresh_rules=False):
        if self.cleanup_exhausted:
            return False
        self.cleanup_confirmed = False
        def attempt():
            if refresh_rules and (self.state.get("active") or number(self.state.get("inventory", "0"))):
                self.rules = self.api.rules(self.settings.symbol)
            self.cleanup()
        try:
            retry_cleanup(self.c, attempt)
        except Exception as exc:
            self.cleanup_exhausted = True
            self.cleanup_failure = self.cleanup_error = "清理失败：" + str(exc)
            self.state["cleanup_error"] = self.cleanup_error
            self.save("failed")
            logger.error("股票清理失败，结束本次清理并继续后续任务：%s", exc)
        finally:
            if self.cleanup_callback:
                self.cleanup_callback(self)
        return self.cleanup_confirmed

    def _continue_after_failed_cleanup(self):
        # Keep the failed task's original order and inventory in encrypted history.
        # Later rounds own only their new fills, never the abandoned holdings.
        failed = list(self.state.get("failed_rounds", []))
        if self.state["completed"] + len(failed) + 1 >= self.settings.rounds:
            failed.append({"round": self.settings.rounds, "inventory": "0", "active": None,
                           "error": self.cleanup_error})
            self.state["failed_rounds"] = failed
            self.save("failed")
            return
        failed.append({"round": self.state["completed"] + len(failed) + 1,
                       "inventory": self.state["inventory"], "active": self.state.get("active"),
                       "error": self.cleanup_error})
        next_state = {**self.state, "phase": "READY", "inventory": "0", "active": None,
                      "failed_rounds": failed}
        for key in ("anchor", "cleanup_anchor", "target"):
            next_state.pop(key, None)
        entry_id = self.journal.begin("stock-task", self.scope, next_state)
        self.entry_id, self.state = entry_id, next_state
        self.cleanup_exhausted = False

    def run(self, stop, progress=lambda *_: None):
        error = ""
        try:
            self.load()
            self.rules = self.api.rules(self.settings.symbol)
            trading_session(self.now(), self.rules)
            if self.state["phase"] == "CLEANUP":
                if not self._cleanup_with_retries():
                    if stop.is_set():
                        raise RuntimeError(self.cleanup_error)
                    self._continue_after_failed_cleanup()
            if self.state.pop("legacy_spot_cleanup", False):
                self.state.update(phase="INIT", cash="0", wallet="CARD")
                self.save()
            if self.state["phase"] == "INIT":
                self.setup_funds(stop)
            while self.state["completed"] + len(self.state.get("failed_rounds", [])) < self.settings.rounds:
                if stop.is_set():
                    raise RuntimeError("已请求停止股票任务")
                try:
                    if self.state["phase"] == "READY":
                        price, anchor, _, fractional = self.price("BUY")
                        budget = number(self.state["cash"]) if self.settings.amount is None else self.settings.amount
                        if budget > min(number(self.state["cash"]), self.api.balance(self.state["wallet"])):
                            raise RuntimeError("固定金额所需资金不足，停止后续轮次")
                        qty = self.quantity((budget - platform_fee(budget) * 2) / price, price, fractional)
                        self.state.update(phase="BUY", target=text_number(qty), anchor=text_number(anchor))
                        self.save()
                    if self.state["phase"] == "BUY":
                        self.execute_side("BUY", stop, progress)
                        self.state["wallet"], self.state["phase"] = "CARD", "SELL"
                        self.state.pop("anchor", None)
                        self.save()
                    if self.state["phase"] == "SELL":
                        self.execute_side("SELL", stop, progress)
                        self.state["completed"] += 1
                        self.state["phase"] = "READY"
                        self.state.pop("anchor", None)
                        self.save()
                except Exception as round_exc:
                    if isinstance(round_exc, ExchangeOperationFailedError) or not (self.state.get("active") or number(self.state.get("inventory", "0"))):
                        raise
                    if self._cleanup_with_retries() or stop.is_set():
                        raise
                    self._continue_after_failed_cleanup()
                processed = self.state["completed"] + len(self.state.get("failed_rounds", []))
                progress(processed, self.settings.rounds, f"股票{self.settings.symbol}已处理{processed}/{self.settings.rounds}轮")
                if processed < self.settings.rounds:
                    stop.wait(self.settings.interval)
            error = self.cleanup_failure
            self.save("failed" if error else "completed")
        except Exception as exc:
            error = str(exc)
            self.stop_cleanup_requested = bool(stop.is_set())
            if self.entry_id:
                self.cleanup_confirmed = not self.state.get("conversion") and not self.state.get("funding_transfer") and not self.state.get("active") and not number(self.state.get("inventory", "0"))
                if not self.cleanup_confirmed:
                    self._cleanup_with_retries()
                if self.cleanup_confirmed and self.state["phase"] != "INIT":
                    self.state["phase"] = "READY"
                    self.state.pop("anchor", None)
                # A failed preflight has no orders to resume. A fresh start may use manually replenished USDC.
                preflight_failed = self.cleanup_confirmed and self.state["phase"] == "INIT" and not number(self.state.get("buy_total", "0")) and not number(self.state.get("sell_total", "0"))
                status = "failed" if self.cleanup_exhausted or self.cleanup_failure else "cancelled" if self.stop_cleanup_requested and self.cleanup_confirmed else "failed" if preflight_failed or isinstance(exc, ExchangeOperationFailedError) and self.cleanup_confirmed else "interrupted"
                self.save(status)
                if not self.cleanup_confirmed and self.cleanup_callback:
                    self.cleanup_callback(self)
            logger.error("股票任务未完成：%s", error)
        return {"completed": self.state.get("completed", 0), "error": error,
                "cleanup_error": self.cleanup_failure or self.cleanup_error, "success": not error,
                "cleanup_exhausted": self.cleanup_exhausted,
                "failed_rounds": self.state.get("failed_rounds", []),
                "processed": self.state.get("completed", 0) + len(self.state.get("failed_rounds", [])),
                "inventory": text_number(number(self.state.get("inventory", "0")) + sum((number(row["inventory"]) for row in self.state.get("failed_rounds", [])), Decimal(0))), "fees": self.state.get("fees", "0"),
                "buy_total": self.state.get("buy_total", "0"), "sell_total": self.state.get("sell_total", "0")}
