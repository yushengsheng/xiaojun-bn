from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from exchange_app_base import ExchangeAppBase
from input_validation import bounded_int, finite_decimal, finite_float
from onchain_transfer_runner import OnchainTransferRunnerMixin


class _Var:
    def __init__(self, value):
        self.value = value
    def get(self):
        return self.value
    def set(self, value):
        self.value = value


class _Owner(OnchainTransferRunnerMixin):
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数量"
    AMOUNT_MODE_ALL = "全部"
    AMOUNT_ALL_LABEL = "全部"
    MAX_TOKEN_DECIMALS = 36
    def __init__(self):
        values = dict(network="BSC", amount="1", random_min="1", random_max="2", delay="0", threads="4",
                      one_to_many_source_threads="4", confirm_timeout="30", receipt_poll_interval="0.1",
                      receipt_retry_count="3", nonce_window="2", relay_fee_reserve="0.001", mode="1对多")
        for key, value in values.items():
            setattr(self, key + "_var", _Var(value))
        self.token = SimpleNamespace(symbol="BNB", is_native=True, contract="", decimals=18)
        self.client = SimpleNamespace(is_address=lambda _value: True)
        self.amount_mode = self.AMOUNT_MODE_FIXED
        self.relay_enabled = False
    def _amount_mode(self):
        return self.amount_mode
    def _mode(self):
        return self.mode_var.get()
    def _is_mode_1m(self):
        return self._mode() == "1对多"
    def _is_mode_m1(self):
        return self._mode() == "多对1"
    def _selected_token(self, **kwargs):
        return self.token
    def _relay_enabled(self):
        return self.relay_enabled
    def _decimal_to_text(self, value):
        return str(value)


class RuntimeInputValidationTests(unittest.TestCase):
    def test_nonfinite_numbers_are_rejected_by_exchange_and_shared_validation(self):
        for value in ("NaN", "sNaN", "Infinity", "-Infinity", "nan", "inf"):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    ExchangeAppBase._decimal_field_value(value, "金额")
                with self.assertRaises(RuntimeError):
                    finite_decimal(value, "金额")
                with self.assertRaises(RuntimeError):
                    finite_float(value, "等待时间")

    def test_numeric_limits_keep_valid_values_and_reject_excessive_workers(self):
        self.assertEqual(bounded_int("128", "线程数"), 128)
        self.assertEqual(finite_float("0", "间隔"), 0)
        self.assertEqual(finite_float("0.1", "轮询", positive=True), 0.1)
        self.assertEqual(finite_decimal("0.00001", "金额"), Decimal("0.00001"))
        for value in ("129", "0", "1.5", "NaN"):
            with self.assertRaises(RuntimeError):
                bounded_int(value, "线程数")
        with self.assertRaises(RuntimeError):
            finite_float("86401", "等待时间")

    def test_onchain_rejects_nonfinite_amounts_and_waits_without_launching(self):
        for field in ("amount", "delay", "confirm_timeout", "receipt_poll_interval", "threads", "nonce_window", "receipt_retry_count"):
            for invalid in ("NaN", "Infinity"):
                with self.subTest(field=field, value=invalid):
                    owner = _Owner()
                    getattr(owner, field + "_var").set(invalid)
                    with patch("onchain_transfer_runner.messagebox.showerror") as error:
                        self.assertIsNone(owner._validate_transfer_params())
                        error.assert_called_once()

    def test_onchain_random_and_reserve_are_finite(self):
        for reserve in (False, True):
            owner = _Owner()
            if reserve:
                owner.relay_enabled = True
                owner.relay_fee_reserve_var.set("NaN")
            else:
                owner.amount_mode = owner.AMOUNT_MODE_RANDOM
                owner.random_min_var.set("NaN")
            with patch("onchain_transfer_runner.messagebox.showerror"):
                self.assertIsNone(owner._validate_transfer_params())

    def test_onchain_mode_is_captured_with_the_other_immutable_parameters(self):
        owner = _Owner()
        params = owner._validate_transfer_params()
        self.assertIsNotNone(params)
        owner.mode_var.set("多对1")
        self.assertEqual(params.mode, "1对多")

    def test_placeholder_delay_is_compatible_but_extreme_delay_is_rejected(self):
        owner = SimpleNamespace(_normalized_entry_text=ExchangeAppBase._normalized_entry_text)
        self.assertEqual(ExchangeAppBase._delay_var_value(owner, _Var("最小"), 1000, "最小"), 1000)
        self.assertEqual(ExchangeAppBase._delay_var_value(owner, _Var("0"), 1000, "最小"), 0)
        with self.assertRaises(RuntimeError):
            ExchangeAppBase._delay_var_value(owner, _Var("999999999999999999999"), 1000, "最小")


if __name__ == "__main__":
    unittest.main()
