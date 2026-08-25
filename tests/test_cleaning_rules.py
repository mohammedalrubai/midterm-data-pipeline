"""
test_cleaning_rules.py - Unit tests for all cleaning rules.
Tests each rule individually with known inputs and expected outputs.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.quality_rules import (
    rule_arabic_digits,
    rule_currency,
    rule_thousands_sep,
    rule_word_price,
    rule_phone,
    rule_email,
    rule_date,
    rule_whitespace_status,
    rule_recalculate_total,
    apply_cleaning_rules,
)


# ──── Rule 1: Arabic Digits ────
class TestArabicDigits:
    def test_basic_conversion(self):
        result, changed = rule_arabic_digits("١٢٣")
        assert result == "123"
        assert changed is True

    def test_mixed_digits(self):
        result, changed = rule_arabic_digits("٧٠٦٠٠٠٫٠")
        assert result == "706000.0"
        assert changed is True

    def test_no_change(self):
        result, changed = rule_arabic_digits("12345")
        assert result == "12345"
        assert changed is False

    def test_arabic_decimal(self):
        result, changed = rule_arabic_digits("٨٥٠٠٫٠")
        assert result == "8500.0"
        assert changed is True


# ──── Rule 2: Currency ────
class TestCurrency:
    def test_rial_text(self):
        result, changed = rule_currency("ريال")
        assert result == "YER"
        assert changed is True

    def test_rial_yemeni(self):
        result, changed = rule_currency("ريال يمني")
        assert result == "YER"
        assert changed is True

    def test_already_yer(self):
        result, changed = rule_currency("YER")
        # "YER" lowered is "yer" which maps to "YER"
        assert result == "YER"

    def test_yer_lowercase(self):
        result, changed = rule_currency("yer")
        assert result == "YER"
        assert changed is True


# ──── Rule 3: Thousands Separators ────
class TestThousandsSep:
    def test_basic(self):
        result, changed = rule_thousands_sep("125,000.00")
        assert result == "125000.00"
        assert changed is True

    def test_millions(self):
        result, changed = rule_thousands_sep("1,000,000")
        assert result == "1000000"
        assert changed is True

    def test_no_change(self):
        result, changed = rule_thousands_sep("5000.0")
        assert result == "5000.0"
        assert changed is False


# ──── Rule 4: Word Prices ────
class TestWordPrice:
    def test_five_thousand(self):
        result, changed = rule_word_price("خمسة آلاف")
        assert result == "5000"
        assert changed is True

    def test_two_thousand(self):
        result, changed = rule_word_price("ألفان")
        assert result == "2000"
        assert changed is True

    def test_unknown_word(self):
        result, changed = rule_word_price("مليون")
        assert changed is False


# ──── Rule 5: Phone ────
class TestPhone:
    def test_spaces(self):
        result, changed = rule_phone("967 77 123 4567")
        assert result == "967771234567"
        assert changed is True

    def test_plus_prefix(self):
        result, changed = rule_phone("+967771234567")
        assert result == "967771234567"
        assert changed is True

    def test_clean_number(self):
        result, changed = rule_phone("771234567")
        assert result == "771234567"


# ──── Rule 6: Email ────
class TestEmail:
    def test_double_at(self):
        result, changed = rule_email("user@@mail.com")
        assert result == "user@mail.com"
        assert changed is True

    def test_double_dot(self):
        result, changed = rule_email("user@mail..com")
        assert result == "user@mail.com"
        assert changed is True

    def test_combined(self):
        result, changed = rule_email("user@@mail..com")
        assert result == "user@mail.com"
        assert changed is True

    def test_clean_email(self):
        result, changed = rule_email("user@example.com")
        assert result == "user@example.com"
        assert changed is False


# ──── Rule 7: Date ────
class TestDate:
    def test_iso_format(self):
        result, changed = rule_date("2025-02-24T21:29:00")
        assert result == "2025-02-24T21:29:00"
        assert changed is False

    def test_dd_mm_yyyy(self):
        result, changed = rule_date("31-01-2025")
        assert result == "2025-01-31T00:00:00"
        assert changed is True

    def test_spaces_in_date(self):
        result, changed = rule_date("2025 /01 /31")
        assert result == "2025-01-31T00:00:00"
        assert changed is True


# ──── Rule 8: Whitespace + Status ────
class TestWhitespaceStatus:
    def test_trim_spaces(self):
        result, changed = rule_whitespace_status("  مؤكد  ", field_name="status")
        assert result == "مؤكد"
        assert changed is True

    def test_synonym(self):
        result, changed = rule_whitespace_status("ملغى", field_name="status")
        assert result == "ملغي"
        assert changed is True

    def test_payment_synonym(self):
        result, changed = rule_whitespace_status("مدفوع", field_name="payment_status")
        assert result == "تم الدفع"
        assert changed is True


# ──── Rule 9: Total Recalculation ────
class TestRecalculateTotal:
    def test_correct_total(self):
        items = '[{"sku":"A","qty":2,"unit_price":100,"total":200}]'
        result, changed = rule_recalculate_total(items, "50", "250")
        assert changed is False  # 200 + 50 = 250, correct

    def test_incorrect_total(self):
        items = '[{"sku":"A","qty":2,"unit_price":100,"total":200}]'
        result, changed = rule_recalculate_total(items, "50", "999")
        assert result == "250.0"  # Should be 200 + 50
        assert changed is True


# ──── Integration: apply_cleaning_rules ────
class TestApplyCleaningRules:
    def test_returns_corrections(self):
        record = {
            "order_id": "طلب-100",
            "order_date": "2025-01-15T10:00:00",
            "status": "مؤكد",
            "customer_id": "عميل-1",
            "customer_name": "أحمد",
            "customer_phone": "967 77 123 4567",
            "customer_email": "user@@mail..com",
            "city": "صنعاء",
            "district": "حدة",
            "delivery_type": "عادي",
            "delivery_cost": "2000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "٥٠٠٠",
            "currency": "ريال يمني",
            "total_amount": "5000",
            "items_json": '[]',
        }
        cleaned, corrections = apply_cleaning_rules(record)

        # Should have corrections for phone, email, payment_amount (arabic digits), currency
        assert len(corrections) > 0
        correction_fields = [c["field"] for c in corrections]
        assert "customer_email" in correction_fields
        assert "currency" in correction_fields

    def test_clean_record_no_corrections(self):
        record = {
            "order_id": "طلب-100",
            "order_date": "2025-01-15T10:00:00",
            "status": "مؤكد",
            "customer_id": "عميل-1",
            "customer_name": "أحمد",
            "customer_phone": "771234567",
            "customer_email": "user@example.com",
            "city": "صنعاء",
            "district": "حدة",
            "delivery_type": "عادي",
            "delivery_cost": "2000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "5000",
            "currency": "YER",
            "total_amount": "5000",
            "items_json": '[]',
        }
        cleaned, corrections = apply_cleaning_rules(record)
        assert len(corrections) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
