"""
test_classification.py - Unit tests for record classification logic.
Tests that records are correctly classified as Valid, Corrected, or Quarantined.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.quality_rules import check_quarantine, apply_cleaning_rules


class TestQuarantineMissingOrderId:
    def test_empty_order_id(self):
        record = {"order_id": "", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "MISSING_ORDER_ID" in codes

    def test_none_order_id(self):
        record = {"order_id": None, "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "MISSING_ORDER_ID" in codes


class TestQuarantineMissingCustomerId:
    def test_empty_customer_id(self):
        record = {"order_id": "طلب-100", "customer_id": "", "order_date": "2025-01-15T10:00:00",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "MISSING_CUSTOMER_ID" in codes


class TestQuarantineInvalidDate:
    def test_impossible_date(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "invalid-date",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "INVALID_IMPOSSIBLE_DATE" in codes

    def test_future_date(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2099-01-01T00:00:00",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "INVALID_IMPOSSIBLE_DATE" in codes


class TestQuarantineCorruptedJson:
    def test_invalid_json(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": "not-json-at-all", "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "CORRUPTED_ITEMS_JSON" in codes


class TestQuarantineEmptyItems:
    def test_empty_array(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": "[]", "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "EMPTY_ITEMS" in codes

    def test_empty_string(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": "", "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "EMPTY_ITEMS" in codes


class TestQuarantineUnknownPrice:
    def test_non_numeric_price(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
                  "total_amount": "???"}
        codes, details = check_quarantine(record)
        assert "UNKNOWN_PRICE" in codes


class TestQuarantineNegativeValue:
    def test_negative_quantity(self):
        record = {"order_id": "طلب-100", "customer_id": "عميل-1", "order_date": "2025-01-15T10:00:00",
                  "items_json": '[{"sku":"A","qty":-2,"unit_price":100,"total":100}]',
                  "total_amount": "100"}
        codes, details = check_quarantine(record)
        assert "AMBIGUOUS_NEGATIVE_VALUE" in codes


class TestQuarantineMultipleErrors:
    def test_many_errors(self):
        record = {"order_id": "", "customer_id": "", "order_date": "invalid",
                  "items_json": "not-json", "total_amount": "???"}
        codes, details = check_quarantine(record)
        assert "MULTIPLE_CONFLICTING_ERRORS" in codes
        assert len(codes) >= 3


class TestValidRecord:
    def test_clean_record_no_quarantine(self):
        record = {
            "order_id": "طلب-100",
            "customer_id": "عميل-1",
            "order_date": "2025-01-15T10:00:00",
            "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
            "total_amount": "100",
        }
        codes, details = check_quarantine(record)
        assert len(codes) == 0


class TestClassificationFlow:
    """Test the full classification flow: clean → check → classify."""

    def test_valid_record(self):
        record = {
            "order_id": "طلب-200",
            "order_date": "2025-03-15T14:30:00",
            "status": "مؤكد",
            "customer_id": "عميل-5",
            "customer_name": "علي",
            "customer_phone": "771234567",
            "customer_email": "ali@example.com",
            "city": "صنعاء",
            "district": "حدة",
            "delivery_type": "عادي",
            "delivery_cost": "2000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "5000",
            "currency": "YER",
            "total_amount": "5000",
            "items_json": '[{"sku":"A","qty":1,"unit_price":3000,"total":3000}]',
        }
        cleaned, corrections = apply_cleaning_rules(record)
        codes, _ = check_quarantine(cleaned)
        assert len(codes) == 0
        assert len(corrections) == 0
        # Classification: valid

    def test_corrected_record(self):
        record = {
            "order_id": "طلب-201",
            "order_date": "2025-03-15T14:30:00",
            "status": "مؤكد",
            "customer_id": "عميل-5",
            "customer_name": "علي",
            "customer_phone": "771234567",
            "customer_email": "ali@@example..com",  # needs correction
            "city": "صنعاء",
            "district": "حدة",
            "delivery_type": "عادي",
            "delivery_cost": "2000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "٥٠٠٠",  # Arabic digits need correction
            "currency": "ريال يمني",  # needs normalization
            "total_amount": "5000",
            "items_json": '[{"sku":"A","qty":1,"unit_price":3000,"total":3000}]',
        }
        cleaned, corrections = apply_cleaning_rules(record)
        codes, _ = check_quarantine(cleaned)
        assert len(codes) == 0  # No quarantine
        assert len(corrections) > 0  # Has corrections
        # Classification: corrected

    def test_quarantined_record(self):
        record = {
            "order_id": "",  # Missing!
            "order_date": "invalid-date",
            "status": "مؤكد",
            "customer_id": "عميل-5",
            "customer_name": "علي",
            "customer_phone": "771234567",
            "customer_email": "ali@example.com",
            "city": "صنعاء",
            "district": "حدة",
            "delivery_type": "عادي",
            "delivery_cost": "2000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "5000",
            "currency": "YER",
            "total_amount": "5000",
            "items_json": "not-valid-json",  # Corrupted!
        }
        cleaned, corrections = apply_cleaning_rules(record)
        codes, _ = check_quarantine(cleaned)
        assert len(codes) > 0  # Should be quarantined
        assert "MISSING_ORDER_ID" in codes


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
