"""
quality_rules.py - Automated cleaning rules (8+ rules) and quarantine validation.

Each cleaning rule:
  - Takes a value, returns (cleaned_value, changed: bool)
  - If changed, the caller records the correction in the audit trail

Quarantine rules check for fatal errors that cannot be safely corrected.

Per the assignment: "Do NOT correct a value unless the conversion rule is clear
and does not rely on guessing."
"""
import re
import json
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# RULE 1: Arabic/Eastern Numerals → Latin
# ══════════════════════════════════════════════════════════════
ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
LATIN_DIGITS = "0123456789"
ARABIC_DECIMAL = "٫"  # Arabic decimal separator


def rule_arabic_digits(value):
    """Convert Eastern Arabic digits (٠-٩) and decimal separator to Latin."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    for ar, la in zip(ARABIC_DIGITS, LATIN_DIGITS):
        value = value.replace(ar, la)
    value = value.replace(ARABIC_DECIMAL, ".")
    return value, (value != original)


# ══════════════════════════════════════════════════════════════
# RULE 2: Currency Normalization → "YER"
# ══════════════════════════════════════════════════════════════
CURRENCY_MAP = {
    "yer": "YER",
    "ريال": "YER",
    "ريال يمني": "YER",
    "ر.ي": "YER",
    "ر.ي.": "YER",
    "يمني ريال": "YER",
}


def rule_currency(value):
    """Normalize currency symbol/name to standard 'YER'."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip().lower()
    if cleaned in CURRENCY_MAP:
        normalized = CURRENCY_MAP[cleaned]
        return normalized, (normalized != original.strip())
    return value.strip(), (value.strip() != original)


# ══════════════════════════════════════════════════════════════
# RULE 3: Remove Thousands Separators
# ══════════════════════════════════════════════════════════════
def rule_thousands_sep(value):
    """Remove thousands commas from numbers: 125,000.00 → 125000.00"""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip()
    # Only process if it looks like a number with commas
    while re.search(r"(\d),(\d{3})", cleaned):
        cleaned = re.sub(r"(\d),(\d{3})", r"\1\2", cleaned)
    return cleaned, (cleaned != original)


# ══════════════════════════════════════════════════════════════
# RULE 4: Price in Arabic Words → Number
# ══════════════════════════════════════════════════════════════
WORD_NUMBERS = {
    "صفر": "0",
    "ألف": "1000", "الف": "1000",
    "ألفين": "2000", "الفين": "2000",
    "ألفان": "2000", "الفان": "2000",
    "ثلاثة آلاف": "3000", "ثلاثة الاف": "3000", "ثلاث آلاف": "3000",
    "أربعة آلاف": "4000", "اربعة الاف": "4000", "اربع آلاف": "4000",
    "خمسة آلاف": "5000", "خمسة الاف": "5000", "خمس آلاف": "5000",
    "ستة آلاف": "6000", "سبعة آلاف": "7000",
    "ثمانية آلاف": "8000", "تسعة آلاف": "9000",
    "عشرة آلاف": "10000", "عشر آلاف": "10000",
}


def rule_word_price(value):
    """Convert known Arabic word prices to numeric strings."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    cleaned = value.strip()
    if cleaned in WORD_NUMBERS:
        return WORD_NUMBERS[cleaned], True
    return cleaned, (cleaned != value)


# ══════════════════════════════════════════════════════════════
# RULE 5: Phone Number Normalization
# ══════════════════════════════════════════════════════════════
def rule_phone(value):
    """Normalize phone: remove spaces, standardize format."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip()
    # Remove all spaces
    cleaned = re.sub(r"\s+", "", cleaned)
    # Remove leading + if present
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]
    # Remove dashes
    cleaned = cleaned.replace("-", "")
    # Keep only digits
    digits = re.sub(r"[^\d]", "", cleaned)
    if len(digits) >= 9:
        return digits, (digits != original.strip())
    return value.strip(), (value.strip() != original)


# ══════════════════════════════════════════════════════════════
# RULE 6: Email Sanitization
# ══════════════════════════════════════════════════════════════
def rule_email(value):
    """Fix obvious email errors: double @@, consecutive dots .."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip().lower()
    # Fix repeated @@
    while "@@" in cleaned:
        cleaned = cleaned.replace("@@", "@")
    # Fix repeated dots
    while ".." in cleaned:
        cleaned = cleaned.replace("..", ".")
    # Remove leading/trailing dots in domain
    parts = cleaned.split("@")
    if len(parts) == 2:
        parts[1] = parts[1].strip(".")
        if parts[0]:
            parts[0] = parts[0].strip(".")
        cleaned = "@".join(parts)
    return cleaned, (cleaned != original)


# ══════════════════════════════════════════════════════════════
# RULE 7: Date Normalization → ISO 8601
# ══════════════════════════════════════════════════════════════
def rule_date(value):
    """Convert various date formats to ISO 8601 (YYYY-MM-DDTHH:MM:SS)."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip()

    if not cleaned:
        return cleaned, False

    # Already ISO format
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", cleaned):
        return cleaned, False

    # Remove extra spaces around separators
    cleaned = re.sub(r"\s*/\s*", "/", cleaned)
    cleaned = re.sub(r"\s*-\s*", "-", cleaned)
    cleaned = cleaned.strip()

    date_formats = [
        ("%Y-%m-%dT%H:%M:%S", False),
        ("%Y-%m-%d %H:%M:%S", False),
        ("%Y-%m-%d", False),
        ("%d-%m-%Y", True),
        ("%d/%m/%Y", True),
        ("%Y/%m/%d", False),
        ("%d-%m-%Y %H:%M:%S", True),
        ("%d/%m/%Y %H:%M:%S", True),
        ("%Y/%m/%d %H:%M:%S", False),
        ("%m/%d/%Y", True),
    ]

    for fmt, _ in date_formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            result = dt.strftime("%Y-%m-%dT%H:%M:%S")
            return result, (result != original.strip())
        except ValueError:
            continue

    return cleaned, (cleaned != original)


# ══════════════════════════════════════════════════════════════
# RULE 8: Whitespace + Status/Synonyms Mapping
# ══════════════════════════════════════════════════════════════
STATUS_SYNONYMS = {
    "مؤكد": "مؤكد",
    "مأكد": "مؤكد",
    "دكؤم": "مؤكد",
    "مٶكد": "مؤكد",
    "مءكد": "مؤكد",
    "قيد الانتظار": "قيد الانتظار",
    "بالانتظار": "قيد الانتظار",
    "انتظار": "قيد الانتظار",
    "قيد الشحن": "قيد الشحن",
    "جاري الشحن": "قيد الشحن",
    "تم التسليم": "تم التسليم",
    "مسلم": "تم التسليم",
    "تم الاستلام": "تم التسليم",
    "مرتجع": "مرتجع",
    "مسترجع": "مرتجع",
    "ملغي": "ملغي",
    "ملغى": "ملغي",
    "الغاء": "ملغي",
    "تم الدفع": "تم الدفع",
    "مدفوع": "تم الدفع",
    "بانتظار الدفع": "بانتظار الدفع",
    "مرفوض": "مرفوض",
}


def rule_whitespace_status(value, field_name=""):
    """Trim whitespace and map status/payment_status synonyms."""
    if not isinstance(value, str):
        return str(value) if value is not None else "", False
    original = value
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)

    if field_name in ("status", "payment_status"):
        if cleaned in STATUS_SYNONYMS:
            mapped = STATUS_SYNONYMS[cleaned]
            return mapped, (mapped != original)

    return cleaned, (cleaned != original)


# ══════════════════════════════════════════════════════════════
# RULE 9 (Bonus): Recalculate Order Total
# ══════════════════════════════════════════════════════════════
def rule_recalculate_total(items_json_str, delivery_cost_str, total_amount_str):
    """Recalculate total from items if components are valid."""
    try:
        items = json.loads(items_json_str) if isinstance(items_json_str, str) else items_json_str
        if not isinstance(items, list) or len(items) == 0:
            return total_amount_str, False

        items_total = 0
        for item in items:
            total_val = item.get("total")
            if total_val is None:
                return total_amount_str, False
            items_total += float(str(total_val))

        delivery = 0
        try:
            delivery = float(str(delivery_cost_str))
        except (ValueError, TypeError):
            pass

        calculated = items_total + delivery
        try:
            current = float(str(total_amount_str))
        except (ValueError, TypeError):
            return str(calculated), True

        if abs(calculated - current) > 0.01:
            return str(calculated), True

        return total_amount_str, False
    except (json.JSONDecodeError, TypeError, ValueError):
        return total_amount_str, False


# ══════════════════════════════════════════════════════════════
# APPLY ALL CLEANING RULES TO A RECORD
# ══════════════════════════════════════════════════════════════
# Map: field_name → list of (rule_function, rule_code)
FIELD_RULES = {
    "order_id":       [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_whitespace_status, "WHITESPACE")],
    "order_date":     [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_date, "DATE_FORMAT")],
    "status":         [(rule_whitespace_status, "STATUS_SYNONYM")],
    "customer_id":    [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_whitespace_status, "WHITESPACE")],
    "customer_phone": [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_phone, "PHONE_FORMAT")],
    "customer_email": [(rule_email, "EMAIL_REPEATED_SYMBOLS")],
    "delivery_cost":  [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_thousands_sep, "THOUSANDS_SEP"), (rule_word_price, "WORD_PRICE")],
    "payment_amount": [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_thousands_sep, "THOUSANDS_SEP"), (rule_word_price, "WORD_PRICE")],
    "payment_status": [(rule_whitespace_status, "STATUS_SYNONYM")],
    "currency":       [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_currency, "CURRENCY_NORMALIZE")],
    "total_amount":   [(rule_arabic_digits, "ARABIC_DIGITS"), (rule_thousands_sep, "THOUSANDS_SEP"), (rule_word_price, "WORD_PRICE")],
    "customer_name":  [(rule_whitespace_status, "WHITESPACE")],
    "city":           [(rule_whitespace_status, "WHITESPACE")],
    "district":       [(rule_whitespace_status, "WHITESPACE")],
    "delivery_type":  [(rule_whitespace_status, "WHITESPACE")],
    "payment_method": [(rule_whitespace_status, "WHITESPACE")],
}


def apply_cleaning_rules(record):
    """
    Apply all cleaning rules to a raw record.

    Returns:
        cleaned_record: dict with cleaned values
        corrections: list of {field, original_value, corrected_value, rule_code}
    """
    cleaned = dict(record)
    corrections = []

    for field, rules in FIELD_RULES.items():
        value = cleaned.get(field, "")
        if value is None:
            value = ""

        for rule_fn, rule_code in rules:
            if rule_fn == rule_whitespace_status:
                new_value, changed = rule_fn(value, field_name=field)
            else:
                new_value, changed = rule_fn(value)

            if changed:
                corrections.append({
                    "field": field,
                    "original_value": value,
                    "corrected_value": new_value,
                    "rule_code": rule_code,
                })
                value = new_value

        cleaned[field] = value

    # Rule 9: Recalculate total
    new_total, total_changed = rule_recalculate_total(
        cleaned.get("items_json", ""),
        cleaned.get("delivery_cost", ""),
        cleaned.get("total_amount", ""),
    )
    if total_changed:
        corrections.append({
            "field": "total_amount",
            "original_value": cleaned["total_amount"],
            "corrected_value": new_total,
            "rule_code": "TOTAL_RECALCULATED",
        })
        cleaned["total_amount"] = new_total

    return cleaned, corrections


# ══════════════════════════════════════════════════════════════
# QUARANTINE VALIDATION
# ══════════════════════════════════════════════════════════════
def check_quarantine(record):
    """
    Check if a record must be quarantined.

    Returns:
        error_codes: list of error code strings (empty = no quarantine needed)
        error_details: list of {code, message} dicts
    """
    error_codes = []
    error_details = []

    # 1. MISSING_ORDER_ID
    raw_order_id = record.get("order_id")
    order_id = str(raw_order_id).strip() if raw_order_id is not None else ""
    if not order_id or order_id.lower() == "none":
        error_codes.append("MISSING_ORDER_ID")
        error_details.append({
            "code": "MISSING_ORDER_ID",
            "message": "Order ID is missing and cannot be inferred",
        })

    # 2. MISSING_CUSTOMER_ID
    raw_customer_id = record.get("customer_id")
    customer_id = str(raw_customer_id).strip() if raw_customer_id is not None else ""
    if not customer_id or customer_id.lower() == "none":
        error_codes.append("MISSING_CUSTOMER_ID")
        error_details.append({
            "code": "MISSING_CUSTOMER_ID",
            "message": "Customer ID is missing",
        })

    # 3. INVALID_IMPOSSIBLE_DATE
    order_date = str(record.get("order_date", "")).strip()
    if order_date:
        try:
            dt = datetime.strptime(order_date, "%Y-%m-%dT%H:%M:%S")
            if dt.year < 2000 or dt.year > 2030:
                error_codes.append("INVALID_IMPOSSIBLE_DATE")
                error_details.append({
                    "code": "INVALID_IMPOSSIBLE_DATE",
                    "message": f"Date is out of valid range: {order_date}",
                })
        except ValueError:
            # If date couldn't be parsed even after cleaning
            try:
                datetime.strptime(order_date, "%Y-%m-%d")
            except ValueError:
                error_codes.append("INVALID_IMPOSSIBLE_DATE")
                error_details.append({
                    "code": "INVALID_IMPOSSIBLE_DATE",
                    "message": f"Date format is invalid/impossible: {order_date}",
                })

    # 4. CORRUPTED_ITEMS_JSON
    items_json = record.get("items_json", "")
    items = None
    if isinstance(items_json, str):
        items_json = items_json.strip()
        if items_json:
            try:
                items = json.loads(items_json)
                if not isinstance(items, list):
                    error_codes.append("CORRUPTED_ITEMS_JSON")
                    error_details.append({
                        "code": "CORRUPTED_ITEMS_JSON",
                        "message": "items_json is not a JSON array",
                    })
                    items = None
            except (json.JSONDecodeError, TypeError):
                error_codes.append("CORRUPTED_ITEMS_JSON")
                error_details.append({
                    "code": "CORRUPTED_ITEMS_JSON",
                    "message": f"items_json is corrupted or unparseable",
                })
        else:
            error_codes.append("EMPTY_ITEMS")
            error_details.append({
                "code": "EMPTY_ITEMS",
                "message": "items_json is empty",
            })
    elif items_json is None or items_json == "":
        error_codes.append("EMPTY_ITEMS")
        error_details.append({
            "code": "EMPTY_ITEMS",
            "message": "items_json is empty/null",
        })

    # 5. EMPTY_ITEMS (valid JSON but empty array)
    if items is not None and isinstance(items, list) and len(items) == 0:
        if "EMPTY_ITEMS" not in error_codes:
            error_codes.append("EMPTY_ITEMS")
            error_details.append({
                "code": "EMPTY_ITEMS",
                "message": "Order has no items",
            })

    # 6. UNKNOWN_PRICE
    total_amount = str(record.get("total_amount", "")).strip()
    if total_amount:
        try:
            val = float(total_amount)
        except (ValueError, TypeError):
            error_codes.append("UNKNOWN_PRICE")
            error_details.append({
                "code": "UNKNOWN_PRICE",
                "message": f"Total amount is not a valid number: {total_amount}",
            })

    # 7. AMBIGUOUS_NEGATIVE_VALUE
    if items is not None and isinstance(items, list):
        for item in items:
            qty = item.get("qty", 0)
            try:
                qty_val = float(str(qty))
                if qty_val < 0:
                    error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                    error_details.append({
                        "code": "AMBIGUOUS_NEGATIVE_VALUE",
                        "message": f"Negative quantity found: {qty_val} in item {item.get('sku', 'unknown')}",
                    })
                    break  # One is enough
            except (ValueError, TypeError):
                pass

        for item in items:
            price = item.get("unit_price", 0)
            try:
                price_val = float(str(price))
                if price_val < 0:
                    if "AMBIGUOUS_NEGATIVE_VALUE" not in error_codes:
                        error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                        error_details.append({
                            "code": "AMBIGUOUS_NEGATIVE_VALUE",
                            "message": f"Negative price found: {price_val}",
                        })
                    break
            except (ValueError, TypeError):
                pass

    # 9. MULTIPLE_CONFLICTING_ERRORS
    if len(error_codes) >= 3:
        if "MULTIPLE_CONFLICTING_ERRORS" not in error_codes:
            error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
            error_details.append({
                "code": "MULTIPLE_CONFLICTING_ERRORS",
                "message": f"Multiple conflicting errors ({len(error_codes) - 1}) prevent safe correction",
            })

    return error_codes, error_details
