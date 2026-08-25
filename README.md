# Midterm Data Pipeline - Hybrid ELT System

##

بناء خط بيانات هجين لمعالجة بيانات طلبات متجر إلكتروني باستخدام Python Batch + Apache Spark + MongoDB.

---

## متطلبات التشغيل

| المتطلب | الإصدار |
|---------|---------|
| Python | 3.10+ |
| Java JDK | 11+ |
| MongoDB | 6.0+ (يعمل كخدمة) |
| PySpark | 3.5+ |

## التثبيت

```bash
# 1. استنساخ المستودع
git clone <repo-url>
cd midterm-data-pipeline

# 2. تثبيت المكتبات
pip install -r requirements.txt

# 3. التأكد من تشغيل MongoDB
# MongoDB يجب أن يعمل على localhost:27017

# 4. وضع ملف البيانات
# ضع ملف orders_huge_mixed_quality.csv في مجلد data/
```

## تشغيل المشروع

### الأمر الرئيسي (نقطة تشغيل واحدة)
```bash
# تشغيل على العيّنة الصغيرة (Python Batch)
py src/main.py data/orders_small_sample.csv --reset
# تشغيل على الملف الكبير (PySpark)
py src/main.py data/orders_huge_mixed_quality.csv --reset
```

### إنشاء عيّنة صغيرة
```bash
py src/main.py data/orders_huge_mixed_quality.csv --reset```

### تشغيل الاختبارات
```bash
pytest tests/ -v
```

### الأوامر المتقدمة
```bash
# تشغيل بدون reset (اختبار Idempotency - التشغيل الثاني)
python src/main.py data/orders_small_sample.csv

# تغيير الإعدادات عبر متغيرات البيئة
set SMALL_FILE_THRESHOLD_MB=100
set BATCH_SIZE=10000
python src/main.py data/orders_small_sample.csv --reset
```

## بنية المشروع

```
midterm-data-pipeline/
├── README.md                    # هذا الملف
├── requirements.txt             # المكتبات المطلوبة
├── config/
│   └── settings.py              # جميع الإعدادات (قابلة للتغيير)
├── data/
│   └── .gitkeep                 # ملفات البيانات (لا تُرفع لـ Git)
├── src/
│   ├── main.py                  # نقطة التشغيل الرئيسية
│   ├── file_router.py           # الموجّه التلقائي (حجم الملف → المحرّك)
│   ├── create_small_sample.py   # إنشاء عيّنة صغيرة
│   ├── batch_loader.py          # Python Batch Loader (ملفات صغيرة)
│   ├── spark_loader.py          # PySpark Loader (ملفات كبيرة)
│   ├── quality_rules.py         # 9 قواعد تنظيف + فحص العزل
│   ├── elt_pipeline.py          # خط ELT (تصنيف + Upsert + Audit Trail)
│   ├── mongo_setup.py           # إعداد MongoDB والفهارس
│   └── metrics.py               # جمع وحفظ القياسات
├── tests/
│   ├── test_cleaning_rules.py   # اختبارات قواعد التنظيف
│   └── test_classification.py   # اختبارات التصنيف
├── reports/
│   ├── results.json             # القياسات (يُنشأ تلقائياً)
│   └── screenshots/             # لقطات الشاشة
└── docs/
    └── architecture.md          # وصف المعمارية
```

## المعمارية

```
CSV File → File Router → [Python Batch | PySpark] → orders_raw
                                                        ↓
                                              Cleaning + Validation
                                                   ↓           ↓
                                          Idempotent Upsert   Quarantine
                                                   ↓           ↓
                                          orders_validated  orders_quarantine
                                                        ↓
                                              reports/results.json
```

## القياسات

كل تشغيل يحفظ في `reports/results.json`:
- `run_id` - معرّف التشغيل
- `file_name`, `file_size_mb` - معلومات الملف
- `engine_used` - المحرّك المستخدم
- `rows_read`, `raw_loaded` - عدد السجلات
- `valid_count`, `corrected_count`, `quarantine_count` - التصنيف
- `inserted_count`, `updated_count`, `unchanged_count` - نتائج Upsert
- `elapsed_seconds`, `throughput` - الأداء
- `error_case_counts` - تفصيل أنواع الأخطاء

## معادلة الاتساق

```
raw_loaded = valid_count + corrected_count + quarantine_count
```

## Idempotency

- التشغيل الأول: جميع السجلات → inserted
- التشغيل الثاني (نفس البيانات): لا duplicate، سجلات → updated أو unchanged
- يُثبت بمقارنة `inserted_count` و `updated_count` بين التشغيلين
