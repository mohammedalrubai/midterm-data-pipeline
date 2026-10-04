# Midterm Data Pipeline - Hybrid ELT System

## المشروع النصفي + النهائي — مقرر البيانات الضخمة | جامعة الرازي

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

# 3. نسخ إعدادات البيئة
copy .env.example .env

# 4. التأكد من تشغيل MongoDB
# MongoDB يجب أن يعمل على localhost:27017

# 5. وضع ملف البيانات في مجلد data/
```

---

## Phase 1: خط البيانات (ELT Pipeline)

### التشغيل الرئيسي
```bash
# تشغيل على العيّنة الصغيرة (Python Batch)
python src/main.py data/orders_small_sample.csv --reset

# تشغيل على الملف الكبير (PySpark)
python src/main.py data/orders_huge_mixed_quality.csv --reset

# اختبار Idempotency (التشغيل الثاني بدون reset)
python src/main.py data/orders_small_sample.csv
```

### تشغيل الاختبارات
```bash
python -m pytest tests/ -v
```

---

## Phase 2: الإضافات الجديدة

### 1. الاستعلامات والفهارس
```bash
python src/queries.py
```
- **5 استعلامات**: orders_in_city, orders_by_status, customer_total_spending, quarantine_by_error_code, city_status_compound
- **3 فهارس**: idx_city, idx_status, idx_city_status_compound (مركّب)
- **Explain**: مقارنة الأداء قبل وبعد إنشاء الفهارس

### 2. التجميعات (5 تقارير)
```bash
python src/aggregations.py
```
- sales_by_city — المبيعات حسب المدينة
- top_products — أفضل المنتجات
- top_customers — أفضل العملاء
- sales_by_period — المبيعات حسب الفترة
- order_status_distribution — توزيع الطلبات حسب الحالة

### 3. العروض المادية (Materialized Views)
```bash
python src/materialized_views.py
```
- **daily_sales_summary** — ملخص المبيعات اليومية حسب التاريخ والمدينة
- **top_products_summary** — ملخص أفضل المنتجات
- يدعم **التحديث التزايدي** (Incremental) — لا يعيد بناء كل البيانات

### 4. المهام المجدولة (Scheduled Jobs)
```bash
python src/scheduler.py
```
- **refresh_views** — تحديث Materialized Views
- **daily_report** — تقرير يومي شامل
- كل مهمة تسجّل: وقت البداية والنهاية، حالة النجاح/الفشل، النتيجة

### 5. واجهة API الموحدة (FastAPI)
```bash
python src/api.py
```
ثم افتح **Swagger UI**: http://localhost:8000/docs

| Method | Endpoint | الوظيفة |
|--------|----------|---------|
| GET | `/health` | فحص حالة الخدمة |
| POST | `/ingest` | تشغيل Pipeline |
| POST | `/indexes` | إنشاء الفهارس + Explain |
| GET | `/queries` | قائمة الاستعلامات |
| GET | `/queries/{name}` | تشغيل استعلام |
| GET | `/aggregations` | قائمة التقارير |
| GET | `/aggregations/{name}` | تشغيل تقرير |
| POST | `/refresh-mv` | تحديث Materialized Views |
| GET | `/jobs` | قائمة المهام المجدولة |
| POST | `/jobs/{name}/run` | تشغيل مهمة يدوياً |

---

## بنية المشروع

```
midterm-data-pipeline/
├── README.md                        # هذا الملف
├── requirements.txt                 # المكتبات المطلوبة
├── .env.example                     # نموذج إعدادات البيئة
├── config/
│   └── settings.py                  # جميع الإعدادات
├── data/
│   └── .gitkeep                     # ملفات البيانات (لا تُرفع)
├── src/
│   ├── main.py                      # نقطة التشغيل الرئيسية (Phase 1)
│   ├── file_router.py               # الموجّه التلقائي
│   ├── create_small_sample.py       # إنشاء عيّنة صغيرة
│   ├── batch_loader.py              # Python Batch Loader
│   ├── spark_loader.py              # PySpark Loader
│   ├── quality_rules.py             # 9 قواعد تنظيف
│   ├── elt_pipeline.py              # خط ELT + Upsert
│   ├── mongo_setup.py               # إعداد MongoDB
│   ├── metrics.py                   # جمع وحفظ القياسات
│   ├── queries.py                   # استعلامات + فهارس (Phase 2)
│   ├── aggregations.py              # 5 تقارير (Phase 2)
│   ├── materialized_views.py        # عروض مادية (Phase 2)
│   ├── scheduler.py                 # مهام مجدولة (Phase 2)
│   └── api.py                       # FastAPI (Phase 2)
├── tests/
│   ├── test_cleaning_rules.py       # اختبارات التنظيف
│   └── test_classification.py       # اختبارات التصنيف
├── reports/
│   └── results.json                 # القياسات
└── docs/
    └── architecture.md              # وصف المعمارية
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
                                                        ↓
                             ┌───────────────────────────┼───────────────────────┐
                             ↓                           ↓                       ↓
                    Queries + Indexes          Aggregation Reports      Materialized Views
                    (5 queries, 3 idx)         (5 reports)             (daily_sales, top_products)
                             ↓                           ↓                       ↓
                             └───────────────────────────┼───────────────────────┘
                                                        ↓
                                              Scheduled Jobs + FastAPI
                                              (http://localhost:8000/docs)
```

## MongoDB Collections

| المجموعة | الغرض | Phase |
|----------|-------|-------|
| orders_raw | البيانات الخام | 1 |
| orders_validated | السجلات النظيفة | 1 |
| orders_quarantine | السجلات المعزولة | 1 |
| daily_sales_summary | ملخص المبيعات اليومية | 2 |
| top_products_summary | ملخص أفضل المنتجات | 2 |
| mv_metadata | بيانات تتبع التحديث التزايدي | 2 |
| job_logs | سجلات تنفيذ المهام | 2 |

## معادلة الاتساق

```
raw_loaded = valid_count + corrected_count + quarantine_count
```

## Idempotency

- التشغيل الأول: جميع السجلات → inserted
- التشغيل الثاني (نفس البيانات): لا duplicate، سجلات → updated أو unchanged
