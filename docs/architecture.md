# Architecture Documentation - Midterm Data Pipeline

## نظرة عامة

خط بيانات هجين يستقبل ملف CSV ضخم يحتوي على بيانات طلبات متجر إلكتروني، ويعالجها عبر نمط ELT:
1. **Extract**: قراءة الملف
2. **Load**: تحميل البيانات الخام كما هي إلى MongoDB (بدون تنظيف)
3. **Transform**: تطبيق قواعد التنظيف والتصنيف

## المكونات

### 1. File Router (`file_router.py`)
- يفحص حجم الملف ويقارنه بالحد الفاصل (`SMALL_FILE_THRESHOLD_MB`)
- الملفات ≤ 200 MB → Python Batch
- الملفات > 200 MB → PySpark

### 2. Python Batch Loader (`batch_loader.py`)
- يقرأ CSV بشكل Streaming عبر `csv.reader`
- لا يحمّل الملف كاملاً بالذاكرة
- يجمّع السجلات في دُفعات (batches) قابلة للضبط
- يستخدم `insert_many` للإدخال السريع

### 3. PySpark Loader (`spark_loader.py`)
- يستخدم `SparkSession` مع Schema ثابتة (جميع الحقول String)
- يكتب إلى MongoDB عبر MongoDB Spark Connector
- لا يستخدم `inferSchema` للحفاظ على القيم الخام

### 4. Raw Layer (`orders_raw`)
- تخزّن جميع السجلات كما وصلت بدون أي تعديل
- تحتوي على metadata: `run_id`, `source_file`, `source_row_number`, `ingested_at`, `engine_used`
- طبقة تاريخية لا تُحذف

### 5. Quality Rules (`quality_rules.py`)
9 قواعد تنظيف:
1. تحويل الأرقام العربية (٠-٩) إلى لاتينية
2. توحيد العملة (ريال → YER)
3. إزالة فواصل الآلاف
4. تحويل الأسعار المكتوبة بالكلمات
5. تنسيق أرقام الهواتف
6. إصلاح البريد الإلكتروني
7. توحيد صيغ التواريخ
8. توحيد حالات الطلب والمترادفات
9. إعادة حساب إجمالي الطلب

### 6. ELT Pipeline (`elt_pipeline.py`)
- يقرأ من `orders_raw` ويطبق قواعد التنظيف
- يصنّف كل سجل: Valid / Corrected / Quarantine
- يكتب إلى `orders_validated` عبر Upsert (Idempotent)
- يكتب إلى `orders_quarantine` مع أكواد الأخطاء
- يحفظ Audit Trail لكل تصحيح

### 7. MongoDB Collections
| Collection | الغرض | المفتاح |
|------------|-------|---------|
| `orders_raw` | البيانات الخام | `run_id` (Index) |
| `orders_validated` | السجلات المعتمدة | `order_id` (Unique Index) |
| `orders_quarantine` | السجلات المعزولة | `run_id` (Index) |

### 8. Idempotency
- `orders_validated` تستخدم Upsert (`replace_one` with `upsert=True`)
- المفتاح: `order_id` (Stable Business Key)
- إعادة التشغيل لا تنشئ سجلات مكررة
- تسجّل: `inserted_count`, `updated_count`, `unchanged_count`

## مخطط التدفق

```
              ┌─────────────────┐
              │   CSV File      │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │  File Router    │
              │  (size check)   │
              └───┬─────────┬───┘
                  │         │
         ≤200MB   │         │  >200MB
                  │         │
          ┌───────▼──┐  ┌───▼──────────┐
          │  Python  │  │   PySpark    │
          │  Batch   │  │   Loader     │
          └───────┬──┘  └───┬──────────┘
                  │         │
              ┌───▼─────────▼───┐
              │   orders_raw    │
              │   (MongoDB)     │
              └───────┬─────────┘
                      │
              ┌───────▼─────────┐
              │  ELT Pipeline   │
              │  (Clean+Class)  │
              └──┬──────────┬───┘
                 │          │
         ┌───────▼──┐  ┌───▼────────────┐
         │validated │  │  quarantine    │
         │(Upsert)  │  │  (with codes)  │
         └───────┬──┘  └───┬────────────┘
                 │         │
              ┌──▼─────────▼──┐
              │   Metrics     │
              │ results.json  │
              └───────────────┘
```

## أكواد العزل (Quarantine Error Codes)

| الكود | السبب |
|-------|-------|
| `MISSING_ORDER_ID` | معرّف الطلب مفقود |
| `MISSING_CUSTOMER_ID` | معرّف العميل مفقود |
| `INVALID_IMPOSSIBLE_DATE` | تاريخ غير منطقي |
| `CORRUPTED_ITEMS_JSON` | JSON تالف |
| `EMPTY_ITEMS` | لا توجد عناصر |
| `UNKNOWN_PRICE` | سعر غير قابل للتحليل |
| `AMBIGUOUS_NEGATIVE_VALUE` | قيمة سالبة غامضة |
| `DUPLICATE_ORDER_ID` | تكرار معرّف الطلب |
| `MULTIPLE_CONFLICTING_ERRORS` | أخطاء متعددة متعارضة |
