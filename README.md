🚀 Hybrid Big Data Pipeline & Analytics Engine
Phase 1 + Phase 2
جامعة الرازي -- كلية الحاسوب وتقنية المعلومات
التخصص: الذكاء الاصطناعي -- المستوى الرابع
اسم الطالب: محمد يوسف حمود سعيد الربيعي
📌 1. نبذة عن المشروع
هذا المشروع عبارة عن نظام Hybrid ELT Data Pipeline لمعالجة وتحليل بيانات الطلبات المخزنة في ملفات CSV.
يعتمد النظام على اختيار طريقة المعالجة المناسبة حسب حجم الملف:
Python Batch Processing للملفات الصغيرة.
Apache Spark / PySpark للملفات الكبيرة.
MongoDB لتخزين البيانات ومعالجتها وتحليلها.
FastAPI لتوفير واجهة REST API للوصول إلى وظائف النظام.
يمر المشروع بمراحل تبدأ من قراءة ملف CSV، ثم تحديد محرك المعالجة، وتحميل البيانات الخام، وتنظيفها والتحقق منها، وتصنيف السجلات، ثم تخزين النتائج وإنشاء التقارير والتحليلات.
📚 2. محتويات المشروع
فكرة النظام
المعمارية العامة
Phase 1 - Data Pipeline
File Router
Batch Processing
Spark Processing
Raw Data Layer
Data Cleaning & Validation
Classification & Quarantine
Idempotency
Metrics
Phase 2 - Analytics
Queries & Indexes
Aggregation Reports
Materialized Views
Scheduled Jobs
FastAPI
Project Structure
Installation
Running
Testing
MongoDB Collections
Technologies
GitHub
💡 3. فكرة النظام
المشكلة التي يعالجها المشروع هي التعامل مع ملفات بيانات تختلف في:
الحجم.
جودة البيانات.
صحة القيم.
إمكانية إعادة تشغيل الـ Pipeline.
لذلك تم بناء النظام ليختار محرك المعالجة تلقائياً.
التدفق الأساسي
CSV File
   │
   ▼
File Router
   │
   ├──────────────► Small File ──────────► Python Batch
   │
   └──────────────► Large File ──────────► PySpark
                                             │
                                             ▼
                                         MongoDB
                                             │
                                             ▼
                                      orders_raw
                                             │
                                             ▼
                                  Cleaning & Validation
                                             │
                              ┌──────────────┼──────────────┐
                              ▼              ▼              ▼
                            Valid         Corrected      Quarantine
                              │              │              │
                              └──────┬───────┘              │
                                     ▼                      ▼
                              orders_validated       orders_quarantine
                                     │
                                     ▼
                                Analytics
                                     │
                         ┌───────────┼───────────┐
                         ▼           ▼           ▼
                      Reports     Views       FastAPI
🏗️ 4. المعمارية العامة
يعتمد المشروع على طبقات واضحة:
Layer 1 --- Input
ملفات CSV التي تحتوي على بيانات الطلبات.
Layer 2 --- Router
يحدد محرك المعالجة المناسب بناءً على حجم الملف.
Layer 3 --- Processing
Python Batch للملفات الصغيرة.
PySpark للملفات الكبيرة.
Layer 4 --- Raw Data
تحميل البيانات الأصلية إلى:
orders_raw
Layer 5 --- Transformation
تنظيف البيانات والتحقق من صحتها.
Layer 6 --- Classification
تصنيف السجلات إلى:
VALID
CORRECTED
QUARANTINE
Layer 7 --- Final Data
تخزين البيانات المقبولة في:
orders_validated
والبيانات غير المقبولة في:
orders_quarantine
Layer 8 --- Analytics
تشمل:
Queries
Indexes
Aggregations
Materialized Views
Scheduled Jobs
FastAPI
🔵 5. Phase 1 - Data Pipeline
Phase 1 هي المرحلة الأساسية للمشروع، وتركز على بناء Pipeline قادر على قراءة البيانات وتحميلها وتنظيفها والتحقق منها.
مراحل Phase 1
Extract
   ↓
Route
   ↓
Load
   ↓
Clean
   ↓
Validate
   ↓
Classify
   ↓
Upsert
   ↓
Metrics
📂 6. File Router
يستخدم النظام حدّاً قدره:
200 MB
لتحديد طريقة المعالجة.
القاعدة
File < 200 MB
      ↓
Python Batch

File >= 200 MB
      ↓
PySpark
ميزة هذا التصميم أن المستخدم لا يحتاج إلى اختيار المحرك يدوياً.
🐍 7. Python Batch Processing
يتم استخدام Python لمعالجة الملفات الصغيرة.
بدلاً من تحميل الملف بالكامل في الذاكرة، تتم قراءة البيانات على شكل Batches.
CSV
 ↓
Read Batch
 ↓
Process
 ↓
MongoDB
 ↓
Next Batch
وهذا يساعد على تقليل استهلاك الذاكرة أثناء المعالجة.
⚡ 8. PySpark Processing
عند التعامل مع الملفات الكبيرة يستخدم النظام:
Apache Spark / PySpark
يتم استخدام Spark لقراءة ومعالجة البيانات الكبيرة، ثم تحميلها إلى MongoDB.
تم إعداد بيئة Spark المطلوبة للتشغيل على Windows، بما في ذلك إعدادات Hadoop المطلوبة.
🗃️ 9. Raw Data Layer
بعد قراءة البيانات يتم تحميلها أولاً إلى:
orders_raw
قبل تنفيذ عمليات التنظيف.
هذه الخطوة مهمة لأن النظام يحتفظ بنسخة Raw من البيانات، مما يسمح بتتبع مصدر السجلات.
يتم الاحتفاظ بمعلومات مثل:
run_id
source_file
source_row_number
ingested_at
engine_used
raw_record
وبذلك يمكن معرفة مصدر السجل والـ Run الذي قام بإدخاله.
🧹 10. Data Cleaning & Validation
بعد تحميل البيانات الخام تبدأ مرحلة التنظيف والتحقق.
يتعامل المشروع مع مجموعة من أخطاء جودة البيانات.
قواعد التنظيف
القاعدة                      الوظيفة
Arabic Digits                تحويل الأرقام العربية Currency Normalization       توحيد تمثيل العملة Thousand Separators          معالجة فواصل الأسعار Word Prices                  معالجة الأسعار النصية Phone Formatting             توحيد أرقام الهواتف Email Repair                 معالجة أخطاء البريد الإلكتروني Date Normalization           توحيد صيغة التاريخ Order Status Normalization   توحيد حالة الطلب Total Recomputation          إعادة حساب إجمالي الطلب
🚦 11. Classification & Quarantine
بعد التنظيف والتحقق يتم تصنيف كل سجل.
                    Record
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
           Valid               Invalid
             │                   │
             ▼          ┌────────┴────────┐
           VALID         ▼                 ▼
                     Correctable      Not Correctable
                         │                 │
                         ▼                 ▼
                     CORRECTED         QUARANTINE
✅ VALID
السجل صحيح ولا يحتاج إلى تعديل.
🔧 CORRECTED
السجل يحتوي على مشكلة يمكن إصلاحها.
يتم الاحتفاظ بمعلومات التعديل كـ Audit Trail.
❌ QUARANTINE
السجل غير صالح أو يحتوي على مشكلة لا يمكن تصحيحها بشكل آمن.
يتم عزله في:
orders_quarantine
⚠️ Error Codes
من أمثلة حالات الأخطاء التي يتعامل معها النظام:
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
EMPTY_ITEMS
CORRUPTED_ITEMS_JSON
INVALID_IMPOSSIBLE_DATE
UNKNOWN_PRICE
DUPLICATE_ORDER_ID
AMBIGUOUS_NEGATIVE_VALUE
MULTIPLE_CONFLICTING_ERRORS
🔄 12. Idempotent Upsert
من المتطلبات الأساسية أن إعادة تشغيل نفس البيانات لا تؤدي إلى إنشاء سجلات مكررة.
لذلك يعتمد النظام على:
order_id
في عملية Upsert للبيانات النهائية.
التشغيل الأول
New Order
   ↓
INSERT
التشغيل مرة أخرى
Same Order
   ↓
UPDATE / UNCHANGED
وبذلك يمكن إعادة تشغيل الـ Pipeline دون إنشاء Duplicates.
📊 13. Metrics & Consistency
يتم تسجيل معلومات التشغيل في:
reports/results.json
ومن أهم الـ Metrics:
run_id
file_name
file_size_mb
engine_used
rows_read
raw_loaded
valid_count
corrected_count
quarantine_count
inserted_count
updated_count
unchanged_count
elapsed_seconds
throughput
error_case_counts
Consistency Check
يتم التحقق من:
raw_loaded =
valid_count +
corrected_count +
quarantine_count
هذه المعادلة تساعد على التأكد من أن جميع السجلات التي وصلت إلى Raw Layer تم تصنيفها في نهاية الـ Pipeline.
🟣 14. Phase 2 - Analytics Layer
Phase 2 تضيف طبقة التحليل والوصول إلى البيانات فوق نتائج Phase 1.
تشمل:
Queries
Indexes
Explain
Aggregations
Materialized Views
Scheduled Jobs
FastAPI
🔎 15. Queries & Indexes
تمت إضافة 5 Queries للتعامل مع بيانات الطلبات وتحليلها.
كما تم إنشاء 3 Indexes، من بينها Compound Index.
الهدف من الـ Indexes هو تحسين عمليات البحث والوصول إلى البيانات.
Explain
يتم استخدام MongoDB Explain لمقارنة تنفيذ الاستعلام قبل وبعد استخدام الـ Index.
Query
 │
 ├── Before Index
 │      ↓
 │   Collection Scan
 │
 └── After Index
        ↓
     Index Scan
وهذا يسمح بقياس تأثير الفهرسة على الاستعلامات.
📈 16. Aggregation Reports
تم إنشاء 5 تقارير تحليلية باستخدام MongoDB Aggregation.
1. Sales By City
تحليل المبيعات حسب المدينة.
2. Top Products
استخراج المنتجات الأعلى مبيعاً.
3. Top Customers
تحديد العملاء الأعلى من حيث قيمة المشتريات.
4. Sales By Period
تحليل المبيعات حسب الفترة الزمنية.
5. Order Status Distribution
تحليل توزيع الطلبات حسب حالة الطلب.
جميع التقارير تعتمد على البيانات الموجودة في MongoDB، وليست على بيانات ثابتة داخل الكود.
🧱 17. Materialized Views
تمت إضافة Materialized Views لتخزين نتائج تحليلية جاهزة.
بدلاً من تنفيذ Aggregation في كل طلب، يمكن قراءة النتائج المخزنة مسبقاً.
Views الموجودة
daily_sales_summary
top_products_summary
📅 daily_sales_summary
تخزن ملخص المبيعات اليومية، مع التجميع حسب اليوم والمدينة.
🏆 top_products_summary
تخزن ملخص المنتجات الأعلى مبيعاً.
🔄 Incremental Refresh
يتم تحديث الـ Materialized Views بطريقة Incremental بدلاً من الاعتماد على إعادة بناء النتائج بشكل غير ضروري.
يستخدم النظام Metadata مرتبطة بعملية التحديث:
mv_metadata
⏰ 18. Scheduled Jobs
تم إنشاء Scheduler لتنفيذ المهام بشكل دوري.
Job 1 --- Refresh Views
تحديث Materialized Views.
Job 2 --- Daily Report
تشغيل التقرير الدوري وتسجيل نتيجة التنفيذ.
يتم تسجيل معلومات مثل:
Start
End
Status
Duration
وهذا يساعد على متابعة نجاح وفشل المهام ووقت تنفيذها.
🚀 19. FastAPI
تم إنشاء API موحدة باستخدام FastAPI للوصول إلى وظائف المشروع.
تشمل الواجهة وظائف مرتبطة بـ:
Health Check
Ingestion
Indexes
Queries
Aggregations
Materialized Views
Jobs
Swagger
بعد تشغيل API يمكن الوصول إلى:
http://localhost:8000/docs
ومن Swagger يمكن تجربة الـ API والاطلاع على الـ endpoints.
🧩 20. Project Structure
midterm-data-pipeline/
│
├── config/
│   └── settings.py
│
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── quality_rules.py
│   ├── elt_pipeline.py
│   ├── mongo_setup.py
│   ├── metrics.py
│   │
│   ├── queries.py
│   ├── aggregations.py
│   ├── materialized_views.py
│   ├── scheduler.py
│   └── api.py
│
├── tests/
│   ├── test_cleaning_rules.py
│   └── test_classification.py
│
├── data/
│
├── reports/
│   └── results.json
│
├── docs/
│   └── architecture.md
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
⚙️ 21. Installation
المتطلبات
Python 3.10+
Java JDK 11+
MongoDB 6.0+
PySpark 3.5+
تثبيت المكتبات
على Windows:
py -m pip install -r requirements.txt
في بيئة Windows المستخدمة للمشروع يتم استخدام py بدلاً من python.
Environment Variables
أنشئ ملف:
.env
بالاعتماد على:
.env.example
ولا تضع أي بيانات سرية داخل GitHub.
▶️ 22. Running the Project
تشغيل Phase 1
py src/main.py
تشغيل Queries
py src/queries.py
تشغيل Aggregations
py src/aggregations.py
تشغيل Materialized Views
py src/materialized_views.py
تشغيل Scheduler
py src/scheduler.py
تشغيل FastAPI
py src/api.py
بعد ذلك افتح:
http://localhost:8000/docs
🧪 23. Testing
لتشغيل الاختبارات:
py -m pytest tests/ -v
الاختبارات الموجودة تغطي وظائف مرتبطة بقواعد التنظيف والتصنيف في Phase 1.
🗄️ 24. MongoDB Collections
Phase 1
orders_raw
orders_validated
orders_quarantine
Phase 2
يتم استخدام Collections مرتبطة بالـ Materialized Views والـ Metadata والـ Job Logs وفق تنفيذ الطبقة التحليلية.
🛠️ 25. Technologies
Technology     الاستخدام
Python         Batch Processing & Pipeline Apache Spark   معالجة البيانات الكبيرة PySpark        تنفيذ Spark MongoDB        التخزين والتحليل FastAPI        REST API Pytest         Testing Java           تشغيل Spark Hadoop         دعم Spark على Windows
🔁 26. End-to-End Flow
                  CSV File
                     │
                     ▼
               File Router
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   Python Batch             PySpark
          │                     │
          └──────────┬──────────┘
                     ▼
                orders_raw
                     │
                     ▼
          Cleaning & Validation
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
        VALID     CORRECTED  QUARANTINE
          │          │          │
          └────┬─────┘          │
               ▼                ▼
       orders_validated   orders_quarantine
               │
               ▼
            Analytics
               │
      ┌────────┼────────┐
      ▼        ▼        ▼
   Queries  Reports   Views
                         │
                         ▼
                     Scheduler
                         │
                         ▼
                      FastAPI
🎯 27. ملخص المتطلبات المنفذة
Phase 1
Hybrid Data Processing
Python Batch
PySpark
File Router
200 MB Threshold
Raw Data Layer
Data Cleaning
Validation
Corrected Records
Quarantine
Error Codes
Audit Trail
Idempotent Upsert
Metrics
Consistency Check
Phase 2
5 Queries
3 Indexes
Compound Index
Explain
5 Aggregation Reports
2 Materialized Views
Incremental Refresh
2 Scheduled Jobs
FastAPI
Swagger Documentation
🎓 الخلاصة
المشروع يقدم Pipeline متكامل لمعالجة بيانات الطلبات يبدأ من ملفات CSV وينتهي بطبقة تحليلية وواجهة API.
الـ Pipeline يجمع بين:
Big Data
+
ELT
+
Data Quality
+
MongoDB
+
Apache Spark
+
Analytics
+
REST API
والتدفق النهائي للنظام هو:
Read
 ↓
Route
 ↓
Load
 ↓
Clean
 ↓
Validate
 ↓
Classify
 ↓
Store
 ↓
Analyze
 ↓
Summarize
 ↓
Expose through API
🔗 GitHub
Repository:
https://github.com/mohammedalrubai/midterm-data-pipeline
👨‍💻 Student
محمد يوسف حمود سعيد الربيعي
جامعة الرازي -- كلية الحاسوب وتقنية المعلومات
الذكاء الاصطناعي -- المستوى الرابع