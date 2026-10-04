🚀 Hybrid Big Data Pipeline & Analytics Engine
Phase 1 + Phase 2
جامعة الرازي -- كلية الحاسوب وتقنية المعلومات
التخصص: الذكاء الاصطناعي -- المستوى الرابع
اسم الطالب: محمد يوسف حمود سعيد الربيعي
📋 Project Overview
هذا المشروع عبارة عن Hybrid ELT System لمعالجة وتحليل بيانات الطلبات القادمة من ملفات CSV.
يعتمد النظام على اختيار محرك المعالجة المناسب حسب حجم الملف:
Python Batch للملفات الصغيرة.
Apache Spark / PySpark للملفات الكبيرة.
MongoDB لتخزين البيانات الخام والنهائية وتنفيذ عمليات التحليل.
FastAPI لتوفير واجهة موحدة للوصول إلى وظائف النظام.
يمر الـ Pipeline بمراحل واضحة:
CSV File
   ↓
File Router
   ↓
Python Batch / PySpark
   ↓
orders_raw
   ↓
Cleaning + Validation
   ↓
Valid / Corrected / Quarantine
   ↓
orders_validated / orders_quarantine
   ↓
Analytics + Reports
   ↓
Materialized Views
   ↓
FastAPI
📚 Table of Contents
Project Overview
Project Objectives
System Architecture
Phase 1 - Hybrid ELT Pipeline
File Router
Python Batch Processing
PySpark Processing
Raw Data Layer
Data Cleaning & Validation
Classification & Quarantine
Idempotency & Upsert
Metrics & Consistency
Phase 2 - Analytics Layer
Queries, Indexes & Explain
Aggregation Reports
Materialized Views
Scheduled Jobs
FastAPI
Project Structure
Installation & Configuration
Running the Project
Testing
MongoDB Collections
Technologies
End-to-End Flow
Conclusion
GitHub
🎯 Project Objectives
يهدف المشروع إلى بناء نظام عملي يستطيع:
معالجة ملفات CSV صغيرة وكبيرة.
اختيار محرك المعالجة تلقائياً.
تحميل البيانات الخام قبل التنظيف.
تطبيق قواعد Data Cleaning وValidation.
تصنيف البيانات إلى Valid وCorrected وQuarantine.
الاحتفاظ بمعلومات تتبع مصدر البيانات.
تنفيذ Idempotent Upsert.
إنتاج Metrics ونتائج تشغيل.
تنفيذ استعلامات وتحليلات على البيانات.
إنشاء Materialized Views.
تنفيذ مهام مجدولة.
توفير REST API من خلال FastAPI.
🏗️ System Architecture
                         ┌─────────────┐
                         │   CSV File  │
                         └──────┬──────┘
                                │
                                ▼
                         ┌─────────────┐
                         │ File Router │
                         └──────┬──────┘
                                │
                 ┌──────────────┴──────────────┐
                 │                             │
                 ▼                             ▼
          Small File                     Large File
                 │                             │
                 ▼                             ▼
          Python Batch                    PySpark
                 │                             │
                 └──────────────┬──────────────┘
                                ▼
                         ┌─────────────┐
                         │ orders_raw  │
                         └──────┬──────┘
                                │
                                ▼
                   ┌──────────────────────┐
                   │ Cleaning + Validation│
                   └──────────┬───────────┘
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
               VALID       CORRECTED   QUARANTINE
                 │            │            │
                 └──────┬─────┘            ▼
                        ▼            orders_quarantine
                orders_validated
                        │
                        ▼
                  Analytics Layer
                        │
        ┌───────────────┼────────────────┐
        ▼               ▼                ▼
     Queries       Aggregations     Materialized
                                       Views
                                          │
                                          ▼
                                      Scheduler
                                          │
                                          ▼
                                       FastAPI
🔵 Phase 1 - Hybrid ELT Pipeline
1. ELT Concept
المشروع يعتمد على مفهوم:
Extract → Load → Transform
أي أن البيانات يتم:
استخراجها من CSV.
تحميلها إلى MongoDB في Raw Layer.
تنفيذ عمليات التنظيف والتحويل بعد التحميل.
وهذا يحافظ على البيانات الأصلية قبل تطبيق التحويلات.
📂 File Router
يستخدم المشروع حدًا قدره:
200 MB
لاختيار محرك المعالجة.
Routing Rule
File Size < 200 MB
        ↓
Python Batch


File Size >= 200 MB
        ↓
PySpark
وبذلك يتم اختيار طريقة المعالجة تلقائياً.
🐍 Python Batch Processing
للملفات الصغيرة يستخدم المشروع Python Batch Processing.
لا يتم الاعتماد على تحميل الملف بالكامل في الذاكرة، وإنما تتم قراءة البيانات على دفعات.
CSV
 ↓
Read Batch
 ↓
Create Documents
 ↓
MongoDB
 ↓
Next Batch
هذا التصميم يساعد على التحكم في استهلاك الذاكرة أثناء معالجة الملفات.
⚡ PySpark Processing
للملفات الكبيرة يتم استخدام:
Apache Spark / PySpark
يتم قراءة البيانات الكبيرة ومعالجتها بواسطة Spark، ثم إرسال البيانات إلى MongoDB.
تم إعداد بيئة Spark على Windows مع إعدادات Hadoop المطلوبة لتشغيل Spark.
🗃️ Raw Data Layer
بعد قراءة الملف يتم تحميل البيانات أولاً إلى:
orders_raw
قبل تنفيذ عمليات التنظيف.
يتم الاحتفاظ بمعلومات تساعد على تتبع مصدر السجل، مثل:
run_id
source_file
source_row_number
ingested_at
engine_used
raw_record
وبذلك يمكن تتبع:
مصدر السجل.
رقم الصف الأصلي.
عملية التشغيل التي أدخلت السجل.
المحرك المستخدم في المعالجة.
🧹 Data Cleaning & Validation
بعد Raw Layer يتم تنفيذ عمليات التنظيف والتحقق.
يحتوي المشروع على مجموعة من قواعد Data Quality:
Rule                         الوظيفة
Arabic Digits                تحويل الأرقام العربية Currency Normalization       توحيد تمثيل العملات Thousand Separators          معالجة فواصل الأسعار Word Prices                  معالجة الأسعار المكتوبة كنص Phone Formatting             توحيد أرقام الهواتف Email Repair                 معالجة أخطاء البريد الإلكتروني Date Normalization           توحيد صيغ التاريخ Order Status Normalization   توحيد حالات الطلب Total Recomputation          إعادة حساب إجمالي الطلب
🚦 Classification & Quarantine
بعد تطبيق قواعد التنظيف والتحقق يتم تصنيف السجلات.
                       Record
                          │
                ┌─────────┴─────────┐
                │                   │
              Valid               Invalid
                │                   │
                ▼          ┌────────┴────────┐
              VALID         │                 │
                            ▼                 ▼
                       Correctable       Not Correctable
                            │                 │
                            ▼                 ▼
                        CORRECTED         QUARANTINE
✅ VALID
السجل صحيح ولا يحتاج إلى تعديل.
🔧 CORRECTED
السجل يحتوي على مشكلة يمكن إصلاحها.
ويتم الاحتفاظ بمعلومات التعديل كجزء من Audit Trail.
❌ QUARANTINE
السجل لا يمكن اعتماده أو تصحيحه بشكل آمن.
يتم عزله في:
orders_quarantine
⚠️ Error Cases
من الحالات التي يتعامل معها النظام:
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
EMPTY_ITEMS
CORRUPTED_ITEMS_JSON
INVALID_IMPOSSIBLE_DATE
UNKNOWN_PRICE
DUPLICATE_ORDER_ID
AMBIGUOUS_NEGATIVE_VALUE
MULTIPLE_CONFLICTING_ERRORS
🔄 Idempotency & Upsert
من أهم خصائص النظام إمكانية إعادة تشغيل نفس البيانات دون إنشاء Duplicates.
يعتمد الـ Upsert في البيانات النهائية على:
order_id
First Run
New Order
   ↓
INSERT
Second Run
Same Order
   ↓
UPDATE / UNCHANGED
وهذا يجعل الـ Pipeline Idempotent عند إعادة تشغيل نفس البيانات.
📊 Metrics & Consistency
يتم تسجيل نتائج التشغيل في:
reports/results.json
ومن أهم المقاييس:
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
Consistency Equation
raw_loaded =
valid_count +
corrected_count +
quarantine_count
تستخدم هذه المعادلة للتحقق من أن جميع السجلات التي تم تحميلها إلى Raw تم تصنيفها في نهاية الـ Pipeline.
🟣 Phase 2 - Analytics Layer
Phase 2 تضيف طبقة التحليل فوق بيانات Phase 1.
orders_validated
       │
       ├──────────────► Queries
       │
       ├──────────────► Indexes
       │
       ├──────────────► Aggregations
       │
       ├──────────────► Materialized Views
       │
       ├──────────────► Scheduled Jobs
       │
       └──────────────► FastAPI
🔎 Queries, Indexes & Explain
تمت إضافة:
5 Queries
3 Indexes
Compound Index
MongoDB Explain
الغرض من الـ Indexes هو تحسين الوصول إلى البيانات.
Explain
يتم استخدام Explain لمقارنة تنفيذ الاستعلام قبل وبعد إضافة الـ Index.
Query
 │
 ├── Before Index
 │      ↓
 │  Collection Scan
 │
 └── After Index
        ↓
     Index Scan
وبذلك يمكن تقييم تأثير الفهرسة على الاستعلامات.
📈 Aggregation Reports
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
جميع التقارير تعتمد على البيانات الموجودة في MongoDB، وليست على قيم ثابتة داخل الكود.
🧱 Materialized Views
تم إنشاء Materialized Views لتخزين نتائج تحليلية جاهزة.
الموجود في المشروع:
daily_sales_summary
top_products_summary
📅 daily_sales_summary
تخزن ملخص المبيعات اليومية مع التجميعات المطلوبة للتحليل.
🏆 top_products_summary
تخزن ملخص المنتجات الأعلى مبيعاً.
🔄 Incremental Refresh
يتم تحديث الـ Views بطريقة Incremental.
ويستخدم المشروع Metadata مرتبطة بعملية التحديث:
mv_metadata
⏰ Scheduled Jobs
تم إنشاء Scheduler لتنفيذ المهام بشكل دوري.
Job 1 --- Refresh Views
تحديث Materialized Views.
Job 2 --- Daily Report
تشغيل التقرير الدوري وتسجيل نتيجة التنفيذ.
يتم تسجيل معلومات التشغيل مثل:
Start
End
Status
Duration
🚀 FastAPI
تم إنشاء REST API موحدة للوصول إلى وظائف المشروع.
تشمل الوظائف:
Health
Ingest
Indexes
Queries
Aggregations
Materialized Views
Jobs
Swagger
بعد تشغيل API:
http://localhost:8000/docs
يمكن استخدام Swagger لتجربة الـ endpoints ومراجعة توثيقها.
🧩 Project Structure
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
⚙️ Installation & Configuration
Requirements
Python 3.10+
Java JDK 11+
MongoDB 6.0+
PySpark 3.5+
Install Dependencies
على Windows PowerShell:
py -m pip install -r requirements.txt
في بيئة Windows المستخدمة للمشروع يتم استخدام py بدلاً من python.
Environment Variables
استخدم:
.env.example
كمرجع لإنشاء:
.env
ولا تضع البيانات السرية داخل GitHub.
▶️ Running the Project
Phase 1
py src/main.py
Queries
py src/queries.py
Aggregations
py src/aggregations.py
Materialized Views
py src/materialized_views.py
Scheduler
py src/scheduler.py
FastAPI
py src/api.py
ثم:
http://localhost:8000/docs
🧪 Testing
لتشغيل الاختبارات:
py -m pytest tests/ -v
وتستخدم الاختبارات للتحقق من وظائف التنظيف والتصنيف الأساسية في المشروع.
🗄️ MongoDB Collections
Phase 1
orders_raw
orders_validated
orders_quarantine
Phase 2
توجد Collections مرتبطة بالـ Materialized Views والـ Metadata والـ Job Logs وفق تنفيذ الطبقة التحليلية.
🛠️ Technologies
Technology     Usage
Python         Batch Processing & Pipeline Apache Spark   Large File Processing PySpark        Spark Processing MongoDB        Storage & Analytics PyMongo        MongoDB Access FastAPI        REST API Pytest         Testing Java           Spark Runtime Hadoop         Spark Support on Windows Git / GitHub   Version Control
🔁 End-to-End Flow
                         CSV
                          │
                          ▼
                    File Router
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
       Python Batch               PySpark
              │                       │
              └───────────┬───────────┘
                          ▼
                     orders_raw
                          │
                          ▼
                Cleaning + Validation
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
           VALID       CORRECTED   QUARANTINE
             │            │            │
             └─────┬──────┘            │
                   ▼                   ▼
            orders_validated    orders_quarantine
                   │
                   ▼
                Analytics
                   │
        ┌──────────┼───────────┐
        ▼          ▼           ▼
     Queries   Aggregations   Views
                                │
                                ▼
                            Scheduler
                                │
                                ▼
                             FastAPI
📦 Phase 1 Deliverables
✓ Hybrid Processing
✓ File Router
✓ 200 MB Threshold
✓ Python Batch
✓ PySpark
✓ Raw Data Layer
✓ Data Cleaning
✓ Validation
✓ Corrected Records
✓ Quarantine
✓ Error Classification
✓ Idempotent Upsert
✓ Metrics
✓ Consistency Check
📦 Phase 2 Deliverables
✓ 5 Queries
✓ 3 Indexes
✓ Compound Index
✓ Explain
✓ 5 Aggregation Reports
✓ daily_sales_summary
✓ top_products_summary
✓ Incremental Refresh
✓ 2 Scheduled Jobs
✓ FastAPI
✓ Swagger Documentation
🎓 Conclusion
المشروع يمثل نظاماً متكاملاً لمعالجة بيانات الطلبات باستخدام Hybrid Processing.
يبدأ النظام من:
CSV
 ↓
Routing
 ↓
Processing
 ↓
Raw Layer
 ↓
Cleaning
 ↓
Validation
 ↓
Classification
 ↓
Upsert
 ↓
Analytics
 ↓
Materialized Views
 ↓
Scheduled Jobs
 ↓
FastAPI
وبذلك يجمع المشروع بين:
Big Data + ELT + Data Quality + Apache Spark + MongoDB + Analytics + REST API
🔗 GitHub
Repository:
https://github.com/mohammedalrubai/midterm-data-pipeline
👨‍💻 Student
محمد يوسف حمود سعيد الربيعي
جامعة الرازي -- كلية الحاسوب وتقنية المعلومات
الذكاء الاصطناعي -- المستوى الرابع