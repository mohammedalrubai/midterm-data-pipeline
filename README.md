🚀 Hybrid Big Data Pipeline & Analytics Engine
Phase 1 + Phase 2
جامعة الرازي -- كلية الحاسوب وتقنية المعلومات
التخصص: الذكاء الاصطناعي -- المستوى الرابع
📌 نظرة عامة
هذا المشروع عبارة عن Hybrid ELT Data Pipeline لمعالجة وتحليل بيانات الطلبات القادمة من ملفات CSV ذات أحجام وجودة مختلفة.
الفكرة الأساسية هي بناء Pipeline قادر على التعامل مع البيانات الصغيرة باستخدام Python Batch Processing، والبيانات الكبيرة باستخدام Apache Spark، ثم تخزين البيانات في MongoDB للاستفادة منها في عمليات التنظيف والتحقق والتحليل وإنتاج التقارير.
يمر النظام بعدة مراحل تبدأ من قراءة ملف CSV، ثم تحديد محرك المعالجة المناسب حسب حجم الملف، وبعد ذلك تحميل البيانات إلى طبقة Raw، وتنفيذ قواعد التنظيف والتحقق، وتصنيف السجلات، ثم تخزين النتائج النهائية وإنتاج المقاييس والتقارير.
في Phase 2 تمت إضافة طبقة تحليلية فوق الـ Pipeline تشمل الاستعلامات والفهارس وعمليات Aggregation وMaterialized Views والمهام المجدولة وواجهة FastAPI موحدة.
📚 المحتويات
فكرة المشروع
أهداف المشروع
Architecture
Phase 1 - Hybrid ELT Pipeline
File Router
Raw Data Layer
Data Cleaning & Validation
Classification & Quarantine
Idempotent Upsert
Metrics & Consistency
Phase 2 - Analytics Layer
Queries & Indexes
Aggregation Reports
Materialized Views
Scheduled Jobs
FastAPI
Project Structure
Installation & Running
Testing
MongoDB Collections
Technologies
GitHub
💡 فكرة المشروع
المشروع يعالج مشكلة شائعة في أنظمة البيانات: كيف نتعامل مع ملفات CSV صغيرة وكبيرة باستخدام محرك معالجة مناسب، مع الحفاظ على جودة البيانات وإمكانية إعادة تشغيل الـ Pipeline بدون إنشاء بيانات مكررة؟
لذلك تم تصميم النظام بحيث يكون لديه Router يحدد طريقة المعالجة بناءً على حجم الملف.
                    CSV File
                       │
                       ▼
                ┌──────────────┐
                │ File Router  │
                └──────┬───────┘
                       │
              ┌────────┴────────┐
              │                 │
          Small File        Large File
              │                 │
              ▼                 ▼
       Python Batch          PySpark
              │                 │
              └────────┬────────┘
                       ▼
                  orders_raw
                       │
                       ▼
             Cleaning & Validation
                       │
             ┌─────────┴─────────┐
             │                   │
          Valid/Corrected     Quarantine
             │                   │
             ▼                   ▼
      orders_validated    orders_quarantine
             │
             ▼
       Reports / Metrics
🎯 أهداف المشروع
المشروع يحقق مجموعة من الأهداف:
معالجة ملفات CSV بأحجام مختلفة.
اختيار محرك المعالجة تلقائياً.
استخدام Python للبيانات الصغيرة.
استخدام PySpark للبيانات الكبيرة.
تطبيق مبدأ ELT من خلال تحميل البيانات الخام أولاً.
تنظيف البيانات غير المنظمة.
اكتشاف أخطاء جودة البيانات.
تصنيف السجلات إلى Valid / Corrected / Quarantine.
الاحتفاظ بسجل تدقيق للتصحيحات.
منع تكرار البيانات عند إعادة تشغيل العملية.
تخزين البيانات في MongoDB.
إنشاء تقارير تحليلية.
توفير Materialized Views.
تشغيل مهام دورية لتحديث النتائج.
توفير REST API من خلال FastAPI.
🏗️ Architecture
المعمارية العامة للمشروع:
CSV
 │
 ▼
File Router
 │
 ├─────────────── Small ───────────────► Python Batch
 │
 └─────────────── Large ───────────────► PySpark
                                             │
                                             ▼
                                      MongoDB Raw Layer
                                         orders_raw
                                             │
                                             ▼
                                   Cleaning & Validation
                                             │
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                       Valid             Corrected          Quarantine
                          │                  │                  │
                          └──────────────────┴──────────────────┘
                                             │
                                             ▼
                                    orders_validated
                                             │
                                             ▼
                                  Analytics / Reports
                                             │
             ┌────────────────┬──────────────┼──────────────┐
             ▼                ▼              ▼              ▼
          Queries        Aggregations   Materialized     FastAPI
                                         Views
                                             │
                                             ▼
                                        Scheduler
🔵 Phase 1 - Hybrid ELT Pipeline
Phase 1 تمثل الجزء الأساسي من النظام.
الـ Pipeline يبدأ بملف CSV، ثم يقوم النظام بفحص حجم الملف وتحديد محرك المعالجة المناسب.
بعد ذلك يتم تحميل البيانات إلى MongoDB في طبقة Raw قبل إجراء عمليات التنظيف.
هذا التصميم يطبق مفهوم ELT:
Extract → Load → Transform
أي أن البيانات يتم استخراجها من الملف، ثم تحميلها إلى Raw Layer، وبعد ذلك يتم تنفيذ عمليات التحويل والتنظيف.
📂 File Router
يستخدم المشروع Router لتحديد طريقة المعالجة.
تم اعتماد حد:
200 MB
القاعدة:
File Size < 200 MB
        ↓
Python Batch

File Size >= 200 MB
        ↓
PySpark
وبذلك لا يحتاج المستخدم إلى تحديد المحرك يدوياً.
النظام يقرأ حجم الملف ويختار المحرك المناسب تلقائياً.
🐍 Python Batch Processing
عند التعامل مع الملفات الصغيرة يتم استخدام Python Batch Processing.
بدلاً من تحميل الملف كاملاً إلى الذاكرة، تتم قراءة البيانات على شكل دفعات.
CSV
 │
 ▼
Read Batch
 │
 ▼
Create Documents
 │
 ▼
MongoDB
 │
 ▼
Read Next Batch
هذه الطريقة تقلل استهلاك الذاكرة مقارنة بتحميل الملف بالكامل.
⚡ PySpark Processing
عند وصول حجم الملف إلى حد المعالجة الكبيرة، يقوم Router باختيار Apache Spark.
Spark مناسب لمعالجة البيانات الكبيرة لأنه يعتمد على DataFrame execution وعمليات المعالجة الموزعة.
يتم استخدام PySpark لقراءة البيانات الكبيرة ثم تحميلها إلى MongoDB Raw Layer.
تم أيضاً إعداد بيئة Hadoop المطلوبة لتشغيل Spark على Windows.
🗃️ Raw Data Layer
بعد قراءة البيانات يتم تحميلها أولاً إلى:
orders_raw
قبل تنفيذ عمليات التنظيف.
الغرض من Raw Layer هو الاحتفاظ بالبيانات الأصلية مع معلومات تساعد على تتبع مصدر السجل.
من معلومات التتبع المستخدمة:
run_id
source_file
source_row_number
ingested_at
engine_used
raw_record
وبذلك يمكن معرفة مصدر كل Record والـ Run الذي قام بإدخاله.
🧹 Data Cleaning & Validation
بعد Raw Layer تبدأ مرحلة تنظيف البيانات والتحقق منها.
يتعامل النظام مع مجموعة من مشاكل جودة البيانات، منها:
1. Arabic Digits
تحويل الأرقام العربية إلى الشكل القياسي.
2. Currency Normalization
توحيد تمثيل الأسعار والعملات.
3. Thousand Separators
معالجة الفواصل المستخدمة داخل قيم الأسعار.
4. Word Prices
معالجة الأسعار المكتوبة بطريقة نصية.
5. Phone Formatting
تنظيف وتوحيد أرقام الهواتف.
6. Email Repair
معالجة بعض أخطاء البريد الإلكتروني.
7. Date Normalization
توحيد صيغ التاريخ.
8. Order Status Normalization
توحيد حالات الطلب.
9. Total Recomputation
إعادة حساب إجمالي الطلب للتحقق من صحة القيمة.
🚦 Classification
بعد تنفيذ قواعد التنظيف والتحقق، يتم تصنيف كل Record.
يوجد ثلاث نتائج رئيسية:
                    Record
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
          Valid?              Invalid?
             │                   │
             ▼          ┌────────┴────────┐
           VALID         │                 │
                         ▼                 ▼
                     Correctable       Not Correctable
                         │                 │
                         ▼                 ▼
                     CORRECTED         QUARANTINE
✅ Valid
السجل صحيح ولا يحتاج إلى تعديل.
🔧 Corrected
السجل يحتوي على مشكلة يمكن إصلاحها بواسطة قواعد التنظيف.
ويتم الاحتفاظ بمعلومات عن التعديل ضمن Audit Trail.
❌ Quarantine
السجل يحتوي على مشكلة لا يمكن قبولها أو تصحيحها بشكل آمن.
يتم عزله في:
orders_quarantine
بدلاً من إدخاله إلى البيانات النهائية.
⚠️ Quarantine Error Codes
يستخدم المشروع Error Codes لوصف أسباب رفض السجلات.
ومن أمثلة الحالات التي ظهرت أثناء التشغيل:
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
EMPTY_ITEMS
CORRUPTED_ITEMS_JSON
INVALID_IMPOSSIBLE_DATE
UNKNOWN_PRICE
DUPLICATE_ORDER_ID
AMBIGUOUS_NEGATIVE_VALUE
MULTIPLE_CONFLICTING_ERRORS
وهذا يجعل تحليل جودة البيانات أسهل من مجرد تسجيل أن السجل Invalid.
🔄 Idempotent Upsert
من المتطلبات المهمة في المشروع أن إعادة تشغيل نفس البيانات لا تؤدي إلى إنشاء Duplicate Records.
لذلك يستخدم النظام:
order_id
كمفتاح للـ Upsert في البيانات النهائية.
الفكرة:
First Run
   │
   ▼
Insert

Second Run - Same Data
   │
   ▼
Update / Unchanged
وبالتالي يمكن تشغيل Pipeline أكثر من مرة بدون إنشاء نسخ مكررة من نفس الطلب.
📊 Metrics & Consistency
بعد انتهاء Pipeline يتم تسجيل مجموعة من المقاييس.
من أهمها:
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
ويتم حفظ نتائج التشغيل في:
reports/results.json
Consistency Check
يتم التحقق من المعادلة:
raw_loaded =
valid_count +
corrected_count +
quarantine_count
إذا تحققت المعادلة فهذا يعني أن جميع السجلات التي تم تحميلها إلى Raw تم تصنيفها ضمن نتائج الـ Pipeline.
🟣 Phase 2 - Analytics Layer
Phase 2 تضيف طبقة تحليلية فوق البيانات الناتجة من Phase 1.
وتشمل:
Queries
Indexes
Explain
Aggregations
Materialized Views
Scheduled Jobs
FastAPI
🔎 Queries & Indexes
تم إنشاء مجموعة من الاستعلامات للتعامل مع البيانات المخزنة في MongoDB.
يوفر المشروع خمسة Queries أساسية للتحليل والوصول إلى البيانات.
كما تم إنشاء ثلاثة Indexes، من بينها Compound Index.
الغرض من الـ Indexes هو تحسين سرعة البحث وتقليل عدد الوثائق التي يحتاج MongoDB إلى فحصها.
📈 Explain
يتم استخدام MongoDB Explain لمقارنة طريقة تنفيذ الاستعلام قبل وبعد إضافة Index.
الفكرة:
Query
  │
  ├── Without Index
  │       ↓
  │    Collection Scan
  │
  └── With Index
          ↓
       Index Scan
وهذا يسمح بقياس تأثير الـ Index على تنفيذ الاستعلام بدلاً من الاعتماد على التخمين.
📊 Aggregation Reports
تمت إضافة خمسة تقارير تحليلية باستخدام MongoDB Aggregation Pipeline.
1. Sales By City
تحليل المبيعات حسب المدينة.
City
  ↓
Total Sales
  ↓
Order Count
2. Top Products
تحديد المنتجات الأكثر مبيعاً.
3. Top Customers
تحديد العملاء الأعلى من حيث قيمة المشتريات.
4. Sales By Period
تحليل المبيعات حسب الفترة الزمنية.
5. Order Status Distribution
تحليل توزيع الطلبات حسب حالتها.
مثلاً:
Pending
Completed
Cancelled
...
يتم إنشاء التقارير من البيانات الفعلية الموجودة في MongoDB، وليست من قيم ثابتة داخل الكود.
🧱 Materialized Views
تمت إضافة Materialized Views لتخزين نتائج تحليلية جاهزة يمكن الوصول إليها بسرعة بدلاً من إعادة تنفيذ Aggregation Pipeline في كل مرة.
المشروع يحتوي على View رئيسية للمبيعات اليومية:
daily_sales_summary
وView للمنتجات الأعلى:
top_products_summary
📅 Daily Sales Summary
تقوم:
daily_sales_summary
بتجميع المبيعات اليومية، مع البيانات اللازمة للتحليل حسب اليوم والمدينة.
بدلاً من تنفيذ Aggregation كامل في كل طلب، يمكن قراءة النتائج الجاهزة من Materialized View.
🏆 Top Products Summary
تقوم:
top_products_summary
بتخزين ملخص المنتجات الأعلى مبيعاً.
وهذا يجعل الوصول إلى نتائج المنتجات الأكثر مبيعاً أسرع وأسهل للتطبيقات التي تحتاج هذه البيانات بشكل متكرر.
🔄 Incremental Refresh
تم تصميم تحديث Materialized Views بحيث يمكن تحديث النتائج بدلاً من الاعتماد على إعادة بناء غير ضرورية لكل البيانات في كل مرة.
يتم الاحتفاظ ببيانات Metadata مرتبطة بالتحديثات، ومنها:
mv_metadata
وهذا يمثل الأساس لعملية Incremental Refresh.
⏰ Scheduled Jobs
تم إنشاء Scheduler لتنفيذ عمليات دورية.
من المهام الموجودة:
Job 1 --- Refresh Views
تحديث Materialized Views.
Job 2 --- Daily Report
تشغيل التقرير الدوري وتسجيل نتيجة التنفيذ.
يتم تسجيل معلومات التشغيل مثل:
start
end
status
duration
وهذا يساعد في معرفة هل المهمة نجحت أم فشلت وكم استغرقت.
🚀 FastAPI
تم إضافة FastAPI كواجهة موحدة للوصول إلى وظائف المشروع.
تشمل الواجهة عمليات مرتبطة بـ:
Health Check
Ingestion
Index Creation
Queries
Aggregations
Materialized Views
Scheduled Jobs
وتوفر FastAPI أيضاً Swagger UI للاختبار.
بعد تشغيل API يمكن فتح:
http://localhost:8000/docs
ومن خلال Swagger يمكن تجربة الـ endpoints مباشرة.
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
⚙️ Installation & Running
1. المتطلبات
يحتاج المشروع إلى:
Python 3.10+
Java JDK 11+
MongoDB 6.0+
PySpark 3.5+
2. تثبيت المكتبات
في Windows PowerShell:
py -m pip install -r requirements.txt
في بيئة Windows الخاصة بالمشروع يتم استخدام py بدلاً من python.
3. إعداد Environment Variables
قم بإنشاء ملف:
.env
اعتماداً على:
.env.example
ولا تضع كلمات المرور أو البيانات السرية داخل GitHub.
▶️ تشغيل Phase 1
يمكن تشغيل الـ Pipeline من:
py src/main.py
▶️ تشغيل مكونات Phase 2
Queries:
py src/queries.py
Aggregations:
py src/aggregations.py
Materialized Views:
py src/materialized_views.py
Scheduler:
py src/scheduler.py
FastAPI:
py src/api.py
ثم افتح:
http://localhost:8000/docs
🧪 Testing
يمكن تشغيل الاختبارات باستخدام:
py -m pytest tests/ -v
ويتم استخدام الاختبارات للتحقق من قواعد التنظيف والتصنيف وعدم كسر وظائف Phase 1 أثناء إضافة Phase 2.
🗄️ MongoDB Collections
الـ Pipeline الأساسي يستخدم:
orders_raw
orders_validated
orders_quarantine
وتوجد أيضاً Collections مرتبطة بالطبقة التحليلية والـ Materialized Views والـ Scheduler وفق تنفيذ Phase 2.
🛠️ Technologies
Technology     Usage
Python         Batch Processing & Pipeline Apache Spark   Large File Processing PySpark        Spark implementation MongoDB        Data Storage & Analytics FastAPI        REST API Pytest         Testing Java           Spark Runtime Hadoop         Spark support on Windows
🔐 Data Quality
أحد أهم أهداف المشروع ليس فقط تحميل البيانات، وإنما معرفة جودة البيانات قبل اعتمادها.
لذلك يتم الفصل بين:
Raw Data
     │
     ▼
Cleaned Data
     │
     ├── Valid
     ├── Corrected
     └── Quarantine
وبهذه الطريقة لا يتم فقد البيانات الأصلية، وفي الوقت نفسه لا يتم إدخال السجلات غير الموثوقة إلى البيانات النهائية.
🔁 End-to-End Flow
التدفق الكامل للمشروع:
CSV File
   │
   ▼
File Size Detection
   │
   ├───────────────┐
   ▼               ▼
Python Batch     PySpark
   │               │
   └───────┬───────┘
           ▼
      orders_raw
           │
           ▼
   Cleaning Rules
           │
           ▼
      Validation
           │
      ┌────┼────┐
      ▼    ▼    ▼
    Valid Corrected Quarantine
      │      │       │
      └──┬───┘       │
         ▼           ▼
 orders_validated  orders_quarantine
         │
         ▼
   Queries / Reports
         │
         ├── Aggregations
         ├── Materialized Views
         ├── Scheduled Jobs
         └── FastAPI
📌 أهم نقاط المشروع
Phase 1
Hybrid processing.
Python Batch للملفات الصغيرة.
PySpark للملفات الكبيرة.
Router threshold = 200 MB.
Raw Layer.
Data Cleaning.
Validation.
Quarantine.
Audit information.
Idempotent Upsert.
Metrics.
Consistency validation.
Phase 2
5 Queries.
3 Indexes.
Compound Index.
Explain.
5 Aggregation Reports.
daily_sales_summary.
top_products_summary.
Incremental Materialized View refresh.
Scheduled Jobs.
FastAPI.
Swagger Documentation.
🎓 الخلاصة
هذا المشروع يمثل نموذجاً عملياً لبناء Hybrid Big Data Pipeline يبدأ من ملفات CSV وينتهي بطبقة تحليلية يمكن الوصول إليها من خلال API.
الفكرة الأساسية ليست فقط معالجة البيانات، وإنما بناء نظام كامل يستطيع:
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
وبذلك يجمع المشروع بين مفاهيم:
Big Data + ELT + Data Quality + MongoDB + Spark + Analytics + REST API
🔗 GitHub
Repository:
https://github.com/mohammedalrubai/midterm-data-pipeline
👨‍💻 Student
الاسم: محمد يوسف حمود سعيد الربيعي
الجامعة: جامعة الرازي
التخصص: الذكاء الاصطناعي
المستوى: الرابع