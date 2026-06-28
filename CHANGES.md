# التغييرات — تبسيط Pipeline

## ما تم حذفه
| الملف | السبب |
|---|---|
| `MainForMonthDataProcess.scala` | نقطة دخول ثانية لم تعد ضرورية |
| `storage/MonthlyKafkaWriter.scala` | كان يكتب لـ Kafka ليُقرأ مرة ثانية — مرحلة وسيطة بلا قيمة |
| `ingestion/ProcessDataReader.scala` | كان يقرأ من Kafka الشهري — يُغني عنه foreachBatch مباشرة |

## ما تغيّر
- `MainForStreamData.scala`: يشغّل stream واحد يكتب إلى ES مباشرة **ويحسب التحليلات الشهرية** في نفس الـ foreachBatch
- `build.sbt`: حُذف `spray-json` (كان ungrouped import غير مستخدم)

## النتيجة
```
قبل: JSON → Kafka(test) → Spark1 → Kafka(people-YYYY-MM) → Spark2 → ES
بعد: JSON → Kafka(linkedin-profiles) → Spark → ES
```
- عدد الملفات: 13 → 10
- عدد الـ Kafka topics الشهرية: N topics → 0
- عدد الـ Spark jobs: 2 → 1
