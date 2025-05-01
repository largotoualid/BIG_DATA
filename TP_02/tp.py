import os
import time
import psutil
import pandas as pd
import dask.dataframe as dd
import kagglehub
from tabulate import tabulate

# 1) تحميل مجموعة البيانات من Kaggle
path = kagglehub.dataset_download("mkechinov/ecommerce-behavior-data-from-multi-category-store")
print("Path to dataset files:", path)

# 2) تحديد مسار النسخة المطلوبة (حسب ما ظهر لديك)
dataset_path = "/root/.cache/kagglehub/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/versions/8"
print("Dataset path:", dataset_path)

# 3) استخدام ملف واحد فقط: "2019-Oct.csv"
csv_file = "2019-Oct.csv"
csv_file_path = os.path.join(dataset_path, csv_file)
print("CSV file path:", csv_file_path)

# قائمة لتجميع مؤشرات الأداء
results = []
process = psutil.Process(os.getpid())

###############################################
# تحليل البيانات باستخدام Pandas (Chunking)
###############################################
print("\n--- تحليل Pandas مع Chunking ---")
chunksize = 1_000_000  # يمكنك تعديل القيمة حسب الموارد المتوفرة
start_time = time.time()
start_mem = process.memory_info().rss / (1024**2)

# قراءة الملف على دفعات وحساب الوصف (لنقوم بحساب الإحصائيات دون طباعة كل دفعة)
for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
    _ = chunk.describe()

end_time = time.time()
end_mem = process.memory_info().rss / (1024**2)
pandas_time = end_time - start_time
pandas_mem = end_mem - start_mem
print(f"Pandas Chunking:\n  - وقت التنفيذ: {pandas_time:.2f} ثانية\n  - استهلاك الذاكرة: {pandas_mem:.2f} ميجابايت")
results.append(["Pandas Chunking", round(pandas_time, 2), round(pandas_mem, 2)])

###############################################
# تحليل البيانات باستخدام Dask (ملف CSV غير مضغوط)
###############################################
print("\n--- تحليل Dask (CSV غير مضغوط) ---")
start_time = time.time()
start_mem = process.memory_info().rss / (1024**2)

# قراءة الملف باستخدام Dask مع تحديد blocksize
df = dd.read_csv(csv_file_path, blocksize="256MB")
_ = df.describe().compute()

end_time = time.time()
end_mem = process.memory_info().rss / (1024**2)
dask_csv_time = end_time - start_time
dask_csv_mem = end_mem - start_mem
print(f"Dask Non Compressed:\n  - وقت التنفيذ: {dask_csv_time:.2f} ثانية\n  - استهلاك الذاكرة: {dask_csv_mem:.2f} ميجابايت")
results.append(["Dask Non Compressed", round(dask_csv_time, 2), round(dask_csv_mem, 2)])

###############################################
# تحويل CSV إلى Parquet ثم تحليله باستخدام Dask
###############################################
print("\n--- تحويل CSV إلى Parquet وتحليل Dask ---")
parquet_folder = "/content/ecommerce_parquet"
if not os.path.exists(parquet_folder):
    os.makedirs(parquet_folder)

pq_path = os.path.join(parquet_folder, csv_file.replace(".csv", ".parquet"))
print(f"تحويل {csv_file} إلى Parquet في: {pq_path}")
start_time_conv = time.time()
df = dd.read_csv(csv_file_path, blocksize="256MB")
df.to_parquet(pq_path, engine="pyarrow", compression="snappy")
end_time_conv = time.time()
conv_time = end_time_conv - start_time_conv
print(f"زمن تحويل CSV إلى Parquet: {conv_time:.2f} ثانية")

print("\n-- تحليل ملف Parquet باستخدام Dask --")
start_time = time.time()
start_mem = process.memory_info().rss / (1024**2)
df_parquet = dd.read_parquet(pq_path, engine="pyarrow")
_ = df_parquet.describe().compute()
end_time = time.time()
end_mem = process.memory_info().rss / (1024**2)
dask_parquet_time = end_time - start_time
dask_parquet_mem = end_mem - start_mem
print(f"Dask Parquet:\n  - وقت التنفيذ: {dask_parquet_time:.2f} ثانية\n  - استهلاك الذاكرة: {dask_parquet_mem:.2f} ميجابايت")
results.append(["Dask Parquet", round(dask_parquet_time, 2), round(dask_parquet_mem, 2)])

###############################################
# عرض جدول مقارنة الأداء
###############################################
results_df = pd.DataFrame(results, columns=["Algorithm", "Execution Time (s)", "Memory Usage (MB)"])
print("\n=== جدول مقارنة الأداء ===")
print(tabulate(results_df, headers="keys", tablefmt="psql"))
