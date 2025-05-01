import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

file_path = "D:\TP_BIG/2019-Oct-Cleaned.csv"
df = pd.read_csv(file_path, nrows=1_000_000)  # تحميل أول مليون صف فقط

# حفظ الملف الجديد
sample_file_path = "D:\TP_BIG/2019-Oct-Sample.csv"
df.to_csv(sample_file_path, index=False)

print(f" تم حفظ عينة البيانات في: {sample_file_path}")