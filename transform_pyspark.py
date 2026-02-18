import os
import io
import sys
import numpy as np
import librosa
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DoubleType
from minio import Minio

# ---------------------------------------------------------
# CẤU HÌNH & KHỞI TẠO
# ---------------------------------------------------------
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Khởi tạo Spark Session
# 'local[*]' nghĩa là sử dụng tất cả các nhân CPU có sẵn trên máy
spark = SparkSession.builder \
    .appName("AudioFeatureExtraction_Spark") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.native.lib", "false") \
    .config("spark.hadoop.util.NativeCodeLoader.isNativeCodeLoaded", "false") \
    .master("local[2]") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# Cấu hình MinIO (Sử dụng bên trong các worker)
MINIO_CONF = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False
}

BUCKET_AUDIO = "raw-audio"
BUCKET_METADATA = "raw-metadata"
BUCKET_CURATED = "curated-data"

# ---------------------------------------------------------
# ĐỊNH NGHĨA HÀM XỬ LÝ (CORE LOGIC)
# ---------------------------------------------------------

def process_partition(iterator):
    """
    Hàm này sẽ chạy trên từng worker (phân tán).
    Thay vì kết nối MinIO 1440 lần, ta dùng mapPartitions để 
    kết nối 1 lần cho mỗi nhóm dữ liệu (partition).
    """
    # 1. Khởi tạo kết nối MinIO bên trong worker
    import io
    import numpy as np
    import librosa
    from minio import Minio
    
    # Cấu hình lại bên trong
    MINIO_CONF = {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "secure": False
    }
    
    minio_client = Minio(**MINIO_CONF)
    
    # 2. Duyệt qua danh sách file trong partition này
    for row in iterator:
        file_name = row.file_name
        
        try:
            # Tải file từ MinIO (In-memory, không lưu xuống đĩa)
            response = minio_client.get_object(BUCKET_AUDIO, file_name)
            audio_bytes = io.BytesIO(response.read())
            response.close()
            
            # Xử lý bằng Librosa
            y, sr = librosa.load(audio_bytes, sr=None)
            
            # --- TRÍCH XUẤT ĐẶC TRƯNG (Logic cũ của bạn) ---
            features = []
            def safe_float(val):
                """Chuyển đổi mọi kiểu numpy/array về float Python chuẩn"""
                try:
                    if isinstance(val, np.ndarray):
                        return float(val.flatten()[0]) # Lấy phần tử đầu tiên nếu là mảng
                    return float(val)
                except:
                    return 0.0 # Giá trị mặc định nếu lỗi
            
            # ZCR
            zcr = np.mean(librosa.feature.zero_crossing_rate(y=y).T, axis=0).item()
            features.append(safe_float(zcr))
            
            # Spectral Centroid
            spec_cent = np.mean(librosa.feature.spectral_centroid(y=y, sr=sr).T, axis=0).item()
            features.append(safe_float(spec_cent))
            
            # Chroma (12 giá trị)
            chroma = np.mean(librosa.feature.chroma_stft(y=y, sr=sr).T, axis=0)
            features.extend([safe_float(x) for x in chroma])
            
            # MFCC (40 giá trị)
            mfcc = np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=40).T, axis=0)
            features.extend([safe_float(x) for x in mfcc])
            
            # Mel Spectrogram
            #mel = np.mean(librosa.feature.melspectrogram(y=y, sr=sr).T, axis=0).item()
            mel = np.mean(librosa.feature.melspectrogram(y=y, sr=sr))
            features.append(safe_float(mel))
            
            # Trả về kết quả: (file_name, [list_of_features])
            yield (file_name, features)
            
        except Exception as e:
            # Nếu lỗi, trả về list rỗng hoặc xử lý tùy ý
            print(f"Error processing {file_name}: {str(e)}")
            yield (file_name, None)

# ---------------------------------------------------------
# PIPELINE CHÍNH
# ---------------------------------------------------------

# 1. Đọc Metadata (Extract)
# Ở môi trường thật, ta đọc trực tiếp từ S3. 
# Ở đây ta giả lập tải CSV về rồi đọc vào Spark.
temp_meta_path = "temp_metadata.csv"
m_client = Minio(**MINIO_CONF)
m_client.fget_object(BUCKET_METADATA, "ravdess_metadata.csv", temp_meta_path)

df_meta = spark.read.csv(temp_meta_path, header=True, inferSchema=True)

# Tăng số partitions để tận dụng đa luồng (parallelism)
# Ví dụ: máy bạn có 8 core, chia thành 16 phần để chạy song song
df_repartitioned = df_meta.select("file_name").repartition(16)

print("Bắt đầu xử lý phân tán với Spark...")

# 2. Transform (MapPartitions)
# Áp dụng hàm process_partition lên từng phần của dữ liệu
rdd_processed = df_repartitioned.rdd.mapPartitions(process_partition)

# Định nghĩa Schema cho dữ liệu đầu ra để tạo DataFrame
# Cấu trúc: file_name (String), features (Array of Float)
output_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("features_array", ArrayType(FloatType()), True)
])

df_features_raw = spark.createDataFrame(rdd_processed, schema=output_schema)

# Lọc bỏ các dòng lỗi (None)
df_features_raw = df_features_raw.filter(col("features_array").isNotNull())

# 3. Tách mảng features thành các cột riêng biệt (Flatten)
# Đây là bước biến mảng [0.1, 5000, ...] thành các cột zcr, spectral, mfcc_1...

# Tạo danh sách tên cột theo đúng thứ tự đã append trong hàm process_partition
col_names = ["zcr_mean", "spectral_centroid_mean"]
col_names += [f"chroma_{i+1}" for i in range(12)]
col_names += [f"mfcc_{i+1}" for i in range(40)]
col_names += ["mel_spectrogram_mean"]

# Dùng biểu thức select để tách mảng
select_exprs = ["file_name"] + [col("features_array")[i].alias(name) for i, name in enumerate(col_names)]
df_features_final = df_features_raw.select(*select_exprs)

# 4. Join với Metadata gốc
df_final = df_meta.join(df_features_final, on="file_name", how="inner")

print("Mẫu dữ liệu sau khi xử lý:")
df_final.show(5)

# 5. Load (Lưu trữ)
output_csv_path = "spark_ravdess_features.csv"
print(f"Đang lưu file CSV tại: {output_csv_path}")

# Xóa file/thư mục cũ nếu tồn tại để tránh lỗi permission
if os.path.exists(output_csv_path):
    if os.path.isdir(output_csv_path):
        import shutil
        shutil.rmtree(output_csv_path)
    else:
        os.remove(output_csv_path)

# Chuyển sang Pandas để ghi file duy nhất, tránh vấn đề Hadoop
df_pandas = df_final.toPandas()
df_pandas.to_csv(output_csv_path, index=False)

# (Tùy chọn) Upload ngược lại MinIO bằng Python client
print("Đang upload lên MinIO...")
m_client.fput_object(BUCKET_CURATED, "spark_output/spark_ravdess_features.csv", output_csv_path)

print("HOÀN TẤT PIPELINE!")
spark.stop()