import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
from minio.error import S3Error
import io

# ----- PHẦN 1: KẾT NỐI VÀ LẤY DỮ LIỆU TỪ DATA LAKE -----

print("Đang kết nối tới MinIO...")
# Cấu hình kết nối tới MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

curated_bucket_name = "curated-data"
parquet_object_name = "ravdess_features.parquet"

# Tải và đọc file Parquet từ MinIO
print(f"Đang tải file '{parquet_object_name}' từ bucket '{curated_bucket_name}'...")
try:
    response = minio_client.get_object(curated_bucket_name, parquet_object_name)
    # Dùng io.BytesIO để pandas đọc trực tiếp từ stream dữ liệu
    data_df = pd.read_parquet(io.BytesIO(response.read()))
    print("Tải và đọc file Parquet thành công.")
    print(f"DataFrame có {len(data_df)} dòng và {len(data_df.columns)} cột.")
except S3Error as exc:
    print("Lỗi khi tải file Parquet từ MinIO:", exc)
    exit()

# ----- PHẦN 2: KẾT NỐI VÀ NẠP DỮ LIỆU VÀO DATA WAREHOUSE (POSTGRESQL) -----

# Chuỗi kết nối tới PostgreSQL
# Cú pháp: "postgresql://<user>:<password>@<host>:<port>/<database>"
# Database mặc định cũng là 'postgres'
db_connection_str = 'postgresql://postgres:postgres@localhost:5432/postgres'

# Tạo một "engine" kết nối
try:
    db_engine = create_engine(db_connection_str)
    print("Kết nối tới PostgreSQL thành công.")
except Exception as e:
    print(f"Lỗi khi kết nối tới PostgreSQL: {e}")
    exit()

# Tên bảng chúng ta sẽ tạo trong PostgreSQL
table_name = 'audio_features'

print(f"Đang nạp dữ liệu vào bảng '{table_name}'...")
try:
    # Dùng hàm to_sql của pandas để nạp dữ liệu
    # if_exists='replace': Nếu bảng đã tồn tại, xóa đi và tạo lại. 
    # Rất tiện cho việc chạy lại script nhiều lần.
    # index=False: Không nạp cột index của DataFrame vào database.
    data_df.to_sql(table_name, db_engine, if_exists='replace', index=False)
    
    print("Nạp dữ liệu vào Data Warehouse thành công!")
    
except Exception as e:
    print(f"Lỗi khi nạp dữ liệu vào bảng: {e}")

# ----- KẾT THÚC -----