import os
import pandas as pd

# ----- PHẦN 1: TRÍCH XUẤT METADATA -----

# Thay đổi đường dẫn này thành đường dẫn bạn đã giải nén bộ RAVDESS
data_path = "D:\\Pipeline_Audio_IT5425\\ravdess-emotional-speech-audio" 

# Định nghĩa các nhãn cảm xúc và giới tính dựa trên tài liệu của RAVDESS
emotions = {
    '01': 'neutral',
    '02': 'calm',
    '03': 'happy',
    '04': 'sad',
    '05': 'angry',
    '06': 'fearful',
    '07': 'disgust',
    '08': 'surprised'
}

# Tạo một danh sách để lưu trữ thông tin của mỗi file
metadata_list = []

print("Bắt đầu quét thư mục dữ liệu...")

# Duyệt qua tất cả các thư mục con (Actor_01, Actor_02, ...)
for actor_folder in os.listdir(data_path):
    actor_path = os.path.join(data_path, actor_folder)
    
    # Kiểm tra xem có phải là thư mục không
    if os.path.isdir(actor_path):
        # Duyệt qua tất cả file .wav trong thư mục của diễn viên
        for file_name in os.listdir(actor_path):
            if file_name.endswith('.wav'):
                # Tách tên file để lấy thông tin
                parts = file_name.split('.')[0].split('-')
                
                # Lấy các thông tin cần thiết
                emotion_code = parts[2]
                actor_id = int(parts[6])
                
                # Chuyển mã cảm xúc thành tên cảm xúc
                emotion_label = emotions[emotion_code]
                
                # Xác định giới tính: số lẻ là Nam, số chẵn là Nữ
                gender = 'female' if actor_id % 2 == 0 else 'male'
                
                # Lấy đường dẫn đầy đủ của file
                file_path = os.path.join(actor_path, file_name)
                
                # Thêm thông tin vào danh sách
                metadata_list.append({
                    'file_name': file_name,
                    'file_path': file_path,
                    'emotion': emotion_label,
                    'gender': gender,
                    'actor_id': actor_id
                })

print(f"Đã quét xong! Tìm thấy {len(metadata_list)} file âm thanh.")

# Chuyển danh sách thành một DataFrame của pandas
metadata_df = pd.DataFrame(metadata_list)

# Lưu DataFrame thành file CSV
csv_path = 'ravdess_metadata.csv'
metadata_df.to_csv(csv_path, index=False)

print(f"Đã lưu metadata vào file '{csv_path}'.")

# ----- KẾT THÚC PHẦN 1 -----
# ----- PHẦN 2: TẢI DỮ LIỆU LÊN MINIO DATA LAKE -----
from minio import Minio
from minio.error import S3Error

# Cấu hình kết nối tới MinIO
# Endpoint là địa chỉ của MinIO, mặc định là localhost:9000
# Access key và Secret key là user/pass bạn đã đặt khi chạy Docker
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False  # Đặt là False vì chúng ta đang chạy trên localhost, không có SSL
)

# Tên các bucket (thùng chứa)
audio_bucket_name = "raw-audio"
metadata_bucket_name = "raw-metadata"

# Tạo bucket nếu chưa tồn tại
try:
    if not minio_client.bucket_exists(audio_bucket_name):
        minio_client.make_bucket(audio_bucket_name)
        print(f"Bucket '{audio_bucket_name}' đã được tạo.")
    else:
        print(f"Bucket '{audio_bucket_name}' đã tồn tại.")

    if not minio_client.bucket_exists(metadata_bucket_name):
        minio_client.make_bucket(metadata_bucket_name)
        print(f"Bucket '{metadata_bucket_name}' đã được tạo.")
    else:
        print(f"Bucket '{metadata_bucket_name}' đã tồn tại.")
        
except S3Error as exc:
    print("Lỗi khi tạo bucket:", exc)


# Tải file metadata lên MinIO
print("\nBắt đầu tải file metadata lên MinIO...")
try:
    minio_client.fput_object(
        metadata_bucket_name, # Tên bucket
        "ravdess_metadata.csv",  # Tên object trong MinIO
        csv_path,               # Đường dẫn file trên máy tính
    )
    print(f"Đã tải thành công '{csv_path}' lên bucket '{metadata_bucket_name}'.")
except S3Error as exc:
    print("Lỗi khi tải file metadata:", exc)

# Tải các file âm thanh lên MinIO
print("\nBắt đầu tải các file âm thanh... (Quá trình này có thể mất vài phút)")
for index, row in metadata_df.iterrows():
    file_path = row['file_path']
    file_name = row['file_name']
    
    try:
        # fput_object dùng để tải file từ máy tính
        minio_client.fput_object(
            audio_bucket_name,  # Tên bucket
            file_name,          # Tên object trong MinIO
            file_path           # Đường dẫn file trên máy tính
        )
        # In ra tiến trình sau mỗi 100 file để biết nó đang chạy
        if (index + 1) % 100 == 0:
            print(f"  Đã tải {index + 1}/{len(metadata_df)} file...")
            
    except S3Error as exc:
        print(f"Lỗi khi tải file '{file_name}':", exc)

print("Đã tải xong toàn bộ file âm thanh!")

# ----- KẾT THÚC PHẦN 2 -----