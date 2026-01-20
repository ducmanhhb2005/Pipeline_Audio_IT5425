import os
import pandas as pd
import librosa
import numpy as np
from minio import Minio
from minio.error import S3Error
import io # Dùng để đọc object từ MinIO như một file

# ----- PHẦN 1: KẾT NỐI VÀ LẤY METADATA -----

print("Đang kết nối tới MinIO...")
# Cấu hình kết nối tới MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

audio_bucket_name = "raw-audio"
metadata_bucket_name = "raw-metadata"
curated_bucket_name = "curated-data"

# Tạo bucket cho dữ liệu đã qua xử lý
try:
    if not minio_client.bucket_exists(curated_bucket_name):
        minio_client.make_bucket(curated_bucket_name)
        print(f"Bucket '{curated_bucket_name}' đã được tạo.")
    else:
        print(f"Bucket '{curated_bucket_name}' đã tồn tại.")
except S3Error as exc:
    print("Lỗi khi tạo bucket:", exc)
    exit() # Thoát nếu không tạo được bucket


# Tải và đọc file metadata từ MinIO
print("Đang tải file metadata...")
try:
    response = minio_client.get_object(metadata_bucket_name, "ravdess_metadata.csv")
    # Đọc nội dung file CSV trực tiếp vào pandas DataFrame
    metadata_df = pd.read_csv(io.BytesIO(response.read()))
    print("Tải và đọc metadata thành công.")
except S3Error as exc:
    print("Lỗi khi tải metadata:", exc)
    metadata_df = pd.DataFrame() # Tạo dataframe rỗng nếu lỗi
    exit()

# ----- PHẦN 2: HÀM TRÍCH XUẤT ĐẶC TRƯNG ÂM THANH -----

def extract_features(audio_data, sample_rate):
    """
    Hàm này nhận vào dữ liệu âm thanh và sample rate,
    sau đó trích xuất các đặc trưng và trả về một dictionary.
    """
    features = {}
    
    # Zero Crossing Rate
    zcr = np.mean(librosa.feature.zero_crossing_rate(y=audio_data).T, axis=0).item()
    features['zcr_mean'] = zcr
    
    # Spectral Centroid
    spectral_centroid = np.mean(librosa.feature.spectral_centroid(y=audio_data, sr=sample_rate).T, axis=0).item()
    features['spectral_centroid_mean'] = spectral_centroid
    
    # Chroma Feature
    chroma = np.mean(librosa.feature.chroma_stft(y=audio_data, sr=sample_rate).T, axis=0)
    # chroma có 12 giá trị, ta sẽ lưu từng giá trị
    for i in range(len(chroma)):
        features[f'chroma_{i+1}'] = chroma[i]
        
    # MFCCs (Mel-Frequency Cepstral Coefficients)
    mfccs = np.mean(librosa.feature.mfcc(y=audio_data, sr=sample_rate, n_mfcc=40).T, axis=0)
    for i in range(len(mfccs)):
        features[f'mfcc_{i+1}'] = mfccs[i]
        
    # Mel Spectrogram
    mel_spec = np.mean(librosa.feature.melspectrogram(y=audio_data, sr=sample_rate).T, axis=0)
    features['mel_spectrogram_mean'] = np.mean(mel_spec)
        
    return features

# ----- PHẦN 3: XỬ LÝ VÀ BIẾN ĐỔI DỮ LIỆU -----

print("\nBắt đầu quá trình trích xuất đặc trưng... (Sẽ mất khá nhiều thời gian!)")
extracted_features_list = []

# Lặp qua từng dòng trong metadata DataFrame
for index, row in metadata_df.iterrows():
    file_name = row['file_name']
    
    try:
        # Tải file âm thanh từ MinIO
        audio_object = minio_client.get_object(audio_bucket_name, file_name)
        
        # Đọc file âm thanh bằng librosa
        # librosa.load có thể đọc trực tiếp từ một file-like object
        audio_data, sample_rate = librosa.load(io.BytesIO(audio_object.read()), sr=None)
        
        # Trích xuất đặc trưng
        features = extract_features(audio_data, sample_rate)
        
        # Thêm tên file vào dictionary để sau này join
        features['file_name'] = file_name
        
        extracted_features_list.append(features)
        
        if (index + 1) % 50 == 0:
            print(f"  Đã xử lý {index + 1}/{len(metadata_df)} file...")
            
    except Exception as e:
        print(f"Lỗi khi xử lý file '{file_name}': {e}")

print("Hoàn tất trích xuất đặc trưng!")

# Chuyển danh sách đặc trưng thành DataFrame
features_df = pd.DataFrame(extracted_features_list)

# Hợp nhất (merge) DataFrame đặc trưng với DataFrame metadata
# Dựa trên cột 'file_name' chung
final_df = pd.merge(metadata_df, features_df, on='file_name', how='inner')

# Lưu DataFrame cuối cùng thành file Parquet
parquet_path = 'ravdess_features.parquet'
final_df.to_parquet(parquet_path, index=False)
print(f"\nĐã tạo file '{parquet_path}' với {len(final_df)} dòng và {len(final_df.columns)} cột.")

# Tải file Parquet đã xử lý lên bucket 'curated-data'
print(f"Đang tải file Parquet lên bucket '{curated_bucket_name}'...")
try:
    minio_client.fput_object(
        curated_bucket_name,
        'ravdess_features.parquet',
        parquet_path
    )
    print("Tải file Parquet thành công!")
except S3Error as exc:
    print("Lỗi khi tải file Parquet:", exc)

# ----- KẾT THÚC -----