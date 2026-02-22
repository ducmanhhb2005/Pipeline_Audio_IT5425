# Pipeline Xử Lý Dữ Liệu Âm Thanh RAVDESS

## Mô tả Dự Án

Dự án này triển khai một pipeline ETL (Extract, Transform, Load) hoàn chỉnh để xử lý dữ liệu âm thanh từ bộ dữ liệu RAVDESS (Ryerson Audio-Visual Database of Emotional Speech and Song). Pipeline bao gồm:

- **Extract**: Thu thập metadata từ các file âm thanh WAV
- **Transform**: Trích xuất đặc trưng âm thanh (MFCC, Chroma, Spectral Centroid, v.v.) sử dụng thư viện Librosa
- **Load**: Lưu trữ dữ liệu đã xử lý vào MinIO (data lake) và PostgreSQL (data warehouse)

## Cấu Trúc Dự Án

```
Pipeline_Audio_IT5425/
├── ravdess-emotional-speech-audio/     # Dữ liệu âm thanh gốc RAVDESS
│   ├── Actor_01/
│   ├── Actor_02/
│   └── ...
├── extract_load.py                     # Script extract metadata
├── transform.py                        # Script transform (pandas)
├── transform_pyspark.py                # Script transform (PySpark)
├── load_to_dw.py                       # Script load vào PostgreSQL
├── clean_data.csv                      # Dữ liệu đã làm sạch
├── ravdess_metadata.csv               # Metadata của dataset
├── spark_ravdess_features.csv         # Đặc trưng từ PySpark
└── README.md                          # Tài liệu này
```

## Yêu Cầu Hệ Thống

- Python 3.8+
- PostgreSQL
- MinIO Server
- Java 8+ (cho PySpark)

## Dependencies

Cài đặt các thư viện cần thiết:

```bash
pip install pandas librosa numpy minio sqlalchemy pyspark psycopg2-binary
```

## Cài Đặt và Chạy

### 1. Chuẩn Bị Dữ Liệu

- Tải bộ dữ liệu RAVDESS từ [kết nối chính thức](https://zenodo.org/record/1188976)
- Giải nén vào thư mục `ravdess-emotional-speech-audio/`

### 2. Khởi Động MinIO Server

```bash
# Sử dụng Docker
docker run -p 9000:9000 -p 9090:9090 \
  -e "MINIO_ACCESS_KEY=minioadmin" \
  -e "MINIO_SECRET_KEY=minioadmin" \
  minio/minio server /data
```

### 3. Khởi Động PostgreSQL

```bash
# Sử dụng Docker
docker run -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  postgres:13
```

### 4. Chạy Pipeline

#### Bước 1: Extract Metadata
```bash
python extract_load.py
```

#### Bước 2: Transform (Chọn một trong hai)
```bash
# Sử dụng pandas (cho dataset nhỏ)
python transform.py

# Sử dụng PySpark (cho dataset lớn, phân tán)
python transform_pyspark.py
```

#### Bước 3: Load vào Data Warehouse
```bash
python load_to_dw.py
```

## Đặc Trưng Âm Thanh Được Trích Xuất

- **Zero Crossing Rate**: Tỷ lệ zero crossing
- **Spectral Centroid**: Tâm phổ
- **Chroma Features**: 12 đặc trưng chroma
- **MFCCs**: 40 hệ số Mel-frequency cepstral
- **Mel Spectrogram**: Trung bình mel spectrogram

## Cấu Hình

Các cấu hình chính trong code:

- **MinIO**: localhost:9000 với access key/secret: minioadmin/minioadmin
- **PostgreSQL**: localhost:5432 với user/password: postgres/postgres
- **Buckets MinIO**:
  - `raw-audio`: File âm thanh gốc
  - `raw-metadata`: Metadata
  - `curated-data`: Dữ liệu đã xử lý

## Kết Quả

Sau khi chạy pipeline, bạn sẽ có:

- File `ravdess_features.parquet` trong MinIO bucket `curated-data`
- Bảng `audio_features` trong PostgreSQL chứa tất cả đặc trưng đã trích xuất

## Lưu Ý

- Đảm bảo đường dẫn đến thư mục dữ liệu âm thanh chính xác trong `extract_load.py`
- Có thể điều chỉnh cấu hình Spark trong `transform_pyspark.py` tùy theo tài nguyên hệ thống
- Pipeline được thiết kế để xử lý dataset RAVDESS, có thể mở rộng cho các dataset âm thanh khác

## Tác Giả

Dự án được phát triển cho môn học IT5425 - Quản trị dữ liệu và trực quan hóa