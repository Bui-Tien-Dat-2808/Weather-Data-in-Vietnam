# 1. 🌤️ Vietnam Weather Data Pipeline with Airflow

Pipeline ETL thu thập **dữ liệu thời tiết của 63 tỉnh/thành Việt Nam** mỗi **3 giờ** từ **OpenWeather API**, sử dụng **Apache Airflow 2.10.2**, **Docker Compose**, **PostgreSQL** và **MinIO**.

---

## 1.1. 📌 Mục tiêu dự án
- Thu thập dữ liệu thời tiết:
  - Nhiệt độ
  - Độ ẩm
  - Tốc độ gió
  - Áp suất
  - Trạng thái thời tiết
- Lưu **raw data (JSON)** vào **MinIO**
- Lưu **dữ liệu đã xử lý** vào **PostgreSQL**
- Orchestrate pipeline bằng **Airflow**
- Chạy tự động **mỗi 3 giờ**

---


---

## 1.2. 🧰 Công nghệ sử dụng
| Công nghệ | Phiên bản |
|---------|-----------|
| Apache Airflow | 2.10.2 |
| Python | 3.11 |
| Docker & Docker Compose | Latest |
| PostgreSQL | 15 |
| MinIO | Latest |
| OpenWeather API | Current Weather API |
| DBeaver | DB Client |

---

## 1.3. 📁 Cấu trúc thư mục
```
Weather_Pipeline/
│── dags/
│── Weather_Pipeline.py
│── docker-compose.yaml
│── requirements.txt
│── .env
│── README.md
```

---

## 1.4. ⚙️ Cài đặt & chạy dự án

### 1.4.1. Build & start containers
```bash
docker-compose up -d
```
### 1.4.2. Khởi tạo Airflow database & user
```bash
docker-compose run --rm airflow-init
```

### 1.4.3. Truy cập các dịch vụ
#### 1.4.3.1. Apache Airflow

- URL: http://localhost:8080

- Username: airflow

- Password: airflow

#### 1.4.3.2. MinIO

- URL: http://localhost:9001

- Access Key: ROOTUSER

- Secret Key: CHANGEME123

### 1.4.4. Thiết lập schedule
```bash
schedule_interval="0 */3 * * *"
```

### 1.4.5. Dừng hệ thống
```bash
docker-compose down
```