# 🌤️ Vietnam Weather Data Pipeline with Airflow

Pipeline ETL thu thập **dữ liệu thời tiết của 63 tỉnh/thành Việt Nam** mỗi **3 giờ** từ **OpenWeather API**, sử dụng **Apache Airflow 2.10.2**, **Docker Compose**, **PostgreSQL** và **MinIO**.

---

## 📌 Mục tiêu dự án
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

## 🧰 Công nghệ sử dụng
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

## 📁 Cấu trúc thư mục
Weather_Pipeline/
│── dags/
│── Weather_Pipeline.py
│── docker-compose.yaml
│── requirements.txt
│── .env
│── README.md


---

## ⚙️ Cài đặt & chạy dự án

1. **Clone repository**
```bash
git clone https://github.com/datbui-280804/Weather_Pipeline.git
cd Weather_Pipeline

2. **Build & start containers**
docker-compose up -d

3. **Khởi tạo Airflow database & user**
docker-compose run --rm airflow-init

4. **Truy cập các dịch vụ**
Apache Airflow

- URL: http://localhost:8080

- Username: airflow

- Password: airflow

MinIO

- URL: http://localhost:9001

- Access Key: ROOTUSER

- Secret Key: CHANGEME123

5. **Thiết lập schedule**
schedule_interval="0 */3 * * *"

6. **Dừng hệ thống**
docker-compose down