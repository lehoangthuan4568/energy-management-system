<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=influxdb&logoColor=white" />
  <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
</p>

<h1 align="center">⚡ Energy Management System</h1>

<p align="center">
  <strong>Hệ thống Quản lý Năng lượng Thông minh dựa trên nền tảng Big Data</strong><br/>
  <em>Simulated Streaming Analytics Pipeline — Kappa Architecture</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-prototype-blue?style=flat-square" />
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" />
  <img src="https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square" />
  <img src="https://img.shields.io/badge/spark-3.5.0-orange?style=flat-square" />
</p>

---

## 📋 Mục lục

- [Giới thiệu](#-giới-thiệu)
- [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
- [Công nghệ sử dụng](#-công-nghệ-sử-dụng)
- [Tính năng chính](#-tính-năng-chính)
- [Cài đặt & Khởi chạy](#-cài-đặt--khởi-chạy)
- [Cấu trúc dự án](#-cấu-trúc-dự-án)
- [Dashboard & Giao diện](#-dashboard--giao-diện)
- [Kịch bản Demo](#-kịch-bản-demo)
- [Nguồn dữ liệu](#-nguồn-dữ-liệu)
- [Tác giả](#-tác-giả)

---

## 🎯 Giới thiệu

**Energy Management System (EMS)** là một hệ thống phân tích luồng dữ liệu năng lượng theo kiến trúc **Kappa**, được xây dựng như một bản nguyên mẫu (Prototype) cho đồ án môn học **"Công nghệ Phân tích Dữ liệu Lớn"**.

Hệ thống mô phỏng toàn bộ quy trình từ **thu thập dữ liệu** tại thiết bị biên → **xử lý luồng** theo thời gian gần thực → **tính toán hóa đơn EVN** bậc thang → **cảnh báo quá tải** → **dự báo AI** → **trực quan hóa Dashboard**, tất cả chạy trên máy cục bộ qua Docker Container.

### Bài toán giải quyết

| Vấn đề hiện trạng | Giải pháp EMS |
|---|---|
| Đồng hồ điện chỉ cho tổng kWh cuối tháng | Thu thập & phân tích từng giây, biết chi tiết mọi thời điểm |
| Không biết thiết bị nào "ngốn" điện nhất | Phân khu phụ tải (Bếp, Giặt, Nước nóng) hiển thị tỷ lệ % |
| Quá tải trạm biến áp mới biết khi mất điện | Cảnh báo 3 cấp (INFO → WARNING → CRITICAL) tức thì |
| Hóa đơn bất ngờ cuối tháng | Tính tiền lũy kế theo bậc thang EVN liên tục |
| Không dự đoán được mức tiêu thụ tương lai | AI Holt-Winters dự báo 7 bước kèm dải tin cậy |

---

## 🏗 Kiến trúc hệ thống

```
┌──────────────┐     ┌──────────────┐     ┌────────────────────────────────┐
│  📄 Dataset  │────▶│  🐍 Producer │────▶│       Apache Kafka             │
│  UCI (CSV)   │     │  producer.py │     │  Topic: smart_home_power       │
└──────────────┘     └──────────────┘     └───────────────┬────────────────┘
                                                          │
                                                          ▼
                                          ┌───────────────────────────────┐
                                          │  ✨ Spark Structured Streaming │
                                          │  ┌─────────────────────────┐  │
                                          │  │ JSON Parse → Schema     │  │
                                          │  │ Stateful: kWh lũy kế   │  │
                                          │  │ EVN 6 bậc → VND        │  │
                                          │  │ Alert Classification    │  │
                                          │  └─────────────────────────┘  │
                                          └───────────────┬───────────────┘
                                                          │
                     ┌────────────────┐                   ▼
                     │ 🤖 AI Module   │         ┌─────────────────┐
                     │ Holt-Winters   │────────▶│  🗄️ InfluxDB    │
                     │ Dự báo 7 bước  │         │  energy_db      │
                     └────────────────┘         └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  📊 Grafana     │
                                                │  Dashboard 3T   │
                                                │  localhost:3000  │
                                                └─────────────────┘
```

---

## 🛠 Công nghệ sử dụng

| Thành phần | Công nghệ | Phiên bản | Vai trò |
|-----------|-----------|-----------|---------|
| Message Broker | Apache Kafka | 7.5.0 (Confluent) | Hàng đợi tin nhắn phân tán |
| Stream Processing | Apache Spark | 3.5.0 (PySpark) | Xử lý Micro-batch 2 giây |
| Time-Series DB | InfluxDB | 1.8 | Lưu trữ dữ liệu chuỗi thời gian |
| Visualization | Grafana | 10.2.0 | Dashboard 3 tầng |
| AI Forecasting | Statsmodels | ≥ 0.13 | Holt-Winters Exponential Smoothing |
| Containerization | Docker Compose | 3.8 | Đóng gói toàn bộ hạ tầng |
| Language | Python | 3.10+ | Producer, Spark job, AI module |

---

## ✨ Tính năng chính

### 📡 Thu thập dữ liệu mô phỏng (Simulated Streaming)
- Đọc Dataset UCI (~2 triệu bản ghi) và phát luồng qua Kafka
- Hỗ trợ **Anomaly Trigger** (nhấn `A`) để giả lập quá tải x10

### ⚡ Xử lý luồng Stateful
- Micro-batch mỗi 2 giây với Spark Structured Streaming
- Tính toán **hóa đơn EVN 6 bậc thang** lũy kế liên tục
- Phân loại cảnh báo 3 cấp: `INFO` / `WARNING` / `CRITICAL`
- **Khôi phục trạng thái** tự động sau sự cố ngắt Spark

### 🤖 Dự báo AI (Holt-Winters)
- Phân tách 3 thành phần: Level, Trend, Seasonality
- Dự báo 7 bước thời gian với dải tin cậy (Confidence Band)
- Cập nhật liên tục mỗi 60 giây

### 📊 Dashboard 3 Tầng
| Tầng | Nội dung | Đối tượng |
|------|---------|-----------|
| Tầng 1 | Tiền điện VND, Peak, tổng kWh, Gauge | Quản lý |
| Tầng 2 | Biểu đồ kW, Doughnut phân khu, Alert Log | Kỹ thuật |
| Tầng 3 | Đồ thị dự báo AI + dải tin cậy | Phân tích |

---

## 🚀 Cài đặt & Khởi chạy

### Yêu cầu hệ thống

- **Docker Desktop** (Windows/Mac/Linux) — [Tải tại đây](https://www.docker.com/products/docker-desktop/)
- **Python 3.10+** — [Tải tại đây](https://www.python.org/downloads/)
- **Java JDK 11+** (cho PySpark) — [Tải tại đây](https://adoptium.net/)
- RAM tối thiểu: **8GB** (khuyến nghị 16GB)

### Bước 1: Clone repository

```bash
git clone https://github.com/<your-username>/energy-management-system.git
cd energy-management-system
```

### Bước 2: Tải Dataset

```bash
python download_data.py
```
> Script tự động tải Dataset UCI (~25MB zip) và giải nén thành CSV (~130MB) vào thư mục `data/`.

### Bước 3: Cài đặt thư viện Python

```bash
pip install -r requirements.txt
```

### Bước 4: Khởi động hạ tầng Docker

```bash
docker-compose up -d
```
> Đợi khoảng 60 giây để Zookeeper, Kafka, InfluxDB và Grafana khởi động hoàn tất.

### Bước 5: Chạy hệ thống

**Cách nhanh (Windows):**
```bash
start_ems.bat
```

**Hoặc chạy thủ công (mở 3 terminal riêng biệt):**
```bash
# Terminal 1 - Producer
python producer/producer.py

# Terminal 2 - Spark Streaming (đợi 3s sau Producer)
python run_spark.py

# Terminal 3 - AI Forecast (đợi 5s sau Spark)
python prediction/predict_power.py
```

### Bước 6: Mở Dashboard

Truy cập **http://localhost:3000** → Đăng nhập `admin` / `admin` → Chọn **EMS Dashboard**.

---

## 📁 Cấu trúc dự án

```
energy-management-system/
│
├── producer/
│   └── producer.py              # Giả lập thiết bị đo, phát luồng Kafka
│
├── streaming/
│   └── spark_streaming.py       # Logic xử lý Spark (kWh, EVN, Alert)
│
├── prediction/
│   └── predict_power.py         # AI Holt-Winters dự báo năng lượng
│
├── grafana/
│   └── provisioning/            # Auto-provisioning datasource & dashboard
│       ├── datasources/         # Cấu hình kết nối InfluxDB
│       └── dashboards/          # Dashboard JSON template
│
├── docker-compose.yml           # Định nghĩa 4 services Docker
├── run_spark.py                 # Entry point cho Spark job
├── download_data.py             # Script tải Dataset UCI
├── start_ems.bat                # Khởi chạy nhanh (Windows)
├── requirements.txt             # Thư viện Python
└── README.md                    # Tài liệu này
```

---

## 🖥 Dashboard & Giao diện

> **Lưu ý:** Chụp ảnh màn hình Dashboard khi hệ thống đang chạy và thay thế vào đây.

### Tầng 1 — Tổng hợp Tài chính
Hiển thị: Tiền điện VND lũy kế • Mức giá cao nhất • Tổng kWh • Gauge công suất

### Tầng 2 — Phân tích Kỹ thuật
Hiển thị: Biểu đồ kW real-time • Phân khu phụ tải (Doughnut) • Alert Log

### Tầng 3 — Dự báo AI
Hiển thị: Đường dự báo Holt-Winters (tím) • Dải tin cậy (bóng mờ)

---

## 🎮 Kịch bản Demo

### 💥 Tạo sự kiện quá tải
Trên cửa sổ **Producer**, nhập `A` + Enter:
```
>> Nhap lenh (A = Anomaly, Q = Quit): A
[ANOMALY] Da gui 5 ban ghi cong suat x10!
```
→ Quan sát Dashboard: biểu đồ kW tăng vọt, vùng đỏ cảnh báo bật sáng, Alert Log ghi nhận CRITICAL.

### 🔄 Kiểm thử Khôi phục trạng thái
1. Để hệ thống chạy một lúc (tích lũy kWh và VND).
2. Đóng cửa sổ Spark (Ctrl+C).
3. Chạy lại `python run_spark.py`.
4. Kiểm tra: giá trị kWh và VND tiếp tục từ mốc cũ, không reset về 0.

---

## 📊 Nguồn dữ liệu

| Thuộc tính | Giá trị |
|-----------|---------|
| Tên | Individual Household Electric Power Consumption |
| Nguồn | [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption) |
| Số bản ghi | ~2,075,259 |
| Khoảng thời gian | 12/2006 — 11/2010 (4 năm) |
| Tần suất | 1 phút / bản ghi |
| Các trường chính | Global_active_power, Sub_metering_1,2,3 |

---

## 👥 Tác giả

| Thành viên | Vai trò |
|-----------|---------|
| [Tên thành viên 1] | Thiết kế Pipeline, Spark Streaming |
| [Tên thành viên 2] | Producer, Kafka, Docker |
| [Tên thành viên 3] | AI Forecast, Grafana Dashboard |

> 📚 Đồ án môn học: **Công nghệ Phân tích Dữ liệu Lớn**

---

## 📄 License

Dự án này được phát hành dưới giấy phép [MIT License](LICENSE).

---

<p align="center">
  <strong>⚡ Built with Big Data Technologies ⚡</strong><br/>
  <em>Kafka → Spark → InfluxDB → Grafana</em>
</p>
