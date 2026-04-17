# ⚡ EVN SPC Power Outage Scraper

Công cụ tự động tra cứu lịch ngừng giảm cung cấp điện tại khu vực miền Nam (EVN SPC).

## 🚀 Tính năng
- **Đa luồng (Multi-threading):** Chạy nhiều mã khách hàng cùng lúc để tiết kiệm thời gian.
- **Tự động hóa:** Sử dụng Selenium kết hợp với WebDriver Manager tự động quản lý trình duyệt.
- **Xuất dữ liệu đa dạng:** Kết quả được lưu dưới dạng file CSV và tự động chuyển đổi sang Excel (.xlsx).
- **Chống chặn (Anti-bot):** Cơ chế delay ngẫu nhiên và giả lập User-Agent.

## 🛠️ Hướng dẫn sử dụng
1. Tải toàn bộ code về máy hoặc `git clone`.
2. Cài đặt các thư viện cần thiết:
   ```bash
   pip install -r requirements.txt
