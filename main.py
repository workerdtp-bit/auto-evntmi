import csv
import pandas as pd
import datetime
import os
import time
import threading
import random
import re
import json
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# ================= CONFIG =================
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

processed = 0
total = 0
lock = threading.Lock()
csv_lock = threading.Lock()

SPREADSHEET_ID = "1tF_Oy6ZKJpNSAj9ElrUZIyHrw-ri4EPiESE_12CCHCw"
TARGET_SHEET = "upload"

# ================= DRIVER =================
def create_driver(driver_path):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver

# ================= SCRAPE =================
def scrape(driver, ma_kh):
    # Ghi nhận thời gian bắt đầu tra cứu cho mã này
    thoi_gian_tra_cuu = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _ in range(2):  # retry 2 lần nếu lỗi
        try:
            driver.get("https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien")

            input_el = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "idMaKhachHang"))
            )

            input_el.clear()
            input_el.send_keys(ma_kh)
            input_el.send_keys(Keys.RETURN)

            # Chờ bảng kết quả xuất hiện
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "idThongTinLichNgungGiamMaKhachHang"))
            )

            time.sleep(2) # Chờ dữ liệu render xong hoàn toàn

            content = driver.find_element(
                By.ID, "idThongTinLichNgungGiamMaKhachHang"
            ).text.strip()

            return {
                "Ma_KH": ma_kh,
                "Thoi_gian": thoi_gian_tra_cuu,
                "Noi_dung": content
            }

        except Exception:
            time.sleep(2)

    return {
        "Ma_KH": ma_kh,
        "Thoi_gian": thoi_gian_tra_cuu,
        "Noi_dung": "Lỗi: Không tìm thấy dữ liệu hoặc timeout"
    }

# ================= WORKER =================
def worker(data, driver_path, output):
    global processed
    driver = create_driver(driver_path)
    buffer = []

    try:
        for ma_kh in data:
            res = scrape(driver, ma_kh)
            buffer.append(res)

            with lock:
                processed += 1
                print(f"📊 {processed}/{total} | {ma_kh}", flush=True)

            if len(buffer) >= 5:
                write_csv(output, buffer)
                buffer = []

            time.sleep(random.uniform(1.5, 3))

        if buffer:
            write_csv(output, buffer)

    finally:
        driver.quit()

# ================= CSV =================
def write_csv(file, rows, mode='a', header=False):
    with csv_lock:
        with open(file, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Ma_KH", "Thoi_gian", "Noi_dung"]
            )
            if header:
                writer.writeheader()
            writer.writerows(rows)

# ================= PROCESS =================
def process(input_csv):
    print("🧹 Đang xử lý dữ liệu thô...")
    df = pd.read_csv(input_csv)
    rows = []

    for _, row in df.iterrows():
        text = str(row["Noi_dung"])
        tg_tra_cuu = row["Thoi_gian"]

        # Tìm tên khách hàng và địa chỉ (thường nằm ở đầu văn bản)
        kh_match = re.search(r"KHÁCH HÀNG:\s*(.+)", text)
        dc_match = re.search(r"ĐỊA CHỈ:\s*(.+)", text)

        kh = kh_match.group(1).strip() if kh_match else ""
        dc = dc_match.group(1).strip() if dc_match else ""

        # Chia nhỏ các block nếu một khách hàng có nhiều lịch cúp điện
        blocks = re.split(r"(?=MÃ.*?LỊCH)", text, flags=re.IGNORECASE)

        for b in blocks:
            ma = re.search(r"MÃ.*LỊCH:\s*(\d+)", b)
            # Regex bắt định dạng: từ 07g00 ngày 20/05/2024 đến 17g00 ngày 20/05/2024
            tg = re.search(r"từ (.+?) ngày (.+?) đến (.+?) ngày (.+)", b)
            lydo = re.search(r"LÝ DO.*:\s*(.+)", b)

            if ma and tg:
                rows.append([
                    row["Ma_KH"], kh, dc,
                    ma.group(1),
                    tg.group(2), tg.group(1),
                    tg.group(4), tg.group(3),
                    lydo.group(1).strip() if lydo else "",
                    tg_tra_cuu
                ])

    df2 = pd.DataFrame(rows, columns=[
        "Ma_KH", "Khach_hang", "Dia_chi",
        "Ma_lich", "Ngay_BD", "Gio_BD",
        "Ngay_KT", "Gio_KT", "Ly_do",
        "Thoi_gian_tra_cuu"
    ])

    # Lưu file Excel cục bộ
    df2.to_excel("output.xlsx", index=False)
    print("📁 Đã xuất file output.xlsx")
    
    # Upload lên Google Sheets
    upload_sheet(df2)

# ================= GOOGLE SHEETS =================
def upload_sheet(df):
    try:
        raw = os.getenv("GCP_JSON")
        if not raw:
            print("⚠️ Bỏ qua upload: Thiếu biến môi trường GCP_JSON")
            return

        raw = raw.replace("\\\\n", "\\n")
        info = json.loads(raw)
        info["private_key"] = info["private_key"].replace("\\n", "\n")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(info, scopes=scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sheet.worksheet(TARGET_SHEET)
        except WorksheetNotFound:
            ws = sheet.add_worksheet(title=TARGET_SHEET, rows="1000", cols="20")

        ws.clear()
        # Chuyển DataFrame thành list of lists để update
        data = [df.columns.tolist()] + df.astype(str).values.tolist()
        ws.update(range_name="A1", values=data)

        print("✅ Upload Google Sheets thành công!")

    except Exception as e:
        print("❌ Lỗi Google Sheets:", e)

# ================= MAIN =================
if __name__ == "__main__":
    file_input = "makh_list.csv"
    file_raw = "raw.csv"

    if not os.path.exists(file_input):
        print(f"❌ Không tìm thấy file {file_input}. Vui lòng chuẩn bị danh sách mã khách hàng.")
        sys.exit()

    with open(file_input, encoding="utf-8") as f:
        data = [r[0] for r in csv.reader(f) if r]

    total = len(data)
    print(f"🚀 Bắt đầu cào {total} mã khách hàng với 4 luồng...")

    driver_path = ChromeDriverManager().install()

    # Khởi tạo file raw mới với tiêu đề
    write_csv(file_raw, [], mode="w", header=True)

    threads = 4
    # Chia nhỏ data cho các luồng
    chunks = [data[i::threads] for i in range(threads)]

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, c, driver_path, file_raw) for c in chunks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"❌ Luồng gặp lỗi: {e}")

    # Xử lý file raw thành file kết quả cuối cùng
    if os.path.exists(file_raw):
        process(file_raw)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    print(f"🏁 HOÀN THÀNH trong {duration} giây.")
