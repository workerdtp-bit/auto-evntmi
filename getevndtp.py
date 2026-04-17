import csv
import pandas as pd
import datetime
import os
import time
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


# =========================
# PROGRESS GLOBAL
# =========================
processed = 0
total = 0
progress_lock = threading.Lock()


# =========================
# 1. CREATE DRIVER
# =========================
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # 🔥 Ẩn log rác
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    service = Service(ChromeDriverManager().install())# chay online
    #service = Service(r"D:\chromedriver\chromedriver.exe") #chay LOCAL
    return webdriver.Chrome(service=service, options=chrome_options)


# =========================
# 2. ĐỌC FILE CSV
# =========================
def read_makh(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            return [row[0].strip() for row in reader if row and row[0].strip()]
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {filename}.")
        return []


# =========================
# 3. GHI CSV THREAD SAFE
# =========================
lock_file = threading.Lock()

def write_to_csv(filename, data, mode='a', header=False):
    fieldnames = ['Ma_KH', 'Thoi_gian_tra_cuu', 'Ket_qua']

    with lock_file:
        with open(filename, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if header:
                writer.writeheader()
            writer.writerows(data)


# =========================
# 4. CSV -> EXCEL
# =========================
def csv_to_excel(csv_filename, excel_filename):
    if not os.path.exists(csv_filename):
        print(f"Không tìm thấy {csv_filename}")
        return

    df = pd.read_csv(csv_filename, encoding='utf-8')
    df.to_excel(excel_filename, index=False)
    print(f"Xuất Excel thành công: {excel_filename}")


# =========================
# 5. SCRAPE
# =========================
def scrape_power_outage(driver, ma_kh):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')

        input_element = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.ID, 'idMaKhachHang'))
        )

        input_element.clear()
        input_element.send_keys(ma_kh)
        input_element.send_keys(Keys.RETURN)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, 'idThongTinLichNgungGiamMaKhachHang'))
        )

        WebDriverWait(driver, 30).until(
            lambda d: d.find_element(
                By.ID,
                'idThongTinLichNgungGiamMaKhachHang'
            ).get_attribute('innerHTML').strip() != ""
        )

        div = driver.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang')
        text_content = div.text.strip()

        result_text = "Lỗi hoặc không tìm thấy thông tin"

        tables = div.find_elements(By.TAG_NAME, 'table')

        if tables:
            rows = tables[0].find_elements(By.TAG_NAME, 'tr')
            all_results = []

            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, 'td')
                cols_text = [c.text.strip() for c in cols if c.text.strip()]
                if cols_text:
                    all_results.append(", ".join(cols_text))

            result_text = "; ".join(all_results) if all_results else "Không có lịch cắt điện"

        elif "không có lịch" in text_content.lower():
            result_text = text_content

        elif text_content:
            result_text = f"Thông báo từ hệ thống: {text_content}"

        return {
            'Ma_KH': ma_kh,
            'Thoi_gian_tra_cuu': current_time,
            'Ket_qua': result_text
        }

    except Exception as e:
        return {
            'Ma_KH': ma_kh,
            'Thoi_gian_tra_cuu': current_time,
            'Ket_qua': f"Lỗi tra cứu: {str(e)}"
        }


# =========================
# 6. RETRY CHUẨN
# =========================
def scrape_with_retry(driver, ma_kh, retries=3):
    for i in range(retries):
        result = scrape_power_outage(driver, ma_kh)

        ket_qua = result['Ket_qua'].lower()

        # ✅ chỉ chấp nhận khi có dữ liệu thật
        if not (
            ket_qua.startswith("lỗi") or
            "không tìm thấy" in ket_qua
        ):
            return result

        print(f"⚠️ Retry {i+1}/{retries} - {ma_kh}")
        time.sleep(1)

    return result


# =========================
# 7. SPLIT LIST
# =========================
def split_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


# =========================
# 8. WORKER
# =========================
def worker(ma_kh_list, thread_id, output_csv):
    global processed

    driver = create_driver()

    try:
        for ma_kh in ma_kh_list:
            print(f"\n[Thread {thread_id}] Đang xử lý: {ma_kh}")

            result = scrape_with_retry(driver, ma_kh, 3)

            with progress_lock:
                processed += 1
                print(f"📊 TIẾN ĐỘ: {processed}/{total} ({processed/total*100:.1f}%)")

            print("====================================")
            print(f"📌 MÃ KH: {result['Ma_KH']}")
            print(f"⏰ THỜI GIAN: {result['Thoi_gian_tra_cuu']}")
            print("📄 KẾT QUẢ:")
            print(result['Ket_qua'])
            print("====================================\n")

            write_to_csv(output_csv, [result])

            # 🔥 delay chống block
            time.sleep(1.2)

    finally:
        driver.quit()


# =========================
# 9. MAIN (6 LUỒNG)
# =========================
if __name__ == '__main__':

    input_file = "makh_list.csv"
    output_csv = "datasauget.csv"
    output_xlsx = "datasauget.xlsx"

    ma_kh_list = read_makh(input_file)

    if not ma_kh_list:
        exit()

    total = len(ma_kh_list)
    print(f"🚀 Tổng số mã: {total}")

    write_to_csv(output_csv, [], mode='w', header=True)

    threads = 6
    chunks = split_list(ma_kh_list, threads)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []

        for i, chunk in enumerate(chunks, start=1):
            futures.append(executor.submit(worker, chunk, i, output_csv))

        for f in as_completed(futures):
            f.result()

    csv_to_excel(output_csv, output_xlsx)

    print("\n✅ HOÀN THÀNH")
