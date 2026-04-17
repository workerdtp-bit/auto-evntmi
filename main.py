import csv
import pandas as pd
import datetime
import os
import time
import threading
import random

from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ================= CONFIG =================
THREADS = 4
RETRIES = 3
SHEET_NAME = "auto-evn"

OUTPUT_CSV = "data.csv"
OUTPUT_XLSX = "data.xlsx"


# ================= GLOBAL =================
processed = 0
total = 0
lock = threading.Lock()


# ================= GOOGLE SHEET =================
def connect_gsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    return gspread.authorize(creds)


def read_makh():
    client = connect_gsheet()
    sheet = client.open(SHEET_NAME).sheet1
    data = sheet.col_values(1)
    return [x.strip() for x in data if x.strip() and x != "Ma_KH"]


def write_gsheet(rows):
    client = connect_gsheet()

    try:
        ws = client.open(SHEET_NAME).worksheet("output")
    except:
        ws = client.open(SHEET_NAME).add_worksheet(title="output", rows="1000", cols="10")

    data = [[r['Ma_KH'], r['Thoi_gian'], r['Ket_qua']] for r in rows]
    ws.append_rows(data)


# ================= DRIVER =================
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    try:
        if os.path.exists(r"D:\chromedriver\chromedriver.exe"):
            service = Service(r"D:\chromedriver\chromedriver.exe")
        else:
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(40)
        return driver

    except Exception as e:
        print("❌ Driver error:", e)
        raise


# ================= CSV =================
file_lock = threading.Lock()

def write_csv(data, mode='a', header=False):
    with file_lock:
        with open(OUTPUT_CSV, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['Ma_KH','Thoi_gian','Ket_qua'])
            if header:
                writer.writeheader()
            writer.writerows(data)


def export_excel():
    df = pd.read_csv(OUTPUT_CSV)
    df.to_excel(OUTPUT_XLSX, index=False)


# ================= LOGIC =================
def is_valid(text):
    t = text.lower()
    return not ("lỗi" in t or "không tìm thấy" in t or t.strip() == "")


def scrape(driver, ma_kh):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')

        ip = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.ID, 'idMaKhachHang'))
        )

        ip.clear()
        ip.send_keys(ma_kh)
        ip.send_keys(Keys.RETURN)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, 'idThongTinLichNgungGiamMaKhachHang'))
        )

        div = driver.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang')
        text = div.text.strip()

        tables = div.find_elements(By.TAG_NAME, 'table')

        if tables:
            rows = tables[0].find_elements(By.TAG_NAME, 'tr')
            data = []

            for r in rows[1:]:
                cols = [c.text.strip() for c in r.find_elements(By.TAG_NAME, 'td') if c.text.strip()]
                if cols:
                    data.append(", ".join(cols))

            result = "; ".join(data) if data else "Không có lịch"

        else:
            result = text or "Không có dữ liệu"

        return {'Ma_KH': ma_kh, 'Thoi_gian': now, 'Ket_qua': result}

    except Exception as e:
        return {'Ma_KH': ma_kh, 'Thoi_gian': now, 'Ket_qua': f"Lỗi: {e}"}


def scrape_retry(driver, ma_kh):
    for i in range(RETRIES):
        r = scrape(driver, ma_kh)
        if is_valid(r['Ket_qua']):
            return r

        time.sleep(2 + i)

    return r


def worker(data):
    global processed

    driver = create_driver()

    try:
        for ma_kh in data:
            r = scrape_retry(driver, ma_kh)

            with lock:
                processed += 1
                print(f"[{processed}/{total}] {ma_kh}")

            write_csv([r])
            write_gsheet([r])

            time.sleep(random.uniform(1.5, 3))

    finally:
        driver.quit()


# ================= MAIN =================
if __name__ == "__main__":

    data = read_makh()

    if not data:
        print("❌ Không có dữ liệu")
        exit()

    total = len(data)
    print(f"🚀 Tổng: {total}")

    write_csv([], mode='w', header=True)

    chunks = [data[i::THREADS] for i in range(THREADS)]

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = [ex.submit(worker, chunk) for chunk in chunks]

        for f in as_completed(futures):
            f.result()

    export_excel()

    print("✅ DONE")
