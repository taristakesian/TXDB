import os
import json
import sqlite3
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from bs4 import BeautifulSoup
from json import JSONDecodeError
import base64

# --- Impor undetected-chromedriver ---
# Pastikan Anda telah menginstalnya: pip install undetected-chromedriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv

load_dotenv()

SP_CLIENT_ID = os.getenv("SKINPORT_CLIENT_ID", "")
SP_CLIENT_SECRET = os.getenv("SKINPORT_CLIENT_SECRET", "")

# -------- LOGS --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("market")

# -------- CONFIG --------
DB_PATH = os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db")
CURRENCY = os.getenv("CURRENCY", "EUR")
APP_ID = int(os.getenv("APP_ID", "730"))  # CS2
STEAM_COUNTRY = os.getenv("STEAM_COUNTRY", "RU")  # untuk pricehistory
STEAM_CURRENCY = int(os.getenv("STEAM_CURRENCY", "5"))  # 5=RUB, 3=EUR, 1=USD dll.
# Untuk Steam pricehistory, diperlukan cookie (akun). Letakkan semuanya dalam satu baris:
# contoh: "sessionid=...; steamLoginSecure=...; ..."
STEAM_COOKIE = os.getenv("STEAM_COOKIE", "")


# -------- DATABASE --------
class Store:
    def __init__(self, path: str = DB_PATH):
        os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._create_schema()

    def _create_schema(self):
        cur = self.conn.cursor()
        # Rangkaian saat ini (etalase + meta)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS skinport_items (
            market_hash_name TEXT,
            currency TEXT,
            suggested_price REAL,
            item_page TEXT,
            market_page TEXT,
            min_price REAL,
            max_price REAL,
            mean_price REAL,
            median_price REAL,
            quantity INTEGER,
            created_at INTEGER,
            updated_at INTEGER,
            image_url TEXT,
            PRIMARY KEY (market_hash_name, currency)
        )
        """)
        # Riwayat yang TIDAK diagregasi (tick/harian) dari Steam pricehistory (atau sumber lain)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_trades (
            market_hash_name TEXT,
            app_id INTEGER,
            ts_utc TEXT,      -- ISO8601 (UTC)
            price REAL,       -- harga per unit dalam mata uang source_currency
            amount INTEGER,   -- jumlah item pada titik waktu (biasanya volume per interval)
            source TEXT,      -- 'steam_pricehistory' | 'skinport_agg_expand' | 'other'
            source_currency TEXT,
            raw JSON
        )
        """)
        # Riwayat agregat Skinport (kita simpan sebagai referensi)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS skinport_sales_agg (
            market_hash_name TEXT,
            currency TEXT,
            item_page TEXT,
            market_page TEXT,
            period TEXT,
            min REAL, max REAL, avg REAL, median REAL, volume INTEGER,
            extracted_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (market_hash_name, currency, period)
        )
        """)
        # Foto/gambar
        cur.execute("""
        CREATE TABLE IF NOT EXISTS item_images (
            market_hash_name TEXT,
            image_url TEXT,
            source TEXT,     -- 'og_image' | 'steam_image' | ...
            extracted_at TEXT DEFAULT (datetime('now'))
        )
        """)
        self.conn.commit()

    def upsert_skinport_items(self, items: List[Dict[str, Any]]):
        cur = self.conn.cursor()
        sql = """
        INSERT INTO skinport_items
        (market_hash_name,currency,suggested_price,item_page,market_page,
         min_price,max_price,mean_price,median_price,quantity,created_at,updated_at,image_url)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(market_hash_name, currency) DO UPDATE SET
            suggested_price=excluded.suggested_price,
            item_page=excluded.item_page,
            market_page=excluded.market_page,
            min_price=excluded.min_price,
            max_price=excluded.max_price,
            mean_price=excluded.mean_price,
            median_price=excluded.median_price,
            quantity=excluded.quantity,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at
        """
        rows = [(
            it.get("market_hash_name"), it.get("currency"), it.get("suggested_price"),
            it.get("item_page"), it.get("market_page"),
            it.get("min_price"), it.get("max_price"), it.get("mean_price"), it.get("median_price"),
            it.get("quantity"), it.get("created_at"), it.get("updated_at"), it.get("image_url"),
        ) for it in items]
        cur.executemany(sql, rows)
        self.conn.commit()

    def upsert_skinport_sales_agg(self, stats: List[Dict[str, Any]]):
        cur = self.conn.cursor()
        sql = """
        INSERT INTO skinport_sales_agg
        (market_hash_name,currency,item_page,market_page,period,min,max,avg,median,volume)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(market_hash_name, currency, period) DO UPDATE SET
            min=excluded.min, max=excluded.max, avg=excluded.avg, median=excluded.median, volume=excluded.volume
        """
        rows = []
        for s in stats:
            for period_key in ("last_24_hours", "last_7_days", "last_30_days", "last_90_days"):
                p = s.get(period_key) or {}
                rows.append((s.get("market_hash_name"), s.get("currency"),
                             s.get("item_page"), s.get("market_page"),
                             period_key, p.get("min"), p.get("max"), p.get("avg"),
                             p.get("median"), p.get("volume")))
        cur.executemany(sql, rows)
        self.conn.commit()

    def insert_sales_trades(self, rows: List[Tuple]):
        cur = self.conn.cursor()
        cur.executemany("""
            INSERT INTO sales_trades
            (market_hash_name, app_id, ts_utc, price, amount, source, source_currency, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        self.conn.commit()

    def insert_images(self, rows: List[Tuple]):
        cur = self.conn.cursor()
        cur.executemany("""
            INSERT INTO item_images (market_hash_name, image_url, source)
            VALUES (?, ?, ?)
        """, rows)
        self.conn.commit()

# -------- HTTP --------
# MODIFIED: Kelas ini sekarang menggunakan undetected-chromedriver
class JsonSession:
    """
    Sesi yang menggunakan undetected-chromedriver untuk membuat permintaan
    dan mengurai JSON dari respons.
    """
    def __init__(self):
        # Header ini akan digunakan oleh requests untuk og:image,
        # tetapi tidak secara langsung oleh Selenium.
        self.headers = {
            "User-Agent": "cs2-etl/1.0 (+github.com/you)",
            "Accept": "application/json",
        }
        # Inisialisasi driver browser sekali dan gunakan kembali jika memungkinkan,
        # tetapi untuk kesederhanaan, kita akan membuat instance baru untuk setiap permintaan.
        # Untuk performa yang lebih baik, Anda dapat mengelola satu instance driver.
        pass

    def get_json(self, url: str, params: Optional[Dict] = None, **kwargs) -> Any:
        driver = None
        # Membuat URL dengan parameter
        if params:
            from urllib.parse import urlencode
            url += '?' + urlencode(params)

        try:
            log.info(f"Memulai driver Chrome untuk URL: {url}")
            options = uc.ChromeOptions()
            # Opsi headless sering kali lebih mudah dideteksi, jadi gunakan dengan hati-hati.
            # options.add_argument('--headless')
            # options.add_argument('--disable-gpu')
            driver = uc.Chrome(options=options)

            driver.get(url)
            
            # API sering kali mengembalikan JSON dalam tag <pre>
            # Tunggu hingga elemen tersebut ada
            wait = WebDriverWait(driver, 20) # Tunggu hingga 20 detik
            pre_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
            
            json_text = pre_element.text
            return json.loads(json_text)
            
        except Exception as e:
            log.error(f"Gagal mengambil atau mengurai JSON dari {url}: {e}")
            if driver:
                # Untuk debugging, Anda bisa menyimpan tangkapan layar atau sumber halaman
                # driver.save_screenshot('error_screenshot.png')
                # with open('error_page_source.html', 'w', encoding='utf-8') as f:
                #     f.write(driver.page_source)
                pass
            raise
        finally:
            if driver:
                log.info("Menutup driver Chrome.")
                driver.quit()
    
    # Tetap gunakan requests untuk permintaan sederhana yang tidak memerlukan browser
    def get(self, url, **kwargs):
        return requests.get(url, **kwargs)


# -------- Klien --------
class SkinportClient:
    BASE = "https://api.skinport.com/v1"

    def __init__(self):
        self.s = JsonSession()
        # Jika kredensial diberikan — tambahkan Otorisasi: Dasar ...
        # Catatan: Header ini tidak akan digunakan oleh Selenium secara default.
        # Jika API memerlukan header, kita perlu mencari cara untuk menambahkannya
        # melalui Selenium (misalnya, melalui proksi atau ekstensi).
        if SP_CLIENT_ID and SP_CLIENT_SECRET:
            b64 = base64.b64encode(f"{SP_CLIENT_ID}:{SP_CLIENT_SECRET}".encode()).decode()
            self.s.headers["Authorization"] = f"Basic {b64}"

    def get_items(self, app_id: int = APP_ID, currency: str = CURRENCY,
                  tradable: Optional[bool] = None):
        p = {"app_id": app_id, "currency": currency}
        if tradable is True:  # hanya 1 atau tidak ada
            p["tradable"] = 1
        return self.s.get_json(f"{self.BASE}/items", params=p)

    def get_sales_history_agg(self, app_id: int = APP_ID, currency: str = CURRENCY,
                              market_hash_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        p = {"app_id": app_id, "currency": currency}
        if market_hash_names:
            p["market_hash_name"] = ",".join(market_hash_names)
        return self.s.get_json(f"{self.BASE}/sales/history", params=p)

    def get_item_og_image(self, item_page_url: str) -> Optional[str]:
        # Permintaan ini lebih sederhana dan cenderung tidak diblokir,
        # jadi kita dapat terus menggunakan `requests` di sini untuk efisiensi.
        try:
            r = self.s.get(item_page_url, timeout=20, headers={
                "User-Agent": "Mozilla/5.0 (compatible; cs2-etl/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            })
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            tag = soup.find("meta", property="og:image")
            if tag and tag.get("content"):
                return tag["content"]
        except Exception as e:
            log.debug(f"og:image tidak ditemukan: {e}")
        return None

# MODIFIKASI TIDAK DIPERLUKAN DI SINI, tetapi bisa diubah jika perlu
class SteamMarketClient:
    """
    Harga/volume yang TIDAK diagregasi dari Steam pricehistory.
    Memerlukan cookie yang diotorisasi (STEAM_COOKIE) dan market_hash_name.
    """
    BASE = "https://steamcommunity.com/market/pricehistory/"

    def __init__(self, cookie: str = STEAM_COOKIE):
        self.cookie = cookie
        # Kita dapat mengganti ini untuk menggunakan undetected-chromedriver juga jika diperlukan
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; cs2-etl/1.0)",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://steamcommunity.com/market/",
        })
        if cookie:
            # diteruskan sebagai string cookie
            self.s.headers.update({"Cookie": cookie})

    @retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
    def price_history(self, app_id: int, market_hash_name: str,
                      country: str, currency: int) -> Dict[str, Any]:
        params = {
            "appid": app_id,
            "market_hash_name": market_hash_name,
            "country": country,
            "currency": currency,
        }
        # Untuk saat ini, kita biarkan `requests`. Jika Steam mulai memblokir,
        # kita dapat menerapkan logika Selenium yang serupa seperti di JsonSession.
        r = self.s.get(self.BASE, params=params, timeout=60)
        r.raise_for_status()
        # terkadang datang text/plain — parsing secara manual
        try:
            return r.json()
        except Exception:
            return json.loads(r.text)

# --- Contoh Penggunaan (opsional) ---
if __name__ == '__main__':
    log.info("Memulai proses ETL.")
    db = Store()
    skinport_client = SkinportClient()
    steam_client = SteamMarketClient()

    # 1. Mengambil item dari Skinport
    log.info("Mengambil item dari Skinport...")
    try:
        sp_items = skinport_client.get_items(tradable=True)
        log.info(f"Diterima {len(sp_items)} item dari Skinport.")
        if sp_items:
            db.upsert_skinport_items(sp_items)
            log.info("Item Skinport berhasil disimpan ke database.")
            
            # Ambil beberapa nama item untuk riwayat penjualan
            sample_names = [item['market_hash_name'] for item in sp_items[:5]]
            
            # 2. Mengambil riwayat penjualan agregat dari Skinport
            log.info(f"Mengambil riwayat penjualan agregat untuk: {sample_names}")
            agg_history = skinport_client.get_sales_history_agg(market_hash_names=sample_names)
            if agg_history:
                db.upsert_skinport_sales_agg(agg_history)
                log.info("Riwayat penjualan agregat Skinport berhasil disimpan.")

            # 3. Mengambil riwayat harga dari Steam untuk item pertama
            if sample_names and STEAM_COOKIE:
                item_name = sample_names[0]
                log.info(f"Mengambil riwayat harga Steam untuk '{item_name}'...")
                steam_history = steam_client.price_history(
                    app_id=APP_ID,
                    market_hash_name=item_name,
                    country=STEAM_COUNTRY,
                    currency=STEAM_CURRENCY
                )
                if steam_history and steam_history.get('success') and steam_history.get('prices'):
                    log.info(f"Diterima {len(steam_history['prices'])} titik data dari riwayat harga Steam.")
                    # (Di sini Anda akan menambahkan logika untuk memasukkan data ini ke tabel sales_trades)
                else:
                    log.warning("Tidak dapat mengambil riwayat harga Steam.")

    except Exception as e:
        log.error(f"Terjadi kesalahan selama proses ETL: {e}", exc_info=True)