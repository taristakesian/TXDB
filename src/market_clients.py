import os, json, sqlite3, logging, time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from bs4 import BeautifulSoup

# -------- ЛОГИ --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("market")

# -------- КОНФИГ --------
DB_PATH = os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db")
CURRENCY = os.getenv("CURRENCY", "EUR")
APP_ID = int(os.getenv("APP_ID", "730"))  # CS2
STEAM_COUNTRY = os.getenv("STEAM_COUNTRY", "RU")  # для pricehistory
STEAM_CURRENCY = int(os.getenv("STEAM_CURRENCY", "5"))  # 5=RUB, 3=EUR, 1=USD и т.п.
# Для Steam pricehistory нужен cookie (учётка). Положи всё в одну строку:
# пример: "sessionid=...; steamLoginSecure=...; ..."
STEAM_COOKIE = os.getenv("STEAM_COOKIE", "")

# -------- БАЗА --------
class Store:
    def __init__(self, path: str = DB_PATH):
        os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._create_schema()

    def _create_schema(self):
        cur = self.conn.cursor()
        # Текущий ассортимент (витрина + мета)
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
        # НЕагрегированная история (tick/суточная) из Steam pricehistory (или иного источника)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_trades (
            market_hash_name TEXT,
            app_id INTEGER,
            ts_utc TEXT,      -- ISO8601 (UTC)
            price REAL,       -- цена за единицу в валюте source_currency
            amount INTEGER,   -- кол-во штук в точке (обычно volume за интервал)
            source TEXT,      -- 'steam_pricehistory' | 'skinport_agg_expand' | 'other'
            source_currency TEXT,
            raw JSON
        )
        """)
        # Агрегированная история Skinport (оставим как справочную)
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
        # Фото/картинки
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
            for period_key in ("last_24_hours","last_7_days","last_30_days","last_90_days"):
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
class JsonSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.headers.update({
            "User-Agent": "cs2-etl/1.0 (+github.com/you)",
            "Accept": "application/json",
        })

    def get_json(self, url: str, **kwargs) -> Any:
        r = self.get(url, timeout=60, **kwargs)
        r.raise_for_status()
        return r.json()

# -------- Клиенты --------
class SkinportClient:
    BASE = "https://api.skinport.com/v1"
    def __init__(self):
        self.s = JsonSession()

    @retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
    def get_items(self, app_id: int = APP_ID, currency: str = CURRENCY,
                  tradable: Optional[bool]=None) -> List[Dict[str, Any]]:
        p = {"app_id": app_id, "currency": currency}
        if tradable is not None:
            p["tradable"] = 1 if tradable else 0
        return self.s.get_json(f"{self.BASE}/items", params=p)

    @retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
    def get_sales_history_agg(self, app_id: int = APP_ID, currency: str = CURRENCY,
                              market_hash_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        p = {"app_id": app_id, "currency": currency}
        if market_hash_names:
            p["market_hash_name"] = ",".join(market_hash_names)
        return self.s.get_json(f"{self.BASE}/sales/history", params=p)

    def get_item_og_image(self, item_page_url: str) -> Optional[str]:
        try:
            r = requests.get(item_page_url, timeout=20, headers={
                "User-Agent": "Mozilla/5.0 (compatible; cs2-etl/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            })
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            tag = soup.find("meta", property="og:image")
            if tag and tag.get("content"):
                return tag["content"]
        except Exception as e:
            log.debug(f"og:image not found: {e}")
        return None

class SteamMarketClient:
    """
    НЕагрегированная цена/объём из Steam pricehistory.
    Требует авторизованный cookie (STEAM_COOKIE) и market_hash_name.
    """
    BASE = "https://steamcommunity.com/market/pricehistory/"
    def __init__(self, cookie: str = STEAM_COOKIE):
        self.cookie = cookie
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; cs2-etl/1.0)",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://steamcommunity.com/market/",
        })
        if cookie:
            # передадим как строку cookie
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
        r = self.s.get(self.BASE, params=params, timeout=60)
        r.raise_for_status()
        # иногда приходит text/plain — распарсим вручную
        try:
            return r.json()
        except Exception:
            import json as _json
            return _json.loads(r.text)
