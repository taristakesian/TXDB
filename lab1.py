# -*- coding: utf-8 -*-
"""
ETL для Skinport и BitSkins:
- Skinport: /v1/items, /v1/sales/history (+ попытка картинок через og:image)
- BitSkins: get_recent_sale_info (+ картинки через Steam CDN, если icon_hash доступен)
Загрузка: SQLite (cs2_market.db)
"""

import os
import time
import json
import sqlite3
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import brotli
import pyotp
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from dateutil import tz

# ------------------------ ЛОГИ --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("cs2-etl")

# ------------------------ КОНФИГ --------------------------------
DB_PATH = os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db")
CURRENCY = os.getenv("CURRENCY", "EUR")
APP_ID = int(os.getenv("APP_ID", "730"))  # CS2/CSGO
# Список айтемов для BitSkins (если хотите собрать историю продаж конкретных вещей)
BITSKINS_TARGET_NAMES = [
    "AK-47 | Redline (Field-Tested)",
    "AWP | Asiimov (Field-Tested)",
    "★ Karambit | Fade (Factory New)"
]

# ------------------------ БАЗА ---------------------------------
class Store:
    def __init__(self, path: str = DB_PATH):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._create_schema()

    def _create_schema(self):
        cur = self.conn.cursor()
        # Skinport — список айтемов (текущая витрина + мета)
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
        # Skinport — агрег. история за 24h/7d/30d/90d
        cur.execute("""
        CREATE TABLE IF NOT EXISTS skinport_sales_agg (
            market_hash_name TEXT,
            currency TEXT,
            item_page TEXT,
            market_page TEXT,
            period TEXT,         -- last_24_hours / last_7_days / last_30_days / last_90_days
            min REAL,
            max REAL,
            avg REAL,
            median REAL,
            volume INTEGER,
            extracted_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (market_hash_name, currency, period)
        )
        """)
        # BitSkins — последние продажи по конкретному айтему (не агрег.)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitskins_recent_sales (
            market_hash_name TEXT,
            app_id INTEGER,
            price REAL,
            currency TEXT,
            occurred_at TEXT,
            page INTEGER,
            raw JSON,
            image_url TEXT,
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
            updated_at=excluded.updated_at,
            image_url=COALESCE(excluded.image_url, skinport_items.image_url)
        """
        rows = [
            (
                it.get("market_hash_name"),
                it.get("currency"),
                it.get("suggested_price"),
                it.get("item_page"),
                it.get("market_page"),
                it.get("min_price"),
                it.get("max_price"),
                it.get("mean_price"),
                it.get("median_price"),
                it.get("quantity"),
                it.get("created_at"),
                it.get("updated_at"),
                it.get("image_url")
            )
            for it in items
        ]
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
                rows.append((
                    s.get("market_hash_name"),
                    s.get("currency"),
                    s.get("item_page"),
                    s.get("market_page"),
                    period_key,
                    p.get("min"), p.get("max"), p.get("avg"), p.get("median"), p.get("volume")
                ))
        cur.executemany(sql, rows)
        self.conn.commit()

    def insert_bitskins_recent_sales(self, rows: List[Tuple]):
        cur = self.conn.cursor()
        sql = """
        INSERT INTO bitskins_recent_sales
        (market_hash_name, app_id, price, currency, occurred_at, page, raw, image_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.executemany(sql, rows)
        self.conn.commit()

# ------------------------ УТИЛЫ HTTP ---------------------------------
class BrotliSession(requests.Session):
    def __init__(self):
        super().__init__()
        # Дай requests/urllib3 самому решать распаковку. Ничего вручную не разжимаем.
        self.headers.update({
            "User-Agent": "cs2-etl/1.0 (+github.com/you)",
            # можно вообще убрать Accept-Encoding — requests сам проставит корректный
            "Accept": "application/json",
        })

    def get_json(self, url: str, **kwargs):
        r = self.get(url, timeout=30, **kwargs)
        r.raise_for_status()
        # Если ответ действительно JSON — это сработает, т.к. requests уже сделал декомпрессию
        return r.json()

# ------------------------ КЛИЕНТЫ API ---------------------------------
class SkinportClient:
    BASE = "https://api.skinport.com/v1"

    def __init__(self):
        self.s = BrotliSession()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
    def get_items(self, app_id: int = APP_ID, currency: str = CURRENCY, tradable: Optional[bool] = None) -> List[Dict[str, Any]]:
        params = {"app_id": app_id, "currency": currency}
        if tradable is not None:
            params["tradable"] = 1 if tradable else 0
        data = self.s.get_json(f"{self.BASE}/items", params=params)
        # возвращает массив объектов с market_hash_name, suggested_price, links и т.п.
        return data

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
    def get_sales_history(self, market_hash_names: Optional[List[str]] = None,
                          app_id: int = APP_ID, currency: str = CURRENCY) -> List[Dict[str, Any]]:
        params = {"app_id": app_id, "currency": currency}
        if market_hash_names:
            params["market_hash_name"] = ",".join(market_hash_names)
        data = self.s.get_json(f"{self.BASE}/sales/history", params=params)
        return data

    def try_extract_image_from_item_page(self, item_page_url: str) -> Optional[str]:
        """Пытаемся вытащить og:image со страницы айтема (без Selenium). Опционально, best-effort."""
        try:
            # Лёгкий headers, не агрессировать Cloudflare
            r = requests.get(item_page_url, timeout=15, headers={
                "User-Agent": "Mozilla/5.0 (compatible; cs2-etl/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            })
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                return og["content"]
        except Exception as e:
            log.debug(f"og:image not found for {item_page_url}: {e}")
        return None

class BitSkinsClient:
    """
    Клиент под API v1 (методы, подтверждённые обёртками).
    Требуются: API_KEY + TOTP_SECRET, code = TOTP.now()
    Пример: get_recent_sale_info (возвращает последние продажи по market_hash_name)
    """
    BASE_V1 = "https://bitskins.com/api/v1"

    def __init__(self, api_key: str, totp_secret: str):
        if not api_key or not totp_secret:
            raise ValueError("Нужны BITSKINS_API_KEY и BITSKINS_TOTP_SECRET")
        self.api_key = api_key
        self.totp = pyotp.TOTP(totp_secret)
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "cs2-etl/1.0 (+github.com/you)",
            "Accept": "application/json"
        })

    def _auth(self) -> Dict[str, Any]:
        # Код одноразовый, обновляем под каждый запрос
        return {"api_key": self.api_key, "code": self.totp.now()}

    @retry(wait=wait_exponential(multiplier=1, min=2, max=20), stop=stop_after_attempt(5))
    def get_recent_sale_info(self, market_hash_name: str, page: int = 1, app_id: int = APP_ID) -> Dict[str, Any]:
        # Эндпоинт по соглашению обёрток: get_recent_sale_info
        params = {
            **self._auth(),
            "market_hash_name": market_hash_name,
            "app_id": app_id,
            "page": page
        }
        url = f"{self.BASE_V1}/get_recent_sale_info/"
        r = self.s.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def build_steam_image_url(self, icon_hash: str) -> str:
        # См. обычное правило для изображений Steam Economy
        return f"https://steamcommunity.com/economy/image/{icon_hash}"

# ------------------------ ETL-ПАЙПЛАЙНЫ ---------------------------------
def etl_skinport(store: Store, tradable_only: Optional[bool] = None):
    log.info("Skinport: вытягиваем список айтемов")
    sp = SkinportClient()
    items = sp.get_items(app_id=APP_ID, currency=CURRENCY, tradable=tradable_only)

    # Больше не дополняем картинки

    store.upsert_skinport_items(items)
    log.info(f"Skinport: сохранено items={len(items)}")

    log.info("Skinport: агрегированная история продаж")
    # Без market_hash_name — вернёт агрегаты по всем предметам
    hist = sp.get_sales_history(app_id=APP_ID, currency=CURRENCY)
    store.upsert_skinport_sales_agg(hist)
    log.info(f"Skinport: сохранено history-rows={len(hist) * 4} (по 4 периода на айтем)")


# def etl_skinport(store: Store, fetch_images: bool = True, tradable_only: Optional[bool] = None):
#     log.info("Skinport: вытягиваем список айтемов")
#     sp = SkinportClient()
#     items = sp.get_items(app_id=APP_ID, currency=CURRENCY, tradable=tradable_only)

#     if fetch_images:
#         log.info("Skinport: пытаемся дополнить картинки через og:image")
#         for it in items:
#             if not it.get("item_page"):
#                 continue
#             img = sp.try_extract_image_from_item_page(it["item_page"])
#             if img:
#                 it["image_url"] = img
#             # мягкое ожидание, чтобы не спамить
#             time.sleep(0.2)

#     store.upsert_skinport_items(items)
#     log.info(f"Skinport: сохранено items={len(items)}")

#     log.info("Skinport: агрегированная история продаж")
#     # Без market_hash_name — вернёт по всем (агрег.) — удобно и экономно по лимитам
#     hist = sp.get_sales_history(app_id=APP_ID, currency=CURRENCY)
#     store.upsert_skinport_sales_agg(hist)
#     log.info(f"Skinport: сохранено history-rows={len(hist) * 4} (по 4 периода на айтем)")

def _normalize_bitskins_recent_sales(resp: Dict[str, Any], market_hash_name: str, page: int) -> List[Tuple]:
    """
    Нормализация ответа BitSkins:get_recent_sale_info -> rows для вставки в БД.
    Структура ответа у BitSkins может слегка различаться (v1/v2),
    поэтому берём безопасные поля: цена, время, валюта, ссылка на картинку, raw.
    """
    data = resp.get("data") or resp.get("items") or resp  # подстрахуемся
    sales = []
    # Ищем список продаж: часто в полях recent_sales, sales, history и т.п.
    candidates = []
    for key in ("recent_sales", "sales", "history", "recentSales", "list"):
        v = data.get(key) if isinstance(data, dict) else None
        if isinstance(v, list):
            candidates = v
            break
    if not candidates and isinstance(data, list):
        candidates = data

    rows = []
    for sale in candidates:
        price = sale.get("price") or sale.get("sale_price") or sale.get("amount")
        currency = sale.get("currency") or "USD"
        ts = sale.get("time") or sale.get("occurred_at") or sale.get("created_at")
        # Форматы времени бывают UNIX/int или ISO8601
        if isinstance(ts, (int, float)):
            occurred_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        elif isinstance(ts, str):
            occurred_at = ts
        else:
            occurred_at = None

        # Картинка: icon_url / image (steam hash)
        image_url = None
        icon_hash = sale.get("icon_url") or sale.get("image") or sale.get("icon")
        if icon_hash and isinstance(icon_hash, str) and icon_hash.startswith("-"):
            image_url = f"https://steamcommunity.com/economy/image/{icon_hash}"

        rows.append((
            market_hash_name, APP_ID, price, currency, occurred_at, page,
            json.dumps(sale, ensure_ascii=False), image_url
        ))
    return rows

def etl_bitskins(store: Store, target_names: List[str]):
    api_key = os.getenv("BITSKINS_API_KEY")
    totp_secret = os.getenv("BITSKINS_TOTP_SECRET")
    if not api_key or not totp_secret:
        log.warning("BitSkins: пропускаем — нет API ключа/TOTP секрета")
        return

    bs = BitSkinsClient(api_key, totp_secret)
    total = 0
    for name in target_names:
        log.info(f"BitSkins: recent sales -> {name}")
        # соберём до 5 страниц, как описано в обёртках
        for page in range(1, 6):
            try:
                resp = bs.get_recent_sale_info(name, page=page, app_id=APP_ID)
            except Exception as e:
                log.warning(f"BitSkins: ошибка для '{name}' page={page}: {e}")
                break

            rows = _normalize_bitskins_recent_sales(resp, name, page)
            if not rows:
                break
            store.insert_bitskins_recent_sales(rows)
            total += len(rows)
            time.sleep(0.3)  # бережный бэкофф
    log.info(f"BitSkins: сохранено {total} продаж")

# ------------------------ MAIN ---------------------------------
def main():
    store = Store(DB_PATH)
    # 1) Skinport
    etl_skinport(store, tradable_only=None)
    # 2) BitSkins (по списку интересующих предметов)
    # etl_bitskins(store, target_names=BITSKINS_TARGET_NAMES)
    log.info("ETL завершён ✔️ БД: %s", DB_PATH)

if __name__ == "__main__":
    main()
