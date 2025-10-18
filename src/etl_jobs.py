"""
Три независимых процесса:
1) load_catalog   — загрузка/обновление ассортимента (Skinport /v1/items + справочная агр. история)
2) load_history   — НЕагрегированная история (Steam pricehistory) -> sales_trades
3) load_images    — загрузка фото (og:image со страниц Skinport)
Запуск: python etl_jobs.py <process> [--args]
"""
import argparse, os, time, logging
from typing import Optional, List, Tuple
from market_clients import Store, SkinportClient, SteamMarketClient, APP_ID, CURRENCY, STEAM_COUNTRY, STEAM_CURRENCY

log = logging.getLogger("jobs")

# --------------------- Процесс 1: ассортимент ---------------------
def process_load_catalog(db_path: Optional[str]=None, tradable_only: Optional[bool]=None,
                         save_agg_history: bool=True):
    store = Store(db_path or os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db"))
    sp = SkinportClient()

    log.info("Skinport items: fetch")
    items = sp.get_items(app_id=APP_ID, currency=CURRENCY, tradable=tradable_only)
    store.upsert_skinport_items(items)
    log.info("Skinport items: saved=%d", len(items))

    if save_agg_history:
        log.info("Skinport sales agg: fetch")
        hist = sp.get_sales_history_agg(app_id=APP_ID, currency=CURRENCY)
        store.upsert_skinport_sales_agg(hist)
        log.info("Skinport sales agg: saved rows=%d (x4 periods each item)", len(hist)*4)

# --------------------- Процесс 2: история (неагрег.) ---------------------
def _rows_from_pricehistory(market_hash_name: str, data: dict) -> List[Tuple]:
    """
    Steam отдаёт "prices": [[date_str, price, volume], ...]
    date_str — локализованная дата; мы её не парсим точно, берём как текст и оставляем raw (или можно распарсить pendulum'ом).
    Для простоты: timestamp берём как «как прислано» (Steam в локальном TZ); лучше всё привести к UTC в реальном проекте.
    """
    rows = []
    prices = data.get("prices") or []
    for entry in prices:
        # entry: [ "Oct 17 2025 01: +0", "12.34", 3 ]
        dt_txt, price, amount = entry[0], entry[1], entry[2]
        # оставим как текст (или попытаться нормализовать):
        ts_utc = dt_txt  # лучше распарсить на своей стороне под твою зону и привести к UTC
        rows.append((
            market_hash_name, APP_ID, ts_utc, float(price), int(amount),
            "steam_pricehistory", str(STEAM_CURRENCY),  # source, currency code
            None
        ))
    return rows

def process_load_history(db_path: Optional[str]=None, names_file: Optional[str]=None):
    """
    Берём список market_hash_name из файла (по одному на строку) и тянем pricehistory.
    Нужен STEAM_COOKIE в переменных окружения.
    """
    store = Store(db_path or os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db"))
    sm = SteamMarketClient()
    if not sm.cookie:
        raise RuntimeError("STEAM_COOKIE не задан. Укажи переменную окружения со своим авторизованным cookie.")

    if not names_file or not os.path.exists(names_file):
        raise FileNotFoundError("Укажи --names-file с market_hash_name по строкам.")

    with open(names_file, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]

    total = 0
    for name in names:
        log.info("Steam pricehistory: %s", name)
        data = sm.price_history(app_id=APP_ID, market_hash_name=name, country=STEAM_COUNTRY, currency=STEAM_CURRENCY)
        rows = _rows_from_pricehistory(name, data)
        if rows:
            store.insert_sales_trades(rows)
            total += len(rows)
        time.sleep(0.3)
    log.info("sales_trades inserted: %d", total)

# --------------------- Процесс 3: фото отдельно ---------------------
def process_load_images(db_path: Optional[str]=None, limit: int=200, sleep_sec: float=0.2):
    store = Store(db_path or os.getenv("CS2_DB_PATH", "TXDB/cs2_market.db"))
    sp = SkinportClient()
    # берём небольшой батч ассортимента, дополняем og:image
    items = sp.get_items(app_id=APP_ID, currency=CURRENCY)
    taken = 0
    rows = []
    for it in items:
        if taken >= limit:
            break
        url = it.get("item_page")
        name = it.get("market_hash_name")
        if not url or not name:
            continue
        img = sp.get_item_og_image(url)
        if img:
            rows.append((name, img, "og_image"))
            taken += 1
        time.sleep(sleep_sec)
    if rows:
        store.insert_images(rows)
    log.info("images saved: %d", len(rows))

# --------------------- CLI ---------------------
def main():
    ap = argparse.ArgumentParser(description="CS2 ETL processes")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("load_catalog", help="загрузка/обновление ассортимента (Skinport)")
    p1.add_argument("--db", dest="db_path", default=None)
    p1.add_argument("--tradable-only", dest="tradable_only", action="store_true")
    p1.add_argument("--no-agg", dest="no_agg", action="store_true", help="не сохранять агрегаты Skinport")

    p2 = sub.add_parser("load_history", help="НЕагрегированная история (Steam pricehistory)")
    p2.add_argument("--db", dest="db_path", default=None)
    p2.add_argument("--names-file", required=True, help="файл со списком market_hash_name (по строке)")

    p3 = sub.add_parser("load_images", help="фото/иконки отдельно")
    p3.add_argument("--db", dest="db_path", default=None)
    p3.add_argument("--limit", type=int, default=200)
    p3.add_argument("--sleep", type=float, default=0.2)

    args = ap.parse_args()

    if args.cmd == "load_catalog":
        process_load_catalog(args.db_path, tradable_only=args.tradable_only, save_agg_history=not args.no_agg)
    elif args.cmd == "load_history":
        process_load_history(args.db_path, names_file=args.names_file)
    elif args.cmd == "load_images":
        process_load_images(args.db_path, limit=args.limit, sleep_sec=args.sleep)

if __name__ == "__main__":
    main()
