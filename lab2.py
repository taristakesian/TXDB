import json
import sqlite3
import uuid
from datetime import datetime

# Функция для генерации UUID на основе market_hash_name (детерминированный)
def generate_uuid(name):
    namespace = uuid.NAMESPACE_DNS  # Можно изменить на другой namespace
    return str(uuid.uuid5(namespace, name))

# Подключение к БД (создаст файл, если не существует)
conn = sqlite3.connect('skin_db.sqlite')
cursor = conn.cursor()

# Создание таблиц (с развернутыми столбцами для history)
cursor.execute('''
CREATE TABLE IF NOT EXISTS all_product (
    id_name TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    item_page TEXT,
    market_page TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS bitskins_stakan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_name TEXT NOT NULL,
    date_extracted TEXT,
    price REAL,
    FOREIGN KEY (id_name) REFERENCES all_product(id_name)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS skinport_stakan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_name TEXT NOT NULL,
    suggested_price REAL,
    date_extracted TEXT,
    min_price REAL,
    max_price REAL,
    mean_price REAL,
    median_price REAL,
    FOREIGN KEY (id_name) REFERENCES all_product(id_name)
)
''')

# Расширенная таблица history с развернутыми агрегатами
cursor.execute('''
CREATE TABLE IF NOT EXISTS skinport_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_name TEXT NOT NULL,
    last_24_hours_min REAL,
    last_24_hours_max REAL,
    last_24_hours_avg REAL,
    last_24_hours_median REAL,
    last_24_hours_volume INTEGER,
    last_7_days_min REAL,
    last_7_days_max REAL,
    last_7_days_avg REAL,
    last_7_days_median REAL,
    last_7_days_volume INTEGER,
    last_30_days_min REAL,
    last_30_days_max REAL,
    last_30_days_avg REAL,
    last_30_days_median REAL,
    last_30_days_volume INTEGER,
    last_90_days_min REAL,
    last_90_days_max REAL,
    last_90_days_avg REAL,
    last_90_days_median REAL,
    last_90_days_volume INTEGER,
    date_extracted TEXT,
    FOREIGN KEY (id_name) REFERENCES all_product(id_name)
)
''')

# Очистка таблиц перед вставкой (для повторных запусков)
cursor.execute('DELETE FROM bitskins_stakan')
cursor.execute('DELETE FROM skinport_stakan')
cursor.execute('DELETE FROM skinport_history')
cursor.execute('DELETE FROM all_product')

# Словарь для хранения уникальных продуктов (id_name -> данные)
products = {}

# Обработка bitskins.txt
with open('bitskins.json', 'r', encoding='utf-8') as f:
    bitskins_data = json.load(f)
    for item in bitskins_data:
        name = item['market_hash_name']
        id_name = generate_uuid(name)
        if id_name not in products:
            products[id_name] = {
                'name': name,
                'item_page': None,  # Нет в bitskins, оставляем None
                'market_page': None
            }
        # Вставка в bitskins_stakan
        cursor.execute('''
        INSERT INTO bitskins_stakan (id_name, date_extracted, price)
        VALUES (?, ?, ?)
        ''', (id_name, item.get('date_extracted'), item.get('price')))

# Обработка items_small.txt (skinport_stakan)
with open('items.json', 'r', encoding='utf-8') as f:
    items_data = json.load(f)
    for item in items_data:
        name = item['market_hash_name']
        id_name = generate_uuid(name)
        if id_name not in products:
            products[id_name] = {
                'name': name,
                'item_page': item.get('item_page'),
                'market_page': item.get('market_page')
            }
        # Вставка в skinport_stakan (date_extracted = NULL)
        cursor.execute('''
        INSERT INTO skinport_stakan (id_name, suggested_price, date_extracted, min_price, max_price, mean_price, median_price)
        VALUES (?, ?, NULL, ?, ?, ?, ?)
        ''', (id_name, item.get('suggested_price'), item.get('min_price'), item.get('max_price'),
              item.get('mean_price'), item.get('median_price')))

# Обработка history_small.txt (skinport_history с разверткой)
with open('history.json', 'r', encoding='utf-8') as f:
    history_data = json.load(f)
    for item in history_data:
        name = item['market_hash_name']
        id_name = generate_uuid(name)
        if id_name not in products:
            products[id_name] = {
                'name': name,
                'item_page': item.get('item_page'),
                'market_page': item.get('market_page')
            }
        # Развертка агрегатов
        last_24 = item.get('last_24_hours', {})
        last_7 = item.get('last_7_days', {})
        last_30 = item.get('last_30_days', {})
        last_90 = item.get('last_90_days', {})
        
        cursor.execute('''
        INSERT INTO skinport_history (
            id_name,
            last_24_hours_min, last_24_hours_max, last_24_hours_avg, last_24_hours_median, last_24_hours_volume,
            last_7_days_min, last_7_days_max, last_7_days_avg, last_7_days_median, last_7_days_volume,
            last_30_days_min, last_30_days_max, last_30_days_avg, last_30_days_median, last_30_days_volume,
            last_90_days_min, last_90_days_max, last_90_days_avg, last_90_days_median, last_90_days_volume,
            date_extracted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ''', (id_name,
              last_24.get('min'), last_24.get('max'), last_24.get('avg'), last_24.get('median'), last_24.get('volume'),
              last_7.get('min'), last_7.get('max'), last_7.get('avg'), last_7.get('median'), last_7.get('volume'),
              last_30.get('min'), last_30.get('max'), last_30.get('avg'), last_30.get('median'), last_30.get('volume'),
              last_90.get('min'), last_90.get('max'), last_90.get('avg'), last_90.get('median'), last_90.get('volume')))

# Вставка уникальных продуктов в all_product
for id_name, data in products.items():
    cursor.execute('''
    INSERT OR IGNORE INTO all_product (id_name, name, item_page, market_page)
    VALUES (?, ?, ?, ?)
    ''', (id_name, data['name'], data['item_page'], data['market_page']))

# Коммит и закрытие
conn.commit()
conn.close()

print("База данных создана и данные вставлены успешно.")