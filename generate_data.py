import os
import uuid
import random
import datetime
import math
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_USER = os.getenv('ODS_PG_USER', 'postgres')
DB_PASS = os.getenv('ODS_PG_PASS', 'postgres')
DB_NAME = os.getenv('ODS_PG_DB', 'postgres')

print(f"DB_HOST: {DB_HOST}, DB_PORT: {DB_PORT}, DB_USER: {DB_USER}, DB_PASS: {DB_PASS}, DB_NAME: {DB_NAME}")

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Please check your connection settings and ensure the database is running.")
        return None

def fetch_products(cursor):
    """Fetch existing products from the database."""
    print("Fetching products from all_product...")
    cursor.execute("SELECT id_name, name FROM all_product")
    rows = cursor.fetchall()
    
    products = []
    for r in rows:
        id_name = r[0] # UUID
        name = r[1]
        
        # Generate a consistent base price based on the name hash
        # This ensures the same product gets the same base price across runs
        # Use simple hash to seed random
        seed = hash(name)
        rng = np.random.default_rng(abs(seed))
        
        # Base price generation (Lognormal)
        # mu=3, sigma=1 -> roughly ranges from 5 to 150+ with some high outliers
        base_price = rng.lognormal(mean=3, sigma=1.2)
        base_price = round(max(0.5, base_price), 2)
        
        products.append({
            'id_name': id_name, # keep as UUID object or string depending on driver, psycopg2 handles UUID
            'name': name,
            '_base_price': base_price
        })
    
    return products

def generate_history_stats(base_price, days, rng=None):
    """
    Generates stats for a given time window.
    Higher price -> Lower volume, Higher volatility
    """
    if rng is None:
        rng = np.random
        
    # Rarity/Price factor
    price_volatility_factor = 0.05 + (math.log1p(base_price) * 0.02)
    
    # Volume: inversely proportional to price
    daily_volume_lambda = max(0.1, 500 / (base_price + 10))
    expected_volume = int(daily_volume_lambda * days)
    
    # Randomize volume (Poisson)
    volume = rng.poisson(expected_volume)
    
    if volume == 0:
        return None  # No sales
        
    # Generate individual sale prices
    prices = rng.normal(loc=base_price, scale=base_price * price_volatility_factor, size=volume)
    prices = np.maximum(0.01, prices)
    
    return {
        'min': float(np.min(prices)),
        'max': float(np.max(prices)),
        'avg': float(np.mean(prices)),
        'median': float(np.median(prices)),
        'volume': int(volume)
    }

def main():
    conn = get_connection()
    if not conn:
        return
        
    cursor = conn.cursor()
    
    # 1. Fetch Products
    products = fetch_products(cursor)
    if not products:
        print("No products found in all_product table! Please populate it first.")
        conn.close()
        return
        
    print(f"Found {len(products)} products.")
    
    # 2. Generate and Insert Dependent Data
    skinport_stakan_data = []
    skinport_history_data = []
    bitskins_stakan_data = []
    
    # Define time range: last 7 days
    today = datetime.datetime.now().date()
    days_to_generate = 7
    dates = [today - datetime.timedelta(days=i) for i in range(days_to_generate)]
    dates.reverse() # Oldest first
    
    print(f"Generating data for dates: {dates[0]} to {dates[-1]}")
    
    for process_date in dates:
        # Convert date to datetime for timestamp fields (using noon as arbitrary time)
        ts = datetime.datetime.combine(process_date, datetime.time(12, 0, 0))
        is_today = (process_date == today)
        
        for p in products:
            base_price = p['_base_price']
            id_name = p['id_name']
            
            # Add some daily fluctuation to the base price for this day
            # so history stats drift slightly day-to-day
            # Deterministic based on id and date if we wanted repeatable, 
            # but here random for simulation is fine if we re-run
            daily_fluctuation = np.random.normal(0, base_price * 0.02)
            current_day_base_price = max(0.01, base_price + daily_fluctuation)
            
            # --- Skinport History (All Days) ---
            h_24 = generate_history_stats(current_day_base_price, 1) or {}
            h_7 = generate_history_stats(current_day_base_price, 7) or {}
            h_30 = generate_history_stats(current_day_base_price, 30) or {}
            h_90 = generate_history_stats(current_day_base_price, 90) or {}
            
            skinport_history_data.append((
                id_name,
                h_24.get('min'), h_24.get('max'), h_24.get('avg'), h_24.get('median'), h_24.get('volume', 0),
                h_7.get('min'), h_7.get('max'), h_7.get('avg'), h_7.get('median'), h_7.get('volume', 0),
                h_30.get('min'), h_30.get('max'), h_30.get('avg'), h_30.get('median'), h_30.get('volume', 0),
                h_90.get('min'), h_90.get('max'), h_90.get('avg'), h_90.get('median'), h_90.get('volume', 0),
                ts
            ))
            
            if is_today:
                # --- Skinport Stakan (Today Only) ---
                volatility = 0.05 + (math.log1p(current_day_base_price) * 0.02)
                suggested = max(0.01, np.random.normal(current_day_base_price, current_day_base_price * volatility))
                
                min_p = suggested * (1 - volatility/2)
                max_p = suggested * (1 + volatility/2)
                
                skinport_stakan_data.append((
                    id_name,
                    suggested,
                    ts,
                    min_p,
                    max_p,
                    suggested, 
                    suggested
                ))
                
                # --- Bitskins Stakan (Today Only) ---
                num_listings = np.random.poisson(max(1, 200/(current_day_base_price+10)))
                for _ in range(num_listings):
                     listing_price = max(0.01, np.random.normal(current_day_base_price, current_day_base_price * volatility * 1.5))
                     bitskins_stakan_data.append((
                         id_name,
                         ts,
                         listing_price
                     ))

    # Batch Insert Skinport Stakan (Today)
    if skinport_stakan_data:
        query_stakan = """
            INSERT INTO skinport_stakan 
            (id_name, suggested_price, date_extracted, min_price, max_price, mean_price, median_price)
            VALUES %s
        """
        execute_values(cursor, query_stakan, skinport_stakan_data)
        print(f"Inserted {len(skinport_stakan_data)} records into skinport_stakan (Today).")
    
    # Batch Insert Skinport History (Range)
    if skinport_history_data:
        query_history = """
            INSERT INTO skinport_history
            (id_name, 
             last_24_hours_min, last_24_hours_max, last_24_hours_avg, last_24_hours_median, last_24_hours_volume,
             last_7_days_min, last_7_days_max, last_7_days_avg, last_7_days_median, last_7_days_volume,
             last_30_days_min, last_30_days_max, last_30_days_avg, last_30_days_median, last_30_days_volume,
             last_90_days_min, last_90_days_max, last_90_days_avg, last_90_days_median, last_90_days_volume,
             date_extracted)
            VALUES %s
        """
        # Execute in chunks to avoid memory issues if range is large
        chunk_size = 1000
        for i in range(0, len(skinport_history_data), chunk_size):
            chunk = skinport_history_data[i:i+chunk_size]
            execute_values(cursor, query_history, chunk)
        print(f"Inserted {len(skinport_history_data)} records into skinport_history (Last 7 days).")

    # Batch Insert Bitskins Stakan (Today)
    if bitskins_stakan_data:
        query_bitskins = """
            INSERT INTO bitskins_stakan (id_name, date_extracted, price)
            VALUES %s
        """
        execute_values(cursor, query_bitskins, bitskins_stakan_data)
        print(f"Inserted {len(bitskins_stakan_data)} records into bitskins_stakan (Today).")

    conn.commit()
    cursor.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
