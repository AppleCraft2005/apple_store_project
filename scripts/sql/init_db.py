from sqlalchemy import create_engine, text
import os
import sys

# --- KONFIGURASI KONEKSI ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
else:
    DB_HOST = 'localhost'

DB_USER = 'admin'; DB_PASS = 'admin'; DB_NAME = 'apple_dwh'; DB_PORT = '5432'
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def init_database():
    print("=== MEMULAI INISIALISASI DATABASE LENGKAP (DDL + RBAC) ===")
    
    try:
        engine = create_engine(CONN_STRING)
        with engine.begin() as conn:
            
            # --- 1. SETUP SCHEMAS ---
            print("--> 1. Membuat Schema...")
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dwh;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts;"))

            # --- 2. TABEL STAGING ---
            print("--> 2. Membuat Tabel Staging...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS staging.stg_category (category_id TEXT, category_name TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_stores (store_id TEXT, store_name TEXT, city TEXT, country TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_employees (salesperson_id TEXT, employee_name TEXT, jabatan TEXT, store_id TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_customers (customer_id TEXT, customer_name TEXT, age_group TEXT, membership_status TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_products (product_id TEXT, product_name TEXT, price TEXT, category_id TEXT, gsmarena_key TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_sales (sale_id TEXT, product_id TEXT, store_id TEXT, quantity TEXT, sale_date TEXT);
                CREATE TABLE IF NOT EXISTS staging.stg_warranty (claim_id TEXT, sale_id TEXT, repair_status TEXT, claim_date TEXT);
                
                DROP TABLE IF EXISTS staging.stg_specs_raw;
                CREATE TABLE staging.stg_specs_raw (
                    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
                    csv_product_name TEXT,
                    raw_content JSONB
                );
            """))

            # --- 3. TABEL DWH ---
            print("--> 3. Membuat Tabel DWH...")
            conn.execute(text("""
                -- Dimensi
                CREATE TABLE IF NOT EXISTS dwh.dim_date (
                    date_key BIGINT PRIMARY KEY, full_date DATE, day_name VARCHAR(20), month_name VARCHAR(20), year INT, quarter INT, is_weekend BOOLEAN
                );
                CREATE TABLE IF NOT EXISTS dwh.dim_category (
                    category_key SERIAL PRIMARY KEY, category_id_original VARCHAR(50), category_name VARCHAR(100)
                );
                CREATE TABLE IF NOT EXISTS dwh.dim_store (
                    store_key SERIAL PRIMARY KEY, store_id_original VARCHAR(50), store_name VARCHAR(100), city VARCHAR(100), country VARCHAR(100)
                );
                CREATE TABLE IF NOT EXISTS dwh.dim_customer (
                    customer_key SERIAL PRIMARY KEY, customer_id_original VARCHAR(50), customer_name VARCHAR(255), age_group VARCHAR(50), membership_status VARCHAR(50)
                );
                CREATE TABLE IF NOT EXISTS dwh.dim_product (
                    product_key SERIAL PRIMARY KEY, product_id_original VARCHAR(50), product_name VARCHAR(255), price NUMERIC,
                    spec_chipset VARCHAR(255), spec_ram VARCHAR(100), spec_storage VARCHAR(100), spec_display VARCHAR(255), spec_camera VARCHAR(255), spec_battery VARCHAR(100),
                    category_key INT REFERENCES dwh.dim_category(category_key)
                );
                CREATE TABLE IF NOT EXISTS dwh.dim_employee (
                    employee_key SERIAL PRIMARY KEY, salesperson_id_original VARCHAR(50), employee_name VARCHAR(255), jabatan VARCHAR(100),
                    store_key INT REFERENCES dwh.dim_store(store_key)
                );

                -- Fakta
                CREATE TABLE IF NOT EXISTS dwh.fact_sales (
                    sales_key SERIAL PRIMARY KEY, sales_id_original VARCHAR(50),
                    date_key BIGINT REFERENCES dwh.dim_date(date_key),
                    product_key INT REFERENCES dwh.dim_product(product_key),
                    store_key INT REFERENCES dwh.dim_store(store_key),
                    customer_key INT REFERENCES dwh.dim_customer(customer_key),
                    employee_key INT REFERENCES dwh.dim_employee(employee_key),
                    quantity INT, unit_price NUMERIC, total_amount NUMERIC
                );
                CREATE TABLE IF NOT EXISTS dwh.fact_warranty (
                    warranty_key SERIAL PRIMARY KEY, claim_id_original VARCHAR(50),
                    date_key BIGINT REFERENCES dwh.dim_date(date_key),
                    sales_key INT REFERENCES dwh.fact_sales(sales_key),
                    repair_status VARCHAR(50)
                );
            """))

            # --- 4. VIEWS MARTS ---
            print("--> 4. Membuat Views Marts...")
            conn.execute(text("""
                CREATE OR REPLACE VIEW marts.view_monthly_sales AS
                SELECT d.year, d.month_name, d.quarter, c.category_name, 
                       SUM(f.quantity) as total_items_sold, SUM(f.total_amount) as total_revenue, COUNT(f.sales_key) as total_transactions
                FROM dwh.fact_sales f
                JOIN dwh.dim_date d ON f.date_key = d.date_key
                JOIN dwh.dim_product p ON f.product_key = p.product_key
                JOIN dwh.dim_category c ON p.category_key = c.category_key
                GROUP BY d.year, d.month_name, d.quarter, c.category_name;

                CREATE OR REPLACE VIEW marts.view_product_sales_yearly AS
                SELECT d.year, c.category_name, p.product_name, 
                       SUM(f.quantity) as total_units, SUM(f.total_amount) as total_revenue
                FROM dwh.fact_sales f
                JOIN dwh.dim_date d ON f.date_key = d.date_key
                JOIN dwh.dim_product p ON f.product_key = p.product_key
                JOIN dwh.dim_category c ON p.category_key = c.category_key
                GROUP BY d.year, c.category_name, p.product_name;

                CREATE OR REPLACE VIEW marts.view_product_risk_analysis AS
                WITH warranty_counts AS (
                    SELECT s.product_key, COUNT(w.warranty_key) as total_warranty_claims
                    FROM dwh.fact_warranty w JOIN dwh.fact_sales s ON w.sales_key = s.sales_key GROUP BY s.product_key
                )
                SELECT c.category_name, p.product_name, p.spec_chipset, p.spec_ram, p.spec_battery,
                       SUM(f.quantity) as total_sold, SUM(f.total_amount) as revenue,
                       COALESCE(wc.total_warranty_claims, 0) as total_warranty_claims,
                       ROUND((COALESCE(wc.total_warranty_claims, 0)::NUMERIC / NULLIF(SUM(f.quantity), 0)) * 100, 2) as risk_percentage
                FROM dwh.dim_product p
                JOIN dwh.fact_sales f ON p.product_key = f.product_key
                JOIN dwh.dim_category c ON p.category_key = c.category_key
                LEFT JOIN warranty_counts wc ON p.product_key = wc.product_key
                GROUP BY c.category_name, p.product_name, p.spec_chipset, p.spec_ram, p.spec_battery, wc.total_warranty_claims;
            """))

            # --- 5. SETUP RBAC (DATA GOVERNANCE) ---
            print("--> 5. Setup RBAC (Data Governance)...")
            
            # Buat User jika belum ada (Idempotent)
            conn.execute(text("""
            DO
            $do$
            BEGIN
               IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'data_analyst') THEN
                  CREATE USER data_analyst WITH PASSWORD 'analyst';
               END IF;
               IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'etl_engineer') THEN
                  CREATE USER etl_engineer WITH PASSWORD 'engineer';
               END IF;
            END
            $do$;
            """))
            
            # Berikan Hak Akses (Grants)
            conn.execute(text("""
                -- Grant untuk Data Analyst (Read-Only di Marts)
                GRANT CONNECT ON DATABASE apple_dwh TO data_analyst;
                GRANT USAGE ON SCHEMA marts TO data_analyst;
                GRANT SELECT ON ALL TABLES IN SCHEMA marts TO data_analyst;
                ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO data_analyst;

                -- Grant untuk ETL Engineer (Full Access di Staging & DWH)
                GRANT CONNECT ON DATABASE apple_dwh TO etl_engineer;
                GRANT USAGE, CREATE ON SCHEMA staging TO etl_engineer;
                GRANT USAGE, CREATE ON SCHEMA dwh TO etl_engineer;
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO etl_engineer;
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dwh TO etl_engineer;
                ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL PRIVILEGES ON TABLES TO etl_engineer;
                ALTER DEFAULT PRIVILEGES IN SCHEMA dwh GRANT ALL PRIVILEGES ON TABLES TO etl_engineer;
            """))
            
            print("[SUKSES] Database, Tables, Views, dan Users berhasil diinisialisasi.")
            
    except Exception as e:
        print(f"[FATAL ERROR] Gagal inisialisasi database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()