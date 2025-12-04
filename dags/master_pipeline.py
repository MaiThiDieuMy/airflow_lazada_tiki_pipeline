"""
Master DAG - Tiki & Lazada Product ETL Pipeline

DAG này điều phối việc crawl dữ liệu sản phẩm từ 2 nguồn chính:
    - Tiki (API-based crawling)
    - Lazada (Selenium-based crawling)

Workflow:
    1. Setup: Tạo database schema (all_products, price_history, sale_periods)
    2. Extract & Transform: Chạy song song ETL cho Tiki và Lazada
    3. Load: Merge dữ liệu vào bảng all_products (upsert)
    4. Track: Ghi snapshot vào price_history để theo dõi trend
    5. Notify: Gửi thông báo thành công/thất bại qua Slack

Features:
    - Parallel processing: Tiki và Lazada crawl đồng thời
    - Upsert logic: Update nếu sản phẩm đã tồn tại
    - Price tracking: Lưu lịch sử giá theo ngày
    - Error handling: Slack notification khi có lỗi
    - Idempotent: An toàn khi retry

Author: Data Team
Schedule: Daily (@daily)
Last Updated: 2025-01-20
"""

import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from typing import List, Dict

from etl_tiki import run_tiki_etl
from etl_lazada import run_lazada_etl

# ... (CONFIGURATION giữ nguyên) ...
POSTGRES_CONN_ID = "postgres_data_conn"
SLACK_CONN_ID = "slack_webhook_conn"
DEFAULT_ARGS = {'owner': 'data_team', 'retries': 1, 'retry_delay': timedelta(minutes=2)}
TIKI_SEARCH_QUERIES = ["dien thoai", "laptop", "may tinh bang"]
TIKI_PAGES_PER_QUERY = 8
LAZADA_SEARCH_QUERIES = ["dien thoai", "laptop", "may tinh bang"]
LAZADA_PAGES_PER_QUERY = 6
LAZADA_MAX_PRODUCTS_PER_QUERY = 180

# ============================================================================
# SQL QUERIES - CẬP NHẬT THÊM CỘT DOANH THU
# ============================================================================

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS all_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price INT,
    sold_count INT DEFAULT 0,
    brand TEXT,
    source TEXT,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, source)
);

-- Migration: Thêm các cột mới (Bao gồm cả cột Doanh thu)
DO $$ 
BEGIN
    -- Các cột cơ bản
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='review_count') THEN
        ALTER TABLE all_products ADD COLUMN review_count INT DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='review_score') THEN
        ALTER TABLE all_products ADD COLUMN review_score DECIMAL(3,2) DEFAULT 0.0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='category') THEN
        ALTER TABLE all_products ADD COLUMN category TEXT;
    END IF;

    -- Các cột Insight (Đã có)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='original_price') THEN
        ALTER TABLE all_products ADD COLUMN original_price INT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='discount_rate') THEN
        ALTER TABLE all_products ADD COLUMN discount_rate INT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='shop_name') THEN
        ALTER TABLE all_products ADD COLUMN shop_name TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='shop_location') THEN
        ALTER TABLE all_products ADD COLUMN shop_location TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='shipping_fee_est') THEN
        ALTER TABLE all_products ADD COLUMN shipping_fee_est INT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='stock_status') THEN
        ALTER TABLE all_products ADD COLUMN stock_status TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='rating_count_1s') THEN
        ALTER TABLE all_products ADD COLUMN rating_count_1s INT DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='rating_count_5s') THEN
        ALTER TABLE all_products ADD COLUMN rating_count_5s INT DEFAULT 0;
    END IF;

    -- QUAN TRỌNG: Cột Doanh Thu
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='all_products' AND column_name='est_monthly_revenue') THEN
        ALTER TABLE all_products ADD COLUMN est_monthly_revenue BIGINT;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    price INT,
    sold_count INT DEFAULT 0,
    crawl_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, source, crawl_date)
);

-- Migration cho price_history (Thêm các cột để lưu lịch sử insight nếu cần)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='price_history' AND column_name='est_monthly_revenue') THEN
        ALTER TABLE price_history ADD COLUMN est_monthly_revenue BIGINT;
    END IF;
    -- ... (Các cột khác giữ nguyên như file cũ) ...
END $$;

-- ... (Bảng sale_periods và Indexes giữ nguyên như cũ) ...
CREATE TABLE IF NOT EXISTS sale_periods (
    id SERIAL PRIMARY KEY,
    period_name TEXT NOT NULL UNIQUE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    period_type TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# ... (POPULATE_SALE_PERIODS_SQL và on_failure_callback giữ nguyên) ...
POPULATE_SALE_PERIODS_SQL = """
INSERT INTO sale_periods (period_name, start_date, end_date, period_type, description)
VALUES 
    ('Tết Nguyên Đán 2025', '2025-01-20', '2025-02-10', 'Tet', 'Tết Nguyên Đán'),
    ('Black Friday 2025', '2025-11-24', '2025-11-30', 'BlackFriday', 'Black Friday'),
    ('Normal Period Q1 2025', '2025-01-01', '2025-01-19', 'Normal', 'Thời gian bình thường')
ON CONFLICT (period_name) DO NOTHING;
"""

def on_failure_callback(context):
    # ... (Code callback giữ nguyên) ...
    pass 

# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id="master_tiki_lazada_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['master', 'tiki', 'lazada', 'etl'],
    default_args={**DEFAULT_ARGS, 'on_failure_callback': on_failure_callback}
)
def master_product_pipeline():
    
    start = EmptyOperator(task_id="start")

    create_master_table = PostgresOperator(
        task_id="create_master_table",
        postgres_conn_id=POSTGRES_CONN_ID, 
        sql=CREATE_TABLES_SQL
    )

    @task(task_id="run_tiki_etl")
    def run_tiki_task() -> List[Dict]:
        return run_tiki_etl(search_queries=TIKI_SEARCH_QUERIES, pages=TIKI_PAGES_PER_QUERY)

    @task(task_id="run_lazada_etl", queue="selenium_queue")
    def run_lazada_task() -> List[Dict]:
        return run_lazada_etl(
            search_queries=LAZADA_SEARCH_QUERIES,
            pages=LAZADA_PAGES_PER_QUERY,
            max_products_per_query=LAZADA_MAX_PRODUCTS_PER_QUERY,
        )

    # ========================================================================
    # Task Load: Đã cập nhật để Insert cột Doanh thu
    # ========================================================================
    @task(task_id="combine_and_load_data")
    def combine_and_load(tiki_data: List[Dict], lazada_data: List[Dict]) -> Dict:
        logging.info(f"Tiki: {len(tiki_data)}, Lazada: {len(lazada_data)}")
        all_data = (tiki_data or []) + (lazada_data or [])
        
        if not all_data:
            return {"processed_total": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        rows_to_insert = []
        for product in all_data:
            if product is None: continue
            try:
                rows_to_insert.append((
                    product['name'], 
                    product['price'], 
                    product['sold_count'],
                    product.get('review_count', 0),
                    product.get('review_score', 0.0),
                    product['brand'], 
                    product['category'], 
                    product['source'],
                    # Enhanced fields
                    product.get('original_price'),
                    product.get('discount_rate'),
                    product.get('shop_name'),
                    product.get('shop_location'),
                    product.get('shipping_fee_est'),
                    product.get('stock_status'),
                    product.get('rating_count_1s', 0),
                    product.get('rating_count_5s', 0),
                    # CỘT MỚI: DOANH THU
                    product.get('est_monthly_revenue', 0)
                ))
            except KeyError: continue
        
        # Update SQL Insert
        insert_sql = """
        INSERT INTO all_products (
            name, price, sold_count, review_count, review_score, brand, category, source, crawled_at,
            original_price, discount_rate, shop_name, shop_location, shipping_fee_est, stock_status, 
            rating_count_1s, rating_count_5s, est_monthly_revenue
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name, source) 
        DO UPDATE SET 
            price = EXCLUDED.price,
            sold_count = EXCLUDED.sold_count,
            review_count = EXCLUDED.review_count,
            original_price = EXCLUDED.original_price,
            discount_rate = EXCLUDED.discount_rate,
            stock_status = EXCLUDED.stock_status,
            est_monthly_revenue = EXCLUDED.est_monthly_revenue,
            crawled_at = CURRENT_TIMESTAMP
        """
        
        try:
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
            processed = len(rows_to_insert)
        except Exception as e:
            conn.rollback()
            logging.error(f"Batch insert failed: {e}")
            processed = 0
        finally:
            cursor.close()
            conn.close()
        
        return {
            "tiki_count": len(tiki_data),
            "lazada_count": len(lazada_data),
            "processed_total": processed
        }

    # ========================================================================
    # Task History: Cập nhật để lưu cả doanh thu vào lịch sử
    # ========================================================================
    @task(task_id="track_price_history")
    def track_price_history(load_summary: Dict) -> int:
        if int(load_summary.get("processed_total", 0)) <= 0:
            return 0
        
        from airflow.operators.python import get_current_context
        ds = get_current_context().get('ds')

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            insert_history_sql = """
            INSERT INTO price_history 
                (name, source, price, sold_count, review_count, review_score, brand, category, crawl_date, created_at,
                 est_monthly_revenue)
            SELECT 
                ap.name, ap.source, ap.price, ap.sold_count, 
                ap.review_count, ap.review_score, ap.brand, ap.category, 
                %s::date AS crawl_date, CURRENT_TIMESTAMP,
                ap.est_monthly_revenue
            FROM all_products ap
            ON CONFLICT (name, source, crawl_date) DO NOTHING
            """
            cursor.execute(insert_history_sql, (ds,))
            conn.commit()
            return cursor.rowcount
        except Exception:
            return 0
        finally:
            cursor.close()
            conn.close()

    @task(task_id="send_success_notification")
    def send_success_alert(load_summary: Dict, history_added: int):
        # ... (Code gửi Slack giữ nguyên) ...
        pass

    populate_sale_periods = PostgresOperator(
        task_id="populate_sale_periods",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=POPULATE_SALE_PERIODS_SQL
    )

    end = EmptyOperator(task_id="end")

    # ========================================================================
    # DEPENDENCIES - LUỒNG CHẠY TUYẾN TÍNH (ETL -> LOAD -> HISTORY)
    # ========================================================================
    
    tiki_data = run_tiki_task()
    lazada_data = run_lazada_task()
    
    # Load dữ liệu (Đã bao gồm tính toán Doanh thu từ Python)
    load_summary = combine_and_load(tiki_data=tiki_data, lazada_data=lazada_data)
    
    # Lưu lịch sử
    history_count = track_price_history(load_summary=load_summary)
    
    # Nối luồng
    start >> create_master_table >> populate_sale_periods
    populate_sale_periods >> [tiki_data, lazada_data]
    
    # ETL (Python) -> Load -> History -> Notify
    load_summary >> history_count >> send_success_alert(load_summary, history_count) >> end

master_product_pipeline()