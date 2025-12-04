"""
ETL Module cho Tiki - Extract, Transform, Load dữ liệu sản phẩm từ Tiki.vn

Module này sử dụng Tiki API để crawl dữ liệu sản phẩm (điện thoại, laptop, máy tính bảng).
Quy trình ETL:
    - Extract: Lấy dữ liệu từ Tiki API (JSON response)
    - Transform: Làm sạch và chuẩn hóa dữ liệu (giá, số lượng bán, brand, category)
    - Load: Trả về dữ liệu đã xử lý cho Airflow DAG

Author: Data Team
Last Updated: 2025-01-20
"""

import logging
import re
import time
from typing import List, Dict

import requests
from data_enricher import enrich_product_data  # Import module làm giàu dữ liệu

# ============================================================================
# CONFIGURATION - Cấu hình constants
# ============================================================================

# API Configuration
DEFAULT_PAGE_LIMIT = 40  # Số sản phẩm tối đa mỗi trang
DEFAULT_TIMEOUT = 30  # Timeout cho HTTP request (giây)
REQUEST_DELAY = 1.0  # Delay giữa các request để tránh bị chặn IP (giây)

# API URL Template
TIKI_API_URL_TEMPLATE = (
    "https://tiki.vn/api/personalish/v1/blocks/listings"
    "?limit={limit}"
    "&include=advertisement"
    "&aggregations=2"
    "&version=home-persionalized"
    "&trackity_id=f2b5815e-421b-72c3-45c5-e1787f30c8d1"
    "&urlKey={search_query}"
    "&category=1789"
    "&page={page_number}"
)

# HTTP Headers
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

# Brand Keywords Mapping
# Dùng để xác định thương hiệu từ tên sản phẩm
BRAND_KEYWORDS = {
    "Apple": ["iphone", "macbook", "ipad", "apple"],
    "Samsung": ["samsung", "galaxy", "note", "tab"],
    "Xiaomi": ["xiaomi", "redmi", "mi"],
    "OPPO": ["oppo", "reno", "find x"],
    "Huawei": ["huawei", "mate", "p20", "honor"],
    "Asus": ["asus", "zenfone", "rog"],
    "Lenovo": ["lenovo", "thinkpad", "yoga"],
    "Dell": ["dell", "xps", "inspiron"],
    "HP": ["hp", "pavilion", "envy"],
}

# ============================================================================
# EXTRACT FUNCTIONS - Lấy dữ liệu từ Tiki API
# ============================================================================

def extract_tiki_data(search_query: str = "dien thoai", pages: int = 8) -> List[Dict]:
    """
    Extract dữ liệu sản phẩm từ Tiki API.
    
    Hàm này sẽ crawl dữ liệu từ Tiki API theo từ khóa tìm kiếm và số trang.
    Tiki API trả về JSON response chứa thông tin sản phẩm đầy đủ.
    
    Args:
        search_query (str): Từ khóa tìm kiếm sản phẩm (vd: "dien thoai", "laptop")
        pages (int): Số trang cần crawl (mỗi trang ~40 sản phẩm)
    
    Returns:
        List[Dict]: Danh sách sản phẩm thô chứa:
            - name_raw: Tên sản phẩm
            - price_raw: Giá (có thể là int hoặc string)
            - sold_raw: Số lượng đã bán (có thể là int hoặc dict)
            - review_count: Số lượng đánh giá
            - review_score: Điểm đánh giá trung bình
    
    Example:
        >>> data = extract_tiki_data("laptop", pages=3)
        >>> len(data)
        120  # ~40 sản phẩm/trang × 3 trang
    """
    logging.info(
        f"Bắt đầu crawl dữ liệu từ API của Tiki với từ khóa '{search_query}' "
        f"(tối đa {pages} trang)..."
    )
    
    all_products_data = []

    for page in range(1, pages + 1):
        # Tạo API URL cho từng trang
        api_url = TIKI_API_URL_TEMPLATE.format(
            limit=DEFAULT_PAGE_LIMIT,
            search_query=search_query,
            page_number=page
        )
        
        logging.info(f"Tiki - Đang crawl trang số: {page}")
        
        try:
            # Gọi API với timeout để tránh treo
            response = requests.get(api_url, headers=REQUEST_HEADERS, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()  # Raise exception nếu status code không phải 2xx
            api_data = response.json()
        except Exception as e:
            logging.warning(f"Tiki - Lỗi khi gọi API trang {page}: {e}. Dừng.")
            break
        
        # Kiểm tra cấu trúc dữ liệu response
        if 'data' not in api_data or not isinstance(api_data['data'], list):
            logging.warning(f"Tiki - Dữ liệu từ trang {page} không hợp lệ. Dừng.")
            break

        # Parse dữ liệu từ response
        for product in api_data['data']:
            product_name = product.get('name') or ''
            product_price = product.get('price')
            
            # Xử lý trường quantity_sold - có thể là dict hoặc int
            sold_info = product.get('quantity_sold', {})
            sold_count = sold_info.get('value', 0) if sold_info and isinstance(sold_info, dict) else 0

            # --- CỐ GẮNG LẤY DỮ LIỆU THẬT TỪ API ---
            original_price = product.get('original_price') or product.get('list_price')
            discount_rate = product.get('discount_rate')
            
            # Lấy thông tin shop (Tiki thường có field seller)
            # API structure có thể thay đổi, cố gắng lấy an toàn
            seller = product.get('seller_product_detail', {}).get('store_info', {})
            shop_name = seller.get('name')
            
            # TikiNow giao nhanh -> phí ship thường cao hơn hoặc free nếu có tiki now
            is_tikinow = product.get('tiki_now') 
            
            # Tạo dict với các trường mới
            product_data = {
                "name_raw": product_name,
                "price_raw": product_price,
                "sold_raw": sold_count,
                "review_count": product.get('review_count') or 0,
                "review_score": product.get('rating_average') or 0,
                # Thêm trường mới cho Insight
                "original_price": original_price,
                "discount_rate": discount_rate,
                "shop_name": shop_name,
                # Các trường này API khó lấy ngay, để None để hàm enrich tự fake
                "shop_location": None, 
                "shipping_fee_est": None,
                "stock_status": None
            }
            
            # GỌI HÀM ENRICH ĐỂ BỔ SUNG DỮ LIỆU (Hybrid: Thật + Fake)
            final_product = enrich_product_data(product_data)
            all_products_data.append(final_product)

        # Delay để tránh bị chặn IP
        time.sleep(REQUEST_DELAY)
    
    logging.info(f"Tiki - Crawl hoàn tất! Đã thu thập {len(all_products_data)} sản phẩm thô.")
    return all_products_data


# ============================================================================
# TRANSFORM FUNCTIONS - Làm sạch và chuẩn hóa dữ liệu
# ============================================================================

def transform_tiki_data(data: List[Dict]) -> List[Dict]:
    """
    Transform và làm sạch dữ liệu sản phẩm Tiki.
    
    Hàm này thực hiện:
        1. Làm sạch tên sản phẩm (xóa '...', strip whitespace)
        2. Chuẩn hóa giá về dạng int (xử lý cả string và int)
        3. Chuẩn hóa số lượng bán (xử lý 'k' notation: 1.2k -> 1200)
        4. Xác định thương hiệu dựa trên keywords trong tên
        5. Phân loại category (Điện thoại, Laptop, Máy tính bảng, Accessory)
    
    Args:
        data (List[Dict]): Danh sách sản phẩm thô từ extract_tiki_data()
    
    Returns:
        List[Dict]: Danh sách sản phẩm đã được làm sạch và chuẩn hóa:
            - name: Tên sản phẩm (cleaned)
            - price: Giá (int)
            - sold_count: Số lượng đã bán (int)
            - review_count: Số lượng đánh giá (int)
            - review_score: Điểm đánh giá (float)
            - brand: Thương hiệu (str)
            - category: Loại sản phẩm (str)
            - source: Nguồn dữ liệu = 'Tiki'
            - original_price: Giá gốc
            - discount_rate: % Giảm giá
            - shop_name: Tên shop
            - shop_location: Vị trí shop
            - shipping_fee_est: Phí ship ước tính
            - stock_status: Tình trạng kho
            - rating_count_5s: Số lượng 5 sao
            - rating_count_1s: Số lượng 1 sao
    """
    logging.info(f"Bắt đầu transform {len(data)} sản phẩm Tiki...")
    transformed_products = []
    
    for product in data:
        # 1. Làm sạch tên sản phẩm
        name_clean = (product.get('name_raw') or '').replace('...', '').strip()
        
        # 2. Làm sạch giá: hỗ trợ cả int và string
        # Tiki API thường trả về int, nhưng đôi khi có thể là string
        price_clean = 0
        raw_price = product.get('price_raw')
        if isinstance(raw_price, (int, float)):
            price_clean = int(raw_price)
        elif raw_price:
            try:
                # Loại bỏ tất cả ký tự không phải số
                price_clean = int(re.sub(r'[^\d]', '', str(raw_price)))
            except ValueError:
                logging.warning(f"Không thể làm sạch giá: {raw_price}")

        # 3. Làm sạch đã bán: xử lý cả số nguyên và notation 'k'
        # Ví dụ: 'Đã bán 1.2k' -> 1200, 'Đã bán 5' -> 5
        sold_raw = product.get('sold_raw', 0)
        sold_clean = 0
        try:
            if isinstance(sold_raw, (int, float)):
                sold_clean = int(sold_raw)
            else:
                # Extract số từ string
                match = re.search(r'[\d\.]+', str(sold_raw))
                if match:
                    sold_value = float(match.group(0))
                    # Nếu có 'k' thì nhân với 1000
                    if 'k' in str(sold_raw).lower():
                        sold_clean = int(sold_value * 1000)
                    else:
                        sold_clean = int(sold_value)
        except (ValueError, TypeError):
            logging.warning(f"Không thể làm sạch dữ liệu bán: {sold_raw}")

        # 4. Làm sạch số lượng đánh giá và điểm đánh giá
        review_count = int(product.get('review_count', 0))
        review_score = float(product.get('review_score', 0))

        # 5. Chuẩn hóa thương hiệu và loại sản phẩm
        name_lower = name_clean.lower()
        
        # Xác định thương hiệu dựa trên keywords
        brand = "Unknown"
        for brand_name, keywords in BRAND_KEYWORDS.items():
            if any(keyword in name_lower for keyword in keywords):
                brand = brand_name
                break

        # 6. Phân loại sản phẩm dựa trên tên
        category = "Accessory"  # Default category
        if "điện thoại" in name_lower or "iphone" in name_lower:
            category = "Điện thoại"
        elif "laptop" in name_lower or "macbook" in name_lower or "thinkpad" in name_lower or "xps" in name_lower:
            category = "Laptop"
        elif "máy tính bảng" in name_lower or "ipad" in name_lower or "galaxy tab" in name_lower:
            category = "Máy tính bảng"

        # 7. Tạo product record đã được transform
        transformed_products.append({
            'name': name_clean,
            'price': price_clean,
            'sold_count': sold_clean,
            'review_count': review_count,
            'review_score': review_score,
            'brand': brand,
            'category': category,
            'source': 'Tiki',  # Đánh dấu nguồn của sản phẩm
            # Pass các trường đã được enrich
            'original_price': product.get('original_price'),
            'discount_rate': product.get('discount_rate'),
            'shop_name': product.get('shop_name'),
            'shop_location': product.get('shop_location'),
            'shipping_fee_est': product.get('shipping_fee_est'),
            'stock_status': product.get('stock_status'),
            'rating_count_5s': product.get('rating_count_5s'),
            'rating_count_1s': product.get('rating_count_1s')
        })
    
    logging.info(f"Tiki - Transform hoàn tất! {len(transformed_products)} sản phẩm đã được làm sạch.")
    return transformed_products


# ============================================================================
# MAIN ETL FUNCTION - Entry point cho Airflow
# ============================================================================

def run_tiki_etl(
    search_queries: List[str] = None,
    pages: int = 8
) -> List[Dict]:
    """
    Hàm chính để chạy ETL pipeline cho Tiki.
    
    Đây là entry point được gọi từ Airflow DAG. Hàm này sẽ:
        1. Crawl dữ liệu cho tất cả search queries
        2. Transform và làm sạch dữ liệu
        3. Aggregate tất cả dữ liệu từ các queries khác nhau
    
    Args:
        search_queries (List[str], optional): Danh sách từ khóa tìm kiếm.
            Mặc định: ["dien thoai", "laptop", "may tinh bang"]
        pages (int): Số trang cần crawl cho mỗi query. Mặc định: 8
    
    Returns:
        List[Dict]: Danh sách tất cả sản phẩm đã được transform từ tất cả queries
    
    Example:
        >>> # Chạy với queries mặc định
        >>> products = run_tiki_etl()
        >>> 
        >>> # Chạy với custom queries
        >>> products = run_tiki_etl(search_queries=["laptop"], pages=5)
    """
    # Sử dụng default queries nếu không được truyền vào
    if search_queries is None:
        search_queries = ["dien thoai", "laptop", "may tinh bang"]
    
    all_transformed_data = []
    
    for search_query in search_queries:
        logging.info(f"Tiki - Bắt đầu ETL cho query: '{search_query}'")
        
        # Step 1: Extract - Lấy dữ liệu thô từ Tiki
        raw_data = extract_tiki_data(search_query=search_query, pages=pages)
        if not raw_data:
            logging.warning(f"Tiki - Không có dữ liệu thô cho {search_query}, bỏ qua query này.")
            continue
        
        # Step 2: Transform - Chuyển đổi dữ liệu thô thành dữ liệu sạch
        transformed_data = transform_tiki_data(raw_data)
        
        # Step 3: Aggregate - Thêm dữ liệu của từng nhóm sản phẩm vào danh sách chung
        all_transformed_data.extend(transformed_data)
    
    logging.info(f"Tiki - ETL hoàn tất! Tổng cộng {len(all_transformed_data)} sản phẩm.")
    return all_transformed_data
