"""
ETL Module cho Lazada - Extract, Transform, Load dữ liệu sản phẩm từ Lazada.vn

Module này sử dụng Selenium WebDriver để crawl dữ liệu sản phẩm từ Lazada.vn.
Do Lazada render động bằng JavaScript, cần sử dụng Selenium thay vì HTTP requests.

Quy trình ETL:
    - Extract: Crawl dữ liệu bằng Selenium (tên, giá, số lượng bán)
    - Enrich: Lấy thông tin review song song bằng concurrent requests
    - Transform: Làm sạch và chuẩn hóa dữ liệu (giá, sold count, brand, category)
    - Load: Trả về dữ liệu đã xử lý cho Airflow DAG

Technical Notes:
    - Sử dụng headless Chrome để tăng performance
    - Concurrent requests (ThreadPoolExecutor) để lấy reviews nhanh hơn
    - Parse reviews từ JSON-LD schema trong HTML

Author: Data Team
Last Updated: 2025-01-20
"""

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from requests import RequestException
from data_enricher import enrich_product_data  # Import module làm giàu dữ liệu
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ============================================================================
# CONFIGURATION - Cấu hình constants
# ============================================================================

# Chrome Driver Configuration
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")

# HTTP Request Configuration
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Selenium Timeouts
DEFAULT_PAGE_LOAD_TIMEOUT = 20  # Thời gian chờ load trang (giây)
DEFAULT_SCROLL_ITERATIONS = 3  # Số lần scroll để load lazy content
SCROLL_DELAY = 1.2  # Delay giữa mỗi lần scroll (giây)

# Review Fetching Configuration
DEFAULT_REVIEW_TIMEOUT = 15  # Timeout cho mỗi request lấy review (giây)
DEFAULT_REVIEW_RETRIES = 2  # Số lần retry khi lấy review thất bại
DEFAULT_REVIEW_WORKERS = 8  # Số threads song song để lấy reviews

# CSS Selectors - Các selector để tìm elements trên Lazada
PRODUCT_ITEM_SELECTOR = "div[data-qa-locator='product-item']"
PRODUCT_NAME_SELECTOR = "a[title]"
PRODUCT_PRICE_SELECTOR = "span.ooOxS"
PRODUCT_SOLD_SELECTOR = "span.sales"
PRODUCT_ORIGINAL_PRICE_SELECTOR = "span.del"  # Thường là giá gạch ngang (cần verify với HTML thực tế)
PRODUCT_LOCATION_SELECTOR = "span.item-location" # Vị trí shop (cần verify)

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
# SELENIUM HELPER FUNCTIONS - Setup và quản lý WebDriver
# ============================================================================

def _build_chrome_options() -> Options:
    """
    Tạo Chrome Options cho Selenium WebDriver.
    
    Cấu hình headless mode và các options để tránh bị detect là bot:
        - Headless mode (không hiện UI)
        - Disable GPU acceleration
        - No sandbox (cần cho Docker)
        - Disable automation flags
        - Custom user agent
    
    Returns:
        Options: Chrome options đã được cấu hình
    """
    options = Options()
    
    # Performance options
    options.add_argument("--headless")  # Chạy ẩn (không mở browser window)
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument("--no-sandbox")  # Cần thiết khi chạy trong Docker
    options.add_argument("--disable-dev-shm-usage")  # Tránh lỗi shared memory
    
    # Anti-detection options
    options.add_argument("--disable-blink-features=AutomationControlled")  # Ẩn automation flag
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Window và user agent
    options.add_argument("--window-size=1920x1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    return options


def _create_driver(chromedriver_path: str = None, options: Options = None) -> webdriver.Chrome:
    """
    Tạo Chrome WebDriver instance.
    
    Args:
        chromedriver_path (str, optional): Đường dẫn tới chromedriver binary.
            Nếu None, sẽ dùng CHROMEDRIVER_PATH từ env var.
        options (Options, optional): Chrome options. Nếu None, sẽ tạo mặc định.
    
    Returns:
        webdriver.Chrome: WebDriver instance đã được khởi tạo
    
    Raises:
        Exception: Nếu không tìm thấy chromedriver tại đường dẫn chỉ định
    """
    chromedriver_path = chromedriver_path or CHROMEDRIVER_PATH
    
    # Kiểm tra chromedriver có tồn tại không
    if not os.path.exists(chromedriver_path):
        logging.error(f"Không tìm thấy chromedriver tại: {chromedriver_path}")
        raise Exception("Chromedriver không tìm thấy.")
    
    options = options or _build_chrome_options()
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    
    logging.info(f"Khởi tạo ChromeDriver thành công tại {chromedriver_path}.")
    return driver


# ============================================================================
# EXTRACT FUNCTIONS - Crawl dữ liệu từ Lazada bằng Selenium
# ============================================================================

def extract_lazada_data(
    search_query: str = "dien thoai",
    pages: int = 8,
    max_products_per_query: Optional[int] = None,
    review_timeout: int = DEFAULT_REVIEW_TIMEOUT,
    review_retries: int = DEFAULT_REVIEW_RETRIES,
    review_workers: int = DEFAULT_REVIEW_WORKERS,
) -> List[Dict]:
    """
    Extract dữ liệu sản phẩm từ Lazada bằng Selenium.
    
    Hàm này sẽ:
        1. Khởi tạo Selenium WebDriver
        2. Duyệt qua các trang kết quả tìm kiếm
        3. Scroll để load lazy content
        4. Extract thông tin cơ bản (tên, giá, sold count)
        5. Lấy reviews song song (concurrent) để tăng tốc độ
    
    Args:
        search_query (str): Từ khóa tìm kiếm (vd: "dien thoai", "laptop")
        pages (int): Số trang cần crawl
        max_products_per_query (int, optional): Giới hạn số sản phẩm tối đa.
            Nếu None, crawl hết tất cả sản phẩm tìm được.
        review_timeout (int): Timeout cho mỗi request lấy review (giây)
        review_retries (int): Số lần retry khi lấy review thất bại
        review_workers (int): Số threads song song để lấy reviews
    
    Returns:
        List[Dict]: Danh sách sản phẩm thô chứa:
            - name_raw: Tên sản phẩm
            - price_raw: Giá (string với format '16.390.000 ₫')
            - sold_raw: Số lượng đã bán (string)
            - review_count: Số lượng đánh giá
            - review_score: Điểm đánh giá trung bình
    """
    logging.info(f"Bắt đầu crawl Lazada bằng Selenium với từ khóa '{search_query}'...")
    
    options = _build_chrome_options()
    driver = _create_driver(options=options)
    
    raw_products_data: List[Dict] = []
    collected = 0
    
    try:
        for page in range(1, pages + 1):
            # Kiểm tra đã đủ số lượng sản phẩm chưa
            if max_products_per_query and collected >= max_products_per_query:
                logging.info("Đã đạt giới hạn sản phẩm cần thu thập, dừng trang tiếp theo.")
                break
            
            # Tạo URL và load trang
            url = f"https://www.lazada.vn/catalog/?q={search_query}&page={page}"
            logging.info(f"Tải trang {page}/{pages}: {url}")
            driver.get(url)
            
            # Chờ sản phẩm xuất hiện trên trang
            try:
                WebDriverWait(driver, DEFAULT_PAGE_LOAD_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR))
                )
            except Exception as e:
                logging.warning(f"Không tìm thấy sản phẩm ở trang {page}: {e}")
                continue

            # Scroll để load lazy content (Lazada dùng lazy loading)
            for _ in range(DEFAULT_SCROLL_ITERATIONS):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_DELAY)

            # Extract thông tin sản phẩm từ trang
            product_elements = driver.find_elements(By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR)
            logging.info(f"Tìm thấy {len(product_elements)} sản phẩm ở trang {page}.")

            page_products: List[Dict] = []

            for item in product_elements:
                # Kiểm tra giới hạn số lượng
                if max_products_per_query and collected + len(page_products) >= max_products_per_query:
                    logging.info("Đã đạt giới hạn sản phẩm cần thu thập, dừng lấy thêm sản phẩm.")
                    break

                # Extract các thông tin cơ bản
                name, price, sold_text, product_url = "", "", "", ""
                
                try:
                    name = item.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR).text
                except Exception:
                    pass
                
                try:
                    price = item.find_element(By.CSS_SELECTOR, PRODUCT_PRICE_SELECTOR).text
                except Exception:
                    pass
                
                try:
                    sold_text = item.find_element(By.CSS_SELECTOR, PRODUCT_SOLD_SELECTOR).text
                except Exception:
                    pass
                
                    product_url = item.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR).get_attribute('href')
                except Exception:
                    pass

                # --- CỐ GẮNG LẤY DỮ LIỆU THẬT CHO INSIGHT ---
                original_price_text = ""
                location_text = ""
                
                try:
                    original_price_text = item.find_element(By.CSS_SELECTOR, PRODUCT_ORIGINAL_PRICE_SELECTOR).text
                except: pass
                
                try:
                    location_text = item.find_element(By.CSS_SELECTOR, PRODUCT_LOCATION_SELECTOR).text
                except: pass

                # Chỉ lưu sản phẩm có đủ thông tin cơ bản
                if name and price and product_url:
                    page_products.append({
                        "name_raw": name,
                        "price_raw": price,
                        "sold_raw": sold_text,
                        "product_url": product_url,
                        # Lưu raw text cho các trường mới, sẽ clean ở bước Transform
                        "original_price_raw": original_price_text,
                        "shop_location_raw": location_text,
                    })

            # Lấy reviews song song để tăng tốc độ (quan trọng!)
            _attach_reviews_concurrently(
                page_products,
                timeout=review_timeout,
                retries=review_retries,
                max_workers=review_workers,
            )

            # Thêm vào danh sách tổng và cập nhật counter
            for product in page_products:
                raw_products_data.append({
                    "name_raw": product["name_raw"],
                    "price_raw": product["price_raw"],
                    "sold_raw": product["sold_raw"],
                    "review_count": product.get("review_count", 0),
                    "review_score": product.get("review_score", 0),
                    "original_price_raw": product.get("original_price_raw"),
                    "shop_location_raw": product.get("shop_location_raw"),
                })
                collected += 1
                
    finally:
        # Đảm bảo đóng driver dù có lỗi hay không
        try:
            driver.quit()
        except Exception:
            pass

    logging.info(f"Tổng cộng thu {len(raw_products_data)} sản phẩm từ {pages} trang.")
    return raw_products_data


# ============================================================================
# REVIEW FETCHING - Lấy thông tin đánh giá sản phẩm
# ============================================================================

def _attach_reviews_concurrently(
    products: List[Dict],
    timeout: int = 15,
    retries: int = 2,
    max_workers: int = 8,
) -> None:
    """
    Gọi song song get_product_reviews để giảm thời gian xử lý.
    
    Thay vì lấy reviews tuần tự (chậm), hàm này sử dụng ThreadPoolExecutor
    để gọi nhiều requests song song, giảm thời gian chờ đợi đáng kể.
    
    Args:
        products (List[Dict]): Danh sách sản phẩm cần lấy reviews.
            Hàm sẽ modify in-place, thêm 'review_count' và 'review_score'.
        timeout (int): Timeout cho mỗi request lấy review
        retries (int): Số lần retry khi thất bại
        max_workers (int): Số threads tối đa chạy song song
    
    Side Effects:
        Modify products in-place, thêm fields:
            - review_count (int): Số lượng đánh giá
            - review_score (float): Điểm đánh giá trung bình
    """
    # Extract danh sách URL và index
    urls = [(idx, p.get("product_url")) for idx, p in enumerate(products) if p.get("product_url")]
    if not urls:
        return

    # Tối ưu số workers (không tạo quá nhiều threads nếu có ít URLs)
    workers = max(1, min(max_workers, len(urls)))
    
    # Nếu chỉ có 1 URL thì không cần concurrent
    if workers == 1:
        for idx, url in urls:
            products[idx].update(
                _safe_get_reviews(url, timeout=timeout, retries=retries)
            )
        return

    # Concurrent execution với ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit tất cả tasks
        futures = {
            executor.submit(_safe_get_reviews, url, timeout, retries): idx
            for idx, url in urls
        }
        
        # Process kết quả khi hoàn thành
        for future in as_completed(futures):
            idx = futures[future]
            try:
                products[idx].update(future.result())
            except Exception as err:
                logging.warning(f"Lỗi khi gắn review (idx={idx}): {err}")
                # Set default values nếu lỗi
                products[idx].setdefault("review_count", 0)
                products[idx].setdefault("review_score", 0.0)


def _safe_get_reviews(url: str, timeout: int, retries: int) -> Dict:
    """
    Wrapper an toàn cho get_product_reviews.
    
    Đảm bảo luôn trả về dict với review_count và review_score,
    ngay cả khi có lỗi.
    
    Args:
        url (str): URL của sản phẩm
        timeout (int): Timeout cho request
        retries (int): Số lần retry
    
    Returns:
        Dict: {"review_count": int, "review_score": float}
    """
    data = get_product_reviews(product_url=url, timeout=timeout, retries=retries)
    
    # Đảm bảo luôn có các fields cần thiết
    if "review_count" not in data:
        data["review_count"] = 0
    if "review_score" not in data:
        data["review_score"] = 0.0
    
    return data


def get_product_reviews(
    product_url: str,
    session: Optional[requests.Session] = None,
    timeout: int = 20,
    retries: int = 3
) -> Dict:
    """
    Lấy thông tin review của sản phẩm từ URL.
    
    Lazada embed thông tin review vào JSON-LD schema trong HTML.
    Hàm này sẽ:
        1. Fetch HTML của product page
        2. Parse JSON-LD schema từ <script type="application/ld+json">
        3. Extract aggregateRating (reviewCount, ratingValue)
    
    Args:
        product_url (str): URL của sản phẩm cần lấy review
        session (requests.Session, optional): Session để reuse connection.
            Nếu None, sẽ tạo session mới.
        timeout (int): Timeout cho HTTP request (giây)
        retries (int): Số lần retry khi request thất bại
    
    Returns:
        Dict: {
            "review_count": int,  # Số lượng đánh giá
            "review_score": float  # Điểm đánh giá (0-5)
        }
    
    Example:
        >>> get_product_reviews("https://www.lazada.vn/products/iphone-15-...")
        {"review_count": 1234, "review_score": 4.5}
    """
    reviews_info = {"review_count": 0, "review_score": 0.0}
    if not product_url:
        return reviews_info

    # Tạo session nếu chưa có
    should_close = False
    if session is None:
        session = requests.Session()
        should_close = True

    # Retry logic
    for attempt in range(1, retries + 1):
        try:
            response = session.get(product_url, headers=REQUEST_HEADERS, timeout=timeout)
            response.raise_for_status()
            
            # Parse reviews từ HTML
            parsed = _parse_reviews_from_html(response.text)
            if parsed["review_count"] or parsed["review_score"]:
                reviews_info = parsed
            break  # Success, thoát khỏi retry loop
            
        except RequestException as err:
            logging.warning(f"Lỗi khi lấy đánh giá từ {product_url} (attempt {attempt}): {err}")
            # Exponential backoff với cap
            time.sleep(min(3 * attempt, 10))
        except Exception as err:
            logging.warning(f"Không thể phân tích review từ {product_url}: {err}")
            break  # Không có ích khi retry lỗi parsing

    # Đóng session nếu tự tạo
    if should_close:
        session.close()

    return reviews_info


def _parse_reviews_from_html(html: str) -> Dict:
    """
    Parse thông tin rating từ JSON-LD schema trong HTML.
    
    Lazada nhúng thông tin rating vào script type="application/ld+json".
    Format chuẩn theo schema.org/Product với aggregateRating.
    
    Args:
        html (str): HTML content của product page
    
    Returns:
        Dict: {
            "review_count": int,
            "review_score": float
        }
    
    Technical Details:
        - Tìm tất cả <script type="application/ld+json">
        - Parse JSON từ mỗi script
        - Tìm field "aggregateRating"
        - Extract reviewCount và ratingValue
    """
    reviews_info = {"review_count": 0, "review_score": 0.0}
    if not html:
        return reviews_info

    # Regex để tìm JSON-LD scripts
    # Pattern: <script type="application/ld+json">...</script>
    scripts = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL | re.IGNORECASE
    )
    
    for script in scripts:
        script = script.strip()
        if not script:
            continue
        
        try:
            data = json.loads(script)
        except json.JSONDecodeError:
            # Một số script có comments HTML, thử clean và parse lại
            try:
                data = json.loads(script.lstrip('<!--').rstrip('-->'))
            except Exception:
                continue

        # Data có thể là list hoặc single object
        if isinstance(data, list):
            candidates = data
        else:
            candidates = [data]

        # Tìm aggregateRating trong candidates
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            
            rating_data = candidate.get("aggregateRating")
            if not rating_data:
                continue
            
            try:
                # Extract và convert sang đúng type
                reviews_info["review_count"] = int(float(rating_data.get("reviewCount", 0) or 0))
                reviews_info["review_score"] = float(rating_data.get("ratingValue", 0) or 0)
                return reviews_info  # Found! Return ngay
            except (TypeError, ValueError):
                # Invalid data, tiếp tục tìm
                continue

    return reviews_info


# ============================================================================
# TRANSFORM FUNCTIONS - Làm sạch và chuẩn hóa dữ liệu
# ============================================================================

def transform_lazada_data(data: List[Dict]) -> List[Dict]:
    """
    Transform và làm sạch dữ liệu sản phẩm Lazada.
    
    Hàm này thực hiện:
        1. Làm sạch tên sản phẩm (xóa '...', strip whitespace)
        2. Chuẩn hóa giá về dạng int (loại bỏ dấu chấm, ₫, khoảng trắng)
        3. Chuẩn hóa số lượng bán (xử lý 'k' notation: 1.2k -> 1200)
        4. Xác định thương hiệu dựa trên keywords trong tên
        5. Phân loại category (Điện thoại, Laptop, Máy tính bảng, Accessory)
    
    Args:
        data (List[Dict]): Danh sách sản phẩm thô từ extract_lazada_data()
    
    Returns:
        List[Dict]: Danh sách sản phẩm đã được làm sạch:
            - name: Tên sản phẩm (cleaned)
            - price: Giá (int)
            - sold_count: Số lượng đã bán (int)
            - review_count: Số lượng đánh giá (int)
            - review_score: Điểm đánh giá (float)
            - brand: Thương hiệu (str)
            - category: Loại sản phẩm (str)
            - source: Nguồn dữ liệu = 'Lazada'
            - original_price: Giá gốc
            - shop_location: Vị trí shop
            - shop_name: Tên shop
            - ... (các trường enriched khác)
    """
    logging.info(f"Bắt đầu transform {len(data)} sản phẩm Lazada...")
    transformed_products = []
    
    for product in data:
        # 1. Làm sạch tên sản phẩm
        name_clean = product['name_raw'].replace('...', '').strip()
        
        # 2. Làm sạch giá: '16.390.000 ₫' -> 16390000
        # Loại bỏ: dấu chấm, khoảng trắng, ký hiệu ₫
        price_clean = 0
        try:
            price_clean = re.sub(r'[.\s₫]', '', product['price_raw'])
            price_clean = int(price_clean)
        except ValueError:
            logging.warning(f"Không thể làm sạch giá: {product['price_raw']}")

        # 3. Làm sạch đã bán: 'Đã bán 1.2k' -> 1200, 'Đã bán 5' -> 5
        sold_clean = 0
        try:
            # Extract số từ string (ví dụ: "1.2" từ "Đã bán 1.2k")
            sold_match = re.search(r'[\d\.]+', product['sold_raw'])
            if sold_match:
                sold_str = sold_match.group(0)
                # Nếu có 'k' thì nhân 1000
                if 'k' in product['sold_raw'].lower():
                    sold_clean = int(float(sold_str) * 1000)
                else:
                    sold_clean = int(sold_str)
        except:
            logging.warning(f"Không thể làm sạch dữ liệu bán: {product['sold_raw']}")

        # 4. Làm sạch review data
        review_count = int(product.get('review_count', 0))
        review_score = float(product.get('review_score', 0))

        # 5. Chuẩn hóa thương hiệu
        name_lower = name_clean.lower()
        
        brand = "Unknown"
        for brand_name, keywords in BRAND_KEYWORDS.items():
            if any(keyword in name_lower for keyword in keywords):
                brand = brand_name
                break

        # 6. Phân loại sản phẩm
        category = "Accessory"  # Default
        if "điện thoại" in name_lower or "iphone" in name_lower:
            category = "Điện thoại"
        elif "laptop" in name_lower or "macbook" in name_lower or "thinkpad" in name_lower or "xps" in name_lower:
            category = "Laptop"
        elif "máy tính bảng" in name_lower or "ipad" in name_lower or "galaxy tab" in name_lower:
            category = "Máy tính bảng"

        # 8. Clean các trường mới (Original Price & Location)
        original_price = 0
        if product.get('original_price_raw'):
            try:
                # Remove non-digits
                op_clean = re.sub(r'[^\d]', '', product['original_price_raw'])
                original_price = int(op_clean)
            except: pass
            
        shop_location = product.get('shop_location_raw')
        if shop_location:
            shop_location = shop_location.strip()

        # 9. Tạo dict sơ bộ
        cleaned_item = {
            'name': name_clean,
            'price': price_clean,
            'sold_count': sold_clean,
            'review_count': review_count,
            'review_score': review_score,
            'brand': brand,
            'category': category,
            'source': 'Lazada',
            # Pass các trường đã lấy được
            'original_price': original_price if original_price > 0 else None,
            'shop_location': shop_location,
            'shop_name': None, # Selenium khó lấy tên shop ở trang search, để Fake
        }
        
        # --- BƯỚC CUỐI: ENRICH (Điền dữ liệu giả cho các trường thiếu) ---
        enriched_item = enrich_product_data(cleaned_item)
        
        transformed_products.append(enriched_item)
    
    logging.info(f"Transform hoàn tất! {len(transformed_products)} sản phẩm đã được làm sạch.")
    return transformed_products


# ============================================================================
# MAIN ETL FUNCTION - Entry point cho Airflow
# ============================================================================

def run_lazada_etl(
    search_query: str = None,
    search_queries: List[str] = None,
    pages: int = 8,
    max_products_per_query: Optional[int] = 150,
) -> List[Dict]:
    """
    Hàm chính để chạy ETL pipeline cho Lazada.
    
    Đây là entry point được gọi từ Airflow DAG. Hàm này sẽ:
        1. Normalize input (single query hoặc multiple queries)
        2. Crawl dữ liệu cho tất cả queries bằng Selenium
        3. Transform và làm sạch dữ liệu
        4. Aggregate tất cả dữ liệu từ các queries khác nhau
    
    Args:
        search_query (str, optional): Từ khóa tìm kiếm đơn.
            Không thể dùng cùng lúc với search_queries.
        search_queries (List[str], optional): Danh sách từ khóa tìm kiếm.
            Mặc định: ["dien thoai", "laptop", "may tinh bang"]
        pages (int): Số trang cần crawl cho mỗi query
        max_products_per_query (int, optional): Giới hạn số sản phẩm cho mỗi query.
            Nếu None, crawl hết tất cả sản phẩm.
    
    Returns:
        List[Dict]: Danh sách tất cả sản phẩm đã được transform
    
    Raises:
        ValueError: Nếu cả search_query và search_queries đều được truyền vào
    
    Example:
        >>> # Chạy với single query
        >>> products = run_lazada_etl(search_query="laptop", pages=5)
        >>> 
        >>> # Chạy với multiple queries
        >>> products = run_lazada_etl(
        ...     search_queries=["dien thoai", "laptop"],
        ...     pages=6,
        ...     max_products_per_query=180
        ... )
    """
    # Validate input
    if search_query and search_queries:
        raise ValueError("Chỉ nên truyền search_query hoặc search_queries, không phải cả hai.")

    # Normalize queries
    if search_query:
        normalized_queries = [search_query]
    elif search_queries:
        normalized_queries = search_queries
    else:
        # Default queries
        normalized_queries = ["dien thoai", "laptop", "may tinh bang"]

    all_transformed_data = []
    
    for current_query in normalized_queries:
        logging.info(f"Lazada - Bắt đầu ETL cho query: '{current_query}'")
        
        # Step 1: Extract - Lấy dữ liệu thô từ Lazada
        raw_data = extract_lazada_data(
            search_query=current_query,
            pages=pages,
            max_products_per_query=max_products_per_query,
        )
        
        if not raw_data:
            logging.warning(f"Lazada - Không có dữ liệu thô cho {current_query}, bỏ qua.")
            continue
        
        # Step 2: Transform - Chuyển đổi dữ liệu thô thành dữ liệu sạch
        transformed_data = transform_lazada_data(raw_data)
        
        # Step 3: Aggregate - Thêm dữ liệu vào danh sách chung
        all_transformed_data.extend(transformed_data)

    logging.info(f"Lazada - ETL hoàn tất! Tổng cộng {len(all_transformed_data)} sản phẩm.")
    return all_transformed_data
