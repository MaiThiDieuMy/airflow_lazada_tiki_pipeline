"""
Module: data_enricher.py
Description: Module làm giàu dữ liệu và giả lập thông minh (Smart Mocking) 
để tạo ra các Insight có tính tương quan cao cho Dashboard.
Module này thực hiện trọn vẹn logic ETL: Làm sạch -> Giả lập -> Tính toán chỉ số.

Author: Data Team
Last Updated: 2025-01-26
"""

"""
Module: data_enricher.py
Description: Module làm giàu dữ liệu.
Nhiệm vụ cốt lõi: Đảm bảo KHÔNG CÓ TRƯỜNG QUAN TRỌNG NÀO BỊ NULL HOẶC 0.
"""

import random
from typing import Dict, Any

# Danh sách Shop giả định
MOCK_SHOPS_TIKI = [
    {"name": "Tiki Trading", "loc": "Hồ Chí Minh", "rating": 4.9},
    {"name": "Samsung Flagship Store", "loc": "Hà Nội", "rating": 4.8},
    {"name": "Anker Official", "loc": "Hồ Chí Minh", "rating": 4.7},
    {"name": "Minh Tuan Mobile", "loc": "Hồ Chí Minh", "rating": 4.5},
]

MOCK_SHOPS_LAZADA = [
    {"name": "LazMall Apple", "loc": "Hồ Chí Minh", "rating": 4.9},
    {"name": "Phụ Kiện Giá Xưởng", "loc": "Quốc Tế", "rating": 4.1},
    {"name": "Tech Zone Global", "loc": "Hà Nội", "rating": 4.3},
    {"name": "Shop Bán Rẻ", "loc": "Lạng Sơn", "rating": 3.8},
]

def enrich_product_data(product: Dict[str, Any]) -> Dict[str, Any]:
    source = product.get('source', 'Unknown')
    price = product.get('price', 0)
    
    # -------------------------------------------------------
    # 1. FAKE SỐ LƯỢNG BÁN (SOLD COUNT) - QUAN TRỌNG NHẤT
    # -------------------------------------------------------
    # Nếu crawler không lấy được (0 hoặc None), ta fake số liệu
    # để sau này tính doanh thu không bị = 0.
    sold_count = product.get('sold_count')
    
    if not sold_count or sold_count == 0:
        # Random từ 50 đến 5000 sản phẩm đã bán
        # Ưu tiên số lớn hơn một chút để nhìn Doanh thu cho "khủng"
        sold_count = random.choice([random.randint(50, 200), random.randint(500, 5000)])
    
    product['sold_count'] = sold_count

    # -------------------------------------------------------
    # 2. FAKE ĐÁNH GIÁ (REVIEW COUNT & SCORE)
    # -------------------------------------------------------
    review_count = product.get('review_count')
    if not review_count or review_count == 0:
        # Giả định 5-10% người mua sẽ review
        review_count = int(sold_count * random.uniform(0.05, 0.1))
        if review_count == 0: review_count = random.randint(1, 10)
    product['review_count'] = review_count

    # Fake điểm đánh giá nếu thiếu
    if not product.get('review_score') or product.get('review_score') == 0:
        product['review_score'] = round(random.uniform(3.5, 5.0), 1)

    # Phân bổ 5 sao và 1 sao (cho biểu đồ Quality)
    if product['review_score'] >= 4.5:
        product['rating_count_5s'] = int(review_count * 0.9)
        product['rating_count_1s'] = int(review_count * 0.02)
    else:
        product['rating_count_5s'] = int(review_count * 0.5)
        product['rating_count_1s'] = int(review_count * 0.3)

    # -------------------------------------------------------
    # 3. FAKE GIÁ GỐC (ORIGINAL PRICE) - ĐỂ TÍNH DISCOUNT
    # -------------------------------------------------------
    original_price = product.get('original_price')
    if not original_price or original_price <= price:
        # Tạo Discount giả từ 10% - 40%
        markup = random.uniform(1.1, 1.4)
        original_price = int(price * markup)
        # Làm tròn số hàng nghìn
        original_price = (original_price // 1000) * 1000
    
    product['original_price'] = original_price
    
    # Tính lại discount_rate cho chính xác
    product['discount_rate'] = int(((original_price - price) / original_price) * 100)

    # -------------------------------------------------------
    # 4. FAKE PHÍ SHIP (SHIPPING FEE) - ĐỂ SO SÁNH GIÁ LĂN BÁNH
    # -------------------------------------------------------
    shipping_fee = product.get('shipping_fee_est')
    if shipping_fee is None:
        if source == 'Tiki':
            # Tiki ship rẻ/free
            shipping_fee = 0 if random.random() < 0.7 else random.choice([12000, 15000])
        else:
            # Lazada ship đắt hơn
            shipping_fee = 0 if random.random() < 0.2 else random.choice([25000, 32000, 45000])
    product['shipping_fee_est'] = shipping_fee

    # -------------------------------------------------------
    # 5. FAKE SHOP & KHO
    # -------------------------------------------------------
    if not product.get('shop_name'):
        shop_data = random.choice(MOCK_SHOPS_TIKI if source == 'Tiki' else MOCK_SHOPS_LAZADA)
        product['shop_name'] = shop_data['name']
        product['shop_location'] = shop_data['loc']
    
    if not product.get('stock_status'):
        # Logic: Bán nhiều (>1000) dễ hết hàng
        if sold_count > 1000:
            product['stock_status'] = random.choice(["Sắp hết hàng", "Hết hàng", "Còn hàng"])
        else:
            product['stock_status'] = "Còn hàng"

    # -------------------------------------------------------
    # 6. TÍNH DOANH THU THÁNG (SNAPSHOT)
    # -------------------------------------------------------
    # Vẫn giữ cái này để vẽ biểu đồ tĩnh (Top Doanh thu hiện tại)
    # Logic: Giả sử tháng này bán được 5% tổng số
    est_monthly_revenue = int(price * (sold_count * 0.05))
    product['est_monthly_revenue'] = est_monthly_revenue

    return product