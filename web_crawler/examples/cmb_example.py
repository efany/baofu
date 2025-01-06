import sys
import os
import time

from loguru import logger

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from cmb_finance_crawler import CMBFinanceCrawler

def main():
    crawler = CMBFinanceCrawler(
        delay=1.0,
        cache_dir="cache/cmb"
    )
    # crawler.crawl_product_nav()         # 爬取所有页面
    # data = crawler.get_finance_data()  # 获取爬取的数据
    # crawler.export_to_excel()         # 导出到Excel

    # all_products = crawler.search_products_sell()
    # logger.info(f"search products success, found {len(all_products)} products")
    
    # # 将产品列表导出到Excel
    # crawler.export_product_list(all_products, "output/cmb", "search")

    series = crawler.get_product_series()
    logger.info(f"Found {len(series)} series")
    time.sleep(1.0)
    all_products = []
    for series in series:
        products = crawler.get_serie_products(series['series_code'])
        logger.info(f"Found {len(products)} products in series {series['series_code']}")
        all_products.extend(products)
        time.sleep(1.0)
    crawler.export_product_list(all_products, "output/cmb", f"all")

    # products = crawler.get_serie_products("010018")
    # logger.info(f"Found {len(products)} products in series 010018")
    # crawler.export_product_list(products, "output/cmb", f"series_010018")

if __name__ == "__main__":
    main() 