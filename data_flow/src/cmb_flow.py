import sys
import os
import time

from loguru import logger

# Add src directories to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'web_crawler', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data_process', 'src'))

from cmb_finance_crawler import CMBFinanceCrawler

def __init_logger(log_dir):
    logger.add(os.path.join(log_dir, "app.log"), rotation="100 KB")
    logger.info(f"日志目录设置为: {log_dir}")

def main():
    output_dir = ""  # 默认输出目录
    log_dir = ""
    if len(sys.argv) > 1:
        try:
            dir_index = sys.argv.index("--output_dir")
            output_dir = sys.argv[dir_index + 1]

            log_dir_index = sys.argv.index("--log_dir")
            log_dir = sys.argv[log_dir_index + 1]

            __init_logger(log_dir)

            # 更新cache_dir为用户指定的目录
            global cache_dir
            cache_dir = output_dir
            logger.info(f"输出目录设置为: {output_dir}, 日志目录设置为: {log_dir}")
        except (ValueError, IndexError):
            pass
    crawler = CMBFinanceCrawler(
        delay=1.0,
        cache_dir="cache/cmb"
    )

    series = crawler.get_product_series()
    logger.info(f"Found {len(series)} series")
    time.sleep(1.0)
    all_products = []
    for series in series:
        products = crawler.get_serie_products(series['series_code'])
        logger.info(f"Found {len(products)} products in series {series['series_code']}")
        all_products.extend(products)
        time.sleep(1.0)
    crawler.export_product_list(all_products, output_dir, f"all")

    # products = crawler.get_serie_products("010018")
    # logger.info(f"Found {len(products)} products in series 010018")
    # crawler.export_product_list(products, "output/cmb", f"series_010018")

if __name__ == "__main__":
    main() 