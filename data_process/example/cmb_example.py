import sys
import os
from loguru import logger

# Add src directories to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'web_crawler', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from cmb_finance_crawler import CMBFinanceCrawler
from fund_nav_processor import FundNavProcessor
from process_utils import export_analysis_to_excel

def main():
    crawler = CMBFinanceCrawler(
        product_code="JY020262",      # 产品代码
        start_date="",      # 起始日期
        end_date="",        # 结束日期
        delay=1.0,
        cache_dir="cache/cmb"
    )
    cmd_datas = crawler.crawl_all_pages()         # 爬取所有页面

    logger.info(f"获取到 {len(cmd_datas['nav_data'])} 条净值数据")

    for data in cmd_datas['info_data']:
        logger.info(data)

    processor = FundNavProcessor(cmd_datas['nav_data'])
    nav_analysis = processor.process_nav_data()
    
    # 将净值分析结果添加到fund_data
    cmd_datas['yearly_returns'] = nav_analysis['yearly_returns']
    cmd_datas['quarterly_returns'] = nav_analysis['quarterly_returns']
    cmd_datas['dividend_records'] = nav_analysis['dividend_records']
    cmd_datas['period_returns'] = nav_analysis['period_returns']

    # 导出分析结果
    export_analysis_to_excel('JY020237', cmd_datas, 'output/analysis')

if __name__ == "__main__":
    main() 