import sys
import os

# Add src directories to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'web_crawler', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from fund_eastmoney_crawler import FundEastmoneyCrawler
from fund_shares_processor import FundSharesProcessor   
from fund_nav_processor import FundNavProcessor
from process_utils import export_analysis_to_excel

def main():
    # 创建爬虫实例
    crawler = FundEastmoneyCrawler(
        fund_code='519078',
        start_date='',  # 开始日期
        end_date='',    # 结束日期
        per_page=40,
        cache_dir='cache/eastmoney'  # 指定缓存目录
    )
    
    # 获取数据
    fund_data = crawler.fetch_fund_data()

    # 导出原始数据到Excel
    crawler.export_to_excel()

    print(f"获取到 {len(fund_data['nav_data'])} 条净值数据")
    print(f"获取到 {len(fund_data['shares_data'])} 条份额数据")

    # 打印费率信息
    print("\n=== 费率信息 ===")
    fee_data = fund_data['fee_data']
    print(f"原始申购费率: {fee_data.get('purchase_rate', '--')}%")
    print(f"实际申购费率: {fee_data.get('actual_rate', '--')}%")
    print(f"费率折扣: {fee_data.get('discount', '--')}折")

    # 处理净值数据
    nav_processor = FundNavProcessor(fund_data['nav_data'], fee_data)
    nav_analysis = nav_processor.process_nav_data()

    # 处理份额数据
    shares_processor = FundSharesProcessor(fund_data['shares_data'])
    shares_analysis = shares_processor.process_shares_data()

    # 将净值分析结果添加到fund_data
    fund_data['yearly_returns'] = nav_analysis['yearly_returns']
    fund_data['quarterly_returns'] = nav_analysis['quarterly_returns']
    fund_data['dividend_records'] = nav_analysis['dividend_records']
    fund_data['period_returns'] = nav_analysis['period_returns']

    fund_data['yearly_shares_stats'] = shares_analysis['yearly_shares_stats']
    fund_data['recent_shares_stats'] = shares_analysis['recent_shares_stats']
    fund_data['total_shares_stats'] = shares_analysis['total_shares_stats'] 

    # 导出分析结果
    export_analysis_to_excel('000032',fund_data, 'output/analysis')

if __name__ == "__main__":
    main() 