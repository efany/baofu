import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from fund_eastmoney_crawler import FundEastmoneyCrawler

def main():
    # 创建爬虫实例
    crawler = FundEastmoneyCrawler(
        fund_code='519078',
        start_date='',  # 开始日期
        end_date='',    # 结束日期
        per_page=40,
        cache_dir='cache/eastmoney'  # 指定缓存目录
    )
    
    # # 获取数据
    crawler.fetch_fund_data()
    
    # 导出到Excel
    crawler.export_to_excel(output_dir="output/eastmoney")

if __name__ == "__main__":
    main() 