import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from morningstar_fund_crawler import MorningstarFundCrawler

def main():
    # 创建爬虫实例
    crawler = MorningstarFundCrawler(
        'config/morningstar_config.json',
        cache_dir='cache/morningstar',
        use_cache=True  # 允许使用缓存
    )
    
    # 获取数据
    morningstar_fund_datas = crawler.fetch_fund_data()
    
    # 检查数据是否为空
    if morningstar_fund_datas is None or len(morningstar_fund_datas) == 0:
        print("未获取到基金数据")
        return
    
    print(f"\n获取到 {len(morningstar_fund_datas)} 条基金数据")
    print("\n基金代码列表:")
    for fund in morningstar_fund_datas:
        fund_code = str(fund['fund_code'])
        print(fund_code)
    
    # 导出到Excel
    crawler.export_to_excel('output/morningstar')

if __name__ == "__main__":
    main() 