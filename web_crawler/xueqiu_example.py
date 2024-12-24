import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from xueqiu_stock_crawler import XueqiuStockCrawler

def main():
    # 使用自动获取的cookies
    crawler = XueqiuStockCrawler(auto_cookies=True)
    
    # 或者使用手动提供的cookies
    # cookie_str = "your_cookie_string_here"
    # crawler = XueqiuStockCrawler(cookies=cookie_str)
    
    # 或者使用默认cookies
    # crawler = XueqiuStockCrawler()
    
    try:
        # 设置股票代码和时间范围
        stock_code = "SH512550"  # 富时A50ETF
        start_date = "20010101"  # 开始日期：2022-01-01
        end_date = "20231231"    # 结束日期：2023-12-31
        
        print(f"\n开始获取 {stock_code} 的数据...")
        print(f"时间范围: {start_date} 到 {end_date}")
        
        # 获取数据（使用前复权数据）
        df = crawler.fetch_stock_data(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            adjust='qfq'  # 使用前复权数据
        )
        
        # 检查数据并打印基本信息
        if df is not None and not df.empty:
            print("\n数据概览:")
            print(f"总记录数: {len(df)}")
            print(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
            print("\n前5条记录:")
            print(df.head())
            
            # 导出到Excel
            filepath = crawler.export_to_excel()
            if filepath:
                print(f"\n数据已成功导出到: {filepath}")
        else:
            print("\n未能获取数据，请检查参数设置和网络连接")
            
    except Exception as e:
        print(f"\n发生错误: {str(e)}")
        
if __name__ == "__main__":
    main() 