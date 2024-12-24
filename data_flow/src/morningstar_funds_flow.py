import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta

# Add src directories to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'web_crawler', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data_process', 'src'))

from fund_eastmoney_crawler import FundEastmoneyCrawler
from fund_shares_processor import FundSharesProcessor
from fund_nav_processor import FundNavProcessor
from process_utils import export_analysis_to_excel
from morningstar_fund_crawler import MorningstarFundCrawler

def export_filtered_results(result_funds: list, output_dirs: str) -> None:
    """
    将筛选后的基金结果导出到Excel文件
    
    Args:
        result_funds: 筛选后的基金列表
        output_dirs: 输出目录路径
    """

    # 重命名列以便于阅读
    column_mapping = {
        'fund_code': '基金代码',
        'fund_name': '基金名称',
        'purchase_status': '申购状态',
        'fund_type': '基金类型',
        'fund_share_type': '份额类型',
        'asset_stock_weight': '股票占比(%)',
        'bond_7_weight': '可转债占比(%)',
        'rating_3year': '三年评级',
        'rating_5year': '五年评级',
        'manager_1_duration_days': '基金经理任期(天)',
        'weighted_predicted_return': '加权预测收益率(%)',
        'weighted_max_drawdown': '加权最大回撤(%)',
        'reinvest_annualized_1y': '一年年化再投资收益率(%)',
        'reinvest_annualized_3y': '三年年化再投资收益率(%)',
        'reinvest_annualized_5y': '五年年化再投资收益率(%)',
        'max_drawdown_1y': '一年最大回撤(%)',
        'max_drawdown_3y': '三年最大回撤(%)',
        'max_drawdown_5y': '五年最大回撤(%)',
        'avg_assets_3y': '三年平均资产规模(亿)',
        'min_assets_3y': '三年最小资产规模(亿)'
    }

    # 对数值型字段进行格式化，保留3位小数
    for fund in result_funds:
        # 股票占比
        if 'asset_stock_weight' in fund and fund['asset_stock_weight'] is not None:
            fund['asset_stock_weight'] = '{:.3f}'.format(float(fund['asset_stock_weight']))
        
        # 可转债占比 
        if 'bond_7_weight' in fund and fund['bond_7_weight'] is not None:
            fund['bond_7_weight'] = '{:.3f}'.format(float(fund['bond_7_weight']))
        
        # 年化再投资收益率
        if 'reinvest_annualized_1y' in fund and fund['reinvest_annualized_1y'] is not None:
            fund['reinvest_annualized_1y'] = '{:.3f}'.format(float(fund['reinvest_annualized_1y']))
        if 'reinvest_annualized_3y' in fund and fund['reinvest_annualized_3y'] is not None:
            fund['reinvest_annualized_3y'] = '{:.3f}'.format(float(fund['reinvest_annualized_3y']))
        if 'reinvest_annualized_5y' in fund and fund['reinvest_annualized_5y'] is not None:
            fund['reinvest_annualized_5y'] = '{:.3f}'.format(float(fund['reinvest_annualized_5y']))
            
        # 最大回撤
        if 'max_drawdown_1y' in fund and fund['max_drawdown_1y'] is not None:
            fund['max_drawdown_1y'] = '{:.3f}'.format(float(fund['max_drawdown_1y']))
        if 'max_drawdown_3y' in fund and fund['max_drawdown_3y'] is not None:
            fund['max_drawdown_3y'] = '{:.3f}'.format(float(fund['max_drawdown_3y']))
        if 'max_drawdown_5y' in fund and fund['max_drawdown_5y'] is not None:
            fund['max_drawdown_5y'] = '{:.3f}'.format(float(fund['max_drawdown_5y']))
    
    # 创建DataFrame
    result_funds_df = pd.DataFrame(result_funds)
    
    # 只选择在DataFrame中实际存在的列
    available_columns = [col for col in column_mapping.keys() if col in result_funds_df.columns]
    export_funds_df = result_funds_df[available_columns]
    
    # 只重命名存在的列
    rename_mapping = {k: column_mapping[k] for k in available_columns}
    export_funds_df = export_funds_df.rename(columns=rename_mapping)
    
    # 导出到Excel
    output_file = os.path.join(output_dirs, 'funds.xlsx')
    export_funds_df.to_excel(output_file, index=False)
    print(f"\n筛选结果已导出到: {output_file}")
    
    # 打印缺失的列（用于调试）
    missing_columns = set(column_mapping.keys()) - set(result_funds_df.columns)
    if missing_columns:
        print("\n警告：以下字段在数据中不存在：")
        for col in missing_columns:
            print(f"- {col}")

def get_period_return(fund_data, period):
    for period in fund_data['period_returns']:
        if period['period'] == period:
            return float(period['reinvest_annualized'])
    print(f"基金 {fund_data['fund_code']} 没有3年再投资收益率")
    return -100.0

def compute_filter_params(morningstar_fund_datas: list, fund_datas: dict) -> list:
    # 遍历每个基金数据，添加年化再投资收益率字段
    for fund in morningstar_fund_datas:
        fund_code = fund['fund_code']
        
        # 如果该基金代码存在对应的详细数据
        if fund_code in fund_datas:
            fund_detail = fund_datas[fund_code]
            
            # 从period_returns中获取1年、3年、5年的年化再投资收益率和最大回撤
            if 'period_returns' in fund_detail:
                # 初始化收益率和最大回撤为None
                fund['reinvest_annualized_1y'] = None 
                fund['reinvest_annualized_3y'] = None
                fund['reinvest_annualized_5y'] = None
                fund['max_drawdown_1y'] = None
                fund['max_drawdown_3y'] = None 
                fund['max_drawdown_5y'] = None
                
                # 遍历period_returns查找对应期间的收益率和最大回撤
                for period_data in fund_detail['period_returns']:
                    period = period_data.get('period')
                    if period == '1y':
                        fund['reinvest_annualized_1y'] = period_data.get('reinvest_annualized')
                        fund['max_drawdown_1y'] = period_data.get('max_drawdown')
                    elif period == '3y':
                        fund['reinvest_annualized_3y'] = period_data.get('reinvest_annualized')
                        fund['max_drawdown_3y'] = period_data.get('max_drawdown')
                    elif period == '5y':
                        fund['reinvest_annualized_5y'] = period_data.get('reinvest_annualized')
                        fund['max_drawdown_5y'] = period_data.get('max_drawdown')

            # 从yearly_returns中计算加权预测收益率
            if 'yearly_returns' in fund_detail:
                yearly_returns = fund_detail['yearly_returns']
                # 将列表按year字段排序
                sorted_years = sorted([item['year'] for item in yearly_returns], reverse=True)
                
                weights = [0.35, 0.25, 0.2, 0.1, 0.1]  # 权重系数
                returns = []
                drawdowns = []  # 新增：存储最大回撤
                total_weight = 0

                is_merge_return = False
                # 如果最近一年的records数量低于60，则和第二年的数据通过records进行加权
                if len(sorted_years) >= 2:
                    current_year = sorted_years[0]
                    last_year = sorted_years[1]
                    current_year_data = next(item for item in yearly_returns if item['year'] == current_year)
                    last_year_data = next(item for item in yearly_returns if item['year'] == last_year)
                    
                    current_records = current_year_data.get('records', 0)
                    if current_records < 60:
                        is_merge_return = True
                        # 获取当年和去年的记录数、收益率和最大回撤
                        current_return = current_year_data.get('reinvest_return')
                        current_drawdown = current_year_data.get('max_drawdown', 0)  # 新增
                        last_return = last_year_data.get('reinvest_return')
                        last_drawdown = last_year_data.get('max_drawdown', 0)  # 新增
                        last_records = last_year_data.get('records', 0)
                    
                        total_records = current_records + last_records
                        weight_current = current_records / total_records
                        weight_last = last_records / total_records
                        
                        weighted_0_returns = current_return * weight_current + last_return * weight_last
                        weighted_0_drawdowns = current_drawdown * weight_current + last_drawdown * weight_last  # 新增
                        
                        returns.append(weighted_0_returns)
                        drawdowns.append(weighted_0_drawdowns)  # 新增
                    else:
                        is_merge_return = False
                        returns.append(current_year_data.get('reinvest_return'))
                        drawdowns.append(current_year_data.get('max_drawdown', 0))  # 新增
                
                start_years = 2 if is_merge_return else 1
                for i, year in enumerate(sorted_years[start_years:5]):
                    year_data = next(item for item in yearly_returns if item['year'] == year)
                    records = year_data.get('records', 0)
                    reinvest_return = year_data.get('reinvest_return')
                    max_drawdown = year_data.get('max_drawdown', 0)  # 新增
                    
                    if reinvest_return is not None:
                        if records < 120:
                            break
                            
                        returns.append(reinvest_return)
                        drawdowns.append(max_drawdown)  # 新增
                
                # 计算加权预测收益率和加权最大回撤
                if len(returns) >= 3:
                    weighted_sum = 0
                    weighted_drawdown_sum = 0  # 新增
                    total_weight = 0
                    
                    for i, (ret, dd) in enumerate(zip(returns, drawdowns)):  # 修改
                        if i < len(weights):
                            weighted_sum += ret * weights[i]
                            weighted_drawdown_sum += dd * weights[i]  # 新增
                            total_weight += weights[i]
                    
                    if total_weight > 0:
                        fund['weighted_predicted_return'] = str(round(weighted_sum / total_weight, 2))
                        fund['weighted_max_drawdown'] = str(round(weighted_drawdown_sum / total_weight, 2))  # 新增
                    else:
                        fund['weighted_predicted_return'] = '0'
                        fund['weighted_max_drawdown'] = '0'  # 新增
                else:
                    fund['weighted_predicted_return'] = '0'
                    fund['weighted_max_drawdown'] = '0'  # 新增
            else:
                fund['weighted_predicted_return'] = '0'
                fund['weighted_max_drawdown'] = '0'  # 新增

            # 从recent_shares_stats中获取3年的资产统计数据
            if 'recent_shares_stats' in fund_detail:
                three_year_stats = None
                for stats in fund_detail['recent_shares_stats']:
                    if stats['period'] == '3y':
                        three_year_stats = stats
                        break
                if three_year_stats:
                    fund['avg_assets_3y'] = three_year_stats.get('avg_asset', '--')
                    fund['min_assets_3y'] = three_year_stats.get('min_asset', '--')
                else:
                    fund['avg_assets_3y'] = '0'
                    fund['min_assets_3y'] = '0'
            else:
                fund['avg_assets_3y'] = '0'
                fund['min_assets_3y'] = '0'
            
            # 将None替换为'--'
            fund['reinvest_annualized_1y'] = fund.get('reinvest_annualized_1y', '--')
            fund['reinvest_annualized_3y'] = fund.get('reinvest_annualized_3y', '--') 
            fund['reinvest_annualized_5y'] = fund.get('reinvest_annualized_5y', '--')
            fund['max_drawdown_1y'] = fund.get('max_drawdown_1y', '--')
            fund['max_drawdown_3y'] = fund.get('max_drawdown_3y', '--')
            fund['max_drawdown_5y'] = fund.get('max_drawdown_5y', '--')

            fund_detail = fund_datas[fund_code]
            if 'fee_data' in fund_detail:
                fund['purchase_status'] = fund_detail['fee_data'].get('purchase_status', '--')
            else:
                fund['purchase_status'] = '--'
    
        # 检查基金经理1任期是否满一年
        manager_1_duration = fund.get('manager_1_duration', '')
        
        # 如果任期为空,设置为0
        if not manager_1_duration:
            fund['manager_1_duration_days'] = 0
            continue
            
        # 解析任期（格式如："3年83天"）
        try:
            years = 0
            days = 0
            if '年' in manager_1_duration:
                years = int(manager_1_duration.split('年')[0])
                if '天' in manager_1_duration:
                    days = int(manager_1_duration.split('年')[1].split('天')[0])
            elif '天' in manager_1_duration:
                days = int(manager_1_duration.split('天')[0])
                
            # 计算总天数
            total_days = years * 365 + days
            fund['manager_1_duration_days'] = total_days
            
        except:
            print(f"基金 {fund.get('fund_code', '--')} 基金经理1任期解析失败")
            fund['manager_1_duration_days'] = 0
        
    return morningstar_fund_datas

def main():
    output_dir = ""  # 默认输出目录
    if len(sys.argv) > 1:
        try:
            dir_index = sys.argv.index("--output_dir") 
            output_dir = sys.argv[dir_index + 1]
            # 更新cache_dir为用户指定的目录
            global cache_dir
            cache_dir = output_dir
            print(f"输出目录设置为: {output_dir}")
        except (ValueError, IndexError):
            pass
    # 创建爬虫实例
    crawler = MorningstarFundCrawler(
        'config/morningstar_config.json',
        cache_dir='cache/morningstar',
        use_cache=True  # 允许使用缓存
    )
    
    # 获取数据
    morningstar_fund_datas = crawler.fetch_fund_data()
    fund_datas = {}

    # 创建带时间戳的输出目录
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_dir = f'output/morningstar_funds_flow_{timestamp}'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建目录: {output_dir}")
    
    # 导出原始数据到Excel
    crawler.export_to_excel(output_dir)

    for fund in morningstar_fund_datas:
        fund_code = fund['fund_code']
        print(f"\n正在从东方财富获取基金 {fund_code} 的数据...")
        
        # 创建东方财富爬虫实例
        eastmoney_crawler = FundEastmoneyCrawler(
            fund_code=fund_code,
            start_date='',  # 开始日期
            end_date='',    # 结束日期
            per_page=40,
            cache_dir='cache/eastmoney'  # 指定缓存目录
        )
        
        # 获取数据
        fund_data = eastmoney_crawler.fetch_fund_data()
        
        # 导出到Excel
        eastmoney_crawler.export_to_excel()

        # 处理净值数据
        fee_data = fund_data['fee_data']
        nav_processor = FundNavProcessor(fund_data['nav_data'], fee_data)
        nav_analysis = nav_processor.process_nav_data()

        # 将净值分析结果添加到fund_data
        fund_data['fund_code'] = fund_code
        fund_data['yearly_returns'] = nav_analysis['yearly_returns']
        fund_data['quarterly_returns'] = nav_analysis['quarterly_returns']
        fund_data['dividend_records'] = nav_analysis['dividend_records']
        fund_data['period_returns'] = nav_analysis['period_returns']
        
        # 处理份额数据
        shares_processor = FundSharesProcessor(fund_data['shares_data'])
        shares_analysis = shares_processor.process_shares_data()

        fund_data['yearly_shares_stats'] = shares_analysis['yearly_shares_stats']
        fund_data['recent_shares_stats'] = shares_analysis['recent_shares_stats']
        fund_data['total_shares_stats'] = shares_analysis['total_shares_stats']

        # 导出分析结果
        export_analysis_to_excel(fund_code, fund_data, output_dir)

        fund_datas[fund_code] = fund_data

        # 添加延迟避免请求过快
        time.sleep(1)
    
    # 筛选和排序基金
    result_funds = compute_filter_params(morningstar_fund_datas,fund_datas)

    # 导出筛选结果
    export_filtered_results(result_funds, output_dir)
    
if __name__ == "__main__":
    main() 