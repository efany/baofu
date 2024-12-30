import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any
from loguru import logger

def export_analysis_to_excel(fund_code: str, fund_data: Dict[str, Any], output_dir: str = "output") -> None:
    """
    导出分析结果到Excel
    
    Args:
        fund_code: 基金代码
        fund_data: 基金数据字典
        output_dir: 输出目录
    """
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"fund_{fund_code}_analysis_{timestamp}.xlsx"
    filepath = os.path.join(output_dir, filename)
    
    # 列名映射
    column_mapping = {
        # 基本信息
        'acc_nav': '累计净值',
        'nav': '单位净值',
        'growth_rate': '日增长率(%)',
        'buy_status': '申购状态',
        'sell_status': '赎回状态',
        'share_date': '份额日期',
        'purchase': '期间申购(亿份)',
        'redeem': '期间赎回(亿份)',
        'total_share': '期末总份额(亿份)',
        'total_asset': '期末净资产(亿元)',
        'change_rate': '净资产变动率(%)',
        'year': '年份',
        'quarter': '季度',
        'period': '区间',
        'start_date': '开始日期',
        'end_date': '结束日期',
        'start_nav': '期初净值',
        'end_nav': '期末净值',
        'total_dividend': '分红总额',
        
        # 收益率指标
        'pure_nav_return': '净值涨幅(%)',
        'nav_return': '分红收益率(%)',
        'reinvest_return': '再投资收益率(%)',
        'pure_nav_annualized': '年化净值涨幅(%)',
        'nav_annualized': '年化分红收益率(%)',
        'reinvest_annualized': '年化再投资收益率(%)',
        
        # 风险指标
        'volatility': '波动率(%)',
        'sharpe_ratio': '夏普比率',
        'annualized_volatility': '年化波动率(%)',
        'max_drawdown': '最大回撤(%)',
        'max_drawdown_peak_date': '回撤峰值日期',
        'max_drawdown_valley_date': '回撤谷值日期',
        'max_drawdown_duration': '回撤持续天数',
        'daily_returns_mean': '日均收益率(%)',
        'daily_returns_std': '日收益率标准差(%)',
        'skewness': '偏度',
        'kurtosis': '峰度',
        'positive_days': '上涨天数',
        'negative_days': '下跌天数',
        'win_rate': '胜率(%)',
        
        # 净值指标
        'max_nav': '最高净值',
        'max_nav_date': '最高净值日期',
        'min_nav': '最低净值',
        'min_nav_date': '最低净值日期',
        'records': '记录数',
        'valid_days': '有效天数',

        # 费率数据
        'purchase_rate': '原始申购费率(%)',
        'actual_rate': '实际申购费率(%)',
        'discount': '费率折扣(折)',
        'manage_rate': '管理费率(%)',
        'custody_rate': '托管费率(%)',
        'sale_rate': '销售服务费率(%)',
        'purchase_status': '申购状态',
        'redeem_status': '赎回状态',
        
        # 分红记录
        'date': '日期',
        'dividend': '分红',
        
        # 份额分析
        'avg_share': '平均份额(亿份)',
        'avg_asset': '平均资产(亿元)',
        'total_purchase': '总申购(亿份)',
        'total_redeem': '总赎回(亿份)',
        'net_purchase': '净申购(亿份)',
        'share_change': '份额变动(亿份)',
        'asset_change': '资产变动(亿元)',
        'min_share': '最小份额(亿份)',
        'max_share': '最大份额(亿份)',
        'min_asset': '最小资产(亿元)',
        'max_asset': '最大资产(亿元)',
        'min_asset_date': '最小资产日期',
        'min_asset_share': '最小资产时份额(亿份)',
        'start_share': '期初份额(亿份)',
        'end_share': '期末份额(亿份)',
        'start_asset': '期初资产(亿元)',
        'end_asset': '期末资产(亿元)',
        'share_volatility': '份额波动率',
        'asset_volatility': '资产波动率',
        'total_records': '记录数',
        'valid_days': '有效天数',
        'date_range': '日期范围',
    }
    
    # 创建Excel写入器
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:

        # 导出净值数据
        if 'nav_data' in fund_data:
            nav_df = pd.DataFrame(fund_data['nav_data'])
            nav_df = nav_df.rename(columns=column_mapping)
            nav_df.to_excel(writer, sheet_name='净值数据', index=False)
        
        # 导出份额数据
        if 'shares_data' in fund_data:
            shares_df = pd.DataFrame(fund_data['shares_data'])
            shares_df = shares_df.rename(columns=column_mapping)
            shares_df.to_excel(writer, sheet_name='份额数据', index=False)

        # 导出费率数据
        if 'fee_data' in fund_data:
            # 将费率字典转换为DataFrame格式
            # 将费率字典转换为列表格式
            fee_list = []
            for key, value in fund_data['fee_data'].items():
                fee_list.append({
                    'name': key,
                    'value': value
                })
            fee_list_df = pd.DataFrame(fee_list)
            fee_list_df = fee_list_df.rename(columns=column_mapping)
            fee_list_df.to_excel(writer, sheet_name='费率信息', index=False)

        # 导出年度收益率
        if 'yearly_returns' in fund_data:
            yearly_df = pd.DataFrame(fund_data['yearly_returns'])
            yearly_df = yearly_df.rename(columns=column_mapping)
            yearly_df.to_excel(writer, sheet_name='年度收益率', index=False)
        
        # 导出季度收益率
        if 'quarterly_returns' in fund_data:
            quarterly_df = pd.DataFrame(fund_data['quarterly_returns'])
            quarterly_df = quarterly_df.rename(columns=column_mapping)
            quarterly_df.to_excel(writer, sheet_name='季度收益率', index=False)
        
        # 导出区间收益率
        if 'period_returns' in fund_data:
            period_df = pd.DataFrame(fund_data['period_returns'])
            period_df = period_df.rename(columns=column_mapping)
            period_df.to_excel(writer, sheet_name='区间收益率', index=False)
        
        # 导出分红记录
        if 'dividend_records' in fund_data:
            dividend_df = pd.DataFrame(fund_data['dividend_records'])
            dividend_df = dividend_df.rename(columns=column_mapping)
            dividend_df.to_excel(writer, sheet_name='分红记录', index=False)

        # 导出份额分析
        if 'yearly_shares_stats' in fund_data:
            yearly_shares_df = pd.DataFrame(fund_data['yearly_shares_stats'])
            yearly_shares_df = yearly_shares_df.rename(columns=column_mapping)
            yearly_shares_df.to_excel(writer, sheet_name='年度份额分析', index=False)

        if 'recent_shares_stats' in fund_data:
            recent_shares_df = pd.DataFrame(fund_data['recent_shares_stats'])
            recent_shares_df = recent_shares_df.rename(columns=column_mapping)
            recent_shares_df.to_excel(writer, sheet_name='区间份额分析', index=False)

        # 导出总份额分析
        if 'total_shares_stats' in fund_data:
            # 将字典转换为DataFrame，添加索引
            total_shares_data = pd.DataFrame([fund_data['total_shares_stats']])
            total_shares_data = total_shares_data.rename(columns=column_mapping)
            total_shares_data.to_excel(writer, sheet_name='总份额分析', index=False)
    
    logger.info(f"\n分析结果已导出到: {filepath}")