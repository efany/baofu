from typing import List, Dict, Optional, Any
import pandas as pd
from datetime import datetime

class FundSharesProcessor:
    """基金数据处理类"""
    
    def __init__(self, shares_data: List[Dict[str, str]]):
        """
        初始化处理器
        
        Args:
            shares_data: 份额数据列表
        """
        self.shares_data = shares_data
        self.df = None
        
    def process_shares_data(self) -> Dict[str, Any]:
        """处理份额数据"""
        try:
            # 转换为DataFrame
            self.df = pd.DataFrame(self.shares_data)
            
            # 转换日期列
            self.df['share_date'] = pd.to_datetime(self.df['share_date'])
            
            # 转换数值列，处理'---'等特殊值
            numeric_columns = ['purchase', 'redeem', 'total_share', 'total_asset', 'change_rate']
            for col in numeric_columns:
                self.df[col] = pd.to_numeric(
                    self.df[col].replace(['---', '%'], ['0', '']), 
                    errors='coerce'
                )
                
            # 计算年度统计
            yearly_stats = self._calculate_yearly_stats()
            self._print_yearly_stats(yearly_stats)
            
            # 计算近三年和近五年统计
            recent_stats = self._calculate_recent_stats()
            self._print_recent_stats(recent_stats)
            
            # 计算总体统计
            total_stats = self._calculate_total_stats()
            self._print_total_stats(total_stats)

            return {
                'yearly_shares_stats': yearly_stats,
                'recent_shares_stats': recent_stats,
                'total_shares_stats': total_stats
            }
            
        except Exception as e:
            print(f"处理份额数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {
                'yearly_shares_stats': [],
                'recent_shares_stats': [],
                'total_shares_stats': {}
            }
        
    def _calculate_yearly_stats(self) -> List[Dict[str, Any]]:
        """计算年度统计数据"""
        yearly_stats = []  # 改为列表存储
        
        # 按年份分组
        for year, group in self.df.groupby(self.df['share_date'].dt.year):
            # 确保数据按日期排序
            group = group.sort_values('share_date')
            # 过滤掉空值数据
            group = group.dropna(subset=['total_share', 'total_asset', 'purchase', 'redeem'])
            
            # 如果过滤后没有数据,跳过该年份
            if len(group) == 0:
                continue
            
            # 找出资产最小值对应的记录
            min_asset_idx = group['total_asset'].idxmin()
            min_asset_record = group.loc[min_asset_idx]
            
            stats = {
                'year': year,  # 添加年份字段
                'avg_share': group['total_share'].mean(),          # 平均份额
                'avg_asset': group['total_asset'].mean(),          # 平均资产
                'total_purchase': group['purchase'].sum(),         # 总申购
                'total_redeem': group['redeem'].sum(),            # 总赎回
                'net_purchase': group['purchase'].sum() - group['redeem'].sum(),  # 净申购
                'share_change': group['total_share'].iloc[-1] - group['total_share'].iloc[0],  # 份额变动
                'asset_change': group['total_asset'].iloc[-1] - group['total_asset'].iloc[0],  # 资产变动
                'min_share': group['total_share'].min(),          # 最小份额
                'max_share': group['total_share'].max(),          # 最大份额
                'min_asset': min_asset_record['total_asset'],     # 最小资产
                'min_asset_date': min_asset_record['share_date'], # 最小资产日期
                'min_asset_share': min_asset_record['total_share'], # 最小资产时的份额
                'max_asset': group['total_asset'].max(),          # 最大资产
                'start_share': group['total_share'].iloc[0],      # 期初份额
                'end_share': group['total_share'].iloc[-1],       # 期末份额
                'start_asset': group['total_asset'].iloc[0],      # 期初资产
                'end_asset': group['total_asset'].iloc[-1],       # 期末资产
                'records': len(group)                             # 记录数
            }
            
            yearly_stats.append(stats)  # 添加到列表
            
        return yearly_stats  # 返回列表而不是字典
        
    def _calculate_total_stats(self) -> Dict[str, float]:
        """计算总体统计数据"""
        # 确保数据按日期排序（升序）
        sorted_df = self.df.sort_values('share_date')
        
        # 找出资产最小值对应的记录
        min_asset_idx = sorted_df['total_asset'].idxmin()
        min_asset_record = sorted_df.loc[min_asset_idx]
        
        return {
            'total_records': len(self.df),
            'date_range': f"{sorted_df['share_date'].min():%Y-%m-%d} 至 {sorted_df['share_date'].max():%Y-%m-%d}",
            'avg_share': sorted_df['total_share'].mean(),
            'avg_asset': sorted_df['total_asset'].mean(),
            'total_purchase': sorted_df['purchase'].sum(),
            'total_redeem': sorted_df['redeem'].sum(),
            'net_purchase': sorted_df['purchase'].sum() - sorted_df['redeem'].sum(),
            'share_change': sorted_df['total_share'].iloc[-1] - sorted_df['total_share'].iloc[0],
            'asset_change': sorted_df['total_asset'].iloc[-1] - sorted_df['total_asset'].iloc[0],
            'min_share': sorted_df['total_share'].min(),
            'max_share': sorted_df['total_share'].max(),
            'min_asset': min_asset_record['total_asset'],
            'min_asset_date': min_asset_record['share_date'],
            'min_asset_share': min_asset_record['total_share'],
            'max_asset': sorted_df['total_asset'].max(),
            'start_share': sorted_df['total_share'].iloc[0],
            'end_share': sorted_df['total_share'].iloc[-1],
            'start_asset': sorted_df['total_asset'].iloc[0],
            'end_asset': sorted_df['total_asset'].iloc[-1],
            'share_volatility': sorted_df['total_share'].std(),
            'asset_volatility': sorted_df['total_asset'].std()
        }
        
    def _calculate_recent_stats(self) -> List[Dict[str, Any]]:
        """计算近三年和近五年统计数据"""
        recent_stats = []  # 改为列表存储
        now = pd.Timestamp.now()
        
        periods = {
            'three_year': (now - pd.DateOffset(years=3), '3y'),
            'five_year': (now - pd.DateOffset(years=5), '5y')
        }
        
        for period_key, (start_date, period_name) in periods.items():
            period_data = self.df[self.df['share_date'] >= start_date].sort_values('share_date')
            if not period_data.empty:
                # 找出资产最小值对应的记录
                min_asset_idx = period_data['total_asset'].idxmin()
                min_asset_record = period_data.loc[min_asset_idx]
                
                stats = {
                    'period': period_name,  # 添加期间名称
                    'avg_share': period_data['total_share'].mean(),
                    'avg_asset': period_data['total_asset'].mean(),
                    'total_purchase': period_data['purchase'].sum(),
                    'total_redeem': period_data['redeem'].sum(),
                    'net_purchase': period_data['purchase'].sum() - period_data['redeem'].sum(),
                    'share_change': period_data['total_share'].iloc[-1] - period_data['total_share'].iloc[0],
                    'asset_change': period_data['total_asset'].iloc[-1] - period_data['total_asset'].iloc[0],
                    'min_share': period_data['total_share'].min(),
                    'max_share': period_data['total_share'].max(),
                    'min_asset': min_asset_record['total_asset'],
                    'min_asset_date': min_asset_record['share_date'],
                    'min_asset_share': min_asset_record['total_share'],
                    'max_asset': period_data['total_asset'].max(),
                    'share_volatility': period_data['total_share'].std(),
                    'asset_volatility': period_data['total_asset'].std(),
                    'records': len(period_data)
                }
                recent_stats.append(stats)
        
        return recent_stats  # 返回列表而不是字典
        
    def _print_yearly_stats(self, stats: List[Dict[str, Any]]) -> None:
        """打印年度统计数据"""
        print("\n=== 年度份额统计 ===")
        for data in sorted(stats, key=lambda x: x['year']):
            print(f"\n{data['year']}年:")
            print(f"平均份额: {data['avg_share']:.2f} 亿份")
            print(f"平均资产: {data['avg_asset']:.2f} 亿元")
            print(f"总申购: {data['total_purchase']:.2f} 亿份")
            print(f"总赎回: {data['total_redeem']:.2f} 亿份")
            print(f"净申购: {data['net_purchase']:.2f} 亿份")
            print(f"份额变动: {data['share_change']:.2f} 亿份")
            print(f"资产变动: {data['asset_change']:.2f} 亿元")
            print(f"份额区间: {data['min_share']:.2f} - {data['max_share']:.2f} 亿份")
            print(f"资产区间: {data['min_asset']:.2f} - {data['max_asset']:.2f} 亿元")
            print(f"最小资产: {data['min_asset']:.2f} 亿元 ({data['min_asset_date']:%Y-%m-%d})")
            print(f"最小资产时份额: {data['min_asset_share']:.2f} 亿份")
            print(f"期初份额: {data['start_share']:.2f} 亿份")
            print(f"期末份额: {data['end_share']:.2f} 亿份")
            print(f"期初资产: {data['start_asset']:.2f} 亿元")
            print(f"期末资产: {data['end_asset']:.2f} 亿元")
            print(f"记录数: {data['records']}")
            
    def _print_total_stats(self, stats: Dict[str, float]) -> None:
        """打印总体统计数据"""
        print("\n=== 总体统计 ===")
        print(f"统计期间: {stats['date_range']}")
        print(f"总记录数: {stats['total_records']}")
        print(f"平均份额: {stats['avg_share']:.2f} 亿份")
        print(f"平均资产: {stats['avg_asset']:.2f} 亿元")
        print(f"总申购: {stats['total_purchase']:.2f} 亿份")
        print(f"总赎回: {stats['total_redeem']:.2f} 亿份")
        print(f"净申购: {stats['net_purchase']:.2f} 亿份")
        print(f"份额变动: {stats['share_change']:.2f} 亿份")
        print(f"资产变动: {stats['asset_change']:.2f} 亿元")
        print(f"份额区间: {stats['min_share']:.2f} - {stats['max_share']:.2f} 亿份")
        print(f"资产区间: {stats['min_asset']:.2f} - {stats['max_asset']:.2f} 亿元")
        print(f"最小资产: {stats['min_asset']:.2f} 亿元 ({stats['min_asset_date']:%Y-%m-%d})")
        print(f"最小资产时份额: {stats['min_asset_share']:.2f} 亿份")
        print(f"期初份额: {stats['start_share']:.2f} 亿份")
        print(f"期末份额: {stats['end_share']:.2f} 亿份")
        print(f"期初资产: {stats['start_asset']:.2f} 亿元")
        print(f"期末资产: {stats['end_asset']:.2f} 亿元")
        print(f"份额波动率: {stats['share_volatility']:.2f}")
        print(f"资产波动率: {stats['asset_volatility']:.2f}")
        
    def _print_recent_stats(self, stats: List[Dict[str, Any]]) -> None:
        """打印近三年和近五年统计数据"""
        for data in stats:
            print(f"\n=== {data['period']}统计 ===")
            print(f"平均份额: {data['avg_share']:.2f} 亿份")
            print(f"平均资产: {data['avg_asset']:.2f} 亿元")
            print(f"总申购: {data['total_purchase']:.2f} 亿份")
            print(f"总赎回: {data['total_redeem']:.2f} 亿份")
            print(f"净申购: {data['net_purchase']:.2f} 亿份")
            print(f"份额变动: {data['share_change']:.2f} 亿份")
            print(f"资产变动: {data['asset_change']:.2f} 亿元")
            print(f"份额区间: {data['min_share']:.2f} - {data['max_share']:.2f} 亿份")
            print(f"资产区间: {data['min_asset']:.2f} - {data['max_asset']:.2f} 亿元")
            print(f"最小资产: {data['min_asset']:.2f} 亿元 ({data['min_asset_date']:%Y-%m-%d})")
            print(f"最小资产时份额: {data['min_asset_share']:.2f} 亿份")
            print(f"份额波动率: {data['share_volatility']:.2f}")
            print(f"资产波动率: {data['asset_volatility']:.2f}")
            print(f"记录数: {data['records']}")