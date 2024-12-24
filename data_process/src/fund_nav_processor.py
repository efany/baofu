from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

class FundNavProcessor:
    """基金净值数据处理类"""
    
    def __init__(self, nav_data: List[Dict[str, str]], fee_data: Dict[str, str] = None):
        """
        初始化处理器
        
        Args:
            nav_data: 净值数据列表
            fee_data: 费率数据字典，包含申购费率等信息
        """
        self.nav_data = nav_data
        self.fee_data = fee_data or {}
        self.df = None
        
    def process_nav_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        处理净值数据
        
        Returns:
            Dict: 处理后的数据，包含年度收益率和分红记录
        """
        try:
            # 转换为DataFrame并过滤掉非法值
            self.df = pd.DataFrame(self.nav_data)
            # 过滤掉nav和acc_nav为空或非数字的记录
            print(len(self.df))
            self.df = self.df[
                self.df['nav'].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna() &
                self.df['acc_nav'].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna() &
                (self.df['nav'] != '---') &
                (self.df['acc_nav'] != '---') &
                (self.df['nav'] != 'None') &
                (self.df['acc_nav'] != 'None')
            ]
            print(len(self.df))
            
            # 转换日期列
            self.df['date'] = pd.to_datetime(self.df['date'])
            
            # 转换净值列为float
            self.df['nav'] = pd.to_numeric(self.df['nav'])
            self.df['acc_nav'] = pd.to_numeric(self.df['acc_nav'])
            
            # 处理分红数据
            self.df['dividend'] = self.df['dividend'].apply(self._parse_dividend)

            # 计算回撤标记
            # 计算每个时点之后的最小累计净值
            self.df['future_min_acc_nav'] = float('inf')
            self.df['future_min_acc_nav_date'] = self.df['date']

            # 最后一个时点的future_min_nav就是当天的nav
            
            min_acc_nav = self.df['acc_nav'].iloc[0]
            min_acc_nav_date = self.df['date'].iloc[0]
            for i in range(len(self.df)):
                if i == 0:
                    self.df.iloc[0, self.df.columns.get_loc('future_min_acc_nav')] = self.df['acc_nav'].iloc[i]
                    self.df.iloc[i, self.df.columns.get_loc('future_min_acc_nav_date')] = self.df['date'].iloc[i]
                else:
                    if self.df['acc_nav'].iloc[i] < min_acc_nav:
                        min_acc_nav = self.df['acc_nav'].iloc[i]
                        min_acc_nav_date = self.df['date'].iloc[i]
                    self.df.iloc[i, self.df.columns.get_loc('future_min_acc_nav')] = min_acc_nav
                    self.df.iloc[i, self.df.columns.get_loc('future_min_acc_nav_date')] = min_acc_nav_date
            
            # 计算每个时点的回撤幅度 (降低的百分比)
            self.df['drawdown'] = (self.df['future_min_acc_nav'] - self.df['acc_nav']) / self.df['acc_nav'] * 100

            # 计算年度收益率
            yearly_returns = self._calculate_yearly_returns()
            self._print_yearly_returns(yearly_returns)
            
            # 计算季度收益率
            quarterly_returns = self._calculate_quarterly_returns()
            self._print_quarterly_returns(quarterly_returns)
            
            # 准备分红记录表
            dividend_records = []
            for year_data in yearly_returns.values():
                for record in year_data.get('dividend_records', []):
                    dividend_records.append({
                        'date': record['date'],
                        'dividend': record['dividend'],
                        'nav': record['nav']
                    })
            
            # 准备年度收益率表
            yearly_data = []
            for year, data in sorted(yearly_returns.items()):
                yearly_data.append({
                    'year': year,
                    'start_date': data['start_date'],
                    'end_date': data['end_date'],
                    'start_nav': data['start_nav'],
                    'end_nav': data['end_nav'],
                    'total_dividend': data['total_dividend'],
                    'pure_nav_return': data['pure_nav_return'],
                    'nav_return': data['nav_return'],
                    'reinvest_return': data['reinvest_return'],
                    'max_nav': data['max_nav'],
                    'max_nav_date': data['max_nav_date'],
                    'min_nav': data['min_nav'],
                    'min_nav_date': data['min_nav_date'],
                    'records': data['records'],
                    'volatility': data['volatility'],
                    'sharpe_ratio': data['sharpe_ratio'],
                    'annualized_volatility': data['annualized_volatility'],
                    'max_drawdown': data['max_drawdown'],
                    'max_drawdown_peak_date': data['max_drawdown_peak_date'],
                    'max_drawdown_valley_date': data['max_drawdown_valley_date'],
                    'max_drawdown_duration': data['max_drawdown_duration'],
                    'daily_returns_mean': data['daily_returns_mean'],
                    'daily_returns_std': data['daily_returns_std'],
                    'skewness': data['skewness'],
                    'kurtosis': data['kurtosis'],
                    'positive_days': data['positive_days'],
                    'negative_days': data['negative_days'],
                    'win_rate': data['win_rate']
                })
            
            # 准备季度收益率表
            quarterly_data = []
            for period, data in sorted(quarterly_returns.items()):
                quarterly_data.append({
                    'year': data['year'],
                    'quarter': data['quarter'],
                    'start_date': data['start_date'],
                    'end_date': data['end_date'],
                    'start_nav': data['start_nav'],
                    'end_nav': data['end_nav'],
                    'total_dividend': data['total_dividend'],
                    'pure_nav_return': data['pure_nav_return'],
                    'nav_return': data['nav_return'],
                    'reinvest_return': data['reinvest_return'],
                    'max_nav': data['max_nav'],
                    'max_nav_date': data['max_nav_date'],
                    'min_nav': data['min_nav'],
                    'min_nav_date': data['min_nav_date'],
                    'records': data['records']
                })
            
            # 计算区间收益率
            period_returns = self._calculate_period_returns()
            self._print_period_returns(period_returns)
            
            return {
                'yearly_returns': yearly_data,  # 年度收益率表
                'quarterly_returns': quarterly_data,  # 季度收益率表
                'dividend_records': dividend_records,  # 分红记录表
                'period_returns': period_returns  # 区间收益率
            }
            
        except Exception as e:
            print(f"处理净值数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {
                'yearly_returns': [],
                'quarterly_returns': [],
                'dividend_records': [],
                'period_returns': {}
            }
        
    def _parse_dividend(self, dividend_str: str) -> float:
        """解析分红数据"""
        try:
            if not dividend_str or dividend_str == '---':
                return 0.0
            
            # 提取分红金额
            if '每份派现金' in dividend_str:
                amount = float(dividend_str.split('每份派现金')[1].split('元')[0])
                return amount
            
            return 0.0
        except:
            return 0.0
        
    def _calculate_yearly_returns(self) -> Dict[int, Dict[str, Any]]:
        """计算年度收益率"""
        yearly_returns = {}
        purchase_rate = float(self.fee_data.get('actual_rate', 0)) / 100  # 转换为小数
        
        # 按年份分组
        for year, group in self.df.groupby(self.df['date'].dt.year):
            # 确保数据按日期排序
            group = group.sort_values('date')
            
            # 获取有效的净值数据（去除空值和异常值）
            valid_data = group[
                (group['nav'].notna()) & 
                (group['nav'] > 0)
            ].sort_values('date')
            
            if len(valid_data) < 2:  # 至少需要两个数据点
                continue
            
            # 计算基本数据
            start_nav = valid_data['nav'].iloc[0]
            end_nav = valid_data['nav'].iloc[-1]
            total_dividend = valid_data['dividend'].sum()
            start_date = valid_data['date'].iloc[0]
            end_date = valid_data['date'].iloc[-1]
            
            # 计算分红再投资收益
            reinvest_value = 0
            for _, row in valid_data[valid_data['dividend'] > 0].iterrows():
                dividend = row['dividend']
                nav_at_dividend = row['nav']
                
                # 计算分红可以买到的份额（考虑申购费率）
                if purchase_rate > 0:
                    purchase_fee = (dividend / (1 + purchase_rate)) * purchase_rate
                    actual_purchase = dividend - purchase_fee
                    additional_shares = actual_purchase / nav_at_dividend
                else:
                    additional_shares = dividend / nav_at_dividend
                    
                # 计算这些份额在期末的价值
                reinvest_value += additional_shares * end_nav
            
            # 计算不同口径的收益率
            pure_nav_return = (end_nav / start_nav - 1) * 100  # 净值涨幅
            nav_return = ((end_nav + total_dividend) / start_nav - 1) * 100  # 简单分红收益率
            reinvest_return = ((end_nav + reinvest_value) / start_nav - 1) * 100  # 分红再投资收益率
            
            # 计算日收益率序列
            daily_returns = valid_data['nav'].pct_change().dropna()
            
            # 计算波动率和夏普比率
            volatility, sharpe_ratio = self._calculate_volatility_and_sharpe(
                daily_returns,
                risk_free_rate=0.02,
                periods_per_year=250
            )
            
            # 计算最大回撤及其区间
            max_drawdown, peak_date, valley_date = self._calculate_max_drawdown(valid_data)
            
            # 找出最高和最低净值的位置
            nav_max_idx = valid_data['nav'].idxmax()
            nav_min_idx = valid_data['nav'].idxmin()
            
            # 准备分红记录
            dividend_records = []
            for _, row in valid_data[valid_data['dividend'] > 0].iterrows():
                dividend_records.append({
                    'date': row['date'],
                    'dividend': row['dividend'],
                    'nav': row['nav']
                })
            
            yearly_returns[year] = {
                'start_date': start_date,
                'end_date': end_date,
                'start_nav': start_nav,
                'end_nav': end_nav,
                'total_dividend': total_dividend,
                'pure_nav_return': pure_nav_return,
                'nav_return': nav_return,
                'reinvest_return': reinvest_return,
                'max_nav': valid_data.loc[nav_max_idx, 'nav'],
                'max_nav_date': valid_data.loc[nav_max_idx, 'date'],
                'min_nav': valid_data.loc[nav_min_idx, 'nav'],
                'min_nav_date': valid_data.loc[nav_min_idx, 'date'],
                'records': len(valid_data),
                'dividend_records': dividend_records,
                'volatility': volatility * 100,
                'sharpe_ratio': sharpe_ratio,
                'annualized_volatility': volatility * 100,
                'max_drawdown': max_drawdown * 100,
                'max_drawdown_peak_date': peak_date,
                'max_drawdown_valley_date': valley_date,
                'max_drawdown_duration': (valley_date - peak_date).days if peak_date and valley_date else 0,
                'daily_returns_mean': daily_returns.mean() * 100,
                'daily_returns_std': daily_returns.std() * 100,
                'skewness': daily_returns.skew(),
                'kurtosis': daily_returns.kurtosis(),
                'positive_days': (daily_returns > 0).sum(),
                'negative_days': (daily_returns < 0).sum(),
                'win_rate': (daily_returns > 0).mean() * 100
            }
        
        return yearly_returns
        
    def _print_yearly_returns(self, returns: Dict[int, Dict[str, float]]) -> None:
        """打印年度收益率"""
        # 获取费率信息用于显示
        purchase_rate = self.fee_data.get('actual_rate', '--')
        
        print("\n=== 年度收益率统计 ===")
        if purchase_rate != '--':
            print(f"实际申购费率: {purchase_rate}%")
        
        for year, data in sorted(returns.items()):
            print(f"\n{year}年:")
            print(f"期间: {data['start_date']:%Y-%m-%d} 至 {data['end_date']:%Y-%m-%d}")
            print(f"净值变化: {data['start_nav']:.4f} -> {data['end_nav']:.4f}")
            print(f"年度分红: {data['total_dividend']:.4f}")
            print(f"净值涨幅: {data['pure_nav_return']:.2f}%")
            print(f"分红收益率: {data['nav_return']:.2f}%")
            print(f"再投资收益率: {data['reinvest_return']:.2f}% (考虑申购费率)")
            print(f"最高净值: {data['max_nav']:.4f} ({data['max_nav_date']:%Y-%m-%d})")
            print(f"最低净值: {data['min_nav']:.4f} ({data['min_nav_date']:%Y-%m-%d})")
            
            # 打印分红记录
            if data['dividend_records']:
                print("\n分红记录:")
                for record in data['dividend_records']:
                    print(f"{record['date']:%Y-%m-%d}: {record['dividend']:.4f} (净值: {record['nav']:.4f})")
            
            print(f"\n记录数: {data['records']}") 
        
    def _calculate_quarterly_returns(self) -> Dict[str, Dict[str, Any]]:
        """计算季度收益率"""
        quarterly_returns = {}
        
        # 获取实际申购费率
        purchase_rate = float(self.fee_data.get('actual_rate', 0)) / 100  # 转换为小数
        
        # 按年份和季度分组
        self.df['quarter'] = self.df['date'].dt.quarter
        self.df['year'] = self.df['date'].dt.year
        
        for (year, quarter), group in self.df.groupby(['year', 'quarter']):
            # 确保数据按日期排序
            group = group.sort_values('date')
            
            # 计算单位净值收益率（考虑分红）
            start_nav = group['nav'].iloc[0]
            end_nav = group['nav'].iloc[-1]
            total_dividend = group['dividend'].sum()  # 季度分红总额
            
            # 计算分红再投资收益
            reinvest_value = 0
            for _, row in group[group['dividend'] > 0].iterrows():
                dividend = row['dividend']
                nav_at_dividend = row['nav']
                
                # 计算分红可以买到的份额（考虑申购费率）
                if purchase_rate > 0:
                    purchase_fee = (dividend / (1 + purchase_rate)) * purchase_rate
                    actual_purchase = dividend - purchase_fee
                    additional_shares = actual_purchase / nav_at_dividend
                else:
                    additional_shares = dividend / nav_at_dividend
                    
                # 计算这些份额在期末的价值
                reinvest_value += additional_shares * end_nav
            
            # 计算不同口径的收益率
            nav_return = ((end_nav + total_dividend) / start_nav - 1) * 100  # 简单分红收益率
            reinvest_return = ((end_nav + reinvest_value) / start_nav - 1) * 100  # 分红再投资收益率
            pure_nav_return = (end_nav / start_nav - 1) * 100  # 净值涨幅
            
            # 找出最高和最低净值
            nav_max_idx = group['nav'].idxmax()
            nav_min_idx = group['nav'].idxmin()
            
            period_key = f"{year}Q{quarter}"
            quarterly_returns[period_key] = {
                'year': year,
                'quarter': quarter,
                'start_date': group['date'].iloc[0],
                'end_date': group['date'].iloc[-1],
                'start_nav': start_nav,
                'end_nav': end_nav,
                'total_dividend': total_dividend,
                'nav_return': nav_return,
                'reinvest_return': reinvest_return,
                'pure_nav_return': pure_nav_return,
                'max_nav': group.loc[nav_max_idx, 'nav'],
                'max_nav_date': group.loc[nav_max_idx, 'date'],
                'min_nav': group.loc[nav_min_idx, 'nav'],
                'min_nav_date': group.loc[nav_min_idx, 'date'],
                'records': len(group)
            }
        
        return quarterly_returns
        
    def _print_quarterly_returns(self, returns: Dict[str, Dict[str, float]]) -> None:
        """打印季度收益率"""
        print("\n=== 季度收益率统计 ===")
        
        for period, data in sorted(returns.items()):
            print(f"\n{period}:")
            print(f"期间: {data['start_date']:%Y-%m-%d} 至 {data['end_date']:%Y-%m-%d}")
            print(f"净值变化: {data['start_nav']:.4f} -> {data['end_nav']:.4f}")
            print(f"季度分红: {data['total_dividend']:.4f}")
            print(f"净值涨幅: {data['pure_nav_return']:.2f}%")
            print(f"分红收益率: {data['nav_return']:.2f}%")
            print(f"再投资收益率: {data['reinvest_return']:.2f}% (考虑申购费率)")
            print(f"最高净值: {data['max_nav']:.4f} ({data['max_nav_date']:%Y-%m-%d})")
            print(f"最低净值: {data['min_nav']:.4f} ({data['min_nav_date']:%Y-%m-%d})")
            print(f"记录数: {data['records']}")

    def _calculate_annualized_return(self, 
                               start_date: pd.Timestamp,
                               end_date: pd.Timestamp,
                               total_return: float) -> float:
        """
        计算年化收益率
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            total_return: 总收益率(%)
            
        Returns:
            float: 年化收益率(%)
        """
        # 计算实际天数
        days = (end_date - start_date).days
        if days <= 0:
            return 0.0
        
        # 转换为年数
        years = days / 365.0
        
        # 计算年化收益率
        if years >= 1:
            # 使用复利公式: (1 + r)^n = (1 + R)
            # 其中r为年化收益率，n为年数，R为总收益率
            annualized_return = ((1 + total_return/100) ** (1/years) - 1) * 100
        else:
            # 对于不足1年的区间，简单年化处理
            annualized_return = (total_return / days) * 365
        
        return annualized_return

    def _calculate_volatility_and_sharpe(self, 
                                       returns: pd.Series, 
                                       risk_free_rate: float = 0.02,  # 默认无风险利率2%
                                       periods_per_year: int = 250    # 交易日数
                                       ) -> tuple:
        """
        计算波动率和夏普比率
        
        Args:
            returns: 收益率序列
            risk_free_rate: 年化无风险利率
            periods_per_year: 年化周期数
            
        Returns:
            tuple: (波动率, 夏普比率)
        """
        try:
            # 计算波动率（年化）
            volatility = returns.std() * (periods_per_year ** 0.5)
            
            # 计算年化收益率
            annual_return = (1 + returns).prod() ** (periods_per_year/len(returns)) - 1
            
            # 计算夏普比率
            excess_return = annual_return - risk_free_rate
            sharpe_ratio = excess_return / volatility if volatility != 0 else 0
            
            return volatility, sharpe_ratio
            
        except Exception as e:
            print(f"计算波动率和夏普比率失败: {str(e)}")
            return 0.0, 0.0

    def _calculate_period_returns(self) -> List[Dict[str, Any]]:
        """计算各个时间区间的收益率"""
        # 确保数据按日期排序
        sorted_df = self.df.sort_values('date')
        now = pd.Timestamp.now()
        
        # 定义时间区间
        periods = {
            '7d': pd.DateOffset(days=7),
            '1m': pd.DateOffset(months=1),
            '3m': pd.DateOffset(months=3),
            '6m': pd.DateOffset(months=6),
            '1y': pd.DateOffset(years=1),
            '2y': pd.DateOffset(years=2),
            '3y': pd.DateOffset(years=3),
            '5y': pd.DateOffset(years=5),
            '10y': pd.DateOffset(years=10),
            'inception': None  # 成立以来
        }
        
        period_returns = []  # 改为列表存储
        purchase_rate = float(self.fee_data.get('actual_rate', 0)) / 100  # 转换为小数
        
        for period_name, offset in periods.items():
            # 获取区间数据
            if offset:
                start_date = now - offset
                period_data = sorted_df[sorted_df['date'] >= start_date]
            else:
                period_data = sorted_df  # 全部数据
                
            if len(period_data) < 2:  # 至少需要两个数据点
                continue
                
            # 获取有效值数据（去除空值和异常值）
            valid_data = period_data[
                (period_data['nav'].notna()) & 
                (period_data['nav'] > 0)
            ].sort_values('date')
            
            if len(valid_data) < 2:
                continue
            
            try:
                # 计算基本数据
                start_nav = valid_data['nav'].iloc[0]
                end_nav = valid_data['nav'].iloc[-1]
                total_dividend = valid_data['dividend'].sum()
                start_date = valid_data['date'].iloc[0]
                end_date = valid_data['date'].iloc[-1]
                
                # 计算分红再投资收益
                reinvest_value = 0
                for _, row in valid_data[valid_data['dividend'] > 0].iterrows():
                    dividend = row['dividend']
                    nav_at_dividend = row['nav']
                    
                    # 计算分红可以买到的份额（考虑申购费率）
                    if purchase_rate > 0:
                        purchase_fee = (dividend / (1 + purchase_rate)) * purchase_rate
                        actual_purchase = dividend - purchase_fee
                        additional_shares = actual_purchase / nav_at_dividend
                    else:
                        additional_shares = dividend / nav_at_dividend
                        
                    # 计算这些份额在期末的价值
                    reinvest_value += additional_shares * end_nav
                
                # 计算不同口径的收益率
                pure_nav_return = (end_nav / start_nav - 1) * 100  # 净值涨幅
                nav_return = ((end_nav + total_dividend) / start_nav - 1) * 100  # 简单分红收益率
                reinvest_return = ((end_nav + reinvest_value) / start_nav - 1) * 100  # 分红再投资收益率
                
                # 计算年化收益率
                pure_nav_annualized = self._calculate_annualized_return(
                    start_date, end_date, pure_nav_return
                )
                nav_annualized = self._calculate_annualized_return(
                    start_date, end_date, nav_return
                )
                reinvest_annualized = self._calculate_annualized_return(
                    start_date, end_date, reinvest_return
                )
                
                # 计算日收益率序列
                daily_returns = valid_data['nav'].pct_change().dropna()
                
                # 计算波动率和夏普比率
                volatility, sharpe_ratio = self._calculate_volatility_and_sharpe(
                    daily_returns,
                    risk_free_rate=0.02,
                    periods_per_year=250
                )
                
                # 计算最大回撤及其区间
                max_drawdown, peak_date, valley_date = self._calculate_max_drawdown(valid_data)
                
                if max_drawdown > 0:
                    print(f"最大回撤: {max_drawdown * 100:.2f}%")
                    print(f"峰值日期: {peak_date:%Y-%m-%d}")
                    print(f"谷值日期: {valley_date:%Y-%m-%d}")  

                # 计算回撤持续天数
                # drawdown_duration = (valley_date - peak_date).days if peak_date is not None and valley_date is not None else 0
                
                # 找出最高和最低净值的日期和值
                nav_max_idx = valid_data['nav'].idxmax()
                print(f"nav_max_idx: {nav_max_idx}")
                nav_min_idx = valid_data['nav'].idxmin()
                print(f"nav_min_idx: {nav_min_idx}")
                
                # 添加数据到结果列表
                period_returns.append({
                    'period': period_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'start_nav': start_nav,
                    'end_nav': end_nav,
                    'total_dividend': total_dividend,
                    'pure_nav_return': pure_nav_return,
                    'nav_return': nav_return,
                    'reinvest_return': reinvest_return,
                    'pure_nav_annualized': pure_nav_annualized,
                    'nav_annualized': nav_annualized,
                    'reinvest_annualized': reinvest_annualized,                
                    'max_nav': valid_data.loc[nav_max_idx, 'nav'],
                    'max_nav_date': valid_data.loc[nav_max_idx, 'date'],
                    'min_nav': valid_data.loc[nav_min_idx, 'nav'],
                    'min_nav_date': valid_data.loc[nav_min_idx, 'date'],
                    'records': len(valid_data),
                    'valid_days': (end_date - start_date).days,
                    'volatility': volatility * 100,
                    'sharpe_ratio': sharpe_ratio,
                    'annualized_volatility': volatility * 100,
                    'daily_returns_mean': daily_returns.mean() * 100,
                    'daily_returns_std': daily_returns.std() * 100,
                    'skewness': daily_returns.skew(),
                    'kurtosis': daily_returns.kurtosis(),
                    'positive_days': (daily_returns > 0).sum(),
                    'negative_days': (daily_returns < 0).sum(),
                    'win_rate': (daily_returns > 0).mean() * 100,
                    'max_drawdown': max_drawdown * 100,
                    'max_drawdown_peak_date': peak_date,
                    'max_drawdown_valley_date': valley_date,
                    'max_drawdown_duration': (valley_date - peak_date).days
                })
                
            except Exception as e:
                print(f"处理区间 {period_name} 数据时出错: {str(e)}")
                import traceback
                print(traceback.format_exc())
                continue
        
        # 按照时间跨度排序
        period_weights = {
            '7d': 7, '1m': 30, '3m': 90, '6m': 180,
            '1y': 365, '2y': 730, '3y': 1095, '5y': 1825,
            '10y': 3650, 'inception': 9999
        }
        period_returns.sort(key=lambda x: period_weights.get(x['period'], 0))
        
        return period_returns

    def _calculate_max_drawdown(self, valid_data: pd.DataFrame) -> tuple:
        """
        计算最大回撤及其发生区间
        1. 遍历每个时点，查找之后的最小累计净值
        2. 通过累计净值跌幅找到最大回撤区间
        
        Args:
            valid_data: DataFrame，包含 'date' 和 'nav' 列
            
        Returns:
            tuple: (最大回撤, 峰值日期, 谷值日期)
        """
        try:
            # 找到回撤最大的点(drawdown最小的点,因为是负值)
            max_drawdown_idx = valid_data['drawdown'].idxmin()

            if max_drawdown_idx is not None:
                # 获取最大回撤点的数据
                max_drawdown = abs(valid_data.loc[max_drawdown_idx, 'drawdown']) / 100  # 转换为小数
                peak_date = valid_data.loc[max_drawdown_idx, 'date']
                valley_date = valid_data.loc[max_drawdown_idx, 'future_min_acc_nav_date']
                print(f"max_drawdown: {max_drawdown}, peak_date: {peak_date}, valley_date: {valley_date}")
                return max_drawdown, peak_date, valley_date
            return 0.0, None, None
        except Exception as e:
            print(f"计算最大回撤失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return 0.0, None, None

    def _print_period_returns(self, returns: List[Dict[str, Any]]) -> None:
        """打印区间收益率"""
        period_names = {
            '7d': '近7天',
            '1m': '近1个月',
            '3m': '近3个月',
            '6m': '近6个月',
            '1y': '近1年',
            '2y': '近2年',
            '3y': '近3年',
            '5y': '近5年',
            '10y': '近10年',
            'inception': '成立以来'
        }
        
        print("\n=== 区间收益率统计 ===")
        # 列表已经排序，直接遍历
        for data in returns:
            period_name = period_names.get(data['period'], data['period'])
            print(f"\n{period_name}:")
            print(f"期间: {data['start_date']:%Y-%m-%d} 至 {data['end_date']:%Y-%m-%d}")
            print(f"净值变化: {data['start_nav']:.4f} -> {data['end_nav']:.4f}")
            print(f"区间分红: {data['total_dividend']:.4f}")
            print(f"净值涨幅: {data['pure_nav_return']:.2f}%")
            print(f"分红收益率: {data['nav_return']:.2f}%")
            print(f"再投资收益率: {data['reinvest_return']:.2f}% (考虑申购费率)")
            
            # 打印年化收益率和风险指标
            if data['valid_days'] > 30:  # 只超过30天的区间显示
                print(f"年化净涨幅: {data['pure_nav_annualized']:.2f}%")
                print(f"��化分红收益: {data['nav_annualized']:.2f}%")
                print(f"年化再投资收益率: {data['reinvest_annualized']:.2f}%")
                print(f"年化波动率: {data['annualized_volatility']:.2f}%")
                print(f"夏普比率: {data['sharpe_ratio']:.2f}")
                print(f"最大回撤: {data['max_drawdown']:.2f}% "
                      f"({data['max_drawdown_peak_date']:%Y-%m-%d} 至 "
                      f"{data['max_drawdown_valley_date']:%Y-%m-%d}, "
                      f"持续{data['max_drawdown_duration']}天)")
                print(f"日均收益率: {data['daily_returns_mean']:.4f}%")
                print(f"日收益率标准差: {data['daily_returns_std']:.4f}%")
                print(f"偏度: {data['skewness']:.2f}")
                print(f"峰度: {data['kurtosis']:.2f}")
                print(f"胜率: {data['win_rate']:.2f}%")
                print(f"上涨/下跌天数: {data['positive_days']}/{data['negative_days']}")
            
            print(f"最高净值: {data['max_nav']:.4f} ({data['max_nav_date']:%Y-%m-%d})")
            print(f"最低净值: {data['min_nav']:.4f} ({data['min_nav_date']:%Y-%m-%d})")
            print(f"记录数: {data['records']} (有效天数: {data['valid_days']})")