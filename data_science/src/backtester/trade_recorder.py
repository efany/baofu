import backtrader as bt
from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt
from matplotlib import font_manager
from .base_analyzer import BaseTimeFrameAnalyzer

class TradeRecorder(BaseTimeFrameAnalyzer):
    """记录买卖操作和持仓信息"""
    
    def __init__(self):
        super(TradeRecorder, self).__init__()
        self.orders = []  # 存储交易记录
        self.prices = []  # 存储价格数据
        self.dates = []   # 存储日期数据
        
        # 设置中文字体
        self._setup_chinese_font()
        
    def _setup_chinese_font(self):
        """设置中文字体"""
        try:
            # 尝试设置微软雅黑
            plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
            plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
        except:
            print("警告: 未能正确设置中文字体，图表中的中文可能无法正确显示")
            
    def next(self):
        """每个交易日调用一次，用于记录价格"""
        current_date = self.strategy.datas[0].datetime.date(0)
        
        # 只记录指定时间区间内的数据
        if self.is_in_timeframe(current_date):
            self.dates.append(current_date)
            self.prices.append(self.strategy.datas[0].close[0])
            
    def notify_order(self, order):
        """当订单状态改变时记录信息"""
        if order.status in [order.Completed]:  # 只记录已完成的订单
            # 获取交易日期
            try:
                trade_date = self.strategy.data.datetime.datetime(0)
            except:
                trade_date = self.strategy.data.datetime.date(0)
            
            # 只记录指定时间区间内的交易
            if not self.is_in_timeframe(trade_date):
                return
                
            # 获取当前持仓信息
            position = self.strategy.position
            portfolio_value = self.strategy.broker.getvalue()
            
            # 记录交易信息
            self.orders.append({
                'date': trade_date,  # 交易日期
                'type': '买入' if order.isbuy() else '卖出',  # 操作类型
                'size': order.executed.size,  # 交易数量
                'price': order.executed.price,  # 成交价格
                'value': order.executed.value,  # 交易金额
                'commission': order.executed.comm,  # 手续费
                'position_size': position.size,  # 当前持仓数量
                'position_price': position.price,  # 持仓均价
                'position_value': position.size * order.executed.price,  # 持仓市值
                'cash': self.strategy.broker.getcash(),  # 剩余现金
                'portfolio_value': portfolio_value,  # 总资产
            })
            
    def get_analysis(self):
        """返回分析结果"""
        return {
            'orders': self.orders
        }
        
    def plot_trades(self, output_dir: str = "output"):
        """
        绘制价格走势图和交易标记
        
        Args:
            output_dir: 输出目录
        """
        if not self.prices or not self.dates:
            print("没有足够的数据来绘制图表")
            return
            
        # 创建图表
        plt.figure(figsize=(15, 8))
        
        # 绘制价格走势
        plt.plot(self.dates, self.prices, label='累计净值', color='blue', alpha=0.7)
        
        # 标记买卖点
        buy_dates = []
        buy_prices = []
        sell_dates = []
        sell_prices = []
        
        for order in self.orders:
            if order['type'] == '买入':
                buy_dates.append(order['date'])
                buy_prices.append(order['price'])
            else:
                sell_dates.append(order['date'])
                sell_prices.append(order['price'])
        
        # 绘制买入点
        if buy_dates:
            plt.scatter(buy_dates, buy_prices, color='red', marker='^', 
                       s=100, label='买入', zorder=5)
            
        # 绘制卖出点
        if sell_dates:
            plt.scatter(sell_dates, sell_prices, color='green', marker='v',
                       s=100, label='卖出', zorder=5)
        
        # 设置图表格式
        plt.title('累计净值走势与交易记录', fontsize=12, pad=20)
        plt.xlabel('日期', fontsize=10, labelpad=10)
        plt.ylabel('累计净值', fontsize=10, labelpad=10)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend(loc='best', fontsize=10)
        
        # 调整x轴日期显示
        plt.gcf().autofmt_xdate()
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图表
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plot_file = os.path.join(output_dir, f'trades_plot_{timestamp}.png')
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"\n交易图表已保存到: {plot_file}")
        
    def save_to_excel(self, output_dir: str, recent_analyzer=None, periodical_analyzer=None):
        """
        将交易记录和分析结果保存到Excel
        
        Args:
            output_dir: 输出目录
            recent_analyzer: RecentReturns分析器实例
            periodical_analyzer: PeriodicalReturns分析器实例
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 创建Excel写入器
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = os.path.join(output_dir, f'backtest_results_{timestamp}.xlsx')
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        
        # 保存交易记录
        if self.orders:
            # 创建交易记录DataFrame
            orders_df = pd.DataFrame(self.orders)
            
            # 确保日期格式正确
            if 'date' in orders_df.columns:
                try:
                    orders_df['date'] = pd.to_datetime(orders_df['date'])
                except:
                    pass
                    
            # 格式化数值列
            numeric_columns = [
                'size', 'price', 'value', 'commission', 
                'position_size', 'position_price', 'position_value',
                'cash', 'portfolio_value'
            ]
            for col in numeric_columns:
                if col in orders_df.columns:
                    orders_df[col] = orders_df[col].round(4)
                    
            # 持仓比例列
            orders_df['position_ratio'] = (
                orders_df['position_value'] / orders_df['portfolio_value'] * 100
            ).round(2)
            
            # 保存到Excel
            orders_df.to_excel(writer, sheet_name='交易记录', index=False)
            
        # 保存近期收益分析
        if recent_analyzer is not None:
            recent_analysis = recent_analyzer.get_analysis()
            if recent_analysis:
                # 转换近期收益数据为DataFrame
                periods = [
                    ('7d', '近7天'),
                    ('1m', '近1个月'),
                    ('3m', '近1季度'),
                    ('1y', '近1年'),
                    ('3y', '近3年'),
                    ('5y', '近5年'),
                    ('10y', '近10年'),
                    ('inception', '成立以来')
                ]
                
                recent_data = []
                for key, label in periods:
                    value = recent_analysis.get(key)
                    if value is not None:
                        recent_data.append({
                            '时间段': label,
                            '收益率(%)': value,
                            '年化收益率(%)': (
                                value if key == '1y'
                                else value / 3 if key == '3y'
                                else value / 5 if key == '5y'
                                else value / 10 if key == '10y'
                                else value * 365.0 / recent_analysis.get('days_since_inception', 0) 
                                    if key == 'inception' and recent_analysis.get('days_since_inception', 0) > 0
                                else None
                            )
                        })
                
                recent_df = pd.DataFrame(recent_data)
                recent_df.to_excel(writer, sheet_name='近期收益分析', index=False)
                
        # 保存周期性收益分析
        if periodical_analyzer is not None:
            periodical_analysis = periodical_analyzer.get_analysis()
            if periodical_analysis:
                # 月度收益
                monthly_df = pd.DataFrame([
                    {'月份': month, '收益率(%)': ret}
                    for month, ret in periodical_analysis['monthly'].items()
                ])
                monthly_df.to_excel(writer, sheet_name='月度收益', index=False)
                
                # 季度收益
                quarterly_df = pd.DataFrame([
                    {'季度': quarter, '收益率(%)': ret}
                    for quarter, ret in periodical_analysis['quarterly'].items()
                ])
                quarterly_df.to_excel(writer, sheet_name='季度收益', index=False)
                
                # 年度收益
                yearly_df = pd.DataFrame([
                    {'年份': year, '收益率(%)': ret}
                    for year, ret in periodical_analysis['yearly'].items()
                ])
                yearly_df.to_excel(writer, sheet_name='年度收益', index=False)
        
        # 保存并关闭Excel文件
        writer.close()
        print(f"\n分析结果已保存到: {excel_file}") 
        
        # 在保存Excel后绘制交易图表
        self.plot_trades(output_dir)