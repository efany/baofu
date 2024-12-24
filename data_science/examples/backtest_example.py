import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from strategy.buy_and_hold_strategy import BuyAndHoldStrategy
from backtester.backtester import Backtester

def main():
    # 创建回测器实例
    backtester = Backtester(
        strategy_class=BuyAndHoldStrategy,
        data_path='C://Users/yanyifan/code/baofu/output/fund_003376_20241211_005007.xlsx',
        cash=1000000.0,
        commission=0.0001
    )
    
    # 设置策略参数
    strategy_params = {
        'printlog': True,
        'min_volume': 1000,      # 最小交易金额
        'size_pct': 0.98,        # 使用98%的资金
        'start_date': '2022-01-01',  # 回测开始日期
        'end_date': '2023-12-31'     # 回测结束日期
    }
     
    # 运行回测
    results = backtester.run(strategy_params)
    
    # 打印回测结果
    print("\n=== 回测结果 ===")
    print(f"初始资金: {results['initial']:.2f}")
    print(f"最终资金: {results['final']:.2f}")
    print(f"总收益率: {results['returns']*100:.2f}%")
    if results['sharpe'] != 0.0:
        print(f"夏普比率: {results['sharpe']:.2f}")
    else:
        print("夏普比率: N/A")
    print(f"最大回撤: {results['drawdown']:.2f}%")
    
    # 绘制图表
    backtester.plot()

if __name__ == "__main__":
    main() 