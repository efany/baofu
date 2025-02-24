from datetime import datetime
import backtrader as bt
import json
from loguru import logger

class BuyAndHoldStrategy(bt.Strategy):
    """买入持有策略"""

    # params: 策略参数，JSON格式
    # params = {
    #     "open_date": None,
    #     "products": [],
    #     "weights": []
    # }
    def __init__(self, params_json: str):
        """初始化策略"""

        self.order_dict = {}  # 记录每个数据的订单
        self.position_opened = False  # 是否已开仓标记
        
        # 解析JSON参数
        try:
            self.params = json.loads(params_json)
            
            # 解析开仓日期
            open_date = self.params.get('open_date')
            self.open_date = datetime.strptime(open_date, '%Y-%m-%d').date()
                
            # 解析投资组合
            self.products = self.params.get('products', [])
            self.weights = self.params.get('weights', [])
            
            # 检查数据和权重是否匹配
            if len(self.products) != len(self.weights):
                logger.error(f"产品列表和权重列表长度不匹配: {len(self.products)} != {len(self.weights)}")
                raise ValueError(f"产品列表和权重列表长度不匹配: {len(self.products)} != {len(self.weights)}")
                
            logger.info(f"策略参数解析成功: 开仓日期={self.open_date}, 投资组合={self.products}, 权重={self.weights}")
            
        except json.JSONDecodeError as e:
            raise ValueError(f"策略参数JSON解析失败: {str(e)}")
        except Exception as e:
            raise ValueError(f"策略参数验证失败: {str(e)}")

    def next(self):
        """
        策略核心逻辑：在指定日期按照配置比例买入并持有
        """
        # 如果已经开仓，不再进行操作
        if self.position_opened:
            return

        # 检查是否达到开仓日期
        current_date = self.data0.datetime.date(0)
        if isinstance(current_date, int):
            current_date = bt.num2date(current_date).date()

        if self.open_date and current_date < self.open_date:
            return
        
        logger.info(f"当前日期: {current_date}, 开仓日期: {self.open_date}")
            
        # 计算每个产品的目标金额
        available_cash = self.broker.getcash()
        
        for i, (product, weight) in enumerate(zip(self.products, self.weights)):
            # 获取对应数据源
            data = None
            for d in self.datas:
                if d._name == product:
                    data = d
                    break

            if data is None:
                logger.error(f"未找到产品{product}对应的数据源")
                continue

            # 计算目标金额和数量
            target_value = available_cash * weight
            price = data.close[0]
            size = target_value / price
            
            logger.info(f"产品{product}: 权重={weight}, 目标金额={target_value:.2f}, "
                       f"价格={price:.4f}, 数量={size:.4f}")
            
            # 创建买入订单
            self.order_dict[product] = self.buy(data=data, size=size)
                
        self.position_opened = True

