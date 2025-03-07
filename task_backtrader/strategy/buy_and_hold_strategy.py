from datetime import datetime, timedelta
import backtrader as bt
import json
from loguru import logger
from typing import Dict, Any
import math  # 添加此行以导入math模块

from task_backtrader.strategy.base_strategy import BaseStrategy

class BuyAndHoldStrategy(BaseStrategy):
    """买入持有策略"""

    # params: 策略参数
    # params = {
    #     "open_date": None,
    #     "close_date": None,
    #     "products": [],
    #     "weights": []
    # }
    def __init__(self, params: Dict[str, Any]):
        """初始化策略"""
        super().__init__(params)  # Call the parent class initializer

        self.position_opened = False  # 是否已开仓标记
        self.open_date = None  # 开仓日期
        self.close_date = None  # 平仓日期
        self.position_closed = False  # 是否已平仓标记
        # 解析JSON参数
        try:
            # 解析开仓日期
            open_date_param = self.params.get('open_date')
            if open_date_param is None or open_date_param == "":
                # 获取各个产品有效净值的最大日期作为开仓日期
                min_date = None
                for d in self.datas:
                    # 获取数据源的有效日期范围
                    valid_dates = [datetime.fromordinal(int(date)).date() for date in d.datetime.array]
                    if not valid_dates:
                        continue
                    # 获取当前数据源的最小日期
                    current_min = min(valid_dates)
                    # 更新最小日期
                    if min_date is None or current_min > min_date:
                        min_date = current_min
                # 设置开仓日期为最小日期
                self.open_date = min_date
                logger.info(f"未指定开仓日期，使用各产品有效净值的最大日期作为开仓日期: {self.open_date}")
            else:
                self.open_date = datetime.strptime(open_date_param, '%Y-%m-%d').date()

            # 解析平仓日期
            close_date_param = self.params.get('close_date')
            if close_date_param is None or close_date_param == "":
                # 获取各个产品有效净值的最小日期作为平仓日期
                max_date = None
                for d in self.datas:
                    # 获取数据源的有效日期范围
                    valid_dates = [datetime.fromordinal(int(date)).date() for date in d.datetime.array]
                    if not valid_dates:
                        continue
                    # 获取当前数据源的最大日期
                    current_max = max(valid_dates)
                    # 更新最大日期
                    if max_date is None or current_max < max_date:
                        max_date = current_max
                # 设置平仓日期为最大日期
                self.close_date = max_date
                logger.info(f"未指定平仓日期，使用各产品有效净值的最小日期作为平仓日期: {self.close_date}")
            else:
                self.close_date = datetime.strptime(close_date_param, '%Y-%m-%d').date()

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

        super().next()
    
        """
        策略核心逻辑：在指定日期按照配置比例买入并持有
        """
        if self.position_closed:
            return
        if self.data0.datetime.get_idx() >= self.data0.datetime.buflen() - 1:
            return

        current_date = self.data0.datetime.date(0)
        next_date = self.data0.datetime.date(1)

        # 如果当前日期小于开仓日期，则不进行操作
        # 在开仓前一天提交购买
        if next_date is not None and next_date >= self.open_date and self.position_opened == False:
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

                # 使用后一日的收盘价
                price = data.close[1]
                size = math.floor(target_value / price)  # 使用math.floor向下取整
                
                logger.info(f"产品{product}: 权重={weight}, 目标金额={target_value:.2f}, "
                        f"价格={price:.4f}, 数量={size:.4f}")
                
                # 创建买入订单
                order = self.buy(data=data, size=size, price=data.close[1])
                self.order_message[order.ref] = "开仓"
            self.position_opened = True
        # 如果当前日期大于平仓日期，则不进行操作
        elif next_date >= self.close_date and self.position_opened:
            # 遍历所有持仓产品进行平仓
            for product in self.products:
                # 获取对应数据源
                data = None
                for d in self.datas:
                    if d._name == product:
                        data = d
                        break
                
                if data is None:
                    logger.error(f"未找到产品{product}对应的数据源")
                    continue
                
                # 获取当前持仓
                pos = self.getposition(data)
                if pos.size > 0:  # 如果有持仓
                    # 创建卖出订单
                    order = self.sell(data=data, size=pos.size)
                    self.order_message[order.ref] = "平仓"
            self.position_closed = True

