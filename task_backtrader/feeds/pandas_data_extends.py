import backtrader as bt

class PandasDataExtends(bt.feeds.PandasData):
    """
    扩展的PandasData类，增加dividend字段
    """
    lines = ('dividend',)  # 新增的字段
    params = (
        ('dividend', -1),  # 在DataFrame中对应的列索引，-1表示不使用
    ) 