"""
模板块系统
提供可扩展的块定义和渲染能力
"""

from .base_block import BaseBlock
from .text_block import TextBlock
from .fund_block import FundBlock
from .index_overview_block import IndexOverviewBlock
from .etf_overview_block import EtfOverviewBlock

# 注册所有可用的块类型
BLOCK_REGISTRY = {
    'text': TextBlock,
    'fund': FundBlock,
    'index_overview': IndexOverviewBlock,
    'etf_overview': EtfOverviewBlock
}

def get_block_class(block_type: str):
    """获取块类"""
    return BLOCK_REGISTRY.get(block_type, TextBlock)

def create_block(block_data: dict, mysql_db=None):
    """创建块实例"""
    block_type = block_data.get('block_type', 'text')
    block_class = get_block_class(block_type)
    
    # 对于需要数据库连接的块类型，传入mysql_db参数
    if block_type in ['fund', 'index_overview', 'etf_overview'] and mysql_db is not None:
        return block_class(block_data, mysql_db=mysql_db)
    else:
        return block_class(block_data)

__all__ = [
    'BaseBlock',
    'TextBlock',
    'FundBlock',
    'IndexOverviewBlock',
    'EtfOverviewBlock',
    'BLOCK_REGISTRY',
    'get_block_class',
    'create_block'
]