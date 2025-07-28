"""
模板块系统
提供可扩展的块定义和渲染能力
"""

from .base_block import BaseBlock
from .text_block import TextBlock
from .fund_block import FundBlock

# 注册所有可用的块类型
BLOCK_REGISTRY = {
    'text': TextBlock,
    'fund': FundBlock
}

def get_block_class(block_type: str):
    """获取块类"""
    return BLOCK_REGISTRY.get(block_type, TextBlock)

def create_block(block_data: dict):
    """创建块实例"""
    block_type = block_data.get('block_type', 'text')
    block_class = get_block_class(block_type)
    return block_class(block_data)

__all__ = [
    'BaseBlock',
    'TextBlock',
    'FundBlock',
    'BLOCK_REGISTRY',
    'get_block_class',
    'create_block'
]