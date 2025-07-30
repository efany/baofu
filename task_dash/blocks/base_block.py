"""
基础块类定义
所有块类型的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class BlockParameter:
    """块参数定义"""
    name: str                    # 参数名称
    label: str                   # 显示标签
    param_type: str              # 参数类型: text, number, boolean, select, multi_select, date
    default_value: Any           # 默认值
    required: bool = False       # 是否必需
    options: Optional[List[Dict]] = None  # 选择项 (用于select类型)
    description: str = ""        # 参数描述
    validation: Optional[Dict] = None     # 验证规则
    placeholder: str = ""        # 占位符文本


class BaseBlock(ABC):
    """块基类"""
    
    def __init__(self, block_data: Dict):
        """初始化块"""
        self.block_id = block_data.get('block_id', '')
        self.block_type = block_data.get('block_type', '')
        self.block_title = block_data.get('block_title', '')
        self.config = block_data.get('config', {})
        self._block_data = block_data
    
    @property
    @abstractmethod
    def block_name(self) -> str:
        """块名称"""
        pass
    
    @property
    @abstractmethod
    def block_icon(self) -> str:
        """块图标"""
        pass
    
    @property
    @abstractmethod
    def block_description(self) -> str:
        """块描述"""
        pass
    
    @property
    @abstractmethod
    def parameters(self) -> List[BlockParameter]:
        """块参数定义"""
        pass
    
    @property
    def default_config(self) -> Dict:
        """默认配置"""
        return {param.name: param.default_value for param in self.parameters}
    
    def get_parameter_value(self, param_name: str, default=None):
        """获取参数值"""
        return self.config.get(param_name, default)
    
    def set_parameter_value(self, param_name: str, value: Any):
        """设置参数值"""
        self.config[param_name] = value
    
    def validate_config(self) -> List[str]:
        """验证配置，返回错误列表"""
        errors = []
        
        for param in self.parameters:
            value = self.get_parameter_value(param.name)
            
            # 检查必需参数
            if param.required and (value is None or value == ""):
                errors.append(f"参数 '{param.label}' 是必需的")
                continue
            
            # 类型验证
            if value is not None and param.validation:
                validation_errors = self._validate_parameter(param, value)
                errors.extend(validation_errors)
        
        return errors
    
    def _validate_parameter(self, param: BlockParameter, value: Any) -> List[str]:
        """验证单个参数"""
        errors = []
        validation = param.validation or {}
        
        if param.param_type == 'number':
            try:
                num_value = float(value)
                if 'min' in validation and num_value < validation['min']:
                    errors.append(f"参数 '{param.label}' 不能小于 {validation['min']}")
                if 'max' in validation and num_value > validation['max']:
                    errors.append(f"参数 '{param.label}' 不能大于 {validation['max']}")
            except (ValueError, TypeError):
                errors.append(f"参数 '{param.label}' 必须是数字")
        
        elif param.param_type == 'text':
            if isinstance(value, str):
                if 'min_length' in validation and len(value) < validation['min_length']:
                    errors.append(f"参数 '{param.label}' 长度不能少于 {validation['min_length']} 字符")
                if 'max_length' in validation and len(value) > validation['max_length']:
                    errors.append(f"参数 '{param.label}' 长度不能超过 {validation['max_length']} 字符")
        
        return errors
    
    @abstractmethod
    def render_to_html(self, for_pdf: bool = False) -> str:
        """渲染为HTML
        
        Args:
            for_pdf: 是否为PDF导出，影响图片路径和样式
        """
        pass
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'block_id': self.block_id,
            'block_type': self.block_type,
            'block_title': self.block_title,
            'config': self.config
        }
    
    @classmethod
    def from_dict(cls, block_data: Dict):
        """从字典创建实例"""
        return cls(block_data)
    
    def get_ui_components(self):
        """获取UI组件配置（用于动态生成编辑界面）"""
        from dash import html, dcc
        import dash_bootstrap_components as dbc
        
        components = []
        
        for param in self.parameters:
            placeholder_text = param.placeholder or param.description
            
            if param.param_type == 'text':
                component = dbc.Input(
                    id=f'{self.block_id}-{param.name}',
                    placeholder=placeholder_text,
                    value=self.get_parameter_value(param.name, param.default_value),
                    style={'width': '100%'}
                )
            elif param.param_type == 'number':
                component = dbc.Input(
                    id=f'{self.block_id}-{param.name}',
                    type='number',
                    placeholder=placeholder_text,
                    value=self.get_parameter_value(param.name, param.default_value),
                    style={'width': '100%'}
                )
            elif param.param_type == 'boolean':
                component = dbc.Switch(
                    id=f'{self.block_id}-{param.name}',
                    label="",  # 空标签，因为已经有外层标签
                    value=self.get_parameter_value(param.name, param.default_value)
                )
            elif param.param_type == 'select':
                component = dcc.Dropdown(
                    id=f'{self.block_id}-{param.name}',
                    options=param.options or [],
                    value=self.get_parameter_value(param.name, param.default_value),
                    placeholder=placeholder_text,
                    style={'width': '100%'}
                )
            elif param.param_type == 'multi_select':
                component = dcc.Dropdown(
                    id=f'{self.block_id}-{param.name}',
                    options=param.options or [],
                    value=self.get_parameter_value(param.name, param.default_value) or [],
                    multi=True,
                    placeholder=placeholder_text,
                    style={'width': '100%'}
                )
            elif param.param_type == 'textarea':
                component = dbc.Textarea(
                    id=f'{self.block_id}-{param.name}',
                    placeholder=placeholder_text,
                    value=self.get_parameter_value(param.name, param.default_value),
                    rows=3,
                    style={'width': '100%'}
                )
            elif param.param_type == 'date':
                component = dcc.DatePickerSingle(
                    id=f'{self.block_id}-{param.name}',
                    placeholder=placeholder_text,
                    date=self.get_parameter_value(param.name, param.default_value),
                    display_format='YYYY-MM-DD',
                    style={'width': '100%'}
                )
            else:
                # 默认为文本输入
                component = dbc.Input(
                    id=f'{self.block_id}-{param.name}',
                    placeholder=placeholder_text,
                    value=self.get_parameter_value(param.name, param.default_value),
                    style={'width': '100%'}
                )
            
            # 包装组件
            components.append(
                dbc.Row([
                    dbc.Col([
                        html.Label(
                            param.label + ("*" if param.required else ""), 
                            className="form-label fw-bold"
                        ),
                        component,
                        html.Small(param.description, className="text-muted") if param.description else None
                    ])
                ], className="mb-3")
            )
        
        return components