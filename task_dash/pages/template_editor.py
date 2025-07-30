"""
JSON模板编辑器页面 - 基于新的模板结构
支持编辑包含模板名称、描述和内容块的报告模板
"""

from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
from typing import Dict, List
from datetime import datetime
import sys
import os

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# 导入新的块系统
from task_dash.blocks import create_block

# 全局样式
CARD_STYLE = {
    'backgroundColor': '#ffffff',
    'border': '1px solid #e0e0e0',
    'borderRadius': '8px',
    'padding': '20px',
    'margin': '10px',
    'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
}

HEADER_STYLE = {
    'textAlign': 'center',
    'color': '#2c3e50',
    'marginBottom': '30px',
    'borderBottom': '2px solid #3498db',
    'paddingBottom': '10px'
}

# 新的默认模板结构
DEFAULT_TEMPLATE = {
    "template_name": "新建报告模板",
    "template_description": "请添加模板描述",
    "template_content": []
}

# Block类型定义
BLOCK_TYPES = {
    "text": {
        "name": "文本块",
        "icon": "📝",
        "description": "纯文本内容，支持Markdown格式",
        "default_config": {
            "content": "请输入文本内容",
            "style": "paragraph"
        }
    },
    "fund": {
        "name": "基金数据",
        "icon": "💰",
        "description": "显示指定基金的净值走势、收益统计和详细数据",
        "default_config": {
            "fund_code": "000001.OF",
            "start_date": "",
            "end_date": "",
            "display_type": "summary",
            "chart_type": "line",
            "show_dividends": True,
            "normalize_data": False,
            "include_stats": True,
            "period_filter": "all",
            "show_benchmark": False
        }
    }
}


def _create_block_parameter_components(block_data: Dict, index: int, mysql_db=None) -> List:
    """创建块参数编辑组件"""
    try:
        # 使用新的块系统
        block = create_block(block_data, mysql_db=mysql_db)
        components = []
        
        for param in block.parameters:
            component_id = {'type': 'block-param', 'index': index, 'param': param.name}
            current_value = block.get_parameter_value(param.name, param.default_value)
            
            if param.param_type == 'text':
                component = dbc.Input(
                    id=component_id,
                    value=current_value,
                    placeholder=param.description,
                    size="sm"
                )
            elif param.param_type == 'textarea':
                component = dbc.Textarea(
                    id=component_id,
                    value=current_value,
                    placeholder=param.description,
                    rows=3,
                    size="sm"
                )
            elif param.param_type == 'number':
                component = dbc.Input(
                    id=component_id,
                    type='number',
                    value=current_value,
                    placeholder=param.description,
                    size="sm"
                )
            elif param.param_type == 'boolean':
                component = dbc.Switch(
                    id=component_id,
                    value=bool(current_value)
                )
            elif param.param_type == 'select':
                component = dcc.Dropdown(
                    id=component_id,
                    options=param.options or [],
                    value=current_value,
                    placeholder=param.description,
                    style={'fontSize': '14px'}
                )
            elif param.param_type == 'multi_select':
                component = dcc.Dropdown(
                    id=component_id,
                    options=param.options or [],
                    value=current_value or [],
                    multi=True,
                    placeholder=param.description,
                    style={'fontSize': '14px'}
                )
            elif param.param_type == 'date':
                component = dcc.DatePickerSingle(
                    id=component_id,
                    date=current_value,
                    placeholder=getattr(param, 'placeholder', param.description),
                    display_format='YYYY-MM-DD',
                    style={'fontSize': '14px', 'width': '100%'}
                )
            else:
                component = dbc.Input(
                    id=component_id,
                    value=current_value,
                    placeholder=getattr(param, 'placeholder', param.description),
                    size="sm"
                )
            
            # 包装组件
            if param.param_type == 'boolean':
                # Switch组件的特殊布局
                components.append(
                    dbc.Row([
                        dbc.Col([
                            html.Div([
                                component,
                                html.Label(
                                    param.label + ("*" if param.required else ""),
                                    className="form-label small ms-2",
                                    style={'display': 'inline-block', 'marginBottom': '0'}
                                )
                            ], className="d-flex align-items-center"),
                            html.Small(param.description, className="text-muted") if param.description else None
                        ])
                    ], className="mb-2")
                )
            else:
                components.append(
                    dbc.Row([
                        dbc.Col([
                            html.Label(
                                param.label + ("*" if param.required else ""),
                                className="form-label small fw-bold"
                            ),
                            component,
                            html.Small(param.description, className="text-muted") if param.description else None
                        ])
                    ], className="mb-2")
                )
        
        return components
        
    except Exception as e:
        # 降级到基本文本输入
        return [
            dbc.Row([
                dbc.Col([
                    html.Label("配置:", className="form-label small fw-bold"),
                    dbc.Textarea(
                        id={'type': 'block-param', 'index': index, 'param': 'config'},
                        value=str(block_data.get('config', {})),
                        placeholder="JSON配置...",
                        rows=3,
                        size="sm"
                    )
                ])
            ], className="mb-2")
        ]

def create_block_card(block_data: Dict, index: int, mysql_db=None) -> dbc.Card:
    """创建块卡片"""
    block_type = block_data.get('block_type', 'text')
    block_config = BLOCK_TYPES.get(block_type, BLOCK_TYPES['text'])
    
    # 创建参数编辑组件
    param_components = _create_block_parameter_components(block_data, index, mysql_db=mysql_db)
    
    return dbc.Card([
        dbc.CardHeader([
            dbc.Row([
                dbc.Col([
                    html.H6([
                        html.Span(block_config['icon'], style={'marginRight': '10px'}),
                        f"{block_data.get('block_title', '未命名块')} ({block_config['name']})"
                    ], className="mb-0")
                ], width=8),
                dbc.Col([
                    dbc.ButtonGroup([
                        dbc.Button("保存", id={'type': 'save-block', 'index': index}, 
                                 color="success", size="sm"),
                        dbc.Button("预览", id={'type': 'preview-block', 'index': index}, 
                                 color="info", size="sm"),
                        dbc.Button("↑", id={'type': 'move-up', 'index': index}, 
                                 color="outline-secondary", size="sm"),
                        dbc.Button("↓", id={'type': 'move-down', 'index': index}, 
                                 color="outline-secondary", size="sm"),
                        dbc.Button("删除", id={'type': 'delete-block', 'index': index}, 
                                 color="danger", size="sm")
                    ])
                ], width=4, className="text-end")
            ])
        ]),
        dbc.CardBody([
            html.P(f"Block ID: {block_data.get('block_id', 'N/A')}", 
                   className="text-muted small mb-2"),
            html.P(block_config['description'], className="mb-3"),
            
            # 块标题编辑
            dbc.Row([
                dbc.Col([
                    html.Label("块标题:", className="form-label fw-bold small"),
                    dbc.Input(
                        id={'type': 'block-title', 'index': index},
                        value=block_data.get('block_title', ''),
                        size="sm",
                        placeholder="输入块标题..."
                    )
                ], width=12)
            ], className="mb-3"),
            
            # 参数编辑组件
            html.Div(param_components, id={'type': 'block-params', 'index': index}),
            
            # 预览区域
            html.Hr(),
            html.Label("预览:", className="form-label fw-bold small text-muted"),
            html.Div(
                _render_block_preview(block_data),
                id={'type': 'block-preview', 'index': index}
            )
        ])
    ], className="mb-3")

def render_block_to_html(block_data: Dict, mysql_db=None, for_pdf: bool = False) -> str:
    """将block渲染为HTML格式（使用新的块系统）"""
    try:
        # 使用新的块系统
        block = create_block(block_data, mysql_db=mysql_db)
        return block.render_to_html(for_pdf=for_pdf)
    except Exception as e:
        # 降级到旧版本处理
        return _legacy_render_block_to_html(block_data)

def _legacy_render_block_to_html(block_data: Dict) -> str:
    """旧版本的block HTML渲染逻辑（作为后备方案）"""
    block_type = block_data.get('block_type', 'text')
    config = block_data.get('config', {})
    block_title = block_data.get('block_title', '未命名块')
    
    if block_type == 'text':
        # 文本块直接返回内容
        content = config.get('content', '空白文本')
        style = config.get('style', 'paragraph')
        
        if style == 'header':
            return f"<h1>{content}</h1>\n"
        elif style == 'paragraph':
            return f"<p>{content}</p>\n"
        else:
            return f"<p>{content}</p>\n"
    else:
        return f'<h2>{block_title}</h2>\n<p><em>未知块类型: {block_type}</em></p>\n'

def _render_block_preview(block_data: Dict) -> html.Div:
    """渲染块预览"""
    return _legacy_render_block_preview(block_data)

def _legacy_render_block_preview(block_data: Dict) -> html.Div:
    """旧版本的块预览逻辑（作为后备方案）"""
    block_type = block_data.get('block_type', 'text')
    config = block_data.get('config', {})
    
    if block_type == 'text':
        content = config.get('content', '空白文本')
        return html.Div([
            html.Small("预览:", className="text-muted"),
            html.P(content[:100] + "..." if len(content) > 100 else content, 
                   className="border-start border-3 ps-3 text-muted")
        ])
    else:
        return html.P("未知块类型", className="text-muted small")

def create_add_block_modal():
    """创建添加块的模态框"""
    return dbc.Modal([
        dbc.ModalHeader([
            html.H4("添加新的内容块", className="modal-title")
        ]),
        dbc.ModalBody([
            html.Label("选择块类型:", className="form-label fw-bold"),
            dbc.RadioItems(
                id='block-type-selector',
                options=[
                    {'label': [
                        html.Span(config['icon'], style={'marginRight': '10px'}),
                        html.Span(config['name']),
                        html.Small(f" - {config['description']}", className="text-muted")
                    ], 'value': block_type}
                    for block_type, config in BLOCK_TYPES.items()
                ],
                value='text',
                className="mb-3"
            ),
            
            html.Label("块标题:", className="form-label fw-bold"),
            dbc.Input(
                id='new-block-title',
                placeholder="输入块标题...",
                className="mb-3"
            ),
            
            html.Label("块ID:", className="form-label fw-bold"),
            dbc.Input(
                id='new-block-id',
                placeholder="自动生成或手动输入...",
                className="mb-3"
            )
        ]),
        dbc.ModalFooter([
            dbc.Button("取消", id="cancel-add-block", color="secondary", className="me-2"),
            dbc.Button("添加", id="confirm-add-block", color="primary")
        ])
    ], id="add-block-modal", size="lg", is_open=False)

def create_full_markdown_preview_modal():
    """创建全文HTML预览模态框"""
    return dbc.Modal([
        dbc.ModalHeader([
            html.H4("完整报告预览", className="modal-title"),
            html.Small("(所有块的渲染内容)", className="text-muted ms-2")
        ]),
        dbc.ModalBody([
            dcc.Loading(
                id="full-markdown-loading",
                children=[
                    html.Div(id="full-markdown-content", style={
                        'backgroundColor': '#ffffff',
                        'padding': '30px',
                        'borderRadius': '8px',
                        'border': '1px solid #dee2e6',
                        'minHeight': '400px',
                        'maxHeight': '80vh',
                        'overflow': 'auto',
                        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
                        'lineHeight': '1.6'
                    })
                ],
                type="default"
            )
        ]),
        dbc.ModalFooter([
            dbc.ButtonGroup([
                html.A(
                    dbc.Button(
                        [html.I(className="fas fa-download me-2"), "导出HTML"],
                        color="success",
                        size="sm"
                    ),
                    id="export-html-link",
                    download="template.html",
                    href="",
                    style={'textDecoration': 'none'}
                ),
                dbc.Button(
                    [html.I(className="fas fa-file-pdf me-2"), "导出PDF"],
                    id="export-pdf-btn",
                    color="danger",
                    size="sm",
                    className="ms-2"
                ),
                dbc.Button("关闭", id="close-full-markdown-modal", color="secondary", size="sm", className="ms-2")
            ])
        ])
    ], id="full-markdown-modal", size="xl", is_open=False, scrollable=True)

def create_preview_modal():
    """创建预览模态框"""
    return dbc.Modal([
        dbc.ModalHeader([
            html.H4(id="preview-modal-title", className="modal-title")
        ]),
        dbc.ModalBody([
            dcc.Loading(
                id="preview-loading",
                children=[
                    html.Div(id="preview-content", style={
                        'backgroundColor': '#ffffff',
                        'padding': '20px',
                        'borderRadius': '8px',
                        'border': '1px solid #dee2e6',
                        'minHeight': '300px',
                        'maxHeight': '600px',
                        'overflow': 'auto'
                    })
                ],
                type="default"
            )
        ]),
        dbc.ModalFooter([
            dbc.Button("关闭", id="close-preview-modal", color="secondary")
        ])
    ], id="preview-modal", size="xl", is_open=False)

def create_template_editor_page(mysql_db):
    """创建模板编辑器页面"""
    
    # 存储数据库连接用于后续使用
    global _mysql_db
    _mysql_db = mysql_db
    
    layout = dbc.Container([
        # 页面标题
        html.H1("JSON模板编辑器", style=HEADER_STYLE),
        
        # 模板基本信息编辑区域
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("模板管理", className="mb-0", style={'color': '#2c3e50'})
                    ]),
                    dbc.CardBody([
                        html.Label("选择模板:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='template-selector',
                            placeholder="选择要编辑的模板...",
                            className="mb-3"
                        ),
                        
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("加载模板", id='load-template-btn', 
                                         color="primary", size="sm", className="me-2")
                            ], width=4),
                            dbc.Col([
                                dbc.Button("复制模板", id='copy-template-btn', 
                                         color="secondary", size="sm", className="me-2")
                            ], width=4),
                            dbc.Col([
                                dbc.Button("新建模板", id='new-template-btn', 
                                         color="info", size="sm")
                            ], width=4)
                        ], className="mb-3"),
                        
                        html.Label("模板名称:", className="form-label fw-bold"),
                        dbc.Input(
                            id='template-name-input',
                            placeholder="输入模板名称...",
                            className="mb-3"
                        ),
                        
                        html.Label("模板描述:", className="form-label fw-bold"),
                        dbc.Textarea(
                            id='template-description-input',
                            placeholder="输入模板描述...",
                            className="mb-3"
                        ),
                        
                        dbc.Button("保存模板", id='save-template-btn', 
                                 color="success", className="w-100")
                    ])
                ])
            ], width=4),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        dbc.Row([
                            dbc.Col([
                                html.H5("JSON预览", className="mb-0", style={'color': '#2c3e50'})
                            ], width=8),
                            dbc.Col([
                                dbc.Button(
                                    [html.I(className="fas fa-eye me-2"), "完整预览"],
                                    id='full-markdown-preview-btn',
                                    color="info",
                                    size="sm",
                                    className="float-end"
                                )
                            ], width=4)
                        ])
                    ]),
                    dbc.CardBody([
                        html.Pre(
                            id='template-preview',
                            style={
                                'backgroundColor': '#f8f9fa',
                                'padding': '15px',
                                'borderRadius': '5px',
                                'maxHeight': '400px',
                                'overflow': 'auto',
                                'fontSize': '12px',
                                'border': '1px solid #dee2e6'
                            }
                        )
                    ])
                ])
            ], width=8)
        ], className="mb-4"),
        
        # 模板内容编辑区域
        dbc.Card([
            dbc.CardHeader([
                dbc.Row([
                    dbc.Col([
                        html.H4("模板内容块", className="mb-0", style={'color': '#2c3e50'})
                    ], width=8),
                    dbc.Col([
                        dbc.Button("+ 添加新块", id='add-block-btn', 
                                 color="success", size="sm")
                    ], width=4, className="text-end")
                ])
            ]),
            dbc.CardBody([
                html.Div(id='template-blocks-display'),
                html.Div(id='empty-template-message', 
                        children=[
                            html.Div([
                                html.I(className="fas fa-plus-circle fa-3x text-muted mb-3"),
                                html.H5("模板内容为空", className="text-muted"),
                                html.P("点击\"添加新块\"开始构建您的报告模板", className="text-muted")
                            ], style={'textAlign': 'center', 'padding': '60px 0'})
                        ],
                        style={'display': 'block'})
            ])
        ], className="mb-4"),
        
        # 添加块的模态框
        create_add_block_modal(),
        
        # 预览模态框
        create_preview_modal(),
        
        # 全文Markdown预览模态框
        create_full_markdown_preview_modal(),
        
        # 存储组件
        dcc.Store(id='template-store', data=DEFAULT_TEMPLATE.copy()),
        dcc.Store(id='available-templates-store', data=[]),
        dcc.Store(id='current-template-file', data=None),
        
        # 消息提示
        html.Div(id='message-display')
    ], fluid=True)
    
    return layout

# 全局数据库连接变量
_mysql_db = None

# 辅助函数获取数据库连接
def get_mysql_db():
    """获取当前的数据库连接"""
    return _mysql_db

# 页面布局
layout = create_template_editor_page(None)