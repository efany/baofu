"""
模板编辑器回调函数 - 基于新的模板结构
处理基于block结构的模板编辑器的所有交互逻辑
"""

from dash import Input, Output, State, callback, ALL, no_update, html, ctx, clientside_callback
import dash_bootstrap_components as dbc
import json
import os
import sys
import copy
import re
import urllib.parse
import base64
from datetime import datetime
from typing import Dict, List, Any
import uuid
import traceback
from task_utils.pdf_utils import template_to_pdf, check_pdf_dependencies, get_dependency_install_instructions, generate_pdf_filename
from task_dash.pages.template_editor import render_block_to_html, DEFAULT_TEMPLATE, BLOCK_TYPES, create_block_card, _render_block_preview
import logging

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_template_from_file(filepath: str) -> Dict:
    """从文件加载模板"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载模板失败: {e}")
        return DEFAULT_TEMPLATE.copy()

def get_available_templates() -> List[Dict]:
    """获取可用的模板列表"""
    templates = []
    
    # 添加默认模板选项
    templates.append({
        'label': '默认模板',
        'value': 'default',
        'filepath': None
    })
    
    # 扫描config目录中的JSON文件
    config_dir = os.path.join(os.path.dirname(__file__), '../..', 'config')
    if os.path.exists(config_dir):
        for filename in os.listdir(config_dir):
            if filename.endswith('.json') and filename != 'default_template.json':
                filepath = os.path.join(config_dir, filename)
                try:
                    # 尝试读取文件以验证是否为有效的模板
                    with open(filepath, 'r', encoding='utf-8') as f:
                        template_data = json.load(f)
                        if 'template_name' in template_data:  # 验证新格式
                            templates.append({
                                'label': template_data.get('template_name', filename),
                                'value': filename,
                                'filepath': filepath
                            })
                except:
                    continue

    return templates

def generate_block_id(block_type: str, existing_ids: List[str]) -> str:
    """生成唯一的块ID"""
    base_id = f"{block_type}_block"
    if base_id not in existing_ids:
        return base_id
    
    counter = 1
    while f"{base_id}_{counter}" in existing_ids:
        counter += 1
    return f"{base_id}_{counter}"

def register_template_editor_callbacks(app, mysql_db):
    """注册模板编辑器相关的回调函数"""
    
    
    @app.callback(
        [Output('template-selector', 'options'),
         Output('available-templates-store', 'data')],
        Input('template-selector', 'id')  # 页面加载时触发
    )
    def load_available_templates(_):
        """加载可用模板列表"""
        templates = get_available_templates()
        options = [{'label': t['label'], 'value': t['value']} for t in templates]
        return options, templates

    @app.callback(
        [Output('template-store', 'data'),
         Output('template-name-input', 'value'),
         Output('template-description-input', 'value'),
         Output('current-template-file', 'data'),
         Output('template-selector', 'value')],
        [Input('load-template-btn', 'n_clicks'),
         Input('copy-template-btn', 'n_clicks'),
         Input('new-template-btn', 'n_clicks')],
        [State('template-selector', 'value'),
         State('available-templates-store', 'data')]
    )
    def manage_template(load_clicks, copy_clicks, new_clicks, selected_template, available_templates):
        """模板管理功能"""
        logger.info(f"模板管理触发 - load:{load_clicks}, copy:{copy_clicks}, new:{new_clicks}, selected:{selected_template}")
        
        if not ctx.triggered:
            return no_update, no_update, no_update, no_update, no_update
        
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
        logger.info(f"触发的组件ID: {triggered_id}")
        
        if triggered_id == 'load-template-btn' and selected_template:
            # 加载选中的模板
            logger.info(f"加载模板: {selected_template}")
            if selected_template == 'default':
                # 加载默认模板
                default_path = os.path.join(os.path.dirname(__file__), '../..', 'config', 'default_template.json')
                template_data = load_template_from_file(default_path)
                logger.info(f"加载默认模板: {default_path}")
            else:
                # 加载指定模板文件
                template_info = next((t for t in available_templates if t['value'] == selected_template), None)
                if template_info and template_info['filepath']:
                    template_data = load_template_from_file(template_info['filepath'])
                    logger.info(f"加载模板文件: {template_info['filepath']}")
                else:
                    template_data = DEFAULT_TEMPLATE.copy()
                    logger.warning(f"未找到模板文件，使用默认模板: {selected_template}")
            
            return (template_data,
                    template_data.get('template_name', ''),
                    template_data.get('template_description', ''),
                    selected_template,
                    selected_template)  # 保持选择器的值
        
        elif triggered_id == 'copy-template-btn' and selected_template:
            # 复制选中的模板
            if selected_template == 'default':
                default_path = os.path.join(os.path.dirname(__file__), '../..', 'config', 'default_template.json')
                template_data = load_template_from_file(default_path)
            else:
                template_info = next((t for t in available_templates if t['value'] == selected_template), None)
                if template_info and template_info['filepath']:
                    template_data = load_template_from_file(template_info['filepath'])
                else:
                    template_data = DEFAULT_TEMPLATE.copy()
            
            # 修改名称以示区别
            template_data['template_name'] = f"{template_data.get('template_name', '未命名模板')} - 副本"
            
            return (template_data,
                    template_data.get('template_name', ''),
                    template_data.get('template_description', ''),
                    None,  # 复制的模板不关联原文件
                    no_update)  # 保持当前选择
        
        elif triggered_id == 'new-template-btn':
            # 创建新模板
            new_template = DEFAULT_TEMPLATE.copy()
            return (new_template,
                    new_template.get('template_name', ''),
                    new_template.get('template_description', ''),
                    None,
                    no_update)  # 清空选择器
        
        return no_update, no_update, no_update, no_update, no_update

    @app.callback(
        Output('template-preview', 'children'),
        [Input('template-store', 'data'),
         Input('template-name-input', 'value'),
         Input('template-description-input', 'value')]
    )
    def update_template_preview(template_data, name, description):
        """更新模板预览"""
        if template_data:
            # 创建副本以避免修改原数据
            preview_data = template_data.copy()
            preview_data['template_name'] = name or "未命名模板"
            preview_data['template_description'] = description or "无描述"
            
            # 返回预览内容
            preview_content = json.dumps(preview_data, ensure_ascii=False, indent=2)
            return preview_content
        return "{}"

    @app.callback(
        Output('template-store', 'data', allow_duplicate=True),
        [Input('template-name-input', 'value'),
         Input('template-description-input', 'value')],
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def update_template_basic_info(name, description, template_data):
        """更新模板基本信息"""
        logger.info(f"更新模板基本信息 - name:{name}, description:{description[:50] if description else None}...")
        
        # 避免在初始加载时触发
        if not ctx.triggered:
            return template_data
            
        updated_template = template_data.copy()
        updated_template['template_name'] = name or "新建报告模板"
        updated_template['template_description'] = description or "请添加模板描述"
        return updated_template

    @app.callback(
        [Output('template-blocks-display', 'children'),
         Output('empty-template-message', 'style')],
        Input('template-store', 'data'),
        prevent_initial_call=True
    )
    def display_template_blocks(template_data):
        """显示模板内容块"""
        logger.info(f"显示模板内容块 - 块数量: {len(template_data.get('template_content', [])) if template_data else 0}")
        
        blocks = template_data.get('template_content', [])
        
        if not blocks:
            return [], {'display': 'block'}
        
        # 只有在非参数更新时才重新渲染所有块
        if ctx.triggered:
            triggered_prop_id = ctx.triggered[0]['prop_id']
            if ('block-title' in triggered_prop_id or 'block-param' in triggered_prop_id):
                return no_update, no_update
        
        block_cards = []
        for index, block in enumerate(blocks):
            block_cards.append(create_block_card(block, index, mysql_db=mysql_db))
        
        return block_cards, {'display': 'none'}

    @app.callback(
        Output('add-block-modal', 'is_open'),
        [Input('add-block-btn', 'n_clicks'),
         Input('cancel-add-block', 'n_clicks'),
         Input('confirm-add-block', 'n_clicks')],
        State('add-block-modal', 'is_open')
    )
    def toggle_add_block_modal(add_clicks, cancel_clicks, confirm_clicks, is_open):
        """控制添加块模态框的显示"""
        logger.info(f"切换添加块模态框 - add:{add_clicks}, cancel:{cancel_clicks}, confirm:{confirm_clicks}, is_open:{is_open}")
        
        if not ctx.triggered:
            return is_open
        
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
        logger.info(f"模态框触发组件: {triggered_id}")
        
        if triggered_id in ['add-block-btn', 'cancel-add-block']:
            return not is_open
        elif triggered_id == 'confirm-add-block':
            return False  # 确认后关闭模态框
        
        return is_open

    @app.callback(
        [Output('new-block-title', 'value'),
         Output('new-block-id', 'value'),
         Output('block-type-selector', 'value')],
        [Input('add-block-modal', 'is_open'),
         Input('block-type-selector', 'value')],
        [State('template-store', 'data'),
         State('new-block-title', 'value'),
         State('add-block-modal', 'is_open')],
        prevent_initial_call=True
    )
    def manage_modal_inputs(modal_opened, block_type, template_data, block_title, was_open):
        """管理模态框输入：清空内容和自动生成ID"""
        logger.info(f"管理模态框输入 - opened:{modal_opened}, type:{block_type}, title:{block_title}")
        
        if not ctx.triggered:
            return no_update, no_update, no_update
        
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
        logger.info(f"模态框输入触发: {triggered_id}")
        
        # 如果是打开模态框，清空所有内容
        if triggered_id == 'add-block-modal' and modal_opened and not was_open:
            return "", "", "text"
        
        # 如果是改变块类型，自动生成ID
        elif triggered_id == 'block-type-selector' and block_type:
            # 获取现有的块ID列表
            existing_ids = [block.get('block_id', '') for block in template_data.get('template_content', [])]
            
            # 如果有标题，尝试基于标题生成ID
            if block_title:
                # 简单的中文转英文映射
                title_map = {
                    '标题': 'title', '摘要': 'summary', '表格': 'table', 
                    '图表': 'chart', '基金': 'fund', '股票': 'stock',
                    '策略': 'strategy', '债券': 'bond', '外汇': 'forex'
                }
                safe_title = ""
                for char in block_title:
                    if char in title_map:
                        safe_title += title_map[char]
                    elif char.isalnum():
                        safe_title += char
                
                if safe_title:
                    base_id = f"{safe_title}_{block_type}"
                    if base_id not in existing_ids:
                        return no_update, base_id, no_update
            
            # 生成默认ID
            generated_id = generate_block_id(block_type, existing_ids)
            return no_update, generated_id, no_update
        
        return no_update, no_update, no_update

    @app.callback(
        Output('template-store', 'data', allow_duplicate=True),
        Input('confirm-add-block', 'n_clicks'),
        [State('block-type-selector', 'value'),
         State('new-block-title', 'value'),
         State('new-block-id', 'value'),
         State('template-store', 'data')],
        prevent_initial_call=True
    )
    def add_new_block(confirm_clicks, block_type, block_title, block_id, template_data):
        """添加新的内容块"""
        logger.info(f"添加新块 - type:{block_type}, title:{block_title}, id:{block_id}")
        
        if not confirm_clicks or not block_type:
            return template_data
        
        # 创建新块
        new_block = {
            'block_id': block_id or generate_block_id(block_type, 
                [block.get('block_id', '') for block in template_data.get('template_content', [])]),
            'block_type': block_type,
            'block_title': block_title or f"新{BLOCK_TYPES[block_type]['name']}",
            'config': BLOCK_TYPES[block_type]['default_config'].copy()
        }
        
        # 添加到模板内容中
        updated_template = template_data.copy()
        if 'template_content' not in updated_template:
            updated_template['template_content'] = []
        
        updated_template['template_content'].append(new_block)
        
        return updated_template

    @app.callback(
        Output('template-store', 'data', allow_duplicate=True),
        [Input({'type': 'delete-block', 'index': ALL}, 'n_clicks'),
         Input({'type': 'move-up', 'index': ALL}, 'n_clicks'),
         Input({'type': 'move-down', 'index': ALL}, 'n_clicks')],
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def manage_blocks(delete_clicks, move_up_clicks, move_down_clicks, template_data):
        """管理块操作：删除、上移、下移"""
        logger.info(f"管理块操作 - delete:{sum(x for x in delete_clicks if x is not None) if delete_clicks else 0}, up:{sum(x for x in move_up_clicks if x is not None) if move_up_clicks else 0}, down:{sum(x for x in move_down_clicks if x is not None) if move_down_clicks else 0}")
        
        if not ctx.triggered:
            return template_data
        
        triggered_id = ctx.triggered[0]['prop_id']
        logger.info(f"块操作触发: {triggered_id}")
        
        if not any(delete_clicks + move_up_clicks + move_down_clicks):
            return template_data
        
        # 解析触发的按钮
        match = re.search(r'"index":(\d+)', triggered_id)
        if not match:
            logger.warning(f"无法解析索引: {triggered_id}")
            return template_data
        
        index = int(match.group(1))
        logger.info(f"操作块索引: {index}")
        blocks = template_data.get('template_content', [])
        
        if index >= len(blocks):
            return template_data
        
        updated_template = template_data.copy()
        updated_blocks = blocks.copy()
        
        if 'delete-block' in triggered_id:
            # 删除块
            updated_blocks.pop(index)
        elif 'move-up' in triggered_id and index > 0:
            # 上移块
            updated_blocks[index], updated_blocks[index-1] = updated_blocks[index-1], updated_blocks[index]
        elif 'move-down' in triggered_id and index < len(blocks) - 1:
            # 下移块
            updated_blocks[index], updated_blocks[index+1] = updated_blocks[index+1], updated_blocks[index]
        
        updated_template['template_content'] = updated_blocks
        return updated_template


    @app.callback(
        [Output('message-display', 'children'),
         Output('template-selector', 'options', allow_duplicate=True),
         Output('current-template-file', 'data', allow_duplicate=True),
         Output('template-selector', 'value', allow_duplicate=True)],
        Input('save-template-btn', 'n_clicks'),
        [State('template-store', 'data'),
         State('current-template-file', 'data'),
         State('available-templates-store', 'data')],
        prevent_initial_call=True
    )
    def save_template(n_clicks, template_data, current_file, available_templates):
        """保存模板"""
        logger.info(f"保存模板 - clicks:{n_clicks}, current_file:{current_file}")
        
        if not n_clicks:
            return "", no_update, no_update, no_update

        try:
            # 创建保存目录
            config_dir = os.path.join(os.path.dirname(__file__), '../..', 'config')
            os.makedirs(config_dir, exist_ok=True)
            
            # 更新模板数据
            save_data = template_data.copy()
            template_name = save_data.get('template_name', '未命名模板')
            
            if current_file:
                if current_file == 'default':
                    # 默认模板保存到 default_template.json
                    filepath = os.path.join(config_dir, 'default_template.json')
                    filename = 'default_template.json'
                    action_msg = "默认模板已更新"
                else:
                    # 更新现有文件
                    template_info = next((t for t in available_templates if t['value'] == current_file), None)
                    if template_info and template_info['filepath']:
                        filepath = template_info['filepath']
                        filename = current_file
                        action_msg = "模板已更新"
                    else:
                        # 当前文件不存在，创建新文件并更新当前文件引用
                        safe_name = "".join(c for c in template_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                        filename = f"{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        filepath = os.path.join(config_dir, filename)
                        current_file = filename  # 更新当前文件引用
                        action_msg = "模板已保存为新文件"
            else:
                # 创建新文件（新建模板）
                safe_name = "".join(c for c in template_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                filepath = os.path.join(config_dir, filename)
                current_file = filename  # 设置当前文件引用
                action_msg = "模板已保存为新文件"
            
            # 保存文件
            logger.info(f"保存模板到文件: {filepath}")
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            logger.info(f"模板保存成功: {filename}")
            
            # 更新模板列表
            updated_templates = get_available_templates()
            options = [{'label': t['label'], 'value': t['value']} for t in updated_templates]
            
            success_msg = dbc.Alert([
                html.I(className="fas fa-check-circle", style={'marginRight': '10px'}),
                f"{action_msg}: {filename}"
            ], color="success", className="mt-3")
            
            return success_msg, options, current_file, current_file
            
        except Exception as e:
            logger.error(f"保存模板失败: {str(e)}")
            error_msg = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle", style={'marginRight': '10px'}),
                f"保存失败: {str(e)}"
            ], color="danger", className="mt-3")
            
            return error_msg, no_update, no_update, no_update

    @app.callback(
        [Output('preview-modal', 'is_open'),
         Output('preview-modal-title', 'children'),
         Output('preview-content', 'children')],
        [Input({'type': 'preview-block', 'index': ALL}, 'n_clicks'),
         Input('close-preview-modal', 'n_clicks')],
        [State('preview-modal', 'is_open'),
         State('template-store', 'data')],
        prevent_initial_call=True
    )
    def handle_preview_modal(preview_clicks, close_clicks, is_open, template_data):
        """处理预览模态框的显示和内容"""
        logger.info(f"处理预览模态框 - preview_clicks:{sum(x for x in preview_clicks if x is not None) if preview_clicks else 0}, close:{close_clicks}")
        
        if not ctx.triggered:
            return is_open, no_update, no_update
        
        triggered_id = ctx.triggered[0]['prop_id']
        logger.info(f"预览模态框触发: {triggered_id}")
        
        # 关闭模态框
        if 'close-preview-modal' in triggered_id:
            return False, no_update, no_update
        
        # 打开预览模态框
        if 'preview-block' in triggered_id and any(preview_clicks):
            # 解析点击的按钮索引
            match = re.search(r'"index":(\d+)', triggered_id)
            if not match:
                logger.warning(f"无法解析预览索引: {triggered_id}")
                return is_open, no_update, no_update
            
            index = int(match.group(1))
            logger.info(f"预览块索引: {index}")
            blocks = template_data.get('template_content', [])
            
            if index >= len(blocks):
                return is_open, no_update, no_update
            
            block_data = blocks[index]
            block_title = block_data.get('block_title', '未命名块')
            
            # 渲染为HTML
            try:
                html_content = render_block_to_html(block_data, mysql_db=mysql_db)
                
                # 创建预览内容 - 使用iframe显示渲染效果
                preview_content = html.Div([
                    html.Iframe(
                        srcDoc=html_content,
                        style={
                            'width': '100%',
                            'height': '400px',  # 设置一个合适的默认高度
                            'border': '1px solid #dee2e6',
                            'borderRadius': '4px'
                        }
                    )
                ], style={
                    'backgroundColor': '#ffffff',
                    'padding': '20px',
                    'borderRadius': '8px',
                    'border': '1px solid #dee2e6',
                    'overflow': 'auto'
                })
                
            except Exception as e:
                logger.error(f"渲染块失败: {str(e)}", exc_info=True)
                preview_content = html.Div([
                    html.H5("渲染失败", style={'color': '#dc3545', 'marginBottom': '15px'}),
                    html.P(f"错误: {str(e)}", style={'color': '#721c24'}),
                    html.Details([
                        html.Summary("查看错误详情", style={'cursor': 'pointer', 'marginTop': '10px'}),
                        html.Pre(traceback.format_exc(), style={
                            'backgroundColor': '#f8d7da',
                            'padding': '10px',
                            'borderRadius': '4px',
                            'fontSize': '12px',
                            'maxHeight': '200px',
                            'overflow': 'auto'
                        })
                    ])
                ], style={
                    'backgroundColor': '#f8d7da',
                    'padding': '20px',
                    'borderRadius': '8px',
                    'border': '1px solid #f5c6cb'
                })
            
            return True, f"预览: {block_title}", preview_content
        
        return is_open, no_update, no_update

    @app.callback(
        Output('template-store', 'data', allow_duplicate=True),
        [Input({'type': 'block-title', 'index': ALL}, 'value'),
         Input({'type': 'block-param', 'index': ALL, 'param': ALL}, 'value')],
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def update_block_parameters(title_values, param_values, template_data):
        """更新块参数"""
        if not ctx.triggered or not template_data:
            return no_update
        
        triggered_prop_id = ctx.triggered[0]['prop_id']
        triggered_value = ctx.triggered[0]['value']
        logger.info(f"更新块参数 - prop_id:{triggered_prop_id}, value:{str(triggered_value)[:100]}...")
        
        # 避免空值触发
        if triggered_value is None:
            return no_update
        
        # 解析触发的组件
        
        # 深拷贝模板数据以避免引用问题
        updated_template = copy.deepcopy(template_data)
        
        # 检查是否是标题更新
        title_match = re.search(r'"type":"block-title","index":(\d+)', triggered_prop_id)
        if title_match:
            index = int(title_match.group(1))
            
            if index < len(updated_template.get('template_content', [])):
                current_title = updated_template['template_content'][index].get('block_title', '')
                
                # 只有当值真正改变时才更新
                if current_title != triggered_value:
                    updated_template['template_content'][index]['block_title'] = triggered_value or "未命名块"
                    return updated_template
            
            return no_update
        
        # 检查是否是参数更新
        param_match = re.search(r'"type":"block-param","index":(\d+),"param":"([^"]+)"', triggered_prop_id)
        if param_match:
            index = int(param_match.group(1))
            param_name = param_match.group(2)
            
            if index < len(updated_template.get('template_content', [])):
                if 'config' not in updated_template['template_content'][index]:
                    updated_template['template_content'][index]['config'] = {}
                
                current_value = updated_template['template_content'][index]['config'].get(param_name)
                
                # 只有当值真正改变时才更新
                if current_value != triggered_value:
                    updated_template['template_content'][index]['config'][param_name] = triggered_value
                    return updated_template
            
            return no_update
        
        return no_update

    @app.callback(
        Output({'type': 'block-preview', 'index': ALL}, 'children'),
        [Input({'type': 'block-title', 'index': ALL}, 'value'),
         Input({'type': 'block-param', 'index': ALL, 'param': ALL}, 'value')],
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def update_block_previews(title_values, param_values, template_data):
        """更新块预览"""
        logger.info(f"更新块预览 - 块数量: {len(template_data.get('template_content', [])) if template_data else 0}")
        
        if not ctx.triggered or not template_data:
            return no_update
        
        blocks = template_data.get('template_content', [])
        previews = []
        
        for i, block in enumerate(blocks):
            # 获取当前输入值并更新block数据
            updated_block = block.copy()
            
            # 更新标题（如果有对应的输入）
            if i < len(title_values) and title_values[i] is not None:
                updated_block['block_title'] = title_values[i]
            
            # 更新参数（这里需要更复杂的逻辑来匹配参数）
            # 为了简化，暂时使用原始block数据
            previews.append(_render_block_preview(updated_block))
        
        return previews

    @app.callback(
        [Output('template-store', 'data', allow_duplicate=True),
         Output('message-display', 'children', allow_duplicate=True)],
        Input({'type': 'save-block', 'index': ALL}, 'n_clicks'),
        [State({'type': 'block-title', 'index': ALL}, 'value'),
         State({'type': 'block-param', 'index': ALL, 'param': ALL}, 'value'),
         State({'type': 'block-param', 'index': ALL, 'param': ALL}, 'id'),
         State('template-store', 'data'),
         State('current-template-file', 'data'),
         State('available-templates-store', 'data')],
        prevent_initial_call=True
    )
    def save_single_block(save_clicks, title_values, param_values, param_ids, template_data, current_file, available_templates):
        """保存单个块的更改"""
        logger.info(f"保存单个块 - clicks:{sum(x for x in save_clicks if x is not None) if save_clicks else 0}")
        
        if not ctx.triggered or not any(save_clicks) or not template_data:
            return no_update, no_update
        
        triggered_prop_id = ctx.triggered[0]['prop_id']
        logger.info(f"保存块触发: {triggered_prop_id}")
        
        # 解析点击的保存按钮索引
        match = re.search(r'"index":(\d+)', triggered_prop_id)
        if not match:
            logger.warning(f"无法解析保存块索引: {triggered_prop_id}")
            return no_update, no_update
        
        index = int(match.group(1))
        logger.info(f"保存块索引: {index}")
        blocks = template_data.get('template_content', [])
        
        if index >= len(blocks):
            return no_update, no_update
        
        try:
            # 深拷贝模板数据
            updated_template = copy.deepcopy(template_data)
            
            # 读取块内的当前参数值并生成完整的块JSON内容
            current_block = updated_template['template_content'][index]
            
            # 更新块标题
            if index < len(title_values) and title_values[index] is not None:
                current_block['block_title'] = title_values[index] or "未命名块"
            
            # 收集该块的所有参数值
            if 'config' not in current_block:
                current_block['config'] = {}
            
            # 使用参数ID和值列表来收集当前块的参数
            for param_id, param_value in zip(param_ids, param_values):
                if (param_id.get('type') == 'block-param' and 
                    param_id.get('index') == index and 
                    param_value is not None):
                    param_name = param_id.get('param')
                    current_block['config'][param_name] = param_value
            
            # 确保使用最新的参数值（已经通过 update_block_parameters 回调更新的）
            # 这是一个后备方案，确保我们有最新的数据
            
            # 生成完整的块JSON内容（已经是最新的）
            complete_block = {
                'block_id': current_block.get('block_id', ''),
                'block_type': current_block.get('block_type', ''),
                'block_title': current_block.get('block_title', '未命名块'),
                'config': current_block.get('config', {})
            }
            
            # 将完整的块JSON替换到模板中对应的块
            updated_template['template_content'][index] = complete_block
            
            # 确定保存文件路径
            config_dir = os.path.join(os.path.dirname(__file__), '../..', 'config')
            os.makedirs(config_dir, exist_ok=True)
            
            if current_file:
                if current_file == 'default':
                    filepath = os.path.join(config_dir, 'default_template.json')
                    filename = 'default_template.json'
                else:
                    template_info = next((t for t in available_templates if t['value'] == current_file), None)
                    if template_info and template_info['filepath']:
                        filepath = template_info['filepath']
                        filename = current_file
                    else:
                        # 创建新文件
                        safe_name = "".join(c for c in updated_template.get('template_name', '未命名模板') if c.isalnum() or c in (' ', '-', '_')).rstrip()
                        filename = f"{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        filepath = os.path.join(config_dir, filename)
            else:
                # 没有当前文件，创建新文件
                safe_name = "".join(c for c in updated_template.get('template_name', '未命名模板') if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                filepath = os.path.join(config_dir, filename)
            
            # 保存到文件
            logger.info(f"保存块到文件: {filepath}")
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(updated_template, f, ensure_ascii=False, indent=2)
            logger.info(f"块保存成功: 块#{index + 1} -> {filename}")
            
            success_msg = dbc.Alert([
                html.I(className="fas fa-check-circle", style={'marginRight': '10px'}),
                f"块 #{index + 1} 已保存到 {filename}"
            ], color="success", dismissable=True, duration=3000, className="mt-3")
            
            # 返回更新的模板数据，这将自动触发JSON预览的更新
            return updated_template, success_msg
                
        except Exception as e:
            logger.error(f"保存块失败: {str(e)}")
            error_msg = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle", style={'marginRight': '10px'}),
                f"保存块失败: {str(e)}"
            ], color="danger", dismissable=True, duration=5000, className="mt-3")
            
            return no_update, error_msg

    @app.callback(
        [Output('full-html-modal', 'is_open'),
         Output('full-html-content', 'children')],
        [Input('full-html-preview-btn', 'n_clicks'),
         Input('close-full-html-modal', 'n_clicks')],
        [State('full-html-modal', 'is_open'),
         State('template-store', 'data')],
        prevent_initial_call=True
    )
    def handle_full_html_preview(preview_clicks, close_clicks, is_open, template_data):
        """处理全文HTML预览"""
        logger.info(f"处理全文HTML预览 - preview:{preview_clicks}, close:{close_clicks}")
        
        if not ctx.triggered:
            return is_open, no_update
        
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # 关闭模态框
        if triggered_id == 'close-full-html-modal':
            return False, no_update
        
        # 打开全文预览模态框
        if triggered_id == 'full-html-preview-btn' and preview_clicks:
            if not template_data or not template_data.get('template_content'):
                empty_content = html.Div([
                    html.I(className="fas fa-info-circle fa-2x text-muted mb-3"),
                    html.H5("暂无内容", className="text-muted"),
                    html.P("请先添加内容块", className="text-muted")
                ], style={'textAlign': 'center', 'padding': '60px 0'})
                
                return True, empty_content
            
            # 简单拼接所有块的HTML内容
            template_name = template_data.get('template_name', '未命名模板')
            template_description = template_data.get('template_description', '')
            
            # 拼接HTML内容
            concatenated_html = f'''
            <div style="font-family: 'Source Han Sans CN', 'Microsoft YaHei', sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; padding: 20px;">
                <header style="margin-bottom: 30px;">
                    <h1 style="color: #1a5490; text-align: center; margin: 0 0 10px 0; border-bottom: 2px solid #1a5490; padding-bottom: 15px;">{template_name}</h1>
            '''
            
            if template_description:
                concatenated_html += f'''
                    <p style="color: #666; font-style: italic; text-align: center; margin: 10px 0 30px 0;">{template_description}</p>
                '''
            
            concatenated_html += '</header>\n'
            
            # 遍历所有块，按顺序拼接HTML
            blocks = template_data.get('template_content', [])
            for i, block_data in enumerate(blocks):
                try:
                    block_html = render_block_to_html(block_data, mysql_db=mysql_db)
                    concatenated_html += f'''
                    <section style="margin: 25px 0; padding: 15px 0; border-bottom: 1px solid #eee;">
                        {block_html}
                    </section>
                    '''
                        
                except Exception as e:
                    # 如果某个块渲染失败，添加错误信息
                    block_title = block_data.get('block_title', f'块 #{i+1}')
                    concatenated_html += f'''
                    <section style="margin: 25px 0; padding: 15px 0; border-bottom: 1px solid #eee;">
                        <div style="background-color: #fdf2f2; border-left: 4px solid #e74c3c; padding: 12px 15px; border-radius: 4px;">
                            <strong>错误: {block_title}</strong><br>
                            渲染失败: {str(e)}
                        </div>
                    </section>
                    '''
            
            concatenated_html += '</div>'
            
            # 创建简单的HTML预览内容
            html_content = html.Iframe(
                srcDoc=concatenated_html,
                style={
                    'width': '100%',
                    'height': '100%',
                    'border': 'none'
                }
            )
            
            return True, html_content
        
        return is_open, no_update

    @app.callback(
        Output('export-html-link', 'href'),
        Input('export-html-link', 'n_clicks'),
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def generate_html_download(n_clicks, template_data):
        """生成HTML下载链接"""
        logger.info(f"生成HTML下载 - clicks:{n_clicks}")
        
        if not n_clicks or not template_data:
            return ""
        
        # 生成完整的HTML内容
        template_name = template_data.get('template_name', '未命名模板')
        template_description = template_data.get('template_description', '')
        
        full_html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{template_name}</title>
    <style>
        body {{
            font-family: "Source Han Sans CN", "Microsoft YaHei", sans-serif;
            line-height: 1.6;
            color: #333;
            margin: 0;
            padding: 20px;
            background-color: #fff;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
        }}
        header {{
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 2px solid #1a5490;
            padding-bottom: 15px;
        }}
        h1 {{
            color: #1a5490;
            margin: 0 0 10px 0;
        }}
        .description {{
            color: #666;
            font-style: italic;
            margin: 5px 0;
        }}
        .block-section {{
            margin: 25px 0;
            padding: 15px 0;
            border-bottom: 1px solid #eee;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 15px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }}
        th {{
            background-color: #1a5490;
            color: white;
        }}
        .chart-container {{
            text-align: center;
            margin: 20px 0;
        }}
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{template_name}</h1>
"""
        
        if template_description:
            full_html += f'            <p class="description">{template_description}</p>\n'
        
        full_html += '        </header>\n        <main>\n'
        
        # 遍历所有块
        blocks = template_data.get('template_content', [])
        for i, block_data in enumerate(blocks):
            try:
                block_html = render_block_to_html(block_data, mysql_db=mysql_db)
                full_html += f'            <section class="block-section">{block_html}</section>\n'
                    
            except Exception as e:
                block_title = block_data.get('block_title', f'块 #{i+1}')
                full_html += f'''            <section class="block-section">
                <div style="background-color: #fdf2f2; border-left: 4px solid #e74c3c; padding: 12px 15px; border-radius: 4px;">
                    <strong>错误: {block_title}</strong><br>
                    渲染失败: {str(e)}
                </div>
            </section>\n'''
        
        full_html += """        </main>
    </div>
</body>
</html>"""
        
        # 创建下载链接
        encoded_content = urllib.parse.quote(full_html)
        safe_filename = "".join(c for c in template_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"{safe_filename}.html" if safe_filename else "template.html"
        
        return f"data:text/html;charset=utf-8,{encoded_content}"

    @app.callback(
        [Output('message-display', 'children', allow_duplicate=True),
         Output('export-pdf-btn', 'disabled')],
        Input('export-pdf-btn', 'n_clicks'),
        State('template-store', 'data'),
        prevent_initial_call=True
    )
    def export_to_pdf(n_clicks, template_data):
        """导出为PDF"""
        logger.info(f"导出PDF开始 - clicks:{n_clicks}, template_name:{template_data.get('template_name', 'N/A') if template_data else 'N/A'}")
        
        if not n_clicks or not template_data:
            logger.info("导出PDF跳过 - 无点击或无模板数据")
            return no_update, False
        
        try:
            # 确定PDF缓存目录
            cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache', 'pdf')
            logger.info(f"创建PDF缓存目录: {cache_dir}")
            os.makedirs(cache_dir, exist_ok=True)
            
            # 使用工具函数生成PDF，指定缓存目录
            template_name = template_data.get('template_name', '未命名模板')
            logger.info(f"模板名称: {template_name}")
            
            output_path = generate_pdf_filename(template_name, cache_dir)
            logger.info(f"生成PDF文件路径: {output_path}")
            
            # 检查模板内容
            blocks = template_data.get('template_content', [])
            logger.info(f"模板包含块数量: {len(blocks)}")
            for i, block in enumerate(blocks):
                logger.info(f"块 #{i+1}: type={block.get('block_type')}, title={block.get('block_title')}")
            
            logger.info("开始调用 template_to_pdf 函数")
            
            # 创建包含mysql_db参数的HTML渲染函数包装器，并设置为PDF导出模式
            def render_block_with_db(block_data):
                return render_block_to_html(block_data, mysql_db=mysql_db, for_pdf=True)
            
            output_path = template_to_pdf(
                template_data=template_data,
                render_block_func=render_block_with_db,
                output_path=output_path
            )
            logger.info(f"PDF生成完成，实际输出路径: {output_path}")
            
            # 检查文件是否真的存在
            if not os.path.exists(output_path):
                logger.error(f"PDF文件未生成或路径错误: {output_path}")
                raise FileNotFoundError(f"PDF文件未生成: {output_path}")
            
            # 检查文件大小
            file_size = os.path.getsize(output_path)
            logger.info(f"PDF文件大小: {file_size} bytes")
            
            # 提取文件名用于显示
            filename = os.path.basename(output_path)
            logger.info(f"PDF文件名: {filename}")
            
            # 读取PDF文件内容并编码为base64
            logger.info("读取PDF文件内容并创建下载链接")
            with open(output_path, 'rb') as f:
                pdf_content = f.read()
            
            pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')
            logger.info(f"PDF内容编码完成，base64长度: {len(pdf_base64)} characters")
            
            # 创建成功消息，包含可点击的下载链接
            logger.info("创建成功消息和下载链接")
            success_msg = html.Div([
                dbc.Alert([
                    html.I(className="fas fa-check-circle", style={'marginRight': '10px'}),
                    f"PDF已生成: {filename}"
                ], color="success", dismissable=True, duration=10000, className="mt-3"),
                
                # 创建可见的下载按钮
                html.Div([
                    html.A(
                        [
                            html.I(className="fas fa-download", style={'marginRight': '8px'}),
                            f"点击下载 {filename}"
                        ],
                        href=f"data:application/pdf;base64,{pdf_base64}",
                        download=filename,
                        className="btn btn-primary btn-sm",
                        style={
                            'textDecoration': 'none',
                            'display': 'inline-block',
                            'margin': '10px 0'
                        }
                    )
                ], style={'textAlign': 'center'})
            ])
            
            logger.info("PDF导出成功完成")
            return success_msg, False

        except ImportError as e:
            logger.error(f"PDF导入错误: {str(e)}")
            logger.error(f"可能缺少PDF相关依赖包")
            error_msg = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle", style={'marginRight': '10px'}),
                f"导入错误: {str(e)}"
            ], color="warning", dismissable=True, duration=5000, className="mt-3")
            return error_msg, False

        except FileNotFoundError as e:
            logger.error(f"PDF文件未找到: {str(e)}")
            error_msg = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle", style={'marginRight': '10px'}),
                f"文件生成失败: {str(e)}"
            ], color="danger", dismissable=True, duration=5000, className="mt-3")
            return error_msg, False

        except Exception as e:
            logger.error(f"PDF导出失败: {str(e)}")
            logger.error(f"错误类型: {type(e).__name__}")
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            
            error_msg = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle", style={'marginRight': '10px'}),
                f"PDF导出失败: {str(e)}"
            ], color="danger", dismissable=True, duration=5000, className="mt-3")
            
            return error_msg, False

