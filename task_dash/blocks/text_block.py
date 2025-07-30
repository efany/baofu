"""
文本块实现
支持纯文本、标题等文本内容
"""

from typing import List
from .base_block import BaseBlock, BlockParameter


class TextBlock(BaseBlock):
    """文本块"""
    
    @property
    def block_name(self) -> str:
        return "文本块"
    
    @property
    def block_icon(self) -> str:
        return "📝"
    
    @property
    def block_description(self) -> str:
        return "纯文本内容，支持Markdown格式"
    
    @property
    def parameters(self) -> List[BlockParameter]:
        return [
            BlockParameter(
                name="content",
                label="文本内容",
                param_type="textarea",
                default_value="请输入文本内容",
                required=True,
                description="支持Markdown格式的文本内容",
                validation={"min_length": 1, "max_length": 5000}
            ),
            BlockParameter(
                name="style",
                label="文本样式",
                param_type="select",
                default_value="paragraph",
                options=[
                    {"label": "段落", "value": "paragraph"},
                    {"label": "标题", "value": "header"},
                    {"label": "副标题", "value": "subheader"},
                    {"label": "引用", "value": "quote"},
                    {"label": "代码", "value": "code"}
                ],
                description="选择文本的显示样式"
            ),
            BlockParameter(
                name="alignment",
                label="对齐方式",
                param_type="select",
                default_value="left",
                options=[
                    {"label": "左对齐", "value": "left"},
                    {"label": "居中", "value": "center"},
                    {"label": "右对齐", "value": "right"}
                ],
                description="文本对齐方式"
            )
        ]
    
    def render_to_html(self, for_pdf: bool = False) -> str:
        """渲染为HTML"""
        content = self.get_parameter_value("content", "空白文本")
        style = self.get_parameter_value("style", "paragraph")
        alignment = self.get_parameter_value("alignment", "left")
        
        import re
        import html
        
        # HTML转义内容以防止XSS
        escaped_content = html.escape(content)
        
        # 简单的Markdown到HTML转换
        html_content = escaped_content
        
        # 转换粗体
        html_content = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html_content)
        html_content = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html_content)
        
        # 转换代码
        html_content = re.sub(r'`(.*?)`', r'<code>\1</code>', html_content)
        
        # 转换换行
        html_content = html_content.replace('\n', '<br>')
        
        # 根据样式格式化内容
        if style == "header":
            tag = "h1"
        elif style == "subheader":
            tag = "h2"
        elif style == "quote":
            tag = "blockquote"
        elif style == "code":
            tag = "pre"
            # 对于代码块，使用原始转义的内容，不进行Markdown转换
            html_content = f"<code>{escaped_content}</code>"
        else:  # paragraph
            tag = "p"
        
        # 创建样式字符串
        styles = [f"text-align: {alignment}"]
        
        if style == "quote":
            styles.extend([
                "border-left: 4px solid #ccc",
                "padding-left: 1em",
                "margin: 1em 0",
                "color: #666",
                "font-style: italic"
            ])
        elif style == "code":
            styles.extend([
                "background-color: #f5f5f5",
                "padding: 1em",
                "border-radius: 4px",
                "overflow-x: auto",
                "font-family: 'Courier New', monospace"
            ])
        
        style_attr = "; ".join(styles)
        
        # 创建HTML元素
        formatted_html = f'<{tag} style="{style_attr}">{html_content}</{tag}>'
        
        return formatted_html