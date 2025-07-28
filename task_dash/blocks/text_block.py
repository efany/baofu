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
    
    def render_to_markdown(self) -> str:
        """渲染为Markdown"""
        content = self.get_parameter_value("content", "空白文本")
        style = self.get_parameter_value("style", "paragraph")
        alignment = self.get_parameter_value("alignment", "left")
        
        # 根据样式格式化内容
        if style == "header":
            formatted_content = f"# {content}"
        elif style == "subheader":
            formatted_content = f"## {content}"
        elif style == "quote":
            formatted_content = f"> {content}"
        elif style == "code":
            formatted_content = f"```\n{content}\n```"
        else:  # paragraph
            formatted_content = content

        # 添加对齐方式（使用HTML标签，因为Markdown本身不支持对齐）
        if alignment == "center":
            formatted_content = f"<div align='center'>\n\n{formatted_content}\n\n</div>"
        elif alignment == "right":
            formatted_content = f"<div align='right'>\n\n{formatted_content}\n\n</div>"
        
        return formatted_content + "\n\n"