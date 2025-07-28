"""
基金数据块实现
接收产品编号和时间窗口，调用fund_data_generator生成数据并展示
"""

from typing import List, Optional
from datetime import datetime, date
from .base_block import BaseBlock, BlockParameter
import sys
import os

# 添加项目路径以便导入数据生成器
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


class FundBlock(BaseBlock):
    """基金数据块"""
    
    @property
    def block_name(self) -> str:
        return "基金数据"
    
    @property
    def block_icon(self) -> str:
        return "💰"
    
    @property
    def block_description(self) -> str:
        return "显示指定基金的净值走势、收益统计和详细数据"
    
    @property
    def parameters(self) -> List[BlockParameter]:
        return [
            BlockParameter(
                name="fund_code",
                label="基金代码",
                param_type="text",
                default_value="",
                required=True,
                description="基金的TS代码，如：000001.OF",
                validation={"min_length": 6, "max_length": 20}
            ),
            BlockParameter(
                name="start_date",
                label="开始日期",
                param_type="text",
                default_value="",
                description="统计开始日期，格式：YYYY-MM-DD，留空则使用全部数据"
            ),
            BlockParameter(
                name="end_date",
                label="结束日期", 
                param_type="text",
                default_value="",
                description="统计结束日期，格式：YYYY-MM-DD，留空则使用最新数据"
            ),
            BlockParameter(
                name="display_type",
                label="显示类型",
                param_type="select",
                default_value="summary",
                required=True,
                options=[
                    {"label": "摘要信息", "value": "summary"},
                    {"label": "净值走势图", "value": "chart"},
                    {"label": "详细数据表", "value": "table"},
                    {"label": "完整报告", "value": "full"}
                ],
                description="选择要显示的内容类型"
            ),
            BlockParameter(
                name="chart_type",
                label="图表类型",
                param_type="select",
                default_value="line",
                options=[
                    {"label": "折线图", "value": "line"},
                    {"label": "面积图", "value": "area"},
                    {"label": "蜡烛图", "value": "candlestick"}
                ],
                description="图表的显示样式（仅当显示类型包含图表时有效）"
            ),
            BlockParameter(
                name="show_dividends",
                label="显示分红",
                param_type="boolean",
                default_value=True,
                description="是否在图表中标记分红点"
            ),
            BlockParameter(
                name="normalize_data",
                label="归一化数据",
                param_type="boolean",
                default_value=False,
                description="是否将净值数据归一化到起始点为1.0"
            ),
            BlockParameter(
                name="include_stats",
                label="包含统计数据",
                param_type="boolean",
                default_value=True,
                description="是否包含年度和季度统计表格"
            )
        ]
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        if not date_str or not date_str.strip():
            return None
        
        try:
            return datetime.strptime(date_str.strip(), '%Y-%m-%d').date()
        except ValueError:
            return None
    
    def _generate_fund_data(self) -> dict:
        """生成基金数据"""
        fund_code = self.get_parameter_value("fund_code", "").strip()
        start_date_str = self.get_parameter_value("start_date", "")
        end_date_str = self.get_parameter_value("end_date", "")
        
        if not fund_code:
            return {"error": "基金代码不能为空"}
        
        # 解析日期
        start_date = self._parse_date(start_date_str)
        end_date = self._parse_date(end_date_str)
        
        try:
            # 导入所需模块（延迟导入以避免循环依赖）
            from task_dash.datas.fund_data_generator import FundDataGenerator
            from database.mysql_database import MySQLDatabase
            
            # 创建数据库连接（使用默认配置）
            mysql_db = MySQLDatabase(
                host='127.0.0.1',
                user='baofu', 
                password='TYeKmJPfw2b7kxGK',
                database='baofu'
            )
            
            # 创建基金数据生成器
            generator = FundDataGenerator(
                fund_code=fund_code,
                mysql_db=mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
            # 加载数据
            success = generator.load()
            if not success:
                return {"error": f"无法加载基金 {fund_code} 的数据"}
            
            # 获取各种数据
            summary_data = generator.get_summary_data()
            chart_data = generator.get_chart_data(
                normalize=self.get_parameter_value("normalize_data", False)
            )
            extra_data = generator.get_extra_datas() if self.get_parameter_value("include_stats", True) else []
            
            return {
                "success": True,
                "fund_code": fund_code,
                "summary": summary_data,
                "chart": chart_data,
                "extra_data": extra_data,
                "start_date": start_date,
                "end_date": end_date
            }
            
        except ImportError as e:
            return {"error": f"导入模块失败: {str(e)}"}
        except Exception as e:
            return {"error": f"数据生成失败: {str(e)}"}
    
    def render_to_markdown(self) -> str:
        """渲染为Markdown"""
        display_type = self.get_parameter_value("display_type", "summary")
        
        # 生成基金数据
        data_result = self._generate_fund_data()
        
        markdown = f"## {self.block_title}\n\n"
        
        # 如果有错误，显示错误信息
        if "error" in data_result:
            markdown += f"**❌ 错误**: {data_result['error']}\n\n"
            return markdown
        
        # 基金基础信息
        fund_code = data_result["fund_code"]
        summary = data_result["summary"]
        
        if display_type in ["summary", "full"]:
            markdown += "### 📊 基金概览\n\n"
            
            if summary:
                for label, value in summary:
                    markdown += f"- **{label}**: {value}\n"
                markdown += "\n"
            else:
                markdown += "*暂无摘要数据*\n\n"
        
        if display_type in ["chart", "full"]:
            markdown += "### 📈 净值走势\n\n"
            chart_data = data_result["chart"]
            
            if chart_data:
                markdown += "```\n"
                markdown += "📈 [净值走势图]\n"
                markdown += f"基金代码: {fund_code}\n"
                
                # 显示数据概览
                if chart_data:
                    first_trace = chart_data[0] if chart_data else {}
                    if 'x' in first_trace and 'y' in first_trace:
                        dates = first_trace['x']
                        values = first_trace['y']
                        if dates and values:
                            start_date = dates[0] if isinstance(dates[0], str) else dates[0].strftime('%Y-%m-%d')
                            end_date = dates[-1] if isinstance(dates[-1], str) else dates[-1].strftime('%Y-%m-%d')
                            start_value = values[0]
                            end_value = values[-1]
                            
                            markdown += f"时间范围: {start_date} 至 {end_date}\n"
                            markdown += f"起始净值: {start_value:.4f}\n"
                            markdown += f"最新净值: {end_value:.4f}\n"
                            
                            if start_value and end_value:
                                return_rate = (end_value - start_value) / start_value * 100
                                markdown += f"区间收益: {return_rate:+.2f}%\n"
                
                # 显示图表配置信息
                chart_type = self.get_parameter_value("chart_type", "line")
                show_dividends = self.get_parameter_value("show_dividends", True)
                normalize_data = self.get_parameter_value("normalize_data", False)
                
                markdown += f"图表类型: {chart_type}\n"
                if show_dividends:
                    markdown += "包含分红标记\n"
                if normalize_data:
                    markdown += "数据已归一化\n"
                
                markdown += "```\n\n"
            else:
                markdown += "*暂无图表数据*\n\n"
        
        if display_type in ["table", "full"]:
            markdown += "### 📋 统计数据\n\n"
            extra_data = data_result["extra_data"]
            
            if extra_data:
                for table_data in extra_data:
                    table_title = table_data.get('title', '数据表')
                    table_columns = table_data.get('columns', [])
                    table_rows = table_data.get('data', [])
                    
                    markdown += f"#### {table_title}\n\n"
                    
                    if table_columns and table_rows:
                        # 创建表格头
                        headers = [col.get('name', '') for col in table_columns]
                        markdown += "| " + " | ".join(headers) + " |\n"
                        markdown += "| " + " | ".join(['---'] * len(headers)) + " |\n"
                        
                        # 添加数据行（限制显示前10行）
                        display_rows = table_rows[:10]
                        for row in display_rows:
                            row_data = []
                            for col in table_columns:
                                col_id = col.get('id', '')
                                value = row.get(col_id, '')
                                # 格式化数值
                                if isinstance(value, (int, float)):
                                    if col_id.endswith('_pct') or col_id.endswith('_rate'):
                                        row_data.append(f"{value:.2f}%")
                                    elif isinstance(value, float):
                                        row_data.append(f"{value:.4f}")
                                    else:
                                        row_data.append(str(value))
                                else:
                                    row_data.append(str(value))
                            
                            markdown += "| " + " | ".join(row_data) + " |\n"
                        
                        if len(table_rows) > 10:
                            markdown += f"| ... | ... | ... | (共{len(table_rows)}行数据) |\n"
                        
                        markdown += "\n"
                    else:
                        markdown += "*暂无表格数据*\n\n"
            else:
                markdown += "*暂无统计数据*\n\n"
        
        # 添加数据源信息
        markdown += "---\n\n"
        markdown += "*数据来源: baofu基金数据库*\n"
        
        return markdown