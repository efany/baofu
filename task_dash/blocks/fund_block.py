"""
基金数据块实现
接收产品编号和时间窗口，调用fund_data_generator生成数据并展示
"""

from typing import List, Optional
from datetime import datetime, date
from .base_block import BaseBlock, BlockParameter
from task_dash.utils import generate_chart_image
import sys
import os
import pandas as pd
import re

# 添加项目路径以便导入数据生成器
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


class FundBlock(BaseBlock):
    """基金数据块"""
    
    def __init__(self, block_data: dict, mysql_db=None):
        """初始化基金数据块
        
        Args:
            block_data: 块数据
            mysql_db: MySQL数据库实例，如果为None则在使用时会报错
        """
        super().__init__(block_data)
        self.mysql_db = mysql_db
    
    @property
    def block_name(self) -> str:
        return "基金数据"
    
    @property
    def block_icon(self) -> str:
        return "💰"
    
    @property
    def block_description(self) -> str:
        return "显示指定基金的净值走势、收益统计和详细数据"
    
    def _get_fund_options(self) -> List[dict]:
        """获取基金选项列表"""
        if not self.mysql_db:
            # 如果没有数据库连接，返回默认选项
            return [
                {"label": "000001.OF - 华夏成长混合", "value": "000001.OF"},
                {"label": "110022.OF - 易方达消费行业股票", "value": "110022.OF"}
            ]
        
        try:
            from database.db_funds import DBFunds
            
            # 使用注入的数据库连接
            db_funds = DBFunds(self.mysql_db)
            funds_df = db_funds.get_all_funds()
            
            # 转换为选项格式
            options = []
            if not funds_df.empty:
                for _, row in funds_df.iterrows():
                    ts_code = row.get('ts_code', '')
                    name = row.get('name', '')
                    management = row.get('management', '')
                    
                    # 构建显示标签
                    label = f"{ts_code}"
                    if name:
                        label += f" - {name}"
                    if management:
                        label += f" ({management})"
                    
                    options.append({
                        "label": label,
                        "value": ts_code
                    })
            
            # 按基金代码排序
            options.sort(key=lambda x: x["value"])
            
            return options
            
        except Exception as e:
            # 如果获取失败，返回默认选项
            return [
                {"label": "000001.OF - 华夏成长混合", "value": "000001.OF"},
                {"label": "110022.OF - 易方达消费行业股票", "value": "110022.OF"}
            ]
    
    @property
    def parameters(self) -> List[BlockParameter]:
        return [
            BlockParameter(
                name="fund_code",
                label="基金代码",
                param_type="select",
                default_value="000001.OF",
                required=True,
                options=self._get_fund_options(),
                description="选择要分析的基金",
                placeholder="请选择基金"
            ),
            BlockParameter(
                name="start_date",
                label="开始日期",
                param_type="date",
                default_value="",
                description="统计开始日期，留空则使用全部数据",
                placeholder="选择开始日期或留空使用全部数据"
            ),
            BlockParameter(
                name="end_date",
                label="结束日期", 
                param_type="date",
                default_value="",
                description="统计结束日期，留空则使用最新数据",
                placeholder="选择结束日期或留空使用最新数据"
            ),
            BlockParameter(
                name="display_type",
                label="显示内容",
                param_type="select",
                default_value="summary",
                required=True,
                options=[
                    {"label": "📊 摘要信息", "value": "summary"},
                    {"label": "📈 净值走势图", "value": "chart"},
                    {"label": "📋 详细数据表", "value": "table"},
                    {"label": "📑 完整报告", "value": "full"}
                ],
                description="选择要显示的内容类型"
            ),
            BlockParameter(
                name="chart_type",
                label="图表样式",
                param_type="select",
                default_value="line",
                options=[
                    {"label": "📈 折线图", "value": "line"},
                    {"label": "📊 面积图", "value": "area"},
                    {"label": "🕯️ 蜡烛图", "value": "candlestick"}
                ],
                description="图表的显示样式（当显示内容包含图表时有效）"
            ),
            BlockParameter(
                name="show_dividends",
                label="显示分红信息",
                param_type="boolean",
                default_value=True,
                description="在图表中标记分红发放点"
            ),
            BlockParameter(
                name="normalize_data",
                label="归一化净值",
                param_type="boolean",
                default_value=False,
                description="将净值数据归一化到起始点为1.0，便于比较不同基金"
            ),
            BlockParameter(
                name="include_stats",
                label="包含统计表格",
                param_type="boolean",
                default_value=True,
                description="显示年度和季度收益统计表格"
            ),
            BlockParameter(
                name="period_filter",
                label="时间周期",
                param_type="select",
                default_value="all",
                options=[
                    {"label": "全部数据", "value": "all"},
                    {"label": "近1年", "value": "1y"},
                    {"label": "近3年", "value": "3y"},
                    {"label": "近5年", "value": "5y"},
                    {"label": "今年至今", "value": "ytd"},
                    {"label": "自定义", "value": "custom"}
                ],
                description="快速选择时间范围（选择自定义时使用上述开始/结束日期）"
            ),
            BlockParameter(
                name="show_benchmark",
                label="显示基准对比",
                param_type="boolean",
                default_value=False,
                description="显示与业绩基准的对比（如果数据可用）"
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
    
    def _format_table_value(self, value, column_name: str = "") -> str:
        """格式化表格数值，提供更好的显示效果"""
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return "—"  # 使用em-dash替代N/A
            
            # 百分比格式化
            if ('率' in column_name or 'pct' in column_name.lower() or 
                'rate' in column_name.lower() or '收益' in column_name or '回撤' in column_name):
                if abs(value) < 0.01:
                    return f"{value:+.3f}%"
                else:
                    return f"{value:+.2f}%"
            
            # 货币和净值格式化
            elif ('净值' in column_name or '价格' in column_name or 'nav' in column_name.lower()):
                return f"{value:.4f}"
            
            # 年份格式化
            elif '年' in column_name and isinstance(value, (int, float)) and 1900 <= value <= 2100:
                return str(int(value))
            
            # 整数格式化
            elif isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
                if abs(value) >= 1000:
                    return f"{int(value):,}"
                else:
                    return str(int(value))
            
            # 普通小数格式化
            else:
                if abs(value) < 0.001:
                    return f"{value:.6f}"
                elif abs(value) < 1:
                    return f"{value:.4f}"
                else:
                    return f"{value:.2f}"
        else:
            # 字符串值处理
            str_value = str(value).strip()
            
            # 处理已经包含百分号的字符串
            if '%' in str_value and ('(' in str_value or '->' in str_value):
                # 如: "+15.25% (1.0000 -> 1.1525)" 或 "投资最大回撤: -8.45%"
                return str_value
            elif '%' in str_value:
                # 简单百分比字符串
                return str_value
            elif re.match(r'^\d{4}$', str_value):
                # 年份格式
                return str_value
            elif re.match(r'^\d{4}[年]?\d{1,2}[月]?', str_value):
                # 日期格式
                return str_value
            else:
                # 普通字符串
                return str_value

    def _create_fancy_table(self, df: pd.DataFrame, title: str) -> str:
        """创建美化的表格markdown"""
        if df.empty:
            return f"#### 📊 {title}\n\n*暂无数据*\n\n"
        
        markdown = f"#### 📊 {title}\n\n"
        
        # 获取列名和格式化数据
        headers = df.columns.tolist()
        formatted_headers = [header.center(15) for header in headers]
        
        # 创建表格顶部边框
        markdown += "┌" + "┬".join("─" * 15 for _ in headers) + "┐\n"
        
        # 创建表头
        markdown += "│" + "│".join(formatted_headers) + "│\n"
        
        # 创建表头分隔线
        markdown += "├" + "┼".join("─" * 15 for _ in headers) + "┤\n"
        
        # 添加数据行（限制显示前10行）
        display_rows = df.head(10)
        for idx, (_, row) in enumerate(display_rows.iterrows()):
            formatted_row = []
            for col_idx, (col_name, value) in enumerate(zip(headers, row)):
                formatted_value = self._format_table_value(value, col_name)
                # 根据列类型调整对齐方式
                if isinstance(value, (int, float)) and not pd.isna(value):
                    formatted_row.append(formatted_value.center(15))
                else:
                    formatted_row.append(formatted_value.center(15))
            
            markdown += "│" + "│".join(formatted_row) + "│\n"
            
            # 在数据行之间添加细分隔线（每隔一行）
            if idx < len(display_rows) - 1 and (idx + 1) % 2 == 0:
                markdown += "├" + "┼".join("╌" * 15 for _ in headers) + "┤\n"
        
        # 如果有更多数据，显示省略信息
        if len(df) > 10:
            markdown += "├" + "┼".join("─" * 15 for _ in headers) + "┤\n"
            more_info = f"... 还有 {len(df) - 10} 行数据 ..."
            colspan_width = len(headers) * 15 + (len(headers) - 1)
            markdown += "│" + more_info.center(colspan_width) + "│\n"
        
        # 创建表格底部边框
        markdown += "└" + "┴".join("─" * 15 for _ in headers) + "┘\n\n"
        
        return markdown

    def _create_simple_table(self, df: pd.DataFrame, title: str) -> str:
        """创建简化版美化表格（markdown标准格式）"""
        if df.empty:
            return f"#### 📊 {title}\n\n*暂无数据*\n\n"
        
        markdown = f"#### 📊 {title}\n\n"
        
        # 获取列名
        headers = df.columns.tolist()
        
        # 创建表格头 - 使用图标和粗体
        header_icons = {
            '指标': '📋', '数值': '💰', '年份': '📅', '季度': '📈',
            '收益率': '📊', '年化收益率': '📈', '最大回撤': '📉',
            '波动率': '📊', '年化波动率': '🌊'
        }
        
        formatted_headers = []
        for header in headers:
            icon = header_icons.get(header, '📋')
            formatted_headers.append(f"{icon} **{header}**")
        
        header_line = "| " + " | ".join(formatted_headers) + " |\n"
        
        # 创建分隔线 - 所有列都设置为居中对齐
        separators = [":---:"] * len(headers)  # 所有列都居中对齐
        
        separator_line = "| " + " | ".join(separators) + " |\n"
        
        markdown += header_line + separator_line
        
        # 添加数据行（限制显示前10行）
        display_rows = df.head(10)
        for row_idx, (_, row) in enumerate(display_rows.iterrows()):
            row_data = []
            for col_name, value in zip(headers, row):
                formatted_value = self._format_table_value(value, col_name)
                
                # 为重要数值添加视觉增强
                if isinstance(value, (int, float)) and not pd.isna(value):
                    # 百分比数值的颜色标识
                    if '率' in col_name or '收益' in col_name or '回撤' in col_name:
                        if value > 0:
                            formatted_value = f"🟢 **{formatted_value}**"
                        elif value < 0:
                            formatted_value = f"🔴 **{formatted_value}**"
                        else:
                            formatted_value = f"⚫ {formatted_value}"
                    # 净值等重要数值加粗
                    elif '净值' in col_name or '价格' in col_name:
                        formatted_value = f"**{formatted_value}**"
                
                # 处理字符串中已包含百分比和符号的情况
                elif isinstance(value, str) and '%' in str(value):
                    if '+' in str(value):
                        formatted_value = f"🟢 **{formatted_value}**"
                    elif '-' in str(value) and '回撤' not in str(value):  # 排除"投资最大回撤"这种标签
                        formatted_value = f"🔴 **{formatted_value}**"
                
                row_data.append(formatted_value)
            
            # 添加行分隔（每两行添加一个微分隔）
            markdown += "| " + " | ".join(row_data) + " |\n"
            
            # 在重要的行后添加空行效果（通过添加细分隔线）
            if row_idx == 0 and '基础指标' in title:  # 第一行后加分隔线
                pass  # markdown中不支持行内分隔，跳过
        
        # 如果有更多数据，添加汇总行
        if len(df) > 10:
            summary_data = ["⋯"] * (len(headers) - 1) + [f"📊 *共 {len(df)} 行数据*"]
            markdown += "| " + " | ".join(summary_data) + " |\n"
        
        markdown += "\n"
        return markdown

    def _calculate_period_dates(self) -> tuple:
        """根据时间周期计算开始和结束日期"""
        from datetime import timedelta
        
        period_filter = self.get_parameter_value("period_filter", "all")
        
        if period_filter == "custom":
            # 使用自定义日期
            start_date_str = self.get_parameter_value("start_date", "")
            end_date_str = self.get_parameter_value("end_date", "")
            return self._parse_date(start_date_str), self._parse_date(end_date_str)
        
        today = date.today()
        
        if period_filter == "1y":
            start_date = today - timedelta(days=365)
            return start_date, today
        elif period_filter == "3y":
            start_date = today - timedelta(days=365 * 3)
            return start_date, today
        elif period_filter == "5y":
            start_date = today - timedelta(days=365 * 5)
            return start_date, today
        elif period_filter == "ytd":
            start_date = date(today.year, 1, 1)
            return start_date, today
        else:  # "all"
            return None, None

    def _generate_fund_data(self) -> dict:
        """生成基金数据"""
        fund_code = self.get_parameter_value("fund_code", "").strip()
        
        if not fund_code:
            return {"error": "基金代码不能为空"}
        
        if not self.mysql_db:
            return {"error": "数据库连接未初始化"}
        
        # 计算日期范围
        start_date, end_date = self._calculate_period_dates()
        
        try:
            # 导入所需模块（延迟导入以避免循环依赖）
            from task_dash.datas.fund_data_generator import FundDataGenerator
            
            # 使用注入的数据库连接
            generator = FundDataGenerator(
                fund_code=fund_code,
                mysql_db=self.mysql_db,
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
    
    def render_to_markdown(self, for_pdf: bool = False) -> str:
        """渲染为Markdown
        
        Args:
            for_pdf: 是否为PDF导出，True时使用绝对路径生成图片
        """
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
                # 尝试生成图表图片
                try:
                    # 根据是否为PDF导出选择返回路径类型
                    image_path = generate_chart_image(chart_data, "fund", fund_code, return_absolute_path=for_pdf)
                    
                    if image_path:
                        # 插入图片到markdown
                        markdown += f"![净值走势图]({image_path})\n\n"
                    else:
                        # 如果图片生成失败，显示文本描述
                        markdown += "```\n"
                        markdown += "📈 [净值走势图]\n"
                        markdown += f"基金代码: {fund_code}\n"
                        markdown += "```\n\n"
                except Exception:
                    # 如果有任何错误，显示文本描述
                    markdown += "```\n"
                    markdown += "📈 [净值走势图]\n"
                    markdown += f"基金代码: {fund_code}\n"
                    markdown += "```\n\n"
                
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
                            
                            markdown += f"**时间范围**: {start_date} 至 {end_date}\n\n"
                            markdown += f"**起始净值**: {start_value:.4f}\n\n"
                            markdown += f"**最新净值**: {end_value:.4f}\n\n"
                            
                            if start_value and end_value:
                                return_rate = (end_value - start_value) / start_value * 100
                                markdown += f"**区间收益**: {return_rate:+.2f}%\n\n"
                
                # 显示图表配置信息
                chart_type = self.get_parameter_value("chart_type", "line")
                show_dividends = self.get_parameter_value("show_dividends", True)
                normalize_data = self.get_parameter_value("normalize_data", False)

                config_info = []
                config_info.append(f"图表类型: {chart_type}")
                if show_dividends:
                    config_info.append("包含分红标记")
                if normalize_data:
                    config_info.append("数据已归一化")
                
                if config_info:
                    markdown += f"**图表配置**: {', '.join(config_info)}\n\n"
            else:
                markdown += "*暂无图表数据*\n\n"
        
        if display_type in ["table", "full"]:
            markdown += "### 📋 统计数据\n\n"
            extra_data = data_result["extra_data"]
            
            if extra_data:
                # 添加数据概览框
                markdown += "> 📊 **数据概览**: 以下表格显示了基金的详细统计信息，包括基础指标、年度统计和季度统计等。\n\n"
                
                for i, table_data in enumerate(extra_data):
                    table_title = table_data.get('name', '数据表')
                    
                    # 检查是否有pd_data字段（新格式）
                    if 'pd_data' in table_data and not table_data['pd_data'].empty:
                        df = table_data['pd_data']
                        
                        # 使用美化的表格渲染
                        table_markdown = self._create_simple_table(df, table_title)
                        markdown += table_markdown
                        
                        # 为重要表格添加解释说明
                        if '基础指标' in table_title:
                            markdown += "> 💡 **说明**: 基础指标显示了投资的核心收益和风险指标，帮助评估基金表现。\n\n"
                        elif '年度统计' in table_title:
                            markdown += "> 📅 **说明**: 年度统计按年份展示收益表现，便于进行历史业绩比较。\n\n"
                        elif '季度统计' in table_title:
                            markdown += "> 📈 **说明**: 季度统计提供更细粒度的业绩分析，有助于识别季节性表现模式。\n\n"
                    
                    # 兼容旧格式（columns和data字段）
                    elif 'columns' in table_data and 'data' in table_data:
                        table_columns = table_data.get('columns', [])
                        table_rows = table_data.get('data', [])
                        
                        if table_columns and table_rows:
                            markdown += f"#### 📊 {table_title}\n\n"
                            
                            # 创建表格头 - 使用粗体
                            headers = [col.get('name', '') for col in table_columns]
                            header_line = "| " + " | ".join(f"**{header}**" for header in headers) + " |\n"
                            
                            # 创建分隔线 - 所有列居中对齐
                            separator_line = "| " + " | ".join([":---:"] * len(headers)) + " |\n"
                            markdown += header_line + separator_line
                            
                            # 添加数据行（限制显示前10行）
                            display_rows = table_rows[:10]
                            for row in display_rows:
                                row_data = []
                                for col in table_columns:
                                    col_id = col.get('id', '')
                                    value = row.get(col_id, '')
                                    formatted_value = self._format_table_value(value, col_id)
                                    row_data.append(formatted_value)
                                
                                markdown += "| " + " | ".join(row_data) + " |\n"
                            
                            if len(table_rows) > 10:
                                summary_cols = ["..."] * (len(headers) - 1) + [f"*共 {len(table_rows)} 行*"]
                                markdown += "| " + " | ".join(summary_cols) + " |\n"
                            
                            markdown += "\n"
                        else:
                            markdown += f"#### 📊 {table_title}\n\n*暂无表格数据*\n\n"
                    else:
                        markdown += f"#### 📊 {table_title}\n\n*暂无表格数据*\n\n"
                
                # 添加数据汇总信息
                markdown += "---\n\n"
                markdown += f"📋 **统计汇总**: 共展示了 {len(extra_data)} 个数据表格\n\n"
            else:
                markdown += "**ℹ️ 提示**: 当前未选择包含统计表格，或者没有可用的统计数据。\n\n"
        
        # 添加数据源信息
        markdown += "---\n\n"
        markdown += "*数据来源: baofu基金数据库*\n"
        
        return markdown