"""
指数概览块实现
以三列的方式展示三个指数数据的概览
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from .base_block import BaseBlock, BlockParameter
import sys
import os
import pandas as pd

# 添加项目路径以便导入数据生成器
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


class IndexOverviewBlock(BaseBlock):
    """指数概览块"""
    
    def __init__(self, block_data: dict, mysql_db=None):
        """初始化指数概览块
        
        Args:
            block_data: 块数据
            mysql_db: MySQL数据库实例，如果为None则在使用时会报错
        """
        super().__init__(block_data)
        self.mysql_db = mysql_db
    
    @property
    def block_name(self) -> str:
        return "指数概览"
    
    @property
    def block_icon(self) -> str:
        return "📊"
    
    @property
    def block_description(self) -> str:
        return "以三列布局展示三个指数的关键数据概览，包括当前价位、涨跌幅等信息"
    
    def _get_index_options(self) -> List[dict]:
        """获取指数选项列表"""
        return [
            {"label": "🏛️ 上证综指 (sh000001)", "value": "sh000001"},
            {"label": "📈 沪深300 (sh000300)", "value": "sh000300"},
            {"label": "🚀 中证500 (sh000905)", "value": "sh000905"},
            {"label": "🔝 上证50 (sh000016)", "value": "sh000016"},
            {"label": "🏢 深证成指 (sz399001)", "value": "sz399001"},
            {"label": "💡 创业板指 (sz399006)", "value": "sz399006"},
            {"label": "📊 中证800 (sh000906)", "value": "sh000906"},
            {"label": "🧩 中小板指 (sz399005)", "value": "sz399005"},
            {"label": "🎯 上证A股指数 (sh000002)", "value": "sh000002"} 
        ]
    
    @property
    def parameters(self) -> List[BlockParameter]:
        index_options = self._get_index_options()
        
        return [
            BlockParameter(
                name="index1",
                label="第一个指数",
                param_type="select",
                default_value="sh000001",
                required=True,
                options=index_options,
                description="选择第一列要显示的指数",
                placeholder="请选择指数"
            ),
            BlockParameter(
                name="index2",
                label="第二个指数",
                param_type="select",
                default_value="sh000300",
                required=True,
                options=index_options,
                description="选择第二列要显示的指数",
                placeholder="请选择指数"
            ),
            BlockParameter(
                name="index3",
                label="第三个指数",
                param_type="select",
                default_value="sh000905",
                required=True,
                options=index_options,
                description="选择第三列要显示的指数",
                placeholder="请选择指数"
            ),
            BlockParameter(
                name="time_period",
                label="统计周期",
                param_type="select",
                default_value="1m",
                required=True,
                options=[
                    {"label": "近1天", "value": "1d"},
                    {"label": "近1周", "value": "1w"},
                    {"label": "近1个月", "value": "1m"},
                    {"label": "近3个月", "value": "3m"},
                    {"label": "近6个月", "value": "6m"},
                    {"label": "近1年", "value": "1y"},
                    {"label": "今年至今", "value": "ytd"},
                    {"label": "全部数据", "value": "all"}
                ],
                description="选择数据统计的时间范围"
            ),
        ]
    
    def _get_last_valid_date(self, index_symbol: str) -> date:
        """获取指定指数的最后有效日期"""
        try:
            from task_dash.datas.index_data_generator import IndexDataGenerator
            
            generator = IndexDataGenerator(
                index_symbol=index_symbol,
                mysql_db=self.mysql_db,
                start_date=None,
                end_date=None
            )
            
            success = generator.load()
            if success and generator.data is not None and not generator.data.empty:
                last_date = generator.data.iloc[-1]['date']
                if isinstance(last_date, str):
                    import pandas as pd
                    return pd.to_datetime(last_date).date()
                elif hasattr(last_date, 'date'):
                    return last_date.date()
                else:
                    return last_date
        except Exception:
            pass
        
        return date.today()
    
    def _calculate_period_dates(self, index_symbol: str = None) -> tuple:
        """根据时间周期计算开始和结束日期"""
        time_period = self.get_parameter_value("time_period", "1m")
        
        # 获取最后有效日期作为end_date
        if index_symbol:
            end_date = self._get_last_valid_date(index_symbol)
        else:
            end_date = date.today()
        
        if time_period == "1d":
            start_date = end_date - timedelta(days=1)
        elif time_period == "1w":
            start_date = end_date - timedelta(days=7)
        elif time_period == "1m":
            start_date = end_date - timedelta(days=30)
        elif time_period == "3m":
            start_date = end_date - timedelta(days=90)
        elif time_period == "6m":
            start_date = end_date - timedelta(days=180)
        elif time_period == "1y":
            start_date = end_date - timedelta(days=365)
        elif time_period == "ytd":
            start_date = date(end_date.year, 1, 1)
        else:  # "all"
            start_date = None
        
        return start_date, end_date
    
    def _get_trend_type(self, return_rate_str: str) -> str:
        """根据收益率字符串判断涨跌趋势"""
        if not return_rate_str or return_rate_str == "无数据":
            return "neutral"
        
        # 提取数字部分，格式如 "+5.25%" 或 "-2.30%"
        try:
            if "%" in return_rate_str:
                rate_num = float(return_rate_str.replace("%", "").replace("+", ""))
                if rate_num > 0:
                    return "up"
                elif rate_num < 0:
                    return "down"
                else:
                    return "neutral"
        except:
            pass
        
        return "neutral"
    
    def _get_index_data(self, index_symbol: str) -> Dict[str, Any]:
        """获取单个指数的数据"""
        if not self.mysql_db:
            return {"error": "数据库连接未初始化"}
        
        start_date, end_date = self._calculate_period_dates(index_symbol)
        
        try:
            from task_dash.datas.index_data_generator import IndexDataGenerator
            
            generator = IndexDataGenerator(
                index_symbol=index_symbol,
                mysql_db=self.mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
            success = generator.load()
            if not success:
                return {"error": f"无法加载指数 {index_symbol} 的数据"}
            
            # 获取摘要数据
            summary_data = generator.get_summary_data()
            
            # 将摘要数据转换为字典格式
            summary_dict = {item[0]: item[1] for item in summary_data}
            
            # 判断涨跌趋势
            return_rate = summary_dict.get("区间收益率", "无数据")
            trend_type = self._get_trend_type(return_rate)
            
            return {
                "success": True,
                "symbol": index_symbol,
                "summary": summary_dict,
                "trend": trend_type
            }
            
        except Exception as e:
            return {"error": f"数据获取失败: {str(e)}"}
    
    
    def render_to_html(self, for_pdf: bool = False) -> str:
        """渲染为HTML"""
        # 在渲染阶段进行严格验证
        if not self.mysql_db:
            return '<div style="color: #666; text-align: center; padding: 20px;">数据库连接未初始化，无法渲染指数概览</div>'
        
        html = ""
        
        # 获取三个指数的配置
        index1 = self.get_parameter_value("index1", "sh000001")
        index2 = self.get_parameter_value("index2", "sh000300") 
        index3 = self.get_parameter_value("index3", "sh000905")
        time_period = self.get_parameter_value("time_period", "1m")
        
        # 时间周期标签映射
        time_labels = {
            "1d": "近1天", "1w": "近1周", "1m": "近1个月", 
            "3m": "近3个月", "6m": "近6个月", "1y": "近1年", 
            "ytd": "今年至今", "all": "全部数据"
        }
        period_label = time_labels.get(time_period, time_period)
        
        # 指数名称映射
        index_names = {
            'sh000001': '上证综指',
            'sh000300': '沪深300',
            'sh000905': '中证500',
            'sh000016': '上证50',
            'sz399001': '深证成指',
            'sz399006': '创业板指',
            'sh000906': '中证800',
            'sz399005': '中小板指',
            'sh000002': '上证A股指数'
        }
        
        # 三列布局
        html += f'''<div style="margin: 20px auto; max-width: 900px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
    <div style="background: #f8f9fa; padding: 16px; border-radius: 6px; margin-bottom: 16px; text-align: center;">
        <h3 style="margin: 0; color: #495057; font-size: 18px; font-weight: 500;">指数概览</h3>
        <p style="margin: 8px 0 0 0; color: #6c757d; font-size: 14px;">统计周期: {period_label}</p>
    </div>
    <table style="width: 100%; border-collapse: collapse; border: 1px solid #dee2e6; border-radius: 6px; overflow: hidden;">
        <tr>'''
        
        indices = [index1, index2, index3]
        
        for index_code in indices:
            index_name = index_names.get(index_code, index_code)
            
            # 获取指数数据
            try:
                data = self._get_index_data(index_code)
                
                if "error" in data:
                    html += f'''            <td style="width: 33.33%; padding: 16px; border: 1px solid #dee2e6; background: #f8f9fa; vertical-align: top;">
                <div style="text-align: center;">
                    <div style="font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 14px;">{index_name}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{index_code}</div>
                    <div style="color: #dc3545; font-weight: 500; font-size: 12px;">数据获取失败</div>
                </div>
            </td>'''
                else:
                    summary = data.get("summary", {})
                    trend = data.get("trend", "neutral")
                    return_rate = summary.get("区间收益率", "无数据")
                    index_change = summary.get("指数变化", "—")
                    
                    # 简洁的颜色方案
                    if trend == "up":
                        return_color = "#28a745"
                        bg_color = "#f8fff8"
                    elif trend == "down":
                        return_color = "#dc3545"
                        bg_color = "#fff8f8"
                    else:
                        return_color = "#6c757d"
                        bg_color = "#ffffff"
                    
                    html += f'''            <td style="width: 33.33%; padding: 16px; border: 1px solid #dee2e6; background: {bg_color}; vertical-align: top;">
                <div style="text-align: center;">
                    <div style="font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 14px;">{index_name}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{index_code}</div>
                    <div style="color: {return_color}; font-weight: 700; font-size: 16px; margin-bottom: 4px;">{return_rate}</div>
                    <div style="color: #6c757d; font-size: 11px;">{index_change}</div>
                </div>
            </td>'''
                    
            except Exception as e:
                html += f'''            <td style="width: 33.33%; padding: 16px; border: 1px solid #dee2e6; background: #f8f9fa; vertical-align: top;">
                <div style="text-align: center;">
                    <div style="font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 14px;">{index_name}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{index_code}</div>
                    <div style="color: #dc3545; font-weight: 500; font-size: 12px;">加载出错</div>
                </div>
            </td>'''
        
        html += f'''        </tr>
    </table>
    <div style="text-align: center; margin-top: 16px; color: #6c757d; font-size: 12px;">
        展示3个主要指数数据
    </div>
</div>'''
        
        return html