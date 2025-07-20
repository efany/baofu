# 指数数据爬虫使用说明

## 概述

新增了通过AKShare获取指数历史数据的爬虫功能，支持股票指数和基金指数的数据获取。

## 功能特性

### 1. 支持的指数类型
- **股票指数**: 上证综指、深证成指、创业板指、沪深300等
- **基金指数**: 偏股混合型基金指数、偏债混合型基金指数等

### 2. 数据字段
- 指数代码 (symbol)
- 交易日期 (date)
- 开盘价/收盘价/最高价/最低价 (open/close/high/low)
- 成交量/成交额 (volume/turnover)
- 净值 (nav, 基金指数)
- 涨跌幅 (change_pct)

## 核心组件

### 1. 爬虫任务 (`akshare_index_history_task.py`)
```python
from task_crawlers.akshare_index_history_task import AKShareIndexHistoryTask

# 配置参数
config = {
    'symbol': 'sh000001',      # 指数代码
    'index_type': 'stock',     # 指数类型: 'stock' 或 'fund'
    'start_date': '2025-01-01', # 开始日期
    'end_date': '2025-01-31',   # 结束日期
}

# 执行爬虫任务
task = AKShareIndexHistoryTask(config)
task.execute()

if task.is_success:
    hist_data = task.task_result['hist_data']
    print(f"获取到 {len(hist_data)} 条历史数据")
```

### 2. 数据库操作 (`db_index_hist.py`)
```python
from database.db_index_hist import DBIndexHist
from database.mysql_database import MySQLDatabase

# 初始化数据库
mysql_db = MySQLDatabase(host='127.0.0.1', user='baofu', password='xxx', database='baofu')
db = DBIndexHist(mysql_db)

# 创建数据表
db.create_index_hist_table()

# 插入历史数据
hist_data = {
    'symbol': 'sh000001',
    'date': '2025-07-19',
    'open': 3200.50,
    'close': 3234.90,
    'high': 3250.20,
    'low': 3180.80,
    'volume': 123456789,
    'turnover': 987654321.12
}
db.insert_index_hist_data(hist_data)

# 查询历史数据
result = db.get_index_hist_data('sh000001', start_date='2025-01-01', end_date='2025-12-31')
```

### 3. 数据更新任务 (`update_index_task.py`)
```python
from task_data.update_index_task import UpdateIndexTask

# 更新指定指数
config = {
    'index_symbols': ['sh000001', 'sz399001'],
    'index_type': 'stock',
    'days_back': 30
}

# 或者更新所有默认指数
config = {
    'update_all': True,
    'index_type': 'stock',
    'days_back': 30
}

task = UpdateIndexTask(config, mysql_db)
task.execute()
```

## 支持的指数列表

### 股票指数
| 代码 | 名称 | 市场 |
|------|------|------|
| sh000001 | 上证综指 | 上海 |
| sh000002 | 上证A股指数 | 上海 |
| sh000016 | 上证50 | 上海 |
| sh000300 | 沪深300 | 沪深 |
| sh000905 | 中证500 | 沪深 |
| sh000906 | 中证800 | 沪深 |
| sz399001 | 深证成指 | 深圳 |
| sz399005 | 中小板指 | 深圳 |
| sz399006 | 创业板指 | 深圳 |

### 基金指数
- 偏股混合型基金指数
- 偏债混合型基金指数  
- 股票型基金指数
- 债券型基金指数

## 使用示例

### 1. 获取上证综指最近30天数据
```python
config = {
    'name': 'get_sse_composite',
    'symbol': 'sh000001',
    'index_type': 'stock',
    'start_date': '2025-06-20',
    'end_date': '2025-07-19'
}

task = AKShareIndexHistoryTask(config)
task.execute()

if task.is_success:
    for data in task.task_result['hist_data']:
        print(f"{data['date']}: 收盘={data['close']}, 涨跌幅={data.get('change_pct', 0)}%")
```

### 2. 批量更新多个指数
```python
config = {
    'name': 'update_major_indices',
    'index_symbols': ['sh000001', 'sh000300', 'sz399001', 'sz399006'],
    'index_type': 'stock',
    'days_back': 7
}

task = UpdateIndexTask(config, mysql_db)
task.execute()

print(f"更新结果: {task.task_result['message']}")
print(f"成功更新: {task.task_result['updated_symbols']}")
```

### 3. 获取基金指数数据
```python
config = {
    'symbol': '偏股混合型基金指数',
    'index_type': 'fund',
    'start_date': '2025-01-01',
    'end_date': '2025-07-19'
}

task = AKShareIndexHistoryTask(config)
task.execute()
```

## 数据库表结构

```sql
CREATE TABLE index_hist_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '指数代码',
    date DATE NOT NULL COMMENT '交易日期',
    open DECIMAL(10,4) DEFAULT NULL COMMENT '开盘价',
    high DECIMAL(10,4) DEFAULT NULL COMMENT '最高价',
    low DECIMAL(10,4) DEFAULT NULL COMMENT '最低价',
    close DECIMAL(10,4) DEFAULT NULL COMMENT '收盘价',
    volume BIGINT DEFAULT NULL COMMENT '成交量',
    turnover DECIMAL(15,2) DEFAULT NULL COMMENT '成交额',
    nav DECIMAL(10,4) DEFAULT NULL COMMENT '净值（基金指数用）',
    change_pct DECIMAL(8,4) DEFAULT NULL COMMENT '涨跌幅（%）',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_date (symbol, date),
    INDEX idx_symbol (symbol),
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 依赖要求

确保已安装以下Python包：
```bash
pip install akshare pandas loguru mysql-connector-python
```

## 注意事项

1. **数据源依赖**: 依赖AKShare库，需要网络连接
2. **数据更新**: 建议定期运行更新任务以保持数据最新
3. **错误处理**: 爬虫任务包含完善的错误处理和重试机制
4. **数据去重**: 数据库设计了唯一键约束，避免重复数据
5. **性能优化**: 支持批量插入和增量更新

## 集成到产品管理界面

新的指数数据功能已集成到scripts_description.json中：

```json
{
  "akshare_index_history_task.py": {
    "class_name": "AKShareIndexHistoryTask",
    "description": "使用AkShare获取指数历史数据，支持股票指数和基金指数",
    "data_type": "index", 
    "is_active": true
  }
}
```

这样可以在产品管理界面中看到和管理指数数据爬虫任务。