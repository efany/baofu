1. 基金基本信息
ts_code        | str    | 基金代码
name           | str    | 简称
management     | str    | 管理人
custodian      | str    | 托管人
fund_type      | str    | 投资类型
found_date     | str    | 成立日期
due_date       | str    | 到期日期
list_date      | str    | 上市时间
issue_date     | str    | 发行日期
delist_date    | str    | 退市日期
issue_amount   | float  | 发行份额(亿)
m_fee          | float  | 管理费
c_fee          | float  | 托管费
duration_year  | float  | 存续期
p_value        | float  | 面值
min_amount     | float  | 起点金额(万元)
exp_return     | float  | 预期收益率
benchmark      | str    | 业绩比较基准
status         | str    | 存续状态D摘牌 I发行 L已上市
invest_type    | str    | 投资风格
type           | str    | 基金类型
trustee        | str    | 受托人
purc_startdate | str    | 日常申购起始日
redm_startdate | str    | 日常赎回起始日
market         | str    | E场内O场外

## 安装依赖

使用 `pip` 安装所需的 Python 库：

```bash
pip install mysql-connector-python
```