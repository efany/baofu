from typing import Optional, Union, Literal
from datetime import date
from .data_generator import DataGenerator
from .fund_data_generator import FundDataGenerator
from .strategy_data_generator import StrategyDataGenerator
from database.db_funds import DBFunds
from database.db_funds_nav import DBFundsNav
from database.db_strategys import DBStrategys
from database.mysql_database import MySQLDatabase
from loguru import logger

DataType = Literal['fund', 'strategy']

def create_data_generator(
    data_type: DataType,
    data_id: Union[str, int],
    mysql_db: MySQLDatabase,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> Optional[DataGenerator]:
    """
    创建数据生成器的工厂函数
    
    Args:
        data_type: 数据类型，'fund' 或 'strategy'
        data_id: 数据ID，基金代码或策略ID
        mysql_db: 数据库连接
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        DataGenerator: 对应类型的数据生成器实例
        None: 如果创建失败
    """
    try:
        if data_type == 'fund':
            if not isinstance(data_id, str):
                raise ValueError("Fund data_id must be string (fund code)")

            logger.info(f"创建基金数据生成器: {data_id}")
            return FundDataGenerator(
                fund_code=data_id,
                mysql_db=mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
        elif data_type == 'strategy':
            if not isinstance(data_id, (int, str)):
                raise ValueError("Strategy data_id must be integer or string")
                
            strategy_id = int(data_id)
            return StrategyDataGenerator(
                strategy_id=strategy_id,
                mysql_db=mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
        else:
            raise ValueError(f"Unknown data type: {data_type}")
            
    except Exception as e:
        print(f"Error creating data generator: {str(e)}")
        return None

def get_data_generator(
    data_type: DataType,
    data_id: Union[str, int],
    mysql_db,
    **kwargs
) -> DataGenerator:
    """
    获取数据生成器，如果创建失败则抛出异常
    
    Args:
        参数同 create_data_generator
        
    Returns:
        DataGenerator: 对应类型的数据生成器实例
        
    Raises:
        ValueError: 如果创建失败
    """
    generator = create_data_generator(data_type, data_id, mysql_db, **kwargs)
    if generator is None:
        raise ValueError(f"Failed to create data generator for {data_type} with id {data_id}")
    return generator
