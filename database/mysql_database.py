import mysql.connector
from mysql.connector import errorcode

class MySQLDatabase:
    def __init__(self, host, user, password, database):
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print("数据库连接成功")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("用户名或密码错误")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("数据库不存在")
            else:
                print(err)
            self.connection = None
            self.cursor = None

    def create_table(self, table_name, table_schema):
        if self.cursor:
            try:
                self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})")
                self.connection.commit()
                print(f"表 '{table_name}' 创建成功或已存在")
            except mysql.connector.Error as err:
                print(f"创建表时出错: {err}")

    def check_table_exists(self, table_name):
        if self.cursor:
            self.cursor.execute(
                "SHOW TABLES LIKE %s",
                (table_name,)
            )
            result = self.cursor.fetchone()
            return result is not None
        return False

    def insert_data(self, table_name, data):
        if self.cursor:
            placeholders = ", ".join(["%s"] * len(data))
            columns = ", ".join(data.keys())
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            try:
                self.cursor.execute(sql, list(data.values()))
                self.connection.commit()
                print("数据插入成功")
            except mysql.connector.Error as err:
                print(f"插入数据时出错: sql: {sql}, err: {err}")
    
    def update_data(self, table_name, data, conditions):
        if self.cursor:
            set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
            where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            try:
                self.cursor.execute(sql, list(data.values()) + list(conditions.values()))
                self.connection.commit()
                print("数据更新成功")
            except mysql.connector.Error as err:
                print(f"更新数据时出错: sql: {sql}, err: {err}")

    def fetch_data(self, table_name, conditions=None):
        if self.cursor:
            sql = f"SELECT * FROM {table_name}"
            if conditions:
                sql += " WHERE " + " AND ".join([f"{k}=%s" for k in conditions.keys()])
            try:
                self.cursor.execute(sql, tuple(conditions.values()) if conditions else None)
                return self.cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"查询数据时出错: {err}")
                return None

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭") 