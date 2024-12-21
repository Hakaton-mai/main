import json
import clickhouse_connect
from loguru import logger
import uuid

class ClickHouseClient:
    """
    Класс для отправки данных в ClickHouse.
    """
    def __init__(self, host, database, table):
        self.client = clickhouse_connect.get_client(host=host)
        self.database = database
        self.table = table
        self.ensure_table()

    def ensure_table(self):
        """
        Проверяет существование таблицы и создает её при необходимости.
        """
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.client.command(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
                    id String,
                    category String,
                    subcategory String,
                    reason String
                ) ENGINE = MergeTree()
                ORDER BY id
            """)
            logger.info(f"Таблица {self.database}.{self.table} готова для использования.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы ClickHouse: {e}")
            raise

    def insert_data(self, data):
        """
        Вставка данных в таблицу.
        """
        try:
            # Форматируем данные под таблицу
            formatted_data = [
                str(uuid.uuid4()),  # Генерация уникального id
                data.get('category'),
                data.get('subcategory'),
                data.get('reason')
            ]
            self.client.insert(f"{self.database}.{self.table}", [formatted_data])
            logger.info(f"Данные успешно вставлены в ClickHouse: {formatted_data}")
        except Exception as e:
            logger.error(f"Ошибка вставки данных в ClickHouse: {e}")
            raise

