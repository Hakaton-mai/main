import ujson
from loguru import logger
from click_house import ClickHouseClient
from rabbit import RabbitMQConsumer
from gpt import giga_chat_request, giga_chat_request_mock


def process_message(body, clickhouse_client):
    """
    Обработка сообщения и отправка в ClickHouse.
    """
    try:
        logger.info(f"Получено сообщение из RabbitMQ")
        logger.info(body)
        message = ujson.loads(body)  # Декодируем JSON
        response = giga_chat_request(message['msg']) #TODO: заменить
        print(response)
        response = ujson.loads(response.replace('json', '').replace('```', ''))

        clickhouse_client.insert_data(response)

    except ujson.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e}")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


def main():
    """
    Основная функция для запуска RabbitMQ и ClickHouse.
    """
    rabbitmq_host = "localhost"
    queue_name = "test"

    clickhouse_host = "localhost"
    clickhouse_database = "mydb"
    clickhouse_table = "test_table"

    # Настройка логирования
    logger.add("main.log", rotation="10 MB", level="INFO", retention="10 days")

    # Инициализация клиентов
    logger.info("Инициализация клиентов RabbitMQ и ClickHouse.")
    rabbitmq_consumer = RabbitMQConsumer(rabbitmq_host, queue_name)
    clickhouse_client = ClickHouseClient(clickhouse_host, clickhouse_database, clickhouse_table)

    # Подключение к RabbitMQ
    rabbitmq_consumer.connect()

    # Обработка сообщений
    rabbitmq_consumer.consume(lambda ch, method, properties, body: process_message(body, clickhouse_client))


if __name__ == "__main__":
    main()
