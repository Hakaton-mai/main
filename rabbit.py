import pika
import json
from loguru import logger


class RabbitMQConsumer:
    """
    Класс для потребления сообщений из RabbitMQ.
    """

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        """
        Подключение к RabbitMQ.
        """
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Подключение к RabbitMQ ({self.host}) успешно установлено.")
        except Exception as e:
            logger.error(f"Ошибка подключения к RabbitMQ: {e}")
            raise

    def consume(self, callback):
        """
        Начало чтения сообщений из очереди.
        """
        logger.info(f"Ожидание сообщений из очереди: {self.queue_name}. Нажмите Ctrl+C для выхода.")
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Получен сигнал завершения. Закрытие соединения.")
        except Exception as e:
            logger.error(f"Ошибка во время потребления сообщений: {e}")
        finally:
            self.connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")
