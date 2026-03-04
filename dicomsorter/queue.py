import logging
import threading
import time

import pika

from dicomsorter.settings import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS

logger = logging.getLogger(__name__)


class MessageQueue:
    def __init__(self, queues: list[str]):
        self._queues = queues
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._url: str | None = None
        self._stop_heartbeat = threading.Event()

    def connect(self, url: str) -> None:
        self._url = url
        for attempt in range(NUMBER_ATTEMPTS):
            logger.info("Trying RabbitMQ connection %s", attempt)
            try:
                self._connection = pika.BlockingConnection(pika.URLParameters(url))
                self._channel = self._connection.channel()
            except Exception as e:
                if attempt < NUMBER_ATTEMPTS - 1:
                    logger.info("Retrying in %s seconds...", RETRY_DELAY_IN_SECONDS)
                    time.sleep(RETRY_DELAY_IN_SECONDS)
                else:
                    raise Exception("Unable to connect to RabbitMQ after retries.") from e
            else:
                return

    def declare_queues(self) -> None:
        for queue in self._queues:
            self._channel.queue_declare(queue=queue, passive=False, durable=True)

    def start_heartbeat(self) -> None:
        self._stop_heartbeat.clear()
        thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._stop_heartbeat.is_set():
            try:
                if self._connection and self._connection.is_open:
                    self._connection.process_data_events(time_limit=0)
                else:
                    logger.warning("RabbitMQ connection closed, reconnecting...")
                    self.connect(self._url)
                    self.declare_queues()
            except Exception:
                logger.exception("Heartbeat error, will retry.")
            time.sleep(10)

    def publish_threadsafe(self, message: str) -> None:
        def _publish():
            for q in self._queues:
                self._channel.basic_publish(
                    exchange="",
                    routing_key=q,
                    body=message.encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            logger.info("Published to queues: %s", self._queues)

        self._connection.add_callback_threadsafe(_publish)

    def close(self) -> None:
        self._stop_heartbeat.set()
        if self._connection and self._connection.is_open:
            self._connection.close()
