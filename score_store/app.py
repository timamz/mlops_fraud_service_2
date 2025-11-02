import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('score-store')


class ScoreStoreService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.topic = os.getenv('KAFKA_SCORING_TOPIC', 'scoring')
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': os.getenv('KAFKA_GROUP_ID', 'fraud-db-writer'),
            'auto.offset.reset': 'earliest',
        })
        self.consumer.subscribe([self.topic])
        self.conn = self._connect_db()
        self._ensure_table()

    def _connect_db(self):
        params = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'dbname': os.getenv('DB_NAME', 'fraud'),
            'user': os.getenv('DB_USER', 'fraud'),
            'password': os.getenv('DB_PASSWORD', 'fraud'),
        }
        while True:
            try:
                conn = psycopg2.connect(**params)
                conn.autocommit = True
                logger.info('Connected to PostgreSQL')
                return conn
            except psycopg2.OperationalError as exc:
                logger.info('Waiting for PostgreSQL: %s', exc)
                time.sleep(1)

    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists transaction_scores (
                    transaction_id uuid primary key,
                    score double precision not null,
                    fraud_flag integer not null,
                    created_at timestamptz not null default now()
                )
                """
            )

    def run(self):
        logger.info('Consuming scoring topic')
        while True:
            try:
                msg = self.consumer.poll(1.0)
            except KafkaException as exc:
                logger.error('Kafka poll failed: %s', exc)
                time.sleep(1)
                continue
            if msg is None:
                continue
            if msg.error():
                logger.error('Kafka message error: %s', msg.error())
                continue
            try:
                payload = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as exc:
                logger.error('Invalid JSON payload: %s', exc)
                continue
            records = payload if isinstance(payload, list) else [payload]
            for record in records:
                self._persist(record)

    def _persist(self, record):
        transaction_id = record.get('transaction_id')
        score = record.get('score')
        fraud_flag = record.get('fraud_flag')
        if not transaction_id or score is None or fraud_flag is None:
            logger.warning('Skipping malformed record: %s', record)
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into transaction_scores (transaction_id, score, fraud_flag)
                values (%s, %s, %s)
                on conflict (transaction_id) do update
                set score = excluded.score,
                    fraud_flag = excluded.fraud_flag
                """,
                (transaction_id, float(score), int(fraud_flag)),
            )
        logger.info('Stored transaction %s', transaction_id)


def main():
    service = ScoreStoreService()
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info('Service interrupted')
    finally:
        service.consumer.close()
        service.conn.close()


if __name__ == '__main__':
    main()
