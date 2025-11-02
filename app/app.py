import json
import logging
import os
import sys
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaException, Producer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.scorer import score_dataframe

LOG_PATH = '/app/logs/service.log'
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger('fraud-scoring')


class KafkaScoringService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.transactions_topic = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
        self.scoring_topic = os.getenv('KAFKA_SCORING_TOPIC', 'scoring')
        models_dir = os.getenv('MODELS_DIR', '/app/models')
        self.model_path = os.getenv('MODEL_PATH', os.path.join(models_dir, 'logreg_model.joblib'))
        self.preprocessors_dir = os.getenv('PREPROCESSORS_DIR', '/app/preprocessors')
        self.threshold = float(os.getenv('SCORE_THRESHOLD', '0.5'))
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': os.getenv('KAFKA_GROUP_ID', 'fraud-detector'),
            'auto.offset.reset': 'earliest',
        })
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        self.consumer.subscribe([self.transactions_topic])

    def run(self):
        logger.info('Waiting for Kafka messages')
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
                self._handle_record(record)

    def _handle_record(self, record):
        transaction_id = record.get('transaction_id')
        data = record.get('data')
        if not transaction_id or data is None:
            logger.warning('Skipping record without required fields: %s', record)
            return
        df = pd.DataFrame([data])
        try:
            scores, labels = score_dataframe(
                df,
                model_path=self.model_path,
                preprocessors_dir=self.preprocessors_dir,
                threshold=self.threshold,
            )
        except Exception as exc:
            logger.error('Scoring failed for %s: %s', transaction_id, exc)
            return
        result = {
            'transaction_id': transaction_id,
            'score': float(scores[0]),
            'fraud_flag': int(labels[0]),
        }
        try:
            self.producer.produce(self.scoring_topic, json.dumps(result).encode('utf-8'))
            self.producer.poll(0)
            logger.info('Scored transaction %s', transaction_id)
        except BufferError:
            self.producer.flush()
            self.producer.produce(self.scoring_topic, json.dumps(result).encode('utf-8'))
        except Exception as exc:
            logger.error('Failed to publish result: %s', exc)


def main():
    service = KafkaScoringService()
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info('Service interrupted')
    finally:
        service.consumer.close()
        service.producer.flush()


if __name__ == '__main__':
    main()
