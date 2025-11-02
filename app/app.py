import logging
import os
import sys
import time
from datetime import datetime

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.scorer import (
    make_submission,
    save_density,
    save_top5_json,
    score_dataframe,
    top5_importances,
)

LOG_PATH = '/app/logs/service.log'
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger('fraud-service')


class ScoringService:
    def __init__(self):
        logger.info('Initializing scoring service')
        self.input_dir = os.environ.get('INPUT_DIR', '/app/input')
        self.output_dir = os.environ.get('OUTPUT_DIR', '/app/output')
        models_dir = os.environ.get('MODELS_DIR', '/app/models')
        self.model_path = os.environ.get(
            'MODEL_PATH', os.path.join(models_dir, 'logreg_model.joblib')
        )
        self.preprocessors_dir = os.environ.get('PREPROCESSORS_DIR', '/app/preprocessors')
        self.threshold = float(os.environ.get('SCORE_THRESHOLD', '0.5'))

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(
            'Watching %s | writing outputs to %s | model %s | preprocessors %s',
            self.input_dir,
            self.output_dir,
            self.model_path,
            self.preprocessors_dir,
        )

    def process_file(self, file_path: str) -> None:
        try:
            logger.info('Processing file %s', file_path)
            df = pd.read_csv(file_path)

            scores, labels = score_dataframe(
                df,
                model_path=self.model_path,
                preprocessors_dir=self.preprocessors_dir,
                threshold=self.threshold,
            )
            submission = make_submission(df.index, labels)

            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            stem = os.path.splitext(os.path.basename(file_path))[0]

            csv_name = f'predictions_{stem}_{timestamp}.csv'
            json_name = f'top5_{stem}_{timestamp}.json'
            png_name = f'score_density_{stem}_{timestamp}.png'

            csv_path = os.path.join(self.output_dir, csv_name)
            json_path = os.path.join(self.output_dir, json_name)
            png_path = os.path.join(self.output_dir, png_name)

            submission.to_csv(csv_path, index=False)
            save_top5_json(
                top5_importances(
                    model_path=self.model_path,
                    preprocessors_dir=self.preprocessors_dir,
                ),
                json_path,
            )
            save_density(scores, png_path)

            logger.info('Wrote %s', csv_path)
            logger.info('Wrote %s', json_path)
            logger.info('Wrote %s', png_path)
        except Exception as exc:
            logger.error('Failed processing %s: %s', file_path, exc, exc_info=True)


class CsvCreatedHandler(FileSystemEventHandler):
    def __init__(self, service: ScoringService):
        self.service = service

    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.lower().endswith('.csv'):
            logger.debug('Ignoring non-CSV file %s', event.src_path)
            return
        logger.debug('Detected new CSV %s', event.src_path)
        self.service.process_file(event.src_path)


def main():
    logger.info('Starting ML scoring service')
    service = ScoringService()

    observer = Observer()
    observer.schedule(CsvCreatedHandler(service), path=service.input_dir, recursive=False)
    observer.start()
    logger.info('File observer started')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Service interrupted, shutting down')
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
