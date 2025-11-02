# ML Fraud Detection Service

> Batch scoring microservice that watches a shared folder, scores incoming CSV files with a pre-trained logistic regression model, and stores predictions together with model explainability artefacts.

## Overview

The service is packaged as a Docker image and is intended to run in an environment where CSV files are dropped into an input directory. A file-system watcher reacts to newly created CSV files only (existing files at startup are ignored), performs preprocessing, loads persisted sklearn artefacts, and produces:
- `predictions_<name>_<timestamp>.csv` — predicted fraud labels
- `top5_<name>_<timestamp>.json` — top five feature importances
- `score_density_<name>_<timestamp>.png` — score distribution plot

All activity is logged both to STDOUT and to `/app/logs/service.log`.

## Repository Layout

```
├── Dockerfile
├── requirements.txt
├── app/
│   └── app.py                # Watchdog-powered batch scoring service
├── src/
│   ├── preprocessing.py      # Feature engineering helpers
│   └── scorer.py             # Model loading & inference utilities
├── models/
│   └── logreg_model.joblib   # Trained logistic regression classifier
├── preprocessors/
│   ├── cat_encoder.joblib
│   ├── cat_imputer.joblib
│   ├── num_imputer.joblib
│   └── num_scaler.joblib     # Persisted preprocessing artefacts
├── input/                    # Mount point for incoming CSVs
└── output/                   # Mount point for scoring results
```

> **Note:** The service expects the model artefact and preprocessing pipeline files to be present before startup. They are pre loaded in this repo and can be reproduced with the training notebook under `.train_model/`. Train and test data can be found [here](https://www.kaggle.com/competitions/teta-ml-1-2025/data)

## Data Processing Pipeline (`src/preprocessing.py`)

1. **Temporal Features** – derives `hour` and `dow` from the transaction timestamp.
2. **Geospatial Feature** – computes the great-circle distance (Haversine) between customer and merchant coordinates.
3. **Column Selection** – drops high-cardinality and identity columns, returning only the features used by the model.
4. **Artefact-driven Transforms** – reuses persisted `SimpleImputer`, `StandardScaler`, imputers, and one-hot encoder stored in `preprocessors/` to reproduce the training pipeline exactly.

## Model Scoring (`src/scorer.py`)

- Loads the logistic regression model stored at `models/logreg_model.joblib`.
- Applies the preprocessing artefacts from `preprocessors/`.
- Computes prediction probabilities, applies the configured classification threshold (default `0.5`), and exposes helper utilities for generating submissions, top feature importances, and diagnostic plots.

## Logging

- File log: `/app/logs/service.log`
- Console log: stdout
- Log levels:
  - `INFO` – lifecycle events and high-level processing steps
  - `DEBUG` – watcher diagnostics (enabled for non-CSV detections)
  - `ERROR` – failures with stack traces

## Requirements

- Docker 20.10+
- ~1 GB free disk space for the image and artefacts

```bash
git clone https://github.com/timamz/mlops_fraud_service
cd mlops_fraud_service
```

## Build & Run

```bash
# Build the image
docker build -t fraud-service .

# Run the container with bind mounts for input/output folders
docker run -it --rm \
  -v ./input:/app/input \
  -v ./output:/app/output \
  fraud-service
```

When the container starts you should see `fraud-service - INFO - File observer started`. Drop a CSV file into the local `./input` folder to trigger processing; results will appear in `./output` with timestamped filenames.

## Runtime Configuration

The following environment variables can be provided (`docker run -e KEY=value ...`) to customise behaviour:

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_DIR` | `/app/input` | Directory watched for new CSV files |
| `OUTPUT_DIR` | `/app/output` | Directory where outputs are written |
| `MODELS_DIR` | `/app/models` | Base directory containing the model artefact |
| `MODEL_PATH` | `${MODELS_DIR}/logreg_model.joblib` | Explicit path to the model file |
| `PREPROCESSORS_DIR` | `/app/preprocessors` | Directory with preprocessing artefacts |
| `SCORE_THRESHOLD` | `0.5` | Probability threshold for classifying fraud |

### Build & Run with custom paths and threshold

```bash
docker run -it --rm \
  -v /path/to/input:/data/input \
  -v /path/to/output:/data/output \
  -v /path/to/models:/data/models \
  -v /path/to/preprocessors:/data/preprocessors \
  -e INPUT_DIR=/data/input \
  -e OUTPUT_DIR=/data/output \
  -e MODELS_DIR=/data/models \
  -e MODEL_PATH=/data/models/logreg_model.joblib \
  -e PREPROCESSORS_DIR=/data/preprocessors \
  -e SCORE_THRESHOLD=0.66 \
  fraud-service
```

1. Replace `/path/to/...` with the host directories you want to mount.
2. Adjust `SCORE_THRESHOLD` or other env vars for your deployment.
