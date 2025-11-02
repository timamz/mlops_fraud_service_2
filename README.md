# Real-Time Fraud Detection System

## 🏗️ Architecture

1. **`interface`** (Streamlit UI):
   - Uploads CSV files, generates unique `transaction_id` values, and publishes each row to the Kafka topic `transactions`.
   - Exposes the “View results” button to display the latest showcase from PostgreSQL.

2. **`fraud_detector`** (ML service):
   - Consumes `transactions`, applies the preprocessing from `src/` with artefacts in `models/` and `preprocessors/`.
   - Produces `score` and `fraud_flag`, writing each result to the Kafka topic `scoring`.

3. **`score_store`** (showcase writer):
   - Reads `scoring` and keeps the PostgreSQL table `transaction_scores` up to date.

4. **Kafka stack**:
   - Zookeeper + Kafka broker.
   - `kafka-setup` creates the `transactions` and `scoring` topics.
   - Kafka UI (port 8080) for monitoring messages.

5. **PostgreSQL**:
   - Stores scoring results for the UI and external systems.

## 🚀 Quick Start

Requirements: Docker 20.10+, Docker Compose 2.0+

```bash
git clone https://github.com/timamz/mlops_fraud_service_2
cd mlops_fraud_service_2
docker compose up --build
```

After the stack is up:
- Streamlit UI: http://localhost:8501
- Kafka UI: http://localhost:8080
- PostgreSQL: localhost:5432 (user `fraud`, password `fraud`)

## 🛠️ Usage

1. Open the Streamlit UI.
2. Upload a CSV from the competition (e.g., `input/test.csv` or `input/test_small.csv` for quick check).
3. Click “Send to Kafka” — each row streams into the `transactions` topic.
4. Click “View results”:
   - See the last 10 transactions with `fraud_flag == 1` (if any exist).
   - View a histogram for the latest 100 scores in the showcase.
5. Use Kafka UI or connect to PostgreSQL directly for additional inspection.

### Verifying PostgreSQL

You can inspect the showcase directly from the Postgres container:

```bash
docker compose exec postgres psql -U fraud -d fraud -c \
"select transaction_id, score, fraud_flag, created_at from transaction_scores order by created_at desc limit 10;"
```

Example output:

```
            transaction_id            |         score         | fraud_flag |          created_at           
--------------------------------------+-----------------------+------------+-------------------------------
 a465fd58-cf51-41aa-a2c7-81645b87cd2c |  0.004825917601726055 |          0 | 2025-11-02 14:59:44.124391+00
 5a096e1e-3757-4f86-b0c0-93cf5ef08844 |   0.40422400462970276 |          0 | 2025-11-02 14:59:44.11684+00
 524a703a-184e-4d08-bd8c-463dfa021754 |  0.006092516382174806 |          0 | 2025-11-02 14:59:44.11649+00
 3b8c3087-8cae-4ce2-93c0-64a2ed7be6a0 | 0.0054243573908884365 |          0 | 2025-11-02 14:59:44.109183+00
 5cbc1a01-f6fc-46f7-92de-cc5df9916926 |    0.2090544974111739 |          0 | 2025-11-02 14:59:44.108665+00
 b19fb63c-6cbe-48ac-a152-a04c7772e5e7 |   0.25788578342061896 |          0 | 2025-11-02 14:59:44.102444+00
 1ae913ef-ea97-40a1-b0f9-992baee278e0 |    0.1738293497585849 |          0 | 2025-11-02 14:59:44.093094+00
 c7a9291f-aa2a-44a1-966d-a928435931be |  0.003779912224879411 |          0 | 2025-11-02 14:59:44.092188+00
 eac0b51f-e4a1-484e-8f97-5b7d63c4e9b6 |   0.25873338554708464 |          0 | 2025-11-02 14:59:44.08434+00
 ee064a23-bed4-4415-81f1-4eb5f38d8fd4 |    0.2607956607705275 |          0 | 2025-11-02 14:59:44.083829+00
(10 rows)
```

## 📂 Project Layout

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── app/
│   └── app.py
├── src/
│   ├── preprocessing.py
│   └── scorer.py
├── models/
├── preprocessors/
├── interface/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── score_store/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── input/test.csv
```

## ⚙️ Kafka & Environment

- Topics: `transactions` (input) and `scoring` (output).
- Partitions: 3, replication factor: 1 (development mode).
- Key environment variables (override in `docker-compose.yml` as needed):
  - `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TRANSACTIONS_TOPIC`, `KAFKA_SCORING_TOPIC`
  - `SCORE_THRESHOLD`
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Scoring logs are available inside the `fraud_detector` container at `/app/logs/service.log`.
