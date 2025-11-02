import json
import os
import time
import uuid

import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'topic': os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions'),
}

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'dbname': os.getenv('DB_NAME', 'fraud'),
    'user': os.getenv('DB_USER', 'fraud'),
    'password': os.getenv('DB_PASSWORD', 'fraud'),
}


def send_to_kafka(df: pd.DataFrame) -> bool:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=10,
    )
    total = len(df)
    for idx, row in df.iterrows():
        payload = {
            'transaction_id': str(uuid.uuid4()),
            'data': row.to_dict(),
        }
        producer.send(KAFKA_CONFIG['topic'], value=payload)
        if total < 500:
            time.sleep(0.01)
    producer.flush()
    return True


def fetch_results():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            flagged = pd.read_sql_query(
                """
                select transaction_id, score, fraud_flag, created_at
                from transaction_scores
                where fraud_flag = 1
                order by created_at desc
                limit 10
                """,
                conn,
            )
            latest = pd.read_sql_query(
                """
                select score
                from transaction_scores
                order by created_at desc
                limit 100
                """,
                conn,
            )
        return flagged, latest
    except psycopg2.OperationalError as exc:
        st.error(f'Database unavailable: {exc}')
        return None, None


st.set_page_config(page_title='Fraud Streaming Console', layout='wide')
st.title('Fraud Streaming Console')

uploaded = st.file_uploader('Upload CSV with transactions', type=['csv'])

if uploaded is not None:
    try:
        df = pd.read_csv(uploaded)
        st.caption(f'{len(df)} rows ready for streaming')
        if st.button('Send to Kafka'):
            with st.spinner('Streaming records...'):
                ok = send_to_kafka(df)
            if ok:
                st.success('Transactions sent')
    except Exception as exc:
        st.error(f'Failed to read file: {exc}')

if st.button('View results'):
    flagged, latest = fetch_results()
    if flagged is not None:
        st.subheader('Fraudulent transactions')
        if flagged.empty:
            st.info('No fraud detected yet')
        else:
            st.dataframe(flagged)
    if latest is not None and not latest.empty:
        st.subheader('Score distribution (last 100)')
        bins = np.linspace(0, 1, 11)
        counts, _ = np.histogram(latest['score'], bins=bins)
        labels = [f'{bins[i]:.2f}-{bins[i+1]:.2f}' for i in range(len(bins) - 1)]
        chart_df = pd.DataFrame({'count': counts}, index=labels)
        st.bar_chart(chart_df)
