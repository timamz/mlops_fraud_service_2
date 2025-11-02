import numpy as np
import pandas as pd
import joblib

def add_features(df):
    df = df.copy()
    dt = pd.to_datetime(df['transaction_time'])
    df['hour'] = dt.dt.hour
    df['dow'] = dt.dt.dayofweek

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371.0
        p1 = np.radians(lat1)
        p2 = np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlambda / 2) ** 2
        return 2 * R * np.arcsin(np.sqrt(a))

    df['distance_km'] = haversine(df['lat'], df['lon'], df['merchant_lat'], df['merchant_lon'])
    return df

def make_X(df):
    df = add_features(df)
    drop_cols = [
        'transaction_time','name_1','name_2','street','one_city',
        'jobs','merch','post_code','target'
    ]
    keep = [c for c in df.columns if c not in drop_cols]
    return df[keep]

def transform_with_artifacts(X, artifacts_dir='preprocessors'):
    num_features = [
        'amount','lat','lon','merchant_lat','merchant_lon',
        'population_city','hour','dow','distance_km'
    ]
    cat_features = ['cat_id','us_state','gender']

    num_imputer = joblib.load(f'{artifacts_dir}/num_imputer.joblib')
    num_scaler = joblib.load(f'{artifacts_dir}/num_scaler.joblib')
    cat_imputer = joblib.load(f'{artifacts_dir}/cat_imputer.joblib')
    cat_encoder = joblib.load(f'{artifacts_dir}/cat_encoder.joblib')

    X_num = num_scaler.transform(num_imputer.transform(X[num_features]))
    X_cat = cat_encoder.transform(cat_imputer.transform(X[cat_features]))
    return np.hstack([X_num, X_cat])
