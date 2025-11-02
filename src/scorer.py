import json
import os

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.preprocessing import make_X, transform_with_artifacts

NUM_FEATURES = [
    'amount','lat','lon','merchant_lat','merchant_lon',
    'population_city','hour','dow','distance_km'
]
CAT_FEATURES = ['cat_id','us_state','gender']

def load_model(path='models/logreg_model.joblib'):
    return joblib.load(path)

def feature_names(preprocessors_dir='preprocessors'):
    enc = joblib.load(os.path.join(preprocessors_dir, 'cat_encoder.joblib'))
    cat_names = enc.get_feature_names_out(CAT_FEATURES)
    return np.array(NUM_FEATURES + list(cat_names))

def score_dataframe(
    df,
    model_path='models/logreg_model.joblib',
    preprocessors_dir='preprocessors',
    threshold=0.5,
):
    X = make_X(df)
    Xf = transform_with_artifacts(X, preprocessors_dir)
    clf = load_model(model_path)
    scores = clf.predict_proba(Xf)[:, 1]
    labels = (scores >= threshold).astype(int)
    return scores, labels

def make_submission(index, labels):
    return pd.DataFrame({'index': index, 'prediction': labels})

def top5_importances(
    model_path='models/logreg_model.joblib', preprocessors_dir='preprocessors'
):
    clf = load_model(model_path)
    names = feature_names(preprocessors_dir)
    coefs = np.ravel(clf.coef_)
    idx = np.argsort(np.abs(coefs))[::-1][:5]
    return {names[i]: float(abs(coefs[i])) for i in idx}

def save_top5_json(items_dict, path):
    with open(path, 'w') as f:
        json.dump(items_dict, f, ensure_ascii=False, indent=2)

def save_density(scores, path):
    plt.figure()
    plt.hist(scores, bins=50, density=True)
    plt.xlabel('score')
    plt.ylabel('density')
    plt.title('Score density')
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
