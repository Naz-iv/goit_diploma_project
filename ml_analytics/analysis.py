import pandas as pd
from typing import Dict, Any
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
import numpy as np


def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    total = len(df)
    success_rate = None
    error_rate = None
    if "Status" in df.columns:
        success_rate = round(df['Status'].str.lower().eq('success').mean() * 100, 2)
        error_rate = round(df['Status'].str.lower().eq('error').mean() * 100, 2)
    avg_duration = None
    if "Duration" in df.columns:
        avg_duration = float(np.nanmean(pd.to_numeric(df["Duration"], errors="coerce")))

    avg_tool_count = None
    if "ToolCount" in df.columns:
        avg_tool_count = float(np.nanmean(pd.to_numeric(df["ToolCount"], errors="coerce")))

    most_common_tool = None
    if "Tools" in df.columns:
        most_common_tool = df["Tools"].mode().iloc[0] if not df["Tools"].mode().empty else None

    metrics = {
        "total_requests": int(total),
        "success_rate_pct": success_rate,
        "error_rate_pct": error_rate,
        "avg_duration_min": avg_duration,
        "avg_tool_count": round(avg_tool_count, 0),
        "most_common_device": most_common_tool
    }
    return metrics


def perform_clustering_with_feature_importance(df, n_clusters=3, random_state=42):
    df = df.copy()

    features = ["Duration", "ToolCount", "Bitrate", "ROP", "Num_FSLs"]
    valid_features = [f for f in features if f in df.columns]

    if not valid_features:
        df["cluster"] = -1
        df.attrs['centroids'] = pd.DataFrame()
        df.attrs['duration_feature_importance'] = pd.Series()
        return df

    df[valid_features] = df[valid_features].apply(pd.to_numeric, errors='coerce').fillna(0)

    X = df[valid_features].values
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    clusters = kmeans.fit_predict(Xs)
    df["cluster"] = clusters

    centroids = scaler.inverse_transform(kmeans.cluster_centers_)
    for i, feature in enumerate(valid_features):
        centroid_map = {c: centroids[c, i] for c in range(n_clusters)}
        df[f"_cluster_centroid_{feature}"] = df["cluster"].map(centroid_map)

    centroid_df = pd.DataFrame(centroids, columns=valid_features)
    df.attrs['centroids'] = centroid_df

    target = "Duration"
    predictor_features = [f for f in valid_features if f != target]

    if predictor_features:
        rf = RandomForestRegressor(random_state=random_state)
        rf.fit(df[predictor_features], df[target])
        importance = pd.Series(rf.feature_importances_, index=predictor_features).sort_values(ascending=False)
        df.attrs['duration_feature_importance'] = importance
    else:
        df.attrs['duration_feature_importance'] = pd.Series()

    return df
