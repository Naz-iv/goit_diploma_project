import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from collections import Counter


def plot_tools_distribution(df: pd.DataFrame, out_path: str):
    tools_lists = df["Tools"].dropna().apply(lambda x: x if isinstance(x, list) else [x])

    all_tools = [tool.strip() for sublist in tools_lists for tool in sublist]

    tool_counts = Counter(all_tools)

    counts = pd.Series(tool_counts).sort_values(ascending=False)

    top_counts = counts.head(30)
    if len(counts) > 30:
        other_count = counts[30:].sum()
        top_counts["Other"] = other_count

    fig, ax = plt.subplots(figsize=(12, 5))
    top_counts.plot(kind="bar", ax=ax)

    ax.set_title("Requests per Tool Type (Top 20 + Other)")
    ax.set_ylabel("Count")
    ax.set_xlabel("Tool Type")
    plt.xticks(rotation=45, ha='right')

    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)

def plot_requests_over_time(df: pd.DataFrame, out_path: str):
    if "Created" not in df.columns:
        return
    s = pd.to_datetime(df["Created"], errors="coerce").dt.floor("D").value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(8,4))
    s.plot(kind="line", ax=ax)
    ax.set_title("Requests over Time (per day)")
    ax.set_ylabel("Requests")
    ax.set_xlabel("Date")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)

def plot_duration_histogram(df: pd.DataFrame, out_path: str):
    if "Duration" not in df.columns:
        return
    durations = pd.to_numeric(df["Duration"], errors="coerce").dropna()
    fig, ax = plt.subplots(figsize=(8,4))
    ax.hist(durations, bins=30)
    ax.set_title("Distribution of Request Duration (min)")
    ax.set_xlabel("Duration (minutes)")
    ax.set_ylabel("Count")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def plot_cluster_profiles(df: pd.DataFrame, out_path: str):
    if "cluster" not in df.columns:
        return

    centroid_cols = [c for c in df.columns if c.startswith("_cluster_centroid_")]
    feature_names = [c.replace("_cluster_centroid_", "") for c in centroid_cols]

    centroids = (
        df.groupby("cluster")[centroid_cols]
        .first()
        .rename(columns=lambda x: x.replace("_cluster_centroid_", ""))
    )

    centroids_norm = (centroids - centroids.min()) / (centroids.max() - centroids.min() + 1e-6)

    num_features = len(feature_names)
    angles = np.linspace(0, 2*np.pi, num_features, endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

    for cluster_id in centroids_norm.index:
        values = centroids_norm.loc[cluster_id].tolist()
        values += values[:1]
        ax.plot(angles, values, linewidth=2, label=f"Cluster {cluster_id}")
        ax.fill(angles, values, alpha=0.15)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(feature_names)
    ax.set_title("Cluster profiles (Radar Chart)")
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1))

    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def plot_clusters_pie(df: pd.DataFrame, out_path: str):
    if "cluster" not in df.columns:
        return

    centroid_cols = [c for c in df.columns if c.startswith("_cluster_centroid_")]
    feature_names = [c.replace("_cluster_centroid_", "") for c in centroid_cols]

    feature_min = df[feature_names].min()
    feature_max = df[feature_names].max()

    cluster_labels = {}
    cluster_centroids = {}

    for cluster_id in df["cluster"].unique():
        cluster_data = df[df["cluster"] == cluster_id]

        centroid_values = cluster_data[centroid_cols].iloc[0]

        normalized = {
            f: (centroid_values[f"_cluster_centroid_{f}"] - feature_min[f]) /
               (feature_max[f] - feature_min[f] + 1e-6)
            for f in feature_names
        }
        cluster_centroids[cluster_id] = normalized

        cluster_labels[cluster_id] = f"Cluster {cluster_id}"

    df["ClusterLabel"] = df["cluster"].map(cluster_labels)

    counts = df["ClusterLabel"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(6, 6))
    counts.plot(kind="pie", autopct='%1.1f%%', ax=ax)
    ax.set_title("Cluster distribution")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)

    for cluster_id, values in cluster_centroids.items():
        radar_out = out_path.replace(".png", f"_cluster_{cluster_id}_radar.png")

        labels = list(values.keys())
        stats = list(values.values())

        labels.append(labels[0])
        stats.append(stats[0])

        angles = np.linspace(0, 2 * np.pi, len(labels))

        fig = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))[0]
        ax = fig.add_subplot(111, polar=True)

        ax.plot(angles, stats, linewidth=2)
        ax.fill(angles, stats, alpha=0.2)

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels[:-1], fontsize=9)
        ax.set_title(f"Cluster {cluster_id} Radar Profile", fontsize=12)

        fig.tight_layout()
        fig.savefig(radar_out)
        plt.close(fig)