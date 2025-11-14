from etl import load_data
from analysis import compute_metrics, perform_clustering_with_feature_importance
from viz import plot_tools_distribution, plot_requests_over_time, plot_duration_histogram, plot_clusters_pie
import json
from pathlib import Path

def main():
    root_dir = Path(__file__).resolve().parent

    outdir = root_dir / "output"

    outdir.mkdir(parents=True, exist_ok=True)

    df = load_data(r"C:\Users\nivankiv\git\frame-generator-project\Frame Requests.db")
    df = perform_clustering_with_feature_importance(df, n_clusters=4)
    metrics = compute_metrics(df)

    csv_out = outdir / "analytics_output.csv"
    json_out = outdir / "metrics.json"
    df.to_csv(csv_out, index=False)
    with open(json_out, "w") as f:
        json.dump(metrics, f, indent=2)

    plot_tools_distribution(df, str(outdir / "device_distribution.png"))
    plot_requests_over_time(df, str(outdir / "requests_over_time.png"))
    plot_duration_histogram(df, str(outdir / "duration_histogram.png"))
    plot_clusters_pie(df, str(outdir / "clusters_pie.png"))

    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()
