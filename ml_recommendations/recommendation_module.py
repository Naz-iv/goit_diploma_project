import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame

from ml_analytics.etl import load_data
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor


class ParameterRecommender:
    def __init__(self, df: DataFrame, table_name: str = "Frame_Request_Data"):
        self.table_name = table_name
        self.df = df
        self.kmeans = None
        self.scaler = None
        self.feature_names = ["ToolCount", "Bitrate", "ROP", "Num_FSLs"]
        self.rf_model = None
        self.recommend_features = ["ROP", "Bitrate"]

    #Формуємо KMeans кластери
    def fit_clusters(self, n_clusters=4, random_state=42):
        self.scaler = StandardScaler()

        self.df[self.feature_names] = self.df[self.feature_names].apply(pd.to_numeric, errors='coerce').fillna(0)

        Xs = self.scaler.fit_transform(self.df[self.feature_names].values)
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
        self.df['cluster'] = self.kmeans.fit_predict(Xs)

        # Store centroids
        centroids = self.scaler.inverse_transform(self.kmeans.cluster_centers_)
        centroid_df = pd.DataFrame(centroids, columns=self.feature_names)
        self.df.attrs['centroids'] = centroid_df

    #Тренування RandomForest для передбачення Duration
    def fit_duration_model(self):
        target = "Duration"
        predictors = [f for f in self.feature_names if f != target]
        if not predictors:
            return
        self.rf_model = RandomForestRegressor(random_state=42)
        self.rf_model.fit(self.df[predictors], self.df[target])
        self.df.attrs['duration_feature_importance'] = pd.Series(
            self.rf_model.feature_importances_, index=predictors
        ).sort_values(ascending=False)

    # Рекомендовані парамтери для запиту
    def recommend(self, new_request: dict):

        req_df = pd.DataFrame(new_request)
        req_df[self.feature_names] = req_df[self.feature_names].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Визначаємо кластер
        req_scaled = self.scaler.transform(req_df[self.feature_names].values)
        cluster_label = self.kmeans.predict(req_scaled)[0]

        # Беремо центроїд кластера для ROP та Bitrate
        centroid = self.df.attrs['centroids'].iloc[cluster_label]
        recommended = {f: int(centroid[f]) for f in self.recommend_features}

        # Прогноз Duration для запиту
        duration_row = req_df.copy()
        for f in self.recommend_features:
            duration_row[f] = centroid[f]

        predicted_duration = None
        if self.rf_model:
            predicted_duration = float(self.rf_model.predict(duration_row[self.feature_names])[0])

        return {
            "cluster": cluster_label,
            "recommended_parameters": recommended,
            "predicted_duration": predicted_duration
        }



if __name__ == "__main__":
    db_file = r"C:\Users\nivankiv\git\frame-generator-project\Frame Requests.db"

    recommender = ParameterRecommender(load_data(db_file))
    recommender.fit_clusters(n_clusters=4)
    recommender.fit_duration_model()

    # приклад запиту
    new_requests = [
        {"ToolCount": 8, "Bitrate": 12, "ROP": 220, "Num_FSLs": 3},
    ]

    recommendations = recommender.recommend(new_requests)
    for p, r in recommendations.items():
        print(p, r)
