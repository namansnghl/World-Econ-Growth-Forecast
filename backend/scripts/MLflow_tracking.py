import mlflow
import mlflow.keras
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from mlflow.models.signature import infer_signature
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def log_metrics_and_artifacts(model, X_test_lstm, y_test, y_pred, scaler):
    with mlflow.start_run(run_name="LSTM_model1") as run:
        test_loss = model.evaluate(X_test_lstm, y_test)
        mlflow.log_metric("test_loss", test_loss)

        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        model_summary = []
        model.summary(print_fn=lambda x: model_summary.append(x))
        summary_str = "\n".join(model_summary)
        with open("model_summary.txt", "w", encoding="utf-8") as f:
            f.write(summary_str)
        mlflow.log_artifact("model_summary.txt")

        with open("scaler.pkl", "wb") as f:
            import pickle
            pickle.dump(scaler, f)
        mlflow.log_artifact("scaler.pkl")

        model_name = "GDP_Forecast_LSTM_Model"
        model_uri = f"runs:/{run.info.run_id}/gdp_forecast_lstm_model.h5"
        mlflow.keras.log_model(model, model_name, registered_model_name=model_name, signature=infer_signature(X_test_lstm, y_pred))

if __name__ == "__main__":
    log_metrics_and_artifacts(model, X_test_lstm, y_test, y_pred, scaler)
