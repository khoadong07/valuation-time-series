import os
from datetime import datetime
from typing import List, Dict

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from tabpfn import TabPFNRegressor


def flatten_forecast_data(nested_data: Dict) -> List[Dict]:
    return [
        {
            "local_authority": local_authority,
            "property_type": prop_type,
            "mae": round(metrics["mae"], 4),
            "mape": round(metrics["mape"], 4),
            "forecast": round(metrics["forecast"], 6),
            "forecast_date": metrics["forecast_date"],
            "best_model": metrics["best_model"],
            "best_time_window": metrics["best_time_window"],
            "model_file": metrics["model_file"]
        }
        for local_authority, prop_data in nested_data.items()
        for prop_type, metrics in prop_data.items()
    ]


async def train_and_forecast_by_authority(
        data: List[Dict],
        property_types: List[str] = ['SemiDetachedIndex', 'DetachedIndex', 'TerracedIndex', 'FlatIndex'],
        forecast_horizon: int = 1,
        future_steps: int = 1,
        model_dir: str = "models"
) -> List[Dict]:
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError("No data provided")

    required_columns = {'Date', 'RegionName'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

    df = df.assign(period=pd.to_datetime(df['Date'], format='%Y-%m-%d').dt.to_period('M').dt.to_timestamp())

    feature_cols = [
        'cpi_rate', 'cpih_rate', 'bank_rate', 'unemployment_rate', 'population',
        'New dwellings Price', 'New dwellings average advance',
        'New dwellings average recorded income of borrowers',
        'Other dwellings Price', 'Other dwellings average advance',
        'Other dwellings average recorded income of borrowers',
        'All dwellings Price', 'All dwellings average advance',
        'All dwellings average recorded income of borrowers',
        'First time buyers Price', 'First time buyers average advance',
        'First time buyers average recorded income of borrowers',
        'Former owner occupiers Price', 'Former owner occupiers average advance',
        'Former owner occupiers average recorded income of borrowers'
    ]

    os.makedirs(model_dir, exist_ok=True)
    results = {}
    time_windows = [5, 10, 15, 20, None]

    for authority in df['RegionName'].unique():
        df_authority = df[df['RegionName'] == authority].copy()
        if df_authority.empty:
            raise ValueError(f"No data for authority: {authority}")

        results[authority] = {}

        for prop_type in property_types:
            if prop_type not in df_authority:
                raise ValueError(f"Missing '{prop_type}' for authority: {authority}")

            df_auth_prop = df_authority.sort_values('period')
            last_date = df_auth_prop['period'].iloc[-1]

            best_model_name = None
            best_mape = float('inf')
            best_model = None
            best_time_window = None
            best_metrics = {}

            for time_window in time_windows:
                df_time = df_auth_prop.copy()
                if time_window is not None:
                    start_date = last_date - pd.DateOffset(years=time_window)
                    df_time = df_time[df_time['period'] >= start_date]

                if len(df_time) < 4:
                    raise ValueError(
                        f"Not enough data for {prop_type} in {authority} with {time_window if time_window else 'all'} years"
                    )

                all_features = feature_cols + [prop_type]
                for col in all_features:
                    for lag in [1, 2, 3]:
                        df_time[f'{col}_lag{lag}'] = df_time[col].shift(lag)

                df_time = df_time.dropna()
                if len(df_time) < forecast_horizon + 1:
                    raise ValueError(
                        f"Not enough data after lag creation for {prop_type} in {authority} with {time_window if time_window else 'all'} years"
                    )

                lag_features = [f'{col}_lag{lag}' for col in all_features for lag in [1, 2, 3]]
                X, y = df_time[lag_features], df_time[prop_type]
                train_size = len(df_time) - forecast_horizon
                X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
                y_train, y_test = y.iloc[:train_size], y.iloc[train_size:]

                models = {
                    'TabPFN': TabPFNRegressor(device='cpu'),
                    'CatBoost': CatBoostRegressor(verbose=0, random_state=42),
                    'LightGBM': LGBMRegressor(random_state=42, verbose=-1)
                }

                for model_name, model in models.items():
                    model_file = os.path.join(
                        model_dir,
                        f"{authority}_{prop_type}_{model_name}_{time_window if time_window else 'all'}_model.pkl"
                    )

                    if os.path.exists(model_file):
                        continue  # Skip training if model exists
                    model.fit(X_train, y_train)

                    y_pred = model.predict(X_test)
                    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

                    if mape < best_mape:
                        best_mape = mape
                        best_model_name = model_name
                        best_model = model
                        best_time_window = time_window
                        best_metrics = {'mae': mean_absolute_error(y_test, y_pred), 'mape': mape}

            if best_model is None:
                raise ValueError(f"No valid model found for {prop_type} in {authority}")

            df_auth_prop_full = df_auth_prop.copy()
            for col in all_features:
                for lag in [1, 2, 3]:
                    df_auth_prop_full[f'{col}_lag{lag}'] = df_auth_prop_full[col].shift(lag)
            df_auth_prop_full = df_auth_prop_full.dropna()
            last_data = df_auth_prop_full[lag_features].iloc[-1:]
            forecast = best_model.predict(last_data)[0]

            forecast_date = last_date + pd.DateOffset(months=1)
            forecast_date_str = forecast_date.strftime('%m/%Y')

            results[authority][prop_type] = {
                'mae': best_metrics['mae'],
                'mape': best_mape,
                'forecast': forecast,
                'forecast_date': forecast_date_str,
                'best_model': best_model_name,
                'best_time_window': best_time_window,
                'model_file': os.path.join(
                    model_dir,
                    f"{authority}_{prop_type}_{best_model_name}_{best_time_window if best_time_window else 'all'}_model.pkl"
                ),
                'training_date': datetime.now().strftime("%Y-%m-%d")
            }

    return flatten_forecast_data(results)