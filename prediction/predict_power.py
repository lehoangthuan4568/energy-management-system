"""
Energy Management System - AI Power Prediction (Batch Processing)
================================================================
Uses Holt-Winters Exponential Smoothing to forecast daily power consumption 
for the next 7 days. This lightweight approach avoids the complex C++ 
build dependencies of Prophet on Windows.

Reads directly from the UCI dataset and writes to InfluxDB measurement 
'power_predictions'.

Usage:
  python prediction/predict_power.py
"""
import os
import sys

# Force UTF-8 encoding for Windows terminals to support emojis
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
import time
import warnings
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from influxdb import InfluxDBClient

warnings.filterwarnings("ignore")

# ───────────────────── Configuration ─────────────────────
INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "localhost")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "energy_db")
FORECAST_DAYS = 7

def _find_data_file():
    """Auto-detect dataset file (handles .txt/.csv and different casing)."""
    env = os.environ.get("DATA_FILE")
    if env:
        return env
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    candidates = [
        "household_power_consumption.txt",
        "Household_power_consumption.csv",
        "household_power_consumption.csv",
        "Household_power_consumption.txt",
    ]
    for name in candidates:
        path = os.path.join(data_dir, name)
        if os.path.exists(path):
            return path
    return os.path.join(data_dir, candidates[0])

DATA_FILE = _find_data_file()


def load_dataset(filepath):
    """Load and clean the UCI power consumption dataset.
    Supports both formats:
      - UCI .txt: semicolon-separated, columns Date;Time;Global_active_power;...
      - Preprocessed .csv: comma-separated, columns datetime,Global_active_power,...
    """
    print(f"📂 Loading dataset: {os.path.basename(filepath)}")

    # Detect format by reading first line
    with open(filepath, "r") as f:
        header = f.readline().strip()

    is_csv = "," in header and ";" not in header

    if is_csv:
        print("   📄 Detected CSV format (comma-separated)")
        df = pd.read_csv(filepath, low_memory=False, na_values=["?", ""])
        df["datetime"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")
        df["Global_active_power"] = pd.to_numeric(df.iloc[:, 1], errors="coerce")
    else:
        print("   📄 Detected TXT format (semicolon-separated)")
        df = pd.read_csv(filepath, sep=";", low_memory=False, na_values=["?", ""])
        df["datetime"] = pd.to_datetime(
            df["Date"] + " " + df["Time"],
            format="%d/%m/%Y %H:%M:%S",
            errors="coerce",
        )
        df["Global_active_power"] = pd.to_numeric(df["Global_active_power"], errors="coerce")

    # Drop rows with missing values
    df = df.dropna(subset=["datetime", "Global_active_power"])

    print(f"   ✅ Loaded {len(df):,} valid records")
    print(f"   📅 Range: {df['datetime'].min()} → {df['datetime'].max()}")

    return df


def prepare_daily_data(df):
    """Aggregate minute-level data to daily total kWh."""
    df["date"] = df["datetime"].dt.date

    # Each reading is 1-minute average kW → kWh = kW * (1/60)
    daily = (
        df.groupby("date")["Global_active_power"]
        .apply(lambda x: (x * (1.0 / 60.0)).sum())  # total kWh per day
        .reset_index()
    )
    daily.columns = ["ds", "y"]
    daily["ds"] = pd.to_datetime(daily["ds"])

    # Remove days with very few readings (likely incomplete)
    min_readings_per_day = 600  # at least ~10 hours of data
    daily_counts = df.groupby("date").size().reset_index(name="count")
    daily_counts["date"] = pd.to_datetime(daily_counts["date"])
    daily = daily.merge(daily_counts, left_on="ds", right_on="date", how="left")
    daily = daily[daily["count"] >= min_readings_per_day][["ds", "y"]].reset_index(drop=True)

    # Sort strictly by date and set as index for statsmodels
    daily = daily.sort_values(by="ds").set_index("ds")
    
    # Fill any missing dates within the range with interpolation
    daily = daily.asfreq("D").interpolate(method="time")

    print(f"\n📊 Daily aggregation:")
    print(f"   Total days: {len(daily)}")
    print(f"   Avg daily kWh: {daily['y'].mean():.2f}")
    print(f"   Max daily kWh: {daily['y'].max():.2f}")

    return daily


def train_and_predict(daily_df, periods=FORECAST_DAYS):
    """Train Holt-Winters model and forecast future days."""
    print(f"\n🤖 Training Holt-Winters Exponential Smoothing model on {len(daily_df)} days of data...")

    # For daily energy consumption, weekly seasonality (7 days) is very strong.
    model = ExponentialSmoothing(
        daily_df["y"], 
        trend="add", 
        seasonal="add", 
        seasonal_periods=7
    )
    fitted_model = model.fit(optimized=True)

    # Forecast future values
    forecast_values = fitted_model.forecast(steps=periods)
    
    # Calculate simple confidence intervals based on standard deviation of residuals
    residuals = fitted_model.resid
    std_resid = residuals.std()

    print("   ✅ Model trained successfully!")
    return forecast_values.values, residuals.std()


def create_demo_predictions(forecast_values, std_resid, periods=FORECAST_DAYS):
    """Map the 7 historical forecasted values to the immediate future (Demo Mode).
    Each day of forecasted energy is mapped to 1 minute in the future on the dashboard.
    """
    now = datetime.utcnow().replace(second=0, microsecond=0)
    
    # In Demo Mode, "1 day" = "1 minute"
    future_dates = [now + timedelta(minutes=i) for i in range(1, periods + 1)]
    
    # Scale prediction values to 1.5x (to match spark streaming DEMO_KWH_MULTIPLIER)
    demo_scaled_values = forecast_values * 1.5
    demo_scaled_resid = std_resid * 1.5
    
    predictions = pd.DataFrame({
        "ds": future_dates,
        "yhat": demo_scaled_values,
        "yhat_lower": demo_scaled_values - (1.96 * demo_scaled_resid),
        "yhat_upper": demo_scaled_values + (1.96 * demo_scaled_resid)
    })
    
    # Ensure no negative predictions (physics says no negative consumption here)
    predictions["yhat"] = predictions["yhat"].clip(lower=0)
    predictions["yhat_lower"] = predictions["yhat_lower"].clip(lower=0)

    print(f"\n🔮 Forecast for next {len(predictions)} days:")
    print("-" * 55)
    print(f"   {'Date':<14} {'Predicted kWh':>14} {'Lower':>10} {'Upper':>10}")
    print("-" * 55)
    for _, row in predictions.iterrows():
        print(
            f"   {row['ds'].strftime('%Y-%m-%d'):<14}"
            f" {row['yhat']:>14.2f}"
            f" {row['yhat_lower']:>10.2f}"
            f" {row['yhat_upper']:>10.2f}"
        )

    return predictions


def write_to_influxdb(predictions):
    """Write forecast results to InfluxDB measurement 'power_predictions'."""
    print(f"\n🗄️ Writing {len(predictions)} predictions to InfluxDB...")

    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    client.create_database(INFLUXDB_DB)

    points = []
    for _, row in predictions.iterrows():
        points.append({
            "measurement": "power_predictions",
            "time": row["ds"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fields": {
                "yhat": float(row["yhat"]),
                "yhat_lower": float(row["yhat_lower"]),
                "yhat_upper": float(row["yhat_upper"]),
            },
        })

    client.write_points(points)
    client.close()
    print(f"   ✅ Written to measurement 'power_predictions' in '{INFLUXDB_DB}'")


def main():
    print("=" * 60)
    print("🔮 ENERGY MANAGEMENT SYSTEM - AI Power Prediction (Holt-Winters)")
    print("=" * 60)

    # Verify data file
    resolved = os.path.abspath(DATA_FILE)
    if not os.path.exists(resolved):
        print(f"❌ Data file not found: {resolved}")
        print("   Please check the data directory.")
        sys.exit(1)

    # Core Pipeline: Train once
    df = load_dataset(resolved)
    daily = prepare_daily_data(df)
    forecast_values, std_resid = train_and_predict(daily)
    
    print("\n" + "=" * 60)
    print("✅ Prediction pipeline ready! Entering Continuous Demo Loop...")
    print("   (Writing predictions 1-7 minutes ahead of real-time every 15s)")
    print("=" * 60)
    
    try:
        while True:
            # Create fresh predictions dynamically pinned to current real-time
            predictions = create_demo_predictions(forecast_values, std_resid)
            write_to_influxdb(predictions)
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n🛑 Stopped Prediction Demo Loop.")


if __name__ == "__main__":
    main()
