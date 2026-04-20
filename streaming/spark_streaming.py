"""
Energy Management System - PySpark Structured Streaming
=======================================================
Consumes real-time power data from Kafka, computes:
  1. Cumulative kWh per month (event-time based)
  2. Electricity bill using EVN 5-tier pricing
  3. High-power alerts (kw > threshold)

Writes results to InfluxDB 1.8 via foreachBatch.

Run with:
  1. Check your PySpark version:  python -m pip show pyspark
  2. Replace <VERSION> below:     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<VERSION> streaming/spark_streaming.py
  Example (PySpark 3.5.0):        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming/spark_streaming.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime
from influxdb import InfluxDBClient

# ═══════════════════ Configuration ═══════════════════
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "smart_home_power"
INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_DB = "energy_db"
ALERT_THRESHOLD_KW = 4.0

# ═══════════════════ EVN 5-Tier Pricing (from Oct 2024) ═══════════════════
# Based on QĐ 14/2025 structure with average retail price 2,103.11 đ/kWh
EVN_TIERS = [
    (100,         1893),   # Bậc 1:   0–100   kWh → 1,893 đ/kWh
    (200,         2271),   # Bậc 2: 101–200   kWh → 2,271 đ/kWh
    (400,         2860),   # Bậc 3: 201–400   kWh → 2,860 đ/kWh
    (700,         3407),   # Bậc 4: 401–700   kWh → 3,407 đ/kWh
    (float("inf"), 3786),  # Bậc 5: 701+      kWh → 3,786 đ/kWh
]


def calculate_evn_cost(kwh):
    """Calculate electricity bill using EVN 5-tier progressive pricing."""
    if kwh <= 0:
        return 0.0
    total = 0.0
    remaining = kwh
    prev_limit = 0
    for limit, price in EVN_TIERS:
        tier_kwh = min(remaining, limit - prev_limit)
        if tier_kwh <= 0:
            break
        total += tier_kwh * price
        remaining -= tier_kwh
        prev_limit = limit
    return round(total, 0)


# ═══════════════════ State Management ═══════════════════
monthly_kwh_state = {}
last_processed_month = None   # tracks the current month for rollover detection


def recover_state():
    """
    Recover cumulative kWh from InfluxDB on Spark restart.
    Queries MAX(kwh_cumulative) for the last active month.
    """
    global monthly_kwh_state
    try:
        client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)

        # Step 1: Find the most recent event
        result = client.query("SELECT LAST(kwh_cumulative) FROM power_usage")
        points = list(result.get_points())

        if points and points[0].get("last") is not None:
            last_time_str = points[0]["time"]

            # Parse timestamp (handle both 'Z' and '+00:00' formats)
            clean_ts = last_time_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_ts)
            month_key = f"demo-{dt.hour}-{dt.minute // 3}"

            # Step 2: Query MAX cumulative for that specific month
            month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if dt.month == 12:
                next_month = month_start.replace(year=dt.year + 1, month=1)
            else:
                next_month = month_start.replace(month=dt.month + 1)

            query = (
                f"SELECT MAX(kwh_cumulative) FROM power_usage "
                f"WHERE time >= '{month_start.strftime('%Y-%m-%dT%H:%M:%SZ')}' "
                f"AND time < '{next_month.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
            )
            result2 = client.query(query)
            pts = list(result2.get_points())

            if pts and pts[0].get("max") is not None:
                monthly_kwh_state[month_key] = float(pts[0]["max"])
                cost = calculate_evn_cost(monthly_kwh_state[month_key])
                print(
                    f"[OK] State recovered: {month_key} -> "
                    f"{monthly_kwh_state[month_key]:,.2f} kWh, "
                    f"{cost:,.0f} VND"
                )
        else:
            print("[INFO] No previous state in InfluxDB. Starting fresh.")

        client.close()
    except Exception as e:
        print(f"[WARN] State recovery failed (starting fresh): {e}")


def write_to_influxdb(batch_df, batch_id):
    """foreachBatch sink: process each micro-batch and write to InfluxDB."""
    global monthly_kwh_state, last_processed_month

    if batch_df.isEmpty():
        return

    # Order by event_time to ensure correct cumulative calculation
    rows = batch_df.orderBy("event_time").collect()

    usage_points = []
    alert_points = []

    for row in rows:
        event_time = row["event_time"]   # pyspark Timestamp → python datetime
        kw = float(row["kw"])
        kwh = float(row["kwh"])

        # CHU KỲ DEMO: Hóa đơn được reset mỗi 3 phút ngoài đời thực để người xem thấy sự thay đổi.
        month_key = f"demo-{event_time.hour}-{event_time.minute // 3}"

        # ── Month Rollover Detection ──
        # When event_time (now a shifted virtual time) crosses into a new month, reset cumulative to 0.
        # This prevents bills from accumulating infinitely across months.
        if month_key not in monthly_kwh_state:
            monthly_kwh_state[month_key] = 0.0
            if last_processed_month is not None and month_key != last_processed_month:
                prev_kwh = monthly_kwh_state.get(last_processed_month, 0)
                prev_vnd = calculate_evn_cost(prev_kwh)
                print(
                    f"\n[ROLLOVER] MONTH ROLLOVER: {last_processed_month} -> {month_key}"
                    f"  |  Final bill: {prev_kwh:,.2f} kWh = {prev_vnd:,.0f} VND"
                    f"  |  New month starts at 0 kWh"
                )
        last_processed_month = month_key

        monthly_kwh_state[month_key] += kwh

        cumulative_kwh = monthly_kwh_state[month_key]
        cumulative_vnd = calculate_evn_cost(cumulative_kwh)

        ts = event_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # ── 3 Tier Alert Logic ──
        if kw < 3.0:
            alert_level = "INFO"
            suggestion = "Hệ thống ổn định."
        elif 3.0 <= kw < 4.0:
            alert_level = "WARNING"
            suggestion = "Tiêu thụ cao, cân nhắc tắt bớt thiết bị."
        else:
            alert_level = "CRITICAL"
            suggestion = "NGUY HIỂM! Quá tải hệ thống, hãy kiểm tra ngay!"

        # Calculate percentages
        total_wh = kw * 1000 / 60
        sub1_raw = getattr(row, "sub1", 0.0) or 0.0
        sub2_raw = getattr(row, "sub2", 0.0) or 0.0
        sub3_raw = getattr(row, "sub3", 0.0) or 0.0
        sub1_pct = (sub1_raw / total_wh) * 100 if total_wh > 0 else 0.0
        sub2_pct = (sub2_raw / total_wh) * 100 if total_wh > 0 else 0.0
        sub3_pct = (sub3_raw / total_wh) * 100 if total_wh > 0 else 0.0

        # ── power_usage measurement ──
        usage_points.append({
            "measurement": "power_usage",
            "time": ts,
            "tags": {
                "alert_level": alert_level
            },
            "fields": {
                "kw": kw,
                "kwh_cumulative": cumulative_kwh,
                "vnd_cumulative": cumulative_vnd,
                "sub1_pct": sub1_pct,
                "sub2_pct": sub2_pct,
                "sub3_pct": sub3_pct,
                "suggestion": suggestion,
            },
        })

        # ── monthly_billing measurement ──
        usage_points.append({
            "measurement": "monthly_billing",
            "time": ts,
            "tags": {
                "month_key": month_key
            },
            "fields": {
                "kwh": cumulative_kwh,
                "vnd": cumulative_vnd
            }
        })

    # Write batch to InfluxDB
    try:
        client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)

        if usage_points:
            client.write_points(usage_points)
            
        alert_count = sum(1 for p in usage_points if p["measurement"] == "power_usage" and p["tags"]["alert_level"] == "CRITICAL")
        if alert_count > 0:
            print(f"\n[ALERT] {alert_count} CRITICAL events in batch #{batch_id}!")

        # Progress log
        if usage_points:
            # We filter only power_usage for the progress log
            power_usage_pts = [p for p in usage_points if p["measurement"] == "power_usage"]
            if power_usage_pts:
                last = power_usage_pts[-1]
                print(
                    f"\r[DATA] Batch #{batch_id:>4} | "
                    f"{len(power_usage_pts):>4} pts | "
                    f"Time: {last['time'][:19]} | "
                    f"kWh: {last['fields']['kwh_cumulative']:>10,.2f} | "
                    f"VND: {last['fields']['vnd_cumulative']:>14,.0f}",
                    end="",
                    flush=True,
                )

        client.close()
    except Exception as e:
        print(f"\n[ERROR] InfluxDB write error (batch #{batch_id}): {e}")


# ═══════════════════ Main ═══════════════════
def main():
    print("=" * 65)
    print(" ENERGY MANAGEMENT SYSTEM - Spark Streaming Processor")
    print("=" * 65)

    spark = (
        SparkSession.builder
        .appName("EMS_StreamProcessor")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"   Kafka    : {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    print(f"   InfluxDB : {INFLUXDB_HOST}:{INFLUXDB_PORT}/{INFLUXDB_DB}")
    print(f"   Alert    : >{ALERT_THRESHOLD_KW} kW")
    print("-" * 65)

    # Recover state from InfluxDB
    recover_state()

    # JSON schema for Kafka messages
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("kw", DoubleType(), True),
        StructField("sub1", DoubleType(), True),
        StructField("sub2", DoubleType(), True),
        StructField("sub3", DoubleType(), True),
    ])

    # Read stream from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON → transform
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("kw", col("kw").cast("double"))
        .withColumn("kwh", col("kw") * 1.5)   # Demo mode: Tăng tốc số kWh để hóa đơn nhanh chóng vượt các Bậc điện lực (EVN)
    )

    # Start streaming with foreachBatch
    query = (
        parsed_df.writeStream
        .foreachBatch(write_to_influxdb)
        .option("checkpointLocation", "./checkpoints/spark_energy")
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("\n[START] Streaming started! Waiting for data from Kafka...\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
