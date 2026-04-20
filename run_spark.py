"""
Wrapper to run spark_streaming.py without needing spark-submit on PATH.
Automatically configures Kafka connector package.

Usage:  python run_spark.py
"""
import subprocess
import sys
import os
import pyspark
import urllib.request

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# ───────────────────── Fix Windows Hadoop/Winutils ─────────────────────
# PySpark on Windows requires winutils.exe to work with local file paths (like checkpoints)
HADOOP_DIR = os.path.join(PROJECT_DIR, ".hadoop")
HADOOP_BIN = os.path.join(HADOOP_DIR, "bin")
WINUTILS_EXE = os.path.join(HADOOP_BIN, "winutils.exe")
HADOOP_DLL = os.path.join(HADOOP_BIN, "hadoop.dll")

if os.name == "nt":
    os.environ["HADOOP_HOME"] = HADOOP_DIR
    os.environ["PATH"] = HADOOP_BIN + os.pathsep + os.environ.get("PATH", "")
    if not os.path.exists(WINUTILS_EXE) or not os.path.exists(HADOOP_DLL):
        print("📥 Downloading winutils.exe and hadoop.dll for Windows PySpark support...")
        os.makedirs(HADOOP_BIN, exist_ok=True)
        # Using a reliable repo for Hadoop 3.3.x binaries
        urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe", WINUTILS_EXE)
        urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll", HADOOP_DLL)

# ───────────────────── Locate Spark Submit ─────────────────────
# Find spark-submit inside the pip-installed pyspark package
PYSPARK_HOME = os.path.dirname(pyspark.__file__)
SPARK_SUBMIT = os.path.join(PYSPARK_HOME, "bin", "spark-submit.cmd")

# Fallback for non-Windows
if not os.path.exists(SPARK_SUBMIT):
    SPARK_SUBMIT = os.path.join(PYSPARK_HOME, "bin", "spark-submit")

PYSPARK_VERSION = pyspark.__version__
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{PYSPARK_VERSION}"
SCRIPT = os.path.join(PROJECT_DIR, "streaming", "spark_streaming.py")

print("=" * 60)
print(f"🔧 PySpark version : {PYSPARK_VERSION}")
print(f"🔧 Kafka package   : {KAFKA_PACKAGE}")
print(f"🔧 HADOOP_HOME     : {os.environ.get('HADOOP_HOME', 'Not required')}")
print("=" * 60)

cmd = [SPARK_SUBMIT, "--packages", KAFKA_PACKAGE, SCRIPT]
sys.exit(subprocess.call(cmd))
