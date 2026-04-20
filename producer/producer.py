"""
Energy Management System - Kafka Producer (Data Simulator)
==========================================================
Reads UCI Household Electric Power Consumption dataset and streams to Kafka.

Features:
  - BATCH_SIZE: configurable time dilation (default 60 = 1 real sec equals 1 hour of data)
  - Anomaly trigger: Press 'A' to multiply next 5 messages by 10x for demo alerts
  - Auto-loops when dataset ends

Usage:
  python producer.py                    # default 60 msgs/sec
  set BATCH_SIZE=120 && python producer.py  # 120 msgs/sec (1 sec = 2 hours)
"""
import json
import time
import os
import sys
import threading
from datetime import datetime, timedelta

from kafka import KafkaProducer

# ───────────────────── Configuration ─────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "smart_home_power")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
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
    return os.path.join(data_dir, candidates[0])  # default for error msg

DATA_FILE = _find_data_file()

# ───────────────────── Anomaly State ─────────────────────
anomaly_remaining = 0
anomaly_lock = threading.Lock()


def _trigger_anomaly():
    """Shared logic to trigger anomaly mode."""
    global anomaly_remaining
    with anomaly_lock:
        anomaly_remaining = 5
    print("\n⚡ ANOMALY TRIGGERED! Next 5 messages will be ×10 power")


def setup_keyboard_listener():
    """
    Setup keyboard listener for anomaly trigger (demo effect).

    Cross-platform strategy (in priority order):
      1. Windows  → msvcrt  (built-in, no pip install, press 'A' without Enter)
      2. Linux/Mac → select + sys.stdin in raw mode (no pip install, press 'A' without Enter)
      3. Any OS   → 'keyboard' lib (pip install keyboard; requires sudo on Linux/Mac)
      4. Fallback → input() (universal, but requires typing 'A' + Enter)
    """
    global anomaly_remaining

    # ── Strategy 1: msvcrt (Windows built-in, no Enter needed) ──
    try:
        import msvcrt

        def _msvcrt_listener():
            while True:
                if msvcrt.kbhit():
                    key = msvcrt.getch().decode("utf-8", errors="ignore").lower()
                    if key == "a":
                        _trigger_anomaly()
                time.sleep(0.05)

        threading.Thread(target=_msvcrt_listener, daemon=True).start()
        print("🎮 Press 'A' anytime to trigger anomaly (next 5 msgs ×10)  [msvcrt]")
        return
    except ImportError:
        pass  # Not Windows

    # ── Strategy 2: select + tty (Linux/Mac, no Enter needed) ──
    try:
        import select
        import tty
        import termios

        def _unix_listener():
            old_settings = termios.tcgetattr(sys.stdin)
            try:
                tty.setcbreak(sys.stdin.fileno())
                while True:
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        key = sys.stdin.read(1).lower()
                        if key == "a":
                            _trigger_anomaly()
            except Exception:
                pass
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

        threading.Thread(target=_unix_listener, daemon=True).start()
        print("🎮 Press 'A' anytime to trigger anomaly (next 5 msgs ×10)  [tty]")
        return
    except (ImportError, Exception):
        pass  # Not Unix or stdin not a terminal

    # ── Strategy 3: 'keyboard' library (cross-platform, needs pip install) ──
    #    NOTE: On Linux/Mac, this requires running with sudo.
    try:
        import keyboard

        keyboard.add_hotkey("a", _trigger_anomaly)
        print("🎮 Press 'A' anytime to trigger anomaly (next 5 msgs ×10)  [keyboard lib]")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"⚠️  'keyboard' lib failed ({e}). Requires sudo on Linux/Mac.")

    # ── Strategy 4: Fallback — input() (universal, requires Enter key) ──
    print("🎮 Type 'A' + Enter to trigger anomaly (next 5 msgs ×10)  [fallback]")
    print("   💡 Tip: pip install keyboard  for press-'A'-only mode")

    def _input_listener():
        while True:
            try:
                cmd = input()
                if cmd.strip().upper() == "A":
                    _trigger_anomaly()
            except (EOFError, KeyboardInterrupt):
                break

    threading.Thread(target=_input_listener, daemon=True).start()


def data_generator(filepath):
    """Generator that reads dataset line by line, looping forever.
    Maps timestamps to start at datetime.now() and increments by 1 minute for each row.
    Supports two formats:
      - UCI .txt: semicolon-separated, columns Date;Time;Global_active_power;...
      - Preprocessed .csv: comma-separated, columns datetime,Global_active_power,...
    """
    loop_count = 0
    # Sử dụng thời gian thực tế thay vì thời gian ảo tích lũy
    
    while True:
        loop_count += 1
        if loop_count > 1:
            print(f"\n🔄 Loop #{loop_count}: Restarting dataset from beginning...")
        with open(filepath, "r") as f:
            header = f.readline().strip()

            # Detect format from header
            is_csv = "," in header and ";" not in header

            if is_csv:
                # CSV format: datetime,Global_active_power,...
                print("   📄 Detected CSV format (comma-separated)" if loop_count == 1 else "", end="")
                for line in f:
                    line = line.strip()
                    if not line or "?" in line:
                        continue
                    parts = line.split(",")
                    if len(parts) < 2:
                        continue
                    try:
                        kw = float(parts[1])
                        sub1 = float(parts[5]) if len(parts) > 5 and parts[5] else 0.0
                        sub2 = float(parts[6]) if len(parts) > 6 and parts[6] else 0.0
                        sub3 = float(parts[7]) if len(parts) > 7 and parts[7] else 0.0
                        
                        event_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        
                        yield {"event_time": event_time, "kw": round(kw, 4), "sub1": round(sub1, 2), "sub2": round(sub2, 2), "sub3": round(sub3, 2)}
                    except (ValueError, IndexError):
                        continue
            else:
                # UCI .txt format: Date;Time;Global_active_power;...
                print("   📄 Detected TXT format (semicolon-separated)" if loop_count == 1 else "", end="")
                for line in f:
                    line = line.strip()
                    if not line or "?" in line:
                        continue
                    parts = line.split(";")
                    if len(parts) < 3:
                        continue
                    try:
                        kw = float(parts[2])
                        sub1 = float(parts[6]) if len(parts) > 6 and parts[6] else 0.0
                        sub2 = float(parts[7]) if len(parts) > 7 and parts[7] else 0.0
                        sub3 = float(parts[8]) if len(parts) > 8 and parts[8] else 0.0
                        
                        event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        yield {"event_time": event_time, "kw": round(kw, 4), "sub1": round(sub1, 2), "sub2": round(sub2, 2), "sub3": round(sub3, 2)}
                    except (ValueError, IndexError):
                        continue


def main():
    global anomaly_remaining

    print("=" * 60)
    print("⚡ ENERGY MANAGEMENT SYSTEM - Data Simulator (Producer)")
    print("=" * 60)
    print(f"  📡 Kafka Server  : {KAFKA_BOOTSTRAP}")
    print(f"  📋 Topic         : {KAFKA_TOPIC}")
    print(f"  ⏩ Batch Size     : {BATCH_SIZE} msgs/sec")
    print(f"  📂 Data File     : {os.path.basename(DATA_FILE)}")
    print("=" * 60)

    # Verify data file
    resolved = os.path.abspath(DATA_FILE)
    if not os.path.exists(resolved):
        print(f"❌ Data file not found: {resolved}")
        print("   Run:  python download_data.py")
        sys.exit(1)

    # Setup anomaly trigger
    setup_keyboard_listener()

    # Create Kafka producer
    print("🔌 Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=10,
        api_version=(2, 0, 0),
    )
    print(f"✅ Connected to Kafka\n")

    gen = data_generator(resolved)
    total_sent = 0

    try:
        while True:
            batch_start = time.time()
            last_msg = None
            suffix = ""

            for _ in range(BATCH_SIZE):
                msg = next(gen)

                # Apply anomaly multiplier
                with anomaly_lock:
                    if anomaly_remaining > 0:
                        msg["kw"] = round(msg["kw"] * 10, 3)
                        msg["sub1"] = round(msg["sub1"] * 10, 3)
                        msg["sub2"] = round(msg["sub2"] * 10, 3)
                        msg["sub3"] = round(msg["sub3"] * 10, 3)
                        anomaly_remaining -= 1
                        suffix = f" 🔴 ANOMALY ({anomaly_remaining} left)"
                    else:
                        suffix = ""

                producer.send(KAFKA_TOPIC, value=msg)
                total_sent += 1
                last_msg = msg

            producer.flush()

            # Progress display
            if last_msg:
                print(
                    f"\r📤 Sent: {total_sent:>10,} | "
                    f"Time: {last_msg['event_time']} | "
                    f"kW: {last_msg['kw']:>8.3f}{suffix}     ",
                    end="",
                    flush=True,
                )

            # Maintain 1-second cadence
            elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopped. Total messages sent: {total_sent:,}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
