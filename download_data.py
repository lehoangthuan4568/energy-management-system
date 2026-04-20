"""
Auto-download UCI Household Electric Power Consumption dataset.
Downloads zip from UCI repository, extracts to data/ folder.
"""
import urllib.request
import zipfile
import os
import sys

URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
ZIP_PATH = os.path.join(DATA_DIR, "household_power_consumption.zip")
TXT_PATH = os.path.join(DATA_DIR, "household_power_consumption.txt")


def download():
    if os.path.exists(TXT_PATH):
        size_mb = os.path.getsize(TXT_PATH) / (1024 * 1024)
        print(f"✅ Dataset already exists: {TXT_PATH} ({size_mb:.1f} MB)")
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"⬇️  Downloading dataset from UCI Repository...")
    print(f"    URL: {URL}")

    def progress_hook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 / total_size)
            bar = "█" * int(pct // 2) + "░" * (50 - int(pct // 2))
            print(f"\r    [{bar}] {pct:.1f}%", end="", flush=True)

    urllib.request.urlretrieve(URL, ZIP_PATH, reporthook=progress_hook)
    print()

    print(f"📦 Extracting archive...")
    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        z.extractall(DATA_DIR)

    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)

    if os.path.exists(TXT_PATH):
        size_mb = os.path.getsize(TXT_PATH) / (1024 * 1024)
        print(f"✅ Dataset ready: {TXT_PATH} ({size_mb:.1f} MB)")
    else:
        print(f"❌ Extraction failed. Expected file not found: {TXT_PATH}")
        sys.exit(1)


if __name__ == "__main__":
    download()
