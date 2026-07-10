"""
Download the Olist Brazilian E-Commerce dataset from Kaggle.

Requirements:
  - Kaggle account at https://www.kaggle.com
  - API token at ~/.kaggle/kaggle.json  (Windows: %USERPROFILE%\.kaggle\kaggle.json)

Usage:
  python scripts/download_dataset.py
"""

import os
import sys
from pathlib import Path


def check_kaggle_credentials():
    """Check that kaggle.json exists at the expected location."""
    home = Path.home()
    kaggle_json = home / ".kaggle" / "kaggle.json"
    if not kaggle_json.exists():
        print("ERROR: Kaggle API credentials not found.")
        print(f"Expected location: {kaggle_json}")
        print("\nTo fix:")
        print("  1. Go to https://www.kaggle.com/settings")
        print("  2. Scroll to 'API' and click 'Create New Token'")
        print(f"  3. Move the downloaded kaggle.json to: {kaggle_json}")
        sys.exit(1)


def download_dataset():
    check_kaggle_credentials()

    try:
        import kaggle
    except ImportError:
        print("Installing kaggle package...")
        os.system(f"{sys.executable} -m pip install kaggle")
        import kaggle

    output_dir = Path("processing/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = "olistbr/brazilian-ecommerce"
    print(f"Downloading dataset: {dataset}")
    print(f"Output directory:    {output_dir.absolute()}\n")

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        dataset,
        path=str(output_dir),
        unzip=True,
        quiet=False,
    )

    print("\n✅ Download complete. Files:")
    csv_files = sorted(output_dir.glob("*.csv"))
    if not csv_files:
        print("  ⚠ No CSV files found — check the output directory.")
    for f in csv_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name:<55} {size_mb:6.1f} MB")


if __name__ == "__main__":
    download_dataset()
