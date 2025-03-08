import os
import requests
import pandas as pd
from io import BytesIO
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# S3 Public URL
S3_BASE_URL = "https://tamedia-interview-exercise.s3.eu-central-1.amazonaws.com/de_interview/"
SUBSCRIPTION_FILE = "current_subscription/subscription_current_20230426.csv"
SESSION_FILES = [
    "ga_session_details/ga_session_details_20230301.csv",
    "ga_session_details/ga_session_details_20230302.csv"
]

# Define the directory where metadata files will be stored
data_dir = "/Users/thamyres.vasconcellos/Desktop/tx_services/data"
os.makedirs(data_dir, exist_ok=True)

def create_metadata_files():
    """Generate metadata files for DBT external table references."""
    metadata = {
        "subscriptions": {
            "source": S3_BASE_URL + SUBSCRIPTION_FILE,
            "format": "csv"
        },
        "sessions": {
            "source": [S3_BASE_URL + file for file in SESSION_FILES],
            "format": "csv"
        }
    }
    metadata_file = os.path.join(data_dir, "external_table_metadata.json")
    with open(metadata_file, "w") as f:
        import json
        json.dump(metadata, f, indent=4)
    print(f"Metadata file saved to {metadata_file}")

def main():
    """Main function to set up external tables for DBT."""
    create_metadata_files()
    print("Metadata for DBT external tables created successfully!")

if __name__ == "__main__":
    main()
