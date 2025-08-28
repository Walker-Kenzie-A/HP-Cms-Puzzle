import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from datetime import datetime

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
# Define a checkpoint file to store last modified dates
METADATA_FILE = "metadata.json"
OUTPUT_DIR = "processed_csv"

def to_snake_case(name):
# Convert to lowercase, replace non-alphanumeric with _, remove consecutive _
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name.lower())
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

# Load existing metadata or initialize empty
def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}

# Save updated metadata to json file
def save_metadata(metadata):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=4)

# Read CSV, process columns, and save locally to OUTPUT_DIR
def process_csv(url, identifier, title):
    try:
        #Creates a DataFrame from the CSV URL
        df = pd.read_csv(url)
        # Convert columns to snake_case
        new_columns = {col: to_snake_case(col) for col in df.columns}
        df = df.rename(columns=new_columns)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # Create a filename from title and identifier for readability and uniqueness
        readable_title = to_snake_case(title)
        output_path = os.path.join(OUTPUT_DIR, f"{readable_title}-{identifier}.csv")
        df.to_csv(output_path, index=False)
        print(f"Processed and saved: {output_path}")
        return True
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return False

def main():
    # Load existing metadata
    metadata = load_metadata()
    # Fetch dataset list from metastore
    response = requests.get(METASTORE_URL)
    response.raise_for_status()
    # Convert response to JSON
    datasets = response.json()
    # Filter datasets for "Hospitals" theme and check for updates
    to_download = []
    # Loop through datasets to find relevant ones
    for dataset in datasets:
        themes = dataset.get("theme", [])
        if any("Hospitals" in theme for theme in themes):
            # Get identifier, title, and modified date
            identifier = dataset["identifier"]
            title = dataset.get("title", "untitled")
            modified_str = dataset.get("modified")
            # Check if modified date is newer than stored metadata -Assuming date format is YYYY-MM-DD
            if modified_str:
                try:
                    modified_date = datetime.strptime(modified_str, "%Y-%m-%d").date()
                    # Compare with stored metadata
                    last_modified_str = metadata.get(identifier)
                    if last_modified_str:
                        # Parse last modified date from metadata
                        last_modified_date = datetime.strptime(last_modified_str, "%Y-%m-%d").date()
                        # Skip if not modified
                        if modified_date <= last_modified_date:
                            continue
                except ValueError:
                    print(f"Skipping dataset with invalid date format: {modified_str}")
                    continue
            else: # If no modified date, skip
                continue
            # Find CSV distribution URL and add to download list
            for dist in dataset.get("distribution", []):
                if dist.get("mediaType") == "text/csv":
                    url = dist["downloadURL"]
                    to_download.append((url, identifier, modified_date, title))
                    break  # Assume one primary CSV per dataset

    if not to_download:
        print("No new datasets to download.")
        return

    # Process downloads asynchronously
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_csv, url, identifier, title) 
                  for url, identifier, _, title in to_download]
        for future in as_completed(futures):
            future.result()

    # Update the metadata with the latest modified date
    for _, identifier, modified_date, _ in to_download:
        metadata[identifier] = modified_date.isoformat()
    save_metadata(metadata)

if __name__ == "__main__":
    main()