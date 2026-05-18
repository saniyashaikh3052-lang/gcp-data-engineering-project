import os
import json
import time
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv
from google.cloud import storage


# ─── Load Environment Variables ──────────────────────────────────────────────
load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")


# ─── TMDB Configuration ──────────────────────────────────────────────────────
BASE_URL = "https://api.themoviedb.org/3/trending/all/day"
MAX_PAGES = 5


# ─── Local Bronze Layer Path ─────────────────────────────────────────────────
OUTPUT_DIR = "data/raw"
BUCKET_NAME = "entertainment-bronze-layer-2026"

GCS_FOLDER = "tmdb_trending_all_day/raw"

# ─── Logging Configuration ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)


# ─── Fetch Single API Page ───────────────────────────────────────────────────
def fetch_page(page: int) -> dict | None:
    """
    Fetch a single page from TMDB trending API.
    Returns parsed JSON response.
    """

    params = {
        "api_key": API_KEY,
        "page": page
    }

    try:

        response = requests.get(
            BASE_URL,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        return response.json()

    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error on page {page}: {e}")

    except requests.exceptions.ConnectionError:
        log.error(f"Connection failed on page {page}")

    except requests.exceptions.Timeout:
        log.error(f"Timeout occurred on page {page}")

    return None


# ─── Save Raw JSON Locally ───────────────────────────────────────────────────
def save_raw(data: list[dict], snapshot_date: str) -> str:
    """
    Save raw extracted data locally.
    """

    output_path = os.path.join(
        OUTPUT_DIR,
        snapshot_date
    )

    os.makedirs(output_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = os.path.join(
        output_path,
        f"trending_raw_{timestamp}.json"
    )

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False
        )

    log.info(f"Saved {len(data)} records → {filename}")

    return filename

def upload_to_gcs(local_file_path: str):
    """
    Upload local JSON file to GCS Bronze layer.
    """

    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)

    blob_name = f"{GCS_FOLDER}/{os.path.basename(local_file_path)}"

    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_file_path)

    log.info(f"Uploaded file to GCS → gs://{BUCKET_NAME}/{blob_name}")


# ─── Extract Multiple Pages ──────────────────────────────────────────────────
def extract(max_pages: int = MAX_PAGES):
    """
    Extract TMDB trending data across multiple pages.
    Adds ingestion metadata to each record.
    """

    all_results = []

    snapshot_date = datetime.today().strftime("%Y-%m-%d")

    for page in range(1, max_pages + 1):

        log.info(f"Fetching page {page}")

        data = fetch_page(page)

        if data:

            results = data.get("results", [])

            for record in results:

                record["_snapshot_date"] = snapshot_date
                record["_source_page"] = page

            all_results.extend(results)

        time.sleep(0.25)

    log.info(f"Total records extracted: {len(all_results)}")

    return all_results, snapshot_date


# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":

    log.info("TMDB extraction pipeline started")

    records, snapshot_date = extract()

    local_file = save_raw(records, snapshot_date)

    upload_to_gcs(local_file)

    log.info("TMDB extraction pipeline completed")