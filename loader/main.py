import pandas as pd
import signal
import sys
import argparse
from dotenv import load_dotenv
from os import getenv
from http_client.streams.discovery import EventsStream
from utils import (
    filter_dicts,
    upload_dataframe_to_gcs,
    load_latest_timestamp,
    save_latest_timestamp,
    list_new_parquet_files,
    load_parquet_to_bigquery,
    process_dataframe,
    event_keys,
)
from logging import getLogger, basicConfig


load_dotenv()

# Configure logging
basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

latest_timestamp = None


def signal_handler(sig, frame):
    logger.info("Exiting...")
    if latest_timestamp:
        save_latest_timestamp(latest_timestamp, snapshots_path)
    sys.exit(0)


def main(
    config: dict[str, str], skip_extraction: bool = False, skip_loading: bool = False
):
    global latest_timestamp

    if not skip_extraction:
        logger.info(f"Starting extraction from {config['params']['startDateTime']}")
        events_stream = EventsStream(config=config)

        for response in events_stream.read_pages():
            data = response.json()
            if data["page"]["totalElements"] == 0:
                logger.info("No events found.")
                break
            events = response.json().get("_embedded").get("events")
            # Apply preprocessing to ensure consistency
            df = process_dataframe(pd.json_normalize(filter_dicts(events, event_keys)))
            file_path = upload_dataframe_to_gcs(
                df, getenv("CLOUD_STORAGE_BUCKET"), "events"
            )
            logger.info(f"Uploaded {file_path}.")

            latest_timestamp = df.iloc[-1]["dates_start_dateTime"]

        logger.info("Loading available data to BigQuery.")
        save_latest_timestamp(
            latest_timestamp or config["params"]["startDateTime"],
            snapshots_path,
        )
    else:
        logger.info("Skipping extraction.")

    if not skip_loading:
        loaded_files = load_parquet_to_bigquery(
            "ticketmaster",
            "raw_events",
            getenv("CLOUD_STORAGE_BUCKET"),
            "events",
            "raw_storage_manifest_table",
        )
        logger.info(f"Loaded {loaded_files} files to BigQuery.")
    else:
        logger.info("Skipping loading.")


if __name__ == "__main__":
    # Argument parser setup
    parser = argparse.ArgumentParser(description="Ticketmaster data loader")
    parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip the extraction process",
    )
    parser.add_argument(
        "--skip-loading",
        action="store_true",
        help="Skip the loading process",
    )
    args = parser.parse_args()

    snapshots_path = "./loader/data/latest_timestamp.json"
    config = {
        "apikey": getenv("TICKETMASTER_API_KEY"),
        "params": {  # This parameters will extract all events from 2020-01-01 to the current date
            "size": 200,
            "sort": "date,name,asc",
            "startDateTime": load_latest_timestamp(
                snapshots_path, "2020-01-01T00:00:00Z"
            ),  # Params can be added based on the API documentation
        },
    }
    logger = getLogger("Main")
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Initializing extraction and loading process.")
    main(
        config=config,
        skip_extraction=args.skip_extraction,
        skip_loading=args.skip_loading,
    )
    logger.info("Process completed.")
