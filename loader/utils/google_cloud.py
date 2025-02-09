import io
import uuid
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
from pandas import DataFrame


def upload_dataframe_to_gcs(df: DataFrame, bucket_name: str, base_path: str) -> str:
    """
    Uploads a Pandas DataFrame to GCS in Parquet format without saving locally.

    :param df: Pandas DataFrame to upload
    :param bucket_name: Name of the GCS bucket
    :param base_path: Base path within the bucket to store the file
    :return: GCS path where the file was saved
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    dt = datetime.now()

    # Generate a unique filename using a timestamp + UUID
    unique_id = uuid.uuid4().hex[:8]  # Shortened UUID for readability
    file_name = f"data_{dt.strftime('%Y%m%d_%H%M%S')}_{unique_id}.parquet"

    gcs_path = (
        f"{base_path}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{file_name}"
    )

    # Using with context for better resource management
    with io.BytesIO() as buffer:
        df.to_parquet(buffer, engine="pyarrow")
        buffer.seek(0)

        blob = bucket.blob(gcs_path)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

    return gcs_path


def list_new_parquet_files(
    bucket_name: str, base_path: str, dataset_id: str, manifest_table: str
) -> list[str]:
    """
    Returns a list of new Parquet files not yet processed in BigQuery

    :param bucket_name: Name of the GCS bucket
    :param base_path: Base path within the bucket to search for files
    :param dataset_id: BigQuery dataset ID
    :param manifest_table: BigQuery table to track processed files
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=base_path)

    # Fetch already processed files from BigQuery
    bq_client = bigquery.Client()
    query = f"SELECT file_name FROM `{dataset_id}.{manifest_table}`"
    processed_files = {row.file_name for row in bq_client.query(query)}

    # Identify new files
    new_files = [blob.name for blob in blobs if blob.name not in processed_files]
    return new_files


def load_parquet_to_bigquery(
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    base_path: str,
    manifest_table: str,
) -> int:
    """
    Loads only new Parquet files from GCS into BigQuery and updates manifest

    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    :param bucket_name: Name of the GCS bucket
    :param new_files: List of new Parquet files to load
    :param manifest_table: BigQuery table to track processed files

    :return: Number of files loaded
    """
    bq_client = bigquery.Client()

    # Get the list of new Parquet files
    new_files = list_new_parquet_files(
        bucket_name, base_path, dataset_id, manifest_table
    )
    if not new_files:
        print("No new files to load.")
        return

    # Prepare URIs for BigQuery loading
    uris = [f"gs://{bucket_name}/{file}" for file in new_files]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],  # Allow new columns found in the Parquet files
    )
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    load_job = bq_client.load_table_from_uri(uris, table_ref, job_config=job_config)
    load_job.result()

    # Insert all processed files into manifest table
    file_values = ", ".join([f"('{file}', CURRENT_TIMESTAMP())" for file in new_files])
    manifest_insert_query = f"""
    INSERT INTO `{dataset_id}.{manifest_table}` (file_name, processed_at)
    VALUES {file_values}
    """
    bq_client.query(manifest_insert_query).result()

    return len(new_files)
