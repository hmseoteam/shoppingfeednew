import os
import gzip
import logging
import paramiko
import pandas as pd
from google.cloud import bigquery

# ---------------- CONFIG ---------------- #
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_DIR = os.getenv("SFTP_DIR", "/incoming")

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "shopping_feeds_new")
BQ_TABLE = os.getenv("BQ_TABLE", "stock_feed_v2")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 100000))

REQUIRED_COLUMNS = [
    "product_id", "sku", "stock", "price", "currency"
]

logging.basicConfig(level=logging.INFO)


# ---------------- VALIDATION ---------------- #
def validate_config():
    required = [SFTP_HOST, SFTP_USER, SFTP_PASS, BQ_PROJECT]
    if not all(required):
        raise Exception("Missing required environment variables")


def validate_schema(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise Exception(f"Schema mismatch. Missing columns: {missing}")


def validate_data(df):
    if df.empty:
        raise Exception("Empty dataset detected")


# ---------------- SFTP ---------------- #
def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logging.info("Connected to SFTP")
    return sftp, transport


def get_latest_feed(sftp):
    files = sftp.listdir(SFTP_DIR)
    logging.info(f"Files in SFTP: {files}")

    candidates = [f for f in files if f.endswith(".tsv.gz")]
    valid_files = [f for f in candidates if "instagram" not in f.lower()]

    if not valid_files:
        raise Exception("No valid feed found")

    latest_file = max(
        valid_files,
        key=lambda f: sftp.stat(f"{SFTP_DIR}/{f}").st_mtime
    )

    logging.info(f"Selected file: {latest_file}")
    return f"{SFTP_DIR}/{latest_file}"


# ---------------- TRANSFORM ---------------- #
def transform(df):
    df.columns = [c.strip().lower() for c in df.columns]

    validate_schema(df)
    validate_data(df)

    df = df.drop_duplicates()

    df["ingestion_time"] = pd.Timestamp.utcnow()

    return df


# ---------------- BIGQUERY ---------------- #
def load_to_bq(client, df):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logging.info(f"Loaded {len(df)} rows into {table_id}")


# ---------------- MAIN ---------------- #
def main():
    validate_config()

    sftp, transport = None, None
    bq_client = bigquery.Client()

    total_rows = 0

    try:
        sftp, transport = connect_sftp()
        file_path = get_latest_feed(sftp)

        with sftp.open(file_path, "rb") as remote_file:
            with gzip.open(remote_file, "rt") as gz_file:

                reader = pd.read_csv(
                    gz_file,
                    sep="\t",
                    chunksize=CHUNK_SIZE
                )

                for i, chunk in enumerate(reader):
                    logging.info(f"Processing chunk {i}")

                    transformed = transform(chunk)

                    load_to_bq(bq_client, transformed)

                    total_rows += len(transformed)

        logging.info(f"Total rows processed: {total_rows}")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        logging.info("SFTP connection closed")


# ---------------- ENTRYPOINT ---------------- #
if __name__ == "__main__":
    main()
