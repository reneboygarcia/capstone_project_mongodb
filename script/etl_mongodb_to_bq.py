# Import
import pandas as pd
import hashlib
from pathlib import Path
import os
from datetime import datetime
from pymongo import MongoClient

# Set-up google credentials
import time
from google.cloud import bigquery

# Display
pd.set_option("display.max_columns", None)
pd.set_option("expand_frame_repr", False)
# Prefect
from prefect_gcp import GcpCredentials
from prefect import task, flow
from prefect.blocks.system import Secret
from prefect_gcp.cloud_storage import GcsBucket

print("Setup Complete")

# ## Extract Sample AirBnb data from MongoDB


# Define a function to load the MongoClient
# Although the URI string for the Sample Dataset is public
# I will implement this code to obscure the access key.
@flow(log_prints=True, name="get-mongo-client")
def get_mongo_client():
    secret_block = Secret.load("sample-mongodb-uri")
    mongo_uri = secret_block.get()
    mongo_client = MongoClient(mongo_uri)
    return mongo_client


# Extract `sample_airbnb` from mongodb sample_database
# # Function extract-collection
# Define a function to extract all the documents inside the collection
@flow(log_prints=True, name="extract-collection-from-mongodb")
def extract_collection(db_name: str, coll_name: str) -> pd.DataFrame:
    mongo_client = get_mongo_client()
    db = mongo_client[db_name]
    db_coll = db.get_collection(coll_name)
    docs = db_coll.find({})
    df = pd.json_normalize(docs, sep="_")
    return df


# Define a function to tweak the Dataframe to different dtypes so it can be processed
@task(log_prints=True, name="tweak-df")
def tweak_df(df: pd.DataFrame) -> pd.DataFrame:
    # This is a challenge when doing pipeline, we are getting error
    # when uploading to GCS or BQ. Thats why we have to convert it.

    # This will convert Decimal128 to float and object to string
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
        elif pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_bool_dtype(
            df[col]
        ):
            df[col] = pd.to_numeric(df[col], errors="ignore", downcast="float")

    # This will convert float to int
    df["accommodates"] = df["accommodates"].astype(int)
    df["number_of_reviews"] = df["number_of_reviews"].astype(int)
    df["host_host_listings_count"] = df["host_host_listings_count"].astype(int)
    df["host_host_total_listings_count"] = df["host_host_total_listings_count"].astype(
        int
    )
    df["availability_availability_30"] = df["availability_availability_30"].astype(int)
    df["availability_availability_60"] = df["availability_availability_60"].astype(int)
    df["availability_availability_90"] = df["availability_availability_90"].astype(int)
    df["availability_availability_365"] = df["availability_availability_365"].astype(
        int
    )

    # This will convert object/string to float
    df["price"] = df["price"].astype(float)
    df["security_deposit"] = df["security_deposit"].astype(float)
    df["cleaning_fee"] = df["cleaning_fee"].astype(float)
    df["extra_people"] = df["extra_people"].astype(float)
    df["weekly_price"] = df["weekly_price"].astype(float)
    df["monthly_price"] = df["weekly_price"].astype(float)
    df["host_host_response_rate"] = df["host_host_response_rate"].astype(float)

    # Add a unique id and inserted at before uploading to GCS and BQ
    # We can use this as reference we run deduplicate
    df = df.assign(
        _record_hash=list(
            map(lambda x: hashlib.sha1(str(x).encode("utf-8")).hexdigest(), df["_id"])
        ),
        _bq_inserted_at=datetime.now(),
    )

    return df


# # Function write-to-gcs


# Convert camelCase to split_case
def convert_to_split_case(text: str) -> str:
    mod_string = list(map(lambda x: "_" + x if x.isupper() else x, text))
    join_string = "".join(mod_string).lower().rstrip("_")
    return join_string


@task(log_prints=True, name="write-to-gcs", retries=3)
def write_to_gcs(df: pd.DataFrame, db_name: str, coll_name: str) -> None:
    directory = Path(f"{db_name}")
    converted_coll_name = convert_to_split_case(coll_name)
    path_name = directory / f"{converted_coll_name}.parquet.snappy"
    try:
        # directory.mkdir()
        os.makedirs(directory, exist_ok=True)
        gcs_block = GcsBucket.load("prefect-gcs-block-airbnb")
        gcs_block.upload_from_dataframe(
            df, to_path=path_name, serialization_format="parquet_snappy"
        )
    except OSError as error:
        print(error)

    print("Loaded data to GCS...Hooray!")
    return


# # write-gcs-to-bq
# ## Function create-bq-dataset
# Define a function that will create the BQ dataset and create a blank Dataframe
@flow(log_prints=True, name="write-mongodb-to-bq")
def create_bq_dataset(db_name: str, coll_name: str) -> None:
    gcp_credentials_block = GcpCredentials.load("prefect-gcs-2023-creds")
    converted_coll_name = convert_to_split_case(coll_name)
    df = pd.DataFrame()
    df.to_gbq(
        destination_table=f"{db_name}.{converted_coll_name}",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
        location="asia-southeast1",
    )
    return


# ## Function get-biqquery-client
# Get bigquery_client
@task(log_prints=True, name="get-bigquery-client")
def get_bigquery_client():
    gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds")
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    return client


# ## Function write-gcs-to-bq


# Upload data from GCS to BigQuery
@flow(log_prints=True, name="write-gcs-to-bq")
def write_gcs_to_bq(db_name: str, coll_name: str) -> None:
    client = get_bigquery_client()
    converted_coll_name = convert_to_split_case(coll_name)
    table_id = f"dtc-de-2023.{db_name}.{converted_coll_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
    )
    # Dont forget to run TERRAFORM to create the bucket
    # OR create bucket in https://console.cloud.google.com/storage/create-bucket
    uri = f"gs://airbnb-gcs-bucket/{db_name}/{converted_coll_name}.parquet.snappy"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


# ## Function dedup
# Remove duplicates
@flow(log_prints=True, name="removing-duplicates-from-bq")
def deduplicate_data():
    client = get_bigquery_client()
    # this will remove the duplicates
    query_dedup = """ 
            -- CREATE A CTE TABLE
            CREATE OR REPLACE TABLE 
                `dtc-de-2023.sample_airbnb.listings_and_reviews` AS 
            WITH 
                CTE1 AS (
                SELECT 
                    *, 
                    ROW_NUMBER() OVER(
                                    PARTITION BY _id 
                                    ORDER BY _bq_inserted_at) AS latest_row
                FROM `dtc-de-2023.sample_airbnb.listings_and_reviews`)

            -- FETCH ONLY THE LATEST ROW WHICH IS THE LATEST BQ INSERTED TIMESTAMP
            SELECT * EXCEPT (latest_row)
            FROM CTE1
            WHERE latest_row = 1 
            """
    # limit query to 10GB or priority=bigquery.QueryPriority.BATCH
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

    # query
    query_job = client.query(query_dedup, job_config=safe_config)

    # Check progress
    while query_job.state == "RUNNING":
        query_job = client.get_job(query_job.job_id, location=query_job.location)
        time.sleep(1)

        print("Complete removing duplicates")
        print(f"Job {query_job.job_id} is currently in state {query_job.state}")
    return


# ### Notes:
# ### In the case of the Airbnb dataset, you could partition the data by date or location,
# and then cluster the data within each partition based on columns such as the type of property, number of bedrooms,
# or amenities. This would allow you to quickly and easily analyze subsets of the data that are relevant to your specific analysis or query.


# Create a table with PARTITION AND CLUSTER
# Query
@flow(log_prints=True, name="create-partition-clustered-bq-table")
def create_partition_clustered_bq_table() -> None:
    print("Creating a separate partition and clustered table")
    client = get_bigquery_client()
    airbnb_part_clus = """
                        CREATE OR REPLACE TABLE 
                            `dtc-de-2023.sample_airbnb.listings_and_reviews_part_clust`
                        PARTITION BY
                            DATE(last_scraped)
                        CLUSTER BY
                            property_type AS (
                                SELECT *
                                FROM `dtc-de-2023.sample_airbnb.listings_and_reviews`
                            )
            """
    # Limit results to 1GB=10**10
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
    # Query
    results_part_clus = client.query(airbnb_part_clus, job_config=safe_config).result()
    print("Done creating partitioned and clustered table")
    return results_part_clus


# # Main
# Main ETL flow to load MongodDB to BigQuery
@flow(log_prints=True, name="etl-mongodb-to-bq")
def etl_mongodb_to_bq(db_name: str, coll_name: str):
    # Extract listings and reviews
    df = extract_collection(db_name, coll_name)
    # Tweak df
    df_ = tweak_df(df)
    # Upload to GCS
    write_to_gcs(df_, db_name, coll_name)
    # Create BQ dataset
    create_bq_dataset(db_name, coll_name)
    # Upload to BigQuery
    write_gcs_to_bq(db_name, coll_name)
    # Remove duplicates by creating or replacing table and
    # using the latest _bq_inserted_at
    deduplicate_data()
    # Create a separate partition and clustered table
    create_partition_clustered_bq_table()


if __name__ == "__main__":
    # Parameters
    db_name = "sample_airbnb"
    coll_name = "listingsAndReviews"
    # Run Main
    etl_mongodb_to_bq(db_name, coll_name)
