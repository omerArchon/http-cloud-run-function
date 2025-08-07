import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

# --- Environment Variables ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = os.environ.get('BIGQUERY_DATASET')
ARCHIVE_BUCKET_NAME = os.environ.get('ARCHIVE_BUCKET')
STAGING_TABLE = 'staging_raw_events'


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans DataFrame column names."""
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    print(f"Sanitized column names: {df.columns.to_list()}")
    return df

def split_category_levels(df: pd.DataFrame) -> pd.DataFrame:
    """Splits the 'Category' column."""
    if 'Category' not in df.columns:
        df['Category_Level_1'], df['Category_Level_2'], df['Category_Level_3'] = pd.NA, pd.NA, pd.NA
        return df

    df['Category'] = df['Category'].fillna('')
    categories_split = df['Category'].str.strip('/').str.split('/', expand=True)
    df['Category_Level_1'] = categories_split.get(0)
    df['Category_Level_2'] = categories_split.get(1)
    df['Category_Level_3'] = categories_split.get(2)
    df.replace({'': pd.NA}, inplace=True)
    return df

def run_etl_pipeline(event, context):
    """Main Cloud Function triggered by a GCS file upload."""
    source_bucket_name = event['bucket']
    source_file_name = event['name']
    
    if source_file_name.startswith('preprocessed/'):
        print(f"File {source_file_name} is already processed. Ignoring.")
        return

    print(f"Starting pipeline for file: gs://{source_bucket_name}/{source_file_name}")

    # --- Pre-processing with pandas ---
    source_gcs_path = f"gs://{source_bucket_name}/{source_file_name}"
    df = pd.read_csv(source_gcs_path)
    df_sanitized = sanitize_column_names(df.copy())
    df_processed = split_category_levels(df_sanitized)
    
    preprocessed_file_name = f"preprocessed/{source_file_name}"
    preprocessed_gcs_path = f"gs://{source_bucket_name}/{preprocessed_file_name}"
    print(f"Saving pre-processed file to: {preprocessed_gcs_path}")
    df_processed.to_csv(preprocessed_gcs_path, index=False)

    # --- BigQuery Load & Transform ---
    bq_client = bigquery.Client(project=PROJECT_ID)
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"

    # --- MODIFIED: The schema is now defined in Terraform. ---
    # We remove 'autodetect'. The load job will now fail if the CSV does not
    # conform to the staging table's schema, which is what we want.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        # autodetect is REMOVED. schema is REMOVED. We rely on the target table's schema.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    print("Starting BigQuery load job based on Terraform-defined schema...")
    load_job = bq_client.load_table_from_uri(preprocessed_gcs_path, staging_table_id, job_config=job_config)
    load_job.result()
    print("Load job finished successfully.")

    print("Running SQL transformations...")
    # This SQL list is now robust because the input types from staging_raw_events are fixed.
        # The final, fully corrected and resilient list of SQL transforms.
    sql_transforms = [
        # --- DIMENSION MERGE STATEMENTS ---
        f"MERGE `{PROJECT_ID}.{DATASET_ID}.dim_user` T USING (SELECT DISTINCT `Unity_User_ID` FROM `{staging_table_id}` WHERE `Unity_User_ID` IS NOT NULL) S ON T.user_id = S.`Unity_User_ID` WHEN NOT MATCHED THEN INSERT (user_id) VALUES (S.`Unity_User_ID`);",
        
        # MODIFIED: Changed CAST to SAFE_CAST for resilience.
        f"""MERGE `{PROJECT_ID}.{DATASET_ID}.dim_content` T 
           USING (SELECT DISTINCT URL, Sentiment, Entities, Category_Level_1, Category_Level_2, Category_Level_3 FROM `{staging_table_id}` WHERE URL IS NOT NULL) S 
           ON T.url = S.URL 
           WHEN NOT MATCHED THEN INSERT (content_id, url, sentiment, entities, category_l1, category_l2, category_l3) 
           VALUES (FARM_FINGERPRINT(S.URL), S.URL, SAFE_CAST(S.Sentiment AS NUMERIC), S.Entities, S.Category_Level_1, S.Category_Level_2, S.Category_Level_3);""",

        f"MERGE `{PROJECT_ID}.{DATASET_ID}.dim_banner` T USING (SELECT DISTINCT `Banner_ID`, `Element_ID` FROM `{staging_table_id}` WHERE `Banner_ID` IS NOT NULL) S ON T.banner_name = S.`Banner_ID` AND IFNULL(T.element_id, '') = IFNULL(S.`Element_ID`, '') WHEN NOT MATCHED THEN INSERT (banner_id, banner_name, element_id) VALUES (FARM_FINGERPRINT(CONCAT(IFNULL(S.`Banner_ID`,''), IFNULL(S.`Element_ID`,''))), S.`Banner_ID`, S.`Element_ID`);",
        
        f"MERGE `{PROJECT_ID}.{DATASET_ID}.dim_location` T USING (SELECT DISTINCT IP, Country, City FROM `{staging_table_id}` WHERE IP IS NOT NULL) S ON T.ip_address = S.IP WHEN NOT MATCHED THEN INSERT (location_id, ip_address, country, city) VALUES(FARM_FINGERPRINT(S.IP), S.IP, S.Country, S.City);",
        
        # --- FACT TABLE INSERT STATEMENT ---
        # MODIFIED: Changed CAST to SAFE_CAST for resilience.
        f"""INSERT INTO `{PROJECT_ID}.{DATASET_ID}.fact_events` 
              (event_id, event_timestamp, time_fk, user_fk, content_fk, banner_fk, location_fk, event_name, amount)
           SELECT
               raw.ID,
               SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', raw.Date) as event_timestamp,
               time.time_id as time_fk,
               users.user_id as user_fk,
               content.content_id as content_fk,
               banner.banner_id as banner_fk,
               loc.location_id as location_fk,
               raw.Event_Name,
               SAFE_CAST(raw.Amount AS NUMERIC) as amount
           FROM `{staging_table_id}` AS raw
           LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user` AS users ON raw.Unity_User_ID = users.user_id
           LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_content` AS content ON raw.URL = content.url
           LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_banner` AS banner ON raw.Banner_ID = banner.banner_name AND IFNULL(raw.Element_ID,'') = IFNULL(banner.element_id,'')
           LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_location` AS loc ON raw.IP = loc.ip_address
           LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_time` AS time ON DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', raw.Date)) = time.date;
        """
    ]

    for i, query in enumerate(sql_transforms):
        print(f"Executing SQL query #{i+1}...")
        job = bq_client.query(query)
        job.result()
    
    print("SQL Transformations complete.")

    # --- Cleanup ---
    print(f"Archiving original file: {source_file_name}")
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(ARCHIVE_BUCKET_NAME)
    source_blob = source_bucket.blob(source_file_name)
    source_bucket.copy_blob(source_blob, destination_bucket, source_file_name)
    source_blob.delete()
    print("Pipeline finished successfully.")
