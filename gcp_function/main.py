import os
import requests
import pandas as pd
from google.cloud import bigquery
import functions_framework

# --- Configuration ---
# These values are pulled from the Cloud Function's environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_ID = os.environ.get("BIGQUERY_DATASET")
STAGING_TABLE_ID = "staging_raw_events"
API_URL = "https://bannerevents.archonphserver.com/api.php"

# --- Initialize BigQuery Client ---
# Initialized globally to reuse the connection across function invocations for efficiency
bq_client = bigquery.Client()

def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs all row-level transformations, cleaning, and feature engineering on the DataFrame.
    This prepares the data for loading into the pre-processed staging table.
    """
    print("Starting pre-processing on the DataFrame...")

    # --- Mapping Dictionaries for Standardization ---
    # Keys are lowercase to ensure case-insensitive matching. Values are the desired canonical form.
    COUNTRY_MAPPING = {
        # Common abbreviations and variations
        'usa': 'United States',
        'u.s.a.': 'United States',
        'united states of america': 'United States',
        'uk': 'United Kingdom',
        'great britain': 'United Kingdom',
        'england': 'United Kingdom', # Note: A simplification for this context
        
        # Different languages or common misspellings
        'türkiye': 'Turkey',
        'deutschland': 'Germany',
        'españa': 'Spain',
        'brasil': 'Brazil',
        
        # Normalizing casing/spacing
        'the netherlands': 'Netherlands',
        'south korea': 'South Korea',
        'new zealand': 'New Zealand',
    }
    
    # Keys are lowercase and have spaces removed to match the transformation logic below.
    CITY_MAPPING = {
        'newyork': 'New York',
        'nyc': 'New York',
        'st petersburg': 'Saint Petersburg', # This becomes 'stpetersburg' after normalization
        'stpetersburg': 'Saint Petersburg',
        'frankfurt am main': 'Frankfurt', # This becomes 'frankfurtammain'
        'frankfurtammain': 'Frankfurt',
        'sf': 'San Francisco',
        'sanfran': 'San Francisco',
        'la': 'Los Angeles',
        'losangeles': 'Los Angeles',
        'sao paulo': 'São Paulo', # This becomes 'saopaulo'
        'saopaulo': 'São Paulo'
    }
    
    
    # 1. Handle Amount -> unit_name, unit_value
    def get_unit(event):
        if event == 'dwell': return 'seconds'
        if event == 'scroll': return 'pixels'
        return 'count'
    
    df['unit_name'] = df['event'].apply(get_unit)
    df['unit_value'] = df['amount']
    
    # 2. Split Banner ID -> banner_name, banner_size
    # Uses pandas' vectorized string operations for performance and handling of nulls.
    df['banner_size'] = df['banner_id'].str.extract(r'(\d+x\d+)', expand=False)
    df['banner_name'] = df['banner_id'].str.replace(r'_(\d+x\d+)$', '', regex=True)

    # 3. Split Category -> category_l1, l2, l3
    cat_splits = df['category'].str.strip('/').str.split('/', expand=True)
    df['category_l1'] = cat_splits.get(0)
    df['category_l2'] = cat_splits.get(1)
    df['category_l3'] = cat_splits.get(2)
    
    # 4. Standardize Location Data
    # Create temporary lowercase columns for robust mapping.
    df['country_lower'] = df['country'].str.lower().str.strip().fillna('')
    # Map values, and for any unmapped items, fall back to the original value (but properly title-cased).
    df['country'] = df['country_lower'].map(COUNTRY_MAPPING).fillna(df['country'].str.title())

    df['city_lower'] = df['city'].str.lower().str.replace(" ", "").str.strip().fillna('')
    df['city'] = df['city_lower'].map(CITY_MAPPING).fillna(df['city'].str.title())
    
    # 5. Coerce data types to align with BigQuery schema and handle errors
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
    df['sentiment'] = pd.to_numeric(df['sentiment'], errors='coerce').astype('float64')
    df['unit_value'] = pd.to_numeric(df['unit_value'], errors='coerce').astype('float64')

    # 6. Define the final schema for the staging table to ensure column order and presence
    final_columns = [
        "id", "url", "element_id", "event", "sentiment", "unity_user_id",
        "entities", "ip", "country", "city", "issue_date",
        "banner_name", "banner_size", "unit_name", "unit_value",
        "category_l1", "category_l2", "category_l3"
    ]
    
    # Reindex ensures the DataFrame has exactly these columns in this order, filling missing ones with NaN.
    df_processed = df.reindex(columns=final_columns)
    
    print("Pre-processing complete. Location data standardized.")
    return df_processed


def run_transformation_sql():
    """
    Executes a series of idempotent SQL queries to process data from the
    staging table into the final dimension and fact tables.

    This function assumes that the `dim_time` table has already been
    pre-populated with all necessary dates.
    """
    
    sql_queries = [
        # 1. Populate dim_user
        # Merges new users based on their unique unity_user_id.
        f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.dim_user` T
        USING (
            SELECT DISTINCT unity_user_id 
            FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}` 
            WHERE unity_user_id IS NOT NULL
        ) S
        ON T.user_id = S.unity_user_id
        WHEN NOT MATCHED THEN
          INSERT (user_sk, user_id) 
          VALUES(FARM_FINGERPRINT(S.unity_user_id), S.unity_user_id);
        """,
        
        # 2. Populate dim_location
        # Merges new locations based on their unique IP address.
        f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.dim_location` T
        USING (
            SELECT DISTINCT ip, country, city 
            FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}` 
            WHERE ip IS NOT NULL
        ) S
        ON T.ip_address = S.ip
        WHEN NOT MATCHED THEN
          INSERT (location_sk, ip_address, country, city) 
          VALUES(FARM_FINGERPRINT(S.ip), S.ip, S.country, S.city);
        """,
        
        # 3. Populate dim_banner
        # Merges new banner variations based on the composite key of name and size.
        f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.dim_banner` T
        USING (
            SELECT DISTINCT banner_name, banner_size
            FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`
            WHERE banner_name IS NOT NULL
        ) S
        ON T.banner_name = S.banner_name 
           AND COALESCE(T.banner_size, '') = COALESCE(S.banner_size, '')
        WHEN NOT MATCHED THEN
          INSERT (banner_sk, banner_name, banner_size)
          VALUES(
            FARM_FINGERPRINT(CONCAT(S.banner_name, COALESCE(S.banner_size, ''))), 
            S.banner_name, 
            S.banner_size
          );
        """,
        
        # 4. Populate dim_content
        # Merges new content pages based on their unique URL.
        f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.dim_content` T
        USING (
            SELECT DISTINCT url, sentiment, entities, category_l1, category_l2, category_l3
            FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`
            WHERE url IS NOT NULL
        ) S
        ON T.url = S.url
        WHEN NOT MATCHED THEN
          INSERT (content_sk, url, sentiment, entities, category_l1, category_l2, category_l3)
          VALUES(
            FARM_FINGERPRINT(S.url), 
            S.url, 
            S.sentiment, 
            S.entities, 
            S.category_l1, 
            S.category_l2, 
            S.category_l3
          );
        """,
        
        # 5. Populate the final fact_events table
        # This is an INSERT statement that looks up all foreign keys from the dimensions.
        f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.fact_events` 
          (event_id, event_timestamp, time_sk, user_sk, content_sk, banner_sk, location_sk, event_name, element_id, unit_name, unit_value)
        SELECT
          s.id AS event_id,
          
          -- The full timestamp is still valuable for precise analysis and partitioning.
          PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', s.issue_date) AS event_timestamp,
          
          -- Foreign Key Lookups --
          t.time_sk,
          u.user_sk,
          c.content_sk,
          b.banner_sk,
          l.location_sk,
          
          -- Measures and Event Details --
          s.event AS event_name,
          s.element_id,
          s.unit_name,
          s.unit_value
        FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}` s

        -- Join to pre-populated dim_time by calculating the YYYYMMDD key from the event's issue_date.
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_time` t 
            ON t.time_sk = CAST(FORMAT_DATE('%Y%m%d', DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', s.issue_date))) AS INT64)
            
        -- Join other dimensions to get their surrogate keys
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user` u ON u.user_id = s.unity_user_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_location` l ON l.ip_address = s.ip
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_content` c ON c.url = s.url
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_banner` b ON 
            b.banner_name = s.banner_name 
            AND COALESCE(b.banner_size, '') = COALESCE(s.banner_size, '') 
            
        -- This WHERE clause ensures we only insert events that are not already in the fact table, making the pipeline idempotent.
        WHERE s.id IS NOT NULL 
          AND s.id NOT IN (SELECT event_id FROM `{PROJECT_ID}.{DATASET_ID}.fact_events`);
        """
    ]

    for i, query in enumerate(sql_queries):
        # We start from 1 for better logging readability
        print(f"Executing transform query {i + 1}/{len(sql_queries)}...")
        job = bq_client.query(query)
        job.result()  # Wait for the query to complete
        
        # DML statements like MERGE and INSERT have a num_dml_affected_rows attribute
        rows_affected = job.num_dml_affected_rows if job.num_dml_affected_rows is not None else 0
        print(f"Query {i + 1} completed. Rows affected: {rows_affected}")

@functions_framework.http
def process_banner_events(request):
    """
    HTTP-triggered Cloud Function orchestrator.
    It performs the full ELT process: Extract, Load (to staging), Transform (to star schema).
    """
    print("Starting event processing pipeline...")

    # 1. --- EXTRACT ---
    try:
        response = requests.get(API_URL, timeout=45)
        response.raise_for_status()
        events_json = response.json()
        if not events_json:
            print("API returned no new events. Exiting.")
            return "Success: No new events to process.", 200
        print(f"Successfully fetched {len(events_json)} events from the API.")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to fetch data from API: {e}")
        return f"API Fetch Error: {e}", 500

    # 2. --- PRE-PROCESS & LOAD ---
    df_raw = pd.DataFrame(events_json)
    df_processed = preprocess_dataframe(df_raw)
    
    table_ref = bq_client.dataset(DATASET_ID).table(STAGING_TABLE_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        print(f"Loading {len(df_processed)} pre-processed rows into staging table...")
        load_job = bq_client.load_table_from_dataframe(df_processed, table_ref, job_config=job_config)
        load_job.result()
        print(f"Successfully loaded {load_job.output_rows} rows.")
    except Exception as e:
        print(f"Error: Failed to load data into BigQuery staging table: {e}")
        if 'load_job' in locals() and load_job.errors: print(f"BigQuery errors: {load_job.errors}")
        return f"BigQuery Load Error: {e}", 500

    # 3. --- TRANSFORM ---
    print("Starting SQL transformation from staging to star schema...")
    try:
        run_transformation_sql()
        print("Successfully transformed data and populated all dimension and fact tables.")
    except Exception as e:
        print(f"Error: SQL transformation failed: {e}")
        return f"SQL Transform Error: {e}", 500
    
    return f"Success: Pipeline completed. Processed {len(events_json)} events.", 200

 