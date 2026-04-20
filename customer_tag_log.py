import os
import json
import logging
from datetime import timedelta
import pandas as pd
from google.cloud import bigquery
from functions import convert_custom_to_utc, fetch_customers_info_basic

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "valor-sales")
DATASET_ID = "STLTH_DEAR"
STG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.stg_customer_updates"
DIM_TABLE = f"{PROJECT_ID}.{DATASET_ID}.customer_tag_log"

api_shop = "vpempire-com"
api_version = '2024-04'
# Assuming you mount this via Google Secret Manager in Cloud Run
api_token = os.getenv('SHOPIFY_STORE_PASSWORD') 
headers = {'X-Shopify-Access-Token': api_token}

client = bigquery.Client(project=PROJECT_ID)

def run_sync():
    logger.info("Starting Shopify Customer SCD2 Sync...")

    # 1. Fetch High-Water Mark
    query = f"SELECT MAX(shopify_updated_at) as last_update FROM `{DIM_TABLE}`"
    results = client.query(query).result()
    last_date = next(results).last_update

    if not last_date:
        logger.warning("No historical data found. Defaulting to 2026-01-01.")
        last_date = pd.to_datetime('2026-01-01')

    # Look back 1 day to ensure we catch late-arriving updates
    acct_updated_since_date = (last_date - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info(f"Searching for updates since {acct_updated_since_date} (Local Time)")
    
    updated_at_min = convert_custom_to_utc(acct_updated_since_date)

    # 2. Extract from Shopify API
    customer_data = fetch_customers_info_basic(api_shop, api_version, headers, updated_at_min, logger)
    
    if not customer_data:
        logger.info("No new updates found. Exiting gracefully.")
        return


    # 3. Transform for Staging
    df = pd.DataFrame(customer_data)
    
    # Rename to match our schema
    df = df.rename(columns={'id': 'customer_id', 'created_at': 'shopify_created_date', 'updated_at': 'shopify_updated_at'})
    df['customer_id'] = df['customer_id'].astype(str)
    
    # Parse the full timestamp 
    df['shopify_created_date'] = pd.to_datetime(df['shopify_created_date'], utc=True)
    df['shopify_updated_at'] = pd.to_datetime(df['shopify_updated_at'], utc=True)

    # Drop the timezone data so BigQuery safely loads it as a naive DATETIME
    df['shopify_created_date'] = df['shopify_created_date'].dt.tz_localize(None)
    df['shopify_updated_at'] = df['shopify_updated_at'].dt.tz_localize(None)
    
    stg_df = df[['customer_id', 'customer_name', 'shopify_created_date', 'shopify_updated_at', 'tags']]

    # 4. Load to Staging (Truncate and Load)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    logger.info("Loading data to BigQuery staging table...")
    load_job = client.load_table_from_dataframe(stg_df, STG_TABLE, job_config=job_config)
    load_job.result()
    logger.info(f"Loaded {load_job.output_rows} rows to {STG_TABLE}.")

    # 5. Execute SCD2 MERGE
    merge_query = f"""
    MERGE INTO `{DIM_TABLE}` T
    USING (
      WITH clean_staging AS (
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY shopify_updated_at DESC) as rn
          FROM `{STG_TABLE}`
        ) WHERE rn = 1
      )

      -- 1. Identify brand new customers
      SELECT 
        s.customer_id, s.customer_name, s.shopify_created_date, s.shopify_updated_at, s.tags,
        CURRENT_TIMESTAMP() AS start_timestamp,
        TIMESTAMP('9999-12-31 00:00:00 UTC') AS end_timestamp, 
        TRUE AS is_current, 'INSERT' AS action_type
      FROM clean_staging s
      LEFT JOIN `{DIM_TABLE}` T ON s.customer_id = T.customer_id AND T.is_current = TRUE
      WHERE T.customer_id IS NULL

      UNION ALL

      -- 2. Expire old records for changed customers (dummy timestamp for start_timestamp here as a placeholder)
      SELECT  
        s.customer_id, s.customer_name, s.shopify_created_date, s.shopify_updated_at, s.tags,
        CURRENT_TIMESTAMP() AS start_timestamp,
        CURRENT_TIMESTAMP() AS end_timestamp,
        FALSE AS is_current, 'UPDATE_EXPIRE' AS action_type
      FROM clean_staging s
      INNER JOIN `{DIM_TABLE}` T ON s.customer_id = T.customer_id AND T.is_current = TRUE
      WHERE s.tags IS DISTINCT FROM T.tags

      UNION ALL

      -- 3. Insert new active records for changed customers
      SELECT 
        s.customer_id, s.customer_name, s.shopify_created_date, s.shopify_updated_at, s.tags,
        CURRENT_TIMESTAMP() AS start_timestamp,
        TIMESTAMP('9999-12-31 00:00:00 UTC') AS end_timestamp,
        TRUE AS is_current, 'UPDATE_INSERT' AS action_type
      FROM clean_staging S
      INNER JOIN `{DIM_TABLE}` T ON S.customer_id = T.customer_id AND T.is_current = TRUE
      WHERE S.tags IS DISTINCT FROM T.tags
    ) AS S
    ON T.customer_id = S.customer_id AND T.is_current = TRUE AND S.action_type = 'UPDATE_EXPIRE'

    WHEN MATCHED THEN 
      UPDATE SET
        T.is_current = FALSE,
        T.end_timestamp = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED BY TARGET THEN
      INSERT(customer_id, customer_name, shopify_created_date, shopify_updated_at, tags, start_timestamp, end_timestamp, is_current)
      VALUES (S.customer_id, S.customer_name, S.shopify_created_date, S.shopify_updated_at, S.tags, S.start_timestamp, S.end_timestamp, S.is_current)
    """
    
    logger.info("Executing SCD2 MERGE operation...")
    merge_job = client.query(merge_query)
    merge_job.result()
    
    logger.info("MERGE complete. Pipeline finished successfully.")

if __name__ == "__main__":
    run_sync()