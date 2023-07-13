									** PROBLEM1 **
1.Monthly Active Users (MAU) based on stream_start and app_start events:
-> 
function defination->
CREATE FUNCTION get_MAU(year INT, month INT)
RETURNS INT
BEGIN
  DECLARE MAU_count INT;
  
  SELECT COUNT(DISTINCT user_id) INTO MAU_count
  FROM event_tracker
  WHERE YEAR(created_at) = year
    AND MONTH(created_at) = month
    AND (event = 'stream_start' OR event = 'app_start');
    
  RETURN MAU_count;
END;

function calling->SELECT get_MAU(2023, 7) AS monthly_active_users;


2.Daily Active Users (DAU) based on stream_start and app_start events:
->
function defination->
CREATE FUNCTION get_DAU(daily_date_val DATE)
RETURNS int
BEGIN
  DECLARE daily_count INT;
  
  SELECT COUNT(DISTINCT user_id) INTO daily_count
  FROM event_tracker
  WHERE DATE(created_at) = daily_date_val
    AND (event = 'stream_start' OR event = 'app_start');
    
  RETURN daily_count;
END;


function calling->SELECT get_DAU('2023-07-13') AS daily_active_users;


3.Daily count of new users:
function defination->

CREATE FUNCTION get_NUC(date_val DATE)
RETURNS INT
BEGIN
  DECLARE NUC INT;
  
  SELECT COUNT(DISTINCT user_id) INTO NUC
  FROM event_tracker
  WHERE DATE(created_at) = date_val
    AND event = 'stream_start';
    
  RETURN NUC;
END;

function calling->SELECT get_NUC('2023-07-13') AS daily_new_users_count;


4.User cohort retention based on stream_start and app_start:

CREATE FUNCTION get_cohort_retention_ss_as(start_date DATE, end_date DATE)
RETURNS DECIMAL(5,2)
BEGIN
  DECLARE cohort_size_val INT;
  DECLARE retention DECIMAL(5,2);
  
  SELECT COUNT(DISTINCT user_id) INTO cohort_size_val
  FROM event_tracker
  WHERE DATE(created_at) = start_date
    AND (event = 'stream_start' OR event = 'app_start');
  
  SELECT COUNT(DISTINCT user_id) INTO retention
  FROM event_tracker
  WHERE DATE(created_at) BETWEEN start_date AND end_date
    AND (event = 'stream_start' OR event = 'app_start');
    
  SET retention = (retention / cohort_size_val) * 100;
  
  RETURN retention;
END;

function calling->
SELECT get_cohort_retention_ss_as('2023-07-01', '2023-07-07') AS cohort_retention;



                                                                                      ** PROBLEM2 **

ETL Pipeline:

from google.cloud import bigquery

# Set up the BigQuery client
client = bigquery.Client()

# Define the source tables and their respective schemas
source_tables = {
    "paid_user_subscriptions": {
        "table_id": "paid_user_subscriptions",
        "columns": ["id", "user_id", "plan_id", "status", "expires_at", "deleted_at", "created_at", "updated_at", "cancellation_at"]
    },
    "stream_logs": {
        "table_id": "stream_logs",
        "columns": ["id", "user_id", "event", "data", "date", "created_at", "updated_at"]
    },
    "_donations": {
        "table_id": "_donations",
        "columns": ["donation_id", "user_id", "currency_code", "amount", "donor_id", "email", "message", "verified", "source", "transaction_id", "usd_amount", "data", "created_at", "updated_at", "deleted_at"]
    }
}

# Define the BigQuery dataset and table where the data will be loaded
dataset_id = "your_dataset_id"
destination_table_id = "your_table_id"

# Function to load data from source tables to BigQuery
def load_data_to_bigquery(table_id, columns):
    # Create the destination table schema
    table_schema = [bigquery.SchemaField(col, "STRING") for col in columns]
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=table_schema)

    # Load data from the source table into the destination table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace existing data in the table
    job_config.skip_leading_rows = 1  # Skip header row if exists
    job_config.source_format = bigquery.SourceFormat.CSV  # Assuming source data is in CSV format

    source_uri = f"gs://your-bucket-name/{table_id}.csv"  # Replace with the actual Cloud Storage URI of the source data file
    load_job = client.load_table_from_uri(source_uri, table, job_config=job_config)

    load_job.result()  # Wait for the job to complete

    print(f"Data loaded from {table_id} to BigQuery table {destination_table_id}")

# Load data from source tables to BigQuery
for table_name, table_info in source_tables.items():
    load_data_to_bigquery(table_info["table_id"], table_info["columns"])


Use Apache Airflow or Google Cloud Composer for building and scheduling the ETL pipeline.
Extract data from the paid_user_subscriptions, stream_logs, and _donations tables.
Transform the data by cleaning, normalizing, and joining tables.
Load the transformed data into a data warehouse optimized for churn analysis.
Data Warehouse:

Use Google BigQuery as the data warehouse for storing and analyzing the data.
Design a schema tailored for churn analysis.
Utilize BigQuery's scalability, fast querying, and integration with other Google Cloud services.
Architecture:
Data Sources -> ETL Pipeline (Apache Airflow or Google Cloud Composer) -> Data Warehouse (Google BigQuery)





