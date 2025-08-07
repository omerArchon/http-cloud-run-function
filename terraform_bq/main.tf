
# --- 1. BIGQUERY DATASET ---
resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Events Star Schema"
  description                 = "Dataset for events analytics with a fact and dimension model, managed by Terraform."
  location                    = var.dataset_location
  delete_contents_on_destroy  = true # Safety setting.
}

# --- 2. STAGING TABLE (MODIFIED) ---
# We now define the schema explicitly in Terraform. This is our data contract.
resource "google_bigquery_table" "staging_raw_events" {
  dataset_id  = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id    = "staging_raw_events"
  description = "Staging table for raw event data. Schema is managed by Terraform."
  deletion_protection = false

  # --- THIS SCHEMA IS NEW ---
  # It defines the expected types for raw data *after* pandas processing but *before* final loading.
  # This makes the pipeline robust by catching type errors at the load step.
  schema = <<EOF
[
  {"name": "ID", "type": "INT64", "mode": "NULLABLE"},
  {"name": "URL", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Category", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Sentiment", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "Entities", "type": "STRING", "mode": "NULLABLE"},
  {"name": "IP", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
  {"name": "City", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Banner_ID", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Element_ID", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Event_Name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Amount", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "Date", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Unity_User_ID", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Category_Level_1", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Category_Level_2", "type": "STRING", "mode": "NULLABLE"},
  {"name": "Category_Level_3", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}

# --- 3. DIMENSION TABLES (No Change) ---
resource "google_bigquery_table" "dim_user" { #... no change ...
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "dim_user"
  description = "Dimension: Stores unique users."
  deletion_protection = false
  schema = <<EOF
[
  {"name": "user_id", "type": "STRING", "mode": "REQUIRED"}
]
EOF
}
resource "google_bigquery_table" "dim_content" { #... no change ...
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "dim_content"
  description = "Dimension: Stores unique content pages."
  deletion_protection = false
  # This schema now reflects the output of your Python script
  schema = <<EOF
[
  {"name": "content_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "sentiment", "type": "NUMERIC", "mode": "NULLABLE"},
  {"name": "entities", "type": "STRING", "mode": "NULLABLE"},
  {"name": "category_l1", "type": "STRING", "mode": "NULLABLE"},
  {"name": "category_l2", "type": "STRING", "mode": "NULLABLE"},
  {"name": "category_l3", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
resource "google_bigquery_table" "dim_banner" { #... no change ...
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "dim_banner"
  description = "Dimension: Stores unique banners and their elements."
  deletion_protection = false
  schema = <<EOF
[
  {"name": "banner_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "banner_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "element_id", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
resource "google_bigquery_table" "dim_location" { #... no change ...
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "dim_location"
  description = "Dimension: Stores unique geographic locations from IPs."
  deletion_protection = false
  schema = <<EOF
[
  {"name": "location_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
  {"name": "country", "type": "STRING", "mode": "NULLABLE"},
  {"name": "city", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
resource "google_bigquery_table" "dim_time" { #... no change ...
  dataset_id  = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id    = "dim_time"
  description = "Dimension: Pre-aggregated time attributes for simplified querying."
  deletion_protection = false
  
  # A rich time dimension. The PK, time_id, is a numeric representation like 20231027.
  schema = <<EOF
[
  {"name": "time_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "date", "type": "DATE", "mode": "NULLABLE"},
  {"name": "year", "type": "INT64", "mode": "NULLABLE"},
  {"name": "quarter", "type": "INT64", "mode": "NULLABLE"},
  {"name": "month", "type": "INT64", "mode": "NULLABLE"},
  {"name": "month_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "day", "type": "INT64", "mode": "NULLABLE"},
  {"name": "day_of_week", "type": "INT64", "mode": "NULLABLE"},
  {"name": "day_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "is_weekend", "type": "BOOLEAN", "mode": "NULLABLE"}
]
EOF
}

# --- 4. FACT TABLE (No Change) ---
resource "google_bigquery_table" "fact_events" { #... no change ...
  dataset_id  = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id    = "fact_events"
  description = "Fact table for all user interaction events."
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp" # Keep this for partitioning!
  }

  # Add time_fk to the clustering for better performance on date-range queries.
  clustering = ["time_fk", "user_fk", "content_fk", "banner_fk"]

  # The schema now adds time_fk and keeps event_timestamp.
  schema = <<EOF
[
  {"name": "event_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "time_fk", "type": "INT64", "mode": "NULLABLE"}, 
  {"name": "user_fk", "type": "STRING", "mode": "NULLABLE"},
  {"name": "content_fk", "type": "INT64", "mode": "NULLABLE"},
  {"name": "banner_fk", "type": "INT64", "mode": "NULLABLE"},
  {"name": "location_fk", "type": "INT64", "mode": "NULLABLE"},
  {"name": "event_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"}
]
EOF
}