# --- 1. BIGQUERY DATASET ---
resource "google_bigquery_dataset" "analytics_dataset" {
  project                    = var.gcp_project_id
  dataset_id                 = var.dataset_id
  friendly_name              = "Events Star Schema"
  description                = "Dataset for events analytics with a fact and dimension model, managed by Terraform."
  location                   = var.dataset_location
  delete_contents_on_destroy = true # Set to false in production
}

# --- 2. STAGING TABLE ---
resource "google_bigquery_table" "staging_raw_events" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "staging_raw_events"
  description         = "Staging table for pre-processed event data. Data is ephemeral and overwritten on each run."
  deletion_protection = false

  schema = <<EOF
[
  {"name": "id", "type": "INT64", "mode": "NULLABLE", "description": "The unique ID of the event from the source system."},
  {"name": "url", "type": "STRING", "mode": "NULLABLE", "description": "The full URL where the event occurred."},
  {"name": "element_id", "type": "STRING", "mode": "NULLABLE", "description": "The specific element within the banner that was interacted with."},
  {"name": "event", "type": "STRING", "mode": "NULLABLE", "description": "The name of the event that occurred."},
  {"name": "sentiment", "type": "FLOAT64", "mode": "NULLABLE", "description": "The sentiment score of the content on the page."},
  {"name": "unity_user_id", "type": "STRING", "mode": "NULLABLE", "description": "The unique GUID identifying the user from the source system."},
  {"name": "entities", "type": "STRING", "mode": "NULLABLE", "description": "A comma-separated string of named entities related to the content."},
  {"name": "ip", "type": "STRING", "mode": "NULLABLE", "description": "The IP address of the user at the time of the event."},
  {"name": "country", "type": "STRING", "mode": "NULLABLE", "description": "The country associated with the user's IP address."},
  {"name": "city", "type": "STRING", "mode": "NULLABLE", "description": "The city associated with the user's IP address."},
  {"name": "issue_date", "type": "STRING", "mode": "NULLABLE", "description": "The raw timestamp string of when the event was issued."},
  {"name": "banner_name", "type": "STRING", "mode": "NULLABLE", "description": "The name part of the banner, extracted from the raw banner_id."},
  {"name": "banner_size", "type": "STRING", "mode": "NULLABLE", "description": "The dimension part of the banner, extracted from the raw banner_id."},
  {"name": "unit_name", "type": "STRING", "mode": "NULLABLE", "description": "The unit of measurement for the event's value (e.g., 'seconds')."},
  {"name": "unit_value", "type": "FLOAT64", "mode": "NULLABLE", "description": "The numeric value associated with the event and its unit_name."},
  {"name": "category_l1", "type": "STRING", "mode": "NULLABLE", "description": "The first level of the content category."},
  {"name": "category_l2", "type": "STRING", "mode": "NULLABLE", "description": "The second level of the content category."},
  {"name": "category_l3", "type": "STRING", "mode": "NULLABLE", "description": "The third level of the content category."}
]
EOF
}

# --- 3. DIMENSION TABLES ---

resource "google_bigquery_table" "dim_user" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "dim_user"
  description         = "Dimension: Stores unique users."
  deletion_protection = false
  schema              = <<EOF
[
  {"name": "user_sk", "type": "INT64", "mode": "REQUIRED", "description": "Surrogate key for the user, used for performant joins."},
  {"name": "user_id", "type": "STRING", "mode": "NULLABLE", "description": "The original unique user identifier from the event source (GUID)."}
]
EOF
}
resource "google_bigquery_table" "dim_content" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "dim_content"
  description         = "Dimension: Stores unique content pages and their attributes."
  deletion_protection = false
  schema              = <<EOF
[
  {"name": "content_sk", "type": "INT64", "mode": "REQUIRED", "description": "Surrogate key for the content, used for performant joins."},
  {"name": "url", "type": "STRING", "mode": "NULLABLE", "description": "The full URL of the content page. This now serves as the natural key."},
  {"name": "sentiment", "type": "FLOAT64", "mode": "NULLABLE", "description": "The sentiment score associated with the content."},
  {"name": "entities", "type": "STRING", "mode": "NULLABLE", "description": "Named entities found within the content."},
  {"name": "category_l1", "type": "STRING", "mode": "NULLABLE", "description": "The first level of the content category, parsed from the raw category string."},
  {"name": "category_l2", "type": "STRING", "mode": "NULLABLE", "description": "The second level of the content category."},
  {"name": "category_l3", "type": "STRING", "mode": "NULLABLE", "description": "The third level of the content category."}
]
EOF
}

resource "google_bigquery_table" "dim_banner" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "dim_banner"
  description         = "Dimension: Stores unique banners and their attributes."
  deletion_protection = false
  schema              = <<EOF
[
  {"name": "banner_sk", "type": "INT64", "mode": "REQUIRED", "description": "Surrogate key for the banner, used for performant joins."},
  {"name": "banner_name", "type": "STRING", "mode": "NULLABLE", "description": "The name part of the banner, extracted from the raw banner_id (e.g., 'unity_finance')."},
  {"name": "banner_size", "type": "STRING", "mode": "NULLABLE", "description": "The dimension part of the banner, extracted from the raw banner_id (e.g., '300x600')."}
]
EOF
}

resource "google_bigquery_table" "dim_location" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "dim_location"
  description         = "Dimension: Stores unique geographic locations based on IP address."
  deletion_protection = false
  schema              = <<EOF
[
  {"name": "location_sk", "type": "INT64", "mode": "REQUIRED", "description": "Surrogate key for the location, used for performant joins."},
  {"name": "ip_address", "type": "STRING", "mode": "NULLABLE", "description": "The unique IP address of the user. This is the natural key."},
  {"name": "country", "type": "STRING", "mode": "NULLABLE", "description": "The country derived from the IP address."},
  {"name": "city", "type": "STRING", "mode": "NULLABLE", "description": "The city derived from the IP address."}
]
EOF
}

resource "google_bigquery_table" "dim_time" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "dim_time"
  description         = "Pre-populated dimension table for dates from 2020 to 2030."
  deletion_protection = false

  schema = <<EOF
[
  {"name": "time_sk", "type": "INT64", "mode": "REQUIRED", "description": "Surrogate and smart key for the date, in YYYYMMDD format (e.g., 20250801)."},
  {"name": "date", "type": "DATE", "mode": "NULLABLE", "description": "The full date value."},
  {"name": "year", "type": "INT64", "mode": "NULLABLE", "description": "The year of the date."},
  {"name": "quarter", "type": "INT64", "mode": "NULLABLE", "description": "The quarter of the year (1-4)."},
  {"name": "month", "type": "INT64", "mode": "NULLABLE", "description": "The month of the year (1-12)."},
  {"name": "month_name", "type": "STRING", "mode": "NULLABLE", "description": "The full name of the month (e.g., 'August')."},
  {"name": "day", "type": "INT64", "mode": "NULLABLE", "description": "The day of the month (1-31)."},
  {"name": "day_of_week", "type": "INT64", "mode": "NULLABLE", "description": "The day of the week (1=Sunday, 7=Saturday)."},
  {"name": "day_name", "type": "STRING", "mode": "NULLABLE", "description": "The full name of the day of the week (e.g., 'Friday')."},
  {"name": "is_weekend", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Boolean flag indicating if the date falls on a Saturday or Sunday."}
]
EOF
}

# --- 4. FACT TABLE ---
resource "google_bigquery_table" "fact_events" {
  project             = google_bigquery_dataset.analytics_dataset.project
  dataset_id          = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id            = "fact_events"
  description         = "Fact table for all user interaction events, linking all dimensions."
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp" # Partitioning by the actual event timestamp
  }

  clustering = ["user_sk", "content_sk", "banner_sk"]

  schema = <<EOF
[
  {"name": "event_id", "type": "INT64", "mode": "REQUIRED", "description": "The unique ID of the event from the source system. This is the primary key of the fact table."},
  {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "The exact timestamp when the event occurred. This column is used for partitioning."},
  {"name": "time_sk", "type": "INT64", "mode": "NULLABLE", "description": "Foreign key linking to the dim_time table."},
  {"name": "user_sk", "type": "INT64", "mode": "NULLABLE", "description": "Foreign key linking to the dim_user table."},
  {"name": "content_sk", "type": "INT64", "mode": "NULLABLE", "description": "Foreign key linking to the dim_content table."},
  {"name": "banner_sk", "type": "INT64", "mode": "NULLABLE", "description": "Foreign key linking to the dim_banner table."},
  {"name": "location_sk", "type": "INT64", "mode": "NULLABLE", "description": "Foreign key linking to the dim_location table."},
  {"name": "event_name", "type": "STRING", "mode": "NULLABLE", "description": "The name of the event (e.g., 'click', 'dwell')."},
  {"name": "element_id", "type": "STRING", "mode": "NULLABLE", "description": "The specific interactive element within the banner (e.g., 'btn1'). NULL if the event is on the banner itself."},
  {"name": "unit_name", "type": "STRING", "mode": "NULLABLE", "description": "The unit of measurement for the event's value (e.g., 'seconds', 'pixels', 'count')."},
  {"name": "unit_value", "type": "FLOAT64", "mode": "NULLABLE", "description": "The numeric value associated with the event and its unit_name."}
]
EOF
}