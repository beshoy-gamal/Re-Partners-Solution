CREATE TABLE `streaming_pubsub_dataflow.user_activities` (
  event_type STRING NOT NULL,
  user_id STRING NOT NULL,
  activity_type STRING NOT NULL,
  ip_address STRING,
  user_agent STRING,
  timestamp TIMESTAMP NOT NULL,
  
  -- Nested field mapped to STRUCT
  metadata STRUCT<
    session_id STRING, 
    platform STRING 
  >
)
-- Partition by the event time
PARTITION BY DATE(timestamp)
-- Cluster the data by user_id and activity_type for efficient analytics
CLUSTER BY user_id, activity_type;