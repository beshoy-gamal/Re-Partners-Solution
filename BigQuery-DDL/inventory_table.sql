CREATE TABLE `streaming_pubsub_dataflow.inventory` (
  event_type STRING NOT NULL,
  inventory_id STRING NOT NULL, 
  product_id STRING NOT NULL,
  warehouse_id STRING NOT NULL,
  quantity_change INT64 NOT NULL, 
  reason STRING NOT NULL, 
  timestamp TIMESTAMP NOT NULL
)
-- Partition by the event time for faster, cheaper queries
PARTITION BY DATE(timestamp)
-- Cluster the data by product and warehouse for efficient joins and lookups
CLUSTER BY product_id, warehouse_id;