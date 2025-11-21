CREATE TABLE `streaming_pubsub_dataflow.orders` (
  event_type STRING,
  order_id STRING,
  customer_id STRING,
  order_date TIMESTAMP,
  status STRING,
  items ARRAY<STRUCT<
    product_id STRING,
    product_name STRING,
    quantity INT64,
    price FLOAT64
  >>,
  shipping_address STRUCT<
    street STRING,
    city STRING,
    country STRING
  >,
  total_amount FLOAT64
)
PARTITION BY DATE(order_date)
CLUSTER BY customer_id, status;