import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from apache_beam.io import fileio

def to_json_bytes(element):
    """Convert dict to JSON bytes"""
    return json.dumps(element, ensure_ascii=False).encode("utf-8")

def destination_path(element_bytes, base_path):  
    # this function for create a partioned hairaical path in GCS by year, month, day, hour, min
    """Create GCS path: event_type/YYYY/MM/DD/HH/MM/eventtype_YYYYMMDDHHMMSS.json"""
    element = json.loads(element_bytes.decode("utf-8"))
    ts_str = element.get("timestamp") or element.get("order_date") # bescause orders events have field order_date
    ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
    event_type = element["event_type"]

    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")
    hour = ts.strftime("%H")
    minute = ts.strftime("%M")
    filename = "{}_{}.json".format(event_type, ts.strftime("%Y%m%d%H%M%S"))

    # return f"{base_path}/{event_type}/{year}/{month}/{day}/{hour}/{minute}/{filename}"

    return "{}/{}/{}/{}/{}/{}/{}".format(
        base_path, event_type, year, month, day, hour, minute, filename)


def parse_json(message):
    """Decode Pub/Sub message and parse JSON."""
    try:
        return json.loads(message.decode("utf-8"))
    except Exception:
        return None


def classify_event(element, num_partitions):
    """Return a partition index based on event_type."""
    event_type = element.get("event_type", "").lower()

    if event_type == "order":
        return 0
    elif event_type == "user":
        return 1
    elif event_type == "inventory":
        return 2
    else:
        return 3   # unknown / invalid messages 


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--project")
    parser.add_argument("--region")
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dataset", required=True)

    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        beam_args,
        streaming=True,
        save_main_session=True,
        runner="DataflowRunner",
        project=args.project,
        region=args.region,
    )


    ORDERS_TABLE = f"{args.project}:{args.dataset}.orders"
    USERS_TABLE = f"{args.project}:{args.dataset}.user_activities"
    INVENTORY_TABLE = f"{args.project}:{args.dataset}.inventory"

    with beam.Pipeline(options=pipeline_options) as p:

        messages = (
            p
            | "ReadFromSubscription" >> beam.io.ReadFromPubSub(
                subscription=args.subscription)
            | "ParseJSON" >> beam.Map(parse_json)
            | "FilterNulls" >> beam.Filter(lambda x: x is not None)
        )

        # Split stream into 3 partitions
        orders, users, inventory, _ = (
            messages
            | "PartitionByEventType"
            >> beam.Partition(classify_event, 4)
        )

        # Write each stream to BigQuery after appling filtering
        
        orders = orders | "FilterStatusEnum" >> beam.Filter(
                lambda x: x.get("status") in {"pending", "processing", "shipped", "delivered"})

        orders  | "WriteOrdersToBQ" >> beam.io.WriteToBigQuery(ORDERS_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,)
        

        
        users = users | "FilterValues_activity" >> beam.Filter(
                lambda x: x.get("activity_type") in {"login", "logout", "view_product", "add_to_cart", "remove_from_cart"})            
        users = users | "FilterValues_platform" >> beam.Filter(
                      lambda x: x.get("metadata", {}).get("platform") in {"web", "mobile", "tablet"})
        users | "WriteUsersToBQ" >> beam.io.WriteToBigQuery(USERS_TABLE,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,)


        
            
        inventory = inventory | "FilterValues_quanity" >> beam.Filter(
                lambda x: -100 <= x.get("quantity_change", 0) <= 100) 
        inventory = inventory | "FilterValues_reason" >> beam.Filter(
                lambda x: x.get("reason") in {"restock", "sale", "return", "damage"})

        inventory | "WriteInventoryToBQ" >> beam.io.WriteToBigQuery(INVENTORY_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )

        # Write each stream to GCS
        # the shape of output format gs://bucket/output/orders/2025/02/08/13/09/order_2025020813090001.json

        for pcoll, event_type in zip([orders, users, inventory], ["orders", "user_activities", "inventory"]):
            (
                pcoll
                | f"WindowInto_{event_type}" >> beam.WindowInto(beam.window.FixedWindows(60))
                | f"{event_type}_ToJSONBytes" >> beam.Map(to_json_bytes)
                | f"Write{event_type.capitalize()}ToGCS" >> fileio.WriteToFiles(
                    path="gs://cpi_poc/temp",
                    destination=lambda e, bp="gs://cpi_poc/temp": destination_path(e, bp),
                    file_naming=fileio.destination_prefix_naming(),
                    shards=1
                )
            )


if __name__ == "__main__":
    run()
