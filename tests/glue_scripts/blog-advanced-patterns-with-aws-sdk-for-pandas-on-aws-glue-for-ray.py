import os
from datetime import datetime

import awswrangler as wr
import ray


dynamodb_table_name = os.environ["dynamodb-table"]
timestream_database_name = os.environ["timestream-database"]
timestream_table_name = os.environ["timestream-table"]

# Filter for 5-star reviews with S3 Select within a partition
df_select = wr.s3.select_query(
    sql="SELECT * FROM s3object s where s.\"star_rating\" >= 5",
    path="s3://amazon-reviews-pds/parquet/product_category=Mobile_Electronics/",
    input_serialization="Parquet",
    input_serialization_params={},
    scan_range_chunk_size=1024*1024*16,
)
# Filter for unique review_id in Modin data frame
df_select = df_select.drop_duplicates(subset=["review_id"])

# Write Modin data frame to DynamoDB
wr.dynamodb.put_df(
    df=df_select,
    table_name=dynamodb_table_name,
    use_threads=4,
)

# Read data back from DynamoDB to Modin
df_dynamodb = wr.dynamodb.read_items(
    table_name=dynamodb_table_name,
    allow_full_scan=True,
)

# Read test Timestream data from S3
df_timestream = wr.s3.read_csv(
    path=["s3://aws-data-wrangler-public-artifacts/sample-data/timestream/sample.csv"],
    names=[
        "ignore0",
        "region",
        "ignore1",
        "az",
        "ignore2",
        "hostname",
        "measure_kind",
        "measure",
        "ignore3",
        "time",
        "ignore4",
    ],
)

# Select columns
df_timestream = df_timestream.loc[:, ["region", "az", "hostname", "measure_kind", "measure", "time"]]
# Overwrite the time column
df_timestream["time"] = datetime.now()
# Reset the index
df_timestream.reset_index(inplace=True, drop=False)
# Filter a measure
df_timestream = df_timestream[df_timestream.measure_kind == "cpu_utilization"]

# Repartition the data into 100 blocks
df_timestream = ray.data.from_modin(df_timestream).repartition(100).to_modin()

# Write data to Timestream
rejected_records = wr.timestream.write(
    df=df_timestream,
    database=timestream_database_name,
    table=timestream_table_name,
    time_col="time",
    measure_col="measure",
    dimensions_cols=["index", "region", "az", "hostname"],
)

# Query
df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{timestream_database_name}"."{timestream_table_name}"')
