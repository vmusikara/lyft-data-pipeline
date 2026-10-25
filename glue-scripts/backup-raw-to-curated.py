# Copyright © Sachin Chandrashekhar - Data Engineering Hub. All Rights Reserved.
#
# This code is provided as part of a paid training program.
# Unauthorized copying, distribution, or
# publication of this code—either in whole or in part—
# is strictly prohibited.
#
# This code may NOT be:
# - Uploaded to public repositories (GitHub, GitLab, etc.) unless approved by Data Engineering Hub
# - Shared with third parties
# - Used for commercial purposes
#
# Licensed for personal educational use only.
#
# If this code is found on a public repository or distributed without
# authorization, please notify: legal@dataengineeringhub.in

"""
AWS Glue ETL - Raw to Curated Transformation
=============================================
Backup script for DEH Silver Bootcamp Mission 2.
Reads raw NYCTLC Lyft (HVFHV) data, applies data quality filters,
adds derived columns, joins with zone lookup, writes to curated layer.

Usage: Deploy as Glue ETL (Spark) job. Bucket name is auto-detected from account ID.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, hour, dayofweek, date_format, to_date,
    round as spark_round, when, lit, unix_timestamp
)
import boto3
import time
from datetime import datetime

# ---------------------------------------------------------
# Glue Job Init
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']
BUCKET = f"mission-deh-hof-nyctlc-live-{ACCOUNT_ID}"


def write_audit_record(job_name, layer, records_read, records_written, records_rejected, duration, status, error_message=""):
    """Write an audit record to DynamoDB after job completion."""
    dynamodb = boto3.client('dynamodb')
    dynamodb.put_item(
        TableName='mission-deh-hof-job-audit-live',
        Item={
            'job_name': {'S': job_name},
            'run_timestamp': {'S': datetime.now().isoformat()},
            'layer': {'S': layer},
            'records_read': {'N': str(records_read)},
            'records_written': {'N': str(records_written)},
            'records_rejected': {'N': str(records_rejected)},
            'duration_seconds': {'N': str(duration)},
            'status': {'S': status},
            'error_message': {'S': error_message}
        }
    )
    print(f"✅ Audit record written: {job_name} | {status} | {duration}s | {records_written} records")


print("=" * 60)
print("RAW TO CURATED TRANSFORMATION — Lyft (HVFHV)")
print(f"Bucket: {BUCKET}")
print("=" * 60)

start_time = time.time()

try:
    # ---------------------------------------------------------
    # Step 1: Read Raw Data
    # ---------------------------------------------------------
    print("\n📦 Step 1: Reading raw HVFHV trip data...")
    trip_df = spark.read.parquet(f"s3://{BUCKET}/raw/nyctlc/fhvhv_tripdata/")
    raw_count = trip_df.count()
    print(f"   Raw records: {raw_count:,}")

    # Filter for Lyft only (HV0005)
    print("\n🔍 Filtering for Lyft (HV0005) only...")
    trip_df = trip_df.filter(col("hvfhs_license_num") == "HV0005")
    lyft_count = trip_df.count()
    print(f"   Lyft records: {lyft_count:,}")

    print("\n📦 Reading zone lookup...")
    zone_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"s3://{BUCKET}/raw/nyctlc/taxi_zone_lookup/")
    print(f"   Zone records: {zone_df.count()}")

    # ---------------------------------------------------------
    # Step 2: Data Quality Filters
    # ---------------------------------------------------------
    print("\n🔍 Step 2: Applying data quality filters...")

    # Tag records with rejection reason (NULL = good record)
    from pyspark.sql.functions import lit
    tagged_df = trip_df.withColumn(
        "rejection_reason",
        when(col("trip_miles").isNull() | (col("trip_miles") <= 0), lit("trip_miles <= 0"))
        .when(col("trip_miles") >= 200, lit("trip_miles >= 200"))
        .when(col("base_passenger_fare").isNull() | (col("base_passenger_fare") <= 0), lit("base_passenger_fare <= 0"))
        .when(col("base_passenger_fare") >= 500, lit("base_passenger_fare >= 500"))
        .when(col("trip_time").isNull() | (col("trip_time") <= 0), lit("trip_time <= 0"))
        .when(col("trip_time") >= 14400, lit("trip_time >= 14400"))
        .when(col("pickup_datetime").isNull(), lit("pickup_datetime is NULL"))
        .when(col("dropoff_datetime").isNull() | (col("dropoff_datetime") <= col("pickup_datetime")), lit("dropoff <= pickup"))
        .when(col("PULocationID").isNull(), lit("PULocationID is NULL"))
        .when(col("DOLocationID").isNull(), lit("DOLocationID is NULL"))
        .otherwise(None)
    )

    # Split into good and rejected
    trip_df = tagged_df.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    rejected_df = tagged_df.filter(col("rejection_reason").isNotNull())

    filtered_count = trip_df.count()
    rejected_count = rejected_df.count()
    print(f"   Good records: {filtered_count:,}")
    print(f"   Rejected records: {rejected_count:,}")
    print(f"   Rejection rate: {rejected_count / lyft_count * 100:.2f}%")

    # Write rejected records to S3
    if rejected_count > 0:
        rejected_path = f"s3://{BUCKET}/rejected/nyctlc/fhvhv_trips_rejected/"
        rejected_df.write.mode("overwrite").parquet(rejected_path)
        print(f"   ✅ Rejected records written to: {rejected_path}")

    # ---------------------------------------------------------
    # Step 3: Derived Columns
    # ---------------------------------------------------------
    print("\n➕ Step 3: Adding derived columns...")

    trip_df = trip_df.withColumn(
        "trip_duration_minutes",
        spark_round(col("trip_time") / 60.0, 2)
    ).withColumn(
        "pickup_date",
        to_date(col("pickup_datetime"))
    ).withColumn(
        "pickup_hour",
        hour(col("pickup_datetime"))
    ).withColumn(
        "pickup_day_of_week",
        dayofweek(col("pickup_datetime"))
    ).withColumn(
        "is_weekend",
        when(dayofweek(col("pickup_datetime")).isin(1, 7), lit(1)).otherwise(lit(0))
    ).withColumn(
        "speed_mph",
        spark_round(
            when(
                col("trip_duration_minutes") > 0,
                col("trip_miles") / (col("trip_duration_minutes") / 60.0)
            ),
            2
        )
    ).withColumn(
        "is_shared_ride",
        when(col("shared_match_flag") == "Y", lit(1)).otherwise(lit(0))
    ).withColumn(
        "wait_time_minutes",
        spark_round(
            when(
                col("request_datetime").isNotNull() & col("pickup_datetime").isNotNull(),
                (unix_timestamp(col("pickup_datetime")) - unix_timestamp(col("request_datetime"))) / 60.0
            ),
            2
        )
    )

    # Filter out unrealistic speeds
    trip_df = trip_df.filter(
        (col("speed_mph").isNull()) | (col("speed_mph") <= 80)
    )

    # Also filter out trips less than 1 minute or more than 4 hours
    trip_df = trip_df.filter(
        (col("trip_duration_minutes") >= 1) &
        (col("trip_duration_minutes") <= 240)
    )

    print(f"   After speed/duration filter: {trip_df.count():,}")

    # ---------------------------------------------------------
    # Step 4: Join with Zone Lookup
    # ---------------------------------------------------------
    print("\n🔗 Step 4: Joining with zone lookup...")

    # Pickup zone join
    pickup_zone = zone_df.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    )

    trip_df = trip_df.join(pickup_zone, on="PULocationID", how="left")

    # Dropoff zone join
    dropoff_zone = zone_df.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone")
    )

    trip_df = trip_df.join(dropoff_zone, on="DOLocationID", how="left")
    print("   ✅ Zone joins complete.")

    # ---------------------------------------------------------
    # Step 5: Select Final Columns
    # ---------------------------------------------------------
    print("\n📋 Step 5: Selecting final columns...")

    curated_df = trip_df.select(
        # Time
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        # Trip
        "trip_miles",
        "trip_time",
        "trip_duration_minutes",
        "speed_mph",
        # Sharing & Wait
        "is_shared_ride",
        "wait_time_minutes",
        # Pickup Location
        "PULocationID",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        # Dropoff Location
        "DOLocationID",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        # Fare
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        # Metadata
        "dispatching_base_num",
        "originating_base_num",
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag"
    )

    # ---------------------------------------------------------
    # Step 6: Write to Curated Layer
    # ---------------------------------------------------------
    print("\n💾 Step 6: Writing to curated layer...")
    output_path = f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/"

    curated_df.write \
        .mode("overwrite") \
        .partitionBy("pickup_date") \
        .parquet(output_path)

    print(f"   ✅ Written to: {output_path}")

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------
    curated_count = curated_df.count()
    print("\n" + "=" * 60)
    print("✅ RAW TO CURATED COMPLETE!")
    print(f"   Input (raw):     {raw_count:,} records")
    print(f"   Output (curated): {curated_count:,} records")
    print(f"   Output path:     {output_path}")
    print("=" * 60)

    # Write SUCCESS audit record
    end_time = time.time()
    duration = int(end_time - start_time)
    write_audit_record(
        job_name='mission-deh-hof-raw-to-curated-etl',
        layer='curated',
        records_read=lyft_count,
        records_written=curated_count,
        records_rejected=lyft_count - curated_count,
        duration=duration,
        status='SUCCESS'
    )

    job.commit()

except Exception as e:
    end_time = time.time()
    duration = int(end_time - start_time)
    write_audit_record(
        job_name='mission-deh-hof-raw-to-curated-etl',
        layer='curated',
        records_read=0,
        records_written=0,
        records_rejected=0,
        duration=duration,
        status='FAILED',
        error_message=str(e)
    )
    raise
