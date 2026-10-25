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
AWS Glue ETL - Curated to Aggregated Transformation
====================================================
Backup script for DEH Silver Bootcamp Mission 2.
Reads curated NYCTLC Lyft data and produces 3 aggregate tables:
1. Daily Borough Summary
2. Hourly Pattern Summary
3. Route Summary

Usage: Deploy as Glue ETL (Spark) job. Bucket name is auto-detected from account ID.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round, when
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
print("CURATED TO AGGREGATED TRANSFORMATION — Lyft")
print(f"Bucket: {BUCKET}")
print("=" * 60)

start_time = time.time()

try:
    # ---------------------------------------------------------
    # Read Curated Data
    # ---------------------------------------------------------
    print("\n📦 Reading curated HVFHV data...")
    curated_df = spark.read.parquet(f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/")
    total_records = curated_df.count()
    print(f"   Curated records: {total_records:,}")

    # ---------------------------------------------------------
    # Aggregate 1: Daily Borough Summary
    # ---------------------------------------------------------
    print("\n📊 Aggregate 1: Daily Borough Summary...")

    daily_borough = curated_df.groupBy("pickup_date", "pickup_borough").agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("base_passenger_fare"), 2).alias("total_revenue"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("tips"), 2).alias("avg_tip"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        spark_round(avg("speed_mph"), 2).alias("avg_speed_mph")
    )

    output_1 = f"s3://{BUCKET}/aggregated/nyctlc/daily_borough_summary/"
    daily_borough.write.mode("overwrite") \
        .partitionBy("pickup_date") \
        .parquet(output_1)

    daily_count = daily_borough.count()
    print(f"   ✅ Written: {daily_count:,} rows → {output_1}")

    # ---------------------------------------------------------
    # Aggregate 2: Hourly Pattern Summary
    # ---------------------------------------------------------
    print("\n📊 Aggregate 2: Hourly Pattern Summary...")

    hourly_pattern = curated_df.groupBy("pickup_hour", "is_weekend", "pickup_borough").agg(
        count("*").alias("total_trips"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg(
            when(col("base_passenger_fare") > 0, col("tips") / col("base_passenger_fare") * 100)
        ), 2).alias("avg_tip_percentage"),
        spark_round(avg("speed_mph"), 2).alias("avg_speed_mph"),
        spark_round(avg("wait_time_minutes"), 2).alias("avg_wait_time_minutes")
    )

    output_2 = f"s3://{BUCKET}/aggregated/nyctlc/hourly_pattern_summary/"
    hourly_pattern.write.mode("overwrite") \
        .parquet(output_2)

    hourly_count = hourly_pattern.count()
    print(f"   ✅ Written: {hourly_count:,} rows → {output_2}")

    # ---------------------------------------------------------
    # Aggregate 3: Route Summary
    # ---------------------------------------------------------
    print("\n📊 Aggregate 3: Route Summary...")

    route_summary = curated_df.groupBy(
        "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"
    ).agg(
        count("*").alias("total_trips"),
        spark_round(avg("base_passenger_fare"), 2).alias("avg_fare"),
        spark_round(avg("trip_miles"), 2).alias("avg_trip_miles"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
    ).filter(col("total_trips") >= 10)

    output_3 = f"s3://{BUCKET}/aggregated/nyctlc/route_summary/"
    route_summary.write.mode("overwrite") \
        .parquet(output_3)

    route_count = route_summary.count()
    print(f"   ✅ Written: {route_count:,} rows → {output_3}")

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------
    records_written = daily_count + hourly_count + route_count
    print("\n" + "=" * 60)
    print("✅ CURATED TO AGGREGATED COMPLETE!")
    print(f"   Input:  {total_records:,} curated records")
    print(f"   Output: 3 aggregate tables written to s3://{BUCKET}/aggregated/")
    print("   Tables:")
    print(f"     1. daily_borough_summary → {output_1}")
    print(f"     2. hourly_pattern_summary → {output_2}")
    print(f"     3. route_summary → {output_3}")
    print("=" * 60)

    # Write SUCCESS audit record
    end_time = time.time()
    duration = int(end_time - start_time)
    write_audit_record(
        job_name='mission-deh-hof-curated-to-aggregated-etl',
        layer='aggregated',
        records_read=total_records,
        records_written=records_written,
        records_rejected=0,
        duration=duration,
        status='SUCCESS'
    )

    job.commit()

except Exception as e:
    end_time = time.time()
    duration = int(end_time - start_time)
    write_audit_record(
        job_name='mission-deh-hof-curated-to-aggregated-etl',
        layer='aggregated',
        records_read=0,
        records_written=0,
        records_rejected=0,
        duration=duration,
        status='FAILED',
        error_message=str(e)
    )
    raise
