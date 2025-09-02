import argparse
import sys
import os
from typing import Optional

from loguru import logger
from dotenv import load_dotenv, find_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Load .env early so env vars are present before the Spark JVM starts
load_dotenv(find_dotenv(), override=False)


def build_spark_session(app_name: str = "MorningFlow-FlightDelays") -> SparkSession:
    """
    Build and return a SparkSession with S3 support.

    What it configures:
    - Loads Hadoop AWS and AWS SDK jars at runtime (no manual jar download).
    - Uses DefaultAWSCredentialsProviderChain so your AWS creds can come from
      environment variables, AWS profile (~/.aws/credentials), or SSO.
    - Enables path-style access (safe default; real S3 ignores endpoint).
    """
    logger.info("Building SparkSession for S3 access")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    # Quiet some overly chatty logs while keeping our logs visible
    spark.sparkContext.setLogLevel("WARN")
    return spark


def preflight_aws(profile: Optional[str], bucket: str, object_key: str, region_env: Optional[str]) -> None:
    """
    Validate AWS setup before Spark starts.

    Steps:
    1) If a profile is provided, set AWS_PROFILE so boto3/SDK use it.
    2) Call STS to print the active identity (sanity check for credentials).
    3) Fetch bucket location for region mismatch warning.
    4) HEAD the input object to warn if it's missing.
    """
    if profile:
        os.environ["AWS_PROFILE"] = profile

    try:
        sts = boto3.client("sts")
        ident = sts.get_caller_identity()
        logger.info(f"AWS identity: {ident['Arn']}")
    except ProfileNotFound as e:
        logger.error(f"AWS profile not found: {profile}. Run: aws configure --profile {profile}")
        sys.exit(2)
    except NoCredentialsError:
        logger.error("No AWS credentials found. Set AWS_PROFILE or load .env with AWS_ACCESS_KEY_ID/SECRET.")
        sys.exit(2)

    s3 = boto3.client("s3")
    # Bucket region and access
    try:
        loc_resp = s3.get_bucket_location(Bucket=bucket)
        bucket_region = loc_resp.get("LocationConstraint") or "us-east-1"
        if region_env and region_env != bucket_region:
            logger.warning(f"AWS_REGION ({region_env}) differs from bucket region ({bucket_region}).")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("AccessDenied", "403", "401"):
            logger.error(
                f"Access denied to bucket '{bucket}'. Ensure IAM policy includes s3:ListBucket on arn:aws:s3:::{bucket}."
            )
            raise
        if code in ("NoSuchBucket",):
            logger.error(f"Bucket does not exist: {bucket}")
            raise
        raise

    # Input object presence
    try:
        s3.head_object(Bucket=bucket, Key=object_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            logger.warning(
                f"Input not found: s3://{bucket}/{object_key}. If not uploaded, run:\n"
                f"aws s3 cp data/departuredelays.csv s3://{bucket}/{object_key}"
            )
        elif code in ("AccessDenied", "403"):
            logger.error(
                f"Access denied to object. Ensure s3:GetObject on arn:aws:s3:::{bucket}/{object_key}"
            )
        else:
            raise


def parse_date_column(df: DataFrame) -> DataFrame:
    """
    Normalize the dataset's date column.

    Source format:
    - The sample uses MMddHHmm without a year (e.g., 1011245 -> 01-01 12:45).

    Strategy:
    - Left-pad to 8 chars; prefix assumed year (2015), then parse as yyyyMMddHHmm.
    - Derive `event_date`, `year`, `month`, `day` for partitioning/aggregations.
    """
    date_str = F.col("date").cast("string")
    # Pad to 8 chars for MMddHHmm -> then prefix assumed year (2015)
    mmddhhmm = F.lpad(date_str, 8, "0")
    yyyymmddhhmm = F.concat(F.lit("2015"), mmddhhmm)
    event_ts = F.to_timestamp(yyyymmddhhmm, "yyyyMMddHHmm")
    event_date = F.to_date(event_ts)
    df2 = (
        df.withColumn("event_date", event_date)
          .withColumn("year", F.year("event_date"))
          .withColumn("month", F.month("event_date"))
          .withColumn("day", F.dayofmonth("event_date"))
    )
    return df2


def read_flight_delays_csv(spark: SparkSession, s3_uri: str) -> DataFrame:
    """
    Read the flight delays CSV from S3. Expected columns typically include:
    date, delay, distance, origin, destination
    """
    logger.info(f"Reading flight delays CSV from {s3_uri}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(s3_uri)
    )

    # Basic cleanup and type normalization
    df = (
        df.withColumn("delay", F.col("delay").cast("int"))
          .withColumn("distance", F.col("distance").cast("int"))
          .withColumn("origin", F.upper(F.col("origin")))
          .withColumn("destination", F.upper(F.col("destination")))
    )
    df = df.dropna(subset=["origin", "destination", "delay"])  # keep essential rows only
    df = parse_date_column(df)
    return df


def write_parquet(df: DataFrame, output_path: str, partitions: Optional[list] = None) -> None:
    writer = df.write.mode("overwrite").format("parquet")
    if partitions:
        writer = writer.partitionBy(*partitions)
    logger.info(f"Writing Parquet to {output_path}")
    writer.save(output_path)


def compute_and_write_aggregations(df: DataFrame, output_base: str) -> None:
    """
    Compute several commonly used aggregations and write them to S3 as Parquet.
    """
    # 1) Average delay by origin
    avg_delay_by_origin = (
        df.groupBy("origin").agg(F.avg("delay").alias("avg_delay"))
          .orderBy(F.desc("avg_delay"))
    )
    write_parquet(avg_delay_by_origin, f"{output_base}/avg_delay_by_origin")

    # 2) Average delay by destination
    avg_delay_by_destination = (
        df.groupBy("destination").agg(F.avg("delay").alias("avg_delay"))
          .orderBy(F.desc("avg_delay"))
    )
    write_parquet(avg_delay_by_destination, f"{output_base}/avg_delay_by_destination")

    # 3) Top delayed routes (origin->destination) with at least 100 flights
    route_stats = (
        df.groupBy("origin", "destination")
          .agg(
              F.count(F.lit(1)).alias("num_flights"),
              F.avg("delay").alias("avg_delay"),
              F.sum(F.when(F.col("delay") > 15, 1).otherwise(0)).alias("num_delayed_15m")
          )
          .filter(F.col("num_flights") >= 100)
          .orderBy(F.desc("avg_delay"))
    )
    write_parquet(route_stats, f"{output_base}/route_stats")

    # 4) Daily delayed (>15m) counts
    daily_delay_counts = (
        df.withColumn("is_delayed_15m", (F.col("delay") > 15).cast("int"))
          .groupBy("event_date", "year", "month", "day")
          .agg(F.sum("is_delayed_15m").alias("delayed_15m_count"))
          .orderBy("event_date")
    )
    write_parquet(daily_delay_counts, f"{output_base}/daily_delay_counts", partitions=["year", "month"])  # partition for efficient queries


def main() -> None:
    parser = argparse.ArgumentParser(description="Flight delays Spark job (S3 read/write)")
    parser.add_argument(
        "--bucket",
        required=False,
        help="Target S3 bucket name (no s3:// prefix). If omitted, uses BUCKET/AWS_S3_BUCKET/S3_BUCKET env vars.",
    )
    parser.add_argument(
        "--profile",
        required=False,
        help="AWS profile to use for this run (sets AWS_PROFILE).",
    )
    parser.add_argument(
        "--region",
        required=False,
        help="AWS region to use (sets AWS_REGION/AWS_DEFAULT_REGION).",
    )
    parser.add_argument(
        "--input-prefix",
        default="flights/raw",
        help="Input S3 prefix where departuredelays.csv resides",
    )
    parser.add_argument(
        "--output-prefix",
        default="flights/output",
        help="Output S3 prefix base for results",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="Limit number of rows to process (0 = all).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=None,
        help="If set, configures spark.sql.shuffle.partitions to this value.",
    )
    args = parser.parse_args()

    # Resolve bucket from CLI or environment (.env is loaded above)
    bucket = args.bucket or os.getenv("BUCKET") or os.getenv("AWS_S3_BUCKET") or os.getenv("S3_BUCKET")
    if not bucket:
        parser.error(
            "--bucket is required when no BUCKET/AWS_S3_BUCKET/S3_BUCKET env var is set."
        )
    input_key = f"{args.input_prefix}/departuredelays.csv"
    input_uri = f"s3a://{bucket}/{input_key}"
    output_base = f"s3a://{bucket}/{args.output_prefix}"

    # Optional env region override
    if args.region:
        os.environ.setdefault("AWS_REGION", args.region)
        os.environ.setdefault("AWS_DEFAULT_REGION", args.region)

    # Preflight checks (credentials, bucket region, input presence)
    try:
        preflight_aws(args.profile, bucket, input_key, os.getenv("AWS_REGION"))
    except Exception:
        logger.error("Preflight AWS checks failed. See messages above.")
        raise

    spark = build_spark_session()
    # Optional Spark tuning
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        if args.shuffle_partitions is not None:
            spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    except Exception:
        pass
    try:
        df = read_flight_delays_csv(spark, input_uri)
        if args.sample and args.sample > 0:
            df = df.limit(args.sample)

        logger.info("Sample records from input data:")
        df.show(10, truncate=False)

        compute_and_write_aggregations(df, output_base)

        # Confirm some outputs
        logger.info("Reading back a sample of avg_delay_by_origin")
        spark.read.parquet(f"{output_base}/avg_delay_by_origin").show(10, truncate=False)
        logger.success("Flight delays job completed successfully.")
    except Exception as exc:  # keep simple for bootcamp level, but log clearly
        msg = str(exc)
        if "AccessDenied" in msg or "403" in msg:
            logger.error(
                f"Access denied. Ensure IAM allows: s3:ListBucket on arn:aws:s3:::{bucket} and s3:GetObject/s3:PutObject on arn:aws:s3:::{bucket}/*"
            )
        logger.exception(f"Job failed: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


