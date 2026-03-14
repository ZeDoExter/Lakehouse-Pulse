import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def build_spark_session() -> SparkSession:
    app_name = env("SPARK_APP_NAME", "lakehouse-pulse-transform")
    hive_metastore = env("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    hdfs_default_fs = env("HDFS_DEFAULT_FS", "hdfs://namenode:9000")
    warehouse_dir = env("HIVE_WAREHOUSE_DIR", "hdfs://namenode:9000/user/hive/warehouse")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.defaultFS", hdfs_default_fs)
        .config("spark.hadoop.hive.metastore.uris", hive_metastore)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    return spark


def openaq_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("source", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("fetched_at", T.StringType(), True),
            T.StructField(
                "result",
                T.StructType(
                    [
                        T.StructField("location", T.StringType(), True),
                        T.StructField("city", T.StringType(), True),
                        T.StructField("country", T.StringType(), True),
                        T.StructField(
                            "coordinates",
                            T.StructType(
                                [
                                    T.StructField("latitude", T.DoubleType(), True),
                                    T.StructField("longitude", T.DoubleType(), True),
                                ]
                            ),
                            True,
                        ),
                        T.StructField(
                            "measurements",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("parameter", T.StringType(), True),
                                        T.StructField("value", T.DoubleType(), True),
                                        T.StructField("unit", T.StringType(), True),
                                        T.StructField("lastUpdated", T.StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )


def process_batch(
    spark: SparkSession,
    raw_batch: DataFrame,
    batch_id: int,
    raw_zone_path: str,
    processed_zone_path: str,
    curated_zone_path: str,
    hive_database: str,
    hive_table: str,
) -> None:
    if raw_batch.rdd.isEmpty():
        print(f"{datetime.now(timezone.utc).isoformat()} batch_id={batch_id} records=0 status=empty")
        return

    now = datetime.now(timezone.utc).isoformat()
    payload_batch = raw_batch.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.current_timestamp().alias("ingested_at"),
    )
    payload_batch.write.mode("append").parquet(raw_zone_path)

    parsed = payload_batch.withColumn("doc", F.from_json(F.col("payload"), openaq_schema())).filter(F.col("doc").isNotNull())
    exploded = parsed.withColumn("measurement", F.explode_outer(F.col("doc.result.measurements")))

    processed = (
        exploded.select(
            F.to_timestamp(F.col("doc.fetched_at")).alias("fetched_at"),
            F.col("doc.country").alias("source_country"),
            F.col("doc.result.country").alias("country"),
            F.col("doc.result.city").alias("city"),
            F.col("doc.result.location").alias("location"),
            F.col("doc.result.coordinates.latitude").alias("latitude"),
            F.col("doc.result.coordinates.longitude").alias("longitude"),
            F.col("measurement.parameter").alias("parameter"),
            F.col("measurement.value").cast("double").alias("value"),
            F.col("measurement.unit").alias("unit"),
            F.to_timestamp(F.col("measurement.lastUpdated")).alias("measurement_time"),
            F.col("kafka_timestamp"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("ingested_at"),
        )
        .filter(F.col("parameter").isNotNull() & F.col("value").isNotNull())
        .withColumn("event_date", F.to_date(F.coalesce(F.col("measurement_time"), F.col("fetched_at"), F.col("ingested_at"))))
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("city"), F.lit("")),
                    F.coalesce(F.col("location"), F.lit("")),
                    F.coalesce(F.col("parameter"), F.lit("")),
                    F.col("value").cast("string"),
                    F.coalesce(F.col("measurement_time").cast("string"), F.lit("")),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                ),
                256,
            ),
        )
        .dropDuplicates(["event_id"])
    )

    processed.write.mode("append").partitionBy("event_date").parquet(processed_zone_path)

    curated = (
        processed.groupBy("event_date", "city", "parameter", "unit")
        .agg(
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("value").alias("min_value"),
            F.count(F.lit(1)).alias("sample_count"),
            F.max("measurement_time").alias("last_measurement_time"),
        )
        .withColumn("snapshot_ts", F.current_timestamp())
        .withColumn("snapshot_date", F.to_date(F.col("snapshot_ts")))
    )

    curated.write.mode("append").partitionBy("snapshot_date").parquet(curated_zone_path)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database}")
    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {hive_database}.{hive_table} (
            event_date DATE,
            city STRING,
            parameter STRING,
            unit STRING,
            avg_value DOUBLE,
            max_value DOUBLE,
            min_value DOUBLE,
            sample_count BIGINT,
            last_measurement_time TIMESTAMP,
            snapshot_ts TIMESTAMP
        )
        PARTITIONED BY (snapshot_date DATE)
        STORED AS PARQUET
        LOCATION '{curated_zone_path}'
        """
    )
    spark.sql(f"MSCK REPAIR TABLE {hive_database}.{hive_table}")

    total_processed = processed.count()
    total_curated = curated.count()
    print(
        f"{now} batch_id={batch_id} status=ok "
        f"processed_rows={total_processed} curated_rows={total_curated}"
    )


def main() -> None:
    spark = build_spark_session()

    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic = env("KAFKA_TOPIC", "openaq.th.latest")
    starting_offsets = env("KAFKA_STARTING_OFFSETS", "latest")
    max_offsets = env("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")

    raw_zone_path = env("HDFS_RAW_PATH", "hdfs://namenode:9000/lakehouse/raw/air_quality")
    processed_zone_path = env("HDFS_PROCESSED_PATH", "hdfs://namenode:9000/lakehouse/processed/air_quality")
    curated_zone_path = env("HDFS_CURATED_PATH", "hdfs://namenode:9000/lakehouse/curated/air_quality")
    checkpoint_path = env("SPARK_CHECKPOINT_PATH", "hdfs://namenode:9000/lakehouse/checkpoints/air_quality_transform")

    hive_database = env("HIVE_DATABASE", "lakehouse_pulse")
    hive_table = env("HIVE_TABLE", "air_quality_curated")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        kafka_stream.writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(
                spark,
                batch_df,
                batch_id,
                raw_zone_path,
                processed_zone_path,
                curated_zone_path,
                hive_database,
                hive_table,
            )
        )
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
