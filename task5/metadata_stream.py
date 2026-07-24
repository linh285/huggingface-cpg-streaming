#!/usr/bin/env python3
"""Task 5: stream source metadata from Kafka into MongoDB with Spark."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def metadata_schema() -> StructType:
    """Return the Task 2 metadata event schema agreed by the team."""
    return StructType(
        [
            StructField("schema_version", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("file_id", StringType(), False),
            StructField("repository", StringType(), False),
            StructField("path", StringType(), False),
            StructField("language", StringType(), False),
            StructField("size_bytes", LongType(), False),
            StructField("line_count", IntegerType(), False),
            StructField("content_sha256", StringType(), False),
            StructField("ast_node_count", IntegerType(), False),
            StructField("ast_edge_count", IntegerType(), False),
            StructField("cfg_edge_count", IntegerType(), False),
            StructField("dfg_edge_count", IntegerType(), False),
            StructField("call_edge_count", IntegerType(), False),
            StructField("status", StringType(), False),
        ]
    )


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CPGMetadataKafkaToMongoDB")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_METADATA_TOPIC", "cpg.metadata")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
    mongodb_database = os.getenv("MONGODB_DATABASE", "cpg")
    mongodb_collection = os.getenv("MONGODB_COLLECTION", "source_metadata")

    checkpoint = os.getenv(
        "CHECKPOINT_LOCATION", "/opt/spark-checkpoints/cpg-metadata"
    )
    trigger = os.getenv("STREAM_TRIGGER", "5 seconds")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    decoded_events = raw_events.select(
        from_json(col("value").cast("string"), metadata_schema()).alias("event"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # _id=file_id makes MongoDB replace/upsert the same file instead of inserting
    # another document when Task 2 reprocesses that file.
    metadata = (
        decoded_events.where(
            col("event").isNotNull()
            & (col("event.event_type") == "FILE_METADATA_UPSERT")
            & (col("event.schema_version") == "1.0.0")
            & col("event.file_id").isNotNull()
        )
        .select(
            col("event.file_id").alias("_id"),
            "event.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("event_time_utc", to_timestamp(col("event_time")))
        .withColumn("processed_at", current_timestamp())
    )

    query = (
        metadata.writeStream.format("mongodb")
        .queryName("cpg_metadata_to_mongodb")
        .option("connection.uri", mongodb_uri)
        .option("database", mongodb_database)
        .option("collection", mongodb_collection)
        .option("operationType", "replace")
        .option("idFieldList", "_id")
        .option("upsertDocument", "true")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(processingTime=trigger)
        .start()
    )

    print(
        "[TASK 5] Streaming started: "
        f"{kafka_topic} -> {mongodb_database}.{mongodb_collection}; "
        f"checkpoint={checkpoint}",
        flush=True,
    )

    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
