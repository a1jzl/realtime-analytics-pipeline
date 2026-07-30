"""Job Spark Structured Streaming: agregation des transactions en temps reel.

Consomme le topic Kafka 'transactions', calcule le chiffre d'affaires
par fenetre glissante et par categorie, puis ecrit les resultats dans
PostgreSQL via JDBC.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, TimestampType

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"
JDBC_URL = "jdbc:postgresql://localhost:5432/analytics"

SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
        StructField("city", StringType()),
        StructField("timestamp", TimestampType()),
    ]
)


def write_to_postgres(batch_df, batch_id: int) -> None:
    batch_df.write.jdbc(
        url=JDBC_URL,
        table="revenue_by_window",
        mode="append",
        properties={"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"},
    )


def main() -> None:
    spark = SparkSession.builder.appName("RealtimeAnalyticsPipeline").getOrCreate()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = raw_stream.select(from_json(col("value").cast("string"), SCHEMA).alias("data")).select("data.*")

    aggregated = (
        events.withWatermark("timestamp", "1 minute")
        .groupBy(window(col("timestamp"), "1 minute"), col("category"))
        .agg(spark_sum("amount").alias("revenue"))
    )

    query = (
        aggregated.writeStream.foreachBatch(write_to_postgres)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
