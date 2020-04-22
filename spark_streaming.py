from configparser import ConfigParser
from pathlib import Path

import pyspark.sql.functions as psf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from utils import get_logger, load_config

logger = get_logger(__file__)

RADIO_CODE_JSON_FILEPATH = str(Path(__file__).parent / "data" / "radio_code.json")

KAFKA_SCHEMA = StructType(
    [
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True),
    ]
)

RADIO_CODE_SCHEMA = StructType(
    [
        StructField("disposition_code", StringType(), False),
        StructField("description", StringType(), True),
    ]
)


def build_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
    """Create a Spark Session

    Args:
        app_name (str): name of the Spark application
        master (str): Spark master URL

    Returns:
        pyspark.sql.SparkSession
    """
    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_sf_crime_stats_spark_job(
    spark: SparkSession, config: ConfigParser, radio_code_json_filepath: str
):
    """Start the San Franciso Spark streaming job

    Args:
        spark (SparkSession): a Spark session
        config (ConfigParser): A ConigParse for configuration
        radio_code_json_filepath (str): path the the radico code JSON data
    """

    df: DataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"].get("bootstrap_servers"))
        .option("subscribe", config["kafka"].get("topic"))
        .option("startingOffsets", config["spark"].get("startingOffsets"))
        .option("maxOffsetsPerTrigger", config["spark"].getint("maxOffsetsPerTrigger"))
        .load()
    )

    logger.info("Print source data schema")
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), KAFKA_SCHEMA).alias("crime_df")
    ).select("crime_df.*")

    distinct_table = service_table.selectExpr("original_crime_type_name", "disposition")

    # count the number of original crime type
    agg_df = (
        distinct_table.groupBy("original_crime_type_name", "disposition",)
        .count()
        .sort("count", ascending=False)
    )

    radio_code_df: DataFrame = (
        spark.read.option("multiline", "true")
        .schema(RADIO_CODE_SCHEMA)
        .json(radio_code_json_filepath)
    )
    renamed_radio_code_df = radio_code_df.withColumnRenamed(
        "disposition_code", "disposition"
    )

    join_df = agg_df.join(renamed_radio_code_df, on="disposition", how="inner").select(
        "original_crime_type_name", "disposition", "description", "count"
    )
    join_query: StreamingQuery = (
        join_df.writeStream.outputMode("complete")
        .trigger(processingTime="5 seconds")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    config = load_config()
    spark = build_spark_session(app_name="SF Crime Stats Streaming")

    logger.info("Spark started")
    try:
        run_sf_crime_stats_spark_job(spark, config, RADIO_CODE_JSON_FILEPATH)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        spark.stop()
