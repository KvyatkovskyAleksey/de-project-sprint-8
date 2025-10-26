from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import DataFrame


TOPIC_IN = "student.topic.cohort39.KvaytkovskyAleksey_in"
TOPIC_OUT = "student.topic.cohort39.KvaytkovskyAleksey_out"


kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    "kafka.bootstrap.servers": "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091",
}


def init_spark():
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    return (
        SparkSession.builder.appName("RestaurantSubscribeStreamingService")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )


def read_stream_df(spark) -> DataFrame:
    # читаем из топика Kafka сообщения с акциями от ресторанов

    # определяем схему входного сообщения для json
    incoming_message_schema = StructType(
        [
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", LongType()),
            StructField("adv_campaign_datetime_end", LongType()),
            StructField("datetime_created", LongType()),
        ]
    )

    return (
        spark.readStream.format("kafka")
        .options(**kafka_security_options)
        .option("subscribe", TOPIC_IN)
        .load()
        # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
        .select(
            F.from_json(F.col("value").cast("string"), incoming_message_schema).alias(
                "parsed_value"
            )
        )
        .select("parsed_value.*")
        .withColumn("event_time", F.from_unixtime("datetime_created").cast("timestamp"))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["restaurant_id", "adv_campaign_id"])
        .withColumn(
            "trigger_datetime_created", F.round(F.unix_timestamp(F.current_timestamp()))
        )
        .where(
            (F.col("adv_campaign_datetime_start") <= F.col("trigger_datetime_created"))
            & (F.col("adv_campaign_datetime_end") >= F.col("trigger_datetime_created"))
        )
    )


def read_restaurants_subscribers(spark: SparkSession):
    # вычитываем всех пользователей с подпиской на рестораны
    return (
        spark.read.format("jdbc")
        .option(
            "url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
        )
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "subscribers_restaurants")
        .option("user", "student")
        .option("password", "de-student")
        .load()
        .select("restaurant_id", "client_id")
        .distinct()
    )


def join_restaurants_with_users(
    restaurants_subscribers_df: DataFrame, stream_df: DataFrame
) -> DataFrame:
    """Join users and restaurants dataframes."""
    return stream_df.join(
        restaurants_subscribers_df,
        on=["restaurant_id"],
        how="inner",
    ).withColumn("date_created", F.current_date())


def write_to_postgres(result_df: DataFrame) -> None:
    # write to the postgres database
    (
        result_df.withColumn("feedback", F.lit(None).cast(StringType()))
        .write.mode("append")
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/de")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "subscribers_feedback")
        .option("user", "jovyan")
        .option("password", "jovyan")
        .save()
    )


def send_data_to_kafka(result_df: DataFrame):
    result_df.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.col("restaurant_id"),
                F.col("adv_campaign_id"),
                F.col("adv_campaign_content"),
                F.col("adv_campaign_owner"),
                F.col("adv_campaign_owner_contact"),
                F.col("adv_campaign_datetime_start"),
                F.col("adv_campaign_datetime_end"),
                F.col("client_id"),
                F.col("datetime_created"),
                F.col("trigger_datetime_created"),
            )
        ),
    ).select("value").write.format("kafka").options(**kafka_security_options).option(
        "topic", TOPIC_OUT
    ).save()


def process_batch(result_df: DataFrame, _: int) -> None:
    """Process batch data."""
    result_df = result_df.select(
        "restaurant_id",
        "adv_campaign_id",
        "adv_campaign_content",
        "adv_campaign_owner",
        "adv_campaign_owner_contact",
        "adv_campaign_datetime_start",
        "adv_campaign_datetime_end",
        "datetime_created",
        "client_id",
        "trigger_datetime_created",
    )
    result_df.persist()
    write_to_postgres(result_df)
    send_data_to_kafka(result_df)
    result_df.unpersist()


if __name__ == "__main__":
    spark_ = init_spark()
    adv_df = read_stream_df(spark_)
    restaurants_subscribers_df_ = read_restaurants_subscribers(spark_)
    joined_df = join_restaurants_with_users(restaurants_subscribers_df_, adv_df)
    query = (
        joined_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/test_subscriber_feedback_checkpoint")
        .trigger(processingTime="1 minute")
        .start()
        .awaitTermination()
    )
