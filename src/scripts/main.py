import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# # метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
# def foreach_batch_function(df, epoch_id):
#     # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
#     ...
#     # записываем df в PostgreSQL с полем feedback
#     ...
#     # создаём df для отправки в Kafka. Сериализация в json.
#     ...
#     # отправляем сообщения в результирующий топик Kafka без поля feedback
#     ...
#     # очищаем память от df
#     ...
# 
# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL

TOPIC_IN = "student.topic.cohort39.KvaytkovskyAleksey_in"


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
    return SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()

def read_stream_df(spark) -> DataFrame:
    # читаем из топика Kafka сообщения с акциями от ресторанов

    # определяем схему входного сообщения для json
    incoming_message_schema = StructType([
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", LongType()),
            StructField("adv_campaign_datetime_end", LongType()),
            StructField("datetime_created", LongType()),
        ])

    restaurant_read_stream_df = (spark.readStream
                                 .format('kafka')
                                 .options(**kafka_security_options)
                                 .option('subscribe', 'student.topic.cohort39.KvaytkovskyAleksey_in')
                                 .load()
                                 .select(F.from_json(F.col('value').cast('string'), incoming_message_schema).alias('parsed_value')))

# # определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))
# 
# # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
# filtered_read_stream_df = ....
# 
# # вычитываем всех пользователей с подпиской на рестораны
# subscribers_restaurant_df = spark.read \
#                     .format('jdbc') \
#                       .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
#                     .option('driver', 'org.postgresql.Driver') \
#                     .option('dbtable', 'subscribers_restaurants') \
#                     .option('user', 'student') \
#                     .option('password', 'de-student') \
#                     .load()
# 
# # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
# result_df = ...
# # запускаем стриминг
# result_df.writeStream \
#     .foreachBatch(foreach_batch_function) \
#     .start() \
#     .awaitTermination()