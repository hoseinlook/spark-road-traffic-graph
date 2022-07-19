from . import config
import os
from abc import ABC, abstractmethod, abstractclassmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws, udf, from_json, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DateType


class Pipeline(ABC):
    _spark = None
    """
        An ETL class to read data from kafka and preprocess on it and write data to kafka

    """

    @property
    @abstractmethod
    def spark_configs(self) -> dict:
        pass

    @property
    @abstractmethod
    def kafka_read_configs(self) -> dict:
        pass

    @property
    @abstractmethod
    def kafka_write_configs(self) -> dict:
        pass

    @property
    @abstractmethod
    def topic(self) -> str:
        pass

    @property
    @abstractmethod
    def reading_schema(self) -> StructType:
        pass

    @property
    def spark(self) -> SparkSession:
        """
        :return:  A singleton sparkSession object
        """
        if self._spark is not None:
            return self._spark

        builder = SparkSession.builder
        for key, value in self.spark_configs.items():
            builder.config(key, value)

        # return spark session
        self._spark = builder.getOrCreate()
        return self._spark

    def extract_from_kafka(self) -> DataFrame:
        df = self.spark.readStream.format('kafka').options(**self.kafka_read_configs).load()
        df = df.withColumn("data", from_json(col("value").cast("string"), schema=self.reading_schema, options={'allowUnquotedControlChars': 'true'}).alias("data")) \
            .select(col('key').cast("string").alias("_key"), "data.*")
        return df

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    def load_to_kafka(self, df: DataFrame):
        df = df.withColumnRenamed("_key", "key")
        df = df.withColumn("value", to_json(struct('*').dropFields("key")))
        df.writeStream.format('kafka').options(**self.kafka_write_configs).start()
        self.spark.streams.awaitAnyTermination()

    def start(self):
        self.load_to_kafka(self.transform(self.extract_from_kafka()))


class TehranPipeline(Pipeline):
    spark_configs = {
        'spark.app.name': 'tehran-pipeline',
        'spark.master': 'local',
        # PyArrow
        'spark.sql.execution.arrow.pyspark.enabled': 'true',
        # Dynamic Allocation
        'spark.shuffle.service.enabled': 'true',
        'spark.dynamicAllocation.enabled': 'true',
        'spark.dynamicAllocation.initialExecutors': 1,

        # Backpressure
        'spark.streaming.backpressure.enabled': 'true',

    }
    topic = "tehran-locations"

    kafka_read_configs = {
        'failOnDataLoss': 'false',
        'startingOffsets': 'earliest',

        # kafka connection
        'kafka.bootstrap.servers': os.getenv('KAFKA_SERVERS', '127.0.0.1:9093'),

        # topic
        'subscribe': topic,
        # 'groupIdPrefix': topic,
        # 'maxOffsetsPerTrigger': int(os.getenv('KAFKA_MAX_OFFSETS_PER_TRIGGER', 10000)),
        # 'fetch.max.bytes': '104857600',

    }

    kafka_write_configs = {
        'kafka.bootstrap.servers': os.getenv('KAFKA_SERVERS', '127.0.0.1:9093'),
        'checkpointLocation': config.PATH_CHECKPOINT,
        "topic": f"{topic}-result"
    }

    reading_schema = StructType([
        StructField("driverId", StringType()),
        StructField("lat", StringType()),
        StructField("long", StringType()),
        StructField("time", DateType()),

    ])

    def transform(self, df: DataFrame) -> DataFrame:
        return df
