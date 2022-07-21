import hashlib
import os
from . import config
from pyspark.sql.pandas.functions import pandas_udf
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, udf, from_json, struct, to_json, window, collect_list, hour, dayofweek, concat, explode, max
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, ArrayType, MapType, IntegerType
import pandas as pd
from .points import TEHRAN_POINTS, Point


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

    def generate_output_key(self, df: DataFrame) -> DataFrame:
        return df

    def load_to_kafka(self, df: DataFrame):
        df = df.withColumnRenamed("_key", "key")
        df = df.withColumn("value", to_json(struct('*').dropFields("key")))
        df = self.generate_output_key(df)
        df.writeStream.format('kafka').outputMode("update").options(**self.kafka_write_configs).start()
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
        # read policy
        'failOnDataLoss': 'false',
        'startingOffsets': 'earliest',

        # kafka connection
        'kafka.bootstrap.servers': config.KAFKA_SERVERS,

        # topic
        'subscribe': topic,
        'maxOffsetsPerTrigger': int(os.getenv('KAFKA_MAX_OFFSETS_PER_TRIGGER', 200)),

    }

    kafka_write_configs = {
        'kafka.bootstrap.servers': config.KAFKA_SERVERS,
        'checkpointLocation': config.PATH_CHECKPOINT,
        "topic": f"{topic}-result"
    }

    reading_schema = StructType([
        StructField("driverId", StringType()),
        StructField("lat", FloatType()),
        StructField("long", FloatType()),
        StructField("time", DateType()),

    ])

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("assigned_point", self.assign_closet_point(col("lat"), col('long'))). \
            withColumn("dayOfWeek_Hour", concat(dayofweek(col("time")), lit("_"), hour(col("time")))). \
            groupBy("driverId", "dayOfWeek_Hour").agg(collect_list(col("assigned_point")).alias("assigned_point"), max(col("time").cast("timestamp")).alias("last_time")).withWatermark("last_time", "2 hours")

        df = df.withColumn("Y", self.generate_graph_edges(col("assigned_point")))
        df = df.withColumn("u", explode(col("Y"))).select("driverId", "dayOfWeek_Hour", "u.*")
        # df = df.groupBy("dayOfWeek_Hour", "p1", "p2").agg(avg(col("edge").alias("edge_on_avg")))

        return df

    @staticmethod
    @pandas_udf(StringType(), None)
    def assign_closet_point(lat_series: pd.Series, long_series: pd.Series) -> pd.Series:
        df = pd.DataFrame({"lat": lat_series, "long": long_series})
        return df.apply(lambda item: min(TEHRAN_POINTS, key=lambda p: Point.distance(p, Point(item["lat"], item["long"], ''))).label, axis=1)

    # MapType(keyType=StringType(), valueType=Union[StringType(), IntegerType()]))
    @staticmethod
    @udf(returnType=ArrayType(
        StructType([
            StructField("p1", StringType()),
            StructField("p2", StringType()),
            StructField("edge", IntegerType()),
        ])))
    def generate_graph_edges(list_of_point_labels: list):
        weights = {label: list_of_point_labels.count(label) for label in set(list_of_point_labels)}
        result = []
        last_point = list_of_point_labels[0]

        for p in list_of_point_labels[1:]:
            if p != last_point:
                result.append({"p1": last_point, "p2": p, "edge": weights[last_point]})
                last_point = p

        return result

    def generate_output_key(self, df: DataFrame) -> DataFrame:
        @pandas_udf(StringType(), None)
        def unique_md5_key(dayOfWeek_Hour_series: pd.Series, p1_series: pd.Series, p2_series: pd.Series) -> pd.Series:
            df = pd.DataFrame({"dayOfWeek_Hour_series": dayOfWeek_Hour_series, "p1_series": p1_series, "p2_series": p2_series})
            df["key"] = df["dayOfWeek_Hour_series"] + df["p1_series"] + df["p2_series"]
            df["key"] = df["key"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            return df["key"]

        return df.withColumn('key', unique_md5_key(col("dayOfWeek_Hour"), col("p1"), col('p2')))
