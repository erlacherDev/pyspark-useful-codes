# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.streaming import StreamingQuery


# COMMAND ----------

class KafkaIntegrator:

    def __init__(self, servers: str, topics: str, consumer_group: str, extra_options: dict = None):
        self.servers = servers
        self.topics = topics
        self.consumer_group = consumer_group
        self.extra_options = extra_options if extra_options is not None else {}

    @property
    def read(self):
        return KafkaReader(
            self.servers, self.topics, self.consumer_group, self.extra_options, False)

    @property
    def write(self):
        return KafkaBatchWritter(
            self.servers, self.topics, self.consumer_group, self.extra_options
        )

    @property
    def readStream(self):
        return KafkaReader(
            self.servers, self.topics, self.consumer_group, self.extra_options, True)

    @property
    def writeStream(self):
        return KafkaStreamWritter(
            self.servers, self.topics, self.consumer_group, self.extra_options
        )


# COMMAND ----------

class KafkaReader:

    def __init__(self, servers: str, topics: str, consumer_group: str, extra_options: dict, isStreaming: bool):
        self.servers = servers
        self.topics = topics
        self.consumer_group = consumer_group
        self.extra_options = extra_options if extra_options is not None else {}
        self.isStreaming = isStreaming
        self._startingOffsets = "earliest"
        self._endingOffsets = "latest"

    def endingOffsets(self, endingValue):
        self._endingOffsets = endingValue
        return self

    def startingOffsets(self, startingValue):
        self._startingOffsets = startingValue
        return self

    def load(self, spark: SparkSession, startingOffsets: str = None, endingOffsets: str = None) -> DataFrame:

        if startingOffsets is None:
            startingOffsets = self._startingOffsets

        if self.isStreaming:
            dataframeReader = spark.readStream

        else:

            if endingOffsets is None:
                endingOffsets = self._endingOffsets

            dataframeReader = spark.read.option("endingOffsets", endingOffsets)

        return dataframeReader.format("kafka") \
            .options(**self.extra_options) \
            .option("kafka.bootstrap.servers", self.servers) \
            .option("subscribePattern", self.topics) \
            .option("startingOffsets", startingOffsets) \
            .load() \
            .withColumn("key", col("key").cast(StringType())) \
            .withColumn("value", col("value").cast(StringType()))


# COMMAND ----------

class KafkaBatchWritter:

    def __init__(self, servers: str, topics: str, consumer_group: str, extra_options: dict):
        self.servers = servers
        self.topics = topics
        self.consumer_group = consumer_group
        self.extra_options = extra_options if extra_options is not None else {}
        self._trigger = None

    def save(self, df: DataFrame) -> None:
        return df.write.format("kafka") \
            .option("kafka.bootstrap.servers", self.servers) \
            .option("topic", self.topics) \
            .options(**self.extra_options) \
            .save()


# COMMAND ----------

class KafkaStreamWritter:

    def __init__(self, servers: str, topics: str, consumer_group: str, extra_options: dict):
        self.servers = servers
        self.topics = topics
        self.consumer_group = consumer_group
        self.extra_options = extra_options if extra_options is not None else {}
        self._trigger = None
        self._checkpointLocation = None

    def checkpointLocation(self, checkPointValue):
        self._checkpointLocation = checkPointValue
        return self

    def trigger(self, processingTime=None, once=None, continuous=None):

        self._trigger = {'processingTime': processingTime}

        if once is not None:
            self._trigger = {'once': True}

        if continuous is not None:
            self._trigger = {'continuous': True}

        return self

    def save(self, df: DataFrame, checkpointLocation: str = None) -> StreamingQuery:

        if checkpointLocation is None:
            checkpointLocation = self._checkpointLocation

        return df.writeStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.servers) \
            .option("topic", self.topics) \
            .options(**self.extra_options) \
            .trigger(**self._trigger) \
            .option('checkpointLocation', checkpointLocation) \
            .start()
