from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, sha1


class GenerateDataFrame:

    def __init__(self):
        self._spark = SparkSession.builder.getOrCreate()

    def skewed_df(self, num_rows, percentage_rows_skewed, num_dummy_columns=1, num_partitions=200):

        _df = self._spark.range(num_rows, numPartitions=num_partitions) \
            .withColumn('id', when(col('id') <= percentage_rows_skewed * num_rows, lit(1)).otherwise(col('id'))) \
            .withColumnRenamed('id', 'key')

        for dummy in range(0, num_dummy_columns):
            _df = _df.withColumn('dummy_' + str(dummy + 1), sha1(col('key').cast('string')))

        return _df.repartition(num_partitions)

    def normal_df(self, num_rows, num_dummy_columns=1, num_partitions=200):

        _df = self._spark.range(num_rows, numPartitions=num_partitions).withColumnRenamed('id', 'key')

        for dummy in range(0, num_dummy_columns):
            _df = _df.withColumn('dummy_' + str(dummy + 1), sha1(col('key').cast('string')))

        return _df.repartition(num_partitions)


def _test():
    gdf = GenerateDataFrame()

    print("Skewed Data With 10 columns, 5 skewed")
    gdf.skewed_df(10, 5, 10).show(20, False)

    print("Normal Data With 10 columns")
    gdf.normal_df(10, 10).show(20, False)


if __name__ == '__main__':
    _test()
