"""

Some utils for write dataframe in hive tables

"""

from pyspark.sql import SparkSession


class WriteTable:

    def __init__(self):
        self._spark = SparkSession.builder.getOrCreate()

    def byNameOfColumns(self, df, tableName, overwrite=False):
        """
        Write in Hive Tables by name of the columns (not positional)
        :param df:
        :param tableName:
        :return:
        """

        orderOfColumns = [column.name for column in self._spark.catalog.listColumns(tableName)]

        df.select(*orderOfColumns).write.insertInto(tableName, overwrite)

        return None


def _test():
    tableName = '___test_1___'

    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

    print("First Data")

    df1 = spark.range(10).selectExpr('id as id1',
                                     '"dummy1" as dummy1',
                                     '"dummy2" as dummy2')

    df1.write.saveAsTable(tableName, mode='overwrite')

    spark.table(tableName).show(truncate=False)

    df2 = spark.range(20).selectExpr('"dummy1" as dummy1',
                                     '"dummy2" as dummy2',
                                     'id as id1')

    print("Inserting by positional (not using byNameOfColumns)")
    df2.write.insertInto(tableName, overwrite=True)
    spark.table(tableName).show(truncate=False)

    print("Inserting by name of columns (using byNameOfColumns)")
    WriteTable().byNameOfColumns(df2, tableName, overwrite=True)

    spark.table(tableName).show(truncate=False)


if __name__ == "__main__":
    _test()
