"""

Some utils for write dataframe in hive tables

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class WriteTable:

    def __init__(self, df, tableName):

        self._spark = SparkSession.builder.getOrCreate()
        self._df = df
        self._tableName = tableName

    def byNameOfColumns(self):
        """
        order the columns in datafrmae
        :return: self
        """
        orderOfColumns = [column.name for column in self._spark.catalog.listColumns(self._tableName)]

        self._df = self._df.select(*orderOfColumns)

        return self

    def checkingNullValues(self, columns=None, tolerance=0):
        """
        Checking null values in columns
        :param columns: columns to check for null values
        :param tolerance: 0 = Error, 1 = Warn
        :return:
        """

        if tolerance not in (0, 1):
            raise ValueError("Tolerance '{0}' must be 0 or 1.".format(str(tolerance)))

        if columns is None:
            columns = self._df.columns

        if isinstance(columns, str):
            columns = [columns]

        for column in columns:

            if self._df.filter(col(column).isNull()).first() is not None:

                log_msg = "DataFrame contains Null values in column {0}".format(column)

                if tolerance == 0:
                    raise AssertionError(log_msg)

                if tolerance == 1:
                    print(log_msg)

        return self

    def write(self, overwrite=False):
        """

        :param overwrite:
        :return:
        """

        self._df.write.insertInto(self._tableName, overwrite)

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
    WriteTable(df2, tableName).byNameOfColumns().write(overwrite=True)

    spark.table(tableName).show(truncate=False)

    print("Validating null values")
    df3 = spark.range(20).selectExpr('Null as dummy1',
                                     '"dummy2" as dummy2',
                                     'id as id1')
    WriteTable(df3, tableName).byNameOfColumns().checkingNullValues(tolerance=1).write(overwrite=True)


if __name__ == "__main__":
    _test()
