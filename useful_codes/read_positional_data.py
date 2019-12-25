from pyspark.sql import DataFrame
from pyspark.sql.functions import substring, col


def read_positional(spark, df_or_path, column_list):
    """
    :param spark: SparkSession
    :param df_or_path: DataFrame with the column 'value' that will be splited or the path of the positional file.
    :param column_list: List ou Tuple for each column (must be in order):
        [column_name, column_range, column_type (opcional)]
        Ex:
        column_list = [('col_1', 5, 'string'), ('col_2', '10', 'int'), ('col_3', 5)
    :return: Return the DataFrame with the columns defined at column_list
    """

    return ReadPositional(spark, df_or_path, column_list).run()


class ReadPositional:

    def __init__(self, spark, df_or_path, column_list):
        self._spark = spark
        self.df_or_path = df_or_path
        self.column_list = column_list

    def run(self):

        self._check_positional_list()
        positional_df = self._get_input_df()
        index_start = 1

        for values_in_tuple in self.column_list:  # (column_name, column_range, column_type)
            positional_df = self._build_positional_columns(index_start, positional_df, values_in_tuple)
            index_start += values_in_tuple[1]

        return positional_df.drop('value')

    @staticmethod
    def _build_positional_columns(index_start, positional_df, values_in_tuple):

        if not isinstance(values_in_tuple, tuple) and not isinstance(values_in_tuple, list):
            raise TypeError(
                "Values inside 'column_list' must be of type 'list' or 'tuple'. Found: '{0}'"
                    .format(type(values_in_tuple)))

        if len(values_in_tuple) < 2 or len(values_in_tuple) > 3:
            raise ValueError("Values inside 'column_list' must be have length of 2 or 3.")

        column_name = values_in_tuple[0]
        column_range = int(values_in_tuple[1])

        positional_df = positional_df.withColumn(column_name, substring(col('value'), index_start, column_range))

        if len(values_in_tuple) == 3:
            positional_df = positional_df.withColumn(column_name, col(column_name).cast(values_in_tuple[2]))

        return positional_df

    def _get_input_df(self):

        if isinstance(self.df_or_path, DataFrame):
            positional_df = self.df_or_path

        elif isinstance(self.df_or_path, str):
            positional_df = spark.read.text(self.df_or_path)

        else:
            raise TypeError(
                "df_or_path must be of type 'DataFrame' or 'str'. Found: {0}".format(type(self.df_or_path)))

        return positional_df

    def _check_positional_list(self):

        if not isinstance(self.column_list, list):
            raise TypeError(
                "'column_list' must be of type 'list'. Found: '{0}'".format(type(self.column_list)))


if __name__ == '__main__':
    from pyspark.sql.functions import expr
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.range(10000000).select(expr(
        "'11111122222223333333444444444555555555556666666666677777777777788888888889999999999900000000000' as value"))

    print('Input DF')
    df.show(20, False)

    tuple_params = [
        ('one', 6, 'int'),
        ('two', 7, 'int'),
        ('three', 7, 'int'),
        ('four', 9, 'int'),
        ('five', 11, 'long'),
        ('six', 11, 'long'),
        ('seven', 12, 'long'),
        ('eight', 10),
        ('nine', 11),
        ('zero', 11)
    ]

    print('Output DF')
    read_positional(spark, df, tuple_params).show()
