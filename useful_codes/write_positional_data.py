from pyspark.sql.functions import rpad, col, concat


def write_positional(spark, df, column_list, complete_with=None, num_partitions=None,
                         output_path=None, output_mode=None, compression=None, lineSep=None):

    """

    :param spark: SparkSession
    :param df: DataFrame
    :param column_list: List ou Tuple for each column (must be in order):
        [column_name, column_length]
        Ex:
        column_list = [('col_1', 5), ('col_2', '10'), ('col_3', 5)
    :param complete_with: Optional -> Value to complete the positional columns when needed (Default: ' ')
    :param num_partitions: Optional -> number of partitions of the output df / number or partitions to write
    :param output_path: Optional -> Path to Write
    :param output_mode: Optional -> Specifies the behavior of the save operation when data already exists.
        * ``append``: Append contents of this :class:`DataFrame` to existing data.
        * ``overwrite``: Overwrite existing data.
        * ``ignore``: Silently ignore this operation if data already exists.
        * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
            exists.

    :param compression: Default: None
    :param lineSep: Default: None
    :return: Return the DataFrame if output_path is None.
            Otherwise, return None (data is writen at a specified directory)
    """

    return WritePositional(spark, df, column_list, complete_with, num_partitions,
                           output_path, output_mode, compression, lineSep).run()


class WritePositional:

    def __init__(self, spark, df, column_list, complete_with=None, num_partitions=None,
                 output_path=None, output_mode=None, compression=None, lineSep=None):

        self._spark = spark
        self.df = df
        self.column_list = column_list
        self.output_path = output_path
        self.complete_with = complete_with
        self.num_partitions = num_partitions
        self.output_mode = output_mode
        self.compression = compression
        self.lineSep = lineSep

    def run(self):

        self._check_positional_list()
        df = self._fix_length_of_columns()
        positional_df = self._build_positional_column(df)

        if self.num_partitions is not None:
            positional_df = positional_df.repartition(int(self.num_partitions))

        if self.output_path is not None:
            self._write_to_path(positional_df)

            return None

        return positional_df

    def _write_to_path(self, positional_df):
        writerDF = positional_df.write

        if self.output_mode is not None:
            writerDF = writerDF.mode(self.output_mode)

        writerDF.text(self.output_path, self.compression, self.lineSep)

        return None

    def _build_positional_column(self, df):

        concat_expr = concat(*[col(x[0]) for x in self.column_list])

        return df.select(concat_expr.alias('value'))

    def _fix_length_of_columns(self):

        if self.complete_with is None:
            complete_with = ' '
        else:
            complete_with = str(self.complete_with)

        df = self.df

        for column_infos in self.column_list:

            if not isinstance(column_infos, tuple) and not isinstance(column_infos, list):
                raise TypeError(
                    "Values inside 'column_list' must be of type 'list' or 'tuple'. Found: '{0}'"
                        .format(type(column_infos)))

            if len(column_infos) != 2:
                raise ValueError("Values inside 'column_list' must be have length of 2 (column_name and column_length)")

            column_name = column_infos[0]
            column_length = int(column_infos[1])

            df = df.withColumn(column_name, rpad(col(column_name).cast('string'), column_length, complete_with))

        return df

    def _check_positional_list(self):

        if not isinstance(self.column_list, list):
            raise TypeError(
                "'column_list' must be of type 'list'. Found: '{0}'".format(type(self.column_list)))


if __name__ == '__main__':
    from pyspark.sql.functions import expr
    from pyspark.sql import SparkSession
    from useful_codes.read_positional_data import read_positional

    spark = SparkSession.builder.getOrCreate()
    df = spark.range(10000000).select(expr(
        "'11111122222223333333444444444555555555556666666666677777777777788888888889999999999900000000000' as value"))

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

    df_positional = read_positional(spark, df, tuple_params)

    column_list = [
        ('one', 10),
        ('two', 10)
    ]

    df_final = write_positional(spark, df, column_list).show()
