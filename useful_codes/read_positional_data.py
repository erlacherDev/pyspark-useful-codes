from pyspark.sql import DataFrame
from pyspark.sql.functions import substring, col


def read_positional(spark, df_or_path, position_list):
    """
    :param spark: SparkSession (default como spark)
    :param df_or_path: DataFrame com a coluna 'value' a ser separada ou o diretorio do arquivo posicional
    :param position_list: Lista ou tupla de parametros para cada coluna (na ordem):
        [nome_coluna, tamanho_coluna, datatype_coluna (opcional)]
        Ex:
        position_list = [('col_1', 5, 'string'), ('col_2', '10', 'int'), ('col_3', 5)
    :return: Retorna o DataFrame com as colunas definidas pelo parametro position_list
    """

    if not isinstance(position_list, list):
        raise TypeError(
            "'position_list' must be of type 'list'. Found: '{0}'".format(type(position_list)))

    if isinstance(df_or_path, DataFrame):
        positional_df = df_or_path
    elif isinstance(df_or_path, str):
        positional_df = spark.read.text(df_or_path)
    else:
        raise TypeError(
            "df_or_path must be of type 'DataFrame' or 'str'. Found: {0}".format(type(df_or_path)))

    index_start = 1

    for values_in_tuple in position_list:
        if not isinstance(values_in_tuple, tuple) and not isinstance(values_in_tuple, list):
            raise TypeError(
                "Values inside 'position_list' must be of type 'list' or 'tuple'. Found: '{0}'"
                    .format(type(values_in_tuple)))

        if len(values_in_tuple) < 2 or len(values_in_tuple) > 3:
            raise ValueError("Values inside 'position_list' must be have length of 2 or 3.")

        column_name = values_in_tuple[0]
        column_range = int(values_in_tuple[1])

        positional_df = positional_df.withColumn(column_name, substring(col('value'), index_start, column_range))
        index_start += values_in_tuple[1]

        if len(values_in_tuple) == 3:
            positional_df = positional_df.withColumn(column_name, col(column_name).cast(values_in_tuple[2]))

    return positional_df.drop('value')


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
