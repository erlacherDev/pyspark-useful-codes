from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col
from pyspark.sql.types import ArrayType, StructType


class UnnestColumn:

    def __init__(self):
        pass

    def unnest(self, df, columns=None):
        """

        :param df:
        :param columns:
        :return:
        """
        if columns is not None:
            if not isinstance(columns, str) and not isinstance(columns, list):
                raise TypeError("dataframe_column '{0}' must be string or list.".format(columns))

        if isinstance(columns, str):
            columns = [columns]

        if columns is None:
            need_conversion = self.check_array_or_struct(df, columns)
            while need_conversion:
                df = self._recursive_run(df, columns)
                need_conversion = self.check_array_or_struct(df)
        else:
            df = self._recursive_run(df, columns)

        return df

    @staticmethod
    def check_array_or_struct(df, columns=None):

        if columns is None:
            columns = df.columns

        schema = df.schema
        contain_array_struct = False

        for column in schema:
            if isinstance(column.dataType, ArrayType) or isinstance(column.dataType, StructType):
                if column.name in columns:
                    contain_array_struct = True

        return contain_array_struct

    def _recursive_run(self, df, columns):
        """

        :param df:
        :param columns:
        :return:
        """
        array_fields = self.get_array_fields(df)

        df_exploded = self._explode_all(df, array_fields) if columns is None \
            else self._explode_columns(df, array_fields, columns)

        df = self._get_flatten_structs(df_exploded, columns)

        return df

    @staticmethod
    def get_array_fields(df):
        """

        :param df:
        :return:
        """
        return dict([(field.name, field.dataType) for field in df.schema.fields
                     if isinstance(field.dataType, ArrayType)])

    @staticmethod
    def _explode_all(df, df_array_fields):
        """

        :param df:
        :param df_array_fields:
        :return:
        """
        for coluna in df_array_fields.keys():
            df = df.withColumn(coluna, explode_outer(col(coluna)))
        return df

    @staticmethod
    def _explode_columns(df, df_array_fields, columns):
        """

        :param df:
        :param df_array_fields:
        :param columns:
        :return:
        """

        for column in columns:
            if column in df_array_fields.keys():
                df = df.withColumn(column, explode_outer(col(column)))
        return df

    def _get_flatten_structs(self, df, columns=None):
        """

        :param df:
        :param columns:
        :return:
        """

        if columns is not None:
            df = self._flatten_structs_columns(df, columns)
        else:
            df = self._flatten_structs_all(df)

        return df

    @staticmethod
    def _flatten_structs_columns(df, columns):
        """

        :param df:
        :param columns:
        :return:
        """

        nested_cols = [column.name for column in df.schema if isinstance(column.dataType, StructType)]

        nested_cols_to_flat = [column for column in nested_cols if column in columns]
        nested_cols_not_to_flat = [column for column in nested_cols if column not in columns]
        flat_cols = [column.name for column in df.schema if not isinstance(column.dataType, StructType)]

        return df.select(flat_cols + nested_cols_not_to_flat + [col(nc + '.' + c).alias(nc + '_' + c)
                                                                for nc in nested_cols_to_flat
                                                                for c in df.select(nc + '.*').columns])

    @staticmethod
    def _flatten_structs_all(df):
        """

        :param df:
        :return:
        """
        nested_cols = [column.name for column in df.schema if isinstance(column.dataType, StructType)]
        flat_cols = [column.name for column in df.schema if not isinstance(column.dataType, StructType)]

        return df.select(flat_cols + [col(nc + '.' + c).alias(nc + '_' + c)
                                      for nc in nested_cols
                                      for c in df.select(nc + '.*').columns])


def _test():
    spark = SparkSession.builder.getOrCreate()

    stringRDD = spark.sparkContext.parallelize(
        [
            {"name": "Michael",
             "cities": ["palo alto", "menlo park"],
             "schools": [{"sname": "stanford", "year": 2010},
                         {"sname": "berkeley", "year": 2012}]},
            {"name": "Andy",
             "cities": ["santa cruz"],
             "schools": [{"sname": "ucsb",
                          "year": 2011}]},
            {"name": "Justin",
             "cities": ["portland"],
             "schools": [{"sname": "berkeley",
                          "year": 2014}]}])

    df = spark.read.json(stringRDD)
    print("Initial DataFrame")
    df.printSchema()
    df.show(20, False)

    df_unnested = UnnestColumn().unnest(df)
    print("After UnnestColumn")
    df_unnested.printSchema()
    df_unnested.show(20, False)


if __name__ == '__main__':
    _test()
