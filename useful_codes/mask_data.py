from faker import Factory
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DateType, StringType, LongType, FloatType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

global _FAKER_BR
global _FAKER_EN

_FAKER_BR = Factory.create('pt_BR')
_FAKER_EN = Factory.create()


def mask_data(spark, df_or_delta, mask_dict, update_delta_table=None, update_delta_where=None):
    """

    :param spark: SparkSession
    :param df_or_delta: :DataFrame: or :str: representing a delta table
    :param mask_dict: a :dict: as the following example:
    {'column_1': 'mask_type_1'})
    * List of mask types:
        'name'
        'street_name'
        'phone'
        'email'
        'cpf'
        'rg'
        'passport'
        'document'
        'birthdate'
        'job'
        'agency'
        'bank_account'
        'salary'
    :param update_delta_table: if True, the delta table will be updated with the new mask columns.
    :param update_delta_where: Case update_delta_table = True, this parameter represents the WHERE clause for the update.
    :return: if update is False, returns a :DataFrame:. Otherwise returns None.
    """
    return MaskData(_MASK_FUNCTIONS).mask_data(spark, df_or_delta, mask_dict, update_delta_table, update_delta_where)


class MaskData:

    def __init__(self, mask_functions):
        self.mask_functions = mask_functions

    def mask_data(self, spark, df_or_delta, mask_dict, update_delta_table=None, update_delta_where=None):

        if update_delta_table is not None:
            update_delta_table = True

        if not isinstance(mask_dict, dict):
            raise TypeError("mask_dict must be of type 'dict'. Found: {0}".format(type(mask_dict)))

        if isinstance(df_or_delta, DataFrame):
            source = df_or_delta

        elif isinstance(df_or_delta, str):
            source = self._get_delta_table(spark, df_or_delta, update_delta_table)

        else:
            raise TypeError(
                "df_or_delta must be of type 'DataFrame' or 'str'. Found: {0}".format(type(df_or_delta)))

        if update_delta_table:
            self._mask_and_update_delta_table(source, mask_dict, update_delta_where)
            return None

        df = self._mask_df(source, mask_dict)

        return df

    def _mask_df(self, df, mask_dict):

        column_types = dict(df.dtypes)

        mask_dict_items = mask_dict.items()

        self._check_columns_and_masks(column_types, mask_dict_items)

        for column, mask_type in mask_dict_items:
            df = df.withColumn(column, self.mask_functions[mask_type](column).cast(column_types[column]))

        return df

    def _get_delta_table(self, spark, table_or_path, update_delta_table):

        try:
            deltaTable = DeltaTable.forPath(spark, table_or_path)
        except:
            try:
                deltaTable = DeltaTable.forName(spark, table_or_path)
            except AssertionError as E:
                raise E

        if update_delta_table:
            return deltaTable

        return deltaTable.toDF()

    def _mask_and_update_delta_table(self, delta_table, mask_dict, update_delta_where):

        if not isinstance(delta_table, DeltaTable):
            raise TypeError("For Update Delta Table 'df_or_delta' must be a valid path of table_name.")

        column_types = dict(delta_table.toDF().dtypes)
        mask_dict_items = mask_dict.items()

        self._check_columns_and_masks(column_types, mask_dict_items)

        set_to_update = {column: self.mask_functions[mask_type](column).cast(column_types[column])
                         for column, mask_type in mask_dict_items}

        delta_table.update(update_delta_where, set_to_update)

        return None

    def _check_columns_and_masks(self, column_types, mask_dict_items):

        for column, mask_type in mask_dict_items:

            if column not in column_types:
                raise AssertionError(
                    "Column '{0}' not found on dataframe. Columns: [{1}]"
                        .format(column, ', '.join(list(column_types.keys()))))

            if mask_type not in self.mask_functions:
                raise AssertionError(
                    "MaskType '{0}' not registered. MaskTypes: [{1}]"
                        .format(mask_type, ', '.join(list(self.mask_functions.keys()))))


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def fkr_name(seed):
    return seed.map(
        lambda x: _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').name().upper() if x is not None else None)


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def fkr_street_name(seed):
    return seed.map(
        lambda x: _FAKER_BR.seed_instance(str(x) + '23ioo0987234*&$!').street_name().upper() if x is not None else None)


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def fkr_phone(seed):
    return seed.map(
        lambda x: _FAKER_BR.seed_instance(str(x) + '23ioo0987234*&$!').phone_number() if x is not None else None)


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def fkr_email(seed):
    return seed.map(
        lambda x: _FAKER_BR.seed_instance(str(x) + '23ioo0987234*&$!').email().upper() if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_cpf(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('###########')) if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_rg(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('#########')) if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_passport(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('#########')) if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_document(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('#####')) if x is not None else None)


@pandas_udf(DateType(), PandasUDFType.SCALAR)
def fkr_birthdate(seed):
    return seed.map(
        lambda x: _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').date_of_birth() if x is not None else None)


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def fkr_job(seed):
    return seed.map(
        lambda x: _FAKER_BR.seed_instance(str(x) + '23ioo0987234*&$!').job().upper() if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_agency(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('####').upper()) if x is not None else None)


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fkr_bank_account(seed):
    return seed.map(lambda x: int(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('#######').upper()) if x is not None else None)


@pandas_udf(FloatType(), PandasUDFType.SCALAR)
def fkr_salary(seed):
    return seed.map(lambda x: float(
        _FAKER_EN.seed_instance(str(x) + '23ioo0987234*&$!').numerify('####.##').upper()) if x is not None else None)


_MASK_FUNCTIONS = {
    'name': fkr_name,
    'street_name': fkr_street_name,
    'phone': fkr_phone,
    'email': fkr_email,
    'cpf': fkr_cpf,
    'rg': fkr_rg,
    'passport': fkr_passport,
    'document': fkr_document,
    'birthdate': fkr_birthdate,
    'job': fkr_job,
    'agency': fkr_agency,
    'bank_account': fkr_bank_account,
    'salary': fkr_salary
}
