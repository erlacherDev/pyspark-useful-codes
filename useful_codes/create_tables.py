from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class CreateTable:

    def __init__(self, name, schema, formatType='parquet', location=None, partitionedBy=None, additionalInfo=None):

        self.name = name
        self.schema = schema
        self.formatType = formatType
        self.location = location
        self.partitionedBy = partitionedBy
        self.additionalInfo = additionalInfo
        self.columns = self._get_columns(self.schema)
        self._kv_schema = self._get_kv_schema(self.schema)

    def build_ddl(self, if_not_exists=True):
        _create = 'CREATE TABLE'

        if if_not_exists:
            _create = 'CREATE TABLE IF NOT EXISTS'

        _location = ''
        _partitionedBy = ''
        _format = "USING " + self.formatType
        _columns_and_dtypes = ', '.join([k + ' ' + v for k, v in self._kv_schema])
        _additionalInfo = ''

        if self.location is not None:
            _location = "LOCATION " + self.location

        if self.partitionedBy is not None:

            if isinstance(self.partitionedBy, str):
                _partitionedBy = 'PARTITIONED BY (' + self.partitionedBy + ")"

            elif isinstance(self.partitionedBy, list):
                _partitionedBy = 'PARTITIONED BY (' + ', '.join(self.partitionedBy) + ")"

            else:
                raise TypeError("partitionedBy must be str or list. Found: {0}".format(type(self.partitionedBy)))

        if self.additionalInfo is not None:
            _additionalInfo = self.additionalInfo

        return "{0} {1} ({2}) {3} {4} {5} {6}". \
            format(_create, self.name, _columns_and_dtypes, _format, _partitionedBy, _location, _additionalInfo)

    def create(self, if_not_exists=True):
        SparkSession.builder.getOrCreate().sql(self.build_ddl(if_not_exists))

    @staticmethod
    def _get_columns(schema):

        if isinstance(schema, dict):
            return [x for x, v in schema.items()]

        elif isinstance(schema, str):
            return [x.split(' ')[0] for x in schema.split(',')]

        elif isinstance(schema, list):
            return [x.split(' ')[0] for x in schema]

        elif isinstance(schema, StructType):
            return schema.names

        else:
            raise TypeError("schema should be dict, str, list or StructType. Found: {0}".format(type(schema)))

    @staticmethod
    def _get_kv_schema(schema):

        if isinstance(schema, dict):
            return [(x, v) for x, v in schema.items()]

        elif isinstance(schema, str):
            return [(x.split(' ')[0], x.split(' ')[1]) for x in schema.replace(', ', ',').split(',')]

        elif isinstance(schema, list):
            return [(x.split(' ')[0], x.split(' ')[1]) for x in schema]

        elif isinstance(schema, StructType):
            return [(f.name, f.dataType.simpleString()) for f in schema.fields]

        else:
            raise TypeError("schema should be dict, str, list or StructType. Found: {0}".format(type(schema)))


def _test():
    name = 'test_table'

    schema_dict = {'field_1': 'string', 'field_2': 'int'}
    schema_str = 'field_1 string, field_2 int'
    schema_list = ['field_1 string', 'field_2 int']
    schema_struct = StructType([StructField('field_1', StringType()), StructField('field_2', IntegerType())])

    schemas = [schema_dict, schema_str, schema_list, schema_struct]

    formatType = 'orc'

    partitionedBy = ['field_2'] or 'field_2'

    additionalInfo = None

    for schema in schemas:
        ddl = CreateTable(name=name,
                          schema=schema,
                          formatType=formatType,
                          location=None,
                          partitionedBy=partitionedBy,
                          additionalInfo=additionalInfo
                          ).build_ddl()

        print("with schema type {0}: \n {1}".format(type(schema), ddl))


if __name__ == '__main__':
    _test()
