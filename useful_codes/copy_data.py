import json
from pyspark.sql.functions import expr
from useful_codes.mask_data import mask_data


def copy_data(spark, json_path_or_dict):
    return CopyData(spark, json_path_or_dict).run()


class CopyData:

    def __init__(self, spark, json_path_or_dict):
        self._prod_path = '/mnt/erlacherstorage/delta/prod'
        self._spark = spark
        self.json = self._build_json(json_path_or_dict)
        self._validate_json()
        self.check_overwrite_prod_path()

    def run(self):

        dataframeReader = self._spark.read.format(self.json['input']['format'])

        if self._dict_has_key('options', self.json['input']):
            dataframeReader = self._build_options(dataframeReader, self.json['input']['options'])

        inputDataFrame = dataframeReader.load(self.json['input']['path'])

        if self._dict_has_key('fields', self.json):
            if isinstance(self.json['fields'], list):
                inputDataFrame = inputDataFrame.select(*[expr(column) for column in self.json['fields']])
            else:
                raise TypeError("parameter 'fields' must be a list. Found: {0}".format(type(self.json['fields'])))

        if self._dict_has_key('aproxSamplePercentage', self.json):
            aproxSampleFraction = float(self.json['aproxSamplePercentage']) / 100.00
            if aproxSampleFraction != 1.00:
                inputDataFrame = inputDataFrame.sample(fraction=aproxSampleFraction)

        if self._dict_has_key('mask', self.json):
            inputDataFrame = mask_data(self._spark, inputDataFrame, self.json['mask'])

        dataframeWriter = inputDataFrame.write.format(self.json['output']['format']).mode(self.json['mode'])

        if self._dict_has_key('options', self.json['output']):
            dataframeWriter = self._build_options(dataframeWriter, self.json['output']['options'])

        dataframeWriter.save(self.json['output']['path'])

    @staticmethod
    def _build_options(df_read_or_write, options):
        for key, value in options.items():
            df_read_or_write = df_read_or_write.option(key, value)

        return df_read_or_write

    @staticmethod
    def _build_json(json_path_or_dict):

        if isinstance(json_path_or_dict, dict):
            _json = json_path_or_dict

        elif isinstance(json_path_or_dict, str):
            if 'dbfs:/' in json_path_or_dict:
                json_path_or_dict = json_path_or_dict.replace('dbfs:/', '/dbfs/')
            elif '/dbfs' in json_path_or_dict:
                pass
            else:
                json_path_or_dict = '/dbfs/' + json_path_or_dict

            with open(json_path_or_dict, 'r') as json_file:
                _json = json.load(json_file)

        else:
            raise TypeError("json_path_or_dict must be of type 'dict' or 'str'. Found: {0}"
                            .format(type(json_path_or_dict)))

        return _json

    def _validate_json(self):
        primary_obg_keys = ['input', 'output', 'fields', 'mode']
        self._validate_keys(primary_obg_keys, list(self.json.keys()), 'main')

        secondary_obg_keys = ['path', 'format']
        trees_to_checks = ['input', 'output']

        for tree in trees_to_checks:
            self._validate_keys(secondary_obg_keys, self.json[tree], tree)

        return None

    @staticmethod
    def _validate_keys(obg_keys, keys, tree):
        for obg_key in obg_keys:
            if obg_key not in keys:
                raise ValueError(
                    "parameter '{0}' is obrigatory. Missing in '{1}' tree (must have {2})".format(obg_key, tree,
                                                                                                  obg_keys))

    @staticmethod
    def _dict_has_key(key, _dict):
        if key in _dict:
            return True

        return False

    def check_overwrite_prod_path(self):
        if self._prod_path in self.json['output']['path'] and self.json['mode'] == 'overwrite':
            raise AssertionError(
                "Modo de escrita 'overwrite' nao permitido para gravacao em ambiente de producao."
                " (outputPath = '{0}'".format(self.json['output']['path']))

        return None
