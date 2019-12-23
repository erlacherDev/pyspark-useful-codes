"""
UNDER CONSTRUCTION
"""

class MigrateData:

    def __init__(self, spark, df, json_path_or_dict):
        self._spark = spark
        self.df = df
        self.json = self._build_json(json_path_or_dict)
        self._validate_json(json)

    def run(outputPath=None):
        return None

    def _build_json(self, json_path_or_dict):

        if isinstance(json_path_or_dict, dict):
            pass

        elif isinstance(json_path_or_dict, str):
            if 'dbfs:/' in json_path_or_dict:
                json_path_or_dict = json_path_or_dict.replace('dbfs:/', '/dbfs/')
            elif '/dbfs' in json_path_or_dict:
                pass
            else:
                json_path_or_dict = '/dbfs/' + json_path_or_dict

        else:
            raise TypeError("json_path_or_dict must be of type 'dict' or 'str'. Found: {0}"
                            .format(type(json_path_or_dict)))

        return json_path_or_dict
