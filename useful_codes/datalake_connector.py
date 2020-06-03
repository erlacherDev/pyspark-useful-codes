from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.streaming import StreamingQuery


class DatalakeConnector:

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._INPUT_COLUMN_NAME = "__INPUT_FILE_NAME__"
        self._TIMESTAMP_COLUMN_NAME = '__TIMESTAMP__'
        self._ABFS_PATH_SILVER = "<ABFS_SILVER>"
        self._ABFS_PATH_GOLD = "<ABFS_GOLD>"

    def _dataframe_reader(self,
                          camada: str,
                          path,
                          format_file: str,
                          extra_options: dict,
                          schema,
                          isStreaming: bool,
                          lote: dict,
                          debug=True) -> DataFrame:

        datalake_path = self._get_datalake_path(camada, path)

        if debug:
            print("---------------------")
            print("Diretorio de leitura:", datalake_path)
            print("---------------------")

        if isStreaming:

            dfReader = self._spark.readStream

            if schema is None:
                self._spark.conf.set('spark.sql.streaming.schemaInference', 'true')

        else:
            dfReader = self._spark.read

        if schema is not None:
            dfReader = dfReader.schema(schema)

        dfReader = dfReader.format(format_file).options(**extra_options)

        df = dfReader.load(datalake_path) \
            .withColumn(self._INPUT_COLUMN_NAME, input_file_name()) \
            .withColumn(self._TIMESTAMP_COLUMN_NAME, current_timestamp())

        for k, v in lote.items():
            df = df.withColumn(k, lit(v))

        return df

    def _get_datalake_path(self, camada, path) -> str:
        if camada.lower() == 'silver':
            ABFS_PREFIX = self._ABFS_PATH_SILVER
        elif camada.lower() == 'gold':
            ABFS_PREFIX = self._ABFS_PATH_GOLD
        else:
            raise ValueError(
                f"Valor invalido '{camada}' para o parametro 'camada' Valores suportados: ['silver', 'gold'])")
        if isinstance(path, str):
            datalake_path = f"{ABFS_PREFIX}/{path}"
        else:
            datalake_path = ",".join([f"{ABFS_PREFIX}/{x}" for x in path])

        return datalake_path

    def read(self,
             camada: str,
             path,
             format_file: str,
             schema=None,
             sep: str = None,
             header: bool = None,
             multiLine: bool = None,
             extra_options: dict = None,
             lote: dict = None
             ) -> DataFrame:

        lote = lote if lote is not None else {}
        extra_options = self._build_extra_options(extra_options, header, multiLine, sep)

        return self._dataframe_reader(camada, path, format_file, extra_options, schema, False, lote)

    def readStream(self,
                   camada: str,
                   path,
                   format_file: str,
                   schema=None,
                   sep: str = None,
                   header: bool = None,
                   multiLine: bool = None,
                   extra_options: dict = None,
                   lote: dict = None
                   ) -> DataFrame:

        lote = lote if lote is not None else {}
        extra_options = self._build_extra_options(extra_options, header, multiLine, sep)

        self._build_extra_options(extra_options, header, multiLine, sep)

        return self._dataframe_reader(camada, path, format_file, extra_options, schema, True, lote)

    @staticmethod
    def _build_extra_options(extra_options, header, multiLine, sep):

        extra_options = extra_options if extra_options is not None else {}

        if sep is not None:
            extra_options['sep'] = sep
        if header is not None:
            extra_options['header'] = str(header).lower()
        if multiLine is not None:
            extra_options['multiLine'] = multiLine

        return extra_options

    def write(self,
              dataframe: DataFrame,
              camada: str,
              path: str,
              format_file: str,
              output_mode: str = "append",
              partitionBy=None,
              sep: str = None,
              header: bool = None,
              multiLine: bool = None,
              extra_options: dict = None
              ) -> None:

        datalake_path = self._get_datalake_path(camada, path)

        print("---------------------")
        print("Diretorio de gravacao:", datalake_path)
        print("---------------------")

        extra_options = self._build_extra_options(extra_options, header, multiLine, sep)

        dataframeWritter = dataframe.write.format(format_file).options(**extra_options)

        if partitionBy is not None:

            if isinstance(partitionBy, str):
                dataframeWritter = dataframeWritter.partitionBy(partitionBy)

            else:
                dataframeWritter = dataframeWritter.partitionBy(*partitionBy)

        dataframeWritter.mode(output_mode).save(datalake_path)

        return None

    def writeStream(self,
                    dataframe: DataFrame,
                    camada: str,
                    path: str,
                    format_file: str,
                    checkpointLocation: str,
                    triggerOnce: bool = None,
                    triggerProcessingTime: str = None,
                    triggerContinuous: bool = None,
                    output_mode: str = "append",
                    partitionBy=None,
                    sep: str = None,
                    header: bool = None,
                    multiLine: bool = None,
                    extra_options: dict = None
                    ) -> StreamingQuery:

        datalake_path = self._get_datalake_path(camada, path)

        print("---------------------")
        print("Diretorio de gravacao:", datalake_path)
        print("---------------------")

        extra_options = self._build_extra_options(extra_options, header, multiLine, sep)

        dataframeWritter = dataframe.writeStream.format(format_file).options(**extra_options)

        params_trigger = [triggerProcessingTime, triggerOnce, triggerContinuous]

        if params_trigger.count(None) == 3:
            dataframeWritter = dataframeWritter.trigger(processingTime="0 seconds")

        elif params_trigger.count(None) < 2:
            raise ValueError('Nao eh permitido dois tipos de triggers ao mesmo tempo.')

        if triggerOnce:
            dataframeWritter = dataframeWritter.trigger(once=True)

        if triggerProcessingTime is not None:
            dataframeWritter = dataframeWritter.trigger(processingTime=triggerProcessingTime)

        if triggerContinuous is not None:
            dataframeWritter = dataframeWritter.trigger(continuous=triggerContinuous)

        if partitionBy is not None:

            if isinstance(partitionBy, str):
                dataframeWritter = dataframeWritter.partitionBy(partitionBy)

            else:
                dataframeWritter = dataframeWritter.partitionBy(*partitionBy)

        return dataframeWritter.option("checkpointLocation", checkpointLocation) \
            .outputMode(output_mode) \
            .option("path", datalake_path) \
            .start()
