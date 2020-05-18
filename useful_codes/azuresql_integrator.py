"""

*** CODE NOT TESTED! ***

"""

import pyodbc
from pyspark.sql import SparkSession, DataFrame

_AZURE_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


class AzureSQLConnector:

    def __init__(self, url: str, database: str, user: str, password: str):
        self.url = url
        self.database = database
        self.user = user
        self.password = password

    def readTable(self, spark: SparkSession, tableName: str) -> DataFrame:

        return spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", f"{self.database}.{tableName}") \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()

    def queryString(self, spark: SparkSession, query: str) -> DataFrame:

        return spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("query", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()

    def executeProcedure(self, procedureName: str) -> None:
        cnxn = pyodbc.connect(
            f"DRIVER={_AZURE_SQL_DRIVER};"
            f"SERVER={self.url};"
            f"PORT=1433;"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password}")

        cursor = cnxn.cursor()

        cursor.execute(f"EXEC PROCEDURE {procedureName}")

        print(f"Executed procedure {procedureName}")

        cnxn.close()

        return None

    def write(self, df: DataFrame, tableName: str) -> None:

        df.write \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", f"{self.database}.{tableName}") \
            .option("user", self.user) \
            .option("password", self.password) \
            .save()

        return None
