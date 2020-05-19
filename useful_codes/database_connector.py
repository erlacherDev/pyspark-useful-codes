"""

*** CODE NOT TESTED! ***

"""

import pyodbc
from pyspark.sql import SparkSession, DataFrame


class DatabaseConnector:

    def __init__(self, url=None, database=None, user=None, password=None, driver=None):
        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

    def read(self, spark: SparkSession, tableName: str, url=None, database=None,
             user=None, password=None, driver=None) -> DataFrame:

        url, database, user, password, driver = self._get_attributes(url, database, user, password, driver)

        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{database}.{tableName}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

    def write(self, df: DataFrame, tableName: str, url=None, database=None,
              user=None, password=None, driver=None) -> None:

        url, database, user, password, driver = self._get_attributes(url, database, user, password, driver)

        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{database}.{tableName}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .save()

        return None

    def readQuery(self, spark: SparkSession, query: str, url=None, database=None,
                  user=None, password=None, driver=None) -> DataFrame:

        url, database, user, password, driver = self._get_attributes(url, database, user, password, driver)

        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

    def executeProcedure(self, procedureName: str, url=None, database=None,
                         user=None, password=None, driver=None) -> None:

        url, database, user, password, driver = self._get_attributes(url, database, user, password, driver)

        cnxn = pyodbc.connect(
            f"DRIVER={driver};"
            f"SERVER={url};"
            f"PORT=1433;"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}")

        cursor = cnxn.cursor()

        cursor.execute(f"EXEC PROCEDURE {procedureName}")

        print(f"Executed procedure {procedureName}")

        cnxn.close()

        return None

    @classmethod
    def oracle(cls, url=None, database=None, user=None, password=None):
        return cls(url, database, user, password, "oracle.jdbc.driver.OracleDriver")

    @classmethod
    def azure_sql(cls, url=None, database=None, user=None, password=None):
        return cls(url, database, user, password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    def _get_attributes(self, url, database, user, password, driver):

        if url is None:

            if self.url is None:
                raise ValueError("Parametro 'url' nao pode ser Nulo.")

            url = self.url

        if database is None:

            if self.database is None:
                raise ValueError("Parametro 'database' nao pode ser Nulo.")

            database = self.database

        if user is None:

            if self.user is None:
                raise ValueError("Parametro 'user' nao pode ser Nulo.")

            user = self.user

        if password is None:

            if self.password is None:
                raise ValueError("Parametro 'password' nao pode ser Nulo.")

            password = self.password

        if driver is None:

            if self.driver is None:
                raise ValueError("Parametro 'driver' nao pode ser Nulo.")

            driver = self.driver

        return url, database, user, password, driver


database_connector = DatabaseConnector(None, None, None, None, None)
