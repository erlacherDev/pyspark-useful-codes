"""

*** CODE NOT TESTED! ***

"""

import pyodbc
from pyspark.sql import SparkSession, DataFrame


def jdbc_connector(url=None, database=None, user=None, password=None, driver=None):
    return JDBCDatabaseConnector(url, database, user, password, driver)


class JDBCDatabaseConnector:

    def __init__(self, url=None, database=None, user=None, password=None, driver=None):
        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

    def readTable(self, spark: SparkSession, tableName: str, url=None, database=None,
                  user=None, password=None, driver=None) -> DataFrame:

        database, driver, password, url, user = self._get_attributes(database, driver, password, url, user)

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

        database, driver, password, url, user = self._get_attributes(database, driver, password, url, user)

        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{database}.{tableName}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .save()

        return None

    def queryString(self, spark: SparkSession, query: str, url=None, user=None,
                    password=None, driver=None) -> DataFrame:

        if url is None:
            url = self.url

        if user is None:
            user = self.user

        if password is None:
            password = self.password

        if driver is None:
            driver = self.driver

        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

    def executeProcedure(self, procedureName: str) -> None:
        cnxn = pyodbc.connect(
            f"DRIVER={self.driver};"
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

    @classmethod
    def oracle(cls, url, database, user, password):
        return cls(url, database, user, password, "oracle.jdbc.driver.OracleDriver")

    @classmethod
    def azure_sql(cls, url, database, user, password):
        return cls(url, database, user, password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    def _get_attributes(self, database, driver, password, url, user):

        if url is None:

            if self.url is None:
                raise ValueError(f"{self.url} nao pode ser Nulo.")

            url = self.url

        if database is None:

            if self.database is None:
                raise ValueError(f"{self.database} nao pode ser Nulo.")

            database = self.database

        if user is None:

            if self.user is None:
                raise ValueError(f"{self.user} nao pode ser Nulo.")

            user = self.user

        if password is None:

            if self.password is None:
                raise ValueError(f"{self.password} nao pode ser Nulo.")

            password = self.password

        if driver is None:

            if self.driver is None:
                raise ValueError(f"{self.driver} nao pode ser Nulo.")

            driver = self.driver

        return database, driver, password, url, user
