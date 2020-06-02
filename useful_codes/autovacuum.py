from pyspark.sql import SparkSession, DataFrame


class AutoVacuum:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.control_tablename = "default._delta_vacuum_control"
        self.DEFAULT_RETENTATION = 168

    def run(self, delta_tables=None, debug=True) -> None:

        if delta_tables is None:
            delta_tables = self.get_delta_tables()

        if isinstance(delta_tables, str):
            delta_tables = [delta_tables]

        control_table_dict = {x.table_name: x.retantion_hours for x in self.control_table.collect()}

        if debug:
            print("Tabelas deltas encontradas no Workspace: \n", delta_tables)
            print("\n--------------------\n")
            print("Tabelas encontradas na tabela de controle com suas respectivas retencoes:\n", control_table_dict)
            print("\n--------------------\n")

        for delta_table in delta_tables:

            if delta_table not in control_table_dict:

                if debug:
                    print(f"Tabela {delta_table} nao encontrada na tabela controle. Adicionando com retencao default"
                          f" ({str(self.DEFAULT_RETENTATION)})")

                self.update_control_table(delta_table, self.DEFAULT_RETENTATION, debug=False)
                control_table_dict[delta_table] = self.DEFAULT_RETENTATION

            if debug:
                print(f"EXECUTANDO VACUUM PARA A TABELA {delta_table}"
                      f" COM RETATION DE {str(control_table_dict[delta_table])} HORAS")

            self.spark.sql(f"VACUUM {delta_table} RETAIN {control_table_dict[delta_table]} HOURS")

            if debug:
                print("VACUUM EXECUTADO COM SUCESSO")
                print("\n--------------------\n")

        return None

    def delete_control_table(self, table_name: str, debug=True) -> None:

        deleteString = f" DELETE FROM {self.control_tablename} WHERE table_name = '{table_name}'"

        if debug:
            print(f"deleteString: {deleteString}")

        self.spark.sql(deleteString)

    def update_control_table(self, table_name: str, retantion_hours: int, debug=True) -> None:

        self.spark.range(1).selectExpr(
            f"'{table_name}' as table_name",
            f"'{retantion_hours}' as retantion_hours",
            f"current_timestamp() as last_updated"
        ).registerTempTable("__builded_temp_df__")

        default_value_crossjoin = self.spark.conf.get('spark.sql.crossJoin.enabled')

        self.spark.conf.set('spark.sql.crossJoin.enabled', 'true')

        mergeString = f"""

            MERGE INTO {self.control_tablename} destino

            USING __builded_temp_df__ origem

            ON origem.table_name = destino.table_name

            WHEN MATCHED THEN
              UPDATE SET destino.retantion_hours = origem.retantion_hours,
                         destino.last_updated = origem.last_updated

            WHEN NOT MATCHED
              THEN INSERT *
        """

        if debug:
            print(f"mergeString: {mergeString}")

        self.spark.sql(mergeString)

        self.spark.conf.set('spark.sql.crossJoin.enabled', default_value_crossjoin)

        return None

    @property
    def control_table(self) -> DataFrame:
        return self.spark.table(self.control_tablename)

    def get_databases(self) -> list:
        return [x.databaseName for x in self.spark.sql("SHOW DATABASES").collect()]

    def get_tables(self, database: str = None) -> list:

        if database is None:
            databases = self.get_databases()
        else:
            databases = [database]

        tables_list = []
        for database in databases:
            self.spark.sql("USE " + database)
            tables_list += [x.database + '.' + x.tableName for x in
                            self.spark.sql("SHOW TABLES").filter('isTemporary == false').collect()]

        return tables_list

    def get_delta_tables(self, tables: list = None) -> list:

        if tables is None:
            tables = self.get_tables()

        delta_tables = []

        for table in tables:

            showcreate_string = self.spark.sql(f"SHOW CREATE TABLE {table}").first()[0]

            if "USING delta" in showcreate_string:

                delta_tables.append(table)

        return delta_tables

    def create_control_table(self, debug=False) -> None:

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.control_tablename} (
            table_name string,
            retantion_hours int,
            last_updated timestamp
            )
        USING DELTA
        """)

        if debug:
            print(f"Created table (if not exists) {self.control_tablename}.")

        return None