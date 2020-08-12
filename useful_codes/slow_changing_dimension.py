from delta.tables import DeltaTable


class DeltaSCD:

    def __init__(self):
        self.current_flag = "current"
        self.effective_date = "effectiveDate"
        self.end_date = "endDate"

    class DataFrameAlias:
        DELTA = "DF_DELTA"
        UPDATE = "DF_UPDATE"
        STAGED_UPDATE = "STAGED_UPDATES"

    def run(self, spark, scd_type, dataframe, key, delta_path=None, delta_table=None,
            current_flag=None, effective_date=None, end_date=None, debug=False):

        if not isinstance(scd_type, int):
            raise TypeError(f"parametro scd_type precisa ser um inteiro. tipagem recebida: {type(scd_type)}")

        if debug: print(f"Running SCD TYPE {str(scd_type)}")

        if scd_type == 1:
            self.run_type_1(spark, dataframe, key, delta_path, delta_table, debug)

        elif scd_type == 2:
            self.run_type_2(spark, dataframe, key, delta_path, delta_table, current_flag, effective_date, end_date, debug)

        else:
            raise NotImplementedError(f"scd_type '{str(scd_type)}' nao implementado.")

        return None

    def run_type_1(self, spark, dataframe, key, delta_path=None, delta_table=None, debug=False):

        delta_table = self._get_delta_table(spark, delta_path, delta_table)

        df_update = dataframe.alias(self.DataFrameAlias.UPDATE)

        mergeCondition = f"{self.DataFrameAlias.UPDATE}.{key} == {self.DataFrameAlias.DELTA}.{key}"
        if debug: print("mergeCondition:", mergeCondition)

        delta_table.alias(self.DataFrameAlias.DELTA).merge(source=df_update, condition=mergeCondition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        return None


    def run_type_2(self, spark, dataframe, key, delta_path=None, delta_table=None, current_flag=None,
                   effective_date=None, end_date=None, debug=False):

        if current_flag is None:
            current_flag = self.current_flag

        if effective_date is None:
            effective_date = self.effective_date

        if end_date is None:
            end_date = self.end_date

        delta_table = self._get_delta_table(spark, delta_path, delta_table)

        df_delta = delta_table.toDF().alias(self.DataFrameAlias.DELTA)

        df_update = dataframe.alias(self.DataFrameAlias.UPDATE)

        no_keys = [column for column in dataframe.columns if column not in [key, current_flag, effective_date, end_date]]
        if debug: print("no_keys:", no_keys)

        no_keys_comparative = ' '.join([f" AND {self.DataFrameAlias.DELTA}.{x} <> {self.DataFrameAlias.UPDATE}.{x}" for x in no_keys])
        if debug: print('no_keys_comparative:', no_keys_comparative)

        newRecordsOfExistingKeysCondition = f"{self.DataFrameAlias.DELTA}.{current_flag} = true {no_keys_comparative}"
        if debug: print(newRecordsOfExistingKeysCondition)

        # Rows 1: Will be inserted in the `whenNotMatched` clause
        rows1 = df_update.join(df_delta, key).where(newRecordsOfExistingKeysCondition) \
            .selectExpr(f"NULL as mergeKey", f"{self.DataFrameAlias.UPDATE}.*")

        # Rows 2: Will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
        rows2 = df_update.selectExpr(f"{self.DataFrameAlias.UPDATE}.{key} as mergeKey", "*")

        # Stage the update by unioning two sets of rows
        stagedUpdates = rows1.unionByName(rows2).alias(self.DataFrameAlias.STAGED_UPDATE)

        no_keys_staged_comparative = ' '.join(
            [f" AND {self.DataFrameAlias.DELTA}.{x} <> {self.DataFrameAlias.STAGED_UPDATE}.{x}" for x in no_keys])
        if debug: print('no_keys_staged_comparative:', no_keys_staged_comparative)

        whenMatchedUpdateCondition = f"{self.DataFrameAlias.DELTA}.{current_flag} = true {no_keys_staged_comparative}"
        if debug: print('whenMatchedUpdateCondition:', whenMatchedUpdateCondition)

        whenMatchedUpdateSet = {current_flag: "false", end_date: f"{self.DataFrameAlias.STAGED_UPDATE}.{effective_date}"}
        if debug: print('whenMatchedUpdateSet:', whenMatchedUpdateSet)

        mergeCondition = f"{self.DataFrameAlias.DELTA}.{key} = mergeKey"
        if debug: print("mergeCondition:", mergeCondition)

        whenNotMatchedInsertValues = {x: f"{self.DataFrameAlias.STAGED_UPDATE}.{x}" for x in no_keys}
        whenNotMatchedInsertValues.update({key: f"{self.DataFrameAlias.STAGED_UPDATE}.{key}", current_flag: "true",
                                           effective_date: f"{self.DataFrameAlias.STAGED_UPDATE}.{effective_date}"})
        if debug: print("whenNotMatchedInsertValues:", whenNotMatchedInsertValues)

        delta_table.alias(self.DataFrameAlias.DELTA).merge(source=stagedUpdates, condition=mergeCondition) \
            .whenMatchedUpdate(condition=whenMatchedUpdateCondition, set=whenMatchedUpdateSet) \
            .whenNotMatchedInsert(values=whenNotMatchedInsertValues) \
            .execute()

        return None

    @staticmethod
    def _get_delta_table(spark, delta_path, delta_table):

        if [delta_path, delta_table].count(None) == 2:
            raise ValueError("delta_path ou delta_table deve ser passado")

        if delta_path is not None:
            delta_table = DeltaTable.forPath(spark, delta_path)

        else:
            delta_table = DeltaTable.forName(spark, delta_table)

        return delta_table