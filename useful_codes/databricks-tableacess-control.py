from pyspark.sql.functions import col, collect_list


class TableAccessControl:

    def __init__(self, spark):
        self.spark = spark

    def run(self, principals, returnType='list'):

        if isinstance(principals, str):
            principals = [principals]

        principals_list = []

        for principal in principals:
            default_shuffle_partitions = self.spark.conf.get('spark.sql.shuffle.partitions')
            self.spark.conf.set('spark.sql.shuffle.partitions', '1')

            databases = self.get_databases()

            grants_on_databases = self.get_grants_on_object(principal, databases, 'DATABASE')

            tables = self.get_tables(databases)

            grants_on_tables = self.get_grants_on_object(principal, tables, 'TABLE')

            tables_with_grant = self.get_tables_with_grant(grants_on_tables)

            all_grants = grants_on_databases.unionByName(grants_on_tables).distinct()

            grants_jsons = self.get_jsons(principal, tables_with_grant, all_grants)

            self.spark.conf.set('spark.shuffle.partitions', str(default_shuffle_partitions))

            principals_list.append(grants_jsons)

        if returnType == 'list':
            return principals_list

        elif returnType == 'dataframe':
            return self.build_dataframe_from_json(principals_list)

        elif returnType == 'json':
            principals_json = {'principals': {}}
            for principal in principals_list:
                principals_json['principals'].update(principal)
            return principals_json
        else:
            raise ValueError('returnType must be "json", "dataframe" or "list"')

    def build_dataframe_from_json(self, principals_list):
        list_principals = []
        schema = 'Principal string, ObjectType string, ObjectName string,' \
                 ' Grants array<string>, Location string, ViewSelect string'
        for json in principals_list:
            for principal_name, object_types in json.items():
                for object_type, object_values in object_types.items():
                    for object_name, values in object_values.items():
                        if object_type == "databases":
                            list_principals.append(
                                [principal_name, 'Database', object_name, values['GRANTS'], None, None])
                        elif object_type == 'tables':
                            if values['Type'] == 'VIEW':
                                list_principals.append(
                                    [principal_name, values['Type'], object_name, values['GRANTS'], None,
                                     values['View Text']])
                            else:
                                list_principals.append(
                                    [principal_name, values['Type'], object_name, values['GRANTS'],
                                     values['Location'], None])

        return self.spark.createDataFrame(list_principals, schema=schema)

    def get_jsons(self, principal, tables_with_grant, all_grants):

        json_tables_paths = self.get_json_tables_paths(principal, tables_with_grant)

        json_databases_grants = self.get_json_grants(principal, all_grants, 'DATABASE')

        json_tables_grants = self.get_json_grants(principal, all_grants, 'TABLE')

        final_json = self._merge_final_dicts(principal, json_databases_grants, json_tables_grants, json_tables_paths)

        return final_json

    def get_databases(self):
        return [x.databaseName for x in self.spark.sql("SHOW DATABASES").collect()]

    def get_tables(self, databases):

        if isinstance(databases, str):
            databases = [databases]

        tables_list = []
        for database in databases:
            self.spark.sql("USE " + database)
            tables_list += [x.database + '.' + x.tableName for x in
                            self.spark.sql("SHOW TABLES").filter('isTemporary == false').collect()]

        return tables_list

    def get_grants_on_object(self, principal, object_keys, object_type):

        grants = self._build_dummy_grants_df()
        for object_key in object_keys:
            grants = grants.unionByName(
                self.spark.sql("SHOW GRANT `{0}` ON {1} {2}".format(principal, object_type, object_key)))

        return grants

    def _build_dummy_grants_df(self):
        return self.spark.range(0).selectExpr(*['"dummy" as ' + dummy_col for dummy_col in
                                                ['Principal', 'ActionType', 'ObjectType', 'ObjectKey']])

    def get_json_tables_paths(self, principal, tables):

        dict_tables = {principal: {}}
        dict_tables[principal]['tables'] = {}
        values_to_filter = ['Type', 'Location', 'View Text']

        for table in tables:
            dict_tables[principal]['tables'][table] = dict([(x[0], x[1]) for x in
                                                            self.spark.sql("DESCRIBE EXTENDED " + table)
                                                           .filter(col('col_name').isin(*values_to_filter))
                                                           .collect()])

        return dict_tables

    @staticmethod
    def get_json_grants(principal, grants_df, object_type):

        objects = {'TABLE': 'tables', 'DATABASE': 'databases'}

        if object_type not in objects:
            raise ValueError('Object_type {0} not registered. Must be in ({1})'.format(
                object_type, ', '.join(objects.keys())))

        rows = grants_df.filter('ObjectType = "{0}"'.format(object_type)) \
            .groupBy('ObjectKey').agg(collect_list('ActionType').alias("ActionType")) \
            .collect()

        dict_grants = {principal: {}}
        dict_grants[principal][objects[object_type]] = {}

        for row in rows:
            dict_grants[principal][objects[object_type]][row.ObjectKey] = {}
            dict_grants[principal][objects[object_type]][row.ObjectKey]['GRANTS'] = row.ActionType

        return dict_grants

    @staticmethod
    def get_tables_with_grant(grants_on_tables):
        return [x.ObjectKey for x in grants_on_tables.select('ObjectKey').distinct().collect()]

    @staticmethod
    def _merge_final_dicts(principal, json_databases_grants, json_tables_grants, json_tables_paths):

        grants_json = json_databases_grants.copy()
        grants_json[principal]['tables'] = json_tables_paths[principal]['tables']

        for table in json_tables_grants[principal]['tables']:
            if table in json_tables_grants[principal]['tables']:
                grants_json[principal]['tables'][table]['GRANTS'] = json_tables_grants[principal]['tables'][table][
                    'GRANTS']

        return grants_json
