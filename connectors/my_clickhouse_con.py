from clickhouse_driver import Client


class ClickHouse:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.client = Client(host=host,
                             user=user,
                             password=password,
                             secure=True,
                             settings={'use_numpy': True})

    def get_data_bases(self):
        query = """SHOW DATABASES;"""
        data = self.client.execute(query)
        data = [x[0] for x in data]
        """
        :return: list 
        """
        return data

    def get_query(self, query):
        result = self.client.execute_iter(query, with_column_types=True)
        return result

    def get_tables(self, db_name: str):
        query = f"""SHOW TABLES FROM {db_name}"""
        data = self.client.execute(query)
        data = [x[0] for x in data]
        """
        :return: list 
        """
        return data

    def create_data_base(self, db_name: str):
        query = f"CREATE DATABASE IF NOT EXISTS {db_name};"

        result = self.client.execute(query)
        """
        :return: [] 
        """
        return result

    def create_table(self, db_name:str, table_name:str, partition_name:str, column_types_list:list):
        """
        :param column_types_list:
            [
            {"name": <name for column in table>, "type": <tye of data in column>},
            {"name": <name for column in table>, "type": <tye of data in column>},
            ...
            ]

            name - name for column in table
            type - tye of data in column:
                * Int64 - example: 1
                * Float64 - example: 1.1
                * VARCHAR(255) - example: "String"
                * Date - example: "YYYY-mm-dd"
                * UInt8 (for bool) - example: True or False
                * DateTime - example: "YYYY-mm-dd MM:HH:SS"
        """

        list_params = [[x['name'], x['type']] for x in column_types_list]

        columns = ", ".join([" ".join(x) for x in list_params])

        query = f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ({columns}) ENGINE = MergeTree() PARTITION BY " \
                f"{partition_name} ORDER BY {partition_name}"

        result = self.client.execute(query)
        """
        :return: [] 
        """

        return result

    def create_data_set_and_tables(self, data_set_id, report_dict, client_name, source, client_id=None):
        if client_id is None:
            client_id = ""
        data_set = self.create_data_base(data_set_id)
        assert data_set == [], "Error"

        for report, report_params in report_dict.items():
            table_name = f"{client_name}_{source}_{client_id}_{report}"
            if source in ["GAnalytics", "YaMetrica"]:
                table_name = f"{client_name}_{source}_{report}"
            table = self.create_table(data_set_id, table_name, report_params['partition_name'], report_params['fields'])
            assert table == [], "Error"

        return []

    def insert_dataframe(self, df, db_name, table_name):
        result = self.client.insert_dataframe(f'INSERT INTO {db_name}.{table_name} VALUES', df)
        return result

    def delete_data_base(self, db_name):
        query = f"DROP DATABASE IF EXISTS {db_name}"
        result = self.client.execute(query)
        """
        :return: [] 
        """
        return result

    def delete_table(self, db_name, table_name):
        query = f"DROP TABLE IF EXISTS {db_name}.{table_name}"
        result = self.client.execute(query)
        """
        :return: [] 
        """
        return result
