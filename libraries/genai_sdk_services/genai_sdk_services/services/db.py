### This code is property of the GGAO ###


# Native imports
import os
import re
import time
import logging
import json
from functools import reduce
from abc import ABCMeta, abstractmethod

# Installed imports
import boto3
import pyathena
import pymysql
import redis
from psycopg2 import connect, extras
import psycopg2

class BaseDBService():

    @abstractmethod
    def _get_db_connection(self, origin):
        """ Create the connection with the database

        param origin: Database to connect to
        return: Connection
        """
        pass

    @abstractmethod
    def insert(self, origin, table, fields, values):
        """ Insert data into db

        param origin: Database to insert to
        param table: Table to insert data into
        param fields: Fields of the data
        param values: Values of the fields
        return: Id of the result
        """
        pass

    @abstractmethod
    def update(self, origin, table, fields, values, **kwargs):
        """ Update data in db

        param origin: Database to update data from
        param table: Table to update data from
        param fields: Fields of the data
        param values: Values of the fields
        param kwargs: Other options
        """
        pass

    @abstractmethod
    def select(self, origin, fields, tables, **kwargs):
        """ Select data from the db

        param origin: Database to select from
        param fields: Fields to select
        param tables: Tables to select from
        param kwargs: Other options
        return: Data selected
        """
        pass

    @abstractmethod
    def get_query_select(self, origin, fields, tables, **kwargs):
        """ Select data from the db

        param origin: Database to select from
        param fields: Fields to select
        param tables: Tables to select from
        param kwargs: Other options
        return: Data selected
        """
        pass

    @abstractmethod
    def execute_query(self, origin, query, **kwargs):
        """ Execute query

        return: Result of query
        """
        pass

    @abstractmethod
    def count(self, origin, fields, tables, **kwargs):
        """ Count data from the db

        param origin: Database to select from
        param fields: Fields to select
        param tables: Tables to select from
        param kwargs: Other options
        return: Data selected
        """
        pass

    @abstractmethod
    def delete(self, origin, table, **kwargs):
        """ Delete data from the db

        param origin: Database to delete from
        param table: Table to delete data from
        param kwargs: "Where" options
        """
        pass

    @classmethod
    def check_origin(cls, origin_type):
        """ Check if it is a valid origin for the service

        param origin_type: Origin to check
        return: (bool) True if the origin is valid
        """
        return origin_type in cls.ORIGIN_TYPES

    @abstractmethod
    def purge(self, origin):
        pass


class MysqlService(BaseDBService):
    ORIGIN_TYPES = ["mysql", "mariadb"]

    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "mysql", "mysql.json")
    env_vars = ["SQLDB_HOST", "SQLDB_USER", "SQLDB_PASSWORD", "SQLBD_PORT"]

    def __init__(self):
        """ Init the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the database

        :param origin: (str) Database to set credentials
        :param credentials: (dict) Credentials to set
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'host': os.getenv(self.env_vars[0]),
                        'user': os.getenv(self.env_vars[1]),
                        'password': os.getenv(self.env_vars[2]),
                        'port': os.getenv(self.env_vars[3], 3306)
                    }
                else:
                    raise Exception("Credentials not found")

            credentials['db'] = origin

            self.credentials[origin] = credentials

    def _get_db_connection(self, origin: str):
        """ Create the connection with the database

        :param origin: (str) Database to connect to
        :return: Connection
        """
        self.logger.debug("Connecting with MYSQL database.")

        return pymysql.connect(
            host=self.credentials[origin]['host'],
            user=self.credentials[origin]['user'],
            password=self.credentials[origin]['password'],
            port=self.credentials[origin]['port'],
            db=self.credentials[origin]['db'],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    def insert(self, origin: str, table: str, fields: list, values: list) -> str:
        """ Insert data into db

        :param origin: (str) Database to insert to
        :param table: (str) Table to insert data into
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :return: (str) Id of the result
        """
        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(str(x) for x in values)})"

        self.logger.debug(f"Inserting in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            result = cursor.lastrowid
            connection.close()
            return result

    def update(self, origin: str, table: str, fields: list, values: list, **kwargs: dict):
        """ Update data in db

        :param origin: (str) Database to update data from
        :param table: (str) Table to update data from
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :param kwargs: (dict) "Where" options
        """
        sets = ""
        for i in range(len(fields)):
            vals = f"{values[i]}" if isinstance(values[i], str) else f"{values[i]}"
            sets += f"{fields[i]}={vals}, "
        sets = sets[:-2]

        sql = f"UPDATE {table} SET {sets}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        self.logger.debug(f"Updating in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            connection.close()

    def upsert(self, origin: str, table: str, fields: list, values: list) -> dict:
        """ Upsert data into db

        :param origin: (str) Database to insert to
        :param table: (str) Table to insert data into
        :param fields: (list) Fields of the data to be inserted
        :param values: (list) Values of the fields. List of lists
            Elements in list represent rows to upsert and sublist represent values to upsert on each row
        :return: (str) Id of the result
        """

        sets = ""
        for i in range(len(values)):
            sets += f"({', '.join(str(x) for x in values[i])}), "
        sets = sets[:-2]

        updates = ""
        for i in range(len(fields)):
            updates += f"{fields[i]}=VALUES({fields[i]}), "
        updates = updates[:-2]

        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES {sets} ON DUPLICATE KEY UPDATE {updates}"

        self.logger.debug(f"Upserting in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            result = cursor.lastrowid
            connection.close()
            return result

    def select(self, origin: str, fields: list, tables: list, **kwargs: dict) -> list:
        """ Select data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) "Where, Group and order by" options
        :return: (list) Data selected
        """
        sql = f"SELECT {', '.join(fields)} FROM {', '.join(tables)}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        if "group" in kwargs.keys():
            group = kwargs["group"]
            sql += f" GROUP BY {', '.join(group)}"

        if "order" in kwargs.keys():
            order_options = kwargs["order"]
            order = ""
            for i in range(len(order_options)):
                order += f"{list(order_options.keys())[i]} {list(order_options.values())[i]}, "
            order = order[:-2]

            sql += f" ORDER BY {order}"

        if "limit" in kwargs.keys():
            sql += f" LIMIT {kwargs['limit']}"

        self.logger.debug(f"Selecting from database: {sql}")

        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            connection.commit()
            connection.close()
            return result

    def execute_query(self, origin: str, query: str, **kwargs: dict) -> list:
        """ Execute query

        :param origin: (str) Database to execute query
        :param query: (str) Query to execute
        :param kwargs: (dict) "Where, Group and order by" options
        :return: (list) Result of query
        """
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()
            connection.close()
            if isinstance(result, list):
                return result

    def count(self, origin: str, fields: list, tables: list, **kwargs: dict):
        """ Count data from the db

        :param origin: Database to select from
        :param fields: Fields to select
        :param tables: Table to select from
        :param kwargs: MANDATORY: Output file. "Where, Group and order by" options
        :return: Count of data
        """
        pass

    def delete(self, origin: str, table: str, **kwargs: dict):
        """ Delete data from the db

        :param origin: (str) Database to delete from
        :param table: (str) Table to delete data from
        :param kwargs: (dict) "Where" options
        """
        sql = f"DELETE FROM {table}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        self.logger.debug(f"Deleting from database: {sql}")

        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            connection.close()

    def purge(self, origin: str):
        """ Purge the database

        :param origin: (str) Database to purge
        """
        pass


class RedisService(BaseDBService):
    ORIGIN_TYPES = ["redis"]

    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "redis", "redis.json")
    env_vars = ["REDIS_HOST", "REDIS_PASSWORD", "REDIS_PORT"]

    def __init__(self):
        """ Init the service """
        self.logger = logging.getLogger(__name__)

    def _get_db_connection(self, origin: str):
        """ Create the connection with the database

        :param origin: (str) Database to connect to
        :return: Connection
        """
        self.logger.debug("Connecting with Redis database.")
        try:
            if self.credentials[origin].get('password', ""):
                connection = redis.Redis(
                    host=self.credentials[origin]['host'],
                    db=self.credentials[origin]['db'],
                    password=self.credentials[origin]['password'],
                    port=self.credentials[origin]['port'],
                )
            else:
                connection = redis.Redis(
                    host=self.credentials[origin]['host'],
                    db=self.credentials[origin]['db'],
                    port=self.credentials[origin]['port'],
                )

            return connection
        except Exception as ex:
            self.logger.error("Error while connecting to %s" % origin)
            raise ex

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the Redis

        :param origin: (str) type of the queue and the name of the queue
        :param credentials: (dict) Credentials to connect to the Redis
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'host': os.getenv(self.env_vars[0]),
                        'password': os.getenv(self.env_vars[1]),
                        'port': os.getenv(self.env_vars[2])
                    }
                else:
                    raise Exception("Credentials not found")

            credentials['db'] = origin
            self.credentials[origin] = credentials

    def insert(self, origin: str, key: str, data_type: str, values: list):
        """ Insert data into db

        :param origin: (str) Database to insert to
        :param key: (str) Key to insert data into
        :param data_type: (str) Type of the data (string/int)
        :param values: (list) Values of the fields
        """
        self.logger.debug(f"Inserting in database: {key}")
        connection = self._get_db_connection(origin)
        connection.set(key, values, nx=True)

    def update(self, origin: str, key: str, fields: list, values: list, **kwargs: dict) -> dict:
        """ Update data in db

        :param origin: (str) Database to update data from
        :param key: (str) Key to update
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :param kwargs: (dict) "Where" options
        :return: (dict) Result of the update
        """
        self.logger.debug(f"Updating in database: {key}")
        connection = self._get_db_connection(origin)

        if "incr" in kwargs:
            return connection.incr(key, kwargs['incr'])
        elif "decr" in kwargs:
            return connection.decr(key, kwargs['decr'])
        else:
            return connection.set(key, values)

    def select(self, origin: str, key: str, tables: list = None, **kwargs: dict) -> list:
        """ Select data from the db

        :param origin: (str) Database to select from
        :param key: (str) Key to select from
        :param tables: (list) Tables to select from
        :param kwargs: (dict) "Where" options
        :return: (dict) Data selected
        """
        self.logger.debug(f"Selecting from database: {origin}")

        connection = self._get_db_connection(origin)

        keys = []
        if "match" in kwargs:
            result = connection.scan(cursor=0, match=kwargs['match'], count=1000)
            keys.extend(result[1])

            while result[0] != 0:
                result = connection.scan(
                    cursor=result[0], match=kwargs['match'], count=1000
                )
                keys.extend(result[1])

            values = []
            for key_ in keys:
                values.append({'key': key_, 'values': connection.get(key_)})
        else:
            values = [{'key': key, 'values': connection.get(key)}]
        return values

    def count(self, origin: str, fields: list, tables: list, **kwargs: dict) -> int:
        """ Count data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) Output file. "Where, Group and order by" options
        :return: (int) Count of data
        """
        pass

    def delete(self, origin: str, keys: list, **kwargs: dict):
        """ Delete data from the db

        :param origin: Database to delete from
        :param keys: Keys to delete
        :param kwargs: "Where" options
        """
        self.logger.debug(f"Deleting from database: {keys}")

        connection = self._get_db_connection(origin)
        connection.delete(*keys)

    def purge(self, origin: str):
        """ Purge the database

        :param origin: Database to purge
        """
        self.logger.debug(f"Purging from database: {origin}")
        connection = self._get_db_connection(origin)
        connection.flushdb()


class AthenaService(BaseDBService):
    ORIGIN_TYPES = ["athena"]

    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "aws", "aws.json")
    env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION_NAME"]

    def __init__(self):
        """ Init the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the bucket

        :param origin: (str) Bucket to set the credentials
        :param credentials: (dict) Credentials to set
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'access_key': os.getenv(self.env_vars[0]),
                        'secret_key': os.getenv(self.env_vars[1]),
                        'region_name': os.getenv(self.env_vars[2])
                    }
                elif eval(os.getenv("AWS_ROLE", "False")):
                    credentials = {'region_name': os.getenv(self.env_vars[2])}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def _get_db_connection(self, origin: str):
        """ Create the client

        :param origin: (str) Database to connect to
        :return: Connection
        """
        self.logger.debug("Connecting with Athena database")
        region_name = self.credentials[origin].get('region_name')

        try:
            if eval(os.getenv("AWS_ROLE", "False")):
                db_client = boto3.client("athena", region_name=region_name)
            else:
                db_client = boto3.client("athena", aws_access_key_id=self.credentials[origin]['access_key'], aws_secret_access_key=self.credentials[origin]['secret_key'], region_name=region_name)
            
            return db_client
        except Exception as ex:
            self.logger.error("Error while connecting to %s" % origin)
            raise ex

    def _get_db_connection_inline(self, origin: str, staging_dir: str):
        """ Create the client

        :param origin: Database to connect to
        :param staging_dir: Staging directory for the query
        :return: Connection
        """
        self.logger.debug("Connecting with Athena database.")
        region_name = self.credentials[origin].get('region_name')
        
        if eval(os.getenv("AWS_ROLE", "False")):
            db_client = pyathena.connect(region_name=region_name)
        else:
            db_client = pyathena.connect(aws_access_key_id=self.credentials[origin]['access_key'], aws_secret_access_key=self.credentials[origin]['secret_key'], s3_staging_dir=f"s3://{origin}/{staging_dir}", region_name=region_name)
        
        return db_client

    def _get_result(self, client, execution: dict) -> str:
        """ Get the result of the query

        :param client: Athena client
        :param execution: (str) Execution of the query
        :return: (str) Result of the query
        """
        execution_id = execution['QueryExecutionId']
        state = "RUNNING"

        while state in ["RUNNING", "QUEUED"]:
            response = client.get_query_execution(QueryExecutionId=execution_id)

            if (
                    "QueryExecution" in response
                    and "Status" in response['QueryExecution']
                    and "State" in response['QueryExecution']['Status']
            ):
                state = response['QueryExecution']['Status']['State']
                if state == "FAILED":
                    self.logger.warning(response)
                    return "failed"
                elif state == "SUCCEEDED":
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall(".*\/(.*)", s3_path)[0]
                    return filename
            time.sleep(1)

        return "failed"

    def create(self, origin: str, table: str, s3_path: str, **kwargs: dict) -> str:
        """ Create a table in the db

        :param origin: (str) Databse in which table will be created
        :param table: (str) Table to create
        :param s3_path: (str) S3 path to create table of
        :param kwargs: (dict) Options for the query
        :return: (str) Query result
        """
        sql = self.get_query_create(table, s3_path, **kwargs)

        self.logger.debug(f"Creating table: {sql}")

        connection = self._get_db_connection(origin)
        query_start = connection.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": origin},
            ResultConfiguration={"OutputLocation": "s3://" + s3_path},
        )
        return self._get_result(connection, query_start)

    def partition(self, origin: str, table_name: str, s3_path: str) -> str:
        """ Repair the table

        :param origin: (str) Database to repair
        :param table_name: (str) Table to repair
        :param s3_path: (str) S3 path to repair
        :return: (str) Query result
        """
        sql = f"MSCK REPAIR TABLE {table_name};"
        connection = self._get_db_connection(origin)
        query_start = connection.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": origin},
            ResultConfiguration={"OutputLocation": "s3://" + s3_path},
        )

        return self._get_result(connection, query_start)

    @staticmethod
    def get_query_create(table_name: str, s3_path: str, **kwargs: dict) -> str:
        """ Get the query to create the table

        :param table_name: (str) Table to create
        :param s3_path: (str) S3 path to create table of
        :param kwargs: (dict) Options for the query
        :return: (str) Query to create the table
        """
        fields = kwargs.get('fields', "\\t")
        lines = kwargs.get('lines', "\\n")
        input = kwargs.get('input', "org.apache.hadoop.mapred.TextInputFormat")
        output = kwargs.get(
            'output', "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )

        columns = ""
        partitioned = ""
        if "type" in kwargs:
            if kwargs['type'] == "DATASET":
                columns = f"""filename string,
                              text string,
                              language string, 
                              n_pags int"""
                partitioned = f"""label string"""

            elif kwargs['type'] == "PREDICTIONS":
                if (
                        "n_classes" in kwargs
                        and isinstance(kwargs['n_classes'], int)
                        and kwargs['n_classes'] > 0
                ):
                    columns_classes = reduce(
                        lambda x, y: x + ",\n" + "\t" * 9 + y,
                        [
                            f"""class_{idx + 1} string,
                                  confidence_{idx + 1} float"""
                            for idx in range(0, kwargs["n_classes"])
                        ],
                    )
                else:
                    raise ValueError("n_classes not specified in PREDICTIONS type")

                columns = f"""filename string,
                              timestamp double,
                              top_prediction float,
                              below_threshold boolean,
                              {columns_classes}"""

            elif kwargs['type'] == "PREDICTIONS_MULTI":
                columns = f"""filename string,
                              category string,
                              pass_result int,
                              belong_result int,
                              pass_result_score float,
                              belong_result_score float
                              """

            elif kwargs['type'] == "IMG_FEATURES":
                columns = f"""filename string,
                            features string"""

            elif kwargs['type'] == "CLUSTER_TXT":
                columns = f"""filename string,
                            cluster_text int,
                            group_text int,
                            recommended_txt boolean"""

            elif kwargs['type'] == "CLUSTER_IMG":
                columns = f"""filename string,
                            cluster_img int,
                            group_img int,
                            recommended_img boolean"""

            elif kwargs['type'] == "CLUSTER_HYBRID":
                columns = f"""filename string,
                            cluster_text int,
                            group_text int,
                            recommended_txt boolean,
                            cluster_img int,
                            group_img int,
                            recommended_img boolean"""

            elif kwargs['type'] == "EXTRACTION":
                if (
                        "n_entities" in kwargs
                        and isinstance(kwargs['n_entities'], int)
                        and kwargs['n_entities'] > 0
                ):

                    columns_classes = reduce(
                        lambda x, y: x + ",\n" + "\t" * 9 + y,
                        [
                            f"""entity_{idx + 1} string"""
                            for idx in range(0, kwargs['n_entities'])
                        ],
                    )
                else:
                    raise ValueError("n_entities not specified in EXTRACTION type")

                columns = f"""filename string,
                              timestamp double,
                              {columns_classes}"""

            elif kwargs['type'] == "IR":
                if (
                        "n_metadata" in kwargs
                        and isinstance(kwargs['n_metadata'], int)
                        and kwargs['n_metadata'] > 0
                ):

                    columns_metadata = reduce(
                        lambda x, y: x + ",\n" + "\t" * 9 + y,
                        [
                            f"""metadata_{idx + 1} string"""
                            for idx in range(0, kwargs["n_metadata"])
                        ],
                    )
                else:
                    raise ValueError("n_metadata not specified in IR type")

                columns = f"""filename string,
                              text string,
                              language string, 
                              n_pags int,
                              {columns_metadata}"""

        else:
            raise ValueError("Type of table not specified")

        if partitioned:
            sql = f"""
                CREATE EXTERNAL TABLE {table_name}(
                  {columns}
                )
                PARTITIONED BY ({partitioned})
                ROW FORMAT DELIMITED 
                    FIELDS TERMINATED BY '{fields}' 
                    LINES TERMINATED BY '{lines}'
                STORED AS INPUTFORMAT 
                    '{input}'
                OUTPUTFORMAT 
                    '{output}'
                LOCATION
                  's3://{s3_path}';
            """
        else:
            sql = f"""
                    CREATE EXTERNAL TABLE {table_name}(
                      {columns}
                    )
                    ROW FORMAT DELIMITED 
                        FIELDS TERMINATED BY '{fields}' 
                        LINES TERMINATED BY '{lines}'
                    STORED AS INPUTFORMAT 
                        '{input}'
                    OUTPUTFORMAT 
                        '{output}'
                    LOCATION
                      's3://{s3_path}';
                """

        return sql

    def insert(self, origin: str, table: str, fields: list, values: list):
        """ Insert data into table

        :param origin: (str) Database to insert to
        :param table: (str) Table to insert data into
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        """
        pass

    def update(self, origin: str, table: str, fields: list, values: list, **kwargs: dict):
        """ Update data in db

        :param origin: (str) Database to update data from
        :param table: (str) Table to update data from
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :param kwargs: (dict) "Where" options
        """
        pass

    def select(self, origin: str, fields: list, tables: list, **kwargs: dict) -> str:
        """ Select data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) Output file. "Where, Group and order by" options
        :return: (str) Data selected
        """
        sql = f"SELECT {', '.join(fields)} FROM {', '.join(tables)}"

        if "where" in kwargs.keys():
            where_options = kwargs['where']
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        if "order" in kwargs.keys():
            order_options = kwargs['order']
            order = ""
            for i in range(len(order_options)):
                order += f"'{list(order_options.keys())[i]}' {list(order_options.values())[i]}, "
            order = order[:-2]

            sql += f" ORDER BY {order}"

        if "group" in kwargs.keys():
            group = kwargs['group']
            sql += f" GROUP BY {', '.join(group)}"

        if "except_q" in kwargs.keys():
            except_q = kwargs['except_q']
            sql += f" EXCEPT {except_q}"

        if "union_q" in kwargs.keys():
            union_q = kwargs['union_q']
            sql += f" UNION {union_q}"

        if "limit" in kwargs.keys():
            sql += f" LIMIT {kwargs['limit']}"

        self.logger.debug(f"Selecting from database: {sql}")

        connection = self._get_db_connection(origin)
        query_start = connection.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': origin},
            ResultConfiguration={'OutputLocation': "s3://" + kwargs['output']},
        )
        return self._get_result(connection, query_start)

    def get_query_select(self, origin: str, fields: list, tables: list, **kwargs: dict) -> str:
        """ Select data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) Output file. "Where, Group and order by" options
        :return: (str) Data selected
        """
        sql = f"SELECT {', '.join(fields)} FROM {', '.join(tables)}"

        if "where" in kwargs.keys():
            where_options = kwargs['where']
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        if "order" in kwargs.keys():
            order_options = kwargs['order']
            order = ""
            for i in range(len(order_options)):
                order += f"'{list(order_options.keys())[i]}' {list(order_options.values())[i]}, "
            order = order[:-2]

            sql += f" ORDER BY {order}"

        if "group" in kwargs.keys():
            group = kwargs['group']
            sql += f" GROUP BY {', '.join(group)}"

        if "except_q" in kwargs.keys():
            except_q = kwargs['except_q']
            sql += f" EXCEPT {except_q}"

        if "union_q" in kwargs.keys():
            union_q = kwargs['union_q']
            sql += f" UNION {union_q}"

        if "limit" in kwargs.keys():
            sql += f" LIMIT {kwargs['limit']}"

        self.logger.debug(f"Selecting from database: {sql}")

        return sql

    def execute_query(self, origin: str, query: str, **kwargs: dict) -> str:
        """ Execute query

        :param origin: (str) Database to select from
        :param query: (str) Query to execute
        :return: (str) Result of query
        """
        connection = self._get_db_connection(origin)
        query_start = connection.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': origin},
            ResultConfiguration={'OutputLocation': "s3://" + kwargs['output']},
        )
        return self._get_result(connection, query_start)

    def count(self, origin: str, fields: list, tables: list, **kwargs: dict) -> int:
        """ Count data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) Output file. "Where, Group and order by" options
        :return: (int) Count of data
        """
        sql = f"SELECT COUNT({', '.join(fields)}) FROM {', '.join([f'{origin}.{table}' for table in tables])}"

        if "where" in kwargs.keys():
            where_options = kwargs['where']
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        if "order" in kwargs.keys():
            order_options = kwargs['order']
            order = ""
            for i in range(len(order_options)):
                order += f"'{list(order_options.keys())[i]}' {list(order_options.values())[i]}, "
            order = order[:-2]

            sql += f" ORDER BY {order}"

        if "group" in kwargs.keys():
            group = kwargs['group']
            sql += f" GROUP BY {', '.join(group)}"

        if "limit" in kwargs.keys():
            sql += f" LIMIT {kwargs['limit']}"

        self.logger.debug(f"Selecting from database: {sql}")

        # TODO: Controlar tables[0]
        if "staging_dir" in kwargs:
            staging_dir = kwargs['staging_dir']
        else:
            staging_dir = tables[0]

        # TODO: Controlar resultado
        connection = self._get_db_connection_inline(origin, staging_dir)
        result = connection.cursor().execute(sql)

        return result.fetchall()[0][0]

    def delete(self, origin: str, table: str, **kwargs: dict) -> str:
        """ Delete data from the db

        :param origin: (str) Database to delete from
        :param table: (str) Table to delete data from
        :param kwargs: (dict) "Where" options
        :return: (str) Result of delete
        """
        query = f"DROP TABLE IF EXISTS {table}"

        connection = self._get_db_connection(origin)
        query_start = connection.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': origin},
            ResultConfiguration={'OutputLocation': "s3://" + kwargs['output']}
        )
        return self._get_result(connection, query_start)

    def purge(self, origin: str):
        """ Purge data from the db

        :param origin: (str) Database to purge
        """
        pass


class PostgreSQLService(BaseDBService):
    ORIGIN_TYPES = ["postgresql"]

    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "postgresql", "postgresql.json")
    env_vars = ["SQLDB_HOST", "SQLDB_USER", "SQLDB_PASSWORD", "SQLBD_PORT"]

    def __init__(self):
        """ Init the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the database

        :param origin: (str) Database to set credentials
        :param credentials: (dict) Credentials to set
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'host': os.getenv(self.env_vars[0]),
                        'user': os.getenv(self.env_vars[1]),
                        'password': os.getenv(self.env_vars[2]),
                        'port': os.getenv(self.env_vars[3], 5432)
                    }
                else:
                    raise Exception("Credentials not found")

            credentials['db'] = origin

            self.credentials[origin] = credentials

    def _get_db_connection(self, origin: str):
        """ Create the connection with the database

        :param origin: (str) Database to connect to
        :return: Connection
        """
        self.logger.debug("Connecting with MYSQL database.")

        try:
            return psycopg2.connect(
                host=self.credentials[origin]['host'],
                user=self.credentials[origin]['user'],
                password=self.credentials[origin]['password'],
                port=self.credentials[origin]['port'],
                dbname=self.credentials[origin]['db'],
                cursor_factory=psycopg2.extras.DictCursor
            )
        except psycopg2.Error as ex:
            self.logger.error(f"Error connecting to database: {ex}")

    def insert(self, origin: str, table: str, fields: list, values: list) -> str:
        """ Insert data into db

        :param origin: (str) Database to insert to
        :param table: (str) Table to insert data into
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :return: (str) Id of the result
        """
        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(str(x) for x in values)})"

        self.logger.debug(f"Inserting in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            result = cursor.lastrowid
            connection.close()
            return result

    def update(self, origin: str, table: str, fields: list, values: list, **kwargs: dict):
        """ Update data in db

        :param origin: (str) Database to update data from
        :param table: (str) Table to update data from
        :param fields: (list) Fields of the data
        :param values: (list) Values of the fields
        :param kwargs: (dict) "Where" options
        """
        sets = ""
        for i in range(len(fields)):
            vals = f"{values[i]}" if isinstance(values[i], str) else f"{values[i]}"
            sets += f"{fields[i]}={vals}, "
        sets = sets[:-2]

        sql = f"UPDATE {table} SET {sets}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        self.logger.debug(f"Updating in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            connection.close()

    def upsert(self, origin: str, table: str, fields: list, values: list) -> dict:
        """ Upsert data into db

        :param origin: (str) Database to insert to
        :param table: (str) Table to insert data into
        :param fields: (list) Fields of the data to be inserted
        :param values: (list) Values of the fields. List of lists
            Elements in list represent rows to upsert and sublist represent values to upsert on each row
        :return: (str) Id of the result
        """

        sets = ""
        for i in range(len(values)):
            sets += f"({', '.join(str(x) for x in values[i])}), "
        sets = sets[:-2]

        updates = ""
        for i in range(len(fields)):
            updates += f"{fields[i]}=VALUES({fields[i]}), "
        updates = updates[:-2]

        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES {sets} ON DUPLICATE KEY UPDATE {updates}"

        self.logger.debug(f"Upserting in database: {sql}")
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            result = cursor.lastrowid
            connection.close()
            return result

    def select(self, origin: str, fields: list, tables: list, **kwargs: dict) -> list:
        """ Select data from the db

        :param origin: (str) Database to select from
        :param fields: (list) Fields to select
        :param tables: (list) Table to select from
        :param kwargs: (dict) "Where, Group and order by" options
        :return: (list) Data selected
        """
        sql = f"SELECT {', '.join(fields)} FROM {', '.join(tables)}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        if "group" in kwargs.keys():
            group = kwargs["group"]
            sql += f" GROUP BY {', '.join(group)}"

        if "order" in kwargs.keys():
            order_options = kwargs["order"]
            order = ""
            for i in range(len(order_options)):
                order += f"{list(order_options.keys())[i]} {list(order_options.values())[i]}, "
            order = order[:-2]

            sql += f" ORDER BY {order}"

        if "limit" in kwargs.keys():
            sql += f" LIMIT {kwargs['limit']}"

        self.logger.debug(f"Selecting from database: {sql}")

        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            connection.commit()
            connection.close()
            return result

    def execute_query(self, origin: str, query: str, **kwargs: dict) -> list:
        """ Execute query

        :param origin: (str) Database to execute query
        :param query: (str) Query to execute
        :param kwargs: (dict) "Where, Group and order by" options
        :return: (list) Result of query
        """
        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(query)
            try:
                result = cursor.fetchall()
            except:
                result = [cursor.rowcount]
            connection.commit()
            connection.close()
            if isinstance(result, list):
                return result

    def count(self, origin: str, fields: list, tables: list, **kwargs: dict):
        """ Count data from the db

        :param origin: Database to select from
        :param fields: Fields to select
        :param tables: Table to select from
        :param kwargs: MANDATORY: Output file. "Where, Group and order by" options
        :return: Count of data
        """
        pass

    def delete(self, origin: str, table: str, **kwargs: dict):
        """ Delete data from the db

        :param origin: (str) Database to delete from
        :param table: (str) Table to delete data from
        :param kwargs: (dict) "Where" options
        """
        sql = f"DELETE FROM {table}"

        if "where" in kwargs.keys():
            where_options = kwargs["where"]
            where = ""
            for i in range(len(where_options)):
                where += f"{where_options[i]} AND "
            where = where[:-5]

            sql += f" WHERE {where}"

        self.logger.debug(f"Deleting from database: {sql}")

        connection = self._get_db_connection(origin)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            connection.close()

    def purge(self, origin: str):
        """ Purge the database

        :param origin: (str) Database to purge
        """
        pass
