from snowflake.snowpark import Session
import toml


class Constant:
    DATABASE = 'DATA_ENGINEER'
    SCHEMA = 'INGEST'
    ROLE = 'SYSADMIN'
    FILE_FORMAT = 'parquet_format'
    STORE_INTEGRATION = 'AZURE_STORAGE_INT'
    AZURE_BLOB_URL = 'azure://csvstorage01.blob.core.windows.net/container01/parquet/'
    STAGE = 'PARQUET_STG'
    STAGE_FILE = 'PARQUET_STG/output.parquet'
    TARGET_TABLE = 'PARQUET_TBL'


class SnowflakeHelper:

    def __init__(self, connection_file, connection_name):
        self._config = toml.load(connection_file)[connection_name]
        self._session = Session.builder.configs(self._config).create()

    def close_connection(self):
        if self._session:
            self._session.close()

    def execute_query(self, sql):
        self._session.sql(sql).show()

    def init_env(self, role, database, schema, stora_integration):
        self.execute_query(f'USE ROLE {role};')
        self.execute_query(f'USE DATABASE {database};')
        self.execute_query(f'USE SCHEMA {schema};')
        self.execute_query(f'GRANT CREATE STAGE ON SCHEMA {database}.{schema} TO ROLE {role};')
        self.execute_query(f'GRANT CREATE TABLE ON SCHEMA {database}.{schema} TO ROLE {role};')
        self.execute_query(f'GRANT CREATE FILE FORMAT ON SCHEMA {database}.{schema} TO ROLE {role};')
        self.execute_query(f'GRANT USAGE ON INTEGRATION {stora_integration} TO ROLE {role};')

    def create_format(self, file_format):
        sql = f'''
            CREATE FILE FORMAT IF NOT EXISTS {file_format}
                TYPE = parquet;
        '''
        self.execute_query(sql)

    def create_stage(self, stage_name, storage_integration, azure_blob_url, file_format):
        self.execute_query('USE ROLE SYSADMIN;')
        sql = f'''
            CREATE OR REPLACE STAGE {stage_name}
            STORAGE_INTEGRATION = {storage_integration}
            URL = "{azure_blob_url}"
            FILE_FORMAT = "{file_format}";
        '''
        self.execute_query(sql)

    def create_target_table(self, staged_parquet, table_name, file_format):
        """
        Create table from parquet schema using snowflake infer_schema
        """
        create_sql = f'''
                    CREATE TABLE IF NOT EXISTS {table_name}
                    USING TEMPLATE (
                      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                      FROM TABLE(
                        INFER_SCHEMA(
                          LOCATION => '@{staged_parquet}',
                          FILE_FORMAT => '{file_format}'
                        )
                      )
                    );
                '''
        self.execute_query(create_sql)

    def load_into_target_table(self, staged_parquet, table_name, file_format):
        """
        Use match_by_col_name to copy from parquet to the table instead of explicitly casting each column from $1
        """
        sql = f"""
                COPY INTO {table_name}
                FROM @{staged_parquet}
                FILE_FORMAT = '{file_format}'
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """
        self.execute_query(sql)


snowflake_helper = SnowflakeHelper(r'../config/config_template.toml', 'myconnection')
snowflake_helper.init_env(Constant.ROLE, Constant.DATABASE, Constant.SCHEMA, Constant.STORE_INTEGRATION)
snowflake_helper.create_format(Constant.FILE_FORMAT)
snowflake_helper.create_stage(Constant.STAGE, Constant.STORE_INTEGRATION, Constant.AZURE_BLOB_URL, Constant.FILE_FORMAT)
snowflake_helper.create_target_table(Constant.STAGE_FILE, Constant.TARGET_TABLE, Constant.FILE_FORMAT)
snowflake_helper.load_into_target_table(Constant.STAGE_FILE, Constant.TARGET_TABLE, Constant.FILE_FORMAT)
snowflake_helper.close_connection()
