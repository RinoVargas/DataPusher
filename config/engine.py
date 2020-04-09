import psycopg2
from sqlalchemy import create_engine
from pandas import read_sql

class DBEngineConfigurator:
    
    __configuration = None
    
    def __init__(self, configuration):
        self.__configuration = configuration
    
    def _create_engine(self):
        creds = self.__configuration.db_credentials
        dialect_driver = "postgresql://{0}:{1}@{2}:5432/{3}".format(creds['user'],creds['password'], 
                                                                    creds['host'], creds['dbname'])
        return create_engine(dialect_driver)