from pandas import read_sql

class DataContainer:
    
    query = None
    __data_container = None
    
    def __init__(self, data_container, query):
        self.__data_container = data_container
        self.query = query
    
    def to_dataframe(self):
        conn = self.__data_container._create_engine()
        result = read_sql(self.query, conn)
        
        return result