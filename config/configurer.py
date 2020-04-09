import yaml
from os import getcwd
import json

class ConfigurationLoader:
    
    db_credentials = None
    gservices_config = None
    ENV = None
    query_config = None
    
    def __init__(self):
        self.ENV = self.__load_config('env.yaml')
        self.db_credentials = self.__load_config(self.ENV['db_credentials'])
        self.gservices_config = self.__load_config(self.ENV['gservice_config'])
        self.query_config = self.__load_config(self.ENV['query_config'])
        
    def __load_config(self, config_file):
        config = None
        ext = None
        with open(config_file) as file:
            try:
                ext = config_file.rsplit(".", maxsplit = 1)[-1]
                
                if ext == 'json':
                    config = json.load(file)
                    return config
                
                elif ext == 'yaml' or ext == 'yml':
                    config = tuple(yaml.load_all(file, Loader=yaml.FullLoader))[0]
                    return config
                
                else:
                    raise NameError(f"Unexpected '{ext}' extension. Only 'yaml' and 'json' formats are supported")
            
            except yaml.YAMLError as exc:
                print(exc)
                
            finally:
                file.close()
        
        
