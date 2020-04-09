import json
from os.path import exists
from os import mkdir
from config.data import DataContainer
from gservices_utils.goaclient import GoogleClient
import gservices_utils.gsheet as gsh
from datetime import datetime

class DataPusher:
    
    __configuration = None
    __engine = None
    
    def __init__(self, configuration, engine):
        self.__configuration = configuration
        self.__engine = engine

    
    def get_query(self, query_config):
        queries_dir = self.__configuration.ENV['queries_dir']
        context = query_config['context']
        query_file = query_config['query_file']
        
        query_file_dir = queries_dir + "/" + context + "/" + query_file
        query = None
        
        if not exists(query_file_dir):
            raise FileNotFoundError(f"The query file '{query_file}' was not found in '{queries_dir}/{context}' directory")
        
        with open(query_file_dir, 'r') as file:
            query = file.read()
            file.close()
        
        limiter = query.index(";")
        query = query[0:limiter + 1]
        
        return query
    
    def get_previous_dir(self, query_config):
        
        queries_dir = self.__configuration.ENV['queries_dir']
        context_dir = queries_dir + "/" + query_config['context']
        previous_dir = context_dir + "/" + "previous-version"
        
        if not exists(context_dir):
            context = query_config['context']
            raise FileNotFoundError(f"The '{queries_dir}/{context}' directory has not found.")
            
        if not exists(previous_dir):
            mkdir(previous_dir)
            
            return previous_dir
        
        else:
            
            return previous_dir
    
    def save_previous_version(self, smanager, query_config, directory):
        
        if 'previous_version' not in query_config:
            
            raise NameError("'previous_version' property has not defined in JSON file.")
        
        else:
            now = datetime.now().strftime("%y-%m-%d_%H-%M-%S")
            previous_name = query_config['previous_version']
            
            # Convertir en DataFrame contenido a reemplazar de la hoja previous version 
            try:
                previous_df = smanager.sheet_to_dataFrame(previous_name)
                
            except gsh.EmptySheetException:
                print(f"The range defined for the '{previous_name}' sheet does not return data")
                
                return False
            
            except gsh.SheetWithoutRowsException:
                
                return True
                
            path = directory + "/" + f"{previous_name}-{now}.xlsx"
            
            # Hacer backup del contenido a reemplazar de la hoja previous-version
            previous_df.to_excel(path, index=False)
            print(f"Previous version saved in {path}") 
            
            return True

            
    def move_to_previous_version(self, smanager, sheet_name, query_config):
        
            previous_name = query_config['previous_version']
            try:
                current_df = smanager.sheet_to_dataFrame(sheet_name)
                
            except gsh.EmptySheetException:
                print(f"The range defined for the '{previous_name}' sheet does not return data")
                
                return False
            
            except gsh.SheetWithoutRowsException:
                
                return True
            
            smanager.dataFrame_to_sheet(current_df, previous_name)
            print(f"The contents of the '{sheet_name}' sheet have been moved to the '{previous_name}' sheet")
            
            return True
            
    
    def push(self):
        config = self.__configuration
        gservices = config.gservices_config
        query_config_container = self.__configuration.query_config
        
        client = GoogleClient(gservices['credentials_path'], gservices['scopes'])
        service = client.get_service('sheets')
        
        for query_config in query_config_container:
            query = self.get_query(query_config)
            spreadsheet_id = gservices['default_spreadsheet_id']
            sheet_name = gservices['default_sheet_name']
        
            if 'spreadsheet_id' in query_config:
                spreadsheet_id = query_config['spreadsheet_id']
        
            if 'sheet_name' in query_config:
                sheet_name = query_config['sheet_name']
            
            # Descargar Datos de la Consulta SQL y convertir a DataFrame
            print("Get data from database...")
            sql_df = DataContainer(self.__engine, query).to_dataframe()
            print("Dataframe from sql has been created.")
            if 'column_dtype' in query_config:
                dtype_config = query_config['column_dtype']
                sql_df =  gsh.GSheetUtils.set_dataFrame_dtype(sql_df, dtype_config)
            
            previous_version_dir = self.get_previous_dir(query_config)
            
            
            smanager = gsh.SpreadSheetManager(service, spreadsheet_id)
            
            # Preparar el contenido en la hoja principal para pasarlo a la hoja previos-version
            saved = self.save_previous_version(smanager, query_config, previous_version_dir)
                        
            if saved is True:
                # Limpiar contenido de la hoja previous-version
                smanager.clear_sheet(query_config['previous_version'])
            
            # Cargar contenido de la hoja principal a la hoja previous-version
            moved = self.move_to_previous_version(smanager, sheet_name, query_config)
            
            if moved is True:
                # Limpiar contenido de la hoja principal
                smanager.clear_sheet(sheet_name)
            
            # Actualizar contenido de la hoja principal con los datos de la consulta SQL
            smanager.dataFrame_to_sheet(sql_df, sheet_name)
            
            print(f"The contents of the sheet '{sheet_name}' have been updated.")
                                    
    
