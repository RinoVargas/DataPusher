from pandas import DataFrame

class SpreadSheetManager:
    
    __service = None
    __spreadsheet_id = None
    
    def __init__(self, service, spreadsheet_id):
        self.__service = service
        self.__spreadsheet_id = spreadsheet_id
        
        
    def __batchUpdate(self, requests):
        body = {
            'requests' : requests
        }
        
        result = self.__service.spreadsheets().batchUpdate(
            spreadsheetId = self.__spreadsheet_id,
            body = body
        ).execute()
        
        return result
    

    def __get(self):
        
        result = self.__service.spreadsheets().get(spreadsheetId = self.__spreadsheet_id).execute()
        
        return result
    
    
    def __values(self):
        
        result = self.__service.spreadsheets().values()
        
        return result    
    
    
    def list_sheets(self):
        result = self.__get()
        
        return result['sheets']
    
    
    def get_sheetName_by_id(self, sheet_id):
        sheets = self.list_sheets()
        
        for sheet in sheets:
            
            if sheet_id == sheet['properties']['sheetId']:
                
                return sheet['properties']['title']
            
        return None
    
    
    def get_sheetId_by_name(self, sheetname):
        sheets = self.list_sheets()
        
        for sheet in sheets: 
            
            if sheetname == sheet['properties']['title']:
                
                return sheet['properties']['sheetId']
        
        return None
    
    
    def rename_sheet(self, sheet_id, new_name):
        requests =  {
            "updateSheetProperties": {
                "properties": {
                    "sheetId" : sheet_id,
                    "title": new_name
                },
                "fields": "title",
            }
        }
        
        return self.__batchUpdate(requests)
    
        
    def create_sheet(self, sheet_name, sheet_id = None):
        requests =  [
            {
                "addSheet": {
                    "properties": {
                        "title": f"{sheet_name}"
                    }
                }
            }
        ]
        
        if sheet_id is not None and isinstance(sheet_id, int):
            requests[0]["addSheet"]["properties"]["sheetId"] = sheet_id

        return self.__batchUpdate(requests)
    
            
    def dataFrame_to_sheet(self, dataframe, A1range, valueInputOption = "RAW"):
        
        headers = dataframe.columns.to_list()
        data_container = [values.to_list() for index, values in dataframe.fillna("").iterrows()]
        data_container.insert(0, headers)
        
        result = self.__values().update(
            spreadsheetId = self.__spreadsheet_id,
            valueInputOption = valueInputOption,
            range = A1range,
            body = {
                "values" : data_container
            }).execute()
        
        return result
    
    
    def sheet_to_dataFrame(self, A1range):
        
        sheetname = A1range.split("!")[0]
        
        result = self.__values().get(
            spreadsheetId = self.__spreadsheet_id, 
            range = A1range
        ).execute()
        
        if 'values' in result:
            values = result['values']
            columns = values[0]
            rows = list(filter(lambda x: values.index(x) != 0 , values))
            
            if len(rows) == 0:
                raise SheetWithoutRowsException(sheetname)
            
            GSheetUtils.normalize_columns_and_rows(columns, rows)
            GSheetUtils.ajust_lastRow(columns, rows)
            
            df = DataFrame(rows, columns = columns).fillna("")
            return df
        
        else:
            raise EmptySheetException(sheetname)
            

    def clear_sheet(self, A1range):
        
        result = self.__values().clear(
            spreadsheetId = self.__spreadsheet_id,
            range = A1range
        ).execute()
        
        return result      
    
    
class EmptySheetException(Exception):
    
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "The range defined for the '{0}' sheet does not return data".format(self.message)
        else:
            return "The range defined for the sheet does not return data"
        
        
class SheetWithoutRowsException(Exception):
    
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "The range defined for the '{0}' sheet only returns columns headers, but no rows".format(self.message)
        else:
            return "The range defined for the sheet sheet only returns columns headers, but no rows"
        
class GSheetUtils:
    
    @staticmethod
    def normalize_columns_and_rows (columns, rows):
        col_len = len(columns)
        rows_len_list = []

        for r in rows:
            r_len = len(r)

            if r_len > col_len:
                rows_len_list.append(r_len)

        major_row = max(rows_len_list) if len(rows_len_list) != 0 else None

        if major_row is None:

            return True

        else:

            to_complete = major_row - col_len

            for x in range(0, to_complete):

                columns.append("Added Column")

            return True
    
    @staticmethod
    def ajust_lastRow(columns, rows):
        col_len = len(columns)
        last_row = rows[-1]
        last_len = len(last_row)

        if (col_len > last_len):

            diff = col_len - last_len

            for x in range(0, diff):
                last_row.append("")

            return True

        else:
            
            return True    
        
    @staticmethod
    def set_dataFrame_dtype(dataframe, dtype_config = None):
        
        dataframe = dataframe.fillna("").astype(str)
        
        if dtype_config is None:
            return dataframe
        
        else:
            
            def change_dtype(column_type, column, dataframe):
                
                def iter_func(dconstruct):
                    return lambda x: dconstruct(x) if x != '' else x
                
                dc = dataframe[column]
                
                if column_type == 'int':
                    to_int = lambda i : int(float(i))
                    return dc.map(iter_func(to_int))
                                    
                elif column_type == 'float':
                    to_float = lambda i : str(float(i)).replace(".",",")
                    return dc.map(iter_func(to_float))
                
                else:
                    return result.iter_func(str)
            
            for column in dtype_config:
                
                column_type = dtype_config[column]
                dataframe[column] = change_dtype(column_type, column, dataframe)
                
            return dataframe