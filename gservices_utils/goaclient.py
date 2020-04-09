from __future__ import print_function
import pickle
from os import path, mkdir
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

class GoogleClient:
    
    __SCOPES = None
    __creds_filename = None
    
    def __init__(self, creds_filename, scopes):
        self.__creds_filename = creds_filename
        self.__SCOPES = scopes
    
    
    def get_credentials(self):
        creds_filename = self.__creds_filename
        creds = None
            
        # If Pickle file exists
        if path.exists(f'{creds_filename}.pickle'):
            
            with open(f'{creds_filename}.pickle', 'rb') as token:
                creds = pickle.load(token)
                
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    f'{creds_filename}.json', self.__SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            
            with open(f'{creds_filename}.pickle', 'wb') as token:
                pickle.dump(creds, token)
                
        return creds
    
    
    def get_service(self, service_name = None, version = None):
        creds = self.get_credentials()
        services = {"sheets" : "v4", "gmail" : "v1", "docs" : "v1", "drive" : "v3", 
                    "slides": "v1", "calendar" : "v3"}
        
        # Check avaiable services
        if service_name not in services:
            raise NameError("Google service undefined.")
        
        # Default Version
        if version is None:
            version = services[service_name]

        service = build(service_name, version, credentials=creds)
        
        return service