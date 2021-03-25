from zeep import Client, Settings
from zeep.plugins import HistoryPlugin
import xml.etree.ElementTree as ET
import pprint


class CopAPI():
    def __init__(self, url, username, password):

        self.request_data = {
            "username": username,
            "password": password,
            "active": True
            }
        settings = Settings(strict=False)
        self.api = Client(url, settings=settings)
        self.request_data["sid"] = (
            self.api.service.getSessionID(self.request_data)
            )

    def getArticlesContent(self, map_id):
        self.request_data["map_id"] = map_id
        response = (self.api.service.getArticlesContent(self.request_data))
        return response
    
    def getArticles(self, query):
        self.request_data["query"] = query
        self.request_data["sort_field"] = "map_id"
        self.request_data["sort_dir"] = "desc"
        self.request_data["multi_retail_prices"] = True
        self.request_data["additional_quality"] = True
        self.request_data["limit"] = 10
        self.request_data["page"] = 1
        
        response = (self.api.service.getArticles(self.request_data))
        return response

    def getArticlesSupplier(self, map_id):
        query = map_id
        self.request_data["map_id"] = query
        self.request_data["check_realtime"] = False
        self.request_data["check_projects"] = False
        self.request_data["additional_quality"] = True
        self.request_data["sup_id"] = {"item": [3, 2, 1]}

        
        

        
        
        
        response = (self.api.service.getArticlesSupplier(self.request_data))
        return response

