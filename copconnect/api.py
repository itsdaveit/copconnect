from zeep import Client, Settings
from zeep.plugins import HistoryPlugin
import xml.etree.ElementTree as ET
from datetime import datetime

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

    def getArticlesSupplier(self, map_id, supp_ids=None):
        query = map_id
        self.request_data["map_id"] = query
        self.request_data["check_realtime"] = False
        self.request_data["check_projects"] = False
        self.request_data["additional_quality"] = True
        self.request_data["sup_id"] = {"item": supp_ids}      
        print(self.request_data)  
        response = (self.api.service.getArticlesSupplier(self.request_data))
        return response

        
    def getGroups(self):
        response = (self.api.service.getGroups(self.request_data))
        return response


    def getOrders(
            self,
            action,
            start_date=None,
            end_date=None,
            order_id=None,
            sup_id=None,
            status=None,
            customer_po="",
            enduser_po="",
            check_responses=False
        ):
        # Required parameters
        self.request_data["action"] = action
        self.request_data["customer_po"] = customer_po
        self.request_data["enduser_po"] = enduser_po
        self.request_data["check_responses"] = check_responses

        # add optional parameters only if they are defined
        if start_date:
            self.request_data["start_date"] = start_date
        else:
            self.request_data["start_date"] = "2018-01-01 00:00:00"
        if end_date:
            self.request_data["end_date"] = end_date
        else:
            self.request_data["end_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if order_id:
            self.request_data["order_id"] = {"item": order_id}
        else:
            self.request_data["order_id"] = {"item": list(range(1, 1000))}
        if sup_id:
            self.request_data["sup_id"] = {"item": sup_id}
        else:
            self.request_data["sup_id"] = {"item": list(range(1, 1000))}
        if status:
            self.request_data["status"] = {"item": status}
        else:
            self.request_data["status"] = {"item": list(range(1, 1000))}
        if enduser_po:
            self.request_data["enduser_po"] = enduser_po

        response = self.api.service.getOrders(self.request_data)
        return response
