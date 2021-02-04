from zeep import Client
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
        self.api = Client(url)
        self.request_data["sid"] = (
            self.api.service.getSessionID(self.request_data)
            )

    def getArticlesContent(self, map_id):
        self.request_data["map_id"] = map_id
        response = (self.api.service.getArticlesContent(self.request_data))
        return response
