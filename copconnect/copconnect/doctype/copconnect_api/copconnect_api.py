# -*- coding: utf-8 -*-
# Copyright (c) 2018, itsdave GmbH and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from zeep import Client
from zeep.plugins import HistoryPlugin
import xml.etree.ElementTree as ET
import pprint


class COPConnectAPI(Document):

    def process_COP_getSuppliers_Response(self, suppliers_response):
        suppliers_list = []
        for supplier in suppliers_response:
            supplier_dict = {}
            supplier_dict["sup_id"] = supplier["sup_id"]
            supplier_dict["sup_name"] = supplier["sup_name"]
            supplier_dict["sup_company"] = supplier["sup_company"]
            supplier_dict["customer_id"] = supplier["customer_id"]
            supplier_dict["amount"] = float(supplier["shipment"]["amount"])
            supplier_dict["free_from"] = float(supplier["shipment"]["free_from"])
            supplier_dict["min_amount"] = float(supplier["shipment"]["min_amount"])
            supplier_dict["min_to"] = float(supplier["shipment"]["min_to"])
            supplier_dict["min_order_value"] = float(supplier["shipment"]["min_order_value"])
            supplier_dict["extra_amount1"] = float(supplier["shipment"]["extra_amount1"])
            supplier_dict["extra_type1"] = supplier["shipment"]["extra_type1"]
            supplier_dict["extra_amount2"] = float(supplier["shipment"]["extra_amount2"])
            supplier_dict["extra_type2"] = supplier["shipment"]["extra_type2"]

            suppliers_list.append(supplier_dict)
        return suppliers_list

    def set_COPSuppliers(self, suppliers_dict):
        #Fügt "COP Lieferant" ein und aktualisiert, wenn Änderungen gefunden werden.
        #Die Lieferanten ID aus COP wird als Index verwendet
        count_COP_Lieferanten = 0
        for supplier in suppliers_dict:
            found_suppliers = frappe.get_all("COP Lieferant", filters={"sup_id": supplier["sup_id"] })
            if len(found_suppliers) > 1:
                frappe.throw("Doppelte sup_id " + supplier["sup_id"])

            elif len(found_suppliers) == 1:

                #print "COP Lieferant mit sup_id " + str(supplier["sup_id"]) + " existiert bereits. Vergleiche."
                #Update der Daten
                COP_Lieferant = frappe.get_doc("COP Lieferant", found_suppliers[0].name )
                change_detected = False

                if COP_Lieferant.sup_name != supplier["sup_name"]:
                    COP_Lieferant.sup_name = supplier["sup_name"]
                    change_detected = True

                if COP_Lieferant.sup_company != supplier["sup_company"]:
                    COP_Lieferant.sup_company = supplier["sup_company"]
                    change_detected = True

                if COP_Lieferant.customer_id != supplier["customer_id"]:
                    COP_Lieferant.customer_id = supplier["customer_id"]
                    change_detected = True

                if COP_Lieferant.amount != supplier["amount"]:
                    COP_Lieferant.amount = supplier["amount"]
                    change_detected = True

                if COP_Lieferant.free_from != supplier["free_from"]:
                    COP_Lieferant.free_from = supplier["free_from"]
                    change_detected = True

                if COP_Lieferant.min_amount != supplier["min_amount"]:
                    COP_Lieferant.min_amount = supplier["min_amount"]
                    change_detected = True

                if COP_Lieferant.min_to != supplier["min_to"]:
                    COP_Lieferant.min_to = supplier["min_to"]
                    change_detected = True

                if COP_Lieferant.min_order_value != supplier["min_order_value"]:
                    COP_Lieferant.min_order_value = supplier["min_order_value"]
                    change_detected = True

                if COP_Lieferant.extra_amount1 != supplier["extra_amount1"]:
                    COP_Lieferant.extra_amount1 = supplier["extra_amount1"]
                    change_detected = True

                if COP_Lieferant.extra_type1 != supplier["extra_type1"]:
                    COP_Lieferant.extra_type1 = supplier["extra_type1"]
                    change_detected = True

                if COP_Lieferant.extra_amount2 != supplier["extra_amount2"]:
                    COP_Lieferant.extra_amount2 = supplier["extra_amount2"]
                    change_detected = True

                if COP_Lieferant.extra_type2 != supplier["extra_type2"]:
                    COP_Lieferant.extra_type2 = supplier["extra_type2"]
                    change_detected = True

                count_COP_Lieferanten += 1

                if change_detected:
                    print("Aenderung gefunden.")
                    COP_Lieferant.save()


            else:
                #Neuanlage des Lieferanten
                item_doc = frappe.get_doc({"doctype": "COP Lieferant",
                "sup_name": supplier["sup_name"],
                "sup_company": supplier["sup_company"],
                "sup_id": supplier["sup_id"],
                "customer_id": supplier["customer_id"],
                "amount": supplier["amount"],
                "free_from": supplier["free_from"],
                "min_amount": supplier["min_amount"],
                "min_to": supplier["min_to"],
                "min_order_value": supplier["min_order_value"],
                "extra_amount1": supplier["extra_amount1"],
                "extra_type1": supplier["extra_type1"],
                "extra_amount2": supplier["extra_amount2"],
                "extra_type2": supplier["extra_type2"],
                })
                print(item_doc.insert().sup_name + " Angelegt")
                count_COP_Lieferanten += 1

        return count_COP_Lieferanten


    def set_ERPNextSuppliers(self):
        COPConnect_settings = frappe.get_doc("COPConnect Settings")
        count_ERPNextSuppliers = 0

        found_COP_Lieferanten = frappe.get_all("COP Lieferant")
        if len(found_COP_Lieferanten) >= 1:
            found_ERPNext_Suppliers = frappe.get_all("Supplier", filters={"supplier_group": COPConnect_settings.destination_supplier_group })
            count_ERPNextSuppliers = len(found_ERPNext_Suppliers)
            for COP_Lieferant in found_COP_Lieferanten:
                COP_Lieferant_doc = frappe.get_doc("COP Lieferant", COP_Lieferant.name)

                if COP_Lieferant_doc.supplier == None:
                    #check if a there is allready an erpnext-supplier with same name
                    previous_existing_erpnext_supplier = frappe.get_all("Supplier", filters={"supplier_name": COP_Lieferant_doc.sup_company})
                    if len(previous_existing_erpnext_supplier) > 1:
                        frappe.throw("Name für Lieferant " + COP_Lieferant_doc.sup_company + " mehrfach vorhanden. Bitte manuell zuweisen.")
                    if len(previous_existing_erpnext_supplier) == 1:
                        ERPNext_Supplier_doc = frappe.get_doc("Supplier", previous_existing_erpnext_supplier[0])
                        COP_Lieferant_doc.supplier = ERPNext_Supplier_doc.name
                        COP_Lieferant_doc.save()
                    else:
                        supplier_doc = frappe.get_doc({"doctype": "Supplier",
                        "supplier_name": COP_Lieferant_doc.sup_company,
                        "supplier_type": "Company",
                        "supplier_group": COPConnect_settings.destination_supplier_group,
                        "represents_company": None})

        
                        inerted_ERPNext_Supplier = supplier_doc.insert()
                        COP_Lieferant_doc.supplier = inerted_ERPNext_Supplier.name
                        COP_Lieferant_doc.save()


                else:
                    change_detected = False
                    ERPNext_Supplier_doc = frappe.get_doc("Supplier", COP_Lieferant_doc.supplier)

                    if ERPNext_Supplier_doc.supplier_name != COP_Lieferant_doc.sup_company:
                        ERPNext_Supplier_doc.supplier_name = COP_Lieferant_doc.sup_company
                        change_detected = True

                    if change_detected:
                        print("Aenderung gefunden.")
                        ERPNext_Supplier_doc.save()

            return count_ERPNextSuppliers


    def cop_getSuppliers(self):
        COPConnect_settings = frappe.get_doc("COPConnect Settings")
        #COPClient = Client(COPConnect_settings.cop_wsdl_url, strict=False)
        COPClient = Client(COPConnect_settings.cop_wsdl_url)
        request_data = {
        "username": COPConnect_settings.cop_user,
        "password": COPConnect_settings.cop_password,
        "active": True}

        request_data["sid"] = (COPClient.service.getSessionID(request_data))
        suppliers_response = (COPClient.service.getSuppliers(request_data))["item"]
        suppliers_list = self.process_COP_getSuppliers_Response(suppliers_response)
        if len(suppliers_list) >= 1:
            count_COP_Lieferanten = self.set_COPSuppliers(suppliers_list)
        else:
            frappe.throw("COP Antwort enthielt keine Lieferanten.")

        if count_COP_Lieferanten >= 1:
            count_ERPNext_Lieferanten = self.set_ERPNextSuppliers()
        else:
            frappe.throw("Keine COP Lieferanten vorhanden.")

        frappe.msgprint("Vorgang Erfolgreich.")
        frappe.msgprint(str(count_COP_Lieferanten) + " COP Lieferanten vorhanden.")
        frappe.msgprint(str(count_ERPNext_Lieferanten) + " ERPNext Lieferanten vorhanden.")
