# -*- coding: utf-8 -*-
# Copyright (c) 2018, itsdave GmbH and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
from frappe.model.document import Document
from frappe.utils import csvutils
from frappe.utils import get_files_path
from ftplib import FTP
import frappe
import glob
import os
import csv


class COPConnectimports(Document):

    @frappe.whitelist()
    def import_from_cop_server(self):
        COPConnect_settings = frappe.get_doc("COPConnect Settings")
        ftp = FTP(COPConnect_settings.ftp_host)
        ftp.login(COPConnect_settings.ftp_user, COPConnect_settings.ftp_pass)
        filenames = ftp.nlst()
        to_do_files = []
        for filename in filenames:
            if filename.startswith("note_") & filename.endswith(".csv"):
                to_do_files.append(filename)
            if filename.startswith("order_") & filename.endswith(".csv"):
                to_do_files.append(filename)
        path = (frappe.utils.get_files_path(is_private=1)) + "/copconnect"
        if not os.path.exists(path):
            os.makedirs(path)
        #gefundene Dateien Verarbeiten
        run_count = 0
        for to_do_file in to_do_files:
            run_count += 1
            percent = run_count * 100 / len(to_do_files)
            frappe.publish_progress(percent, "verarbeite Dateien")

            local_filename = path + "/" + to_do_file
            if os.path.isfile(local_filename):
                os.remove(local_filename)
            file_writer = open(local_filename, 'wb')
            ftp.retrbinary('RETR '+ to_do_file, file_writer.write  )
            file_writer.close()
            ftp.delete(to_do_file)
            self.process_file(to_do_file, path, COPConnect_settings)
            os.remove(local_filename)
        ftp.quit()


    def process_file(self, current_file, path, COPConnect_settings, create_po=False):
        csv_headers = {}
        csv_headers['COP_note'] = ['map_id', 'man_name', 'man_aid', 'product_quality', 'desc_short', 'price_min', 'price_special', 'qty_status_max', 'item_remarks', 'user_name', 'sup_name', 'sup_id', 'sup_aid', 'price_amount', 'qty_status', 'item_qty', 'vk_netto']
        csv_headers['COP_order'] = ['map_id', 'sup_name', 'sup_id', 'sup_aid', 'man_name', 'man_aid', 'product_quality', 'desc_short', 'ean', 'price_requested', 'price_confirmed', 'qty_requested', 'qty_confirmed', 'qty_delivered', 'item_remark', 'user_name', 'reference', 'customer_po', 'order_name', 'order_date', 'response_date', 'order_status', 'project_id', 'price_invoiced', 'qty_invoiced']
        csv_file = os.path.join(path, current_file)
        #print("processing " + csv_file)
        #COP Merkzettel
        if current_file.startswith("note_") & current_file.endswith(".csv"):
            self.process_csv(csv_file, "COP_note", COPConnect_settings, csv_headers)
        #COP Bestellungen
        if current_file.startswith("order_") & current_file.endswith(".csv"):
            self.process_csv(csv_file, "COP_order", COPConnect_settings, csv_headers, create_po)



    @frappe.whitelist()
    def import_from_csv_folder(self):
        COPConnect_settings = frappe.get_doc("COPConnect Settings")
        #CSV Headers, which we expect
        csv_headers = {}
        csv_headers['COP_note'] = ['map_id', 'man_name', 'man_aid', 'product_quality', 'desc_short', 'price_min', 'price_special', 'qty_status_max', 'item_remarks', 'user_name', 'sup_name', 'sup_id', 'sup_aid', 'price_amount', 'qty_status', 'item_qty', 'vk_netto']
        csv_headers['COP_order'] = ['map_id', 'sup_name', 'sup_id', 'sup_aid', 'man_name', 'man_aid', 'product_quality', 'desc_short', 'ean', 'price_requested', 'price_confirmed', 'qty_requested', 'qty_confirmed', 'qty_delivered', 'item_remark', 'user_name', 'reference', 'customer_po', 'order_name', 'order_date', 'response_date', 'order_status', 'project_id', 'price_invoiced', 'qty_invoiced']


        if COPConnect_settings.csv_import_folder != "":
            files = []
            run_count = 0
            files = os.listdir(COPConnect_settings.csv_import_folder)
            for file in files:
                run_count += 1
                percent = run_count * 100 / len(files)
                frappe.publish_progress(percent, "verarbeite Dateien")
                current_file = os.path.join(COPConnect_settings.csv_import_folder, file)
                #COP Merkzettel
                if file.startswith("note_") & file.endswith(".csv"):
                    files.append(current_file)
                    self.process_csv(current_file, "COP_note", COPConnect_settings, csv_headers)
                #COP Bestellungen
                if file.startswith("order_") & file.endswith(".csv"):
                    files.append(current_file)
                    self.process_csv(current_file, "COP_order", COPConnect_settings, csv_headers)
            if len(files) == 0:
                frappe.throw("No files found in directory " + COPConnect_settings.csv_import_folder)

        else:
            frappe.throw("CSV directory error")

    def process_csv(self, csv_filename, file_type, COPConnect_settings, csv_headers, create_po=False):
            if os.path.isfile(csv_filename):
                print("processing " + csv_filename + " as " + file_type)
                csv_rows = []

                with open(csv_filename, 'r', encoding='ISO-8859-1') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter=str(u';'), quotechar=str(u'"'))
                    for row in spamreader:
                        csv_rows.append(row)

                if self.check_csv_format(csv_rows, file_type, csv_headers):
                    print("CSV check OK")
                    csv_rows.pop(0)
                    item_rows = []
                    for row in csv_rows:
                        item_data = self.assign_item_data(row, csv_headers[file_type])
                        item_rows.append(item_data)
                        #print item_data
                        self.create_item(item_data, COPConnect_settings, file_type)
                    if create_po:
                        self.set_purchase_order(item_rows, COPConnect_settings)


                else:
                    frappe.throw("Fehler in CSV Datei " + csv_filename)
            else:
                frappe.throw("Datei nicht gefunden: " + csv_filename)

    def assign_item_data(self, row, csv_header):
        keys = []
        for key in csv_header:
            keys.append(key)
        values = []
        for value in row:
            values.append(value)
        item_data = dict(zip(keys, values))
        return item_data

    def calculate_standard_rate(self, item):
        if "price_amount" in item:
            if item["price_amount"] != "":
                if float(item["price_amount"].replace(",",".")) > 0:
                    return float(item["price_amount"].replace(",",".")) * 1.15

        elif "price_requested" in item:
            if item["price_requested"] != "":
                if float(item["price_requested"].replace(",",".")) > 0:
                    return float(item["price_requested"].replace(",",".")) * 1.15
        else:
            return False



    def create_item(self, item_data, COPConnect_settings, file_type):

        item_code = "MAPID-" + item_data["map_id"]
        found_items = frappe.get_all("Item", filters={"item_code": item_code }, fields=["name", "item_code"] )
        COPConnect_Supplier_name = frappe.get_doc("COP Lieferant", item_data["sup_name"]).supplier
        if len(found_items) >= 1:
            print("Item " + item_code + " allready exists.")
            item_doc = frappe.get_doc("Item", item_code)
            something_changed = False
            standard_rate = self.calculate_standard_rate(item_data)
            if standard_rate != False:
                if item_doc.standard_rate != standard_rate:
                    item_doc.standard_rate = standard_rate
                    something_changed = True
            if item_doc.default_supplier != COPConnect_Supplier_name:
                item_doc.default_supplier = COPConnect_Supplier_name
                something_changed = True
            if something_changed:
                item_doc.save()
        else:
            item_doc = frappe.get_doc({"doctype": "Item",
                                        "item_code": item_code,
                                        "item_group": COPConnect_settings.destination_item_group,
                                        "item_name": item_data["desc_short"][:140],
                                        "default_supplier": COPConnect_Supplier_name,
                                        "is_stock_item": 1
            })
            standard_rate = self.calculate_standard_rate(item_data)
            if standard_rate != False:
                item_doc.standard_rate =  standard_rate
            item_doc.insert()
        if file_type == "COP_order":
            self.set_supplier_item_code(item_data, item_code)

    def set_supplier_item_code(self, item_data, item_code):
        COPConnect_Supplier_name = frappe.get_doc("COP Lieferant", item_data["sup_name"]).supplier
        item_doc = frappe.get_doc("Item", item_code)

        for item_sup in item_doc.supplier_items:
            #print(item_sup.supplier + " | " + COPConnect_Supplier_name + " | " + item_sup.supplier_part_no  + " | " + item_data["sup_aid"])
            if item_sup.supplier == COPConnect_Supplier_name and item_sup.supplier_part_no == item_data["sup_aid"]:
                    return True

        supplier_item_doc = frappe.get_doc({"doctype": "Item Supplier",
                                        "parentfield": "supplier_items",
                                        "supplier": COPConnect_Supplier_name,
                                        "supplier_part_no": item_data["sup_aid"]})
        item_doc.supplier_items.append(supplier_item_doc)
        item_doc.save()

    def check_csv_format(self, csv_rows, file_type, csv_headers):
        #COP Merkzelltel Format prüfen
        if file_type == "COP_note":
            if csv_rows[0] == csv_headers["COP_note"]:
                if len(csv_rows) > 1:
                    return True
            else:
                print(csv_rows[0])
                print(csv_headers["COP_note"])
        #COP Bestellung Format prüfen
        if file_type == "COP_order":
            if csv_rows[0] == csv_headers["COP_order"]:
                if len(csv_rows) > 1:
                    return True
            else:
                print(csv_rows[0])
                print(csv_headers["COP_order"])
        return False

    @frappe.whitelist()
    def import_orders_from_cop_server(self):
        COPConnect_settings = frappe.get_doc("COPConnect Settings")
        ftp = FTP(COPConnect_settings.ftp_host)
        ftp.login(COPConnect_settings.ftp_user, COPConnect_settings.ftp_pass)
        filenames = ftp.nlst()
        to_do_files = []
        for filename in filenames:
            if filename.startswith("order_") & filename.endswith(".csv"):
                to_do_files.append(filename)
        path = (frappe.utils.get_files_path(is_private=1)) + "/copconnect"
        if not os.path.exists(path):
            os.makedirs(path)
        #gefundene Dateien Verarbeiten
        run_count = 0
        for to_do_file in to_do_files:
            run_count += 1
            percent = run_count * 100 / len(to_do_files)
            frappe.publish_progress(percent, "verarbeite Dateien")
            local_filename = path + "/" + to_do_file
            if os.path.isfile(local_filename):
                os.remove(local_filename)
            file_writer = open(local_filename, 'wb')
            ftp.retrbinary('RETR '+ to_do_file, file_writer.write  )
            file_writer.close()
            ftp.delete(to_do_file)
            self.process_file(to_do_file, path, COPConnect_settings, True)
            os.remove(local_filename)
        ftp.quit()
        pass

    def set_purchase_order(self, item_rows, COPConnect_settings):
        po_title = item_rows[0]["sup_name"] + "-"  + item_rows[0]["customer_po"]
        found_pos = frappe.get_all("Purchase Order", filters={"title": po_title})
        if len(found_pos) > 0:
            frappe.msgprint("Purchase Order " + po_title + " bereits vorhanden.")
            return False

        po_doc = frappe.get_doc({"doctype": "Purchase Order",
                                "title": po_title,
                                "supplier": frappe.get_doc("COP Lieferant", item_rows[0]["sup_name"]).supplier,
                                "set_warehouse": frappe.get_doc("Stock Settings").default_warehouse,
                                "company": frappe.get_doc("Global Defaults").default_company,
                                "taxes_and_charges": COPConnect_settings.purchase_taxes_and_charges_template_for_imported_cop_orders,
                                "payment_terms_template": COPConnect_settings.payment_terms_template_for_imported_cop_orders
                                })
        for row in item_rows:
            po_item_doc = frappe.get_doc({"doctype": "Purchase Order Item",
                                        "item_code": "MAPID-" + row["map_id"],
                                        "qty": float(row["qty_confirmed"].replace(",",".")),
                                        "schedule_date": frappe.utils.data.today(),
                                        "rate": float(row["price_confirmed"].replace(",","."))
                                        })
            po_doc.append("items", po_item_doc)
        po_doc.insert()
