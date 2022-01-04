from datetime import datetime, timedelta
from pprint import pprint
import re
from warnings import filters
from attr import fields
import frappe
from copconnect.api import CopAPI
from frappe.sessions import get
import requests
from frappe.core.doctype.file.file import create_new_folder
from frappe.utils.file_manager import save_file, get_content_hash
from frappe import enqueue, get_value
from six import BytesIO
from pprint import pprint
import time
from frappe import _
from frappe.utils import get_site_name, get_site_base_path, get_site_url


@frappe.whitelist()
def importitem(map_id):

    if str(map_id).startswith("MAPID-"):
        map_id = str(map_id).split("-")[1]    

    settings = frappe.get_doc("COPConnect Settings")
    if settings.use_base_url == 1:
        url = settings.base_url
    else:
        base_path= get_site_base_path()
        site = str(base_path).split("/")[1]
        url = get_site_url(site)

    start_dt = datetime.now()

    text = "Artikel mit Map ID " + map_id + " importiert."
    result = get_item(map_id, start_dt)
    if result:
        return_massage = result["message"]  
    else:
        return_massage = "Artikel <a href=\"" + str(url) + "/app/item/MAPID-" + str(map_id) + "\" target=\"_blank\">MAPID-" + str(map_id) + "</a> angelegt."
    end_dt = datetime.now()
    time = end_dt - start_dt

    return return_massage + " (" + str(round(time.total_seconds(),3)) + " s)"

@frappe.whitelist()
def importnote(note_id, customer_id=None):
    print(note_id)
    if customer_id:

        return "vMerkliste mit Note ID " + str(note_id) + " importiert. Angebot für Kunde " + str(customer_id) + " erstellt." 
    else:
        return "vMerkliste mit Note ID " + str(note_id) + " importiert." 



"""
Erstellen oder aktualisieren von Artikel wird über get_item durchgeführt.
"""

def get_item(map_id, start_dt):
    settings = frappe.get_doc("COPConnect Settings")
    frappe.db.set_default("item_naming_by","Item Code")
    api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password)
    
    r = api.getArticles("mapid:" + str(map_id))
    if r["rows"]["item"][0]:
        cop_item_row = r["rows"]["item"][0]

    if cop_item_row.man_name:
        create_brand_if_not_exists(cop_item_row.man_name, cop_item_row.man_id)
    print("create_brand_if_not_exists ", (datetime.now() - start_dt).total_seconds())
    
    change_detected = False
    print(cop_item_row)

    filters = {"name": "MAPID-" + str(map_id)}
    items = frappe.get_all("Item", filters=filters)
    print("frappe.get_all Item ", (datetime.now() - start_dt).total_seconds())
    if items:
        item_doc = frappe.get_doc("Item", items[0]["name"])
        item_code = item_doc.item_code
        response_item_doc, response_change  = _update_item(cop_item_row, item_doc)
        print("update Item ", (datetime.now() - start_dt).total_seconds())
        if response_change:
            change_detected = True
            item_doc = response_item_doc
    else:
        #Artikel neuanlage
        change_detected = True
        item_doc = _create_item(cop_item_row)
        print("create Item ", (datetime.now() - start_dt).total_seconds())
        item_code = item_doc.item_code

    response_item_doc, response_change = set_suppliers_and_prices(item_doc=item_doc, settings=settings, api=api)
    print("set_suppliers_and_prices ", (datetime.now() - start_dt).total_seconds())
    if response_change:
        change_detected = True
        item_doc = response_item_doc

    response_item_doc, response_change = update_selling_price(item_code, item_doc=item_doc, settings=settings, api=api, start_dt=start_dt)
    print("update_selling_price ", (datetime.now() - start_dt).total_seconds())
    if response_change:
        change_detected = True
        item_doc = response_item_doc

    if change_detected:
        try:
            item_doc.save()
            frappe.db.commit()
            frappe.db.set_default("item_naming_by","Naming Series")
        except Exception as e:
            frappe.db.set_default("item_naming_by","Naming Series")
            rdict = {
                "state": "500",
                "message": str(e)
            }
            return rdict
        
    
    if settings.get_datasheet == 1:
        if settings.use_enqueue == 1:
            enqueue("copconnect.remoteapi.get_item_datasheet", item_code=item_code) #geht leider nicht 
        else:
            get_item_datasheet(item_code)
        print("copconnect.remoteapi.get_item_datasheet ", (datetime.now() - start_dt).total_seconds())

    if settings.get_productimage == 1:
        if settings.use_enqueue == 1:
            enqueue("copconnect.remoteapi.get_item_images", item_code=item_code)
        else:
            get_item_images(item_code)
        print("copconnect.remoteapi.get_item_images ", (datetime.now() - start_dt).total_seconds())
    


def set_suppliers_and_prices(item_doc, settings=None, api=None):
    change_detected = False
    i = item_doc.item_code
    print(i)
    print("###########")
    if not i.startswith("MAPID-"):
        frappe.throw("Artielnummer muss mit MAPID- anfangen.")
    map_id = i[6:]
    if not settings:
        settings = frappe.get_single("COPConnect Settings")
    if not api:
        api = CopAPI(
            settings.cop_wsdl_url, settings.cop_user, settings.cop_password
            )
    
    supps = frappe.get_all("COP Lieferant", filters=[["level", "<=", settings.min_level_for_supplier_part_no]], fields=["sup_id","supplier"])
    sup_id_list = []
    for el in supps:
        sup_id_list.append(el["sup_id"])
    r = api.getArticlesSupplier(map_id, sup_id_list)
    if r:
        if r.item:
            for el in r.item:
                #für jeden zurückgegebenen ArticlesSupplier bestimmen wir ERPNext Supplier
                for s in supps:
                    if s["sup_id"] == str(el.sup_id): # ERPNext Supplier gefunden
                        existing_supplier_item = False
                        for si in item_doc.supplier_items:
                            if si.supplier == s["supplier"]: # bestehende Lieferantenartikelnummer gefunden
                                existing_supplier_item = True
                                if si.supplier_part_no != str(el.sup_aid):
                                    si.supplier_part_no = str(el.sup_aid) #wenn vorhanden und abweichend, aktualisieren
                                    change_detected = True
                                continue
                        if not existing_supplier_item: #wenn nicht gefunden, neuanlage
                            item_supplier_doc = frappe.get_doc(
                                {
                                    "doctype": "Item Supplier",
                                    "supplier": s["supplier"],
                                    "supplier_part_no": str(el.sup_aid)}
                            )
                            item_doc.append("supplier_items", item_supplier_doc )
                            change_detected = True
                        if hasattr(el, "price_amount"):
                            change_result = _update_buying_price(s["supplier"], item_doc.item_code, el.price_amount, settings=settings, item_doc=item_doc)
                            if change_result:
                                change_detected = True
                        continue
    return item_doc, change_detected


def _update_buying_price(supplier,item_code,price_list_rate, settings=None, item_doc=None):
    #preise anlegen und ggf. aktualisieren:
    settings = frappe.get_single("COPConnect Settings") if not settings else settings
    item_doc = frappe.get_doc("Item", item_code) if not item_doc else item_doc
    print("getting item prices for ", supplier)
    curret_buying_prices = frappe.get_all(
        "Item Price", 
        filters={
            "buying": 1,
            "item_code": item_doc.item_code,
            "price_list": settings.price_list_buying,
            "supplier": supplier
        },
        fields=[
            "name",
            "supplier",
            "price_list_rate",
            "uom"
        ])
    if len(curret_buying_prices) > 1:
        frappe.throw(_("Found more then one buying price for item and supplier."))
    if len(curret_buying_prices) == 1:
        price = curret_buying_prices[0]
        if price_list_rate == -1:
            frappe.delete_doc("Item Price", curret_buying_prices[0]["name"] )
            return True
        if price["price_list_rate"] == price_list_rate:
            return False
        else:
            item_price_doc = frappe.get_doc("Item Price", price["name"])
            item_price_doc.price_list_rate = price_list_rate
            item_price_doc.save()
            return True
    if len(curret_buying_prices) == 0:
        if price_list_rate == -1:
            return False
        item_price_doc = frappe.get_doc(
            {
                "doctype": "Item Price",
                "buying": 1,
                "item_code": item_doc.item_code,
                "price_list": settings.price_list_buying,
                "supplier": supplier,
                "uom": item_doc.stock_uom,
                "price_list_rate": price_list_rate
            }
        )  
        item_price_doc.insert()   
        return True


@frappe.whitelist()
def update_selling_price(item_code, settings=None, item_doc=None, api=None, start_dt=None):
    settings = frappe.get_single("COPConnect Settings") if not settings else settings
    item_doc = frappe.get_doc("Item", item_code) if not item_doc else item_doc
    start_dt = datetime.now() if not start_dt else start_dt
    curret_selling_prices = frappe.get_all(
        "Item Price", 
        filters={
            "selling": 1,
            "item_code": item_code,
            "price_list": settings.price_list_selling,
        },
        fields=[
            "name",
            "price_list_rate",
            "uom"
        ])
    print("frappe get_all item price, selling ", (datetime.now() - start_dt).total_seconds())
    selling_price_dict = get_selling_price(item_code, settings=settings, item_doc=item_doc, api=api, start_dt=start_dt)
    print("get get_selling_price dict ", (datetime.now() - start_dt).total_seconds())
    new_price = selling_price_dict["price"]
    if len(curret_selling_prices) > 1:
        frappe.throw(_("Found more then one buying price for item and supplier."))
    if len(curret_selling_prices) == 1:
        price = curret_selling_prices[0]
        if price["price_list_rate"] == new_price:
            return item_doc, False
        else:
            print("changed price detected")
            item_price_doc = frappe.get_doc("Item Price", price["name"])
            item_price_doc.price_list_rate = new_price
            item_price_doc.save()
            return item_doc, True
    if len(curret_selling_prices) == 0:
        item_price_doc = frappe.get_doc(
            {
                "doctype": "Item Price",
                "selling": 1,
                "item_code": item_doc.item_code,
                "price_list": settings.price_list_selling,
                "uom": item_doc.stock_uom,
                "price_list_rate": new_price
            }
        )
        item_price_doc.insert()
        return item_doc, True

@frappe.whitelist()
def get_best_buying_price(item_code, settings=None, item_doc=None, qty=1, ignore_qty=False, with_realtime=False, api=None, start_dt=None):
    settings = frappe.get_single("COPConnect Settings") if not settings else settings
    item_doc = frappe.get_doc("Item", item_code) if not item_doc else item_doc
    start_dt = datetime.now() if not start_dt else start_dt
    api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password) if not api else api
    i = item_doc.item_code
    if not i.startswith("MAPID-"):
        frappe.throw("Artielnummer muss mit MAPID- anfangen.")
    map_id = i[6:]
    best_buying_price_dict = {
        "price": None,
        "supplier_id": None,
        "supplier": None,
        "supplier_name": None,
        "qty": None,
        "found": None
        }
    supps = frappe.get_all("COP Lieferant", filters=[["level", "<=", settings.min_level_for_selling_price]], fields=["sup_id","supplier"])
    print("frappe.get_all(COP Lieferant) ", (datetime.now() - start_dt).total_seconds())
    sup_id_list = []

    for supp in supps:
        print("for el in supps", (datetime.now() - start_dt).total_seconds())
        sup_id_list.append(supp["sup_id"])
    r = api.getArticlesSupplier(map_id, sup_id_list)
    if r:
        if r.item:
            for el in r.item:
                if hasattr(el, "price_amount"):
                    if el.price_amount < 0:
                        pass
                    else:
                        if not best_buying_price_dict["price"] or el.price_amount < best_buying_price_dict["price"]:
                            if ignore_qty or el.qty_real >= qty:
                                best_buying_price_dict["price"] = el.price_amount
                                best_buying_price_dict["supplier_id"] = el.sup_id
                                best_buying_price_dict["qty"] = el.qty_real
                                best_buying_price_dict["found"] = True
    
    if best_buying_price_dict["found"]:
        best_buying_price_dict["supplier"] = frappe.get_all("COP Lieferant", filters={"sup_id": best_buying_price_dict["supplier_id"]}, fields=["supplier"])[0]["supplier"]
    
    return best_buying_price_dict

 
@frappe.whitelist()
def get_selling_price(item_code, settings=None, item_doc=None, qty=1, with_realtime=False, api=None, start_dt=None):
    settings = frappe.get_single("COPConnect Settings") if not settings else settings
    item_doc = frappe.get_doc("Item", item_code) if not item_doc else item_doc
    start_dt = datetime.now() if not start_dt else start_dt
    api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password) if not api else api
    i = item_doc.item_code
    if not i.startswith("MAPID-"):
        frappe.throw("Artielnummer muss mit MAPID- anfangen.")
    map_id = i[6:]
    selling_price_dict = {
        "price": 0,
        "qty": None,
        "found": None
        }
    best_buying_price_dict = get_best_buying_price(item_code, settings=settings, item_doc=item_doc, qty=qty, with_realtime=with_realtime, api=api, start_dt=start_dt)
    print("get_best_buying_price ", (datetime.now() - start_dt).total_seconds())
    if not best_buying_price_dict["found"]:
        return selling_price_dict
    pricing_rule = get_best_pricing_rule(item_code, best_buying_price_dict["price"], settings=settings, item_doc=item_doc)
    if not pricing_rule:
        return selling_price_dict

    selling_price = round(apply_pricing_rule(pricing_rule, round(best_buying_price_dict["price"],2)),2)
    selling_price_dict["found"] = True
    selling_price_dict["price"] = selling_price
    selling_price_dict["qty"] = qty
    print(best_buying_price_dict)
    print(pricing_rule)
    print(selling_price_dict)
    return selling_price_dict


def get_best_pricing_rule(item_code, buying_price, settings=None, item_doc=None):
    settings = frappe.get_single("COPConnect Settings") if not settings else settings
    item_doc = frappe.get_doc("Item", item_code) if not item_doc else item_doc
    pricing_rule_list = frappe.get_all("COPConnect Pricing Rule", order_by="name asc", fields={"name", "from_price", "to_price", "calculation_factor", "extra_charge", "item_group"})
    for rule in pricing_rule_list:
        if buying_price >= rule["from_price"] and buying_price <= rule["to_price"]:
            if not rule["item_group"] or rule["item_group"] == item_doc.item_group:
                return rule
    return None


def apply_pricing_rule(rule, buying_price):
    if rule["calculation_factor"] and rule["calculation_factor"] >= 1:
        buying_price = buying_price * rule["calculation_factor"]
    if rule["extra_charge"] and rule["extra_charge"] > 0:
         buying_price = buying_price + rule["extra_charge"]
    return buying_price
        

def _create_item(cop_item_row):
    item_dict = _get_item_dict(cop_item_row)
    item_doc = frappe.get_doc(item_dict)

    for key in item_dict:
        if key in ["barcode", "barcode_mapid"]:
            item_doc = _set_barcode(item_doc, key, item_dict[key])
    itemdoc = _set_item_defaults(item_doc)
    result = item_doc.insert()
    return item_doc

def _update_item(cop_item_row, item_doc):
    item_dict = _get_item_dict(cop_item_row)
    change_detected = False
    for key in item_dict:
        #Sonderbehandlung von Child-Table Barcodes
        if key in ["barcode", "barcode_mapid"]:
            item_doc = _set_barcode(item_doc, key, item_dict[key])
            if hasattr(item_doc, "change_detected"):
                if item_doc.change_detected == True:
                    change_detected = True
            continue
        if key == "change_detected":
            continue

        #Behandlung von normalen Attributen
        if getattr(item_doc, key) != item_dict[key]:
            change_detected = True
            setattr(item_doc, key, item_dict[key])
    itemdoc = _set_item_defaults(item_doc)
    return item_doc, change_detected

def _set_barcode(item_doc, type, value):

    barcode_exists = False
    if hasattr(item_doc, "barcodes"):
        for el in item_doc.barcodes:
            if el.barcode == value:
                barcode_exists = True
        
    if not barcode_exists:
        if type == "barcode":
            barcode_doc = frappe.get_doc(
                {
                    "doctype": "Item Barcode",
                    "barcode_type": "EAN",
                    "barcode": value
                }
            )
            item_doc.append("barcodes", barcode_doc)
            item_doc.change_detected = True
        
        if type == "barcode_mapid":
            barcode_doc = frappe.get_doc(
                {
                    "doctype": "Item Barcode",
                    "barcode_type": "",
                    "barcode": value
                }
            )
            item_doc.append("barcodes", barcode_doc)
            item_doc.change_detected = True

    return item_doc

def _set_item_defaults(item_doc):
    item_doc = _set_default_warehouse(item_doc)
    
    return item_doc

def _get_default_warehouse(item_group, company):
    item_group_doc = frappe.get_doc("Item Group", item_group)
    if hasattr(item_group_doc, "item_group_defaults"):
        for el in item_group_doc.item_group_defaults:
            if el.company == company:
                if el.default_warehouse:
                    return el.default_warehouse
                else:
                    return False


def _set_default_warehouse(item_doc):
    company = frappe.get_value(doctype="Global Defaults", fieldname="default_company")
    dest_warehouse = _get_default_warehouse(item_doc.item_group, company)
    found_item_default_for_company = False
    if not dest_warehouse:
        print("no dest_warehouse found.")
        return item_doc
    if hasattr(item_doc, "item_defaults"):
        for el in item_doc.item_defaults:
            if el.company == company:
                found_item_default_for_company = True
                if el.default_warehouse != dest_warehouse:
                    el.default_warehouse = dest_warehouse
                    item_doc.change_detected = True
    
    if not found_item_default_for_company:
        item_defaults_doc = frappe.get_doc({
            "doctype": "Item Default",
            "company": company,
            "default_warehouse": dest_warehouse})
        item_doc.append("item_defaults", item_defaults_doc)
        item_doc.change_detected = True

    return item_doc




            

def _get_item_dict(cop_item_row, settings=None):

    if not settings:
        settings = frappe.get_doc("COPConnect Settings")
    item_fields_matching_table = {
        "doctype": "Item",
        "item_code": "MAPID-" + str(cop_item_row["map_id"]),
        "item_name": str(cop_item_row["desc_short"])[:140],
        "item_group": create_item_group_if_not_exists(cop_item_row),
        "description": str(cop_item_row["desc_long"]),
        "brand": str(cop_item_row.man_name),
        "hersteller_artikel_nummer": str(cop_item_row.man_aid),
        "barcode": str(cop_item_row.ean),
        "barcode_mapid": "MAPID-" + str(cop_item_row["map_id"]),
        "is_stock_item": 1
        }
    return item_fields_matching_table


def get_item_images(item_code, cop_item_row=None, settings=None, api=None):
    print("### get_images")
    i = item_code
    if not i.startswith("MAPID-"):
        frappe.throw("Artielnummer muss mit MAPID- anfangen.")
    map_id = i[6:]
    if not settings:
        settings = frappe.get_single("COPConnect Settings")
    if not api:
        api = CopAPI(
            settings.cop_wsdl_url, settings.cop_user, settings.cop_password
            )

    if not cop_item_row:
        r = api.getArticles("mapid:" + str(map_id))
        if r["rows"]["item"][0]:
            cop_item_row = r["rows"]["item"][0]


    if hasattr(cop_item_row, "url_pic"):
        if cop_item_row.url_pic:

            with requests.Session() as s:
                d = s.get(cop_item_row.url_pic)
                print(d)
                buffer = BytesIO(d.content)
                itemdoc = frappe.get_doc("Item", item_code)
                doctype_folder = create_folder("Item", "Home")
                title_folder = create_folder("title-images", doctype_folder)
                suffix = str(cop_item_row.url_pic).split(".")[-1]
                filename = "Abbildung_"
                filename += cop_item_row.man_name + "_" if cop_item_row.man_name else None
                filename += cop_item_row.man_aid + "_" if cop_item_row.man_aid else None
                filename += item_code + "." + suffix
                filename = filename.replace(" ", "_")
                filename = filename.replace("/", "_")
                filename = filename.replace("|", "_")
                filename = filename.replace("\\", "_")
                print(filename)
                files = frappe.get_all(
                    "File", 
                    filters = {
                        "attached_to_doctype": "Item",
                        "attached_to_name": item_code,
                        "attached_to_field": "image",
                        "file_name": ["like", "Abbildung%MAPID%"]
                        },
                    fields = ["name", "file_name", "content_hash", "file_url"]
                    )

                if len(files) == 0:
                    rv = save_file(filename, buffer.getvalue(), "Item",
                                    item_code, title_folder, is_private=1, df="image")
                    itemdoc.image = rv.file_url
                    itemdoc.save()
                    frappe.db.commit()
                
                if len(files) == 1:
                    if files[0]["content_hash"] != get_content_hash(buffer.getvalue()):
                        frappe.delete_doc("File", files[0]["name"])
                        rv = save_file(filename, buffer.getvalue(), "Item",
                                        item_code, title_folder, is_private=1, df="image")
                        itemdoc.image = rv.file_url
                        itemdoc.save()
                        frappe.db.commit()

                if len(files) > 1:
                    for f in files:
                        frappe.delete_doc("File", f["name"])
                    rv = save_file(filename, buffer.getvalue(), "Item",
                                    item_code, title_folder, is_private=1, df="image")
                    itemdoc.image = rv.file_url
                    itemdoc.save()
                    frappe.db.commit()
                

def get_item_datasheet(item_code, cop_item_row=None, settings=None, api=None):
    
    i = item_code
    if not i.startswith("MAPID-"):
        frappe.throw("Artielnummer muss mit MAPID- anfangen.")
    map_id = i[6:]
    if not settings:
        settings = frappe.get_single("COPConnect Settings")
    if not api:
        api = CopAPI(
            settings.cop_wsdl_url, settings.cop_user, settings.cop_password
            )
    if not cop_item_row:
        r = api.getArticles("mapid:" + str(map_id))
        if r["rows"]["item"][0]:
            cop_item_row = r["rows"]["item"][0]
    
    if hasattr(cop_item_row, "url_pdf"):
        if cop_item_row.url_pdf:
            with requests.Session() as s:
                d = s.get(cop_item_row.url_pdf)
                buffer = BytesIO(d.content)
                doctype_folder = create_folder("Item", "Home")
                title_folder = create_folder("datasheets", doctype_folder)
                suffix = str(cop_item_row.url_pdf).split(".")[-1]
                filename = "Datenblatt_"
                filename += cop_item_row.man_name + "_" if cop_item_row.man_name else None
                filename += cop_item_row.man_aid + "_" if cop_item_row.man_aid else None
                filename += item_code + "." + suffix
                filename = filename.replace(" ", "_")
                filename = filename.replace("/", "_")
                filename = filename.replace("|", "_")
                filename = filename.replace("\\", "_")
                files = frappe.get_all("File", filters= {"file_name": filename})
                if not files:
                    rv = save_file(filename, buffer.getvalue(), "Item",
                                    item_code, title_folder, is_private=1)
                    frappe.db.commit()


def create_folder(folder, parent):
    """Make sure the folder exists and return it's name."""
    new_folder_name = "/".join([parent, folder])
    
    if not frappe.db.exists("File", new_folder_name):
        create_new_folder(folder, parent)
    
    return new_folder_name

def create_brand_if_not_exists(brand, cop_man_id=None):
    brands = frappe.get_all("Brand", filters={'brand': brand})
    if len(brands) == 0:
        brand_doc = frappe.get_doc({
            "doctype": "Brand",
            "brand": brand,
            "cop_man_id": cop_man_id,
            "description": "erstellt durch COP Connect Import"})
        brand_doc.save()
    return brand

def create_item_group_if_not_exists(cop_item_row, api=None, settings=None):
    '''
        'group_id': 1072,
        'group_name': 'MAC Notebooks',
        'group_level1': 'Notebooks u. Tablets',
        'group_level2': 'MAC Notebooks',"""
        'group_level3': None,

        Es kann im COP den gleichen group_name mehrfach geben.
        Deswegen verketten wir immer die group_id an den group_name zzgl #

        Weiterhin gibt es Artikel, die keiner Item Group zugewiesen sind:

        'group_id': -1,
        'group_name': 'Keine Zuordnung',
        'group_level1': None,
        'group_level2': None,
        'group_level3': None,
    '''
    settings = frappe.get_single("COPConnect Settings") if not settings else None

    #default Gruppe, falls keine Zuordnung
    if cop_item_row.group_id == -1:
        return settings.destination_item_group

    item_group_name = cop_item_row.group_name + " #" + str(cop_item_row.group_id)

    if not frappe.get_all("Item Group", filters={"item_group_name": item_group_name}):

        
        api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password) if not api else None
        group_list = api.getGroups().item

        # benötigten pfad auflösen:
        needed_groups = [cop_item_row.group_id]
        if cop_item_row.group_level2:
            for group in group_list:
                if cop_item_row.group_id == group.group_id:
                    needed_groups.append(group.group_id_parent)
        if cop_item_row.group_level3:
            for group in group_list:
                if needed_groups[-1] == group.group_id:
                    needed_groups.append(group.group_id_parent)

        group_level1_name = ""
        group_level2_name = ""
        
        # group_level1 behandeln
        if cop_item_row.group_level1:
            for group in group_list:
                if group.group_name == cop_item_row.group_level1 and group.group_id in needed_groups:
                    if not frappe.get_all("Item Group", filters={"item_group_name": cop_item_row.group_level1 + " #" + str(group.group_id) }):
                    # group_level_1 noch nicht vorhanden:
                        item_group_doc = frappe.get_doc({
                            "doctype": "Item Group",
                            "item_group_name": cop_item_row.group_level1 + " #" + str(group.group_id),
                            "parent_item_group": settings.destination_item_group,
                            "is_group": 1,
                            "cop_group_id": group.group_id,
                            "description": "erstellt durch COP Import"})
                        print("saving level 1")
                        print(item_group_doc.item_group_name)
                        item_group_doc.save()
                    group_level1_name = cop_item_row.group_level1 + " #" + str(group.group_id)

        # group_level2 behandeln
        if cop_item_row.group_level2:
            for group in group_list:
                if group.group_name ==  cop_item_row.group_level2 and group.group_id in needed_groups:
                    if not frappe.get_all("Item Group", filters={"item_group_name": cop_item_row.group_level2  + " #" + str(group.group_id)}):
                    # group_level_2 noch nicht vorhanden:
                        item_group_doc = frappe.get_doc({
                            "doctype": "Item Group",
                            "item_group_name": cop_item_row.group_level2 + " #" + str(group.group_id),
                            "parent_item_group": group_level1_name,
                            "is_group": 1,
                            "cop_group_id": group.group_id,
                            "description": "erstellt durch COP Import"})
                        print("saving level 2")
                        print(item_group_doc.item_group_name)
                        print("with parent")
                        print(item_group_doc.parent_item_group)
                        item_group_doc.save()
                    group_level2_name = cop_item_row.group_level2 + " #" + str(group.group_id)
        
        # group_level3 behandeln
        if cop_item_row.group_level3:
            for group in group_list:
                if group.group_name ==  cop_item_row.group_level3 and group.group_id in needed_groups:
                    if not frappe.get_all("Item Group", filters={"item_group_name": cop_item_row.group_level3 + " #" + str(group.group_id)}):
                        # group_level_3 noch nicht vorhanden:
                        item_group_doc = frappe.get_doc({
                            "doctype": "Item Group",
                            "item_group_name": cop_item_row.group_level3 + " #" + str(group.group_id),
                            "parent_item_group": group_level2_name,
                            "is_group": 1,
                            "cop_group_id": group.group_id,
                            "description": "erstellt durch COP Import"})
                        print("saving level 3")
                        print(item_group_doc.item_group_name)
                        item_group_doc.save()

    return item_group_name
       