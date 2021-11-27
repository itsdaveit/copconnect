from datetime import datetime
from pprint import pprint
import re
from warnings import filters
from attr import fields
import frappe
from copconnect.api import CopAPI
import requests
from frappe.core.doctype.file.file import create_new_folder
from frappe.utils.file_manager import save_file, get_content_hash
from frappe import enqueue
from six import BytesIO
from pprint import pprint
import datetime
import time

@frappe.whitelist()
def importitem(map_id):
    start_dt = datetime.datetime.now()
    settings = frappe.get_doc("COPConnect Settings")
    text = "Artikel mit Map ID " + map_id + " importiert."
    result = get_item(map_id)
    return_massage = "Artikel <a href=\"" + settings.base_url + "app/item/MAPID-" + map_id + "\" target=\"_blank\">MAPID-" + map_id + "</a> angelegt."
    end_dt = datetime.datetime.now()
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

def get_item(map_id):
    settings = frappe.get_doc("COPConnect Settings")
    api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password)
    
    r = api.getArticles("mapid:" + str(map_id))
    if r["rows"]["item"][0]:
        cop_item_row = r["rows"]["item"][0]

    if cop_item_row.man_name:
        create_brand_if_not_exists(cop_item_row.man_name, cop_item_row.man_id)
    

    change_detected = False

    filters = {"name": "MAPID-" + str(map_id)}
    items = frappe.get_all("Item", filters=filters)
    
    if items:
        print("### Artikelattribute aktualisieren")
        item_doc = frappe.get_doc("Item", items[0]["name"])
        item_code = item_doc.item_code
        response_item_doc, response_change  = _update_item(cop_item_row, item_doc)
        if response_change:
            print("Änderung gefunden")
            change_detected = True
            item_doc = response_item_doc
        else:
            print("Keine Änderung gefunden")
       
    else:
        #Artikel neuanlage
        change_detected = True
        item_doc = _create_item(cop_item_row)
        item_code = item_doc.item_code

    print("### Artikellieferanten aktualisieren")
    response_item_doc, response_change = set_suppliers_and_prices(item_doc=item_doc, settings=settings, api=api)
    if response_change:
        print("Änderung gefunden")
        change_detected = True
        item_doc = response_item_doc
    else:
        print("Keine Änderung gefunden")

    if change_detected:
        item_doc.save()
        frappe.db.commit()
        
    #get_item_images(item_code, cop_item_row, settings=settings, api=api)
    enqueue("copconnect.remoteapi.get_item_images", item_code=item_code) #geht leider nicht
    #get_item_images(item_code)
    get_item_datasheet(item_code, cop_item_row, settings=settings, api=api)
    
def sleep(seconds):
    time.sleep(seconds)
    return True

def set_suppliers_and_prices(item_doc, settings=None, api=None):
    change_detected = False
    i = item_doc.item_code
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
    #print(supps)
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
                        continue
    return item_doc, change_detected
    #preise anlegen und ggf. aktualisieren:
    #frappe.get_all("Item Price", filters=)
        

def _create_item(cop_item_row):
    item_dict = _get_item_dict(cop_item_row)
    item_doc = frappe.get_doc(item_dict)
    result = item_doc.insert()
    return item_doc

def _update_item(cop_item_row, item_doc):
    item_dict = _get_item_dict(cop_item_row)
    change_detected = False
    for key in item_dict:
        #Sonderbehandlung von Child-Table Barcodes
        if key == "barcode":
            barcode_exists = False
            for el in item_doc.barcodes:
                if el.barcode_type == "EAN" and el.barcode == item_dict[key]:
                    barcode_exists = True
            if not barcode_exists:
                barcode_doc = frappe.get_doc(
                    {
                        "doctype": "Item Barcode",
                        "barcode_type": "EAN",
                        "barcode": item_dict[key]
                    }
                )
                item_doc.append("barcodes", barcode_doc)
                change_detected = True
            continue
        #Behandlung von normalen Attributen
        if getattr(item_doc, key) != item_dict[key]:
            change_detected = True
            setattr(item_doc, key, item_dict[key])
    return item_doc, change_detected
            

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
        "barcode": str(cop_item_row.ean)
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
    print(cop_item_row)
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
    '''
    item_group_name = cop_item_row.group_name + " #" + str(cop_item_row.group_id)

    if not frappe.get_all("Item Group", filters={"item_group_name": item_group_name}):

        settings = frappe.get_single("COPConnect Settings") if not settings else None
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
       