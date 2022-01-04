import frappe
import copconnect.remoteapi

@frappe.whitelist()
def update_all_items():
    items_list = frappe.get_all("Item", filters={"item_code": ["like", "MAPID-%"]} )
    counter = 0
    log = ""
    for el in items_list:
        counter += 1
        text = "processing " + str(el["name"]) + " " + str(counter) + " of " + " " + str(len(items_list))
        print(text)
        log += text
        try:
            result = copconnect.remoteapi.importitem(el["name"])
        except Exception as e:
            log += str(e)
    return log

    