import frappe


@frappe.whitelist()


def importitem(map_id):
    return "Artikel mit Map ID " + map_id + " importiert."


@frappe.whitelist()

def importnote(note_id, customer_id=None):
    if customer_id:

        return "Merkliste mit Note ID " + str(note_id) + " importiert. Angebot für Kunde " + str(customer_id) + " erstellt." 
    else:
        return "Merkliste mit Note ID " + str(note_id) + " importiert." 
