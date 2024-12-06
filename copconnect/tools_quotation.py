import frappe
import copconnect.remoteapi
from copconnect.api import CopAPI
from zeep.helpers import serialize_object
from erpnext.stock.utils import get_stock_balance

@frappe.whitelist()
def check_item_prices_and_availability(docname):
    # Quotation-Dokument abrufen
    quotation = frappe.get_doc("Quotation", docname)

    # Ergebnisliste für die HTML-Ausgabe
    results = []

    # Artikel aus der Quotation-Positionstabelle prüfen
    for item in quotation.items:
        map_id = item.item_code.split("-")[1] if "MAPID-" in item.item_code else None
        print(map_id)
        if not map_id:
            continue
        
        # Artikelinformationen abrufen
        info = get_item_data(map_id)
        print(info)
        if info:
            # Angebotspreis aus der Quotation-Position
            angebotspreis = item.rate
            info["Angebotspreis"] = f"{angebotspreis:.2f} EUR"
            results.append(info)

    if not results:
        return "Keine relevanten Informationen für die Artikel in diesem Angebot gefunden."
    # HTML aus den Ergebnissen generieren
    html_output = generate_html_from_results(results, docname)
    return html_output

def get_item_data(map_id):
    settings = frappe.get_doc("COPConnect Settings")
    api = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password)
    
    try:
        # Artikelinformationen von der API abrufen
        r = serialize_object(api.getArticles("mapid:" + str(map_id)))
        item = r['rows']['item'][0]  # Erstes Element der Liste extrahieren
        
        item_data = {
            "Artikel-ID": f"MAPID-{item['map_id']}",
            "COP-Daten": {
                "Preis": {
                    "EVP": f"{item['evp']} EUR"
                },
                "Lieferstatus": "Verfügbar" if item['qty_status'] == 1 else "Nicht verfügbar"
            }
        }

        item_code = "MAPID-" + map_id
        print(item_code)


        # Verfügbarkeit prüfen
        stock_qty = get_act_stock(item_code)

        # Letzten Einkaufspreis aus ERPNext abrufen und Lagerstatus hinzufügen
        try:
            item_doc = frappe.get_doc("Item", item_code)
            last_purchase_rate = item_doc.last_purchase_rate or 0
            name = item_doc.item_name
        except frappe.DoesNotExistError:
            last_purchase_rate = None
        if last_purchase_rate:
            rule = get_best_pricing_rule(item_code, last_purchase_rate, settings)
            if rule:
                selling_price_lpr = apply_pricing_rule(rule, last_purchase_rate)
                s_p_lpr = f"{selling_price_lpr:.2f} EUR"
        else:
            s_p_lpr ="Kein Einkaufspreis vorhanden"


        item_data["itsdave"] = {
            "Status Lager": f"Auf Lager: {stock_qty}" if stock_qty and stock_qty > 0 else "Nicht auf Lager",
            "Letzter Einkaufspreis": f"{last_purchase_rate:.2f} EUR" if last_purchase_rate is not None else "Keine Daten verfügbar",
            "Verkaufspreis auf Basis von Letztem Einkaufspreis":s_p_lpr
        }
        item_data["Artikel-Name"] = name

                # Lieferantendaten abrufen
        supps = frappe.get_all(
            "COP Lieferant",
            filters=[["level", "<=", settings.min_level_for_selling_price]],
            fields=["sup_id", "supplier"]
        )

        # Liste der Supplier IDs und ein Dictionary für zusätzliche Lieferantendaten erstellen
        sup_id_list = [el["sup_id"] for el in supps]
        supplier_info = {el["sup_id"]: el["supplier"] for el in supps}
        
        print(supplier_info)

        # Artikel von Lieferanten abrufen
        supplier_data = serialize_object(api.getArticlesSupplier(map_id, sup_id_list))

        # Zusätzliche Speicherung der Lieferantendaten
        supplier_data["suppliers"] = supplier_info  # Verknüpfen der Lieferantennamen mit IDs

        # Verfügbare Artikel filtern und sortieren
        # Nur Artikel mit qty_status == 1 und price_amount >= 0
        items = supplier_data.get('item', [])
        available_suppliers = [
            supplier for supplier in items
            if supplier['qty_status'] == 1 and supplier['price_amount'] >= 0
        ]

        # Nach Preis sortieren
        sorted_suppliers = sorted(available_suppliers, key=lambda x: x['price_amount'])

        # Anzahl der Lieferanten basierend auf settings.qty_of_suppliers begrenzen
        qty_of_suppliers = settings.qty_of_suppliers or 3  # Fallback auf 3, falls nicht definiert
        cheapest_suppliers = sorted_suppliers[:qty_of_suppliers]
        for item in cheapest_suppliers:
            id = str(item['sup_id'])
            print(id)
            erp_next_sup= supplier_info.get(id,"Unbekannt")
            print(erp_next_sup)
            item["erp_next_sup"] = erp_next_sup

        # Datenstruktur für Ausgabe erstellen
        item_data["COP-Daten"]["Lieferanten"] = [
            {
                'Supplier ID': supplier['sup_id'],
                'Supplier Name': supplier['sup_name'],
                "ERPNext Lieferant":supplier["erp_next_sup"],
                'Preis': supplier['price_amount'],
                'Status': supplier['qty_status']
            }
            for supplier in cheapest_suppliers
        ]


        # # Lieferantendaten abrufen
        # supps = frappe.get_all(
        #     "COP Lieferant",
        #     filters=[["level", "<=", settings.min_level_for_supplier_part_no]],
        #     fields=["sup_id", "supplier"]
        # )
        # sup_id_list = [el["sup_id"] for el in supps]
        # supplier_data = serialize_object(api.getArticlesSupplier(map_id, sup_id_list))
        # items = supplier_data.get('item', [])

        # # Verfügbare Artikel filtern und sortieren
        # # Nur Artikel mit qty_status == 1 und price_amount >= 0
        # available_suppliers = [
        #     supplier for supplier in items
        #     if supplier['qty_status'] == 1 and supplier['price_amount'] >= 0
        # ]
        # sorted_suppliers = sorted(available_suppliers, key=lambda x: x['price_amount'])
        # cheapest_suppliers = sorted_suppliers[:3]

        # item_data["COP-Daten"]["Lieferanten"] = [
        #     {
        #         'Supplier ID': supplier['sup_id'],
        #         'Supplier Name': supplier['sup_name'],
        #         'Preis': supplier['price_amount'],
        #         'Status': supplier['qty_status']
        #     }
        #     for supplier in cheapest_suppliers
        # ]

        # Verkaufspreis berechnen
        if cheapest_suppliers:
            lowest_price = cheapest_suppliers[0]['price_amount']
            item_data["COP-Daten"]["Preis"] ["Mindestpreis"] = lowest_price
            rule = get_best_pricing_rule(item_code, lowest_price, settings)
            if rule:
                selling_price = apply_pricing_rule(rule, lowest_price)
                item_data["COP-Daten"]["Verkaufspreis"] = f"{selling_price:.2f} EUR"
            else:
                item_data["COP-Daten"]["Verkaufspreis"] = "Keine Regel gefunden"
        else:
            item_data["COP-Daten"]["Verkaufspreis"] = "Keine Lieferantendaten verfügbar"
            item_data["COP-Daten"]["Preis"]["Mindestpreis"] = "Keine Artikel verfügbar"
        # Ergebnis ausgeben
        print(item_data)
        return item_data
    
    except Exception as e:
        frappe.log_error(f"Fehler beim Abrufen der Artikelinformationen: {str(e)}", "COPConnect")
        return None

def bewertung_angebotspreis(angebotspreis, cop_preis, lpr_preis, lager_status):
    """
    Bewertet den Angebotspreis basierend auf Lagerstatus, COP-Preis und LPR-Preis.
    
    :param angebotspreis: Angebotspreis als Float
    :param cop_preis: Verkaufspreis nach COPConnect Pricing Rule oder None
    :param lpr_preis: Verkaufspreis auf Basis von letztem Einkaufspreis als Float
    :param lager_status: Status des Lagers ("Auf Lager", "Nicht auf Lager", etc.)
    :return: Tuple (Status, Beschreibung, Maßstab)
    """

    # Fall 1: Artikel nicht auf Lager und nicht lieferbar
    if lager_status == "Nicht auf Lager" and cop_preis is None:
        return "status-red", "Artikel im Moment nicht lieferbar", "N/A"

    # Fall 2: Artikel nicht auf Lager, aber lieferbar
    if lager_status == "Nicht auf Lager" and cop_preis is not None:
        maßstab = cop_preis
        maßstab_beschreibung = "Verkaufspreis COP"
        if angebotspreis < maßstab:
            status = "status-red"
            beschreibung = f"Angebotspreis niedriger als {maßstab_beschreibung}"
        elif angebotspreis > maßstab:
            status = "status-yellow"
            beschreibung = f"Angebotspreis höher als {maßstab_beschreibung}"
        else:
            status = "status-green"
            beschreibung = f"Angebotspreis gleich {maßstab_beschreibung}"

        return status, beschreibung, f"{maßstab:.2f} EUR"

    # Fall 3: Artikel auf Lager
    if cop_preis is None or lpr_preis > cop_preis:
        maßstab = lpr_preis
        maßstab_beschreibung = "Verkaufspreis LPR"
    else:
        maßstab = cop_preis
        maßstab_beschreibung = "Verkaufspreis COP"

    # Vergleich mit Angebotspreis
    if angebotspreis < maßstab:
        status = "status-red"
        beschreibung = f"Angebotspreis niedriger als {maßstab_beschreibung}"
    elif angebotspreis > maßstab:
        status = "status-yellow"
        beschreibung = f"Angebotspreis höher als {maßstab_beschreibung}"
    else:
        status = "status-green"
        beschreibung = f"Angebotspreis gleich {maßstab_beschreibung}"

    # Rückgabe der Ergebnisse
    maßstab_wert = f"{maßstab:.2f} EUR" if isinstance(maßstab, (int, float)) else "N/A"
    return status, beschreibung, maßstab_wert

def generate_html_from_results(results, docname):
    html = f"""
    <html>
    <head>
        <title>Artikelinformationen</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 15px; }}
            .card {{ border: 1px solid #ccc; border-radius: 5px; padding: 15px; margin-bottom: 15px; }}
            .card h2 {{ margin-top: 0; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            table th, table td {{ border: 1px solid #ccc; padding: 8px; text-align: left; }}
            table th {{ background-color: #f9f9f9; }}
            .status-green {{ background-color: #d4edda; color: #155724; padding: 5px; }}
            .status-yellow {{ background-color: #fff3cd; color: #856404; padding: 5px; }}
            .status-red {{ background-color: #f8d7da; color: #721c24; padding: 5px; }}
            button {{ margin-top: 10px; padding: 10px 15px; background-color: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; }}
            button:hover {{ background-color: #0056b3; }}
        </style>
       
    </head>
    <body>
    <h3>Artikelübersicht</h3>
    """

    for item in results:
        angebotspreis = float(item.get("Angebotspreis", "0").replace(" EUR", ""))
        cop_preis_raw = item['COP-Daten']["Verkaufspreis"]

        try:
            cop_preis = float(cop_preis_raw.replace(" EUR", ""))
        except ValueError:
            cop_preis = None

        try:
            lpr_preis = float(item['itsdave']['Verkaufspreis auf Basis von Letztem Einkaufspreis'].replace(" EUR", ""))
        except ValueError:
            lpr_preis = 0

        lager_status = item['itsdave']['Status Lager']
        status, beschreibung, maßstab = bewertung_angebotspreis(angebotspreis, cop_preis, lpr_preis, lager_status)
        if maßstab == 'N/A':
            maßstab_less_euro = 'N/A'
        else:
            maßstab_less_euro = float(maßstab.replace(" EUR", "").strip())

        html += f"""
        <div class="card">
            <div class="{status}">
                <strong>Status:</strong> {beschreibung}
            </div>
            <h5><strong>{item.get("Artikel-ID", "Unbekannt")}</strong></h5>
            <h5>{item.get("Artikel-Name", "Unbekannt")}</h5>
            
            <h5><strong> Lager</strong></h5>
            <ul>
                <li><strong>Status Lager:</strong> {lager_status}</li>
                <li><strong>Letzter Einkaufspreis:</strong> {item['itsdave']['Letzter Einkaufspreis']}</li>
                <li><strong>Verkaufspreis letzter Einkaufspreis (LPR):</strong> {lpr_preis:.2f} EUR</li>
            </ul>
            <h5><strong>Einkaufs-Daten</strong></h5>
            <ul>
                <li><strong>Mindestpreis:</strong> {item['COP-Daten']['Preis']['Mindestpreis']}</li>
                <li><strong>EVP:</strong> {item['COP-Daten']['Preis']['EVP']}</li>
                <li><strong>Lieferstatus:</strong> {item['COP-Daten']['Lieferstatus']}</li>
                <li><strong>Verkaufspreis nach COPConnect Pricing Rule:</strong> {cop_preis_raw}</li>
                <li><strong>Angebotspreis:</strong> {angebotspreis:.2f} EUR</li>
                <li><strong>Maßstab:</strong> {maßstab}</li>
            </ul>
            <button onclick="setOfferPrice('{docname}','{item.get("Artikel-ID", "Unbekannt")}', '{maßstab_less_euro}')">
                Maßstabspreis übernehmen
            </button>
            
            <h5><strong>Lieferanten</strong></h5>
            <table>
                <thead>
                    <tr>
                        <th>Supplier Name</th>
                        <th>ERPNext Lieferant</th>
                        <th>Preis</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
        """
        for supplier in item['COP-Daten']['Lieferanten']:
            html += f"""
                    <tr>
                        <td>{supplier['Supplier Name']}</td>
                        <td>{supplier['ERPNext Lieferant']}</td>
                        <td>{supplier['Preis']} EUR</td>
                        <td>{'Verfügbar' if supplier['Status'] == 1 else 'Nicht verfügbar'}</td>
                    </tr>
            """
        html += """
                </tbody>
            </table>
        </div>
        """

    html += """
    </body>
    </html>
    """
    return html



# def generate_html_from_results(results):


#     html = """
#     <html>
#     <head>
#         <title>Artikelinformationen</title>
#         <style>
#             body { font-family: Arial, sans-serif; margin: 15px; }
#             .card { border: 1px solid #ccc; border-radius: 5px; padding: 15px; margin-bottom: 15px; }
#             .card h2 { margin-top: 0; }
#             table { width: 100%; border-collapse: collapse; margin-top: 10px; }
#             table th, table td { border: 1px solid #ccc; padding: 8px; text-align: left; }
#             table th { background-color: #f9f9f9; }
#             .status-green { background-color: #d4edda; color: #155724; padding: 5px; }
#             .status-yellow { background-color: #fff3cd; color: #856404; padding: 5px; }
#             .status-red { background-color: #f8d7da; color: #721c24; padding: 5px; }
#         </style>
#     </head>
#     <body>
#     <h3>Artikelübersicht</h3>
#     """

#     for item in results:
#         angebotspreis = float(item.get("Angebotspreis", "0").replace(" EUR", ""))
#         cop_preis_raw = item['COP-Daten']["Verkaufspreis"]

#         # Überprüfung, ob COP-Preis verfügbar ist
#         try:
#             cop_preis = float(cop_preis_raw.replace(" EUR", ""))
#         except ValueError:
#             cop_preis = None

#         # Überprüfung, ob COP-Preis verfügbar ist   
#         try:
#             lpr_preis = float(item['itsdave']['Verkaufspreis auf Basis von Letztem Einkaufspreis'].replace(" EUR", ""))
#         except ValueError:
#             lpr_preis = 0

#         lager_status = item['itsdave']['Status Lager']
        

#         # Bewertung des Angebotspreises
#         status, beschreibung, maßstab = bewertung_angebotspreis(angebotspreis, cop_preis, lpr_preis, lager_status)

#         html += f"""
#         <div class="card">
#             <div class="{status}">
#                 <strong>Status:</strong> {beschreibung}
#             </div>
#             <h5><strong>{item.get("Artikel-ID", "Unbekannt")}</strong></h5>
#             <h5>{item.get("Artikel-Name", "Unbekannt")}</h5>
            
#             <h5><strong> Lager</strong></h5>
#              <ul>
#                 <li><strong>Status Lager:</strong> {lager_status}</li>
#                 <li><strong>Letzter Einkaufspreis:</strong> {item['itsdave']['Letzter Einkaufspreis']}</li>
#                 <li><strong>Verkaufspreis letzter Einkaufspreis (LPR):</strong> {lpr_preis:.2f} EUR</li>
#             </ul>
#             <h5><strong>Einkaufs-Daten</strong></h5>
#             <ul>
#                 <li><strong>Mindestpreis:</strong> {item['COP-Daten']['Preis']['Mindestpreis']}</li>
#                 <li><strong>EVP:</strong> {item['COP-Daten']['Preis']['EVP']}</li>
#                 <li><strong>Lieferstatus:</strong> {item['COP-Daten']['Lieferstatus']}</li>
#                 <li><strong>Verkaufspreis nach COPConnect Pricing Rule:</strong> {cop_preis_raw}</li>
#                 <li><strong>Angebotspreis:</strong> {angebotspreis:.2f} EUR</li>
#                 <li><strong>Maßstab:</strong> {maßstab}</li>
#             </ul>
            
#              <h5><strong>Lieferanten</strong></h5>
#             <table>
#                 <thead>
#                     <tr>
#                         <th>Supplier Name</th>
#                         <th>ERPNext Lieferant</th>
#                         <th>Preis</th>
#                         <th>Status</th>
#                     </tr>
#                 </thead>
#                 <tbody>
#         """
#         for supplier in item['COP-Daten']['Lieferanten']:
#             html += f"""
#                     <tr>
#                         <td>{supplier['Supplier Name']}</td>
#                         <td>{supplier['ERPNext Lieferant']}</td>
#                         <td>{supplier['Preis']} EUR</td>
#                         <td>{'Verfügbar' if supplier['Status'] == 1 else 'Nicht verfügbar'}</td>
#                     </tr>
#             """
#         html += """
#                 </tbody>
#             </table>
#         </div>
#         """

    

#     html += """
#     </body>
#     </html>
#     """
#     return html



# def generate_html_from_results(results):
#     html = """
#     <html>
#     <head>
#         <title>Artikelinformationen</title>
#         <style>
#             body { font-family: Arial, sans-serif; margin: 15px; }
#             .card { border: 1px solid #ccc; border-radius: 5px; padding: 15px; margin-bottom: 15px; }
#             .card h2 { margin-top: 0; }
#             table { width: 100%; border-collapse: collapse; margin-top: 10px; }
#             table th, table td { border: 1px solid #ccc; padding: 8px; text-align: left; }
#             table th { background-color: #f9f9f9; }
#         </style>
#     </head>
#     <body>
#     <h3>Artikelübersicht</h3>
#     """

#     for item in results:
#         html += f"""
#         <div class="card">
#             <h5><strong>{item.get("Artikel-ID", "Unbekannt")}</strong></h5>
#             <h5>{item.get("Artikel-Name", "Unbekannt")}</h5>
#             <h5><strong> Lager</strong></h5>
#              <ul>
#                 <li><strong>Status Lager:</strong> {item['itsdave']['Status Lager']}</li>
#                 <li><strong>Letzter Einkaufspreis:</strong> {item['itsdave']['Letzter Einkaufspreis']}</li>
#                 <li><strong>Verkaufspreis auf Basis von letztem Einkaufspreis:</strong> {item['itsdave']['Verkaufspreis auf Basis von Letztem Einkaufspreis']}</li>
#             </ul>
#             <h5><strong>Einkaufs-Daten</strong></h5>
#             <ul>
#                 <li><strong>Mindestpreis:</strong> {item['COP-Daten']['Preis']['Mindestpreis']}</li>
#                 <li><strong>EVP:</strong> {item['COP-Daten']['Preis']['EVP']}</li>
#                 <li><strong>Lieferstatus:</strong> {item['COP-Daten']['Lieferstatus']}</li>
#                 <li><strong>Verkaufspreis nach COPConnect Pricing Rule:</strong> {item['COP-Daten']["Verkaufspreis"]}</li>
#                 <li><strong>Angebotspreis:</strong> {item['Angebotspreis']}</li>


#             </ul>
#             <h5><strong>Lieferanten</strong></h5>
#             <table>
#                 <thead>
#                     <tr>
#                         <th>Supplier ID</th>
#                         <th>Supplier Name</th>
#                         <th>Preis</th>
#                         <th>Status</th>
#                     </tr>
#                 </thead>
#                 <tbody>
#         """
#         for supplier in item['COP-Daten']['Lieferanten']:
#             html += f"""
#                     <tr>
#                         <td>{supplier['Supplier ID']}</td>
#                         <td>{supplier['Supplier Name']}</td>
#                         <td>{supplier['Preis']} EUR</td>
#                         <td>{'Verfügbar' if supplier['Status'] == 1 else 'Nicht verfügbar'}</td>
#                     </tr>
#             """
#         html += """
#                 </tbody>
#             </table>
#         </div>
#         """

#     html += """
#     </body>
#     </html>
#     """
#     return html

@frappe.whitelist()
def set_offer_price(docname, article_id, rate):

    print(docname)
    """
    Aktualisiert den Preis eines Artikels in einem Angebot.
    
    :param docname: Name des Quotation-Dokuments
    :param article_id: Artikel-ID, die aktualisiert werden soll
    :param rate: Neuer Preis (Maßstabspreis)
    :return: "success" oder Fehlermeldung
    """
    try:
        rate = float(rate)  # Stelle sicher, dass der Preis numerisch ist
    except ValueError:
        frappe.throw(_("Ungültiger Preiswert: {0}").format(rate))

    # Hole das Angebot-Dokument
    doc = frappe.get_doc("Quotation", docname)

    # Überprüfen, ob der Status des Angebots "Submitted" ist
    if doc.docstatus == 1:
        frappe.msgprint("Das Angebot ist bereits gebucht. Die Preise können nicht angepasst werden.")

    # Ansonsten den Preis anpassen
    for item in doc.items:
        if item.item_code == article_id:
            item.rate = rate

    # Speichern und die Änderungen übernehmen
    doc.save()
    frappe.db.commit()

    return "success"

    # try:
    #     # Lade das Quotation-Dokument
    #     quotation = frappe.get_doc("Quotation", docname)
    #     print(quotation)
        
    #     # Finde das passende Item und aktualisiere den Preis
    #     for item in quotation.items:
    #         if item.item_code == article_id:
    #             item.rate = rate
    #             print(item.rate)
    #             break
    #     else:
    #         # Artikel nicht gefunden
    #         return "Artikel nicht im Angebot gefunden."
        
    #     # Speichere die Änderungen
    #     quotation.save()
    #     frappe.db.commit()
    #     return "success"
    # except Exception as e:
    #     frappe.log_error(frappe.get_traceback(), "Fehler beim Setzen des Angebotspreises")
    #     return str(e)


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



@frappe.whitelist()
def get_act_stock(name):
    warehouses = frappe.get_all("Warehouse", filters={"disabled": 0}, pluck="name")  # Alle aktiven Warenlager erhalten
    total_qty = 0

    for warehouse in warehouses:
        item_qty = get_stock_balance(name, warehouse)
        total_qty += item_qty
    
    return total_qty