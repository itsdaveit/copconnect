# -*- coding: utf-8 -*-
# Copyright (c) 2018, itsdave GmbH and contributors
# For license information, please see license.txt


import frappe
from typing import Optional, Tuple, List, Dict

def parse_series_from_name(name: str, rules: List[Dict]) -> Optional[Tuple[str, int]]:
    """
    Finde anhand eines generierten Namens (z. B. 'ITEM-00071') die passende Document Naming Rule
    und gib (rule_name, vergebene_nummer) zurück.
    'rules' ist das Ergebnis von frappe.get_all('Document Naming Rule', fields=['name','prefix','prefix_digits','counter','priority']).
    """
    for r in rules:
        prefix = (r.get("prefix") or "")
        digits = int(r.get("prefix_digits") or 0)
        if digits <= 0:
            continue
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if suffix.isdigit() and len(suffix) == digits:
            return (r["name"], int(suffix))
    return None


def get_max_series_number(doctype: str, prefix: str, digits: int) -> int:
    """
    Ermittle die höchste bereits vergebene Seriennummer für Namen mit diesem Prefix & fester Länge.
    Beispiel: doctype='Item', prefix='ITEM-', digits=5 -> sucht max(00071) in 'ITEM-.00071'
    """
    start_pos = len(prefix) + 1  # SUBSTRING ist 1-basiert
    res = frappe.db.sql(
        f"""
        SELECT MAX(CAST(SUBSTRING(name, {start_pos}) AS UNSIGNED))
        FROM `tab{doctype}`
        WHERE name LIKE %s
          AND LENGTH(name) = %s
        """,
        (f"{prefix}%", len(prefix) + digits),
    )
    return int(res[0][0] or 0)


def reset_naming_counter_after_rename(doctype: str, generated_name: str, *, debug: bool = False) -> Dict:

    """
    Passt den Counter in Document Naming Rule an, nachdem ein Serienname umbenannt wurde.
    """
    summary = {
        "doctype": doctype,
        "generated_name": generated_name,
        "rule_name": None,
        "rule_prefix": None,
        "rule_digits": None,
        "counter_before": None,
        "assigned_num": None,
        "optimistic_decrement_rows": 0,
        "counter_after_dec": None,
        "fallback_max_num": None,
        "counter_final": None,
        "cache_cleared": False,
        "note": None,
    }

    # 1) aktive Rules laden
    rules = frappe.get_all(
        "Document Naming Rule",
        filters={"document_type": doctype, "disabled": 0 },
        fields=["name", "prefix", "prefix_digits", "counter", "priority"],
        order_by="priority desc",
    )
    if debug:
        print(f"[NR] rules for {doctype}: {rules}")

    parsed = parse_series_from_name(generated_name, rules)
    if not parsed:
        summary["note"] = "no_matching_rule"
        if debug:
            print(f"[NR] No matching rule for generated_name={generated_name}")
        return summary

    rule_name, assigned_num = parsed
    rule_doc = frappe.get_doc("Document Naming Rule", rule_name)
    summary["rule_name"] = rule_doc.name
    summary["rule_prefix"] = rule_doc.prefix or ""
    summary["rule_digits"] = int(rule_doc.prefix_digits or 0)
    summary["assigned_num"] = int(assigned_num)
    summary["counter_before"] = int(rule_doc.counter or 0)

    if debug:
        print(f"[NR] matched rule={rule_doc.name} prefix='{rule_doc.prefix}' digits={rule_doc.prefix_digits} "
              f"assigned_num={assigned_num} counter_before={summary['counter_before']}")

    # 2) optimistisches Dekrement (nur wenn gleich)
    frappe.db.sql(
        """
        UPDATE `tabDocument Naming Rule`
           SET counter = counter - 1
         WHERE name = %s
           AND counter = %s
        """,
        (rule_doc.name, int(assigned_num)),
    )
    try:
        affected = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
    except Exception:
        affected = 0
    summary["optimistic_decrement_rows"] = int(affected)

    # 3) Counter nach Dekrement lesen
    after_dec = int(frappe.db.get_value("Document Naming Rule", rule_doc.name, "counter") or 0)
    summary["counter_after_dec"] = after_dec

    if debug:
        print(f"[NR] optimistic_decrement_rows={affected} counter_after_dec={after_dec}")

    if after_dec != assigned_num - 1:
        max_num = get_max_series_number(doctype, summary["rule_prefix"], summary["rule_digits"])
        summary["fallback_max_num"] = int(max_num)

        if debug:
            print(f"[NR] fallback: max_num={max_num} (from existing {doctype}s with prefix='{summary['rule_prefix']}')")

        if after_dec != max_num:
            frappe.db.set_value("Document Naming Rule", rule_doc.name, "counter", int(max_num), update_modified=False)
            summary["counter_final"] = int(max_num)
        else:
            summary["counter_final"] = after_dec
    else:
        summary["counter_final"] = after_dec

    # 4) Cache invalidieren
    try:
        frappe.cache().delete_value(f"naming_rule:{rule_doc.name}")
        summary["cache_cleared"] = True
    except Exception:
        pass
    frappe.clear_cache(doctype="Document Naming Rule")

    if debug:
        print(f"[NR] final summary: {summary}")

    return summary


@frappe.whitelist()
def test_naming_rule_counter():
    """
    Hilfsfunktion zum Testen des Document Naming Rule Counters
    """
    try:
        # 1) Aktuellen Counter-Wert ablesen
        current_rules = frappe.get_all(
            "Document Naming Rule",
            filters={"document_type": "Item", "prefix": "ITEM-"},
            fields=["name", "prefix", "counter", "priority"],
            order_by="priority desc"
        )
        
        if not current_rules:
            return {
                "status": "error",
                "message": "Keine Document Naming Rule für Item mit Prefix 'ITEM-' gefunden"
            }
        
        rule = current_rules[0]  # Erste (höchste Priorität)
        old_counter = rule.counter
        
        print(f"=== COUNTER TEST ===")
        print(f"Regel: {rule.name}")
        print(f"Prefix: {rule.prefix}")
        print(f"Alter Counter: {old_counter}")
        print(f"Priorität: {rule.priority}")
        
        # 2) Counter auf Testwert setzen
        test_counter = 100
        frappe.db.set_value("Document Naming Rule", rule.name, "counter", test_counter)
        frappe.db.commit()
        
        # 3) Neuen Wert bestätigen
        new_counter = frappe.db.get_value("Document Naming Rule", rule.name, "counter")
        
        print(f"Neuer Counter gesetzt auf: {test_counter}")
        print(f"Bestätigter Counter: {new_counter}")
        print(f"===================")
        
        # 4) Frontend-Ausgabe
        return {
            "status": "success",
            "message": f"Counter erfolgreich geändert",
            "details": {
                "rule_name": rule.name,
                "old_counter": old_counter,
                "new_counter": new_counter,
                "test_counter": test_counter
            }
        }
        
    except Exception as e:
        error_msg = f"Fehler beim Testen des Counters: {str(e)}"
        print(error_msg)
        return {
            "status": "error",
            "message": error_msg
        }


@frappe.whitelist()
def reset_counter_to_original():
    """
    Counter auf den ursprünglichen Wert zurücksetzen
    """
    try:
        # Ursprünglichen Wert wiederherstellen (angenommen: 1)
        original_counter = 1
        
        current_rules = frappe.get_all(
            "Document Naming Rule",
            filters={"document_type": "Item", "prefix": "ITEM-"},
            fields=["name"],
            limit=1
        )
        
        if current_rules:
            rule_name = current_rules[0].name
            frappe.db.set_value("Document Naming Rule", rule_name, "counter", original_counter)
            frappe.db.commit()
            
            print(f"Counter auf {original_counter} zurückgesetzt")
            return {
                "status": "success",
                "message": f"Counter auf {original_counter} zurückgesetzt"
            }
        else:
            return {
                "status": "error",
                "message": "Keine Regel gefunden"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Fehler beim Zurücksetzen: {str(e)}"
        }

