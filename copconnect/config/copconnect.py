from __future__ import unicode_literals
from frappe import _

def get_data():

    return [
        {
            "label": _("COP Connect"),
            "icon": "octicon octicon-file-symlink-file",
            "items": [
                {
                    "type": "doctype",
                    "name": "COPConnect imports",
                    "label": _("COPConnect imports"),
                },
                {
                    "type": "doctype",
                    "name": "COP Lieferant",
                    "label": _("COP Lieferant"),
                },
                {
                    "type": "doctype",
                    "name": "COPConnect API",
                    "label": _("COPConnect API"),
                },
                {
                    "type": "doctype",
                    "name": "COPConnect Pricing Rule",
                    "label": _("COPConnect Pricing Rule"),
                },
                {
                    "type": "doctype",
                    "name": "COPConnect Settings",
                    "label": _("COPConnect Settings"),
                }
            ]
        }
    ]