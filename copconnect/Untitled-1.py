
def get_item_images(item_name):
    settings = frappe.get_single("COPConnect Settings")
    CAPI = CopAPI(settings.cop_wsdl_url, settings.cop_user, settings.cop_password)
    if item_name.startswith("MAPID-"):
        map_id = item_name[6:]
        r = CAPI.getArticlesContent(map_id)["item"]
        print(r)
        for el in r:
            if el["sup_name"] in ("cop media", "cnet.de"):
                with requests.Session() as s:
                    if el["url_pic"]:
                        d = s.get(el["url_pic"])
                        buffer = BytesIO(d.content)
                        itemdoc = frappe.get_doc("Item", item_name)

                        doctype_folder = create_folder("Item", "Home")
                        title_folder = create_folder("title-images", doctype_folder)

                        filename = "title_image_" + item_name + ".jpg"
                        
                        files = frappe.get_all("File", filters= {"file_name": filename})
                        if not files:
                            print(filename)
                            if not frappe.db.exists("File", filename):
                                rv = save_file(filename, buffer.getvalue(), "Item",
                                            item_name, title_folder, is_private=1)
                                print(filename)
                            if not itemdoc.image == rv.file_url:
                                itemdoc.image = rv.file_url
                                itemdoc.save()

def create_folder(folder, parent):
    """Make sure the folder exists and return it's name."""
    new_folder_name = "/".join([parent, folder])
    
    if not frappe.db.exists("File", new_folder_name):
        create_new_folder(folder, parent)
    
    return new_folder_name