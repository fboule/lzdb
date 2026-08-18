from .constants import *

class LZDBItem(dict):
    def __init__(self, collection, **kwargs):
        super().__init__()

        self.__collection = collection
        self.__dirty = False
        self.__id = None

        # Each link is: { "item": <LZDBItem>, "reltype": <enum string> }
        self.__links = []

        for k, v in kwargs.items():
            self[k] = v

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__dirty = True

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self.__dirty = True

    def markDirty(self):
        self.__dirty = True

    def clearDirty(self):
        self.__dirty = False

    @property
    def isDirty(self):
        return self.__dirty

    @property
    def foreignKeys(self):
        result = {}

        for field, value in self.items():
            if isinstance(value, LZDBItem):
                result[field] = value.collection

        return result

    @property
    def fields(self):
        return tuple(self.keys())

    def link(self, item, reltype=None):
        """
        reltype may be:
            - None → defaults to LZDB_REL_DIRECTED
            - LZDB_REL_DIRECTED (0)
            - LZDB_REL_UNDIRECTED (1)
            - or directly the enum string ('directed', 'undirected')
        """

        if reltype is None:
            reltype = LZDB_REL_DIRECTED

        if reltype == LZDB_REL_DIRECTED:
            reltype_str = "directed"
        elif reltype == LZDB_REL_UNDIRECTED:
            reltype_str = "undirected"
        else:
            reltype_str = reltype

        if isinstance(item, (list, tuple)):
            for it in item:
                self.link(it, reltype_str)
            return

        self.__links.append({
            "item": item,
            "reltype": reltype_str
        })
        self.markDirty()

    @property
    def links(self):
        return tuple(self.__links)

    def del_link(self, item):
        original_len = len(self.__links)
        self.__links = [x for x in self.__links if x["item"] is not item and x["item"] != item]
        
        if len(self.__links) < original_len:
            self.markDirty()

    def clear_links(self):
        if self.__links:
            self.__links.clear()
            self.markDirty()

    def set(self, **kwargs):
        for k, v in kwargs.items():
            self[k] = v

    @property
    def uniqueDict(self):
        return {k: self[k] for k in self.virtualKeys}

    @property
    def collection(self):
        return self.__collection

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, value):
        self.__id = value

    @property
    def virtualKeys(self):
        if self.__collection is None:
            return tuple(sorted([k for k, v in self.items() if not isinstance(v, list)]))
        return self.__collection.uniqueKeys
