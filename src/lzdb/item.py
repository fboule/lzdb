from .constants import *

class LZDBItem(dict):
    def __init__(self, collection, **kwargs):
        super().__init__()

        self.__collection = collection
        self.__id = None
        self.__dirty = False

        self.__links = []

        for k, v in kwargs.items():
            self[k] = v

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__dirty = True

    def markDirty(self):
        self.__dirty = True

    def clearDirty(self):
        self.__dirty = False

    def isDirty(self):
        return self.__dirty

    def foreignKeys(self):
        result = {}

        for field, value in self.items():
            if isinstance(value, LZDBItem):
                result[field] = value.collection()

        return result

    def fields(self):
        return list(self.keys())

    def link(self, item, reltype=None):

        if reltype is None:
            reltype = LZDB_REL_DIRECTED

        if isinstance(item, list):

            for it in item:
                self.link(it, reltype)

            return

        self.__links.append({
            "item": item,
            "reltype": reltype
        })

    def links(self):
        return self.__links

    def set(self, **kwargs):
        """
        Update fields of the item (original lzdb behavior).
        """
        for k, v in kwargs.items():
            self[k] = v

    def uniqueDict(self):
        return {k: self[k] for k in self.virtualKeys()}

    def collection(self):
        return self.__collection

    def id(self, value=None):
        if value is not None:
            self.__id = value
        return self.__id

    def virtualKeys(self):
        if self.__collection is None:
            return sorted(list(self.keys()))
        else:
            return self.__collection.uniqueKeys()


