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

    @property
    def links(self):
        return tuple(self.__links)

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
    