from .constants import *

class LZDBItem(dict):
    def __init__(self, collection, **kwargs):
        super().__init__()

        self.__collection = collection
        self.__id = None
        self.__loaded = False
        self.__dirty = True

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

    def markLoaded(self):
        self.__loaded = True

    def isLoaded(self):
        return self.__loaded

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
        """
        Return the virtual PK dictionary.
        This is used for deduplication and schema grouping.
        """
        return {k: self[k] for k in self.virtualKeys()}

    def collection(self):
        return self.__collection

    def id(self, value=None):
        if value is not None:
            self.__id = value
        return self.__id

    def virtualKeys(self):
        """
        Virtual PK = schema descriptor.
        These fields determine the table schema,
        NOT uniqueness constraints.
        """
        keys = []
        for k, v in self.items():
            if k == "id":
                continue
            if k.startswith("refers"):
                continue
            if isinstance(v, list):
                continue
            keys.append(k)
        return sorted(keys)


