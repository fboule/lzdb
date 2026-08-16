from lzdb.collectionstats import CollectionStats

class FakeItem(dict):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self.id = id

class FakeDBMS:
    def __init__(self, items):
        self._items = items

    def items(self, collection):
        return self._items

class FakeCollection:
    uniqueKeys = ()
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_full_identity_pipeline():
    coll = FakeCollection()

    # Balanced clustering → identity field
    items = [
        FakeItem(id=1, f=1),
        FakeItem(id=2, f=1),
        FakeItem(id=3, f=2),
        FakeItem(id=4, f=2),
    ]

    dbms = FakeDBMS(items)
    stats = CollectionStats(coll, dbms)

    stats.compute()
    stats.promote_identity()

    assert coll.uniqueKeys == ("f",)
