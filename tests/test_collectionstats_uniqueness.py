from lzdb.collectionstats import CollectionStats

class FakeCollection:
    uniqueKeys = ()
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_low_uniqueness_is_acceptable():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # 100 items, 4 clusters of equal size → uniqueness = 0.04
    items = []
    for i in range(25):
        items.append({"f": 1})
        items.append({"f": 2})
        items.append({"f": 3})
        items.append({"f": 4})

    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    # Identity candidates should include "f"
    candidates = stats.identity_candidates()
    assert candidates == ["f"]
