from lzdb.collectionstats import CollectionStats

class FakeCollection:
    uniqueKeys = ()
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_high_uniqueness_rejected():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # 100 items, each with a unique value → uniqueness = 1.0
    items = []
    for i in range(100):
        items.append({"f": i})

    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    # Identity candidates should be empty
    candidates = stats.identity_candidates()
    assert candidates == []
