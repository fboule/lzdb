from lzdb.collectionstats import CollectionStats

class FakeCollection:
    uniqueKeys = None
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_identity_candidates_and_promotion():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # Items designed so that:
    # - field 'a' is present in all items, stable, moderately unique
    # - field 'b' is present in 50% → should NOT be identity
    items = [
        {"a": 1, "b": 10},
        {"a": 1},
        {"a": 2, "b": 20},
        {"a": 2},
    ]

    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    candidates = stats.identity_candidates()
    assert candidates == ["a"]

    # Promote identity
    new_identity = stats.promote_identity()
    assert new_identity == ("a",)
