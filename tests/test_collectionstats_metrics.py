from lzdb.collectionstats import CollectionStats
from lzdb.fieldstats import FieldStats

class FakeCollection:
    uniqueKeys = None
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_presence_uniqueness_stability():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # Simulate items
    items = [
        {"a": 1, "b": 10},
        {"a": 1, "b": 20},
        {"a": 2},               # b missing
        {"a": 2, "b": 10},
    ]

    # Update stats with fake entity IDs
    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    fs_a = stats.fields["a"]
    fs_b = stats.fields["b"]

    # Presence
    assert stats.presence(fs_a) == 1.0
    assert stats.presence(fs_b) == 0.75

    # Uniqueness
    assert stats.uniqueness(fs_a) == 1 / 2
    assert stats.uniqueness(fs_b) == 2 / 3

    # Stability (each entity has only one value)
    assert stats.stability(fs_a) == 1.0
    assert stats.stability(fs_b) == 1.0
