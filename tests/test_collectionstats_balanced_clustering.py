import math
from lzdb.collectionstats import CollectionStats
from lzdb.fieldstats import FieldStats

class FakeCollection:
    uniqueKeys = ()
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_balanced_clustering_detection():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # Balanced clustering:
    # 100 items split evenly into 4 clusters:
    # - 25 items with value 1
    # - 25 items with value 2
    # - 25 items with value 3
    # - 25 items with value 4
    items = []
    for i in range(25):
        items.append({"f": 1})
        items.append({"f": 2})
        items.append({"f": 3})
        items.append({"f": 4})

    # Update stats
    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    fs = stats.fields["f"]

    # Presence: field appears in all items
    assert stats.presence(fs) == 1.0

    # Uniqueness: 4 distinct values / 100 items
    assert stats.uniqueness(fs) == 4 / 100

    # Cluster imbalance: largest cluster = 25 / 100 = 0.25
    imbalance = stats.cluster_imbalance(fs)
    assert imbalance == 0.25

    # Entropy: high because distribution is balanced
    # p = 0.25 for each cluster
    expected_entropy = -4 * (0.25 * math.log(0.25))
    H = stats.entropy(fs)
    assert abs(H - expected_entropy) < 1e-9

    # Identity candidates should include this field
    candidates = stats.identity_candidates()
    assert candidates == ["f"]

    # Promote identity
    new_identity = stats.promote_identity()
    assert new_identity == ("f",)
