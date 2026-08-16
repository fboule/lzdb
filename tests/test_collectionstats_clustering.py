import math
from lzdb.collectionstats import CollectionStats
from lzdb.fieldstats import FieldStats

class FakeCollection:
    uniqueKeys = ()
    def extendUniqueKeys(self, keys):
        self.uniqueKeys = tuple(keys)

def test_poor_clustering_detection():
    coll = FakeCollection()
    stats = CollectionStats(coll, dbms=None)

    # 100 items:
    # - 98 items have value 1
    # - 1 item has value 2
    # - 1 item has value 3
    items = []
    for i in range(98):
        items.append({"f": 1})
    items.append({"f": 2})
    items.append({"f": 3})

    # Update stats
    for i, item in enumerate(items):
        stats.update(entity_id=i, item=item)

    fs = stats.fields["f"]

    # Presence: field appears in all items
    assert stats.presence(fs) == 1.0

    # Uniqueness: 3 distinct values / 100 items
    assert stats.uniqueness(fs) == 3 / 100

    # Cluster imbalance: largest cluster = 98 / 100 = 0.98
    imbalance = stats.cluster_imbalance(fs)
    assert imbalance == 0.98

    # Entropy: very low because distribution is extremely skewed
    H = stats.entropy(fs)
    # entropy should be close to:
    # -0.98*log(0.98) - 0.01*log(0.01) - 0.01*log(0.01)
    expected_entropy = (
        -0.98 * math.log(0.98)
        -0.01 * math.log(0.01)
        -0.01 * math.log(0.01)
    )
    assert abs(H - expected_entropy) < 1e-9

    # Identity candidates should be empty because clustering is poor
    assert stats.identity_candidates() == []
