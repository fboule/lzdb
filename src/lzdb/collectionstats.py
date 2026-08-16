from .fieldstats import FieldStats

class CollectionStats:
    """
    Independent statistical engine for a Collection.
    It retrieves all items from the DBMS and computes identity metrics.
    """

    def __init__(self, collection, dbms):
        self.collection = collection
        self.dbms = dbms
        self.fields = {}   # field_name -> FieldStats

    # ------------------------------------------------------------
    # Compute stats from scratch
    # ------------------------------------------------------------
    def compute(self):
        """
        Retrieve all items belonging to the collection and compute stats.
        """
        items = self.dbms.items(collection=self.collection)

        for item in items:
            entity_id = item.id
            self.update(entity_id, item)

    # ------------------------------------------------------------
    # Update stats from a single item
    # ------------------------------------------------------------
    def update(self, entity_id, item):
        all_fields = set(self.fields.keys()) | set(item.keys())

        for fname in all_fields:
            fs = self.fields.setdefault(fname, FieldStats())
            fs.count_total += 1

            if fname in item:
                value = item[fname]
                fs.count_present += 1
                fs.value_counts[value] = fs.value_counts.get(value, 0) + 1
                fs.entity_values.setdefault(entity_id, set()).add(value)

    # ------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------
    def presence(self, fs: FieldStats):
        return fs.count_present / fs.count_total if fs.count_total else 0.0

    def uniqueness(self, fs: FieldStats):
        return len(fs.value_counts) / fs.count_present if fs.count_present else 0.0

    def stability(self, fs: FieldStats):
        if not fs.entity_values:
            return 0.0
        stable = sum(1 for vals in fs.entity_values.values() if len(vals) == 1)
        return stable / len(fs.entity_values)

    # ------------------------------------------------------------
    # Identity candidate detection
    # ------------------------------------------------------------
    def identity_candidates(self):
        candidates = []

        for fname, fs in self.fields.items():
            p = self.presence(fs)
            u = self.uniqueness(fs)
            s = self.stability(fs)

            if (
                p >= 0.8 and
                0.05 <= u <= 0.95 and
                s >= 0.9
            ):
                candidates.append(fname)

        return sorted(candidates)

    # ------------------------------------------------------------
    # Apply identity promotion to a Collection
    # ------------------------------------------------------------
    def promote_identity(self):
        current = set(self.collection.uniqueKeys or [])
        new = set(self.identity_candidates())

        merged = sorted(current | new)

        if tuple(merged) != self.collection.uniqueKeys:
            self.collection.extendUniqueKeys(merged)

        return self.collection.uniqueKeys
