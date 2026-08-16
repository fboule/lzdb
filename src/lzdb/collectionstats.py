from .fieldstats import FieldStats

class CollectionStats:
    """
    Statistical engine for a Collection.
    Tracks field-level statistics and computes identity candidates.
    """

    def __init__(self):
        self.fields = {}   # field_name -> FieldStats

    def reset(self):
        self.fields = {}   # field_name -> FieldStats

    # ------------------------------------------------------------
    # Update stats from a new item
    # ------------------------------------------------------------
    def update(self, entity_id, item):
        """
        Update statistics based on the given item.
        entity_id may be None if items have no stable identity yet.
        """
        all_fields = set(self.fields.keys()) | set(item.keys())

        for fname in all_fields:
            fs = self.fields.setdefault(fname, FieldStats())
            fs.count_total += 1

            if fname in item:
                value = item[fname]
                fs.count_present += 1
                fs.value_counts[value] = fs.value_counts.get(value, 0) + 1

                if entity_id is not None:
                    fs.entity_values.setdefault(entity_id, set()).add(value)

    # ------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------
    def presence(self, fs: FieldStats):
        """Fraction of items where the field is present."""
        return fs.count_present / fs.count_total if fs.count_total else 0.0

    def uniqueness(self, fs: FieldStats):
        """How many distinct values exist relative to presence."""
        return len(fs.value_counts) / fs.count_present if fs.count_present else 0.0

    def stability(self, fs: FieldStats):
        """
        Fraction of entities for which the field never changes.
        Only meaningful when entity_id is provided during updates.
        """
        if not fs.entity_values:
            return 0.0
        stable = sum(1 for vals in fs.entity_values.values() if len(vals) == 1)
        return stable / len(fs.entity_values)

    # ------------------------------------------------------------
    # Identity candidate detection
    # ------------------------------------------------------------
    def identity_candidates(self):
        """
        Determine which fields should be promoted to vPK.
        Uses presence, uniqueness, and stability metrics.
        """
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
    def promote_identity(self, collection):
        """
        Promote fields to the collection's identity (vPK).
        Identity evolution is monotonic: fields are only added.
        """
        current = set(collection.uniqueKeys or [])
        new = set(self.identity_candidates())

        merged = sorted(current | new)

        if tuple(merged) != collection.uniqueKeys:
            collection.extendUniqueKeys(merged)

        return collection.uniqueKeys
