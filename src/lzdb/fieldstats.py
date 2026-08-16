class FieldStats:
    """
    Flyweight object storing raw statistical data for a single field.
    All logic is implemented in CollectionStats.
    """
    def __init__(self):
        self.count_present = 0          # number of items where field exists
        self.count_total = 0            # total number of items processed
        self.value_counts = {}          # value -> count
        self.entity_values = {}         # entity_id -> set(values)
