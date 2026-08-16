class FieldStats:
    """
    Flyweight object storing raw statistical data for a single field.
    All logic is implemented in CollectionStats.
    """
    def __init__(self):
        self.count_present = 0
        self.count_total = 0
        self.value_counts = {}      # value -> count
        self.entity_values = {}     # entity_id -> set(values)
