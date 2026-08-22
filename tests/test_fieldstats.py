from lzdb.fieldstats import FieldStats

def test_fieldstats_initial_state():
    fs = FieldStats()
    assert fs.count_present == 0
    assert fs.count_total == 0
    assert fs.value_counts == {}
    assert fs.entity_values == {}
