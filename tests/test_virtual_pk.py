import psycopg as pg
from lzdb import LZDB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fresh_db():
    conn = pg.connect(
        dbname="test",
        host="localhost"
    )
    dbms = LZDB(conn, traceon=False)
    dbms.register()
    return dbms


# ---------------------------------------------------------------------------
# Virtual PK Tests
# ---------------------------------------------------------------------------

def test_virtual_pk_creates_collection():
    """
    Creating the first item creates a collection.
    """
    dbms = fresh_db()

    dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.commit()

    cur = dbms.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lzdb")

    assert cur.fetchone()[0] >= 1


def test_virtual_pk_inventory_record():
    """
    Inventory table stores the schema signature.
    """
    dbms = fresh_db()

    dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.commit()

    cur = dbms.conn.cursor()
    cur.execute(
        """
        SELECT ukeys
        FROM lzdb
        WHERE ukeys = 'endtime,param,starttime'
        """
    )

    row = cur.fetchone()

    assert row is not None
    assert row[0] == "endtime,param,starttime"


def test_virtual_pk_same_schema_goes_to_same_collection():
    """
    Same schema must reuse the same collection.
    """
    dbms = fresh_db()

    item1 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    item2 = dbms.newItem(
        param="2005",
        starttime="04-jan-2000:00:00:00",
        endtime="05-jan-2000:00:00:00"
    )

    assert item1.collection() is item2.collection()


def test_virtual_pk_different_schema_creates_new_collection():
    """
    Different schema → different collection.
    """
    dbms = fresh_db()

    item1 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    item2 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00"
    )

    assert item1.collection() is not item2.collection()


def test_virtual_pk_allows_duplicate_rows():
    """
    Virtual PK describes schema only.
    Duplicate values are allowed and create distinct rows.
    """
    dbms = fresh_db()

    item1 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    item2 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.commit()

    assert item1.id() != item2.id()

    cur = dbms.conn.cursor()

    table_name = item1.collection().id()

    cur.execute(f"SELECT COUNT(*) FROM {table_name}")

    count = cur.fetchone()[0]

    assert count >= 2

def test_fk_column_creation():
    dbms = fresh_db()

    satellite = dbms.newItem(
        name="SAT1"
    )

    event = dbms.newItem(
        satellite=satellite,
        timestamp="2025-01-01"
    )

    dbms.commit()

    cur = dbms.conn.cursor()

    # Verify FK column exists
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{event.collection().id()}'
    """)

    columns = {row[0] for row in cur.fetchall()}

    assert "satellite" in columns

def test_fk_constraint_creation():
    dbms = fresh_db()

    satellite = dbms.newItem(name="SAT1")

    event = dbms.newItem(
        satellite=satellite,
        timestamp="2025-01-01"
    )

    dbms.commit()

    cur = dbms.conn.cursor()

    cur.execute(f"""
        SELECT
            kcu.column_name,
            ccu.table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name = '{event.collection().id()}'
    """)

    fks = cur.fetchall()

    assert ("satellite", satellite.collection().id()) in fks

def test_fk_value_storage():
    dbms = fresh_db()

    satellite = dbms.newItem(name="SAT1")

    event = dbms.newItem(
        satellite=satellite,
        timestamp="2025-01-01"
    )

    dbms.commit()

    cur = dbms.conn.cursor()

    cur.execute(f"""
        SELECT satellite
        FROM {event.collection().id()}
        WHERE id = {event.id()}
    """)

    fk_value = cur.fetchone()[0]

    assert fk_value == satellite.id()

def test_link_creation():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")

    m1 = dbms.newItem(timestamp="t1", value="1")
    m2 = dbms.newItem(timestamp="t2", value="2")

    sat.link(m1)
    sat.link(m2)

    dbms.commit()

    cur = dbms.conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM lzdb_links
        WHERE src_collection=%s
          AND src_id=%s
    """,
    (
        int(sat.collection().id().split('__')[1]),
        sat.id()
    ))

    assert cur.fetchone()[0] == 2

def test_link_retrieval():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")

    m1 = dbms.newItem(timestamp="t1", value="1")
    m2 = dbms.newItem(timestamp="t2", value="2")

    sat.link(m1)
    sat.link(m2)

    dbms.commit()

    items = dbms.linkedItems(sat)

    assert len(items) == 2

def test_reload_persists_items():
    conn = pg.connect(
        dbname="test",
        host="localhost"
    )

    dbms = LZDB(conn)

    item = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.commit()

    dbms2 = LZDB(conn)

    items = dbms2.items()

    assert len(items) >= 1

    loaded = items[0]

    assert loaded["param"] == "2004"

def test_reload_persists_fk():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")

    event = dbms.newItem(
        satellite=sat,
        timestamp="2025-01-01"
    )

    dbms.commit()

    dbms2 = LZDB(dbms.conn)

    found = False

    for item in dbms2.items():
        if item.get("timestamp") == "2025-01-01":
            found = True

            print(item)
            print(item.keys())

            assert "satellite" in item
            assert item["satellite"] is not None

    assert found

def test_undirected_link():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b, LZDB.REL_UNDIRECTED)

    dbms.commit()

    a_links = dbms.linkedItems(a)
    b_links = dbms.linkedItems(b)

    assert b in a_links
    assert a in b_links

def test_fk_and_graph_link_coexist():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")

    measure = dbms.newItem(
        satellite=sat
    )

    sat.link(measure)

    dbms.commit()

    links = dbms.linkedItems(sat)

    assert measure in links

def test_list_fields_not_in_virtual_key():
    dbms = fresh_db()

    item1 = dbms.newItem(
        satellite="SAT1",
        values=[1, 2, 3]
    )

    item2 = dbms.newItem(
        satellite="SAT2",
        values=[4, 5, 6]
    )

    assert item1.collection() is item2.collection()

def test_collection_reuse_after_commit():
    dbms = fresh_db()

    item1 = dbms.newItem(
        a="1",
        b="2"
    )

    dbms.commit()

    item2 = dbms.newItem(
        a="3",
        b="4"
    )

    assert item1.collection() is item2.collection()

def test_empty_virtual_key_collection():
    dbms = fresh_db()

    a = dbms.newItem()
    b = dbms.newItem()

    dbms.commit()

    assert a.collection() is b.collection()

def test_duplicate_link_inserted_once():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b)
    a.link(b)

    dbms.commit()

    cur = dbms.conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM lzdb_links
        WHERE src_collection=%s
          AND src_id=%s
          AND dst_collection=%s
          AND dst_id=%s
    """,
    (
        int(a.collection().id().split('__')[1]),
        a.id(),
        int(b.collection().id().split('__')[1]),
        b.id()
    ))

    assert cur.fetchone()[0] == 1


