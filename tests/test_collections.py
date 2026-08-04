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
    dbms.expose()
    return dbms


# ---------------------------------------------------------------------------
# Tests
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
    Different schema -> different collection.
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

