import psycopg as pg
import pytest
from lzdb import LZDB

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fresh_db():
    """
    Creates a fresh LZDB instance connected to a temporary PostgreSQL database.
    Assumes the test database already exists and is empty.
    """
    conn = pg.connect(
        dbname="test",
        host="localhost"
    )
    return LZDB(conn, traceon=False)


# ---------------------------------------------------------------------------
# Virtual Primary Key Tests
# ---------------------------------------------------------------------------

def test_virtual_pk_creates_new_table():
    """
    Creating the first item defines a virtual primary key and creates lzdb__1.
    """
    dbms = fresh_db()

    item = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.commit()

    # Check that lzdb__1 exists
    cur = dbms.conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'lzdb__1'
    """)
    assert cur.fetchone() is not None


def test_virtual_pk_inventory_record():
    """
    The lzdb inventory table must contain a record describing the virtual PK.
    """
    dbms = fresh_db()

    dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )
    dbms.commit()

    cur = dbms.conn.cursor()
    cur.execute("SELECT id, ukeys FROM lzdb")
    row = cur.fetchone()

    assert row is not None
    assert row[0] == 1
    assert row[1] == "endtime,param,starttime"


def test_virtual_pk_same_schema_goes_to_same_table():
    """
    Items with identical virtual PK fields must go into the same table.
    """
    dbms = fresh_db()

    item1 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    item2 = dbms.newItem(
        param="2004",
        starttime="04-jan-2000:00:00:00",
        endtime="05-jan-2000:00:00:00"
    )

    dbms.commit()

    # Both items must be in lzdb__1
    cur = dbms.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lzdb__1")
    count = cur.fetchone()[0]

    assert count == 2


def test_virtual_pk_different_schema_creates_new_table():
    """
    Items with different virtual PK fields must create a new table.
    """
    dbms = fresh_db()

    dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00"
    )

    dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        # Missing endtime → different virtual PK
    )

    dbms.commit()

    cur = dbms.conn.cursor()

    # lzdb__1 must exist
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'lzdb__1'
    """)
    assert cur.fetchone() is not None

    # lzdb__2 must also exist
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'lzdb__2'
    """)
    assert cur.fetchone() is not None


def test_virtual_pk_uniqueness_constraint():
    """
    Virtual PKs do NOT enforce uniqueness.
    Inserting an item with the same virtual PK must update the existing row,
    not raise an exception.
    """
    dbms = fresh_db()

    # First item defines the virtual PK
    item1 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00",
        value="first"
    )
    dbms.commit()

    # Insert duplicate virtual PK with different data fields → must update
    item2 = dbms.newItem(
        param="2004",
        starttime="03-jan-2000:00:00:00",
        endtime="04-jan-2000:00:00:00",
        value="updated"
    )
    dbms.commit()

    # Fetch the row
    cur = dbms.conn.cursor()
    cur.execute("""
        SELECT value
        FROM lzdb__1
        WHERE param='2004'
          AND starttime='03-jan-2000:00:00:00'
          AND endtime='04-jan-2000:00:00:00'
    """)
    row = cur.fetchone()

    # The row must exist and must be updated
    assert row is not None
    assert row[0] == "updated"
