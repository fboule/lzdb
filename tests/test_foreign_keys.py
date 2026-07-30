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
# Tests
# ---------------------------------------------------------------------------

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

def test_reload_preserves_fk_relationship():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")

    event = dbms.newItem(
        satellite=sat,
        timestamp="2025-01-01"
    )

    dbms.commit()

    dbms2 = LZDB(dbms.conn)

    satellite = None
    reloaded_event = None

    for item in dbms2.items():

        if item.get("name") == "SAT1":
            satellite = item

        if item.get("timestamp") == "2025-01-01":
            reloaded_event = item

    assert satellite is not None
    assert reloaded_event is not None

    assert reloaded_event["satellite"] is satellite

