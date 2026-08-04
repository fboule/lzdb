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

