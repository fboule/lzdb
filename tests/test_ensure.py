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
# ensure() Tests
# ---------------------------------------------------------------------------

def test_ensure_creates_when_missing():
    """
    ensure() must create a new item when no matching item exists.
    """
    dbms = fresh_db()

    obj = dbms.ensure(
        name="ENSURE_CREATE_TEST"
    )

    dbms.commit()

    assert obj is not None
    assert obj["name"] == "ENSURE_CREATE_TEST"
    assert obj.id() is not None


def test_ensure_returns_existing_object():
    dbms = fresh_db()

    sat1 = dbms.newItem(
        name="ENSURE_EXISTING_TEST"
    )

    dbms.commit()

    sat2 = dbms.ensure(
        name="ENSURE_EXISTING_TEST"
    )

    assert sat2 is not None
    assert sat2["name"] == "ENSURE_EXISTING_TEST"

    cur = dbms.conn.cursor()

    cur.execute(
        f"""
        SELECT MIN(id)
        FROM {sat1.collection().id()}
        WHERE name='ENSURE_EXISTING_TEST'
        """
    )

    oldest_id = cur.fetchone()[0]

    assert sat2.id() == oldest_id

def test_ensure_does_not_create_duplicate_row():
    """
    Repeated ensure() calls must not create additional rows.
    """
    dbms = fresh_db()

    sat1 = dbms.ensure(
        name="ENSURE_NO_DUPLICATE_TEST"
    )

    dbms.commit()

    sat2 = dbms.ensure(
        name="ENSURE_NO_DUPLICATE_TEST"
    )

    dbms.commit()

    assert sat1.id() == sat2.id()

    cur = dbms.conn.cursor()

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {sat1.collection().id()}
        WHERE name = 'ENSURE_NO_DUPLICATE_TEST'
        """
    )

    assert cur.fetchone()[0] == 1


def test_ensure_returns_oldest_match():
    """
    If several matching rows exist, ensure() returns
    the row with the smallest id().
    """
    dbms = fresh_db()

    sat1 = dbms.newItem(
        name="ENSURE_FIRST_MATCH_TEST"
    )

    sat2 = dbms.newItem(
        name="ENSURE_FIRST_MATCH_TEST"
    )

    dbms.commit()

    sat = dbms.ensure(
        name="ENSURE_FIRST_MATCH_TEST"
    )

    cur = dbms.conn.cursor()

    cur.execute(
        f"""
        SELECT MIN(id)
        FROM {sat.collection().id()}
        WHERE name = 'ENSURE_FIRST_MATCH_TEST'
        """
    )

    oldest_id = cur.fetchone()[0]

    assert sat.id() == oldest_id
