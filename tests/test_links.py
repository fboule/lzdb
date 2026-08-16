import psycopg as pg
from lzdb import *


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
        int(sat.collection.id.split('__')[1]),
        sat.id
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

def test_undirected_link():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b, LZDB_REL_UNDIRECTED)

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
        int(a.collection.id.split('__')[1]),
        a.id,
        int(b.collection.id.split('__')[1]),
        b.id
    ))

    assert cur.fetchone()[0] == 1

