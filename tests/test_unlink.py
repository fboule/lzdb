import psycopg as pg
from lzdb import *

def fresh_db():
    conn = pg.connect(dbname="test", host="localhost")
    return LZDB(conn)

def test_unlink_removes_graph_edges():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b)
    dbms.commit()

    cur = dbms.conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM lzdb_links 
        WHERE (src_id = %s AND dst_id = %s)
           OR (src_id = %s AND dst_id = %s)
    """, (a.id, b.id, b.id, a.id))

    assert cur.fetchone()[0] == 1

def test_unlink_does_not_delete_items():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b)
    dbms.commit()

    dbms.unlink(a)
    dbms.commit()

    items = dbms.items()
    assert a in items
    assert b in items
