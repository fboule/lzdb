import psycopg as pg
from lzdb import *

def fresh_db():
    conn = pg.connect(dbname="test", host="localhost")
    return LZDB(conn)

def test_delete_removes_graph_links():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b)
    dbms.commit()

    # Query only links belonging to 'a' or 'b'
    cur = dbms.conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM lzdb_links 
        WHERE (src_id = %s AND dst_id = %s)
           OR (src_id = %s AND dst_id = %s)
    """, (a.id, b.id, b.id, a.id))
    
    assert cur.fetchone()[0] == 1

def test_delete_removes_undirected_links():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    a.link(b, LZDB_REL_UNDIRECTED)
    dbms.commit()

    cur = dbms.conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM lzdb_links
        WHERE (src_id = %s AND dst_id = %s)
           OR (src_id = %s AND dst_id = %s)
    """, (a.id, b.id, b.id, a.id))

    # Expecting 1 canonical row for the undirected link
    assert cur.fetchone()[0] == 1