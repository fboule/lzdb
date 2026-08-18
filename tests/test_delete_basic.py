import psycopg as pg
from lzdb import *

def fresh_db():
    conn = pg.connect(dbname="test", host="localhost")
    dbms = LZDB(conn)
    return dbms

def test_delete_basic():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    b = dbms.newItem(name="B")

    dbms.commit()

    assert a in dbms.items()
    assert b in dbms.items()

    dbms.delete(a)
    dbms.commit()

    items = dbms.items()
    assert a not in items
    assert b in items

def test_delete_removes_row_from_table():
    dbms = fresh_db()

    a = dbms.newItem(name="A")
    dbms.commit()

    coll = a.collection
    cur = dbms.conn.cursor()

    # row exists before delete
    cur.execute(f'SELECT COUNT(*) FROM "{coll.id}" WHERE id=%s', (a.id,))
    assert cur.fetchone()[0] == 1

    dbms.delete(a)
    dbms.commit()

    # row removed
    cur.execute(f'SELECT COUNT(*) FROM "{coll.id}" WHERE id=%s', (a.id,))
    assert cur.fetchone()[0] == 0
