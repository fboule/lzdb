import psycopg as pg
from lzdb import *

def fresh_db():
    conn = pg.connect(dbname="test", host="localhost")
    return LZDB(conn)

def test_delete_auto_cleanups_fk():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")
    measure = dbms.newItem(satellite=sat)

    dbms.commit()

    # Deleting sat automatically nullifies measure['satellite']
    dbms.delete(sat)
    dbms.commit()

    assert measure["satellite"] is None

    cur = dbms.conn.cursor()
    cur.execute(
        f'SELECT "satellite" FROM "{measure.collection.id}" WHERE id=%s',
        (measure.id,)
    )
    assert cur.fetchone()[0] is None

def test_cleanup_fk_references():
    dbms = fresh_db()

    sat = dbms.newItem(name="SAT1")
    measure = dbms.newItem(satellite=sat)

    dbms.commit()

    dbms.delete(sat)
    dbms.cleanup_fk_references(sat)
    dbms.commit()

    assert measure["satellite"] is None

    cur = dbms.conn.cursor()
    cur.execute(
        f'SELECT "satellite" FROM "{measure.collection.id}" WHERE id=%s',
        (measure.id,)
    )
    assert cur.fetchone()[0] is None
