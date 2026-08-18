import datetime

from .constants import *
from .item import LZDBItem

class Collection(object):
    __id = None
    __ukeys = None
    __fkeys = None
    __fields = None
    __dbms = None
    __tname = ''

    def __init__(self, dbms, ukeys=None, fkeys={}, dbitem=None, tname=''):
        self.__dbms = dbms
        self.__tname = tname

        self.__fields = []
        self.__fkeys = {}

        if ukeys is not None:
            self.__ukeys = sorted(ukeys)
            self.__fields.extend(self.__ukeys)
            self.__fkeys = fkeys or {}

        if dbitem is not None:
            self.__ukeys = dbitem.virtualKeys
            self.__fields.extend(self.__ukeys)

        for field in self.__fkeys:
            if field not in self.__fields:
                self.__fields.append(field)

    @property
    def id(self):
        return self.__id

    @property
    def fkeys(self):
        return tuple(self.__fkeys)

    @property
    def name(self):
        return self.__tname

    @name.setter
    def name(self, tname=None):
        self.__tname = tname

    def extendUniqueKeys(self, ukeys):
        if self.__ukeys is None or ukeys is None:
            return None
        if set(ukeys).issubset(self.__fields):
            self.__ukeys = sorted(set(self.__ukeys).union(ukeys))
        return tuple(self.__ukeys)

    @property
    def uniqueKeys(self):
        if self.__ukeys is None:
            return None
        return tuple(self.__ukeys)

    def parse_value(self, s):
        if type(s) is not str:
            return s
        
        if s.isdigit(): 
            return int(s)

        try:
            return float(s)
        except:
            pass

        try:
            return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except:
            pass

        return s
        
    def read(self, db, id):
        self.__id = id
        self.read_fkeys(db, id)

        rows = db.execute(f'SELECT * FROM "{id}"')
        self.__fields = [desc[0] for desc in db.description]

        if self.__dbms.traceon:
            tname = f" as '{self.__tname}'" if self.__tname else ""
            if len(self.__fkeys) == 0:
                print(f"Found {rows.rowcount} rows in {id}({','.join(self.uniqueKeys or [])}){tname}")
            else:
                print(f"Found {rows.rowcount} rows in {id}({','.join(self.uniqueKeys or [])}){tname} with references:")
                for name, collection in self.__fkeys.items():
                    print(f"  {name} to {collection.id}")

        for row in rows:
            pkitems = dict(zip(self.__fields, row))
            items = {}

            for kk in self.__fields:
                if kk in self.__fkeys:
                    matches = self.__dbms.items(collection=self.__fkeys[kk], id=pkitems[kk])
                    items[kk] = matches[0] if len(matches) == 1 else (matches or None)
                else:
                    items[kk] = self.parse_value(pkitems[kk])

            obj = {}

            for field in (self.uniqueKeys or []):
                obj[field] = items[field]

            for field in self.__fkeys:
                obj[field] = items[field]

            dbitem = self.__dbms.newItem(collection=self, **obj)
            dbitem.id = items['id']

            for field in items:
                if field not in (self.uniqueKeys or []):
                    dbitem[field] = items[field]

            dbitem.clearDirty()

    def read_fkeys(self, db, id):
        s = """SELECT 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';""" % id

        db.execute(s)
        items = db.fetchall()
        self.__fkeys = {}

        for field, collid in dict(items).items():
            coll = self.__dbms.collections(id=collid)
            self.__fkeys[field] = coll

    def createNewFields(self, db, dbitem):
        db.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{self.__id}'
        """)
        existing = {row[0] for row in db.fetchall()}

        newFields = []

        for field in dbitem.fields:
            if field in existing:
                continue

            value = dbitem.get(field)

            if isinstance(value, LZDBItem):
                db.execute(
                    f'ALTER TABLE "{self.__id}" '
                    f'ADD COLUMN "{field}" INTEGER REFERENCES "{value.collection.id}"'
                )
                continue

            newFields.append(field)

        for field in newFields:
            db.execute(f'ALTER TABLE "{self.__id}" ADD COLUMN "{field}" VARCHAR')

        self.__fields.extend(newFields)

    def createTable(self, db):
        if self.__id is None:
            ukeys = ",".join(self.uniqueKeys or [])
            tname = self.name() if hasattr(self, "name") else ""

            db.execute(
                """
                INSERT INTO lzdb(ukeys, tname)
                VALUES (%s, %s)
                ON CONFLICT (ukeys)
                DO UPDATE SET
                    tname = EXCLUDED.tname
                RETURNING id
                """,
                (ukeys, tname),
            )
            res = db.fetchone()
            self.__id = f"lzdb__{res[0]}"

        s = f"CREATE TABLE IF NOT EXISTS {self.__id}(id SERIAL PRIMARY KEY"

        for k, collection in self.__fkeys.items():
            fk = f'"{k}" INTEGER REFERENCES "{collection.id}"'
            s += f', {fk}'

        fields = self.uniqueKeys or []
        data_fields = [f'"{x}" VARCHAR' for x in fields if x not in self.__fkeys]
        if data_fields:
            s += ", " + ", ".join(data_fields)

        s += ");"

        db.execute(s)
