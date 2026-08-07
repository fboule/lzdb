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

        # Initialize fields
        self.__fields = []
        self.__fkeys = {}

        # CASE 1: Collection created from virtual PK signature
        if ukeys is not None:
            self.__ukeys = sorted(ukeys)
            self.__fields.extend(self.__ukeys)
            self.__fkeys = fkeys

        # CASE 2: Collection created from dbitem (loading from DB)
        if dbitem is not None:
            # IMPORTANT:
            # virtualKeys() define schema, NOT uniqueness
            self.__ukeys = dbitem.virtualKeys()
            self.__fields.extend(self.__ukeys)

        # Add FK fields to schema
        for field in self.__fkeys:
            if field not in self.__fields:
                self.__fields.append(field)

    def id(self):
        return self.__id

    def name(self, tname=None):
        if tname is not None:
            self.__tname = tname
        return self.__tname

    def uniqueKeys(self):
        return self.__ukeys

    def read(self, db, id):
        self.__id = id
        self.read_fkeys(db, id)

        rows = db.execute("select * from %s" % id)
        self.__fields = [desc[0] for desc in db.description]

        if self.__dbms.traceon:
            tname = f" as '{self.__tname}'" if self.__tname else ""
            if len(self.__fkeys) == 0:
                print(f"Found {rows.rowcount} rows in {id}({','.join(self.__ukeys)}){tname}")
            else:
                print(f"Found {rows.rowcount} rows in {id}({','.join(self.__ukeys)}){tname} with references:")
                for name, collection in self.__fkeys.items():
                    print(f"  {name} to {collection.id()}")

        for row in rows:
            pkitems = dict(zip(self.__fields, row))
            items = {}

            for kk in self.__fields:
                if kk in self.__fkeys:
                    items[kk] = self.__dbms.items(collection=self.__fkeys[kk], id=pkitems[kk])
                else:
                    try:
                        items[kk] = datetime.datetime.strptime(pkitems[kk], "%Y-%m-%d %H:%M:%S")
                    except:
                        items[kk] = pkitems[kk]

            obj = {}

            for field in (self.__ukeys or []):
                obj[field] = items[field]

            for field in self.__fkeys:
                obj[field] = items[field]

            dbitem = self.__dbms.newItem(collection=self, **obj)
            dbitem.id(items['id'])

            for field in items:
                if field not in (self.__ukeys or []):
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
        # Get existing columns
        db.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{self.__id}'
        """)
        existing = {row[0] for row in db.fetchall()}

        newFields = []

        for field in dbitem.fields():
            if field in existing:
                continue

            value = dbitem.get(field)

            # TRUE foreign key: value is another lzdbItem
            if isinstance(value, LZDBItem):
                db.execute(
                    f"ALTER TABLE {self.__id} "
                    f"ADD COLUMN {field} INTEGER REFERENCES {value.collection().id()}"
                )
                continue

            # Normal field -> VARCHAR
            newFields.append(field)

        # Add normal fields
        for field in newFields:
            db.execute(f"ALTER TABLE {self.__id} ADD COLUMN {field} VARCHAR")

        self.__fields.extend(newFields)

    def createTable(self, db):
        if self.__id is not None:
            return

        # Register this collection in the inventory table
        ukeys = ",".join(self.uniqueKeys() or [])
        tname = self.name() if hasattr(self, "name") else ""

        db.execute(
            """
            INSERT INTO lzdb(ukeys, tname)
            VALUES (%s, %s)
            ON CONFLICT (ukeys)
            DO UPDATE SET tname = EXCLUDED.tname
            RETURNING id
            """,
            (ukeys, tname),
        )
        res = db.fetchone()
        self.__id = f"lzdb__{res[0]}"

        # Build CREATE TABLE statement for the actual collection table
        s = f"CREATE TABLE IF NOT EXISTS {self.__id}(id SERIAL PRIMARY KEY"

        # Foreign keys
        for k, collection in self.__fkeys.items():
            fk = f"{k} INTEGER REFERENCES {collection.id()}"
            s += f", {fk}"

        fields = self.uniqueKeys() or []
        data_fields = [f"{x} VARCHAR" for x in fields if x not in self.__fkeys]
        if data_fields:
            s += ", " + ", ".join(data_fields)

        # Close CREATE TABLE
        s += ");"

        db.execute(s)


