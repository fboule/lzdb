# lzdb

LZDB stands for Lazy Database. I am too lazy to create tables in a proper database from my Python scripts. Also this is not relevant per se for what I need to do. LZDB creates the tables with names by itself. Push the lzdbItem object to LZDB and added to the collections of objects with the same fields. If no collection match, a new one is created.

List of supported features:
 * Instanciating a new item. 
 * Cross-references between items
 * Adding/updating fields 
 
## Foreword

Initializing LZDB:

```
import psycopg2 as pg
from lzdb import *
LZDB.traceon = True # Optional
conn = pg.connect(database = 'test', host='localhost')
dbms = LZDB(conn)
```

The created tables will be sequentially numbered with the prefix `lzdb__`. The `lzdb` table contains the inventory of all the tables with 
their virtual primary key. Each table has one primary key which is named `id` and is a sequential number. The virtual primary key is in
fact a `unique` declaration. The virtual primary key is on one hand a way to identify duplicates in the table and on the other hand to identify
the table.

Let's go for an example in the next section.

***Important note***: everything remains volatile until you explicitly run `dbms.commit()`. An `autocommit` may come in the future.

## Instanciating a new item

```
item1 = dbms.newItem(param='2004', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00')
```

This will create the following table:

```
CREATE TABLE IF NOT EXISTS public.lzdb__1
(
    id integer NOT NULL DEFAULT nextval('lzdb__1_id_seq'::regclass),
    endtime character varying,
    param character varying,
    starttime character varying,
    CONSTRAINT lzdb__1_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__1_endtime_param_starttime_key UNIQUE (endtime, param, starttime)
)
```

In the lzdb table, the following record will be inserted:
 
 * id: 1
 * ukeys: endtime,param,starttime
 
Each subsequent item created with the same virtual primary key will end up in the table with id 1.

## Cross-references between items

Let's go with an example:

```
item2 = dbms.newItem(refers=item1)
```

This will create a second table with `refers` as virtual primary key and declare the field as foreign key as follows:

```
CREATE TABLE IF NOT EXISTS public.lzdb__2
(
    id integer NOT NULL DEFAULT nextval('lzdb__2_id_seq'::regclass),
    refers integer,
    CONSTRAINT lzdb__2_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__2_refers_key UNIQUE (refers),
    CONSTRAINT lzdb__2_refers_fkey FOREIGN KEY (refers)
        REFERENCES public.lzdb__1 (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
```

The inserted record will look like the following:

 * id: 1
 * refers: 1
 
## Adding fields to an item

Now, let's attach some data to the records. There are two syntaxes possible.

The dict-way:

```
item2['clusters'] = [1,2,3]
item2['freqmap'] = [4,5,6]
```

The `set` method:

```
item2.set(clusters=[1,2,3], freqmap=[4,5,6])
```

Since the table `lzdb__2` has already been created, it will be altered with the `ADD COLUMN` statement. 

The table will then have the following definition:

```
CREATE TABLE IF NOT EXISTS public.lzdb__2
(
    id integer NOT NULL DEFAULT nextval('lzdb__2_id_seq'::regclass),
    refers integer,
    clusters character varying,
    freqmap character varying,
    CONSTRAINT lzdb__2_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__2_refers_key UNIQUE (refers),
    CONSTRAINT lzdb__2_refers_fkey FOREIGN KEY (refers)
        REFERENCES public.lzdb__1 (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
```

The record will be ***updated*** as follows:

 * id: 1
 * refers: 1
 * clusters: [1,2,3]
 * freqmap: [4,5,6]
 
If the value of an existing field is changed, the record in the database will simply be updated.
