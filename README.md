# lzdb

*A schema-emergent database layer for Python and PostgreSQL.*

## Overview

`lzdb` (Lazy Database) is a Python persistence engine designed for environments where domain structures are fluid or unknown. Instead of requiring predefined tables, primary keys, foreign keys, and DDL migrations up front, `lzdb` ingests raw objects, evaluates structural similarities at runtime, and allows schemas and relationships to emerge dynamically within PostgreSQL.

The core guiding principle is:

> **Store first. Model later.**

### Primary Use Cases

* **Exploratory Data Analysis & Research:** Persisting heterogeneous telemetry or experimental datasets without manual table creation.
* **Rapid Prototyping:** Accelerating early-stage software development by eliminating upfront database migrations.
* **ETL & Data Staging:** Ingesting chaotic third-party API payloads while preserving human-interpretable schemas.
* **Evolving Knowledge Domains:** Supporting application states where entities gain new attributes and relationships iteratively over time.

---

## Core Architecture & Concepts

* **Emergent Collections:** Objects sharing the exact same key topography naturally belong to the same collection. Membership is determined by a generated *collection signature* (e.g., `endtime,param,starttime`).
* **Non-Enforced Uniqueness:** Structural parity does not imply duplicate rejection. `lzdb` allows duplicate rows and relies on PostgreSQL-generated IDs for physical identity.
* **Automatic Schema Evolution:** Adding a new key-value pair to a runtime object dynamically expands the target PostgreSQL table on `commit()`.
* **Structural vs. Semantic Relationships:** Direct object references automatically instantiate PostgreSQL foreign keys. Arbitrary semantic connections are stored as graph edges in `lzdb_links`.

---

## Installation

### Install via PyPI

```bash
pip install lzdb2
```

### Build and Install from Source

```bash
python -m build
pip install dist/*.whl
```

---

## Getting Started

### Database Connection & Initialization

`lzdb` wraps a standard `psycopg` connection:

```python
import psycopg as pg
from lzdb import LZDB

conn = pg.connect(
    dbname="exploratory_db",
    user="postgres",
    password="secret",
    host="localhost",
    port=5432
)

# Initialize LZDB instance
dbms = LZDB(conn, traceon=False)
```

Enable SQL tracing to monitor dynamic table creation and DDL queries:

```python
dbms = LZDB(conn, traceon=True)
```

### Exposing Namespace Helpers

To streamline script development, expose global helper shortcuts (`lzitem`, `lzitems`, `lzc`, `lzcnames`, `ld`, `pp`) directly into your current namespace:

```python
dbms.expose()

# Now available directly without 'dbms.' prefix
sat = lzitem(name="SAT-1")
```

---

## Object Management & Persistence

### Creating Objects

Objects can be instantiated via the `LZDB` instance or via global helpers:

```python
# Via DBMS instance
sat = dbms.newItem(name="SAT-1", orbit="LEO")

# Via global helper (requires dbms.expose())
sat = lzitem(name="SAT-1", orbit="LEO")
```

Objects remain in local memory until explicitly persisted:

```python
dbms.commit()
```

### Deterministic Object Retrieval with `ensure()`

To prevent unwanted duplicate rows when querying known objects, use `ensure()`. It retrieves an existing record matching the provided key-value pairs or creates one if no match exists:

```python
# Creates object if missing; returns oldest match if multiple exist
sat = dbms.ensure(name="SAT-1")
```

---

## Relationships & Schema Evolution

### Structural Foreign Keys

Passing an `lzdb` object as an attribute value automatically triggers foreign key generation in PostgreSQL:

```python
sat = lzitem(name="SAT-1")

measurement = lzitem(
    satellite=sat,
    timestamp="2026-08-09T10:00:00Z",
    val=42.8
)

dbms.commit()
```

When reloaded, `measurement["satellite"]` automatically resolves and returns the referenced `sat` object.

### Schema Evolution

Attributes can be appended dynamically at runtime without executing manual DDL migrations:

```python
# Append new fields on the fly
sat["operator"] = "ESA"
sat["launch_year"] = 2024

# Automatically alters the underlying PostgreSQL table schema
dbms.commit()
```

### Graph & Semantic Links

For directed or undirected semantic relationships that exist outside physical table structures, use `.link()`:

```python
# Single link
sat.link(measurement)

# Bulk linking
sat.link([measurement1, measurement2, measurement3])

# Undirected semantic relationship
sat.link(measurement, LZDB_REL_UNDIRECTED)

dbms.commit()
```

Semantic links are stored in the reserved system table `lzdb_links`. Query connected items using:

```python
for item in dbms.linkedItems(sat):
    print(item)
```

---

## Querying

`lzdb` provides a unified query interface across inferred collections:

```python
# Retrieve all stored items across all collections
all_items = dbms.items()

# Filter by exact attribute values
sensor_data = dbms.items(param="2004")

# Filter by target collection reference
collection_items = dbms.items(collection=my_collection)
```

---

## Lazy Parquet Integration (`lzdict`)

For large analytical files, `lzdict` provides lazy-loading wrappers around local Parquet storage:

```python
ld = lzdict()

# Loads and parses Parquet file contents on access
df = ld["PQTFILE"]
```

---

## The Lazy Data Modeling Dogma

1. **Data precedes structure:** Payloads are empirical evidence, not conformance checks.
2. **IDs define physical identity:** System-assigned keys separate identity from structural layout.
3. **Duplicate rows are valid:** Observations are preserved prior to statistical deduplication.
4. **Object references become foreign keys:** Structural dependencies create schema constraints automatically.
5. **Semantic relationships belong in `lzdb_links`:** Graph linkages remain decoupled from tabular column definitions.
6. **Schemas evolve automatically:** Table shapes adapt inductively as fields are appended.
7. **Explicit persistence:** State changes require an explicit `commit()` call.
8. **Evolving history:** Existing objects and collection structures remain adaptable as understanding grows.

---

## Complete Workflow Example

```python
import psycopg as pg
from lzdb import LZDB

# 1. Connect and initialize
conn = pg.connect(dbname="test_db", host="localhost")
dbms = LZDB(conn)
dbms.expose()

# 2. Ingest structured payloads
measurements = [
    lzitem(param="2004", val=10.1),
    lzitem(param="2005", val=12.4),
    lzitem(param="2006", val=11.8)
]

sat = lzitem(name="SAT-1")

# 3. Create semantic graph links
sat.link(measurements)

# 4. Evolve item schema dynamically
sat["operator"] = "ESA"
sat["status"] = "ACTIVE"

# 5. Persist to PostgreSQL
dbms.commit()

# 6. Traversal
print(f"Satellite: {sat['name']} ({sat['operator']})")
print("Linked Measurements:")
for item in dbms.linkedItems(sat):
    print(f" - Param: {item['param']}, Val: {item['val']}")
```

---

## Testing

Run the full regression test suite using `pytest`:

```bash
pytest -v
```

The test suite validates:
* Collection creation and signature matching
* Foreign key creation and auto-reloading
* Single and bulk semantic link traversals (directed/undirected)
* Automatic table schema evolution on `commit()`
* Deterministic behavior of `ensure()`

---

## License

Distributed under the **GNU General Public License v3.0 or later (GPL-3.0-or-later)**. See `LICENSE` for full details.
