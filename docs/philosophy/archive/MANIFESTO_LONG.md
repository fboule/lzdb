# Lazy Data Modeling Manifesto

*A declaration for schema-emergent data systems*

## Preface

This manifesto describes the philosophy that led to the creation of lzdb and the broader idea of Lazy Data Modeling.

It is not merely a software design document. It is an argument about how knowledge, uncertainty, discovery, and data systems should interact.

---

# 1. Historical Context

The history of databases is largely the history of increasing structure.

Early systems were hierarchical. Data was stored in trees. Relationships were predefined and rigid.

Relational databases introduced a major breakthrough. They separated data from physical storage and provided a powerful mathematical framework for organizing information.

For decades the relational model dominated computing.

As software systems grew larger, database design increasingly became associated with:

- upfront schema design
- normalization
- constraints
- migrations
- governance
- institutional control

These approaches worked extraordinarily well in mature domains.

Banking systems.

Accounting systems.

Inventory management.

Telecommunications.

Where the structure of reality was already reasonably understood.

But a different class of problems emerged.

Scientific research.

Machine learning.

Exploratory analytics.

Knowledge discovery.

Rapid prototyping.

These activities do not begin with stable knowledge.

They begin with questions.

The traditional database workflow became increasingly misaligned with exploratory work.

---

# 2. The Problem of Premature Structure

Most database methodologies implicitly assume certainty.

The developer is expected to know:

- what entities exist
- how entities relate
- which fields matter
- which constraints should hold
- which relationships are important

before significant data exists.

In exploratory environments this assumption is often false.

Researchers frequently learn new things only after collecting data.

The schema evolves because understanding evolves.

Traditional systems treat this as an operational problem.

Lazy Data Modeling treats it as the normal condition of discovery.

---

# 3. Exploration as a First-Class Activity

Exploration is not a failure to design.

Exploration is a legitimate state of knowledge.

A scientist observing a new phenomenon does not know the schema.

An analyst exploring unfamiliar data does not know the schema.

A startup validating an idea does not know the schema.

The system should therefore support uncertainty rather than attempting to eliminate it.

---

# 4. The Central Principle

> Store first. Understand later.

The purpose of a data system is not to enforce assumptions.

The purpose of a data system is to preserve knowledge while understanding evolves.

Structure should emerge from observation.

Structure should not be demanded beforehand.

---

# 5. Emergent Collections

In lzdb, collections emerge from object structure.

Objects sharing the same structure belong to the same collection.

This eliminates the need to define tables before storing information.

Collections become observations about data rather than declarations about data.

---

# 6. Collection Signatures

A collection is described by a collection signature.

Example:

```text
endtime,param,starttime
```

The signature defines organization.

It does not define uniqueness.

It does not define identity.

It merely groups structurally similar observations.

This distinction is fundamental.

---

# 7. Identity Is Not Structure

For many years lzdb experimented with the notion of virtual primary keys.

Experience ultimately demonstrated that structure and identity are separate concepts.

Two observations may have identical structure and identical values.

They may still represent two different observations.

Modern lzdb therefore adopts a simpler rule:

> Structure determines collections.
>
> IDs determine identity.

Duplicate rows are allowed.

Identity is provided by PostgreSQL-generated identifiers.

---

# 8. Structural Relationships

Some relationships define the structure of an object.

Example:

```python
event = lzitem(
    satellite=satellite
)
```

This relationship is fundamental to the event.

It becomes a foreign key.

Such relationships become part of the evolving schema.

---

# 9. Semantic Relationships

Not all relationships define structure.

Many relationships communicate meaning.

Example:

```python
satellite.link(measurement)
```

This does not redefine either object.

Instead it expresses a semantic connection.

These relationships are stored in `lzdb_links`.

The distinction between structural and semantic relationships is one of the most important ideas in modern lzdb.

---

# 10. Schema Evolution

Understanding changes.

The database should adapt.

New fields should not require migration projects.

New ideas should not require redesign sessions.

When new information appears:

```python
satellite['operator'] = 'ESA'
```

structure evolves automatically.

Knowledge produces schema.

Not the reverse.

---

# 11. Stable Discovery

Exploratory environments often contain duplicates.

The goal is not necessarily to eliminate them.

The goal is deterministic access.

For this reason lzdb provides:

```python
dbms.ensure(...)
```

which offers stable retrieval while preserving flexibility.

---

# 12. Explicit Persistence

Nothing is committed automatically.

```python
dbms.commit()
```

remains the moment at which exploration becomes recorded fact.

This preserves a clear boundary between experimentation and persistence.

---

# 13. The Ethics of Uncertainty

Many software systems treat uncertainty as an error.

Lazy Data Modeling rejects this mindset.

Uncertainty is not a defect.

It is a natural part of learning.

Systems should support:

- revision
- reinterpretation
- discovery
- curiosity

without imposing unnecessary friction.

---

# 14. Current lzdb Dogma

1. Collections emerge from structure.
2. IDs define identity.
3. Duplicate rows are allowed.
4. Object references become foreign keys.
5. Semantic relationships belong in `lzdb_links`.
6. Schemas evolve automatically.
7. Persistence occurs only through `commit()`.
8. Understanding is expected to evolve.

---

# 15. Future Directions

Lazy Data Modeling is not limited to PostgreSQL.

Nor is it limited to lzdb.

The broader goal is a class of data systems that:

- adapt automatically
- emerge from usage
- embrace uncertainty
- reduce friction
- preserve freedom of exploration

lzdb is one implementation.

The philosophy is larger than the implementation.

---

# Call to Action

Challenge the assumption that schemas must come first.

Challenge the assumption that uncertainty is failure.

Allow structure to emerge.

Allow understanding to evolve.

Let structure follow discovery.

---

# License

Copyright (C) 2026 Fabien Bouleau

This document is part of the lzdb project.

Licensed under the GNU General Public License v3.0 or later.
