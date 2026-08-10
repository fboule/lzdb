# The Lazy Data Modeling Manifesto v1.0
*A founding document for schema-emergent data systems*

## Preamble

For decades, software systems have been built upon a largely unquestioned assumption:

> **Before data can be stored, its structure must be known.**

This assumption shaped database design, software architecture, and even the mental models developers use to understand information. But when exploring new domains, structure is not known in advance — it emerges through observation.

Lazy Data Modeling rejects premature structural commitments. It treats data as empirical evidence and structure as a hypothesis that evolves.

---

## Article I — The Right to Explore

Exploration is not a failure to design.  
It is a legitimate stage of understanding.

Systems must allow developers to ingest data *before* deciding what the data “is.”  
Structure should follow insight, not precede it.

---

## Article II — Data Comes Before Structure

Incoming payloads are observations.  
They should not be forced into predefined shapes.

Schemas are not prerequisites for storage.  
They are *consequences* of understanding.

---

## Article III — Schemas Are Theories

A schema is a theory about how the world is organized.

Like all theories, schemas should be allowed to evolve, refine, or collapse as new evidence arrives.  
A schema that cannot change is not a model — it is a constraint.

---

## Article IV — Collections Emerge

Collections should not be declared upfront.  
They should emerge from recurring patterns in the data.

If items share identity shapes, they belong together.  
If they diverge, collections should split naturally.

---

## Article V — Identity and Structure Are Different

Identity is not structure.

Identity answers: *“What makes this item itself?”*  
Structure answers: *“How is this item organized?”*

Confusing these two leads to brittle systems.

---

## Article VI — Duplicate Observations Are Not a Defect

Multiple observations of the same entity are not errors.  
They are evidence.

Systems must preserve duplicates until identity can be inferred.  
Premature deduplication destroys information.

---

## Article VII — Structural Relationships

Structural relationships deserve structural representation.

If two items consistently reference each other, the system should infer a relationship — even if no foreign key was declared.

These relationships are structural, not semantic.

---

## Article VIII — Semantic Relationships

Semantic relationships belong outside the structural layer.

They are interpretations, not constraints.  
They should be stored explicitly and separately.

---

## Article IX — Evolution Is Normal

Domains evolve.  
Schemas must evolve with them.

A system that treats schema change as an anomaly will resist learning.  
A system that embraces evolution will reveal structure.

---

## Article X — Discovery Must Be Cheaper Than Migration

Discovery is continuous.  
Migration should be rare.

If discovering new structure requires expensive manual migrations, the system discourages exploration.

Lazy Data Modeling demands that discovery be cheap.

---

## Article XI — Explicit Persistence

Systems must distinguish between:

- *observations* (incoming payloads), and  
- *interpretations* (derived structure).

Both deserve explicit persistence.

---

## Article XII — The Ethics of Knowledge

Premature structure is a form of epistemic overreach.

We must not impose certainty where none exists.  
We must allow data to speak before we decide what it means.

Lazy Data Modeling is an ethical stance:  
**understanding should emerge from evidence, not assumption.**

---

# The LZDB Dogma

1. Data precedes structure.  
2. Identity precedes schema.  
3. Collections emerge.  
4. Structure evolves.  
5. Semantic relationships belong in `lzdb_links`.  
6. Observations are sacred.  
7. Migrations are optional.  
8. Understanding is iterative.  
9. Structure is a hypothesis.  
10. Evidence wins.

---

# A Declaration

We declare that data modeling must be inductive, not prescriptive.  
We declare that schemas must emerge from observation.  
We declare that exploration is a first-class activity.  
We declare that premature engineering is harmful.  
We declare that understanding is a process, not a prerequisite.

Lazy Data Modeling is not a technique.  
It is a philosophy of knowledge.

---

# License

This manifesto is released under the GNU Free Documentation License, Version 1.3 (GFDL 1.3), with no Invariant Sections, no Front-Cover Texts, and no Back-Cover Texts.

You are free to copy and redistribute this document under the terms of the GFDL.
