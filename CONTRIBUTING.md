# Contributing to lzdb

Thank you for your interest in contributing to **lzdb**.

lzdb is both:

- a software project
- an experiment in **Lazy Data Modeling**

The project explores the idea that structure should emerge from data rather than be imposed before data exists.

Before contributing, please read:

- README.md
- MANIFESTO.md

Understanding the philosophy behind the project will help ensure that contributions remain aligned with its goals.

---

# 1. Ways to Contribute

## Code Contributions

Examples:

- Bug fixes
- Performance improvements
- PostgreSQL compatibility enhancements
- Querying improvements
- Foreign-key handling improvements
- Relationship handling improvements
- Testing improvements
- Internal refactoring
- Documentation tooling

## Documentation Contributions

Examples:

- README improvements
- New tutorials
- Usage examples
- Architectural documentation
- API documentation
- Diagrams and workflows

## Conceptual Contributions

lzdb is also a research project.

Examples:

- Collection signature strategies
- Schema-emergent systems
- Structural vs semantic relationship models
- Exploratory persistence systems
- Epistemology of exploratory programming
- Lazy Data Modeling principles
- Future directions for schema emergence

---

# 2. Development Environment

## Requirements

- Python 3.10+
- PostgreSQL
- psycopg
- build
- pytest

## Clone the Repository

```bash
git clone https://github.com/fboule/lzdb2
cd lzdb2
```

## Install Development Dependencies

```bash
pip install -r requirements.txt
```

## Build the Package

```bash
rm -rf dist build *.egg-info
python -m build
```

Install the generated wheel:

```bash
pip install dist/*.whl --force-reinstall --no-deps
```

---

# 3. Design Principles

## Structure Follows Discovery

Structure should emerge from actual usage.

## Collections Emerge From Structure

Objects with the same structure belong to the same collection.

## IDs Define Identity

Object identity is determined by database-generated identifiers.

## Duplicate Observations Are Allowed

lzdb does not attempt to eliminate duplicates automatically.

## Structural Relationships Become Foreign Keys

Object references become foreign keys.

## Semantic Relationships Belong In lzdb_links

Semantic relationships should be stored in `lzdb_links`.

## Explicit Persistence

Persistence remains explicit through:

```python
dbms.commit()
```

---

# 4. Coding Guidelines

- Follow PEP 8
- Prefer readability over cleverness
- Avoid unnecessary abstraction
- Keep functions focused

Large refactorings should preserve behavior and be accompanied by tests.

---

# 5. Testing

Run:

```bash
pytest -v
```

Tests should cover:

- Collections
- Collection signatures
- Persistence
- Foreign keys
- Links
- ensure()
- Schema evolution

Any bug fix or new feature should include tests whenever practical.

---

# 6. Pull Requests

Create a branch:

```bash
git checkout -b feature/my-change
```

Use clear commit messages.

Explain:

- What changed
- Why it changed
- How it aligns with Lazy Data Modeling
- What tests were added

---

# 7. Design Discussions

Please open a discussion before implementing major conceptual changes.

Examples:

- New relationship types
- Autocommit behavior
- Collection-signature strategies
- Persistence semantics

---

# 8. Philosophy Before Features

Ask:

- Does it reduce friction?
- Does it support exploration?
- Does it preserve schema emergence?
- Does it align with the manifesto?

---

# 9. Code of Conduct

Be respectful.

Be constructive.

Be curious.

Challenge ideas, not people.

---

# 10. License

By contributing to lzdb, you agree that your contributions will be licensed under the same license as the project:

**GNU General Public License v3.0 or later (GPL-3.0-or-later).**
