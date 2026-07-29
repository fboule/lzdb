# Contributing to lzdb

Thank you for your interest in contributing to **lzdb** — a schema-emergent database layer built around the principles of **Lazy Data Modeling**.

This project welcomes contributions of all kinds: bug fixes, new features, documentation improvements, examples, and conceptual discussions.

Before contributing, please take a moment to understand the philosophy behind lzdb:

See **[Lazy Data Modeling](MANIFESTO.md)**

---

## 1. How to Contribute

There are several ways to contribute to lzdb:

### Code contributions
- Fix bugs or edge cases  
- Improve schema evolution logic  
- Enhance relationship handling (`lzdb_links`)  
- Add new convenience APIs (`lzitem`, `lzdict`, etc.)  
- Improve PostgreSQL compatibility or performance  

### Documentation contributions
- Improve README clarity  
- Add examples demonstrating real-world usage  
- Expand conceptual explanations  
- Add diagrams or workflow descriptions  

### Conceptual contributions
lzdb is also a research project. Contributions may include:
- Proposals for new Lazy Data Modeling principles  
- Discussions about schema emergence  
- Notes on epistemology of exploratory programming  
- Alternative approaches to virtual primary keys  

---

## 2. Development Environment

### Requirements
- Python 3.10+  
- PostgreSQL (local or remote)  
- `psycopg`  
- Standard build tools (`build`, `pip`, etc.)

### Setup

Clone the repository:

```bash
git clone https://github.com/fboule/lzdb2
cd lzdb2
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Build the package:

```bash
rm -rf dist
python3 -m build
pip install dist/*.whl --force-reinstall --no-deps
```

---

## 3. Coding Guidelines

### Style
- Follow standard Python style (PEP8)  
- Keep functions small and focused  
- Avoid unnecessary abstraction  
- Prefer clarity over cleverness  

### Philosophy alignment
lzdb is not a traditional ORM or database layer.  
When contributing, keep these principles in mind:

- **Structure follows discovery**  
- **Schema evolves lazily**  
- **Relationships should not require migrations**  
- **Virtual primary keys must remain simple and predictable**  
- **Cross-references modify schema; relationships do not**  

If a feature violates these principles, open a discussion first.

---

## 4. Testing

lzdb relies heavily on PostgreSQL behavior.  
Tests should:

- Use a real PostgreSQL instance  
- Avoid mocking database internals  
- Cover schema evolution scenarios  
- Cover relationship creation and traversal  
- Cover virtual primary key inference  
- Cover lazy field addition (`ALTER TABLE ADD COLUMN`)  

A typical test workflow:

```bash
pytest -v
```

If your contribution affects schema evolution, add tests for:

- newItem() behavior  
- virtual primary key inference  
- cross-reference creation  
- lzdb_links consistency  
- commit() behavior  

---

## 5. Submitting Changes

### 1. Fork the repository
Create your own fork on GitHub.

### 2. Create a feature branch
```bash
git checkout -b feature/my-change
```

### 3. Commit your changes
Use clear commit messages:

```
Fix: handle duplicate virtual primary keys
Add: support for undirected relationship traversal
Doc: improve README examples
```

### 4. Push your branch
```bash
git push origin feature/my-change
```

### 5. Open a Pull Request
Describe:
- what the change does  
- why it is needed  
- how it aligns with Lazy Data Modeling  
- any tests added  

---

## 6. Discussion and Design Proposals

For conceptual or architectural changes, open a discussion first.

Examples:
- new relationship types  
- alternative virtual primary key strategies  
- autocommit semantics  
- schema evolution heuristics  
- new convenience APIs  

Use the GitHub Discussions tab.

---

## 7. Code of Conduct

Be respectful, constructive, and curious.  
lzdb is a research-driven project — disagreement is welcome, hostility is not.

---

## 8. License

By contributing, you agree that your contributions will be licensed under the MIT License.

