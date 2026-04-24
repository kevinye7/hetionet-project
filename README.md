# HetioNet Project I

HetioNet is modeled with **two NoSQL stores**: Neo4j (graph) and MongoDB (documents). The CLI loads `nodes.tsv` and `edges.tsv`, then answers the two required queries. The GUI calls the same backends (Query 1 → MongoDB, Query 2 → Neo4j).

**Files:** `hetionet_cli.py`, `hetionet_gui.py`, `requirements.txt`, `nodes.tsv`, `edges.tsv`.

---

## Design overview

- **Neo4j:** Full graph — all node kinds and all edges. Relationship **types** match HetioNet `metaedge` codes (e.g. `:CtD`, `:DlA`, `:CuG`). Used for **Query 2** (multi-hop graph pattern).
- **MongoDB:** Denormalized **one document per disease** with nested lists for drugs, genes, and anatomies. Used for **Query 1** (single `findOne` by disease id — fast).

Rough flow: TSV files → `load-neo4j` / `build-mongo` → databases → `q1-mongo` / `q2-neo4j` or GUI.

---

## Input data

- **`nodes.tsv`** — columns: `id`, `name`, `kind`
  - Example: `Gene::9997`, `SC02`, `Gene`
- **`edges.tsv`** — columns: `source`, `metaedge`, `target`
  - Example: `Disease::DOID:263`, `DuG`, `Gene::857`

**Node kinds** used as Neo4j labels / Mongo types: `Disease`, `Compound`, `Gene`, `Anatomy`.

**Relationship codes** (subset; full graph loads every `metaedge` in the TSV):

- **Compound → Disease:** `CtD` (treats), `CpD` (palliates)
- **Disease → Anatomy:** `DlA` (localizes)
- **Disease → Gene:** `DuG`, `DdG`, `DaG` (up / down / associate) — used in Mongo Query 1
- **Compound → Gene:** `CuG`, `CdG` (up / down) — used in Neo4j Query 2
- **Anatomy → Gene:** `AuG`, `AdG` (up / down) — used in Neo4j Query 2

---

## Neo4j (graph store)

- **Nodes:** label = `kind`; properties `id`, `name`
- **Relationships:** type = `metaedge` string from TSV (not a single generic type)
- **Constraints:** unique `id` on `Disease`, `Compound`, `Gene`, `Anatomy`

**Load:** `python hetionet_cli.py load-neo4j --nodes nodes.tsv --edges edges.tsv`  
Creates constraints, merges nodes in batches, merges edges in batches (grouped by relationship type).

**Query 2** — Given a disease id, find compounds that could treat it as _new_ drugs: disease localizes to anatomy; anatomy up/down-regulates a gene; compound regulates that gene in the **opposite** direction; exclude compounds that already have `CtD` or `CpD` to that disease. One Cypher query:

```cypher
MATCH (d:Disease {id: $disease_id})-[:DlA]->(a:Anatomy)
MATCH (a)-[ag:AuG|AdG]->(g:Gene)
MATCH (c:Compound)-[cg:CuG|CdG]->(g)
WITH d, c,
  CASE WHEN type(ag) = 'AuG' THEN 1 ELSE -1 END AS anatomy_sign,
  CASE WHEN type(cg) = 'CuG' THEN 1 ELSE -1 END AS compound_sign
WHERE anatomy_sign = -compound_sign
  AND NOT (c)-[:CtD|CpD]->(d)
RETURN DISTINCT c.id AS compound_id, c.name AS compound_name
ORDER BY compound_name;
```

If you previously loaded with a different edge model, delete Neo4j’s `data` folder and run `load-neo4j` again.

---

## MongoDB (document store)

- **Database:** `hetionet` (override with `MONGODB_DB` in `.env`)
- **Collection:** `diseases`
- **Build:** `python hetionet_cli.py build-mongo --nodes nodes.tsv --edges edges.tsv`

**Document shape** (one per disease):

```json
{
  "_id": "Disease::DOID:263",
  "name": "Disease name",
  "drugs": [
    { "id": "Compound::DB09028", "name": "Cytisine", "relation": "CtD" }
  ],
  "genes": [{ "id": "Gene::857", "name": "SC02", "relation": "DuG" }],
  "anatomies": [{ "id": "Anatomy::UBERON:...", "name": "Brain" }]
}
```

**Query 1** — Given a disease id, return in one read: disease name, drugs that treat or palliate, associated genes, anatomies where the disease localizes:

```python
db.diseases.find_one({"_id": "<disease_id>"})
```

---

## Setup

```bash
pip install -r requirements.txt
```

Optional `.env`:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=hetionet
```

Start **Neo4j** and **MongoDB** before loading or querying.

## Commands

```bash
python hetionet_cli.py load-neo4j --nodes nodes.tsv --edges edges.tsv
python hetionet_cli.py q2-neo4j --disease-id Disease::DOID:10283

python hetionet_cli.py build-mongo --nodes nodes.tsv --edges edges.tsv
python hetionet_cli.py q1-mongo --disease-id Disease::DOID:10283

python hetionet_gui.py
```

---

## Project 2 — MapReduce (PySpark)

**File:** `mapreduce_queries.py`  
**Run:** `python3 mapreduce_queries.py` (output also saved to `mapreduce_output.txt`)

---

### Design Pattern

All three queries use the **Aggregation / Counting** pattern as their core: each mapper tags individual edges with a key and a partial count, and each reducer sums those counts per key. Q2 chains two jobs because the question has two levels of grouping. Q3 uses the **Replicated Join** pattern -- the small `nodes.tsv` name table is loaded into memory and broadcast to all workers rather than shuffled through the network.

---

### Pseudo Code

#### Q1 — Count genes and diseases per drug (single MapReduce job)

```
MAPPER(edge: source, metaedge, target):
  if source starts with "Compound::":
    if metaedge in {CbG, CuG, CdG}:
      emit(source, (1, 0))        // gene hit
    elif metaedge in {CtD, CpD}:
      emit(source, (0, 1))        // disease hit

REDUCER(compound_id, values: list of (g, d)):
  gene_count    = sum of all g
  disease_count = sum of all d
  emit(compound_id, gene_count, disease_count)

POST: sort by gene_count descending, output top 5
```

#### Q2 : Count diseases grouped by how many drugs treat them (two chained jobs)

```
JOB 1 MAPPER(edge: source, metaedge, target):
  if source starts with "Compound::" and metaedge in {CtD, CpD}:
    emit(target, source)          // key = disease_id, value = compound_id

JOB 1 REDUCER(disease_id, compound_ids):
  drug_count = count of distinct compound_ids
  emit(disease_id, drug_count)

JOB 2 MAPPER(disease_id, drug_count):
  emit(drug_count, 1)             // key = #drugs, value = 1 disease

JOB 2 REDUCER(drug_count, ones):
  disease_count = sum(ones)
  emit(drug_count, disease_count)

POST: sort by disease_count descending, output top 5
```

#### Q3 — Resolve top-5 drug IDs to human-readable names (replicated join)

```
SETUP:
  load nodes.tsv into memory → name_lookup = {compound_id: name}
  broadcast name_lookup to all workers

MAPPER(compound_id, gene_count, disease_count):   // input = Q1 top-5 results
  name = name_lookup[compound_id]
  emit(name, gene_count)

REDUCER: pass-through (order already established by Q1)
```

---

### Results

**Q1 — Top 5 drugs by gene count**

| Drug ID           | #Genes | #Diseases |
| ----------------- | -----: | --------: |
| Compound::DB08865 |    585 |         1 |
| Compound::DB01254 |    564 |         1 |
| Compound::DB00997 |    532 |        17 |
| Compound::DB00570 |    523 |         7 |
| Compound::DB00390 |    522 |         2 |

**Q2 — Diseases grouped by number of treating drugs (top 5 groups)**

| #Drugs | #Diseases |
| -----: | --------: |
|      1 |        10 |
|      2 |         7 |
|      3 |         6 |
|     11 |         6 |
|      9 |         6 |

**Q3 — Top 5 drug names by gene count**

| Drug Name   | #Genes |
| ----------- | -----: |
| Crizotinib  |    585 |
| Dasatinib   |    564 |
| Doxorubicin |    532 |
| Vinblastine |    523 |
| Digoxin     |    522 |

---

## Potential improvements

- Index `name` (or other fields) in Neo4j / MongoDB if you add name-based search.
- Cache Query 2 results (e.g. Redis) or precompute candidate compounds per disease in MongoDB.
- For huge graphs: Neo4j clustering, MongoDB sharding/replication.
