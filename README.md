# HetioNet Project I – Multi‑Store NoSQL Implementation

This project implements the HetioNet user case using two NoSQL technologies:

- **Neo4j** as a **graph store**
- **MongoDB** as a **document store**

The implementation is driven by a single Python command‑line tool, `hetionet_cli.py`, which can:

- Create and populate the databases from `nodes.tsv` and `edges.tsv`
- Answer the two assignment queries with an emphasis on fast response time

---

## Data model

### Input files

- `nodes.tsv`
  - Columns: `id`, `name`, `kind`
  - Example rows:
    - `Gene::9997\tSC02\tGene`
    - `Compound::DB09028\tCytisine\tCompound`
- `edges.tsv`
  - Columns: `source`, `metaedge`, `target`
  - Example rows:
    - `Gene::801\tGiG\tGene::7428`
    - `Disease::DOID:263\tDuG\tGene::857`

The `kind` field in `nodes.tsv` maps directly to node labels in Neo4j and high‑level types in MongoDB:

- `Disease`
- `Compound`
- `Gene`
- `Anatomy`

The `metaedge` field encodes the semantic type of a relationship. For this project we focus on:

- **Compound → Disease**
  - `CtD`: compound **treats** disease
  - `CpD`: compound **palliates** disease
- **Disease → Anatomy**
  - `DlA`: disease **localizes** to anatomy
- **Disease → Gene**
  - `DuG`: disease **up‑regulates** gene
  - `DdG`: disease **down‑regulates** gene
  - `DaG`: disease **associates** gene
- **Compound → Gene**
  - `CuG`: compound **up‑regulates** gene
  - `CdG`: compound **down‑regulates** gene
  - `CbG`: compound **binds** gene
- **Anatomy → Gene**
  - `AuG`: anatomy **up‑regulates** gene
  - `AdG`: anatomy **down‑regulates** gene
  - `AeG`: anatomy **expresses** gene

If your actual `edges.tsv` uses slightly different metaedge codes, you can adjust the constant sets at the top of `hetionet_cli.py`.

---

## Database designs

### 1. Graph store – Neo4j

- **Node labels**
  - `Disease`, `Compound`, `Gene`, `Anatomy`
- **Node properties**
  - `id` (e.g. `Disease::DOID:263`)
  - `name`
- **Relationship type**
  - Unified type: `:HETIO { metaedge: <code> }`
  - Example: `(c:Compound)-[:HETIO {metaedge: 'CtD'}]->(d:Disease)`

This schema keeps the physical model simple while still allowing query engines to filter by `metaedge`. Indexes/constraints are created on `id` for each major label to guarantee fast lookup:

- `Disease(id)`
- `Compound(id)`
- `Gene(id)`
- `Anatomy(id)`

#### Query 2 (Neo4j)

**Assumption (project logic)**  
We assume a compound can treat a disease if:

- The disease **up‑regulates** or **down‑regulates** a gene.
- The disease **localizes** to an anatomy where that gene is active
  (connected by `AuG`, `AdG`, or `AeG`).
- The compound regulates the **same gene** in the **opposite direction**
  (disease up → compound down, disease down → compound up).
- The compound does **not already** have a `CtD` or `CpD` edge to that disease.

Cypher:

```cypher
MATCH (d:Disease {id: $disease_id})-[:HETIO {metaedge: 'DlA'}]->(a:Anatomy)

MATCH (d)-[dg:HETIO]->(g:Gene)
WHERE dg.metaedge IN ['DuG', 'DdG']

MATCH (a)-[ag:HETIO]->(g)
WHERE ag.metaedge IN ['AuG', 'AdG', 'AeG']

MATCH (c:Compound)-[cg:HETIO]->(g)
WHERE cg.metaedge IN ['CuG', 'CdG']

WITH
  d, c,
  CASE WHEN dg.metaedge = 'DuG' THEN 1 ELSE -1 END AS disease_sign,
  CASE WHEN cg.metaedge = 'CuG' THEN 1 ELSE -1 END AS compound_sign

WHERE disease_sign = -compound_sign

OPTIONAL MATCH (c)-[cd:HETIO]->(d)
WITH d, c, collect(cd.metaedge) AS existing_edges
WHERE NONE(x IN existing_edges WHERE x IN ['CtD', 'CpD'])

RETURN DISTINCT
  c.id   AS compound_id,
  c.name AS compound_name
ORDER BY compound_name;
```

This query exactly matches the textual requirement: it identifies **new** compounds that could treat the disease based on opposite regulation at the disease‑specific anatomical locations, and filters out compounds that already treat or palliate it.

---

### 2. Document store – MongoDB

MongoDB is used to speed up Query 1 by pre‑aggregating all required information into a single document per disease. This demonstrates a second NoSQL type (document store) and shows how denormalization can improve query performance.

- **Database**: `hetionet`
- **Collection**: `diseases`
- **Document structure**:

```json
{
  "_id": "Disease::DOID:263",
  "name": "Disease name",
  "drugs": [
    { "id": "Compound::DB09028", "name": "Cytisine", "relation": "CtD" }
  ],
  "genes": [
    { "id": "Gene::857", "name": "SC02", "relation": "DuG" }
  ],
  "anatomies": [
    { "id": "Anatomy::123", "name": "Brain" }
  ]
}
```

These documents are built directly from `nodes.tsv` and `edges.tsv` by `build-mongo` in `hetionet_cli.py`. An index is created on `name` for optional lookup by disease name.

#### Query 1 (MongoDB)

Query 1 can now be implemented as a single document lookup:

```python
db.diseases.find_one({"_id": "<disease_id>"})
```

This returns, in one network round‑trip, the disease name, all drug names (treat/palliate), all gene names, and all anatomy locations, matching the assignment requirement.

---

## Python command‑line interface

The CLI is implemented in `hetionet_cli.py`. It expects `nodes.tsv` and `edges.tsv` to be accessible on disk.

### Installation

From the project directory:

```bash
pip install -r requirements.txt
```

Ensure you have:

- A running **Neo4j** instance (default `bolt://localhost:7687`)
- A running **MongoDB** instance (default `mongodb://localhost:27017`)

Optionally configure environment variables:

- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`
- `MONGODB_URI`, `MONGODB_DB`

### Load data into Neo4j (graph store)

```bash
python hetionet_cli.py load-neo4j --nodes nodes.tsv --edges edges.tsv
```

This:

- Creates uniqueness constraints on node `id`
- Loads all nodes with appropriate labels
- Loads all edges as `:HETIO` relationships with a `metaedge` property

### Query 2 via Neo4j

```bash
python hetionet_cli.py q2-neo4j --disease-id Disease::DOID:263
```

Outputs a list of **candidate compounds** that could treat the disease under the up/down regulation assumption, excluding existing treating/palliating drugs.

### Build MongoDB documents

```bash
python hetionet_cli.py build-mongo --nodes nodes.tsv --edges edges.tsv
```

This:

- Reads `nodes.tsv` and `edges.tsv`
- Builds one denormalized document per disease
- Stores them in `hetionet.diseases`

### Query 1 via MongoDB

```bash
python hetionet_cli.py q1-mongo --disease-id Disease::DOID:263
```

This executes Query 1 as a single document lookup in MongoDB for fast response.

---

## Potential improvements (for the report)

- **Index tuning**
  - Add Neo4j indexes on additional properties if needed (e.g. `name`).
  - Add MongoDB compound indexes on frequently queried fields beyond `_id`.
- **Caching**
  - Cache results of expensive graph queries (especially Query 2) in a key‑value store such as Redis.
- **Pre‑computation of candidate drugs**
  - Periodically pre‑compute the candidate compound set for each disease and store the results in a separate MongoDB collection to make Query 2 constant‑time.
- **Sharding / replication**
  - For very large HetioNet instances, enable Neo4j clustering and MongoDB sharding to improve fault tolerance and horizontal scalability.

These points can be used directly in the project document under "Potential improvements (e.g. how to speed up query)".