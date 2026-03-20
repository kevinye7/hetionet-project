#!/usr/bin/env python
import argparse
import csv
import os
from dataclasses import dataclass
from typing import Dict, List, Set

from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymongo import MongoClient

load_dotenv()


# Shared constants

TREAT_EDGES: Set[str] = {"CtD", "CpD"}
DISEASE_GENE_EDGES: Set[str] = {"DuG", "DdG", "DaG"}


# Neo4j (graph store)

class Neo4jBackend:
    def __init__(
        self,
        uri: str = None,
        user: str = None,
        password: str = None,
    ) -> None:
        uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = user or os.getenv("NEO4J_USER", "neo4j")
        password = password or os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(
            uri, auth=(user, password), connection_timeout=60
        )

    def close(self) -> None:
        self.driver.close()

    def init_schema(self) -> None:
        stmts = [
            "CREATE CONSTRAINT disease_id IF NOT EXISTS "
            "FOR (d:Disease) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT compound_id IF NOT EXISTS "
            "FOR (c:Compound) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT gene_id IF NOT EXISTS "
            "FOR (g:Gene) REQUIRE g.id IS UNIQUE",
            "CREATE CONSTRAINT anatomy_id IF NOT EXISTS "
            "FOR (a:Anatomy) REQUIRE a.id IS UNIQUE",
        ]
        with self.driver.session() as session:
            for stmt in stmts:
                session.run(stmt)

    def load_data(self, nodes_path: str, edges_path: str) -> None:
        BATCH_NODES = 5000
        BATCH_EDGES = 25000

        self.init_schema()
        with self.driver.session() as session:
            # Nodes by label
            nodes_by_label: Dict[str, List[Dict]] = {}
            with open(nodes_path, newline="", encoding="utf-8") as f_nodes:
                reader = csv.DictReader(f_nodes, delimiter="\t")
                for row in reader:
                    label = row.get("kind", "")
                    if label not in {"Disease", "Gene", "Compound", "Anatomy"}:
                        label = "Other"
                    nodes_by_label.setdefault(label, []).append(
                        {"id": row["id"], "name": row.get("name", "")}
                    )
            for label, rows in nodes_by_label.items():
                for i in range(0, len(rows), BATCH_NODES):
                    batch = rows[i : i + BATCH_NODES]
                    session.run(
                        f"""
                        UNWIND $batch AS row
                        MERGE (n:{label} {{id: row.id}})
                        SET n.name = row.name
                        """,
                        batch=batch,
                    )

            # Edges
            with open(edges_path, newline="", encoding="utf-8") as f_edges:
                reader = csv.DictReader(f_edges, delimiter="\t")
                batches_by_type: Dict[str, List[Dict]] = {}

                def flush(rel_type: str, batch: List[Dict]) -> None:
                    if not batch:
                        return
                    session.run(
                        f"""
                        UNWIND $batch AS row
                        MATCH (s {{id: row.source}}), (t {{id: row.target}})
                        MERGE (s)-[:{rel_type}]->(t)
                        """,
                        batch=batch,
                    )

                for row in reader:
                    rel_type = row["metaedge"]
                    batch = batches_by_type.setdefault(rel_type, [])
                    batch.append({"source": row["source"], "target": row["target"]})
                    if len(batch) >= BATCH_EDGES:
                        flush(rel_type, batch)
                        batches_by_type[rel_type] = []

                for rel_type, batch in batches_by_type.items():
                    flush(rel_type, batch)

    def query2(self, disease_id: str) -> None:
        cypher = """
        MATCH (d:Disease {id: $disease_id})-[:DlA]->(a:Anatomy)

        MATCH (a)-[ag:AuG|AdG]->(g:Gene)

        MATCH (c:Compound)-[cg:CuG|CdG]->(g)

        WITH d, c,
          CASE WHEN type(ag) = 'AuG' THEN 1 ELSE -1 END AS anatomy_sign,
          CASE WHEN type(cg) = 'CuG' THEN 1 ELSE -1 END AS compound_sign

        WHERE anatomy_sign = -compound_sign
          AND NOT (c)-[:CtD|CpD]->(d)

        RETURN DISTINCT
          c.id   AS compound_id,
          c.name AS compound_name
        ORDER BY compound_name
        """
        with self.driver.session() as session:
            result = list(session.run(cypher, disease_id=disease_id))

        if not result:
            print("No new candidate compounds found.")
            return

        print("Candidate compounds (not already treating/palliating):")
        for r in result:
            print(f"- {r['compound_id']}: {r['compound_name']}")


# MongoDB (document store)

@dataclass
class Node:
    id: str
    name: str
    kind: str


class MongoBackend:
    def __init__(
        self,
        uri: str = None,
        db_name: str = None,
    ) -> None:
        uri = uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        db_name = db_name or os.getenv("MONGODB_DB", "hetionet")
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def close(self) -> None:
        self.client.close()

    def build_from_tsv(self, nodes_path: str, edges_path: str) -> None:
        nodes: Dict[str, Node] = {}
        with open(nodes_path, newline="", encoding="utf-8") as f_nodes:
            reader = csv.DictReader(f_nodes, delimiter="\t")
            for row in reader:
                node_id = row["id"]
                nodes[node_id] = Node(
                    id=node_id,
                    name=row.get("name", ""),
                    kind=row.get("kind", ""),
                )

        diseases: Dict[str, Dict] = {}
        for node in nodes.values():
            if node.kind == "Disease":
                diseases[node.id] = {
                    "_id": node.id,
                    "name": node.name,
                    "drugs": [],
                    "genes": [],
                    "anatomies": [],
                }

        with open(edges_path, newline="", encoding="utf-8") as f_edges:
            reader = csv.DictReader(f_edges, delimiter="\t")
            for row in reader:
                source = row["source"]
                target = row["target"]
                metaedge = row["metaedge"]

                # Compound -> Disease (treat/palliate)
                if metaedge in TREAT_EDGES and target in diseases and source in nodes:
                    disease = diseases[target]
                    compound_node = nodes[source]
                    disease["drugs"].append(
                        {
                            "id": compound_node.id,
                            "name": compound_node.name,
                            "relation": metaedge,
                        }
                    )

                # Disease -> Anatomy
                elif metaedge == "DlA" and source in diseases and target in nodes:
                    disease = diseases[source]
                    anatomy_node = nodes[target]
                    disease["anatomies"].append(
                        {
                            "id": anatomy_node.id,
                            "name": anatomy_node.name,
                        }
                    )

                # Disease -> Gene
                elif metaedge in DISEASE_GENE_EDGES and source in diseases and target in nodes:
                    disease = diseases[source]
                    gene_node = nodes[target]
                    disease["genes"].append(
                        {
                            "id": gene_node.id,
                            "name": gene_node.name,
                            "relation": metaedge,
                        }
                    )

        # Deduplicate lists
        for doc in diseases.values():
            for field in ("drugs", "genes", "anatomies"):
                seen = set()
                uniq_list: List[Dict] = []
                for item in doc[field]:
                    key = (item["id"], item.get("relation"))
                    if key not in seen:
                        seen.add(key)
                        uniq_list.append(item)
                doc[field] = uniq_list

        coll = self.db["diseases"]
        coll.drop()
        if diseases:
            coll.insert_many(list(diseases.values()))
        coll.create_index("name")

    def query1(self, disease_id: str) -> None:
        coll = self.db["diseases"]
        doc = coll.find_one({"_id": disease_id})
        if not doc:
            print(f"Disease {disease_id} not found in MongoDB.")
            return

        print("Disease ID      :", doc["_id"])
        print("Disease name    :", doc.get("name", ""))
        drugs = [d["name"] for d in doc.get("drugs", [])]
        genes = [g["name"] for g in doc.get("genes", [])]
        anatomies = [a["name"] for a in doc.get("anatomies", [])]
        print("Drugs (treat/palliate):", ", ".join(drugs) if drugs else "(none)")
        print("Genes (associated)   :", ", ".join(genes) if genes else "(none)")
        print("Anatomy locations    :", ", ".join(anatomies) if anatomies else "(none)")


# CLI

def main() -> None:
    parser = argparse.ArgumentParser(
        description="HetioNet Project 1 - Multi-Store CLI (Neo4j + MongoDB)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # Neo4j commands
    neo4j_load = sub.add_parser(
        "load-neo4j", help="Load nodes.tsv and edges.tsv into Neo4j graph store"
    )
    neo4j_load.add_argument("--nodes", required=True, help="Path to nodes.tsv")
    neo4j_load.add_argument("--edges", required=True, help="Path to edges.tsv")

    neo4j_q2 = sub.add_parser("q2-neo4j", help="Run Query 2 in Neo4j")
    neo4j_q2.add_argument("--disease-id", required=True)

    # MongoDB commands
    mongo_build = sub.add_parser(
        "build-mongo",
        help="Build denormalized disease documents in MongoDB from TSV files",
    )
    mongo_build.add_argument("--nodes", required=True, help="Path to nodes.tsv")
    mongo_build.add_argument("--edges", required=True, help="Path to edges.tsv")

    mongo_q1 = sub.add_parser("q1-mongo", help="Run Query 1 (MongoDB only)")
    mongo_q1.add_argument("--disease-id", required=True)

    args = parser.parse_args()

    if args.command in {"q2-neo4j", "load-neo4j"}:
        backend = Neo4jBackend()
        try:
            if args.command == "load-neo4j":
                backend.load_data(args.nodes, args.edges)
                print("Data loaded into Neo4j.")
            elif args.command == "q2-neo4j":
                backend.query2(args.disease_id)
        finally:
            backend.close()

    elif args.command in {"build-mongo", "q1-mongo"}:
        backend = MongoBackend()
        try:
            if args.command == "build-mongo":
                backend.build_from_tsv(args.nodes, args.edges)
                print("MongoDB diseases collection built.")
            elif args.command == "q1-mongo":
                backend.query1(args.disease_id)
        finally:
            backend.close()


if __name__ == "__main__":
    main()