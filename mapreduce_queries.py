#!/usr/bin/env python3
"""
PySpark MapReduce implementation for HetIONet Project 2.

Q1: For each drug, count #genes and #diseases. Top 5 by gene count (desc).
Q2: For each disease, count distinct drugs treating it. Group by drug count,
    count diseases. Top 5 by disease count (desc).
Q3: Top 5 drugs by gene count with human-readable names (replicated join).
"""

from pyspark import SparkContext, SparkConf

EDGES_PATH = "edges.tsv"
NODES_PATH = "nodes.tsv"

GENE_EDGES = frozenset({"CbG", "CuG", "CdG"})
DISEASE_EDGES = frozenset({"CtD", "CpD"})


def main() -> None:
    conf = SparkConf().setAppName("HetioNet-Project2").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    raw_edges = sc.textFile(EDGES_PATH)
    edges_header = raw_edges.first()
    # Each element: (source, metaedge, target)
    edges = (
        raw_edges
        .filter(lambda line: line != edges_header)
        .map(lambda line: line.split("\t"))
        .filter(lambda parts: len(parts) == 3)
        .map(lambda parts: (parts[0], parts[1], parts[2]))
    )

    # Mapper: keep only Compound edges, emit (compound_id, (gene_count, disease_count))
    q1_mapped = (
        edges
        .filter(lambda e: e[0].startswith("Compound::"))
        .filter(lambda e: e[1] in GENE_EDGES or e[1] in DISEASE_EDGES)
        .map(lambda e: (e[0], (1, 0)) if e[1] in GENE_EDGES else (e[0], (0, 1)))
    )

    # Reducer: sum (gene_count, disease_count) per compound
    q1_reduced = q1_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Sort by gene count descending, take top 5
    q1_results = (
        q1_reduced
        .map(lambda kv: (kv[0], kv[1][0], kv[1][1]))   # (compound_id, genes, diseases)
        .sortBy(lambda x: x[1], ascending=False)
        .take(5)
    )

    # Job 1 Mapper: (disease_id, compound_id) for every drug->disease edge
    q2_job1_mapped = (
        edges
        .filter(lambda e: e[0].startswith("Compound::") and e[1] in DISEASE_EDGES)
        .map(lambda e: (e[2], e[0]))   # flip: disease is the key
    )

    # Job 1 Reducer: count distinct drugs per disease
    q2_disease_drug_counts = (
        q2_job1_mapped
        .groupByKey()
        .mapValues(lambda compounds: len(set(compounds)))
    )

    # Job 2 Mapper: (drug_count, 1) — one emission per disease
    q2_job2_mapped = q2_disease_drug_counts.map(lambda kv: (kv[1], 1))

    # Job 2 Reducer: sum diseases per drug_count, top 5 by disease count
    q2_results = (
        q2_job2_mapped
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .take(5)
    )

    # Replicated join: load compound names from nodes.tsv into a broadcast variable
    raw_nodes = sc.textFile(NODES_PATH)
    nodes_header = raw_nodes.first()
    compound_names = dict(
        raw_nodes
        .filter(lambda line: line != nodes_header)
        .map(lambda line: line.split("\t"))
        .filter(lambda parts: len(parts) >= 3 and parts[2] == "Compound")
        .map(lambda parts: (parts[0], parts[1]))   # (compound_id, name)
        .collect()
    )
    broadcast_names = sc.broadcast(compound_names)

    # Mapper: resolve compound IDs to human-readable names via broadcast lookup
    q3_results = (
        sc.parallelize(q1_results)
        .map(lambda row: (broadcast_names.value.get(row[0], row[0]), row[1]))
        .collect()
    )
    
    print("=== Q1: Top 5 drugs by gene count ===")
    print(f"{'Drug ID':<30} {'Genes':>8} {'Diseases':>10}")
    print("-" * 52)
    for compound_id, genes, diseases in q1_results:
        print(f"{compound_id:<30} {genes:>8} {diseases:>10}")

    print()
    print("=== Q2: Diseases grouped by number of treating drugs (top 5) ===")
    print(f"{'Drugs':>8}   {'Diseases':>10}")
    print("-" * 22)
    for drug_count, disease_count in q2_results:
        print(f"{drug_count:>8}   {disease_count:>10}")

    print()
    print("=== Q3: Top 5 drug names by gene count ===")
    print(f"{'Drug Name':<35} {'Genes':>8}")
    print("-" * 45)
    for name, genes in q3_results:
        print(f"{name:<35} {genes:>8}")

    # ── Save to file ───────────────────────────────────────────────────────────
    with open("mapreduce_output.txt", "w") as f:
        f.write("=== Q1: Top 5 drugs by gene count ===\n")
        f.write(f"{'Drug ID':<30} {'Genes':>8} {'Diseases':>10}\n")
        for compound_id, genes, diseases in q1_results:
            f.write(f"{compound_id:<30} {genes:>8} {diseases:>10}\n")

        f.write("\n=== Q2: Diseases grouped by number of treating drugs (top 5) ===\n")
        f.write(f"{'Drugs':>8}   {'Diseases':>10}\n")
        for drug_count, disease_count in q2_results:
            f.write(f"{drug_count:>8}   {disease_count:>10}\n")

        f.write("\n=== Q3: Top 5 drug names by gene count ===\n")
        f.write(f"{'Drug Name':<35} {'Genes':>8}\n")
        for name, genes in q3_results:
            f.write(f"{name:<35} {genes:>8}\n")

    sc.stop()


if __name__ == "__main__":
    main()
