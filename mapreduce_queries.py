#!/usr/bin/env python3
"""
MapReduce simulation for HetIONet Project 2.

Q1: For each drug, count #genes and #diseases. Top 5 by gene count (desc).
Q2: For each disease, count distinct drugs treating it. Group by drug count,
    count diseases. Top 5 by disease count (desc).
Q3: Top 5 drugs by gene count with human-readable names (replicated join).
"""

import csv
from collections import defaultdict
from typing import Dict, Generator, List, Tuple

EDGES_PATH = "edges.tsv"
NODES_PATH = "nodes.tsv"

GENE_EDGES = {"CbG", "CuG", "CdG"}
DISEASE_EDGES = {"CtD", "CpD"}

def q1_mapper(edges_path: str) -> Generator[Tuple[str, str], None, None]:
    """
    Input : each row of edges.tsv
    Output: (compound_id, "G") for drug->gene edges
            (compound_id, "D") for drug->disease edges
    """
    with open(edges_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            source = row["source"]
            metaedge = row["metaedge"]
            if not source.startswith("Compound::"):
                continue
            if metaedge in GENE_EDGES:
                yield (source, "G")
            elif metaedge in DISEASE_EDGES:
                yield (source, "D")

def q1_reducer(pairs: Generator) -> List[Tuple[str, int, int]]:
    """
    Input : (compound_id, tag) pairs from mapper
    Output: top 5 (compound_id, gene_count, disease_count) sorted by gene_count desc
    """
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"G": 0, "D": 0})
    for compound_id, tag in pairs:
        counts[compound_id][tag] += 1

    results = [
        (cid, c["G"], c["D"])
        for cid, c in counts.items()
    ]
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:5]

def run_q1(edges_path: str) -> List[Tuple[str, int, int]]:
    return q1_reducer(q1_mapper(edges_path))

# q2
def q2_mapper_job1(edges_path: str) -> Generator[Tuple[str, str], None, None]:
    """
    Job 1 mapper
    Input : each row of edges.tsv
    Output: (disease_id, compound_id) for every drug->disease edge
    """
    with open(edges_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            source = row["source"]
            target = row["target"]
            metaedge = row["metaedge"]
            if source.startswith("Compound::") and metaedge in DISEASE_EDGES:
                yield (target, source)

def q2_reducer_job1(pairs: Generator) -> Dict[str, int]:
    """
    Job 1 reducer
    Input : (disease_id, compound_id) pairs
    Output: {disease_id: drug_count} — how many distinct drugs treat each disease
    """
    disease_drugs: Dict[str, set] = defaultdict(set)
    for disease_id, compound_id in pairs:
        disease_drugs[disease_id].add(compound_id)
    return {d: len(drugs) for d, drugs in disease_drugs.items()}

def q2_mapper_job2(
    disease_drug_counts: Dict[str, int]
) -> Generator[Tuple[int, int], None, None]:
    """
    Job 2 mapper
    Input : {disease_id: drug_count} from job 1
    Output: (drug_count, 1) — one emission per disease
    """
    for _disease_id, drug_count in disease_drug_counts.items():
        yield (drug_count, 1)

def q2_reducer_job2(pairs: Generator) -> List[Tuple[int, int]]:
    """
    Job 2 reducer
    Input : (drug_count, 1) pairs
    Output: top 5 (drug_count, disease_count) sorted by disease_count desc
    """
    totals: Dict[int, int] = defaultdict(int)
    for drug_count, one in pairs:
        totals[drug_count] += one

    results = list(totals.items())
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:5]


def run_q2(edges_path: str) -> List[Tuple[int, int]]:
    job1_pairs = q2_mapper_job1(edges_path)
    disease_drug_counts = q2_reducer_job1(job1_pairs)
    job2_pairs = q2_mapper_job2(disease_drug_counts)
    return q2_reducer_job2(job2_pairs)

# q3
def load_compound_names(nodes_path: str) -> Dict[str, str]:
    """
    Replicated join: load the entire Compound section of nodes.tsv into memory.
    This is valid because nodes.tsv is small enough to broadcast to all mappers.
    """
    names: Dict[str, str] = {}
    with open(nodes_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("kind") == "Compound":
                names[row["id"]] = row["name"]
    return names


def q3_mapper(
    q1_results: List[Tuple[str, int, int]],
    compound_names: Dict[str, str],
) -> Generator[Tuple[str, int], None, None]:
    """
    Input : top-5 Q1 results + in-memory name lookup table
    Output: (drug_name, gene_count) — compound IDs resolved to human-readable names
    """
    for compound_id, gene_count, _disease_count in q1_results:
        name = compound_names.get(compound_id, compound_id)
        yield (name, gene_count)


def q3_reducer(pairs: Generator) -> List[Tuple[str, int]]:
    """
    Input : (drug_name, gene_count) pairs — already sorted by Q1
    Output: list preserved in order
    """
    return list(pairs)


def run_q3(edges_path: str, nodes_path: str) -> List[Tuple[str, int]]:
    q1_results = run_q1(edges_path)
    compound_names = load_compound_names(nodes_path)
    return q3_reducer(q3_mapper(q1_results, compound_names))

# save output to a file instead of printing to console
with open("mapreduce_output.txt", "w") as f:
    def print_to_file(*args, **kwargs):
        print(*args, **kwargs, file=f)

    print_to_file("=== Q1: Top 5 drugs by gene count ===")
    print_to_file(f"{'Drug ID':<30} {'Genes':>8} {'Diseases':>10}")
    for compound_id, genes, diseases in run_q1(EDGES_PATH):
        print_to_file(f"{compound_id:<30} {genes:>8} {diseases:>10}")

    print_to_file()
    print_to_file("=== Q2: Diseases grouped by number of treating drugs (top 5) ===")
    print_to_file(f"{'Drugs':>8}   {'Diseases':>10}")
    for drug_count, disease_count in run_q2(EDGES_PATH):
        print_to_file(f"{drug_count:>8}   {disease_count:>10}")

    print_to_file()
    print_to_file("=== Q3: Top 5 drug names by gene count ===")
    print_to_file(f"{'Drug Name':<35} {'Genes':>8}")
    for name, genes in run_q3(EDGES_PATH, NODES_PATH):
        print_to_file(f"{name:<35} {genes:>8}")


def main() -> None:
    print("=== Q1: Top 5 drugs by gene count ===")
    print(f"{'Drug ID':<30} {'Genes':>8} {'Diseases':>10}")
    for compound_id, genes, diseases in run_q1(EDGES_PATH):
        print(f"{compound_id:<30} {genes:>8} {diseases:>10}")

    print()
    print("=== Q2: Diseases grouped by number of treating drugs (top 5) ===")
    print(f"{'Drugs':>8}   {'Diseases':>10}")
    for drug_count, disease_count in run_q2(EDGES_PATH):
        print(f"{drug_count:>8}   {disease_count:>10}")

    print()
    print("=== Q3: Top 5 drug names by gene count ===")
    print(f"{'Drug Name':<35} {'Genes':>8}")
    for name, genes in run_q3(EDGES_PATH, NODES_PATH):
        print(f"{name:<35} {genes:>8}")


if __name__ == "__main__":
    main()
