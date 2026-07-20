#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "weaviate-client @ git+https://github.com/weaviate/weaviate-python-client.git@trengrj/hybrid-diversity",
#   "numpy>=1.26",
#   "matplotlib>=3.8",
#   "rich>=13",
#   "datasets>=2.19",
#   "huggingface-hub",
# ]
# ///
"""Benchmark of diversity selection (MMR) for vector and hybrid search, swept over ef.

Runs the Amazon Products 2023 dataset (~117k products, pre-computed 1536-dim
embeddings) against a local Weaviate and measures, for each HNSW `ef` value:

  * recall@k vs exact (brute-force) vector ground truth — computed for EVERY
    config: only pure vector search can reach 1.0, so for boost/hybrid/MMR it
    reads as "how far results stray from pure vector search"
  * div@k (intra-list distance: mean pairwise cosine distance of the returned
    vectors), distinct categories, and average price — diversity/composition
  * p50 query latency

Configurations (all crossed with every ef):

  group  config          query limit (window)   returned
  k10    vector          10                      10
  k10    vector+mmr      20                      MMR limit=10
  k10    hybrid          10                      10
  k10    hybrid+mmr      20                      MMR limit=10
  k100   vector          100                     100
  k100   vector+mmr      250                     MMR limit=100
  k100   hybrid          100                     100
  k100   hybrid+mmr      250                     MMR limit=100

plus each of the eight crossed with a price boost (`+boost`):
Boost.numeric_decay("price", origin=0, scale=25, curve=EXPONENTIAL,
weight=0.5) — favours cheaper products, visible in the avg-price metric.

Per-window MMR model: the query `limit` is the diversification window the MMR
pass sees, `Diversity.mmr(limit=...)` is the page size returned. BM25-only is
excluded because it does not depend on ef.

Queries are deterministic: objects are imported with uuid5(row index), and the
query set is a seeded sample of stored objects — the object vector is the
near-vector, the first words of its title are the hybrid/BM25 text. No OpenAI
key needed. Ground truth (exact cosine top-N over all stored vectors) is
computed once and cached to <output>/ground_truth.npz, so repeated runs skip
straight to the sweep.

Latency is measured on runs without `include_vector`; metrics (ILD needs the
vectors) come from a separate untimed run that also serves as warmup. With the
default 100 queries each latency point is a p50 over 100 samples, so --repeats
defaults to 1.

Requires a locally-built Weaviate with hybrid diversity support (branch
trengrj/hybrid-diversity) on standard ports (8080/50051).

Usage:
    uv run benchmark_diversity.py                 # import + benchmark
    uv run benchmark_diversity.py --query-only    # skip import, reuse data
    uv run benchmark_diversity.py --query-only --efs 32,128,512 --queries 15

Outputs into --output (default ./diversity-results): recall_vs_ef.png,
latency_vs_ef.png, div_vs_ef.png, categories_vs_ef.png, avg_price_vs_ef.png,
results.json, and a self-contained results.html (graphs embedded) combining
everything.
"""

import argparse
import base64
import datetime
import json
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import weaviate
import weaviate.classes.config as wc
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from weaviate.classes.config import Reconfigure
from weaviate.classes.query import Boost, Diversity, MetadataQuery
from weaviate.util import generate_uuid5

COLLECTION_NAME = "AmazonProduct"
VECTOR_NAME = "default"
GT_DEPTH = 250  # deepest window in the config matrix
QUERY_TEXT_WORDS = 6  # hybrid/BM25 text = first N words of the sampled title
RETURN_PROPS = ["title", "main_category", "price"]


BOOST_WEIGHT = 0.5  # price-boost strength


@dataclass(frozen=True)
class Config:
    name: str
    mode: str  # "vector" | "hybrid"
    mmr: bool
    page: int  # results returned (MMR limit, or query limit when mmr=False)
    window: int  # query limit when mmr=True (diversification window)
    group: str  # "k10" | "k100"
    boost: bool = False


def _build_configs() -> list["Config"]:
    base = [
        Config("vector", "vector", False, 10, 10, "k10"),
        Config("vector+mmr", "vector", True, 10, 20, "k10"),
        Config("hybrid", "hybrid", False, 10, 10, "k10"),
        Config("hybrid+mmr", "hybrid", True, 10, 20, "k10"),
        Config("vector", "vector", False, 100, 100, "k100"),
        Config("vector+mmr", "vector", True, 100, 250, "k100"),
        Config("hybrid", "hybrid", False, 100, 100, "k100"),
        Config("hybrid+mmr", "hybrid", True, 100, 250, "k100"),
    ]
    boosted = [
        Config(c.name + "+boost", c.mode, c.mmr, c.page, c.window, c.group, boost=True)
        for c in base
    ]
    return base + boosted


CONFIGS = _build_configs()

COLORS = {
    "vector": "tab:blue",
    "vector+mmr": "tab:cyan",
    "hybrid": "tab:orange",
    "hybrid+mmr": "tab:red",
}


def price_boost():
    # Favor cheaper products: score decays as price rises from 0.
    return Boost.numeric_decay(
        "price", origin=0, scale=25, curve=Boost.Curve.EXPONENTIAL, weight=BOOST_WEIGHT
    )


# -- import --


def import_dataset(client: weaviate.WeaviateClient, limit: int, console: Console) -> None:
    from datasets import load_dataset

    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)

    client.collections.create(
        name=COLLECTION_NAME,
        vector_config=wc.Configure.Vectors.self_provided(),
        properties=[
            wc.Property(name="title", data_type=wc.DataType.TEXT),
            wc.Property(name="description", data_type=wc.DataType.TEXT),
            wc.Property(name="main_category", data_type=wc.DataType.TEXT),
            wc.Property(name="price", data_type=wc.DataType.NUMBER),
            wc.Property(name="average_rating", data_type=wc.DataType.NUMBER),
        ],
    )
    console.print(f"Created collection '{COLLECTION_NAME}'")

    console.print("Downloading dataset (cached by huggingface after first run)...")
    ds = load_dataset("milistu/AMAZON-Products-2023", split="train")

    collection = client.collections.get(COLLECTION_NAME)
    imported = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]importing"),
        TextColumn("[cyan]{task.fields[imported]}[/cyan] objects"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("import", total=None, imported=0)
        with collection.batch.fixed_size(batch_size=1000, concurrent_requests=4) as batch:
            for i, row in enumerate(ds):
                if limit > 0 and imported >= limit:
                    break
                if row["price"] is None or row["embeddings"] is None:
                    continue
                batch.add_object(
                    uuid=generate_uuid5(f"amazon-product-{i}"),
                    properties={
                        "title": row["title"] or "",
                        "description": (row["description"] or "")[:5000],
                        "main_category": row["main_category"] or "",
                        "price": float(row["price"]),
                        "average_rating": float(row["average_rating"] or 0),
                    },
                    vector=row["embeddings"],
                )
                imported += 1
                progress.update(task, imported=imported)

    if collection.batch.failed_objects:
        console.print(f"[red]{len(collection.batch.failed_objects)} objects failed to import")
    total = collection.aggregate.over_all(total_count=True).total_count
    console.print(f"Imported {imported} products, collection count = {total}")


# -- ground truth --


def build_ground_truth(collection, n_queries: int, seed: int, console: Console) -> dict:
    """Fetch all vectors, pick a seeded query sample, brute-force exact top-GT_DEPTH."""
    console.print("Fetching all vectors for exact ground truth (one-off, cached)...")
    uuids, vectors, titles = [], [], []
    for obj in collection.iterator(
        include_vector=True, return_properties=["title"], cache_size=2000
    ):
        uuids.append(str(obj.uuid))
        vectors.append(obj.vector[VECTOR_NAME])
        titles.append(obj.properties.get("title") or "")

    n = len(uuids)
    if n < GT_DEPTH:
        raise SystemExit(f"Only {n} objects in collection; need at least {GT_DEPTH}")
    console.print(f"Fetched {n} vectors, computing exact top-{GT_DEPTH}...")

    mat = np.asarray(vectors, dtype=np.float32)
    del vectors
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat /= norms

    rng = np.random.default_rng(seed)
    q_idx = np.sort(rng.choice(n, size=n_queries, replace=False))
    q_vecs = mat[q_idx].copy()

    sims = mat @ q_vecs.T  # (n, n_queries)
    top = np.argpartition(-sims, GT_DEPTH, axis=0)[:GT_DEPTH]  # unordered top per query
    uuid_arr = np.asarray(uuids)
    gt_uuids = []
    for qi in range(n_queries):
        order = np.argsort(-sims[top[:, qi], qi])
        gt_uuids.append(uuid_arr[top[:, qi][order]])

    q_texts = [" ".join(titles[i].split()[:QUERY_TEXT_WORDS]) for i in q_idx]
    return {
        "total_count": n,
        "query_uuids": uuid_arr[q_idx],
        "query_vectors": q_vecs,
        "query_texts": np.asarray(q_texts),
        "gt_uuids": np.asarray(gt_uuids),  # (n_queries, GT_DEPTH), exact-rank order
    }


def load_or_build_ground_truth(
    collection, cache_file: Path, n_queries: int, seed: int, console: Console
) -> dict:
    total = collection.aggregate.over_all(total_count=True).total_count
    if total == 0:
        raise SystemExit("Collection is empty — run without --query-only to import first.")

    if cache_file.exists():
        data = np.load(cache_file, allow_pickle=False)
        gt = {k: data[k] for k in data.files}
        probes_ok = all(
            collection.query.fetch_object_by_id(u) is not None for u in gt["query_uuids"][:3]
        )
        if int(gt["total_count"]) == total and len(gt["query_uuids"]) == n_queries and probes_ok:
            console.print(f"Loaded ground truth cache ({cache_file})")
            return gt
        console.print("[yellow]Ground truth cache stale (collection changed) — rebuilding")

    gt = build_ground_truth(collection, n_queries, seed, console)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cache_file, **gt)
    console.print(f"Ground truth cached to {cache_file}")
    return gt


# -- search dispatch --


def run_search(collection, cfg: Config, vector, text: str, alpha: float, balance: float,
               include_vector: bool):
    common = dict(
        boost=price_boost() if cfg.boost else None,
        include_vector=include_vector,
        return_metadata=MetadataQuery(distance=True, score=True),
        return_properties=RETURN_PROPS,
    )
    if cfg.mode == "vector":
        if cfg.mmr:
            return collection.query.near_vector(
                near_vector=vector,
                limit=cfg.window,
                diversity_selection=Diversity.mmr(limit=cfg.page, balance=balance),
                **common,
            )
        return collection.query.near_vector(near_vector=vector, limit=cfg.page, **common)

    if cfg.mmr:
        return collection.query.hybrid(
            query=text,
            alpha=alpha,
            limit=cfg.window,
            vector=vector,
            diversity_selection=Diversity.mmr(limit=cfg.page, balance=balance),
            **common,
        )
    return collection.query.hybrid(query=text, alpha=alpha, limit=cfg.page, vector=vector, **common)


# -- metrics --


def obj_vector(obj) -> Optional[np.ndarray]:
    v = obj.vector
    if isinstance(v, dict):
        v = v.get(VECTOR_NAME) or next(iter(v.values()), None)
    return np.asarray(v, dtype=np.float32) if v is not None else None


def intra_list_distance(objs) -> Optional[float]:
    vecs = [v for v in (obj_vector(o) for o in objs) if v is not None]
    if len(vecs) < 2:
        return None
    m = np.vstack(vecs)
    norms = np.linalg.norm(m, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    m = m / norms
    sims = m @ m.T
    iu = np.triu_indices(len(vecs), k=1)
    return float(np.mean(1.0 - sims[iu]))


def distinct_categories(objs) -> int:
    return len({(o.properties.get("main_category") or "") for o in objs})


def avg_price(objs) -> Optional[float]:
    prices = [o.properties.get("price") for o in objs if o.properties.get("price") is not None]
    return float(np.mean(prices)) if prices else None


# -- ef control --


def set_ef(collection, ef: int) -> None:
    collection.config.update(
        vector_config=Reconfigure.Vectors.update(
            name=VECTOR_NAME,
            vector_index_config=Reconfigure.VectorIndex.hnsw(ef=ef),
        )
    )


def get_current_ef(collection) -> int:
    cfg = collection.config.get()
    try:
        return cfg.vector_config[VECTOR_NAME].vector_index_config.ef
    except (AttributeError, KeyError, TypeError):
        return -1


# -- sweep --


def run_sweep(collection, gt: dict, efs: list[int], repeats: int, alpha: float,
              balance: float, console: Console) -> list[dict]:
    n_queries = len(gt["query_uuids"])
    pages = {cfg.page for cfg in CONFIGS}
    gt_pages = [
        {p: set(gt["gt_uuids"][qi][:p]) for p in pages} for qi in range(n_queries)
    ]

    vectors = [gt["query_vectors"][qi].tolist() for qi in range(n_queries)]
    texts = [str(t) for t in gt["query_texts"]]

    rows: list[dict] = []
    total_steps = len(efs) * len(CONFIGS) * n_queries
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("sweep", total=total_steps)

        for ef in efs:
            set_ef(collection, ef)
            # warm the new setting before any timed run
            run_search(collection, CONFIGS[0], vectors[0], texts[0], alpha, balance,
                       include_vector=False)

            for cfg in CONFIGS:
                progress.update(task, description=f"ef={ef} {cfg.group}/{cfg.name}")
                for qi in range(n_queries):
                    # untimed metrics run (needs vectors for ILD), doubles as warmup
                    res = run_search(collection, cfg, vectors[qi], texts[qi],
                                     alpha, balance, include_vector=True)
                    objs = res.objects
                    ids = {str(o.uuid) for o in objs}

                    # vs exact vector GT for every config: only pure vector can
                    # reach 1.0; for boost/hybrid/MMR it measures drift from
                    # pure vector search
                    recall = len(ids & gt_pages[qi][cfg.page]) / cfg.page

                    # timed runs, without vector payloads
                    lat = []
                    for _ in range(max(1, repeats)):
                        t0 = time.perf_counter()
                        run_search(collection, cfg, vectors[qi], texts[qi],
                                   alpha, balance, include_vector=False)
                        lat.append((time.perf_counter() - t0) * 1000)

                    rows.append({
                        "ef": ef,
                        "group": cfg.group,
                        "config": cfg.name,
                        "query": qi,
                        "n_results": len(objs),
                        "latency_ms": min(lat),
                        "recall": recall,
                        "ild": intra_list_distance(objs),
                        "categories": distinct_categories(objs),
                        "avg_price": avg_price(objs),
                    })
                    progress.advance(task)
    return rows


# -- aggregation / output --


def aggregate(rows: list[dict], efs: list[int]) -> list[dict]:
    def mean(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    summary = []
    for ef in efs:
        for cfg in CONFIGS:
            sub = [r for r in rows if r["ef"] == ef and r["group"] == cfg.group
                   and r["config"] == cfg.name]
            summary.append({
                "ef": ef,
                "group": cfg.group,
                "config": cfg.name,
                "p50_ms": median(r["latency_ms"] for r in sub),
                "recall": mean([r["recall"] for r in sub]),
                "ild": mean([r["ild"] for r in sub]),
                "categories": mean([r["categories"] for r in sub]),
                "avg_price": mean([r["avg_price"] for r in sub]),
            })
    return summary


def print_summary(summary: list[dict], efs: list[int], console: Console) -> None:
    for ef in efs:
        table = Table(title=f"ef = {ef} (means over queries)")
        for col in ["group", "config", "p50 ms", "recall@k", "div@k", "cats", "avg $"]:
            table.add_column(col)
        for s in (s for s in summary if s["ef"] == ef):
            table.add_row(
                s["group"],
                s["config"],
                f"{s['p50_ms']:.1f}",
                f"{s['recall']:.3f}" if s["recall"] is not None else "-",
                f"{s['ild']:.3f}" if s["ild"] is not None else "-",
                f"{s['categories']:.1f}" if s["categories"] is not None else "-",
                f"{s['avg_price']:.2f}" if s["avg_price"] is not None else "-",
            )
        console.print(table)


def series(summary: list[dict], group: str, config: str, key: str, efs: list[int]):
    vals = []
    for ef in efs:
        v = next((s[key] for s in summary if s["ef"] == ef and s["group"] == group
                  and s["config"] == config), None)
        vals.append(v)
    return vals


def plot_grouped(summary, efs, key, ylabel, title, path):
    """One subplot per group (k10/k100); solid = plain, dashed = +boost."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharex=True)
    for ax, group in zip(axes, ["k10", "k100"]):
        for cfg in (c for c in CONFIGS if c.group == group):
            ys = series(summary, group, cfg.name, key, efs)
            if all(y is None for y in ys):
                continue
            base = cfg.name.removesuffix("+boost")
            ax.plot(efs, ys, marker="o", label=cfg.name, color=COLORS[base],
                    linestyle="--" if cfg.boost else "-")
        ax.set_xscale("log", base=2)
        ax.set_xticks(efs, [str(e) for e in efs])
        ax.set_xlabel("ef")
        ax.set_title(f"{group}  (page={'10' if group == 'k10' else '100'})")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8, ncols=2)
    axes[0].set_ylabel(ylabel)
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


GRAPH_FILES = ["recall_vs_ef.png", "latency_vs_ef.png", "div_vs_ef.png",
               "categories_vs_ef.png", "avg_price_vs_ef.png"]


def write_graphs(summary: list[dict], efs: list[int], outdir: Path, console: Console) -> None:
    plot_grouped(summary, efs, "recall", "recall@page vs exact vector GT",
                 "Recall vs ef (non-vector configs read as drift from pure vector search)",
                 outdir / "recall_vs_ef.png")
    plot_grouped(summary, efs, "p50_ms", "p50 latency (ms)",
                 "Query latency vs ef", outdir / "latency_vs_ef.png")
    plot_grouped(summary, efs, "ild", "div@k (intra-list distance)",
                 "Result diversity vs ef", outdir / "div_vs_ef.png")
    plot_grouped(summary, efs, "categories", "distinct categories",
                 "Category diversity vs ef", outdir / "categories_vs_ef.png")
    plot_grouped(summary, efs, "avg_price", "avg price ($)",
                 "Average price of results vs ef (boost impact)",
                 outdir / "avg_price_vs_ef.png")
    for name in GRAPH_FILES:
        console.print(f"  wrote {outdir / name}")


def write_html(summary: list[dict], efs: list[int], meta: dict, outdir: Path,
               console: Console) -> None:
    """Single self-contained report: run metadata, embedded graphs, per-ef tables."""

    def fmt(v, spec=".3f"):
        return format(v, spec) if v is not None else "–"

    meta_rows = "".join(
        f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in meta.items()
    )

    images = ""
    for name in GRAPH_FILES:
        b64 = base64.b64encode((outdir / name).read_bytes()).decode()
        images += (f'<div class="graph"><img alt="{name}" '
                   f'src="data:image/png;base64,{b64}"></div>\n')

    tables = ""
    for ef in efs:
        body = ""
        for s in (s for s in summary if s["ef"] == ef):
            body += (
                f"<tr><td>{s['group']}</td><td>{s['config']}</td>"
                f"<td>{fmt(s['p50_ms'], '.1f')}</td>"
                f"<td>{fmt(s['recall'])}</td>"
                f"<td>{fmt(s['ild'])}</td>"
                f"<td>{fmt(s['categories'], '.1f')}</td>"
                f"<td>{fmt(s['avg_price'], '.2f')}</td></tr>\n"
            )
        tables += f"""
<h3>ef = {ef}</h3>
<table>
  <thead><tr><th>group</th><th>config</th><th>p50 ms</th><th>recall@k</th>
  <th>div@k</th><th>cats</th><th>avg $</th></tr></thead>
  <tbody>{body}</tbody>
</table>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Diversity (MMR) + hybrid search ef-sweep benchmark</title>
<style>
  body {{ font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
         max-width: 1100px; margin: 2rem auto; padding: 0 1rem; color: #1a1a2e; }}
  h1 {{ font-size: 1.5rem; }}
  h2 {{ margin-top: 2.5rem; border-bottom: 1px solid #ddd; padding-bottom: .3rem; }}
  table {{ border-collapse: collapse; margin: .75rem 0 1.5rem; font-size: .9rem; }}
  th, td {{ border: 1px solid #ccc; padding: .35rem .7rem; text-align: right; }}
  th {{ background: #f4f4f8; }}
  td:first-child, td:nth-child(2), th:first-child {{ text-align: left; }}
  .meta th {{ text-align: left; }}
  .graph img {{ max-width: 100%; height: auto; margin: 1rem 0; }}
  .note {{ color: #555; font-size: .85rem; }}
</style>
</head>
<body>
<h1>Diversity (MMR) + hybrid search — ef sweep</h1>
<p class="note">Deterministic seeded queries against exact brute-force ground truth.
"recall@k" compares every config against the exact vector top-k — only pure vector
search can reach 1.0; for boost/hybrid/MMR it reads as how far results stray from pure
vector search. "div@k" is intra-list distance (mean pairwise cosine distance of the
result vectors); "avg $" is the mean price of returned items — "+boost" configs apply
Boost.numeric_decay(price, origin=0, scale=25, weight=0.5) favouring cheaper products.
MMR configs: query limit = diversification window, MMR limit = page. In graphs,
solid = plain, dashed = +boost.</p>

<h2>Run</h2>
<table class="meta">{meta_rows}</table>

<h2>Graphs</h2>
{images}
<h2>Summary tables (means over queries)</h2>
{tables}
</body>
</html>
"""
    path = outdir / "results.html"
    path.write_text(html)
    console.print(f"  wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="MMR diversity + hybrid search ef-sweep benchmark")
    parser.add_argument("--query-only", action="store_true",
                        help="Skip the import phase, benchmark existing collection")
    parser.add_argument("--import-limit", type=int, default=0,
                        help="Cap imported objects (0 = full dataset, ~117k)")
    parser.add_argument("--queries", type=int, default=100,
                        help="Deterministic queries per configuration")
    parser.add_argument("--efs", type=str, default="16,32,64,128,256,512",
                        help="Comma-separated ef values to sweep")
    parser.add_argument("--repeats", type=int, default=1,
                        help="Timed repeats per query (min taken)")
    parser.add_argument("--alpha", type=float, default=0.5, help="Hybrid alpha")
    parser.add_argument("--balance", type=float, default=0.5,
                        help="MMR balance (1.0 relevance, 0.0 diversity)")
    parser.add_argument("--seed", type=int, default=42, help="Query sampling seed")
    parser.add_argument("--output", type=str, default="diversity-results",
                        help="Output directory for graphs, results.json, GT cache")
    args = parser.parse_args()

    efs = [int(e) for e in args.efs.split(",")]
    outdir = Path(args.output)
    outdir.mkdir(parents=True, exist_ok=True)
    console = Console()

    client = weaviate.connect_to_local()
    try:
        if not args.query_only:
            import_dataset(client, args.import_limit, console)
        collection = client.collections.get(COLLECTION_NAME)

        gt = load_or_build_ground_truth(
            collection, outdir / "ground_truth.npz", args.queries, args.seed, console
        )
        console.print(f"{len(gt['query_uuids'])} queries, e.g. "
                      f"{[str(t) for t in gt['query_texts'][:3]]}\n")

        orig_ef = get_current_ef(collection)
        try:
            rows = run_sweep(collection, gt, efs, args.repeats, args.alpha,
                             args.balance, console)
        finally:
            set_ef(collection, orig_ef if orig_ef is not None else -1)

        summary = aggregate(rows, efs)
        print_summary(summary, efs, console)

        meta = {
            "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
            "collection": COLLECTION_NAME,
            "total_count": int(gt["total_count"]),
            "queries": len(gt["query_uuids"]),
            "efs": efs,
            "repeats": args.repeats,
            "alpha": args.alpha,
            "balance": args.balance,
            "boost_weight": BOOST_WEIGHT,
            "seed": args.seed,
        }
        (outdir / "results.json").write_text(json.dumps({
            "meta": meta,
            "summary": summary,
            "per_query": rows,
        }, indent=2))
        console.print(f"\n  wrote {outdir / 'results.json'}")
        write_graphs(summary, efs, outdir, console)
        write_html(summary, efs, meta, outdir, console)
    finally:
        client.close()


if __name__ == "__main__":
    main()
