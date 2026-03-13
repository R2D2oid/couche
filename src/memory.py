"""
Memory store — ChromaDB backed daily summaries + insight storage.

Two collections:
  - "daily_summaries"  : one document per day, auto-embedded
  - "insights"         : free-form findings stored by the agent

Storage: $COUCHE_SCRATCH/memory/chromadb/  (Lustre scratch, set via env var)

Usage:
  from memory import store_daily_summary, search_similar, memorize_all
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


import chromadb
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

sys.path.insert(0, str(Path(__file__).parent))
from db import get_connection

logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).parent.parent
_scratch = os.environ.get("COUCHE_SCRATCH")
MEMORY_DIR = Path(_scratch) / "memory" / "chromadb" if _scratch else BASE_DIR / "memory" / "chromadb"
MEMORY_DIR.mkdir(parents=True, exist_ok=True)

# Force ChromaDB model cache to scratch before any embedding function is loaded
if _scratch:
    _cache = str(Path(_scratch) / "chroma_cache")
    os.environ["CHROMA_CACHE_DIR"] = _cache
    os.environ["SENTENCE_TRANSFORMERS_HOME"] = _cache  # fallback for some chromadb versions

# ── ChromaDB client (persistent) ───────────────────────────────────────────────
_client = chromadb.PersistentClient(path=str(MEMORY_DIR))
_ef = DefaultEmbeddingFunction()

_summaries = _client.get_or_create_collection(
    name="daily_summaries",
    embedding_function=_ef,
    metadata={"hnsw:space": "cosine"},
)
_insights = _client.get_or_create_collection(
    name="insights",
    embedding_function=_ef,
    metadata={"hnsw:space": "cosine"},
)


# ── Summary text builder ────────────────────────────────────────────────────────

def _build_summary_text(date: str, conversion: dict, crowd: dict, groups: dict) -> str:
    """Convert agent metric dicts into rich natural-language text for embedding."""
    try:
        dow   = datetime.strptime(date, "%Y-%m-%d").strftime("%A")
        month = datetime.strptime(date, "%Y-%m-%d").strftime("%B")
    except Exception:
        dow, month = "", ""

    lines = [f"Date: {date} ({dow}, {month})."]

    ov = conversion.get("overall", {})
    if ov:
        lines.append(
            f"Total visitors: {ov.get('total_visitors')}. "
            f"Conversion rate: {ov.get('conversion_rate_pct')}%. "
            f"Buyers: {ov.get('buyers')}. Non-buyers: {ov.get('nonbuyers')}."
        )

    jd = conversion.get("journey_depth", [])
    buyer    = next((r for r in jd if r.get("is_buyer")),     {})
    nonbuyer = next((r for r in jd if not r.get("is_buyer")), {})
    if buyer:
        lines.append(
            f"Buyers visited avg {buyer.get('avg_zones_visited')} zones "
            f"and stayed {buyer.get('avg_duration_seconds')}s. "
            f"Non-buyers visited avg {nonbuyer.get('avg_zones_visited')} zones "
            f"and stayed {nonbuyer.get('avg_duration_seconds')}s."
        )

    top_zone = (conversion.get("zone_conversion") or [{}])[0]
    if top_zone:
        lines.append(
            f"Highest-converting zone: {top_zone.get('zone')} "
            f"at {top_zone.get('conversion_rate_pct')}%."
        )

    ds = crowd.get("door_shoppers_overall", {})
    if ds:
        lines.append(
            f"Door-shopper rate: {ds.get('door_shopper_rate_pct')}% "
            f"({ds.get('door_shoppers')} of {ds.get('total_visitors')} left within 30s)."
        )

    threshold = crowd.get("abandonment_threshold_occupancy")
    if threshold:
        lines.append(f"Crowd abandonment threshold: occupancy bin {threshold}.")

    corr = crowd.get("crowd_door_shopper_correlation")
    if corr is not None:
        lines.append(f"Crowd-abandonment correlation: {corr}.")

    peak_hrs = crowd.get("peak_abandonment_hours", [])
    if peak_hrs:
        hrs = ", ".join(str(r.get("hour")) for r in peak_hrs[:3])
        lines.append(f"Peak abandonment hours: {hrs}.")

    gf = groups.get("groups_found")
    if gf is not None:
        lines.append(f"Groups detected: {gf}.")

    bp = groups.get("buying_patterns", {})
    if bp:
        lines.append(
            f"Group buying — all buy: {bp.get('pct_all_buy')}%, "
            f"partial: {bp.get('pct_partial_buy')}%, "
            f"none: {bp.get('pct_none_buy')}%."
        )

    top_nb = (groups.get("partial_nonbuyer_zones") or [{}])[0]
    if top_nb:
        lines.append(f"Top zone for non-buying group members: {top_nb.get('zone')}.")

    return " ".join(lines)


# ── Public API ─────────────────────────────────────────────────────────────────

def store_daily_summary(
    date: str,
    conversion: dict,
    crowd: dict,
    groups: dict,
    overwrite: bool = False,
) -> str:
    """Embed and store a daily summary. Returns the summary text."""
    existing = _summaries.get(ids=[date])
    if existing["ids"] and not overwrite:
        logger.info("Memory: %s already stored, skipping.", date)
        return existing["documents"][0]

    summary = _build_summary_text(date, conversion, crowd, groups)
    meta = {
        "date":               date,
        "dow":                datetime.strptime(date, "%Y-%m-%d").strftime("%A"),
        "month":              datetime.strptime(date, "%Y-%m-%d").strftime("%B"),
        "conversion_rate":    str((conversion.get("overall") or {}).get("conversion_rate_pct", "")),
        "door_shopper_rate":  str((crowd.get("door_shoppers_overall") or {}).get("door_shopper_rate_pct", "")),
        "groups_found":       str(groups.get("groups_found", "")),
    }

    _summaries.upsert(ids=[date], documents=[summary], metadatas=[meta])
    logger.info("Memory: stored summary for %s (total: %d)", date, _summaries.count())
    return summary


def search_similar(query: str, n: int = 5, where: dict | None = None) -> list[dict]:
    """
    Find the n most semantically similar past days to the query.
    Optional `where` filter e.g. {"dow": "Tuesday"} to restrict by metadata.
    Returns list of dicts: date, distance, summary, metadata.
    """
    if _summaries.count() == 0:
        return [{"error": "Memory is empty. Run python main.py --memorize first."}]

    kwargs = {"query_texts": [query], "n_results": min(n, _summaries.count())}
    if where:
        kwargs["where"] = where

    res = _summaries.query(**kwargs)

    results = []
    for i, doc_id in enumerate(res["ids"][0]):
        results.append({
            "date":       doc_id,
            "distance":   round(res["distances"][0][i], 4),
            "summary":    res["documents"][0][i],
            "metadata":   res["metadatas"][0][i],
        })
    return results


def store_insight(text: str, tags: list[str] | None = None) -> str:
    """Store a free-form insight string (e.g. from the agent during chat)."""
    insight_id = f"insight_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    meta = {"tags": json.dumps(tags or []), "created_at": datetime.now().isoformat()}
    _insights.upsert(ids=[insight_id], documents=[text], metadatas=[meta])
    logger.info("Memory: stored insight %s", insight_id)
    return insight_id


def search_insights(query: str, n: int = 3) -> list[dict]:
    """Retrieve insights semantically similar to the query."""
    if _insights.count() == 0:
        return []
    res = _insights.query(query_texts=[query], n_results=min(n, _insights.count()))
    return [
        {"id": doc_id, "distance": round(dist, 4), "text": doc, "metadata": meta}
        for doc_id, dist, doc, meta in zip(
            res["ids"][0], res["distances"][0], res["documents"][0], res["metadatas"][0]
        )
    ]


def list_memorized_dates() -> list[str]:
    """Return all dates stored in memory."""
    if _summaries.count() == 0:
        return []
    res = _summaries.get()
    return sorted(res["ids"])


def memorize_all(force: bool = False):
    """
    Memorize every processed Parquet date not yet in ChromaDB.
    Runs lightweight per-day agent queries.
    """
    from agents.conversion import analyze_conversion
    from agents.crowd      import analyze_crowd
    from agents.groups     import analyze_groups

    conn = get_connection()
    all_dates = [
        r[0] for r in
        conn.execute("SELECT DISTINCT date FROM tracks ORDER BY date").fetchall()
    ]

    memorized = set(list_memorized_dates())
    to_process = [d for d in all_dates if d not in memorized or force]
    logger.info("Memory: %d dates to memorize (%d already done)", len(to_process), len(memorized))

    for date in to_process:
        logger.info("  Memorizing %s …", date)
        try:
            conversion = analyze_conversion(date_filter=date)
            crowd      = analyze_crowd(date_filter=date)
            groups     = analyze_groups(date_filter=date)
            store_daily_summary(date, conversion, crowd, groups, overwrite=force)
        except Exception as e:
            logger.error("  Failed %s: %s", date, e)

    logger.info("Memory: done. %d summaries total.", _summaries.count())
