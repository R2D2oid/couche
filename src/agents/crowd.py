"""
Crowd & Door-Shopper Agent
Answers: Are people leaving immediately because the store is too crowded?
  - Door-shopper rate overall, by hour, by day
  - Occupancy at each visitor's entrance time
  - Correlation between crowd level and door-shopper / non-conversion rates
  - Threshold analysis: at what occupancy does abandonment spike?
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from db import get_connection

DOOR_SHOPPER_SECS = 30          # visitors who leave within this are "door shoppers"
OCCUPANCY_BINS = [0, 5, 10, 15, 20, 25, 30, 1000]
OCCUPANCY_LABELS = ["1-5", "6-10", "11-15", "16-20", "21-25", "26-30", "30+"]


def _compute_occupancy(tracks_df: pd.DataFrame) -> pd.Series:
    """
    For each row, count how many OTHER active tracks overlap at that track's
    entrance time (i.e. how many people were already in the store).
    Uses a vectorised merge instead of a per-row loop.
    """
    df = tracks_df[["master_track_id", "entrance", "exit"]].copy()
    df = df.dropna(subset=["entrance", "exit"])

    # Convert to int64 timestamps for fast comparison
    ent = df["entrance"].values.astype("int64")
    ext = df["exit"].values.astype("int64")

    # For each track i, occupancy = number of j where entrance_j <= entrance_i <= exit_j
    occ = np.zeros(len(df), dtype=int)
    for i in range(len(df)):
        occ[i] = int(((ent <= ent[i]) & (ext >= ent[i])).sum()) - 1  # exclude self

    return pd.Series(occ, index=df.index)


def analyze_crowd() -> dict:
    conn = get_connection()
    results = {}

    # ── Door-shopper overall ─────────────────────────────────────────────────
    results["door_shoppers_overall"] = conn.execute(f"""
        SELECT
            COUNT(*)                                                              AS total_visitors,
            SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                          AND NOT is_buyer THEN 1 ELSE 0 END)                    AS door_shoppers,
            ROUND(100.0 * SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                                       AND NOT is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                                AS door_shopper_rate_pct
        FROM tracks
        WHERE NOT is_staff
    """).df().to_dict("records")[0]

    # ── Door-shoppers by hour ────────────────────────────────────────────────
    results["door_shoppers_by_hour"] = conn.execute(f"""
        SELECT
            EXTRACT(HOUR FROM entrance)                                          AS hour,
            COUNT(*)                                                             AS visitors,
            SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                         AND NOT is_buyer THEN 1 ELSE 0 END)                    AS door_shoppers,
            ROUND(100.0 * SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                                       AND NOT is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                               AS door_shopper_rate_pct
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """).df().to_dict("records")

    # ── Door-shoppers by day of week ─────────────────────────────────────────
    results["door_shoppers_by_dow"] = conn.execute(f"""
        SELECT
            DAYNAME(entrance::DATE)                                              AS day_of_week,
            DAYOFWEEK(entrance::DATE)                                            AS dow_num,
            COUNT(*)                                                             AS visitors,
            SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                         AND NOT is_buyer THEN 1 ELSE 0 END)                    AS door_shoppers,
            ROUND(100.0 * SUM(CASE WHEN duration_seconds <= {DOOR_SHOPPER_SECS}
                                       AND NOT is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                               AS door_shopper_rate_pct
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL
        GROUP BY day_of_week, dow_num
        ORDER BY dow_num
    """).df().to_dict("records")

    # ── Load all tracks for occupancy computation ────────────────────────────
    all_tracks = conn.execute("""
        SELECT master_track_id, entrance, exit, is_buyer, is_staff,
               duration_seconds, date
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL AND exit IS NOT NULL
    """).df()

    all_tracks["entrance"] = pd.to_datetime(all_tracks["entrance"])
    all_tracks["exit"]     = pd.to_datetime(all_tracks["exit"])

    # Compute per-track occupancy
    all_tracks["occupancy"] = _compute_occupancy(all_tracks)
    all_tracks["is_door_shopper"] = (
        (all_tracks["duration_seconds"] <= DOOR_SHOPPER_SECS) & (~all_tracks["is_buyer"])
    )

    # ── Crowd vs door-shopper rate ───────────────────────────────────────────
    all_tracks["occ_bin"] = pd.cut(
        all_tracks["occupancy"],
        bins=OCCUPANCY_BINS,
        labels=OCCUPANCY_LABELS,
        right=True,
    )
    crowd_agg = (
        all_tracks.groupby("occ_bin", observed=True)
        .agg(visitors=("master_track_id", "count"),
             door_shoppers=("is_door_shopper", "sum"),
             buyers=("is_buyer", "sum"))
        .reset_index()
    )
    crowd_agg["door_shopper_rate_pct"] = (
        crowd_agg["door_shoppers"] / crowd_agg["visitors"].replace(0, pd.NA) * 100
    ).round(2)
    crowd_agg["conversion_rate_pct"] = (
        crowd_agg["buyers"] / crowd_agg["visitors"].replace(0, pd.NA) * 100
    ).round(2)
    results["crowd_vs_abandonment"] = crowd_agg.to_dict("records")

    # ── Correlation: occupancy ↔ door-shopper ────────────────────────────────
    corr = all_tracks[["occupancy", "is_door_shopper"]].corr().iloc[0, 1]
    results["crowd_door_shopper_correlation"] = round(float(corr), 4)

    # ── Abandonment threshold (first bin where rate jumps > 2× baseline) ────
    baseline = results["crowd_vs_abandonment"][0]["door_shopper_rate_pct"] if results["crowd_vs_abandonment"] else None
    threshold_bin = None
    if baseline:
        for row in results["crowd_vs_abandonment"]:
            rate = row.get("door_shopper_rate_pct") or 0
            if rate and rate > 2 * baseline:
                threshold_bin = row["occ_bin"]
                break
    results["abandonment_threshold_occupancy"] = threshold_bin

    # ── Peak abandonment hours (top 5) ──────────────────────────────────────
    peak = all_tracks[all_tracks["is_door_shopper"]].copy()
    peak["hour"] = peak["entrance"].dt.hour
    results["peak_abandonment_hours"] = (
        peak.groupby("hour").size()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={0: "door_shoppers"})
        .to_dict("records")
    )

    return results
