"""
Group Agent
Answers: When a group visits, why does only one person buy?
  - Detects groups by co-entry time window
  - Classifies groups: all-buy / partial-buy / none-buy
  - Finds which zones group non-buyers browse without converting
  - Conversion rate by group size
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from db import get_connection

GROUP_WINDOW_SECS = 30   # people entering within this window are considered a group
MIN_GROUP_SIZE   = 2


def _detect_groups(tracks: pd.DataFrame) -> pd.DataFrame:
    """
    Greedy group detection: sort by (date, entrance); assign a group_id to
    consecutive tracks whose entrance falls within GROUP_WINDOW_SECS of the
    first member of that group.
    Returns a DataFrame with columns [group_id, master_track_id].
    """
    tracks = tracks.sort_values(["date", "entrance"]).reset_index(drop=True)
    assignments = []
    group_id = 0
    i = 0

    while i < len(tracks):
        row = tracks.iloc[i]
        window_end = row["entrance"] + pd.Timedelta(seconds=GROUP_WINDOW_SECS)
        same_day = tracks["date"] == row["date"]
        in_window = (tracks["entrance"] >= row["entrance"]) & (tracks["entrance"] <= window_end) & same_day
        members = tracks[in_window].index.tolist()

        if len(members) >= MIN_GROUP_SIZE:
            for m in members:
                assignments.append({"group_id": group_id, "track_idx": m})
            group_id += 1
            i = members[-1] + 1   # skip past all assigned members
        else:
            i += 1

    if not assignments:
        return pd.DataFrame(columns=["group_id", "master_track_id"])

    assign_df = pd.DataFrame(assignments)
    merged = assign_df.merge(
        tracks.reset_index().rename(columns={"index": "track_idx"}),
        on="track_idx",
    )
    return merged[["group_id", "master_track_id", "is_buyer", "is_staff",
                   "duration_seconds", "zone_count", "poi_count", "date", "entrance"]]


def analyze_groups(date_filter: str | None = None) -> dict:
    conn = get_connection()
    results = {}
    date_clause = f"AND date = '{date_filter}'" if date_filter else ""

    tracks = conn.execute(f"""
        SELECT master_track_id, entrance, exit, is_buyer, is_staff,
               duration_seconds, zone_count, poi_count, date, gender
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL {date_clause}
    """).df()

    tracks["entrance"] = pd.to_datetime(tracks["entrance"])
    tracks["exit"]     = pd.to_datetime(tracks["exit"])

    group_members = _detect_groups(tracks)

    if group_members.empty:
        results["groups_found"] = 0
        return results

    # ── Group-level stats ────────────────────────────────────────────────────
    grp = (
        group_members.groupby("group_id")
        .agg(
            group_size        = ("master_track_id", "count"),
            buyers            = ("is_buyer", "sum"),
            avg_duration_secs = ("duration_seconds", "mean"),
            avg_zones         = ("zone_count", "mean"),
            avg_pois          = ("poi_count", "mean"),
            date              = ("date", "first"),
        )
        .reset_index()
    )
    grp["all_buy"]     = grp["buyers"] == grp["group_size"]
    grp["none_buy"]    = grp["buyers"] == 0
    grp["partial_buy"] = (~grp["all_buy"]) & (~grp["none_buy"])
    grp["conversion_rate_pct"] = (grp["buyers"] / grp["group_size"] * 100).round(2)

    results["groups_found"] = int(len(grp))
    results["group_size_distribution"] = (
        grp["group_size"].value_counts().sort_index().to_dict()
    )
    results["buying_patterns"] = {
        "all_buy":           int(grp["all_buy"].sum()),
        "partial_buy":       int(grp["partial_buy"].sum()),
        "none_buy":          int(grp["none_buy"].sum()),
        "pct_all_buy":       round(float(grp["all_buy"].mean() * 100), 2),
        "pct_partial_buy":   round(float(grp["partial_buy"].mean() * 100), 2),
        "pct_none_buy":      round(float(grp["none_buy"].mean() * 100), 2),
    }

    # ── Conversion rate by group size ────────────────────────────────────────
    by_size = (
        grp.groupby("group_size")
        .agg(
            num_groups       = ("group_id", "count"),
            avg_conversion   = ("conversion_rate_pct", "mean"),
            avg_buyers       = ("buyers", "mean"),
        )
        .reset_index()
    )
    by_size["avg_conversion"] = by_size["avg_conversion"].round(2)
    by_size["avg_buyers"]     = by_size["avg_buyers"].round(2)
    results["conversion_by_group_size"] = by_size.to_dict("records")

    # ── Zones browsed by non-buyers in partial-buy groups ───────────────────
    partial_ids = grp[grp["partial_buy"]]["group_id"].tolist()
    partial_nonbuyer_tids = group_members[
        (group_members["group_id"].isin(partial_ids)) & (~group_members["is_buyer"])
    ]["master_track_id"].tolist()

    if partial_nonbuyer_tids:
        tid_list = ",".join(map(str, partial_nonbuyer_tids))
        results["partial_nonbuyer_zones"] = conn.execute(f"""
            SELECT
                zone,
                COUNT(DISTINCT master_track_id) AS nonbuyer_visits
            FROM events
            WHERE master_track_id IN ({tid_list})
              AND zone IS NOT NULL
              AND event_type = 'ZONE_ENTRY'
            GROUP BY zone
            ORDER BY nonbuyer_visits DESC
            LIMIT 15
        """).df().to_dict("records")
    else:
        results["partial_nonbuyer_zones"] = []

    # ── Weekly trend of group partial-buy rate ───────────────────────────────
    grp["week"] = pd.to_datetime(grp["date"]).dt.to_period("W").astype(str)
    weekly = (
        grp.groupby("week")
        .agg(
            total_groups   = ("group_id", "count"),
            partial_groups = ("partial_buy", "sum"),
        )
        .reset_index()
    )
    weekly["partial_buy_rate_pct"] = (
        weekly["partial_groups"] / weekly["total_groups"].replace(0, pd.NA) * 100
    ).round(2)
    results["weekly_partial_buy_trend"] = weekly.to_dict("records")

    return results
