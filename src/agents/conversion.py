"""
Conversion Agent
Answers: Why do some non-shoppers become shoppers?
  - Overall & trend conversion rates
  - Which zones/POIs correlate with buying
  - Buyer vs non-buyer journey depth (zones visited, dwell time)
  - Hourly and day-of-week patterns
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from db import get_connection


def analyze_conversion() -> dict:
    conn = get_connection()
    results = {}

    # ── Overall conversion ────────────────────────────────────────────────────
    results["overall"] = conn.execute("""
        SELECT
            COUNT(*)                                                          AS total_visitors,
            SUM(CASE WHEN is_buyer  THEN 1 ELSE 0 END)                        AS buyers,
            SUM(CASE WHEN NOT is_buyer AND NOT is_staff THEN 1 ELSE 0 END)    AS nonbuyers,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                             AS conversion_rate_pct
        FROM tracks
        WHERE NOT is_staff
    """).df().to_dict("records")[0]

    # ── Weekly conversion trend ───────────────────────────────────────────────
    results["weekly_trend"] = conn.execute("""
        SELECT
            DATE_TRUNC('week', entrance::DATE)                                AS week_start,
            COUNT(*)                                                          AS visitors,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                             AS conversion_rate_pct
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL
        GROUP BY week_start
        ORDER BY week_start
    """).df().to_dict("records")

    # ── Conversion by zone ───────────────────────────────────────────────────
    results["zone_conversion"] = conn.execute("""
        WITH tz AS (
            SELECT DISTINCT t.master_track_id, t.is_buyer, e.zone
            FROM tracks t
            JOIN events e ON t.master_track_id = e.master_track_id
            WHERE e.zone IS NOT NULL
              AND e.event_type = 'ZONE_ENTRY'
              AND NOT t.is_staff
        )
        SELECT
            zone,
            COUNT(DISTINCT master_track_id)                                    AS visitors,
            SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)                          AS buyers,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(DISTINCT master_track_id), 0), 2)       AS conversion_rate_pct
        FROM tz
        GROUP BY zone
        HAVING visitors > 50
        ORDER BY conversion_rate_pct DESC
    """).df().to_dict("records")

    # ── Conversion by POI ────────────────────────────────────────────────────
    results["poi_conversion"] = conn.execute("""
        WITH tp AS (
            SELECT DISTINCT t.master_track_id, t.is_buyer, e.poi_name
            FROM tracks t
            JOIN events e ON t.master_track_id = e.master_track_id
            WHERE e.poi_name IS NOT NULL
              AND NOT t.is_staff
        )
        SELECT
            poi_name,
            COUNT(DISTINCT master_track_id)                                    AS visitors,
            SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)                          AS buyers,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(DISTINCT master_track_id), 0), 2)       AS conversion_rate_pct
        FROM tp
        GROUP BY poi_name
        HAVING visitors > 30
        ORDER BY conversion_rate_pct DESC
    """).df().to_dict("records")

    # ── Hourly conversion ────────────────────────────────────────────────────
    results["hourly_conversion"] = conn.execute("""
        SELECT
            EXTRACT(HOUR FROM entrance)                                        AS hour,
            COUNT(*)                                                           AS visitors,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                              AS conversion_rate_pct
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """).df().to_dict("records")

    # ── Day-of-week conversion ───────────────────────────────────────────────
    results["dow_conversion"] = conn.execute("""
        SELECT
            DAYNAME(entrance::DATE)                                            AS day_of_week,
            DAYOFWEEK(entrance::DATE)                                          AS dow_num,
            COUNT(*)                                                           AS visitors,
            ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                              AS conversion_rate_pct
        FROM tracks
        WHERE NOT is_staff AND entrance IS NOT NULL
        GROUP BY day_of_week, dow_num
        ORDER BY dow_num
    """).df().to_dict("records")

    # ── Journey depth: buyers vs non-buyers ─────────────────────────────────
    results["journey_depth"] = conn.execute("""
        SELECT
            is_buyer,
            ROUND(AVG(zone_count), 2)         AS avg_zones_visited,
            ROUND(AVG(poi_count), 2)           AS avg_pois_visited,
            ROUND(AVG(duration_seconds), 1)    AS avg_duration_seconds,
            ROUND(MEDIAN(duration_seconds), 1) AS median_duration_seconds
        FROM tracks
        WHERE NOT is_staff
        GROUP BY is_buyer
    """).df().to_dict("records")

    # ── Top zones visited by non-buyers only ────────────────────────────────
    results["nonbuyer_top_zones"] = conn.execute("""
        SELECT
            e.zone,
            COUNT(DISTINCT t.master_track_id) AS nonbuyer_visits
        FROM tracks t
        JOIN events e ON t.master_track_id = e.master_track_id
        WHERE NOT t.is_staff
          AND NOT t.is_buyer
          AND e.zone IS NOT NULL
          AND e.event_type = 'ZONE_ENTRY'
        GROUP BY e.zone
        ORDER BY nonbuyer_visits DESC
        LIMIT 15
    """).df().to_dict("records")

    return results
