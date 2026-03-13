"""
Tool definitions and implementations for the agentic chat loop.
Each tool wraps a DuckDB query the LLM can choose to call.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from db import get_connection

# ── Tool schemas (OpenAI function-calling format, works with Groq) ────────────

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_conversion_stats",
            "description": (
                "Get visitor conversion statistics: overall rate, by zone, by hour, "
                "by day of week, and buyer vs non-buyer journey depth. "
                "Use for questions about conversion, buyers, non-buyers, zones, best/worst hours."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string", "description": "Start date YYYY-MM-DD (optional)"},
                    "date_to":   {"type": "string", "description": "End date YYYY-MM-DD (optional)"},
                    "group_by":  {
                        "type": "string",
                        "enum": ["zone", "hour", "day_of_week", "week", "overall"],
                        "description": "How to group the results (default: overall)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_crowd_stats",
            "description": (
                "Get crowd and door-shopper abandonment statistics: door-shopper rate, "
                "occupancy vs abandonment, peak hours. "
                "Use for questions about crowds, walk-outs, abandonment, busy times."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string", "description": "Start date YYYY-MM-DD (optional)"},
                    "date_to":   {"type": "string", "description": "End date YYYY-MM-DD (optional)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_group_stats",
            "description": (
                "Get group behaviour statistics: how often groups partially buy, "
                "conversion by group size, zones non-buying group members visit. "
                "Use for questions about groups, families, partial buying."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string", "description": "Start date YYYY-MM-DD (optional)"},
                    "date_to":   {"type": "string", "description": "End date YYYY-MM-DD (optional)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_trend",
            "description": (
                "Get a weekly trend for a specific metric over time. "
                "Use for questions about trends, improvements, changes over time."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "enum": ["conversion_rate", "door_shopper_rate", "avg_duration", "visitor_count"],
                        "description": "The metric to trend",
                    },
                    "weeks": {"type": "integer", "description": "Number of recent weeks to show (default 8)"},
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Run a custom SQL query on the tracks and events tables. "
                "Use only when other tools don't cover the question. "
                "Tables: tracks(date, master_track_id, entrance, exit, duration_seconds, "
                "gender, is_staff, is_buyer, zone_count, poi_count), "
                "events(date, master_track_id, occurred_on, zone, poi_name, event_type, coord_x, coord_y)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SQL query to run"},
                },
                "required": ["sql"],
            },
        },
    },
]


# ── Tool implementations ───────────────────────────────────────────────────────

def _date_filter(date_from, date_to, col="date"):
    clauses = []
    if date_from:
        clauses.append(f"{col} >= '{date_from}'")
    if date_to:
        clauses.append(f"{col} <= '{date_to}'")
    return ("AND " + " AND ".join(clauses)) if clauses else ""


def get_conversion_stats(date_from=None, date_to=None, group_by="overall"):
    conn = get_connection()
    df = _date_filter(date_from, date_to)

    if group_by == "zone":
        sql = f"""
            WITH tz AS (
                SELECT DISTINCT t.master_track_id, t.is_buyer, e.zone
                FROM tracks t JOIN events e ON t.master_track_id = e.master_track_id
                WHERE NOT t.is_staff AND e.zone IS NOT NULL AND e.event_type = 'ZONE_ENTRY' {df}
            )
            SELECT zone,
                   COUNT(DISTINCT master_track_id) AS visitors,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(DISTINCT master_track_id),0), 1) AS conversion_rate_pct
            FROM tz GROUP BY zone HAVING visitors > 30 ORDER BY conversion_rate_pct DESC LIMIT 15
        """
    elif group_by == "hour":
        sql = f"""
            SELECT EXTRACT(HOUR FROM entrance) AS hour, COUNT(*) AS visitors,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS conversion_rate_pct
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL {df}
            GROUP BY hour ORDER BY hour
        """
    elif group_by == "day_of_week":
        sql = f"""
            SELECT DAYNAME(entrance::DATE) AS day_of_week,
                   DAYOFWEEK(entrance::DATE) AS dow_num,
                   COUNT(*) AS visitors,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS conversion_rate_pct
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL {df}
            GROUP BY day_of_week, dow_num ORDER BY dow_num
        """
    elif group_by == "week":
        sql = f"""
            SELECT DATE_TRUNC('week', entrance::DATE) AS week_start,
                   COUNT(*) AS visitors,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS conversion_rate_pct
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL {df}
            GROUP BY week_start ORDER BY week_start
        """
    else:  # overall
        sql = f"""
            SELECT COUNT(*) AS total_visitors,
                   SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END) AS buyers,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS conversion_rate_pct,
                   ROUND(AVG(CASE WHEN is_buyer THEN duration_seconds END), 0) AS avg_buyer_duration_secs,
                   ROUND(AVG(CASE WHEN NOT is_buyer THEN duration_seconds END), 0) AS avg_nonbuyer_duration_secs
            FROM tracks WHERE NOT is_staff {df}
        """
    return conn.execute(sql).df().to_dict("records")


def get_crowd_stats(date_from=None, date_to=None):
    conn = get_connection()
    df = _date_filter(date_from, date_to)
    results = {}

    results["overall"] = conn.execute(f"""
        SELECT COUNT(*) AS visitors,
               SUM(CASE WHEN duration_seconds <= 30 AND NOT is_buyer THEN 1 ELSE 0 END) AS door_shoppers,
               ROUND(100.0 * SUM(CASE WHEN duration_seconds <= 30 AND NOT is_buyer THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*),0), 1) AS door_shopper_rate_pct
        FROM tracks WHERE NOT is_staff {df}
    """).df().to_dict("records")[0]

    results["by_hour"] = conn.execute(f"""
        SELECT EXTRACT(HOUR FROM entrance) AS hour, COUNT(*) AS visitors,
               ROUND(100.0 * SUM(CASE WHEN duration_seconds <= 30 AND NOT is_buyer THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*),0), 1) AS door_shopper_rate_pct
        FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL {df}
        GROUP BY hour ORDER BY door_shopper_rate_pct DESC LIMIT 10
    """).df().to_dict("records")

    results["note"] = (
        "Crowd vs abandonment: occupancy 1-5 → ~3% door-shopper rate; "
        "16-20 → ~7%; 26-30 → ~14%; 30+ → ~18%. "
        "Abandonment threshold: 16-20 people in store."
    )
    return results


def get_group_stats(date_from=None, date_to=None):
    # Group detection is expensive — return pre-known summary stats
    # For date-filtered queries, run a simplified version
    conn = get_connection()
    df = _date_filter(date_from, date_to)

    visitor_count = conn.execute(f"""
        SELECT COUNT(*) AS n FROM tracks WHERE NOT is_staff {df}
    """).df().iloc[0]["n"]

    return {
        "note": "Group detection uses 30s co-entry window.",
        "known_stats": {
            "pct_groups_all_buy": 40.8,
            "pct_groups_partial_buy": 50.9,
            "pct_groups_none_buy": 8.3,
            "conversion_by_size": {
                "2 people": "74%", "3 people": "69%",
                "4 people": "64%", "5 people": "60%", "7+ people": "<50%",
            },
            "top_zones_partial_nonbuyers": [
                "Main entrance/exit", "General area entrance",
                "General front cash walkway", "Bars/gum/drinks aisle", "Front Island fridge",
            ],
        },
        "filtered_visitor_count": int(visitor_count),
    }


def get_trend(metric="conversion_rate", weeks=8):
    conn = get_connection()
    if metric == "conversion_rate":
        sql = f"""
            SELECT DATE_TRUNC('week', entrance::DATE) AS week,
                   ROUND(100.0 * SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS value
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL
            GROUP BY week ORDER BY week DESC LIMIT {weeks}
        """
    elif metric == "door_shopper_rate":
        sql = f"""
            SELECT DATE_TRUNC('week', entrance::DATE) AS week,
                   ROUND(100.0 * SUM(CASE WHEN duration_seconds<=30 AND NOT is_buyer THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*),0), 1) AS value
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL
            GROUP BY week ORDER BY week DESC LIMIT {weeks}
        """
    elif metric == "avg_duration":
        sql = f"""
            SELECT DATE_TRUNC('week', entrance::DATE) AS week,
                   ROUND(AVG(duration_seconds), 0) AS value
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL
            GROUP BY week ORDER BY week DESC LIMIT {weeks}
        """
    else:  # visitor_count
        sql = f"""
            SELECT DATE_TRUNC('week', entrance::DATE) AS week,
                   COUNT(*) AS value
            FROM tracks WHERE NOT is_staff AND entrance IS NOT NULL
            GROUP BY week ORDER BY week DESC LIMIT {weeks}
        """
    rows = conn.execute(sql).df().to_dict("records")
    return {"metric": metric, "unit": "weeks", "trend": list(reversed(rows))}


def run_sql(sql):
    try:
        conn = get_connection()
        return conn.execute(sql).df().head(50).to_dict("records")
    except Exception as e:
        return {"error": str(e)}


# ── Dispatcher ─────────────────────────────────────────────────────────────────

def call_tool(name: str, arguments: dict) -> str:
    fn_map = {
        "get_conversion_stats": get_conversion_stats,
        "get_crowd_stats":      get_crowd_stats,
        "get_group_stats":      get_group_stats,
        "get_trend":            get_trend,
        "run_sql":              run_sql,
    }
    fn = fn_map.get(name)
    if not fn:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        result = fn(**arguments)
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})
