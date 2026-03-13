"""
ETL: Unzip daily JSON files, flatten to two Parquet tables per day.
  - processed/tracks/tracks_YYYY-MM-DD.parquet  (one row per visitor track)
  - processed/events/events_YYYY-MM-DD.parquet  (one row per tracking event)
"""

import os
import zipfile
import json
import re
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
_scratch = os.environ.get("COUCHE_SCRATCH")
PROCESSED_DIR = Path(_scratch) / "processed" if _scratch else BASE_DIR / "processed"


def _parse_point(s):
    """Parse 'POINT(x y)' → (float, float) or (None, None)."""
    if not s:
        return None, None
    m = re.match(r"POINT\(([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\)", s)
    return (float(m.group(1)), float(m.group(2))) if m else (None, None)


def _date_from_name(name):
    """Extract YYYY-MM-DD from a filename."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", name)
    return m.group(1) if m else None


def process_zip(zip_path: Path):
    """
    Parse one zip file and return (tracks_df, events_df).
    Returns (None, None) if the zip contains no JSON.
    """
    with zipfile.ZipFile(zip_path) as zf:
        json_names = [n for n in zf.namelist() if n.lower().endswith(".json")]
        if not json_names:
            logger.warning("No JSON in %s", zip_path.name)
            return None, None
        with zf.open(json_names[0]) as f:
            data = json.load(f)

    date_str = _date_from_name(zip_path.name)
    sel = data.get("metadata", {}).get("selection_information", {})
    location = sel.get("location_name", "Unknown")

    track_rows, event_rows = [], []

    for t in data.get("master_tracks", []):
        tid = t["master_track_id"]
        track_rows.append(
            {
                "date": date_str,
                "location": location,
                "master_track_id": tid,
                "entrance": t.get("entrance"),
                "exit": t.get("exit"),
                "duration_seconds": t.get("duration_seconds"),
                "gender": t.get("gender"),
                "is_staff": bool(t.get("is_staff", False)),
                "is_buyer": bool(t.get("is_buyer", False)),
                "zone_count": t.get("zone_count", 0),
                "poi_count": t.get("poi_count", 0),
            }
        )
        for ev in t.get("master_track_details", []):
            cx, cy = _parse_point(ev.get("coordinate"))
            vx, vy = _parse_point(ev.get("view_direction"))
            event_rows.append(
                {
                    "date": date_str,
                    "master_track_id": tid,
                    "occurred_on": ev.get("occurred_on"),
                    "zone": ev.get("zone"),
                    "poi_name": ev.get("poi_name"),
                    "measured_height_m": ev.get("measured_height_m"),
                    "detected_gender": ev.get("detected_gender"),
                    "event_type": ev.get("event_type"),
                    "coord_x": cx,
                    "coord_y": cy,
                    "view_x": vx,
                    "view_y": vy,
                }
            )

    tracks_df = pd.DataFrame(track_rows)
    events_df = pd.DataFrame(event_rows)

    for col in ("entrance", "exit"):
        tracks_df[col] = pd.to_datetime(tracks_df[col], errors="coerce")
    events_df["occurred_on"] = pd.to_datetime(events_df["occurred_on"], errors="coerce")

    return tracks_df, events_df


def run_etl(force: bool = False):
    """Process every zip in DATA_DIR into Parquet files."""
    tracks_dir = PROCESSED_DIR / "tracks"
    events_dir = PROCESSED_DIR / "events"
    tracks_dir.mkdir(parents=True, exist_ok=True)
    events_dir.mkdir(parents=True, exist_ok=True)

    zip_files = sorted(DATA_DIR.glob("*.zip"))
    logger.info("Found %d zip files", len(zip_files))
    processed, skipped, errors = 0, 0, 0

    for zp in zip_files:
        date_str = _date_from_name(zp.name)
        if not date_str:
            continue

        t_out = tracks_dir / f"tracks_{date_str}.parquet"
        e_out = events_dir / f"events_{date_str}.parquet"

        if not force and t_out.exists() and e_out.exists():
            skipped += 1
            continue

        logger.info("Processing %s …", date_str)
        try:
            tracks_df, events_df = process_zip(zp)
            if tracks_df is not None:
                tracks_df.to_parquet(t_out, index=False)
                events_df.to_parquet(e_out, index=False)
                logger.info("  → %d tracks, %d events", len(tracks_df), len(events_df))
                processed += 1
        except Exception as exc:
            logger.error("Error processing %s: %s", zp.name, exc)
            errors += 1

    logger.info("ETL done. processed=%d  skipped=%d  errors=%d", processed, skipped, errors)


if __name__ == "__main__":
    run_etl()
