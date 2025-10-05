#!/usr/bin/env python3
"""
scrape.py

Usage:
    python collect_lamppost_weather_fields_only.py --gdb /path/to/your.gdb --out data/lamppost_data.csv

Output columns (exact names in CSV):
 - lamppost_id
 - measurement_datetime  (YYYY-MM-DDTHH:MM:SS)
 - air_temperature_c
 - relative_humidity_pct
 - device_height_m
 - lp_latitude
 - lp_longitude
 - source_url

Only the listed fields are kept. Matching of CSV columns from device is done using simple substring checks
(e.g. "Air temperature" in column name). English-only substring matching.
"""

import argparse
import io
import os
import sys
from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests

# Device URL preference order
DEVICE_URL_COLUMNS = ["DEVICE_04_DATA_URL", "DEVICE_02_DATA_URL", "DEVICE_01_DATA_URL"]

# What we look for in device CSV columns (simple substring search, English only)
SEARCH_KEYS = {
    "lamppost_id": ["Lamppost ID"],
    "measurement_year": ["Data measurement", "Year"],
    "measurement_month": ["Data measurement", "Month"],
    "measurement_day": ["Data measurement", "Day"],
    "measurement_hour": ["Data measurement", "Hour"],
    "measurement_minute": ["Data measurement", "Minute"],
    "measurement_second": ["Data measurement", "Second"],
    "air_temperature": ["Air temperature"],             # keep units column name as provided; we'll rename
    "relative_humidity": ["Relative humidity"],
    "device_height": ["Device height"],
}

# Output column names (final)
OUT_COLS = [
    "lamppost_id",
    "measurement_datetime",
    "air_temperature_c",
    "relative_humidity_pct",
    "device_height_m",
    "lp_latitude",
    "lp_longitude",
    "source_url",
]


def find_active_url_from_row(row: pd.Series):
    for c in DEVICE_URL_COLUMNS:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
            return str(row[c]).strip()
    return None


def try_parse_csv_bytes(content_bytes: bytes) -> pd.DataFrame:
    text = content_bytes.decode("utf-8")
    return pd.read_csv(io.StringIO(text), sep=',', engine="python")
 


def find_column_by_substring(columns, substrings):
    """
    Return the first column name in `columns` where ALL substrings appear (in that column name).
    substrings: list of strings (case-sensitive substring check to match user's CSV headers).
    If substrings is a single-element list, it's just that substring.
    Returns column name or None.
    """
    for col in columns:
        ok = True
        for s in substrings:
            if s not in col:
                ok = False
                break
        if ok:
            return col
    return None


def build_measurement_datetime_from_row_using_columns(row: pd.Series, cols_map: dict):
    """
    cols_map: map like {"year": colname_or_None, "month": ..., ...}
    If year/month/day present, return string "YYYY-MM-DDTHH:MM:SS" (no commas).
    If not enough pieces, return None.
    """
    try:
        y_col = cols_map.get("measurement_year")
        m_col = cols_map.get("measurement_month")
        d_col = cols_map.get("measurement_day")
        if not (y_col and m_col and d_col):
            return None

        def to_int_safe(v):
            try:
                if pd.isna(v):
                    return None
                return int(float(v))
            except Exception:
                return None

        y = to_int_safe(row.get(y_col))
        mo = to_int_safe(row.get(m_col))
        da = to_int_safe(row.get(d_col))
        if y is None or mo is None or da is None:
            return None

        h = to_int_safe(row.get(cols_map.get("measurement_hour"))) or 0
        mi = to_int_safe(row.get(cols_map.get("measurement_minute"))) or 0
        s = to_int_safe(row.get(cols_map.get("measurement_second"))) or 0

        dt = datetime(y, mo, da, h, mi, s)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def extract_fields_from_device_df(device_df: pd.DataFrame):
    """
    Given a parsed device_df (original column names), find the columns we want via substring matching.
    Returns a DataFrame with columns: lamppost_id, measurement_datetime, air_temperature_c, relative_humidity_pct, device_height_m
    There may be multiple rows in device_df — we keep them all (usually 1).
    """
    print(device_df.iloc[0,0])
    cols = list(device_df.columns)

    # Find each column name (original header) by checking substring presence
    found = {}
    # single-key finds
    found["lamppost_id"] = find_column_by_substring(cols, SEARCH_KEYS["lamppost_id"])
    found["air_temperature"] = find_column_by_substring(cols, SEARCH_KEYS["air_temperature"])
    found["relative_humidity"] = find_column_by_substring(cols, SEARCH_KEYS["relative_humidity"])
    found["device_height"] = find_column_by_substring(cols, SEARCH_KEYS["device_height"])
    # measurement pieces
    for k in ("measurement_year", "measurement_month", "measurement_day", "measurement_hour", "measurement_minute", "measurement_second"):
        found[k] = find_column_by_substring(cols, SEARCH_KEYS[k])

    # Build output rows
    out_rows = []
    for _, r in device_df.iterrows():
        lamppost_id = r.get(found["lamppost_id"]) if found["lamppost_id"] else None
        measurement_datetime = build_measurement_datetime_from_row_using_columns(r, found)
        air_temp = r.get(found["air_temperature"]) if found["air_temperature"] else None
        rel_hum = r.get(found["relative_humidity"]) if found["relative_humidity"] else None
        dev_height = r.get(found["device_height"]) if found["device_height"] else None

        out_rows.append({
            "lamppost_id": lamppost_id if pd.notna(lamppost_id) else None,
            "measurement_datetime": measurement_datetime,
            "air_temperature_c": float(air_temp) if (air_temp is not None and not pd.isna(air_temp)) else None,
            "relative_humidity_pct": float(rel_hum) if (rel_hum is not None and not pd.isna(rel_hum)) else None,
            "device_height_m": float(dev_height) if (dev_height is not None and not pd.isna(dev_height)) else None,
        })
    return pd.DataFrame(out_rows)


def find_gdb_latlon(gdf_row):
    """
    Search GDF columns for LP_LATITUDE and LP_LONGITUDE (case-insensitive),
    returns (lat, lon) or (None, None).
    """
    lat = None
    lon = None
    for c in gdf_row.index:
        if c.lower() == "lp_latitude":
            lat = gdf_row.get(c)
        if c.lower() == "lp_longitude":
            lon = gdf_row.get(c)
    return lat, lon


def main():
    p = argparse.ArgumentParser(description="Collect lamppost weather fields-only CSV")
    p.add_argument("--gdb", required=True, help="Path to geopackage/gdb containing smart_lamppost layer")
    p.add_argument("--out", required=True, help="Output CSV file (will create/append)")
    args = p.parse_args()

    gdb_path = args.gdb
    out_csv = args.out

    # read layer
    try:
        gdf = gpd.read_file(gdb_path, layer="smart_lamppost")
    except Exception as e:
        print("ERROR: cannot read layer 'smart_lamppost' from", gdb_path, ":", e)
        sys.exit(1)

    session = requests.Session()
    collected = []

    for idx, gdf_row in gdf.iterrows():
        url = find_active_url_from_row(gdf_row)
        if not url:
            print(f"[{idx}] no device url; skipping")
            continue

        print(f"[{idx}] fetching {url}")
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"[{idx}] request failed: {e}; skipping")
            continue

        try:
            device_df = try_parse_csv_bytes(resp.content)
        except Exception as e:
            print(f"[{idx}] parse failed: {e}; skipping")
            continue

        # extract only required fields from device df by substring matching
        extracted = extract_fields_from_device_df(device_df)
        if extracted.empty:
            print(f"[{idx}] no matching columns found in device CSV; skipping")
            continue

        # Add LP_LATITUDE / LP_LONGITUDE from gdb row (case-insensitive search)
        lat, lon = find_gdb_latlon(gdf_row)
        extracted["lp_latitude"] = lat if lat is not None and not pd.isna(lat) else None
        extracted["lp_longitude"] = lon if lon is not None and not pd.isna(lon) else None

        extracted["source_url"] = url

        # Ensure measurement_datetime is string or None; if None, will be left None
        collected.append(extracted)

    if not collected:
        print("No device rows collected; exiting.")
        return

    new_df = pd.concat(collected, ignore_index=True, sort=False)

    # Ensure consistent column order and names; create missing columns with None
    for col in OUT_COLS:
        if col not in new_df.columns:
            new_df[col] = None
    new_df = new_df[OUT_COLS]

    # Normalize measurement_datetime column: ensure string ISO or None
    new_df["measurement_datetime"] = pd.to_datetime(new_df["measurement_datetime"], errors="coerce").apply(
        lambda t: t.strftime("%Y-%m-%dT%H:%M:%S") if pd.notna(t) else None
    )

    # Combine with existing CSV if exists
    if os.path.exists(out_csv):
        try:
            existing = pd.read_csv(out_csv, dtype=str, low_memory=False)
            # Ensure same columns
            for col in OUT_COLS:
                if col not in existing.columns:
                    existing[col] = None
            existing = existing[OUT_COLS]
            # coerce types where appropriate
            combined = pd.concat([existing, new_df], ignore_index=True, sort=False)
        except Exception as e:
            print("WARNING: could not read existing CSV; overwriting. Error:", e)
            combined = new_df
    else:
        combined = new_df

    # Deduplicate by measurement_datetime + lamppost_id
    # Convert lamppost_id to string for stable comparison
    combined["lamppost_id"] = combined["lamppost_id"].astype(str).where(combined["lamppost_id"].notna(), None)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["measurement_datetime", "lamppost_id"], keep="first", ignore_index=True)
    after = len(combined)
    print(f"Dedup: removed {before - after} duplicates. Total rows now: {after}")

    # Save CSV (measurement_datetime contains no commas)
    combined.to_csv(out_csv, index=False)
    print("Saved combined CSV to:", out_csv)


if __name__ == "__main__":
    main()
