#!/usr/bin/env python3
"""
estimate_capacity.py — Estimate venue capacity from building footprints + ML.

Designed for the Awksion venue database pipeline.
Input:  indieonthemove_venues.csv (or any CSV with the same schema)
Output: indieonthemove_with_capacity.csv

Usage:
    python estimate_capacity.py                          # defaults
    python estimate_capacity.py --input my_venues.csv    # custom input
    python estimate_capacity.py --input big.csv --output big_with_cap.csv
"""

import argparse
import json
import logging
import math
import os
import re
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

# Geocoding
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# GIS
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
from shapely.ops import transform
import pyproj

# ML
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_predict, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────
DEFAULT_INPUT = "indieonthemove_venues.csv"
DEFAULT_OUTPUT = "indieonthemove_with_capacity.csv"
GEOCODE_CACHE = "geocode_cache.json"
FOOTPRINT_CACHE = "footprint_cache.json"

# ── Column mapping (actual CSV → internal names) ─────────────
# This makes the script adaptable: change this dict if column names differ.
COL_MAP = {
    "name": "Name",
    "address": "Address",
    "city": "City",
    "province": "State/Province",
    "zip": "Zip_Code",
    "categories": "Categories",
    "description": "Description",
    "capacity": "Capacity",
}

# ── Venue type classification ────────────────────────────────
TYPE_KEYWORDS = {
    "bar":        ["bar", "pub", "tavern", "taproom", "lounge", "saloon"],
    "club":       ["club", "nightclub", "night club", "dance"],
    "restaurant": ["restaurant", "cafe", "bistro", "diner", "grill", "eatery", "kitchen"],
    "theatre":    ["theatre", "theater", "concert hall", "opera", "arena", "amphitheatre", "auditorium"],
    "festival":   ["festival"],
    "gallery":    ["gallery", "art gallery"],
    "venue":      ["music venue", "venue", "rental"],
}

# Code factors (sq ft per person)
CODE_FACTORS = {
    "bar": 7, "club": 7, "theatre": 7,
    "restaurant": 15, "gallery": 12,
    "festival": 10, "venue": 10,
}

# Multi-tenant area thresholds (sq ft)
MT_THRESHOLDS = {
    "bar": 8_000, "club": 8_000,
    "restaurant": 15_000,
    "theatre": 50_000,
    "festival": 100_000,
    "gallery": 12_000, "venue": 12_000,
}

FRONT_OF_HOUSE = 0.60
MT_PENALTY_DIVISOR = 4
UNIT_KEYWORDS = re.compile(r"\b(unit|suite|floor|level|ste|apt)\b|#\d", re.I)


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 0 — Helpers                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def classify_venue(categories: str, description: str = "") -> str:
    """Return a venue type string from Categories + Description text."""
    text = f"{categories} {description}".lower()
    # Priority order matters: club > bar > restaurant > theatre > ...
    for vtype in ["club", "bar", "restaurant", "theatre", "festival", "gallery", "venue"]:
        if any(kw in text for kw in TYPE_KEYWORDS[vtype]):
            return vtype
    return "venue"  # default


def parse_capacity(val) -> int | None:
    """Parse a capacity value; return None if missing/invalid."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace(",", "")
    if s in ("", "N/A", "0", "0.0", "nan"):
        return None
    try:
        n = int(float(s))
        return n if n > 0 else None
    except ValueError:
        return None


def round_to_5(x):
    return int(round(x / 5) * 5) if pd.notna(x) and x > 0 else None


def load_json_cache(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_json_cache(data: dict, path: str):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 1 — Load & Split                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def load_and_split(input_path: str):
    log.info(f"Loading {input_path}...")
    df = pd.read_csv(input_path, encoding="utf-8")

    # Rename columns if needed via COL_MAP (validates they exist)
    for internal, actual in COL_MAP.items():
        if actual not in df.columns:
            log.warning(f"Column '{actual}' not found — will be treated as empty.")
            df[actual] = ""

    df["capacity_parsed"] = df[COL_MAP["capacity"]].apply(parse_capacity)
    df["venue_type"] = df.apply(
        lambda r: classify_venue(
            str(r.get(COL_MAP["categories"], "")),
            str(r.get(COL_MAP["description"], "")),
        ),
        axis=1,
    )

    has_cap = df["capacity_parsed"].notna()
    log.info(f"  Total venues: {len(df)}")
    log.info(f"  With known capacity: {has_cap.sum()}")
    log.info(f"  Missing capacity: {(~has_cap).sum()}")
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 2 — Geocode                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def build_address_string(row) -> str:
    parts = [
        str(row.get(COL_MAP["address"], "")).strip(),
    ]
    # If address already contains city, skip appending
    addr = parts[0]
    city = str(row.get(COL_MAP["city"], "")).strip()
    prov = str(row.get(COL_MAP["province"], "")).strip()
    if city and city.lower() not in addr.lower():
        parts.append(city)
    if prov and prov not in addr:
        parts.append(prov)
    parts.append("Canada")
    return ", ".join(p for p in parts if p and p != "N/A")


def geocode_venues(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Geocoding venues...")
    cache = load_json_cache(GEOCODE_CACHE)
    geolocator = Nominatim(user_agent="awksion_capacity_estimator/1.0", timeout=10)

    lats, lons = [], []
    new_cache_entries = 0

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Geocoding"):
        addr_str = build_address_string(row)
        name = str(row.get(COL_MAP["name"], ""))

        if addr_str in cache:
            lats.append(cache[addr_str].get("lat"))
            lons.append(cache[addr_str].get("lon"))
            continue

        lat, lon = None, None
        for attempt in range(3):
            try:
                time.sleep(1.1)
                loc = geolocator.geocode(addr_str)
                if loc:
                    lat, lon = loc.latitude, loc.longitude
                break
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                log.debug(f"  Retry {attempt+1} for {name}: {e}")
                time.sleep(2)

        cache[addr_str] = {"lat": lat, "lon": lon}
        new_cache_entries += 1
        lats.append(lat)
        lons.append(lon)

        # Periodic cache save
        if new_cache_entries % 20 == 0:
            save_json_cache(cache, GEOCODE_CACHE)

    save_json_cache(cache, GEOCODE_CACHE)
    df["lat"] = lats
    df["lon"] = lons
    geocoded = df["lat"].notna().sum()
    log.info(f"  Geocoded: {geocoded}/{len(df)}")
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 3 — Building Footprints                              ║
# ╚══════════════════════════════════════════════════════════════╝

def get_utm_crs(lat, lon):
    """Return the EPSG code for the appropriate UTM zone."""
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


def score_osm_match(tags: dict, venue_type: str) -> int:
    """Score how well an OSM building's tags match the venue type."""
    amenity = str(tags.get("amenity", "")).lower()
    building = str(tags.get("building", "")).lower()
    leisure = str(tags.get("leisure", "")).lower()
    all_tags = f"{amenity} {building} {leisure}"

    match_map = {
        "bar":        ["bar", "pub"],
        "club":       ["nightclub", "club"],
        "restaurant": ["restaurant", "cafe", "fast_food"],
        "theatre":    ["theatre", "cinema", "arts_centre"],
        "gallery":    ["gallery", "arts_centre"],
    }
    keywords = match_map.get(venue_type, [])
    return sum(1 for kw in keywords if kw in all_tags)


def retrieve_footprint(lat, lon, venue_type, address_str):
    """Get building footprint area in sq ft with multi-tenant detection."""
    if pd.isna(lat) or pd.isna(lon):
        return None, False, None

    point = Point(lon, lat)
    utm_crs = get_utm_crs(lat, lon)

    for radius in [50, 100, 200]:
        try:
            gdf = ox.features.features_from_point((lat, lon), tags={"building": True}, dist=radius)
            polys = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
            if len(polys) > 0:
                break
        except Exception:
            polys = gpd.GeoDataFrame()
            continue
    else:
        return None, False, None

    if polys.empty:
        return None, False, None

    # Project to UTM for accurate distance/area
    polys_utm = polys.to_crs(utm_crs)
    point_utm = gpd.GeoSeries([point], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
    buffer = point_utm.buffer(15)

    # Find intersecting polygons
    intersecting = polys_utm[polys_utm.geometry.intersects(buffer)]

    if len(intersecting) > 1:
        # Prefer best OSM tag match
        scores = intersecting.apply(
            lambda r: score_osm_match(r.to_dict(), venue_type), axis=1
        )
        best_score = scores.max()
        if best_score > 0:
            selected = intersecting.loc[scores.idxmax()]
        else:
            # Nearest centroid
            dists = intersecting.geometry.centroid.distance(point_utm)
            selected = intersecting.loc[dists.idxmin()]
    elif len(intersecting) == 1:
        selected = intersecting.iloc[0]
    else:
        # No intersection — fall back to nearest centroid
        dists = polys_utm.geometry.centroid.distance(point_utm)
        selected = polys_utm.loc[dists.idxmin()]

    area_m2 = selected.geometry.area
    area_sqft = area_m2 * 10.7639

    # Try to get building levels
    levels = None
    tags = selected.to_dict() if hasattr(selected, "to_dict") else {}
    for key in ["building:levels", "building_levels"]:
        if key in tags:
            try:
                levels = int(float(tags[key]))
            except (ValueError, TypeError):
                pass

    # Multi-tenant detection
    threshold = MT_THRESHOLDS.get(venue_type, 12_000)
    size_flag = area_sqft > threshold
    addr_flag = bool(UNIT_KEYWORDS.search(str(address_str)))
    mt_flag = size_flag or addr_flag

    raw_area = area_sqft
    if mt_flag:
        area_sqft = area_sqft / MT_PENALTY_DIVISOR

    return {
        "raw_area_sqft": round(raw_area, 1),
        "adj_area_sqft": round(area_sqft, 1),
        "mt_flag": mt_flag,
        "levels": levels,
    }, mt_flag, levels


def retrieve_all_footprints(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Retrieving building footprints...")
    cache = load_json_cache(FOOTPRINT_CACHE)

    raw_areas, adj_areas, mt_flags, levels_list = [], [], [], []
    new_entries = 0

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Footprints"):
        cache_key = f"{row['lat']}_{row['lon']}"
        name = str(row.get(COL_MAP["name"], ""))

        if cache_key in cache and cache_key != "None_None":
            entry = cache[cache_key]
            raw_areas.append(entry.get("raw_area_sqft"))
            adj_areas.append(entry.get("adj_area_sqft"))
            mt_flags.append(entry.get("mt_flag", False))
            levels_list.append(entry.get("levels"))
            continue

        if pd.isna(row["lat"]) or pd.isna(row["lon"]):
            raw_areas.append(None)
            adj_areas.append(None)
            mt_flags.append(False)
            levels_list.append(None)
            continue

        try:
            result, mt, lvl = retrieve_footprint(
                row["lat"], row["lon"], row["venue_type"],
                str(row.get(COL_MAP["address"], ""))
            )
        except Exception as e:
            log.debug(f"  Footprint error for {name}: {e}")
            result, mt, lvl = None, False, None

        if result:
            raw_areas.append(result["raw_area_sqft"])
            adj_areas.append(result["adj_area_sqft"])
            mt_flags.append(result["mt_flag"])
            levels_list.append(result.get("levels"))
            cache[cache_key] = result
        else:
            raw_areas.append(None)
            adj_areas.append(None)
            mt_flags.append(False)
            levels_list.append(None)
            cache[cache_key] = {"raw_area_sqft": None, "adj_area_sqft": None, "mt_flag": False, "levels": None}

        new_entries += 1
        if new_entries % 10 == 0:
            save_json_cache(cache, FOOTPRINT_CACHE)

    save_json_cache(cache, FOOTPRINT_CACHE)

    df["Raw_Building_Area_SqFt"] = raw_areas
    df["Building_Area_SqFt"] = adj_areas
    df["Multi_Tenant_Flag"] = mt_flags
    df["building_levels"] = levels_list

    got_area = df["Building_Area_SqFt"].notna().sum()
    mt_count = df["Multi_Tenant_Flag"].sum()
    log.info(f"  Footprints retrieved: {got_area}/{len(df)}")
    log.info(f"  Multi-tenant flagged: {mt_count}")
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 4 — Heuristic Capacity                               ║
# ╚══════════════════════════════════════════════════════════════╝

def compute_heuristic(row) -> float | None:
    area = row.get("Building_Area_SqFt")
    if pd.isna(area) or area is None or area <= 0:
        return None
    vtype = row.get("venue_type", "venue")
    factor = CODE_FACTORS.get(vtype, 10)
    levels = row.get("building_levels")
    if levels and levels > 1:
        return (area * FRONT_OF_HOUSE * min(levels, 2)) / factor
    return (area * FRONT_OF_HOUSE) / factor


def add_heuristic(df: pd.DataFrame) -> pd.DataFrame:
    df["heuristic_capacity"] = df.apply(compute_heuristic, axis=1)
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 5 — Train Model                                      ║
# ╚══════════════════════════════════════════════════════════════╝

def train_model(df: pd.DataFrame):
    """Train on venues with BOTH known capacity AND building area."""
    mask = df["capacity_parsed"].notna() & df["Building_Area_SqFt"].notna()
    train_df = df[mask].copy()

    if len(train_df) < 5:
        log.warning(f"  Only {len(train_df)} training samples — model may be unreliable.")
        if len(train_df) < 2:
            log.error("  Not enough data to train. Falling back to heuristics only.")
            return None, None

    # Features
    train_df["log_area"] = np.log1p(train_df["Building_Area_SqFt"])
    train_df["has_unit"] = train_df[COL_MAP["address"]].apply(
        lambda x: 1 if UNIT_KEYWORDS.search(str(x)) else 0
    )
    train_df["mt_binary"] = train_df["Multi_Tenant_Flag"].astype(int)

    feature_cols = ["log_area", "heuristic_capacity", "mt_binary", "has_unit"]
    cat_cols = ["venue_type", COL_MAP["province"]]

    # Fill NaN heuristic with 0 for model
    train_df["heuristic_capacity"] = train_df["heuristic_capacity"].fillna(0)

    y = np.log1p(train_df["capacity_parsed"].values)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", feature_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ]
    )

    pipeline = Pipeline([
        ("pre", preprocessor),
        ("gbr", GradientBoostingRegressor(
            n_estimators=100, max_depth=3, learning_rate=0.1,
            random_state=42, min_samples_leaf=2,
        )),
    ])

    X = train_df[feature_cols + cat_cols]

    # Cross-validate
    n_folds = min(5, len(train_df))
    if n_folds < 2:
        n_folds = 2

    cv_preds_log = cross_val_predict(pipeline, X, y, cv=n_folds)
    cv_preds = np.expm1(cv_preds_log)
    actuals = train_df["capacity_parsed"].values

    rmse = np.sqrt(np.mean((cv_preds - actuals) ** 2))
    r2_scores = cross_val_score(pipeline, X, y, cv=n_folds, scoring="r2")
    r2 = r2_scores.mean()
    mape = np.mean(np.abs((actuals - cv_preds) / np.clip(actuals, 1, None))) * 100

    log.info(f"  Model CV (n={len(train_df)}, {n_folds}-fold):")
    log.info(f"    R²:   {r2:.3f}")
    log.info(f"    RMSE: {rmse:.0f}")
    log.info(f"    MAPE: {mape:.1f}%")

    # Fit final model on all training data
    pipeline.fit(X, y)

    metrics = {"r2": r2, "rmse": rmse, "mape": mape, "n_train": len(train_df)}
    return pipeline, metrics


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 6 — Predict                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def predict_capacity(df: pd.DataFrame, model, metrics):
    # Compute type medians from training set for fallback
    train_mask = df["capacity_parsed"].notna()
    type_medians = df[train_mask].groupby("venue_type")["capacity_parsed"].median().to_dict()
    global_median = df.loc[train_mask, "capacity_parsed"].median()

    est_caps = []
    methods = []
    confidences = []

    feature_cols = ["log_area", "heuristic_capacity", "mt_binary", "has_unit"]
    cat_cols = ["venue_type", COL_MAP["province"]]

    for _, row in df.iterrows():
        # Skip venues that already have capacity
        if pd.notna(row["capacity_parsed"]):
            est_caps.append(row["capacity_parsed"])
            methods.append("known")
            confidences.append("known")
            continue

        has_area = pd.notna(row.get("Building_Area_SqFt")) and row["Building_Area_SqFt"] > 0

        if has_area and model is not None:
            # Model prediction
            pred_row = pd.DataFrame([{
                "log_area": np.log1p(row["Building_Area_SqFt"]),
                "heuristic_capacity": row.get("heuristic_capacity", 0) or 0,
                "mt_binary": int(row.get("Multi_Tenant_Flag", False)),
                "has_unit": 1 if UNIT_KEYWORDS.search(str(row.get(COL_MAP["address"], ""))) else 0,
                "venue_type": row["venue_type"],
                COL_MAP["province"]: row.get(COL_MAP["province"], ""),
            }])
            try:
                pred_log = model.predict(pred_row[feature_cols + cat_cols])[0]
                pred = np.expm1(pred_log)
                pred = max(pred, 10)  # floor
                est_caps.append(round_to_5(pred))
                methods.append("model")
                confidences.append("medium" if row.get("Multi_Tenant_Flag", False) else "high")
            except Exception:
                # Fallback to heuristic
                h = row.get("heuristic_capacity")
                if h and h > 0:
                    est_caps.append(round_to_5(h))
                    methods.append("heuristic_fallback")
                    confidences.append("medium")
                else:
                    med = type_medians.get(row["venue_type"], global_median)
                    est_caps.append(round_to_5(med) if med else None)
                    methods.append("type_median_fallback")
                    confidences.append("low")
        elif has_area:
            # No model but have area — use heuristic
            h = row.get("heuristic_capacity")
            if h and h > 0:
                est_caps.append(round_to_5(h))
                methods.append("heuristic_fallback")
                confidences.append("medium")
            else:
                est_caps.append(None)
                methods.append("unable")
                confidences.append("")
        else:
            # No area — type median fallback
            vtype = row["venue_type"]
            med = type_medians.get(vtype, global_median)
            if med and med > 0:
                est_caps.append(round_to_5(med))
                methods.append("type_median_fallback")
                confidences.append("low")
            else:
                est_caps.append(None)
                methods.append("unable")
                confidences.append("")

    df["Estimated_Capacity"] = est_caps
    df["Estimation_Method"] = methods
    df["Confidence"] = confidences
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 7 — Save & Report                                    ║
# ╚══════════════════════════════════════════════════════════════╝

def save_and_report(df: pd.DataFrame, output_path: str, metrics: dict | None):
    # Drop internal working columns from output
    drop_cols = ["capacity_parsed", "building_levels", "log_area", "mt_binary", "has_unit"]
    out = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
    out.to_csv(output_path, index=False, encoding="utf-8")

    print("\n" + "=" * 60)
    print("📊 CAPACITY ESTIMATION REPORT")
    print("=" * 60)
    total = len(df)
    known = (df["Estimation_Method"] == "known").sum()
    geocoded = df["lat"].notna().sum()
    has_fp = df["Building_Area_SqFt"].notna().sum()
    mt = df["Multi_Tenant_Flag"].sum()
    by_model = (df["Estimation_Method"] == "model").sum()
    by_heur = (df["Estimation_Method"] == "heuristic_fallback").sum()
    by_median = (df["Estimation_Method"] == "type_median_fallback").sum()
    unable = (df["Estimation_Method"] == "unable").sum()

    print(f"  Total venues:             {total}")
    print(f"  Known capacity:           {known}")
    print(f"  Geocoded:                 {geocoded}")
    print(f"  Building footprint found: {has_fp}")
    print(f"  Multi-tenant flagged:     {mt}")
    print()
    if metrics:
        print(f"  Model R²:   {metrics['r2']:.3f}")
        print(f"  Model RMSE: {metrics['rmse']:.0f}")
        print(f"  Model MAPE: {metrics['mape']:.1f}%")
        print(f"  Training samples: {metrics['n_train']}")
    else:
        print("  ⚠️ Model not trained (insufficient data). Used heuristics only.")
    print()
    print(f"  Predicted via model:      {by_model}")
    print(f"  Predicted via heuristic:  {by_heur}")
    print(f"  Predicted via median:     {by_median}")
    print(f"  Unable to estimate:       {unable}")
    print(f"\n  Output: {output_path}")
    print("=" * 60)


# ╔══════════════════════════════════════════════════════════════╗
# ║  MAIN                                                       ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(description="Estimate venue capacity from building footprints + ML.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input CSV path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path")
    parser.add_argument("--skip-geocode", action="store_true", help="Skip geocoding (use cache only)")
    parser.add_argument("--skip-footprint", action="store_true", help="Skip footprint retrieval (use cache only)")
    args = parser.parse_args()

    # Step 1
    df = load_and_split(args.input)

    # Step 2
    if args.skip_geocode:
        log.info("Skipping geocoding (loading from cache)...")
        cache = load_json_cache(GEOCODE_CACHE)
        lats, lons = [], []
        for _, row in df.iterrows():
            addr_str = build_address_string(row)
            entry = cache.get(addr_str, {})
            lats.append(entry.get("lat"))
            lons.append(entry.get("lon"))
        df["lat"] = lats
        df["lon"] = lons
    else:
        df = geocode_venues(df)

    # Step 3
    if args.skip_footprint:
        log.info("Skipping footprint retrieval (loading from cache)...")
        cache = load_json_cache(FOOTPRINT_CACHE)
        raw_a, adj_a, mt_f, lvl = [], [], [], []
        for _, row in df.iterrows():
            key = f"{row['lat']}_{row['lon']}"
            entry = cache.get(key, {})
            raw_a.append(entry.get("raw_area_sqft"))
            adj_a.append(entry.get("adj_area_sqft"))
            mt_f.append(entry.get("mt_flag", False))
            lvl.append(entry.get("levels"))
        df["Raw_Building_Area_SqFt"] = raw_a
        df["Building_Area_SqFt"] = adj_a
        df["Multi_Tenant_Flag"] = mt_f
        df["building_levels"] = lvl
    else:
        df = retrieve_all_footprints(df)

    # Step 4
    df = add_heuristic(df)

    # Step 5
    model, metrics = train_model(df)

    # Step 6
    df = predict_capacity(df, model, metrics)

    # Step 7
    save_and_report(df, args.output, metrics)

if __name__ == "__main__":
    main()
    