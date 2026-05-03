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
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import cross_val_predict, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────
DEFAULT_INPUT = "../data/indieonthemove_venues.csv"
DEFAULT_OUTPUT = "../data/indieonthemove_with_capacity.csv"
GEOCODE_CACHE = "../data/geocode_cache.json"
FOOTPRINT_CACHE = "../data/footprint_cache.json"

# ── Column mapping (actual CSV → internal names) ─────────────
COL_MAP = {
    "name": "Name",
    "address": "Address",
    "city": "City",
    "province": "State/Province",
    "zip": "Zip_Code",
    "categories": "Categories",
    "genres": "Genres",
    "description": "Description",
    "capacity": "Capacity",
    "phone": "Phone",
    "website": "Website",
    "booking": "Booking_Info",
    "events": "Upcoming_Events",
    "age": "Age_Restriction",
    "source_province": "Source_Province_Search",
}

# ── Venue type classification ────────────────────────────────
TYPE_KEYWORDS = {
    "bar":        ["bar", "pub", "tavern", "taproom", "lounge", "saloon", "alehouse"],
    "club":       ["club", "nightclub", "night club", "dance"],
    "restaurant": ["restaurant", "cafe", "café", "bistro", "diner", "grill",
                   "eatery", "kitchen", "coffee", "pizzeria"],
    "theatre":    ["theatre", "theater", "concert hall", "opera", "arena",
                   "amphitheatre", "auditorium", "playhouse"],
    "festival":   ["festival", "fest"],
    "gallery":    ["gallery", "art gallery", "arts centre", "arts center"],
    "venue":      ["music venue", "venue", "rental", "ballroom", "hall"],
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

# ── Province normalization (handles 2-letter codes + full names) ─
PROVINCE_NORMALIZATION = {
    "ON": "Ontario", "QC": "Quebec", "BC": "British Columbia",
    "AB": "Alberta", "MB": "Manitoba", "NS": "Nova Scotia",
    "NB": "New Brunswick", "SK": "Saskatchewan",
    "NL": "Newfoundland and Labrador", "NF": "Newfoundland and Labrador",
    "PE": "Prince Edward Island", "PEI": "Prince Edward Island",
    "YT": "Yukon", "NT": "Northwest Territories", "NU": "Nunavut",
}

# ── City tiers (major metro / mid-size / small-rural) ────────
# 1 = major metro (CMA > 500k), 2 = mid-size (~100-500k), 3 = small/rural
_CITY_TIER_1 = {
    "toronto", "montreal", "montréal", "vancouver", "calgary", "edmonton",
    "ottawa", "winnipeg", "quebec city", "québec", "quebec",
    "hamilton", "kitchener", "london", "halifax", "victoria", "windsor",
    "saskatoon", "regina", "st. catharines", "st catharines",
    "mississauga", "brampton", "surrey", "laval", "markham", "vaughan",
    "gatineau", "longueuil", "burnaby", "richmond", "oakville", "burlington",
    "barrie", "oshawa", "sherbrooke", "kelowna", "abbotsford",
}
_CITY_TIER_2 = {
    "kingston", "trois-rivieres", "trois-rivières", "guelph", "cambridge",
    "whitby", "ajax", "milton", "moncton", "saint john", "fredericton",
    "thunder bay", "sudbury", "kanata", "nanaimo", "lethbridge",
    "peterborough", "st. john's", "st johns", "saint john's",
    "red deer", "medicine hat", "kamloops", "chilliwack", "drummondville",
    "saint-jérôme", "saint-jerome", "saguenay", "lévis", "levis",
    "saanich", "richmond hill", "north vancouver", "west vancouver",
    "new westminster", "coquitlam", "delta", "saanich", "saint-hyacinthe",
    "shawinigan", "rimouski", "granby", "victoriaville", "salaberry-de-valleyfield",
    "brandon", "prince albert", "moose jaw", "charlottetown", "sydney",
    "cape breton", "truro", "new glasgow", "yellowknife", "whitehorse",
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 0 — Helpers                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def classify_venue(categories: str, description: str = "") -> str:
    """Return a venue type string from Categories + Description text."""
    text = f"{categories} {description}".lower()
    for vtype in ["club", "bar", "restaurant", "theatre", "festival", "gallery", "venue"]:
        if any(kw in text for kw in TYPE_KEYWORDS[vtype]):
            return vtype
    return "venue"


def parse_capacity(val) -> int | None:
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


def normalize_province(p) -> str:
    """Map '2-letter codes' and full names to a single canonical name."""
    if not p or (isinstance(p, float) and math.isnan(p)):
        return ""
    s = str(p).strip()
    if not s or s == "N/A":
        return ""
    return PROVINCE_NORMALIZATION.get(s.upper(), s)


def city_tier(city) -> int:
    """1 = major metro, 2 = mid-size, 3 = small/rural."""
    if not city or (isinstance(city, float) and math.isnan(city)):
        return 3
    key = str(city).strip().lower()
    if key in _CITY_TIER_1:
        return 1
    if key in _CITY_TIER_2:
        return 2
    return 3


def count_items(text, sep: str = ",") -> int:
    if not text or (isinstance(text, float) and math.isnan(text)):
        return 0
    s = str(text).strip()
    if not s or s == "N/A":
        return 0
    return sum(1 for x in s.split(sep) if x.strip())


def text_len(text) -> int:
    if not text or (isinstance(text, float) and math.isnan(text)):
        return 0
    s = str(text).strip()
    if not s or s == "N/A":
        return 0
    return len(s)


# Patterns that suggest a number is a venue capacity, not a year/address.
_CAPACITY_PATTERNS = [
    re.compile(r"capacity\s*(?:of|is|:|=)?\s*(\d{2,5})", re.I),
    re.compile(r"(\d{2,5})[-\s]*(?:person|people|seat|cap)\s*(?:capacity|venue|hall|theatre)?", re.I),
    re.compile(r"holds?\s*(?:up\s*to\s*)?(\d{2,5})\s*(?:people|persons|guests|patrons)", re.I),
    re.compile(r"seats?\s*(?:up\s*to\s*)?(\d{2,5})", re.I),
    re.compile(r"(\d{2,5})[-\s]*seat\s*(?:venue|theatre|theater|hall|auditorium|room)", re.I),
    re.compile(r"room\s*for\s*(\d{2,5})", re.I),
    re.compile(r"accommodat(?:e|es|ing)\s*(?:up\s*to\s*)?(\d{2,5})", re.I),
    re.compile(r"audience\s*of\s*(?:up\s*to\s*)?(\d{2,5})", re.I),
]


def extract_capacity_from_text(text) -> int | None:
    """Return the largest plausible capacity number found in description text."""
    if not text or (isinstance(text, float) and math.isnan(text)):
        return None
    s = str(text)
    if not s or s == "N/A":
        return None
    candidates = []
    for pat in _CAPACITY_PATTERNS:
        for m in pat.finditer(s):
            try:
                n = int(m.group(1))
                if 20 <= n <= 30000:
                    candidates.append(n)
            except (ValueError, IndexError):
                pass
    return max(candidates) if candidates else None


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 1 — Load, Parse, Engineer Features                  ║
# ╚══════════════════════════════════════════════════════════════╝

def load_and_split(input_path: str):
    log.info(f"Loading {input_path}...")
    df = pd.read_csv(input_path, encoding="utf-8")

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


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive extra ML features from existing CSV columns."""
    df["n_genres"] = df[COL_MAP["genres"]].apply(count_items)
    df["n_categories"] = df[COL_MAP["categories"]].apply(count_items)

    desc = df[COL_MAP["description"]]
    df["description_length"] = desc.apply(text_len)
    df["log_desc_len"] = np.log1p(df["description_length"])

    df["has_website"] = (df[COL_MAP["website"]].astype(str).str.strip() != "N/A").astype(int)
    df["has_phone"] = (df[COL_MAP["phone"]].astype(str).str.strip() != "N/A").astype(int)

    booking = df[COL_MAP["booking"]].astype(str).str.lower()
    df["requires_premium"] = booking.str.contains("premium").astype(int)

    df["n_events"] = df[COL_MAP["events"]].apply(lambda x: count_items(x, sep=";"))

    age = df[COL_MAP["age"]].astype(str)
    df["is_age_restricted"] = age.str.match(r"^\s*\d+\+", na=False).astype(int)

    df["desc_capacity_hint"] = desc.apply(extract_capacity_from_text)

    df["city_tier"] = df[COL_MAP["city"]].apply(city_tier)

    src_norm = df[COL_MAP["source_province"]].apply(normalize_province)
    fallback_norm = df[COL_MAP["province"]].apply(normalize_province)
    df["province_norm"] = src_norm.where(src_norm != "", fallback_norm)

    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 2 — Geocode                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def build_address_string(row) -> str:
    parts = [str(row.get(COL_MAP["address"], "")).strip()]
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

    lats, lons = [], []
    cache_hits = 0
    cache_misses = 0

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Geocoding"):
        addr_str = build_address_string(row)

        if addr_str in cache:
            lats.append(cache[addr_str].get("lat"))
            lons.append(cache[addr_str].get("lon"))
            cache_hits += 1
        else:
            lats.append(None)
            lons.append(None)
            cache_misses += 1

    df["lat"] = lats
    df["lon"] = lons
    geocoded = df["lat"].notna().sum()
    log.info(f"  Geocoded: {geocoded}/{len(df)} (cache hits: {cache_hits}, misses: {cache_misses})")
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 3 — Building Footprints                              ║
# ╚══════════════════════════════════════════════════════════════╝

def get_utm_crs(lat, lon):
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


def score_osm_match(tags: dict, venue_type: str) -> int:
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
    if pd.isna(lat) or pd.isna(lon):
        return None, False, None

    point = Point(lon, lat)
    utm_crs = get_utm_crs(lat, lon)

    polys = gpd.GeoDataFrame()
    for radius in [50, 100, 200]:
        try:
            gdf = ox.features.features_from_point((lat, lon), tags={"building": True}, dist=radius)
            polys = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
            if len(polys) > 0:
                break
        except Exception:
            polys = gpd.GeoDataFrame()
            continue

    if polys.empty:
        return None, False, None

    polys_utm = polys.to_crs(utm_crs)
    point_utm = gpd.GeoSeries([point], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
    buffer = point_utm.buffer(15)
    intersecting = polys_utm[polys_utm.geometry.intersects(buffer)]

    if len(intersecting) > 1:
        scores = intersecting.apply(lambda r: score_osm_match(r.to_dict(), venue_type), axis=1)
        if scores.max() > 0:
            selected = intersecting.loc[scores.idxmax()]
        else:
            dists = intersecting.geometry.centroid.distance(point_utm)
            selected = intersecting.loc[dists.idxmin()]
    elif len(intersecting) == 1:
        selected = intersecting.iloc[0]
    else:
        dists = polys_utm.geometry.centroid.distance(point_utm)
        selected = polys_utm.loc[dists.idxmin()]

    area_m2 = selected.geometry.area
    area_sqft = area_m2 * 10.7639

    levels = None
    tags = selected.to_dict() if hasattr(selected, "to_dict") else {}
    for key in ["building:levels", "building_levels"]:
        if key in tags:
            try:
                levels = int(float(tags[key]))
            except (ValueError, TypeError):
                pass

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
            cache[cache_key] = {"raw_area_sqft": None, "adj_area_sqft": None,
                                "mt_flag": False, "levels": None}

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
# ║  STEP 5 — Train Model (with quantile intervals)            ║
# ╚══════════════════════════════════════════════════════════════╝

NUM_FEATURES = [
    "log_area",
    "heuristic_capacity",
    "n_genres",
    "n_categories",
    "log_desc_len",
    "has_website",
    "has_phone",
    "requires_premium",
    "n_events",
    "is_age_restricted",
    "city_tier",
    "desc_capacity_hint",
]

CAT_FEATURES = ["venue_type", "province_norm"]


def _build_pipeline(loss="squared_error", quantile=None):
    pre = ColumnTransformer([
        ("num", "passthrough", NUM_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT_FEATURES),
    ])
    kwargs = dict(
        loss=loss,
        max_iter=250,
        max_depth=4,
        learning_rate=0.07,
        min_samples_leaf=3,
        l2_regularization=0.5,
        random_state=42,
    )
    if quantile is not None:
        kwargs["quantile"] = quantile
    return Pipeline([("pre", pre), ("hgb", HistGradientBoostingRegressor(**kwargs))])


def _filter_outliers(train_df: pd.DataFrame) -> pd.DataFrame:
    """Drop obvious bad rows from training: very small / huge / IQR outliers in log-space."""
    cap = train_df["capacity_parsed"]
    mask = (cap >= 20) & (cap <= 30000)
    train_df = train_df[mask]
    if len(train_df) > 25:
        log_cap = np.log1p(train_df["capacity_parsed"])
        q1, q3 = log_cap.quantile([0.05, 0.95])
        iqr = q3 - q1
        train_df = train_df[(log_cap >= q1 - 1.5 * iqr) & (log_cap <= q3 + 1.5 * iqr)]
    return train_df


def train_model(df: pd.DataFrame):
    """Train P50 + P10/P90 quantile models for capacity prediction."""
    mask = df["capacity_parsed"].notna() & df["Building_Area_SqFt"].notna()
    train_df = df[mask].copy()
    train_df = _filter_outliers(train_df)

    if len(train_df) < 5:
        log.warning(f"  Only {len(train_df)} training samples — model may be unreliable.")
        if len(train_df) < 2:
            log.error("  Not enough data to train. Falling back to heuristics only.")
            return None, None, None, None

    train_df["log_area"] = np.log1p(train_df["Building_Area_SqFt"])

    X = train_df[NUM_FEATURES + CAT_FEATURES]
    y = np.log1p(train_df["capacity_parsed"].values)

    main_pipeline = _build_pipeline(loss="squared_error")

    n_folds = min(5, max(2, len(train_df) // 4))
    try:
        cv_preds_log = cross_val_predict(main_pipeline, X, y, cv=n_folds)
        cv_preds = np.expm1(cv_preds_log)
        actuals = train_df["capacity_parsed"].values
        rmse = float(np.sqrt(np.mean((cv_preds - actuals) ** 2)))
        r2_scores = cross_val_score(main_pipeline, X, y, cv=n_folds, scoring="r2")
        r2 = float(r2_scores.mean())
        mape = float(np.mean(np.abs((actuals - cv_preds) / np.clip(actuals, 1, None))) * 100)
    except Exception as e:
        log.warning(f"  CV failed ({e}); skipping cross-validation metrics.")
        rmse = r2 = mape = float("nan")

    log.info(f"  Model CV (n={len(train_df)}, {n_folds}-fold):")
    log.info(f"    R²:   {r2:.3f}")
    log.info(f"    RMSE: {rmse:.0f}")
    log.info(f"    MAPE: {mape:.1f}%")

    main_pipeline.fit(X, y)

    low_pipeline = _build_pipeline(loss="quantile", quantile=0.10)
    high_pipeline = _build_pipeline(loss="quantile", quantile=0.90)
    try:
        low_pipeline.fit(X, y)
        high_pipeline.fit(X, y)
    except Exception as e:
        log.warning(f"  Quantile models failed: {e}")
        low_pipeline = high_pipeline = None

    metrics = {"r2": r2, "rmse": rmse, "mape": mape, "n_train": len(train_df)}
    return main_pipeline, low_pipeline, high_pipeline, metrics


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 6 — Predict (with intervals + tiered fallback)       ║
# ╚══════════════════════════════════════════════════════════════╝

def _safe_predict(pipeline, row_df):
    try:
        return float(np.expm1(pipeline.predict(row_df)[0]))
    except Exception:
        return None


def predict_capacity(df: pd.DataFrame, main_model, low_model, high_model, metrics):
    train_mask = df["capacity_parsed"].notna()
    prov_type_med = (
        df[train_mask].groupby(["province_norm", "venue_type"])["capacity_parsed"]
        .median().to_dict()
    )
    type_med = df[train_mask].groupby("venue_type")["capacity_parsed"].median().to_dict()
    global_med = df.loc[train_mask, "capacity_parsed"].median()

    df["log_area"] = df["Building_Area_SqFt"].apply(
        lambda x: np.log1p(x) if pd.notna(x) and x > 0 else np.nan
    )

    est_mid, est_low, est_high = [], [], []
    methods, confidences = [], []

    for _, row in df.iterrows():
        if pd.notna(row["capacity_parsed"]):
            cap = row["capacity_parsed"]
            est_mid.append(cap)
            est_low.append(cap)
            est_high.append(cap)
            methods.append("known")
            confidences.append("known")
            continue

        has_area = pd.notna(row.get("Building_Area_SqFt")) and row["Building_Area_SqFt"] > 0
        pred_row = pd.DataFrame([{c: row.get(c) for c in NUM_FEATURES + CAT_FEATURES}])

        if has_area and main_model is not None:
            mid = _safe_predict(main_model, pred_row)
            if mid is not None:
                mid = max(mid, 10)
                low = _safe_predict(low_model, pred_row) if low_model is not None else None
                high = _safe_predict(high_model, pred_row) if high_model is not None else None
                if low is None or low > mid:
                    low = mid * 0.65
                if high is None or high < mid:
                    high = mid * 1.55
                low = max(low, 5)
                high = max(high, low)

                est_mid.append(round_to_5(mid))
                est_low.append(round_to_5(low))
                est_high.append(round_to_5(high))
                methods.append("model")
                conf = "high"
                if row.get("Multi_Tenant_Flag", False):
                    conf = "medium"
                confidences.append(conf)
                continue

        # Heuristic fallback (footprint exists but model unavailable / failed)
        if has_area:
            h = row.get("heuristic_capacity")
            if h and h > 0:
                est_mid.append(round_to_5(h))
                est_low.append(round_to_5(h * 0.6))
                est_high.append(round_to_5(h * 1.7))
                methods.append("heuristic_fallback")
                confidences.append("medium")
                continue

        # Description-only fallback (no footprint, but text mentioned a number)
        hint = row.get("desc_capacity_hint")
        if hint and not pd.isna(hint) and hint > 0:
            est_mid.append(round_to_5(hint))
            est_low.append(round_to_5(hint * 0.7))
            est_high.append(round_to_5(hint * 1.4))
            methods.append("description_hint")
            confidences.append("medium")
            continue

        # Per-province × type median, then type median, then global median
        key = (row.get("province_norm", ""), row.get("venue_type", ""))
        med = prov_type_med.get(key)
        if med is None or pd.isna(med):
            med = type_med.get(row.get("venue_type", ""), global_med)
        if med and not pd.isna(med) and med > 0:
            est_mid.append(round_to_5(med))
            est_low.append(round_to_5(med * 0.4))
            est_high.append(round_to_5(med * 2.5))
            methods.append("province_type_median")
            confidences.append("low")
        else:
            est_mid.append(None)
            est_low.append(None)
            est_high.append(None)
            methods.append("unable")
            confidences.append("")

    df["Estimated_Capacity"] = est_mid
    df["Estimated_Capacity_Low"] = est_low
    df["Estimated_Capacity_High"] = est_high
    df["Estimation_Method"] = methods
    df["Confidence"] = confidences
    return df


# ╔══════════════════════════════════════════════════════════════╗
# ║  STEP 7 — Save & Report                                    ║
# ╚══════════════════════════════════════════════════════════════╝

def save_and_report(df: pd.DataFrame, output_path: str, metrics: dict | None):
    drop_cols = [
        "capacity_parsed", "building_levels", "log_area",
        "n_genres", "n_categories", "description_length", "log_desc_len",
        "has_website", "has_phone", "requires_premium", "n_events",
        "is_age_restricted", "city_tier", "desc_capacity_hint", "province_norm",
    ]
    out = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
    out.to_csv(output_path, index=False, encoding="utf-8")

    print("\n" + "=" * 60)
    print("CAPACITY ESTIMATION REPORT")
    print("=" * 60)
    total = len(df)
    known = (df["Estimation_Method"] == "known").sum()
    geocoded = df["lat"].notna().sum()
    has_fp = df["Building_Area_SqFt"].notna().sum()
    mt = df["Multi_Tenant_Flag"].sum()
    by_model = (df["Estimation_Method"] == "model").sum()
    by_heur = (df["Estimation_Method"] == "heuristic_fallback").sum()
    by_desc = (df["Estimation_Method"] == "description_hint").sum()
    by_median = (df["Estimation_Method"] == "province_type_median").sum()
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
        print("  [!] Model not trained (insufficient data). Used heuristics only.")
    print()
    print(f"  Predicted via model:        {by_model}")
    print(f"  Predicted via heuristic:    {by_heur}")
    print(f"  Predicted via desc hint:    {by_desc}")
    print(f"  Predicted via prov×type:    {by_median}")
    print(f"  Unable to estimate:         {unable}")

    # Per-province breakdown
    if "province_norm" in df.columns or COL_MAP["province"] in df.columns:
        print()
        print("  By province (count of estimates):")
        prov_col = "province_norm" if "province_norm" in df.columns else COL_MAP["province"]
        # Re-derive in case it was dropped
        prov_series = df.get(prov_col)
        if prov_series is None:
            prov_series = df[COL_MAP["province"]].apply(normalize_province)
        for prov, n in prov_series.value_counts().head(15).items():
            print(f"    {prov or '(unknown)':<30} {n}")

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

    df = load_and_split(args.input)
    df = add_features(df)

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

    df = add_heuristic(df)

    main_model, low_model, high_model, metrics = train_model(df)

    df = predict_capacity(df, main_model, low_model, high_model, metrics)

    save_and_report(df, args.output, metrics)


if __name__ == "__main__":
    main()
