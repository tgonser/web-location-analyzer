import os
import json
import time
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from csv_exporter import export_monthly_csv
from geo_utils import reverse_geocode, haversine_distance, is_over_water, geo_cache, save_geo_cache
import pandas as pd
import math

# Ensure config directory exists and set cache file path
os.makedirs('config', exist_ok=True)
cache_file = "config/geo_cache.json"

def process_location_file(file_path, start_date, end_date, output_dir, group_by,
                         geoapify_key, google_key, onwater_key, delay, batch_size,
                         log_func, cancel_check, include_distance=True):
    log_func(f"📂 Loading: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log_func(f"❌ Error loading file: {e}")
        return None

    if isinstance(data, dict):
        timeline_objects = data.get("timelineObjects", data)
    else:
        timeline_objects = data

    coords = []
    activity_blocks = []
    unique_coords = set()
    timestamps = set()
    modes_seen = set()

    for obj in timeline_objects:
        if cancel_check():
            log_func("❌ Canceled during parsing.")
            return None

        if "activity" in obj:
            activity = obj["activity"]
            start_str = obj.get("startTime")
            end_str = obj.get("endTime")
            if start_str and end_str and activity.get("start", "").startswith("geo:"):
                latlon = activity["start"].replace("geo:", "").split(",")
                if len(latlon) == 2:
                    try:
                        lat = float(latlon[0])
                        lon = float(latlon[1])
                        dt = pd.to_datetime(start_str, utc=True)
                        if not (start_date <= dt.date() <= end_date):
                            continue
                        mode = activity.get("topCandidate", {}).get("type", "unknown").lower()
                        modes_seen.add(mode)
                        coord_key = (round(lat, 5), round(lon, 5), round(dt.timestamp() / 600))
                        if coord_key not in unique_coords:
                            coords.append((dt, lat, lon))
                            unique_coords.add(coord_key)
                            timestamps.add(dt.isoformat())
                            activity_blocks.append({"mode": mode})
                    except Exception:
                        continue

        elif "timelinePath" in obj:
            start_str = obj.get("startTime")
            if start_str:
                start_dt = pd.to_datetime(start_str, utc=True)
                if not (start_date <= start_dt.date() <= end_date):
                    continue
                last_offset = None
                for point in obj["timelinePath"]:
                    if cancel_check():
                        log_func("❌ Canceled during parsing.")
                        return None
                    if "point" in point and point["point"].startswith("geo:"):
                        latlon = point["point"].replace("geo:", "").split(",")
                        if len(latlon) == 2:
                            try:
                                lat = float(latlon[0])
                                lon = float(latlon[1])
                                offset = float(point.get("durationMinutesOffsetFromStartTime", 0))
                                if last_offset is None or offset >= last_offset + 10:
                                    point_dt = start_dt + timedelta(minutes=offset)
                                    if not (start_date <= point_dt.date() <= end_date):
                                        continue
                                    mode = point.get("mode", point.get("type", "unknown")).lower()
                                    modes_seen.add(mode)
                                    coord_key = (round(lat, 5), round(lon, 5), round(point_dt.timestamp() / 600))
                                    if coord_key not in unique_coords:
                                        coords.append((point_dt, lat, lon))
                                        unique_coords.add(coord_key)
                                        timestamps.add(point_dt.isoformat())
                                        activity_blocks.append({"mode": mode})
                                        last_offset = offset
                            except Exception:
                                continue

    log_func(f"📍 Parsed {len(coords)} location points, {len(unique_coords)} unique coordinates, {len(timestamps)} unique timestamps.")
    log_func(f"📅 Timestamps: {sorted(list(timestamps))[:5]} ... (showing first 5)")
    log_func(f"📊 Modes seen: {sorted(list(modes_seen))}")
    if not coords:
        log_func("⚠️ No location data found.")
        return None

    # Stricter deduplication
    deduped_coords = []
    deduped_activity_blocks = []
    last_coord = None
    last_dt = None
    for i, ((dt, lat, lon), act) in enumerate(zip(coords, activity_blocks)):
        if last_coord is None or haversine_distance(last_coord[0], last_coord[1], lat, lon) > 0.124274 or (dt - last_dt).total_seconds() > 600:
            deduped_coords.append((dt, lat, lon))
            deduped_activity_blocks.append(act)
            last_coord = (lat, lon)
            last_dt = dt

    coords = deduped_coords
    activity_blocks = deduped_activity_blocks
    log_func(f"📍 After deduplication: {len(coords)} location points")

    combined = sorted(zip(coords, activity_blocks), key=lambda x: x[0][0])
    coords, activity_blocks = zip(*combined) if combined else ([], [])
    log_func("🔦 Reverse geocoding locations...")

    city_time = defaultdict(list)  # Store time intervals per place
    total_distance = 0.0
    rate_limit_hit = False
    last_point = None
    last_date = None
    last_dt = None

    for i, (dt, lat, lon) in enumerate(coords):
        if cancel_check():
            log_func("❌ Canceled during geocoding.")
            return None
        if i % 10 == 0 and cancel_check():
            log_func("❌ Canceled during geocoding.")
            return None

        key = (round(lat, 5), round(lon, 5))
        loc = reverse_geocode(lat, lon, geoapify_key, google_key, delay, log_func)

        if not loc:
            continue

        date = dt.date()
        city = loc.get("city", "Unknown")
        state = loc.get("state", "")
        country = loc.get("country", "")

        if group_by == "by_city":
            place = city or "Unknown"
        else:
            if country == "United States":
                place = state or "Unknown US State"
            else:
                place = country or "Unknown"

        # Track time interval
        if last_dt:
            time_diff = (dt - last_dt).total_seconds() / (24 * 3600)  # Fractional days
            city_time[place].append(time_diff)
        last_dt = dt

        if last_point:
            dist = haversine_distance(*last_point, lat, lon)
            if dist > 0.1:
                total_distance += dist
        last_point = (lat, lon)
        last_date = date

    # Aggregate time intervals into fractional days
    aggregated_city_time = {place: sum(intervals) for place, intervals in city_time.items()}

    log_func(f"🗺️ Total distance: {total_distance:.2f} mi")

    try:
        export_monthly_csv(aggregated_city_time, output_dir, group_by, log_func)
    except Exception as e:
        log_func(f"❌ Error exporting monthly CSV: {e}")

    try:
        mode_counts = generate_city_jump_csv(coords, output_dir, group_by, log_func, activities=activity_blocks, cancel_check=cancel_check, onwater_key=onwater_key, delay=delay, geoapify_key=geoapify_key, google_key=google_key)
    except Exception as e:
        log_func(f"❌ Error generating city jump CSV: {e}")
        mode_counts = None

    if rate_limit_hit:
        log_func("⚠️ Warning: OnWater API errors occurred. Some modes may be inaccurate. Check API key or use a paid tier.")
    if os.path.exists(cache_file):
        log_func(f"⚠️ Note: Cached results in {cache_file} may affect water detection. Consider resetting if modes are incorrect.")
    return mode_counts

def generate_city_jump_csv(coords, output_dir, group_by, log_func, activities=None, cancel_check=None, onwater_key="", delay=0.5, geoapify_key="", google_key=""):
    log_func(f"🧪 generate_city_jump_csv received {len(coords)} entries")

    jump_file = os.path.join(output_dir, "city_jumps_with_mode.csv")

    if group_by != "by_city":
        log_func("📂 Skipping city jump output (not in by_city mode)")
        return None

    mode_map = {
        "in train": "Train",
        "in passenger vehicle": "Car",
        "walking": "Walking",
        "in ferry": "Ferry",
        "slow_mobility": "Walking",
        "fast_mobility": "Car",
        "medium_mobility": "Car",
        "flying": "Flight",
        "sailing": "Ferry",
        "skiing": "Unknown",
        "unknown": "Unknown",
        "in subway": "Train",
        "in tram": "Train",
        "stationary": "Walking"
    }

    coastal_countries = ["Croatia", "Montenegro"]
    rate_limit_hit = False
    cache_hits = 0
    cache_misses = 0
    unique_jumps = set()

    rows = []
    prev_city = None
    prev_dt = None
    prev_coords = None

    for i, (dt, lat, lon) in enumerate(coords):
        if cancel_check and cancel_check():
            log_func("❌ Canceled during city jump generation.")
            return None
        loc = reverse_geocode(lat, lon, geoapify_key, google_key, 0, log_func=None)
        city = loc.get("city", "Unknown")
        state = loc.get("state", "")
        country = loc.get("country", "")
        place_name = loc.get("place", "")
        if country != "United States" and country:
            place = f"{city}, {country}" if city != "Unknown" else country
        else:
            place = f"{city}, {state}, USA"

        if prev_city and place != prev_city:
            jump_key = f"{prev_city}:{place}"
            if jump_key in unique_jumps:
                continue
            unique_jumps.add(jump_key)

            distance = haversine_distance(*prev_coords, lat, lon)
            duration_hrs = (dt - prev_dt).total_seconds() / 3600 if prev_dt else 0
            speed_mph = distance / duration_hrs if duration_hrs > 0 else 0
            raw_mode = "Unknown"
            if activities and i < len(activities):
                act = activities[i]
                if isinstance(act, dict):
                    if "topCandidate" in act:
                        raw_mode = act.get("topCandidate", {}).get("type", "Unknown").lower()
                    elif "mode" in act:
                        raw_mode = act.get("mode", "Unknown").lower()
            mode = mode_map.get(raw_mode, "Unknown")
            log_func(f"Raw mode for jump {i} ({prev_city} to {place}): {raw_mode}, mapped to {mode}")

            # Use Google's mode if reliable
            if mode in ["Flight", "Train", "Ferry", "Walking"]:
                log_func(f"Using Google mode: {mode}")
            else:
                # Fallback for in passenger vehicle and unknown
                prev_country = prev_city.split(", ")[-1] if ", " in prev_city else prev_city
                is_international = country != prev_country and country != "Unknown" and prev_country != "Unknown"
                if (raw_mode in ["in passenger vehicle", "unknown"]) and is_international and (distance > 20 or duration_hrs < 1.5):
                    mode = "Flight"
                    log_func(f"Overriding mode to {mode} due to international jump")
                elif raw_mode in ["unknown", "walking"] and country in coastal_countries and distance < 2:
                    mode = "Boat"
                    log_func(f"Overriding mode to {mode} due to coastal country short jump")
                elif 0.5 < distance < 100:
                    jump_cache_key = f"jump:{round(prev_coords[0], 5)},{round(prev_coords[1], 5)}:{round(lat, 5)},{round(lon, 5)}"
                    jump_cache_key_fallback = f"jump:{round(prev_coords[0], 4)},{round(prev_coords[1], 4)}:{round(lat, 4)},{round(lon, 4)}"
                    if jump_cache_key in geo_cache:
                        is_water = geo_cache[jump_cache_key]
                        log_func(f"🌊 Jump cache HIT for {prev_city} to {place}: {'Water' if is_water else 'Land'}")
                        cache_hits += 1
                    elif jump_cache_key_fallback in geo_cache:
                        is_water = geo_cache[jump_cache_key_fallback]
                        log_func(f"🌊 Jump cache HIT (fallback): {prev_city} to {place}: {'Water' if is_water else 'Land'}")
                        cache_hits += 1
                    else:
                        points = [
                            (prev_coords[0], prev_coords[1]),
                            ((prev_coords[0] + lat) / 2, (prev_coords[1] + lon) / 2),
                            (lat, lon)
                        ] if distance < 10 else [(prev_coords[0], prev_coords[1]), (lat, lon)]
                        water_checks = [is_over_water(p[0], p[1], onwater_key, delay, log_func, geoapify_key, google_key) for p in points]
                        water_count = sum(1 for w in water_checks if w is True)
                        is_water = water_count > 0 or "waters" in place_name.lower() or "sea" in place_name.lower()
                        if any(w is None for w in water_checks):
                            rate_limit_hit = True
                        log_func(f"Water checks for {prev_city} to {place} (dist={distance:.2f} mi, time={duration_hrs:.2f} hrs, speed={speed_mph:.2f} mph, points={points}, place={place_name}): {water_checks}")
                        if is_water or (country in coastal_countries and distance < 2):
                            mode = "Ferry" if distance > 2 else "Boat"
                            log_func(f"Overriding mode to {mode} due to water detection")
                        elif country in coastal_countries and distance > 2 and "inland" not in place_name.lower():
                            mode = "Ferry"
                            log_func(f"Overriding mode to {mode} due to coastal country context")
                        elif distance > 2:
                            mode = "Car"
                            log_func(f"Overriding mode to {mode} due to distance")
                        else:
                            mode = "Walking"
                        geo_cache[jump_cache_key] = is_water
                        save_geo_cache()
                        cache_misses += 1

            # Time-based validation
            if mode in ["Ferry", "Car", "Train"] and duration_hrs < 0.5 and distance > 10:
                mode = "Flight"
                log_func(f"Overriding mode to {mode} due to short duration ({duration_hrs:.2f} hrs) for distance {distance:.2f} mi")

            # Restrict Walking
            if mode == "Walking" and (distance > 2 or duration_hrs > 0.5):
                mode = "Car"
                log_func(f"Overriding mode to {mode} due to excessive distance ({distance:.2f} mi) or duration ({duration_hrs:.2f} hrs)")

            rows.append([
                prev_dt.strftime("%Y-%m-%d %H:%M"),
                prev_city,
                place,
                mode,
                round(distance, 2)
            ])
            log_func(f"Jump {i}: {prev_city} to {place}, mode={mode}, distance={distance:.2f} mi, duration={duration_hrs:.2f} hrs, speed={speed_mph:.2f} mph")

        prev_city = place
        prev_coords = (lat, lon)
        prev_dt = dt

    if rows:
        modes = [row[3] for row in rows]
        mode_counts = {mode: modes.count(mode) for mode in sorted(set(modes))}
        log_func("📊 Mode distribution:")
        for mode, count in mode_counts.items():
            log_func(f"  {mode}: {count} jumps")
        log_func(f"📊 Cache stats: {cache_hits} hits, {cache_misses} misses, {len(unique_jumps)} unique jumps")
    else:
        mode_counts = {}

    log_func(f"💾 Writing: {jump_file}")
    try:
        with open(jump_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "From", "To", "Mode", "Distance (mi)"])
            writer.writerows(rows)
    except Exception as e:
        log_func(f"❌ Error writing {jump_file}: {e}")

    if rate_limit_hit:
        log_func("⚠️ Warning: OnWater API errors occurred. Some modes may be inaccurate. Check API key or use a paid tier.")
    if os.path.exists(cache_file):
        log_func(f"⚠️ Note: Cached results in {cache_file} may affect water detection. Consider resetting if modes are incorrect.")
    return mode_counts