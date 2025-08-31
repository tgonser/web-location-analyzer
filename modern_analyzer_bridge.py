# modern_analyzer_bridge.py - Bridge between old and new analyzers
"""
Bridge to use the modern async location analyzer with the existing synchronous interface.
This maintains compatibility while using the improved caching and async processing.
"""

import asyncio
import os
import csv
import json
import pandas as pd
from datetime import datetime, date
from collections import defaultdict, namedtuple
from geo_utils import reverse_geocode, get_global_stats, reset_global_stats


def process_location_file(file_path, start_date, end_date, output_dir, group_by,
                         geoapify_key, google_key, onwater_key, delay, batch_size,
                         log_func, cancel_check, include_distance=True):
    """
    Bridge function that maintains the old interface but uses the modern analyzer.
    This function is synchronous and can be called from the existing unified_app.py
    """
    
    # Reset statistics for this run
    reset_global_stats()
    
    # Simple implementation without complex inheritance
    try:
        return asyncio.run(run_modern_analysis(
            file_path, start_date, end_date, output_dir, group_by,
            geoapify_key, google_key, delay, log_func, cancel_check
        ))
    except Exception as e:
        log_func(f"Analysis failed: {str(e)}")
        return None


async def run_modern_analysis(file_path, start_date, end_date, output_dir, group_by,
                             geoapify_key, google_key, delay, log_func, cancel_check):
    """Simplified modern analysis with fixed visit parsing"""
    
    def ensure_date_object(date_input):
        if isinstance(date_input, date):
            return date_input
        elif isinstance(date_input, str):
            try:
                return datetime.strptime(date_input, '%Y-%m-%d').date()
            except ValueError:
                return date.today()
        else:
            return date.today()
    
    def haversine_distance(lat1, lon1, lat2, lon2):
        import math
        R = 3958.8  # miles
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = (math.sin(delta_phi / 2)**2 + 
             math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    
    log_func(f"Starting modern analysis from {start_date} to {end_date}")
    
    # Parse the file
    start_date = ensure_date_object(start_date)
    end_date = ensure_date_object(end_date)
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    timeline_objects = data.get("timelineObjects", data) if isinstance(data, dict) else data
    
    # Track parsing statistics
    parse_stats = {
        'total_entries': len(timeline_objects),
        'activities': 0,
        'visits': 0,
        'timeline_paths': 0,
        'other_objects': 0,
        'final_count': 0
    }
    
    # Extract location points
    LocationPoint = namedtuple('LocationPoint', ['timestamp', 'latitude', 'longitude'])
    points = []
    
    log_func(f"Parsing {len(timeline_objects)} timeline objects...")
    
    for i, obj in enumerate(timeline_objects):
        if i % 1000 == 0:
            log_func(f"Progress: {i}/{len(timeline_objects)}")
            
        if cancel_check and cancel_check():
            log_func("Canceled during parsing.")
            return None
        
        # Debug: Show keys of first few objects to understand structure
        if i < 5:
            log_func(f"DEBUG: Object {i} keys: {list(obj.keys())}")
        
        # Parse activity objects
        if "activity" in obj:
            parse_stats['activities'] += 1
            activity = obj["activity"]
            start_str = obj.get("startTime")
            
            if start_str and activity.get("start", "").startswith("geo:"):
                try:
                    latlon = activity["start"].replace("geo:", "").split(",")
                    if len(latlon) == 2:
                        lat = float(latlon[0])
                        lon = float(latlon[1])
                        dt = pd.to_datetime(start_str, utc=True)
                        if start_date <= dt.date() <= end_date:
                            points.append(LocationPoint(dt, lat, lon))
                except Exception:
                    continue
        
        # Parse visit objects (FIXED: was looking for "placeVisit", should be "visit")
        elif "visit" in obj:
            parse_stats['visits'] += 1
            
            # Add debug logging to see visit structure
            if parse_stats['visits'] <= 3:  # Only log first 3 visits
                log_func(f"DEBUG: Visit object {parse_stats['visits']} keys: {list(obj.keys())}")
                log_func(f"DEBUG: Visit object {parse_stats['visits']} visit structure: {obj.get('visit', {})}")
            
            visit = obj["visit"]
            start_str = obj.get("startTime")
            
            # Check for topCandidate.placeLocation with geo: format
            top_candidate = visit.get("topCandidate", {})
            place_location = top_candidate.get("placeLocation", "")
            
            if start_str and place_location.startswith("geo:"):
                try:
                    latlon = place_location.replace("geo:", "").split(",")
                    if len(latlon) == 2:
                        lat = float(latlon[0])
                        lon = float(latlon[1])
                        dt = pd.to_datetime(start_str, utc=True)
                        if start_date <= dt.date() <= end_date:
                            points.append(LocationPoint(dt, lat, lon))
                            if parse_stats['visits'] <= 3:
                                log_func(f"DEBUG: Successfully parsed visit coordinates: {lat}, {lon}")
                except Exception as e:
                    if parse_stats['visits'] <= 3:
                        log_func(f"DEBUG: Failed to parse visit coordinates: {e}")
                    continue
            else:
                if parse_stats['visits'] <= 3:
                    log_func(f"DEBUG: Visit missing location data - startTime: {bool(start_str)}, placeLocation: {place_location}")
        
        # Parse timelinePath objects
        elif "timelinePath" in obj:
            parse_stats['timeline_paths'] += 1
            start_time = obj.get("startTime")
            if start_time:
                start_dt = pd.to_datetime(start_time, utc=True)
                if start_date <= start_dt.date() <= end_date:
                    timeline_path = obj.get("timelinePath", [])
                    for point in timeline_path:
                        if "point" in point and point["point"].startswith("geo:"):
                            try:
                                latlon = point["point"].replace("geo:", "").split(",")
                                if len(latlon) == 2:
                                    lat = float(latlon[0])
                                    lon = float(latlon[1])
                                    offset = float(point.get("durationMinutesOffsetFromStartTime", 0))
                                    point_dt = start_dt + pd.Timedelta(minutes=offset)
                                    if start_date <= point_dt.date() <= end_date:
                                        points.append(LocationPoint(point_dt, lat, lon))
                            except Exception:
                                continue
        
        # Count other object types for debugging
        else:
            parse_stats['other_objects'] += 1
            if parse_stats['other_objects'] <= 3:  # Show first few unknown objects
                log_func(f"DEBUG: Unknown object type {parse_stats['other_objects']} keys: {list(obj.keys())}")
    
    parse_stats['final_count'] = len(points)
    
    # Enhanced parse statistics logging
    log_func(f"Parse statistics: {parse_stats['activities']} activities, {parse_stats['visits']} visits, {parse_stats['timeline_paths']} timeline paths")
    log_func(f"Found {len(points)} location points")
    
    # Calculate totals for verification
    total_parsed = parse_stats['activities'] + parse_stats['visits'] + parse_stats['timeline_paths'] + parse_stats['other_objects']
    if total_parsed != parse_stats['total_entries']:
        log_func(f"DEBUG: Parsing mismatch - Total objects: {parse_stats['total_entries']}, Parsed: {total_parsed}, Missing: {parse_stats['total_entries'] - total_parsed}")
    
    if not points:
        log_func("No location data found.")
        return None
    
    # Sort points by time
    points = sorted(points, key=lambda p: p.timestamp)
    
    # Filter significant points
    filtered_points = [points[0]]
    last_point = points[0]
    
    for point in points[1:]:
        distance = haversine_distance(
            last_point.latitude, last_point.longitude,
            point.latitude, point.longitude
        )
        time_diff = (point.timestamp - last_point.timestamp).total_seconds() / 3600
        
        if distance > 0.5 or time_diff > 0.5:  # min thresholds
            filtered_points.append(point)
            last_point = point
    
    log_func(f"Filtered to {len(filtered_points)} significant points")
    
    # Geocode points using proper batch processing
    log_func("Reverse geocoding locations...")
    
    # Extract unique coordinates for batch processing
    unique_coords = []
    coord_to_points = {}
    
    for point in filtered_points:
        coord_tuple = (point.latitude, point.longitude)
        coord_key = f"{point.latitude:.5f},{point.longitude:.5f}"
        
        if coord_key not in coord_to_points:
            unique_coords.append(coord_tuple)
            coord_to_points[coord_key] = []
        coord_to_points[coord_key].append(point)
    
    log_func(f"Batch geocoding {len(unique_coords)} unique coordinates...")
    
    # Use the batch geocoding function
    try:
        from geo_utils import batch_reverse_geocode
        
        batch_results = await batch_reverse_geocode(
            coordinates=unique_coords,
            geoapify_key=geoapify_key,
            google_key=google_key,
            batch_size=25,  # Good balance of speed and API limits
            log_func=log_func,
            stats=get_global_stats()
        )
        
        # Map batch results back to individual points
        geocoded_points = {}
        for coord_key, point_list in coord_to_points.items():
            # Find the corresponding coordinate tuple
            sample_point = point_list[0]
            coord_tuple = (sample_point.latitude, sample_point.longitude)
            
            if coord_tuple in batch_results:
                result = batch_results[coord_tuple]
                # Apply the same result to all points with this coordinate
                for point in point_list:
                    geocoded_points[point] = result
            else:
                # Fallback for missing coordinates
                fallback_result = {"city": "Unknown", "country": "Unknown", "state": "", "place": "batch failed"}
                for point in point_list:
                    geocoded_points[point] = fallback_result
        
        log_func(f"Batch geocoding completed for {len(geocoded_points)} points")
        
    except Exception as e:
        log_func(f"Batch geocoding failed: {e}, falling back to individual calls")
        # Fallback to individual synchronous calls
        geocoded_points = {}
        for point in filtered_points:
            if cancel_check and cancel_check():
                log_func("Canceled during geocoding.")
                return None
                
            result = reverse_geocode(
                point.latitude, point.longitude,
                geoapify_key, google_key, 0,  # No delay for cached results
                log_func, get_global_stats()
            )
            geocoded_points[point] = result
    
    # Report geocoding statistics
    stats = get_global_stats()
    summary_messages = stats.summary()
    for msg in summary_messages:
        log_func(msg)
    
    # Calculate time spent by location
    city_time = defaultdict(float)
    state_time = defaultdict(float)
    last_point = None
    last_result = None
    
    for point in filtered_points:
        result = geocoded_points[point]
        
        if last_point and last_result:
            time_diff = (point.timestamp - last_point.timestamp).total_seconds() / (24 * 3600)  # days
            
            city = result.get("city", "Unknown")
            country = result.get("country", "Unknown")
            state = result.get("state", "")
            
            city_key = f"{city}, {country}"
            city_time[city_key] += time_diff
            
            if country == "United States":
                state_key = state or "Unknown US State"
            else:
                state_key = country or "Unknown"
            state_time[state_key] += time_diff
        
        last_point = point
        last_result = result
    
    # Calculate jumps
    LocationJump = namedtuple('LocationJump', ['from_location', 'to_location', 'distance_miles', 'duration_hours', 'timestamp'])
    jumps = []
    last_location = None
    last_point = None
    
    for point in filtered_points:
        result = geocoded_points[point]
        city = result.get("city", "Unknown")
        country = result.get("country", "Unknown")
        location = f"{city}, {country}"
        
        if last_location and location != last_location and last_point:
            distance = haversine_distance(
                last_point.latitude, last_point.longitude,
                point.latitude, point.longitude
            )
            
            duration_hours = (point.timestamp - last_point.timestamp).total_seconds() / 3600
            
            if distance > 10:  # Only record jumps > 10 miles
                jumps.append(LocationJump(
                    from_location=last_location,
                    to_location=location,
                    distance_miles=distance,
                    duration_hours=duration_hours,
                    timestamp=point.timestamp
                ))
        
        last_location = location
        last_point = point
    
    total_distance = sum(jump.distance_miles for jump in jumps)
    log_func(f"Total distance: {total_distance:.2f} miles")
    
    # Export CSV files
    os.makedirs(output_dir, exist_ok=True)
    
    # City time CSV
    city_file = os.path.join(output_dir, "by_city_location_days.csv")
    with open(city_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Location", "Fractional Days"])
        for location, days in sorted(city_time.items(), key=lambda x: -x[1]):
            writer.writerow([location, f"{days:.1f}"])
    
    # State time CSV 
    state_file = os.path.join(output_dir, "by_state_location_days.csv")
    with open(state_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Location", "Fractional Days"])
        for location, days in sorted(state_time.items(), key=lambda x: -x[1]):
            writer.writerow([location, f"{days:.1f}"])
    
    # Jumps CSV
    jump_file = os.path.join(output_dir, "city_jumps_with_mode.csv")
    with open(jump_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "From", "To", "Mode", "Distance (mi)"])
        for jump in jumps:
            # Simple mode inference
            if jump.distance_miles < 1:
                mode = "Walking"
            elif jump.distance_miles > 100:
                mode = "Flight"
            else:
                mode = "Car"
            
            writer.writerow([
                jump.timestamp.strftime("%Y-%m-%d %H:%M"),
                jump.from_location,
                jump.to_location,
                mode,
                round(jump.distance_miles, 2)
            ])
    
    # Summary file
    summary_file = os.path.join(output_dir, "analysis_summary.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("LOCATION ANALYSIS SUMMARY\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Analysis Period: {start_date} to {end_date}\n")
        f.write(f"Total Objects Processed: {parse_stats['total_entries']}\n")
        f.write(f"Activities: {parse_stats['activities']}\n")
        f.write(f"Visits: {parse_stats['visits']}\n")
        f.write(f"Timeline Paths: {parse_stats['timeline_paths']}\n")
        f.write(f"Other Objects: {parse_stats['other_objects']}\n")
        f.write(f"Location Points Extracted: {parse_stats['final_count']}\n")
        f.write(f"Significant Points: {len(filtered_points)}\n")
        f.write(f"Total Distance Traveled: {total_distance:.2f} miles\n")
        f.write(f"Total Location Jumps: {len(jumps)}\n") 
        f.write(f"Cities Visited: {len(city_time)}\n")
        f.write(f"States/Countries Visited: {len(state_time)}\n")
    
    log_func(f"Results exported to {output_dir}")
    
    return {
        'total_distance': total_distance,
        'total_jumps': len(jumps),
        'cities_visited': len(city_time),
        'jumps': jumps,
        'parse_stats': parse_stats
    }