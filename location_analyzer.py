# location_analyzer.py - Modern Async Location Analysis Engine (Fixed Version)
"""
High-performance location analyzer using async/await for efficient API calls.
Processes Google Location History data and generates travel analysis reports.
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from datetime import datetime, date
import json
import pandas as pd
from collections import defaultdict
import math
import os

@dataclass(frozen=True)
class LocationPoint:
    """Represents a single location point with timestamp and coordinates"""
    timestamp: datetime
    latitude: float
    longitude: float

@dataclass(frozen=True)
class GeocodeResult:
    """Geocoding result with city, state, country information"""
    city: Optional[str]
    state: Optional[str]
    country: Optional[str] 
    place_name: str = ""
    is_water: bool = False

@dataclass(frozen=True)
class LocationJump:
    """Represents a significant movement between locations"""
    from_location: str
    to_location: str
    distance_miles: float
    duration_hours: float
    timestamp: datetime

@dataclass
class AnalysisConfig:
    """Configuration for the location analyzer"""
    geoapify_key: str
    google_key: str = ""
    api_delay: float = 0.1
    min_distance_filter: float = 0.5  # miles
    min_time_filter: float = 0.5  # hours
    max_concurrent_requests: int = 20
    cache_precision: int = 5

class LocationAnalyzer:
    """
    Modern async location analyzer for Google Location History data.
    """
    
    def __init__(self, config: AnalysisConfig):
        self.config = config
        self.geocode_cache: Dict[str, GeocodeResult] = {}
        self.log_file = None  # Don't create log file by default
        self.load_cache()
    
    def _log(self, message: str):
        """Log with reduced console output"""
        if not any(skip in message for skip in [
            '🚀', '📍', '📊', '📈', '✅', '🔍', '🌍', '💾', '🗺️', '🏃'
        ]):
            print(message)
        
        if self.log_file:
            self.log_file.write(message + "\n")
            self.log_file.flush()
    
    def __del__(self):
        """Close log file when analyzer is destroyed"""
        if hasattr(self, 'log_file') and self.log_file:
            self.log_file.close()
    
    def _ensure_date_object(self, date_input):
        """Ensure input is a date object, convert if needed"""
        if isinstance(date_input, date):
            return date_input
        elif isinstance(date_input, str):
            try:
                return datetime.strptime(date_input, '%Y-%m-%d').date()
            except ValueError:
                return date.today()
        else:
            return date.today()
    
    def load_cache(self):
        """Load existing geocoding cache from file"""
        try:
            cache_file = "config/geo_cache.json"
            # Fallback to old location for backward compatibility
            if not os.path.exists(cache_file):
                cache_file = "geo_cache.json"
                
            with open(cache_file, "r") as f:
                cache_data = json.load(f)
                for key, data in cache_data.items():
                    if not key.startswith("water:") and not key.startswith("jump:"):
                        if isinstance(data, dict):
                            self.geocode_cache[key] = GeocodeResult(
                                city=data.get('city'),
                                state=data.get('state'),
                                country=data.get('country'),
                                place_name=data.get('place_name', data.get('place', '')),
                                is_water=data.get('is_water', False)
                            )
        except FileNotFoundError:
            pass
        except Exception as e:
            self._log(f"Cache load error: {e}")
    
    def save_cache(self):
        """Save geocoding cache to file"""
        cache_data = {}
        for key, result in self.geocode_cache.items():
            cache_data[key] = {
                'city': result.city,
                'state': result.state, 
                'country': result.country,
                'place_name': result.place_name,
                'is_water': result.is_water
            }
        
        # Ensure config directory exists
        os.makedirs('config', exist_ok=True)
        
        with open("config/geo_cache.json", "w") as f:
            json.dump(cache_data, f, indent=2)
    
    def parse_location_data(self, file_path: str, start_date, end_date) -> List[LocationPoint]:
        """Parse Google location history JSON file into LocationPoint objects"""
        
        # Ensure dates are date objects
        start_date = self._ensure_date_object(start_date)
        end_date = self._ensure_date_object(end_date)
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        timeline_objects = data.get("timelineObjects", data) if isinstance(data, dict) else data
        points = []
        
        self._log(f"Parsing {len(timeline_objects)} timeline objects...")
        
        for i, obj in enumerate(timeline_objects):
            # Reduce progress output frequency
            if i % 1000 == 0:
                self._log(f"Progress: {i}/{len(timeline_objects)}")
            
            # Parse activity objects
            if "activity" in obj:
                activity = obj["activity"]
                start_str = obj.get("startTime")
                end_str = obj.get("endTime")
                
                # Parse start coordinate
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
                
                # Parse end coordinate
                if end_str and activity.get("end", "").startswith("geo:"):
                    try:
                        latlon = activity["end"].replace("geo:", "").split(",")
                        if len(latlon) == 2:
                            lat = float(latlon[0])
                            lon = float(latlon[1])
                            dt = pd.to_datetime(end_str, utc=True)
                            if start_date <= dt.date() <= end_date:
                                points.append(LocationPoint(dt, lat, lon))
                    except Exception:
                        continue
            
            # Parse placeVisit objects
            elif "placeVisit" in obj:
                location = obj["placeVisit"].get("location", {})
                if "latitudeE7" in location and "longitudeE7" in location:
                    lat = location["latitudeE7"] / 1e7
                    lon = location["longitudeE7"] / 1e7
                    start_time = obj["placeVisit"].get("duration", {}).get("startTimestamp")
                    if start_time:
                        dt = pd.to_datetime(start_time, utc=True)
                        if start_date <= dt.date() <= end_date:
                            points.append(LocationPoint(dt, lat, lon))
            
            # Parse activitySegment paths
            elif "activitySegment" in obj:
                start_time = obj["activitySegment"].get("duration", {}).get("startTimestamp")
                if start_time:
                    dt = pd.to_datetime(start_time, utc=True)
                    if not (start_date <= dt.date() <= end_date):
                        continue
                    
                    waypoints = obj["activitySegment"].get("waypointPath", {}).get("waypoints", [])
                    for waypoint in waypoints[::10]:  # Sample every 10th point
                        if "latE7" in waypoint and "lngE7" in waypoint:
                            lat = waypoint["latE7"] / 1e7  
                            lon = waypoint["lngE7"] / 1e7
                            points.append(LocationPoint(dt, lat, lon))
            
            # Parse timelinePath objects
            elif "timelinePath" in obj:
                start_time = obj.get("startTime")
                if start_time:
                    start_dt = pd.to_datetime(start_time, utc=True)
                    if not (start_date <= start_dt.date() <= end_date):
                        continue
                    
                    for point in obj["timelinePath"]:
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
        
        self._log(f"Found {len(points)} location points")
        return sorted(points, key=lambda p: p.timestamp)
    
    def filter_significant_points(self, points: List[LocationPoint]) -> List[LocationPoint]:
        """Filter points to only keep significant location changes"""
        if not points:
            return []
            
        filtered = [points[0]]
        last_point = points[0]
        
        for point in points[1:]:
            distance = self.haversine_distance(
                last_point.latitude, last_point.longitude,
                point.latitude, point.longitude
            )
            time_diff = (point.timestamp - last_point.timestamp).total_seconds() / 3600
            
            if distance > self.config.min_distance_filter or time_diff > self.config.min_time_filter:
                filtered.append(point)
                last_point = point
        
        return filtered
    
    async def geocode_points(self, points: List[LocationPoint]) -> Dict[LocationPoint, GeocodeResult]:
        """Geocode location points using Geoapify API with async processing"""
        results = {}
        
        # Group points by rounded coordinates for efficient caching
        coord_groups = defaultdict(list)
        for point in points:
            coord_key = f"{point.latitude:.{self.config.cache_precision}f},{point.longitude:.{self.config.cache_precision}f}"
            coord_groups[coord_key].append(point)
        
        semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        geocoded_count = 0
        
        async def geocode_coordinate(coord_key: str, group_points: List[LocationPoint]):
            nonlocal geocoded_count
            async with semaphore:
                # Check cache first
                if coord_key in self.geocode_cache:
                    cached_result = self.geocode_cache[coord_key]
                    for point in group_points:
                        results[point] = cached_result
                    return
                
                try:
                    lat, lon = coord_key.split(',')
                    async with aiohttp.ClientSession() as session:
                        url = f"https://api.geoapify.com/v1/geocode/reverse"
                        params = {
                            'lat': lat,
                            'lon': lon,
                            'apiKey': self.config.geoapify_key,
                            'format': 'json'
                        }
                        
                        await asyncio.sleep(self.config.api_delay)
                        
                        async with session.get(url, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                if data.get('results'):
                                    result_data = data['results'][0]
                                    
                                    result = GeocodeResult(
                                        city=result_data.get('city'),
                                        state=result_data.get('state'),
                                        country=result_data.get('country'),
                                        place_name=result_data.get('formatted', ''),
                                        is_water=False
                                    )
                                    
                                    self.geocode_cache[coord_key] = result
                                    for point in group_points:
                                        results[point] = result
                                    
                                    geocoded_count += 1
                                    if geocoded_count % 10 == 0:
                                        self._log(f"Geocoded {geocoded_count} locations")
                                    
                except Exception as e:
                    # Use fallback result for failed geocoding
                    fallback = GeocodeResult(
                        city="Unknown",
                        state="Unknown", 
                        country="Unknown",
                        place_name=f"Lat: {lat}, Lon: {lon}"
                    )
                    for point in group_points:
                        results[point] = fallback
        
        # Execute all geocoding requests concurrently
        tasks = [geocode_coordinate(coord_key, group_points) 
                for coord_key, group_points in coord_groups.items()]
        
        await asyncio.gather(*tasks)
        self.save_cache()
        
        return results
    
    def calculate_jumps(self, points: List[LocationPoint], geocode_results: Dict[LocationPoint, GeocodeResult]) -> List[LocationJump]:
        """Calculate significant location jumps between cities"""
        jumps = []
        last_location = None
        last_point = None
        
        for point in points:
            if point not in geocode_results:
                continue
                
            result = geocode_results[point]
            location = f"{result.city}, {result.country}"
            
            if last_location and location != last_location and last_point:
                distance = self.haversine_distance(
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
        
        return jumps
    
    def generate_time_reports(self, points: List[LocationPoint], geocode_results: Dict[LocationPoint, GeocodeResult]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Generate time spent reports by city and state/country"""
        city_time = defaultdict(float)
        state_time = defaultdict(float)
        
        last_point = None
        last_result = None
        
        for point in points:
            if point not in geocode_results:
                continue
                
            result = geocode_results[point]
            
            if last_point and last_result:
                time_diff = (point.timestamp - last_point.timestamp).total_seconds() / (24 * 3600)  # days
                
                city_key = f"{last_result.city}, {last_result.country}"
                city_time[city_key] += time_diff
                
                if last_result.country == "United States":
                    state_key = last_result.state or "Unknown US State"
                else:
                    state_key = last_result.country or "Unknown"
                state_time[state_key] += time_diff
            
            last_point = point
            last_result = result
        
        return dict(city_time), dict(state_time)
    
    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in miles using Haversine formula"""
        R = 3958.8  # Earth radius in miles
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_phi / 2)**2 + 
             math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    async def analyze_location_history(self, file_path: str, start_date, end_date, output_dir: str):
        """Main analysis function - processes location history and generates reports"""
        self._log(f"Starting analysis from {start_date} to {end_date}")
        
        # 1. Parse location data
        points = self.parse_location_data(file_path, start_date, end_date)
        self._log(f"Found {len(points)} location points")
        
        # 2. Filter significant points
        filtered_points = self.filter_significant_points(points)
        self._log(f"Filtered to {len(filtered_points)} significant points")
        
        # 3. Geocode points
        geocode_results = await self.geocode_points(filtered_points)
        self._log(f"Geocoded {len(geocode_results)} locations")
        
        # 4. Calculate jumps and time reports
        jumps = self.calculate_jumps(filtered_points, geocode_results)
        city_time, state_time = self.generate_time_reports(filtered_points, geocode_results)
        
        total_distance = sum(jump.distance_miles for jump in jumps)
        self._log(f"Total distance: {total_distance:.2f} miles")
        self._log(f"Total jumps: {len(jumps)}")
        
        # 5. Export results
        self._export_results(jumps, city_time, state_time, output_dir)
        
        return {
            'total_distance': total_distance,
            'total_jumps': len(jumps),
            'cities_visited': len(city_time),
            'jumps': jumps
        }
    
    def _export_results(self, jumps: List[LocationJump], city_time: Dict[str, float], state_time: Dict[str, float], output_dir: str):
        """Export analysis results to CSV files and summary report"""
        import csv
        
        os.makedirs(output_dir, exist_ok=True)
        
        # City jumps CSV
        jumps_file = os.path.join(output_dir, "city_jumps.csv")
        with open(jumps_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "From", "To", "Distance (mi)", "Duration (hrs)"])
            for jump in jumps:
                writer.writerow([
                    jump.timestamp.strftime("%Y-%m-%d %H:%M"),
                    jump.from_location,
                    jump.to_location,
                    round(jump.distance_miles, 2),
                    round(jump.duration_hours, 2)
                ])
        
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
        
        # Summary report
        summary_file = os.path.join(output_dir, "analysis_summary.txt")
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write("LOCATION ANALYSIS SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total Distance Traveled: {sum(jump.distance_miles for jump in jumps):.2f} miles\n")
            f.write(f"Total Location Jumps: {len(jumps)}\n")
            f.write(f"Cities Visited: {len(city_time)}\n")
            f.write(f"States/Countries Visited: {len(state_time)}\n\n")
            
            f.write("TOP 10 CITIES BY TIME SPENT:\n")
            f.write("-" * 30 + "\n")
            for i, (location, days) in enumerate(sorted(city_time.items(), key=lambda x: -x[1])[:10], 1):
                f.write(f"{i:2d}. {location}: {days:.1f} days\n")
            
            f.write("\nTOP 10 STATES/COUNTRIES BY TIME SPENT:\n")
            f.write("-" * 40 + "\n")
            for i, (location, days) in enumerate(sorted(state_time.items(), key=lambda x: -x[1])[:10], 1):
                f.write(f"{i:2d}. {location}: {days:.1f} days\n")
        
        self._log(f"Results exported to {output_dir}")

# Example usage
async def main():
    """Example usage of the LocationAnalyzer"""
    config = AnalysisConfig(
        geoapify_key="your_geoapify_key_here",
        api_delay=0.1,
        min_distance_filter=0.5,
        max_concurrent_requests=20
    )
    
    analyzer = LocationAnalyzer(config)
    
    results = await analyzer.analyze_location_history(
        file_path="location-history.json",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        output_dir="./output"
    )
    
    print(f"Analysis complete: {results}")

if __name__ == "__main__":
    asyncio.run(main())