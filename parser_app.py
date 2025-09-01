from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
import json
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import threading
import uuid
from typing import Dict
import multiprocessing

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size

# Create directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

# Setup logging with cleaner format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('location_processor.log', mode='w'),  # Overwrite each run
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Filter out Flask HTTP request logs
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# Settings file
SETTINGS_FILE = 'settings.json'

def load_settings():
    """Load settings from settings.json file."""
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                return json.load(f)
        else:
            return {
                'distance_threshold': 200.0,
                'probability_threshold': 0.1,
                'duration_threshold': 600,
                'from_date': '',
                'to_date': '',
                'last_input_file': '',
                'last_output_file': ''
            }
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        return {}

def save_settings(settings, input_file='', output_file=''):
    """Save settings to settings.json file."""
    try:
        settings_to_save = settings.copy()
        settings_to_save.update({
            'last_input_file': input_file,
            'last_output_file': output_file
        })
        
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings_to_save, f, indent=2)
        logger.info(f"Settings saved to {SETTINGS_FILE}")
    except Exception as e:
        logger.error(f"Error saving settings: {e}")

# Progress tracking
progress_store = {}

def update_progress(task_id: str, message: str, percentage: float = None):
    """Store progress update for web interface."""
    if task_id not in progress_store:
        progress_store[task_id] = {}
    
    progress_store[task_id].update({
        'message': message,
        'percentage': percentage or 0,
        'timestamp': datetime.now().isoformat(),
        'diagnostics': progress_store[task_id].get('diagnostics', [])
    })

def add_diagnostic(task_id: str, message: str, level: str = "INFO"):
    """Add diagnostic message to progress store only."""
    if task_id in progress_store:
        diagnostics = progress_store[task_id].get('diagnostics', [])
        diagnostics.append({
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'message': message
        })
        # Keep only last 100 messages
        progress_store[task_id]['diagnostics'] = diagnostics[-100:]

class LocationProcessor:
    """Production-ready location processor with optimized timeline handling."""
    
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.stats = {
            'total_entries': 0,
            'date_filtered': 0,
            'activities': 0,
            'visits': 0,
            'timeline_paths': 0,
            'final_count': 0
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log message to file and add to diagnostics."""
        logger.info(message)
        add_diagnostic(self.task_id, message, level)
    
    def progress(self, message: str, percentage: float = None):
        """Update progress."""
        update_progress(self.task_id, message, percentage)
        self.log(f"PROGRESS: {message}")
    
    def parse_timestamp(self, timestamp_input) -> Optional[pd.Timestamp]:
        """Parse timestamp from any Google format."""
        if not timestamp_input:
            return None
        
        try:
            if isinstance(timestamp_input, str):
                return pd.to_datetime(timestamp_input, utc=True)
            elif isinstance(timestamp_input, (int, float)):
                timestamp_str = str(int(timestamp_input))
                if len(timestamp_str) == 13:  # milliseconds
                    return pd.to_datetime(timestamp_input, unit='ms', utc=True)
                elif len(timestamp_str) == 10:  # seconds
                    return pd.to_datetime(timestamp_input, unit='s', utc=True)
            return None
        except:
            return None
    
    def parse_coordinates(self, coord_input) -> Optional[Tuple[float, float]]:
        """Parse coordinates from any Google format."""
        if not coord_input:
            return None
        
        try:
            # geo: string format
            if isinstance(coord_input, str):
                if coord_input.startswith('geo:'):
                    coords = coord_input.replace('geo:', '').split(',')
                    if len(coords) == 2:
                        lat, lon = float(coords[0]), float(coords[1])
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            return lat, lon
            
            # Object formats
            elif isinstance(coord_input, dict):
                # E7 format
                if 'latitudeE7' in coord_input and 'longitudeE7' in coord_input:
                    lat = float(coord_input['latitudeE7']) / 10000000
                    lon = float(coord_input['longitudeE7']) / 10000000
                    return lat, lon
                
                # Decimal degrees
                elif 'latitude' in coord_input and 'longitude' in coord_input:
                    lat = float(coord_input['latitude'])
                    lon = float(coord_input['longitude'])
                    return lat, lon
            
            return None
        except:
            return None
    
    @staticmethod
    def calculate_distance(coords1: Tuple[float, float], coords2: Tuple[float, float]) -> float:
        """Calculate distance between two coordinate points in meters."""
        from math import radians, cos, sin, asin, sqrt
        
        lat1, lon1 = coords1
        lat2, lon2 = coords2
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371  # Earth radius in kilometers
        return c * r * 1000  # Convert to meters
    
    def sample_points(self, points: List[Dict], max_points: int) -> List[Dict]:
        """Sample points evenly, always preserving first and last points."""
        if len(points) <= max_points:
            return points
        
        if max_points < 2:
            return [points[0]]  # At least keep the first point
        
        sampled = [points[0]]  # Always keep first point
        
        if max_points > 2:
            # Calculate indices for middle points
            middle_count = max_points - 2
            if middle_count > 0:
                # Create evenly spaced indices between first and last
                step_size = (len(points) - 1) / (middle_count + 1)
                for i in range(1, middle_count + 1):
                    index = int(round(i * step_size))
                    if index < len(points) - 1:  # Don't duplicate the last point
                        sampled.append(points[index])
        
        # Always keep last point (unless it's the same as first)
        if len(points) > 1:
            sampled.append(points[-1])
        
        return sampled
    
    def extract_timestamp_fast(self, entry: Dict) -> Optional[pd.Timestamp]:
        """Fast timestamp extraction for date filtering."""
        try:
            # Direct timestamp
            if 'startTime' in entry:
                return pd.to_datetime(entry['startTime'], utc=True)
            
            # Activity/visit nested
            if 'activity' in entry and 'startTime' in entry['activity']:
                return pd.to_datetime(entry['activity']['startTime'], utc=True)
            if 'visit' in entry and 'startTime' in entry['visit']:
                return pd.to_datetime(entry['visit']['startTime'], utc=True)
            
            # Legacy format
            if 'timestampMs' in entry:
                return pd.to_datetime(int(entry['timestampMs']), unit='ms', utc=True)
            
            return None
        except:
            return None
    
# COMPLETE FIX - Replace these methods in LocationProcessor class
# This ensures: 1) Fast date filtering, 2) Threshold application, 3) Correct output

    def fast_date_filter(self, entries, from_dt, to_dt):
        """Filter entries by date range - fast version based on working test."""
        self.progress(f"Date filtering {len(entries):,} entries...", 20)
        
        relevant_entries = []
        
        # Year pre-filtering for speed
        start_year = from_dt.year
        end_year = to_dt.year
        
        entries_checked = 0
        
        for entry in entries:
            entries_checked += 1
            
            # Get timestamp string for year check
            timestamp_str = None
            
            if 'startTime' in entry:
                timestamp_str = entry['startTime']
            elif 'activity' in entry and isinstance(entry['activity'], dict) and 'startTime' in entry['activity']:
                timestamp_str = entry['activity']['startTime']
            elif 'visit' in entry and isinstance(entry['visit'], dict) and 'startTime' in entry['visit']:
                timestamp_str = entry['visit']['startTime']
            
            if timestamp_str:
                # Quick year check first
                if len(timestamp_str) >= 4:
                    try:
                        year = int(timestamp_str[:4])
                        if year < start_year or year > end_year:
                            continue  # Skip entries outside year range
                    except:
                        pass
                
                # Full timestamp check for entries in the right year
                timestamp = self.extract_timestamp_fast(entry)
                if timestamp and from_dt <= timestamp < to_dt:
                    relevant_entries.append(entry)
            
            # Progress updates
            if entries_checked % 10000 == 0:
                progress_pct = 20 + (entries_checked / len(entries)) * 30
                self.progress(f"Date filtering: {len(relevant_entries):,} found from {entries_checked:,} checked", progress_pct)
        
        self.stats['date_filtered'] = len(relevant_entries)
        self.log(f"Date filtering complete: {len(relevant_entries):,} entries in date range")
        
        return relevant_entries

    def process_file(self, input_file: str, settings: Dict) -> Dict:
        """Main processing function that applies BOTH date filtering AND thresholds."""
        try:
            # STEP 1: Load file
            file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
            self.progress(f"Loading {file_size_mb:.1f}MB file...", 5)
            
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # STEP 2: Extract entries
            self.progress("Extracting entries...", 10)
            if isinstance(data, dict):
                if 'timelineObjects' in data:
                    entries = data['timelineObjects']
                elif 'locations' in data:
                    entries = data['locations']
                else:
                    entries = [data]
            elif isinstance(data, list):
                entries = data
            else:
                return {'error': 'Unsupported file format'}
            
            self.stats['total_entries'] = len(entries)
            self.log(f"Loaded {len(entries):,} total entries")
            
            # Free original data
            del data
            
            # STEP 3: Parse date range (CRITICAL - add 1 day to end date for inclusive range)
            from_dt = pd.to_datetime(settings['from_date'], utc=True)
            to_dt = pd.to_datetime(settings['to_date'], utc=True) + pd.Timedelta(days=1)
            
            # Log what we're doing
            self.log("=" * 50)
            self.log(f"Processing parameters:")
            self.log(f"  Date range: {from_dt.date()} to {to_dt.date() - pd.Timedelta(days=1)}")
            self.log(f"  Distance threshold: {settings.get('distance_threshold', 200)}m")
            self.log(f"  Probability threshold: {settings.get('probability_threshold', 0.1)}")
            self.log(f"  Duration threshold: {settings.get('duration_threshold', 600)}s")
            self.log("=" * 50)
            
            # STEP 4: Filter by date range FIRST (fast)
            relevant_entries = self.fast_date_filter(entries, from_dt, to_dt)
            del entries  # Free memory
            
            if not relevant_entries:
                self.log(f"No entries found between {from_dt.date()} and {to_dt.date() - pd.Timedelta(days=1)}")
                return {'error': f'No entries found in date range {settings["from_date"]} to {settings["to_date"]}'}
            
            # STEP 5: Apply threshold filters to date-filtered entries
            self.progress("Applying distance/duration/probability filters...", 60)
            processed_entries = []
            
            batch_size = 5000
            for i in range(0, len(relevant_entries), batch_size):
                batch = relevant_entries[i:i + batch_size]
                
                for entry in batch:
                    # This applies the threshold filters!
                    processed = self.process_entry(entry, settings)
                    if processed:  # Only add if it passes ALL filters
                        processed_entries.append(processed)
                
                # Progress update
                if i % 10000 == 0:
                    progress_pct = 60 + (i / len(relevant_entries)) * 30
                    self.progress(f"Filtering: {len(processed_entries):,} kept from {i:,} examined", progress_pct)
            
            # STEP 6: Sort by time
            self.progress("Finalizing output...", 95)
            processed_entries.sort(key=lambda x: x['startTime'])
            
            self.stats['final_count'] = len(processed_entries)
            
            # STEP 7: Calculate and log results
            reduction_ratio = (1 - len(processed_entries) / len(relevant_entries)) * 100 if relevant_entries else 0
            
            self.log("=" * 50)
            self.log("PROCESSING COMPLETE:")
            self.log(f"  Total entries in file: {self.stats['total_entries']:,}")
            self.log(f"  Entries in date range: {self.stats['date_filtered']:,}")
            self.log(f"  After applying filters: {self.stats['final_count']:,}")
            self.log(f"  Reduction: {reduction_ratio:.1f}%")
            self.log(f"  Activities: {self.stats.get('activities', 0):,}")
            self.log(f"  Visits: {self.stats.get('visits', 0):,}")
            self.log(f"  Timeline paths: {self.stats.get('timeline_paths', 0):,}")
            self.log("=" * 50)
            
            # Verify output date range
            if processed_entries:
                first_time = self.parse_timestamp(processed_entries[0]['startTime'])
                last_time = self.parse_timestamp(processed_entries[-1]['startTime'])
                self.log(f"Output date range: {first_time.date()} to {last_time.date()}")
            
            return {
                'success': True,
                'data': processed_entries,
                'stats': self.stats,
                'reduction_percentage': round(reduction_ratio, 1)
            }
            
        except Exception as e:
            self.log(f"Processing failed: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return {'error': str(e)}

# VERIFICATION: Add this test method temporarily to verify it works
    def verify_date_filtering(self, output_file: str):
        """Verify that output file only contains requested date range."""
        with open(output_file, 'r') as f:
            data = json.load(f)
        
        if not data:
            print("No data in output file")
            return
        
        dates = []
        for entry in data:
            if 'startTime' in entry:
                dt = pd.to_datetime(entry['startTime'])
                dates.append(dt.date())
        
        if dates:
            print(f"Output file date range: {min(dates)} to {max(dates)}")
            print(f"Total entries: {len(data)}")
        else:
            print("No dates found in output")
    
    def process_entry(self, entry: Dict, settings: Dict) -> Optional[Dict]:
        """Process a single entry based on its type."""
        try:
            # Determine entry type and extract data
            if 'activity' in entry:
                return self.process_activity(entry, settings)
            elif 'visit' in entry:
                return self.process_visit(entry, settings)
            elif 'timelinePath' in entry:
                return self.process_timeline_path(entry, settings)
            elif 'timestampMs' in entry:
                return self.process_legacy_location(entry, settings)
            else:
                return None
        except Exception as e:
            return None
    
    def process_activity(self, entry: Dict, settings: Dict) -> Optional[Dict]:
        """Process activity entry."""
        try:
            activity = entry['activity']
            
            # Get coordinates
            start_coords = self.parse_coordinates(activity.get('start'))
            end_coords = self.parse_coordinates(activity.get('end'))
            if not start_coords or not end_coords:
                return None
            
            # Check distance threshold
            distance = float(activity.get('distanceMeters', 0))
            if distance < settings.get('distance_threshold', 200):
                return None
            
            self.stats['activities'] += 1
            return {
                'startTime': entry['startTime'],
                'endTime': entry['endTime'],
                'activity': {
                    'start': f"geo:{start_coords[0]:.6f},{start_coords[1]:.6f}",
                    'end': f"geo:{end_coords[0]:.6f},{end_coords[1]:.6f}",
                    'distanceMeters': str(int(distance)),
                    'topCandidate': activity.get('topCandidate', {}),
                    'probability': str(activity.get('probability', 0.0))
                }
            }
        except:
            return None
    
    def process_visit(self, entry: Dict, settings: Dict) -> Optional[Dict]:
        """Process visit entry."""
        try:
            visit = entry['visit']
            
            # Get coordinates
            coords = None
            if 'topCandidate' in visit and 'placeLocation' in visit['topCandidate']:
                coords = self.parse_coordinates(visit['topCandidate']['placeLocation'])
            if not coords:
                return None
            
            # Check duration threshold
            start_dt = self.parse_timestamp(entry['startTime'])
            end_dt = self.parse_timestamp(entry['endTime'])
            if start_dt and end_dt:
                duration = (end_dt - start_dt).total_seconds()
                if duration < settings.get('duration_threshold', 600):
                    return None
            
            # Check probability threshold
            probability = float(visit.get('probability', 0.0))
            if probability < settings.get('probability_threshold', 0.1):
                return None
            
            # Preserve all original visit fields
            top_candidate = visit.get('topCandidate', {})
            result = {
                'startTime': entry['startTime'],
                'endTime': entry['endTime'],
                'visit': {
                    'topCandidate': {
                        'placeLocation': f"geo:{coords[0]:.6f},{coords[1]:.6f}",
                        'probability': str(top_candidate.get('probability', probability))
                    },
                    'probability': str(probability)
                }
            }
            
            # Add optional fields if they exist
            if 'placeID' in top_candidate:
                result['visit']['topCandidate']['placeID'] = top_candidate['placeID']
            if 'semanticType' in top_candidate:
                result['visit']['topCandidate']['semanticType'] = top_candidate['semanticType']
            
            self.stats['visits'] += 1
            return result
        except:
            return None
    
    def process_timeline_path(self, entry: Dict, settings: Dict) -> Optional[Dict]:
        """Process timeline path entry with guaranteed first point and better local movement handling."""
        try:
            timeline_path = entry.get('timelinePath', [])
            if not timeline_path:
                return None
            
            distance_threshold = settings.get('distance_threshold', 200)
            
            # ALWAYS keep the first point - this solves the "missing start of day" issue
            filtered_points = []
            
            for i, point in enumerate(timeline_path):
                coords = self.parse_coordinates(point.get('point'))
                if not coords:
                    continue
                
                # Always add the first valid point
                if i == 0:
                    filtered_points.append({
                        'point': f"geo:{coords[0]:.6f},{coords[1]:.6f}",
                        'durationMinutesOffsetFromStartTime': point.get('durationMinutesOffsetFromStartTime', '0'),
                        'mode': point.get('mode', 'unknown')
                    })
                    continue
                
                # For subsequent points, apply distance filtering
                if filtered_points:  # We have at least one point
                    last_point_coords = None
                    last_point_str = filtered_points[-1]['point']
                    if last_point_str.startswith('geo:'):
                        coord_parts = last_point_str.replace('geo:', '').split(',')
                        if len(coord_parts) == 2:
                            last_point_coords = (float(coord_parts[0]), float(coord_parts[1]))
                    
                    if last_point_coords:
                        distance = self.calculate_distance(last_point_coords, coords)
                        if distance < distance_threshold:
                            continue  # Skip points too close together
                
                filtered_points.append({
                    'point': f"geo:{coords[0]:.6f},{coords[1]:.6f}",
                    'durationMinutesOffsetFromStartTime': point.get('durationMinutesOffsetFromStartTime', '0'),
                    'mode': point.get('mode', 'unknown')
                })
            
            # If we only have 1 point (local movement), that's fine - return it
            if len(filtered_points) == 1:
                self.stats['timeline_paths'] += 1
                return {
                    'startTime': entry['startTime'],
                    'endTime': entry['endTime'],
                    'timelinePath': filtered_points
                }
            
            # Apply intelligent sampling based on movement type and settings
            original_length = len(timeline_path)
            filtered_length = len(filtered_points)
            
            # Determine if this is local movement or travel
            if original_length <= 10 or filtered_length <= 5:
                # Local movement - keep more granular data
                max_points = min(8, filtered_length)
            else:
                # Travel movement - sample more aggressively based on threshold
                if distance_threshold >= 2000:
                    max_points = 5
                elif distance_threshold >= 1000:
                    max_points = 8
                elif distance_threshold >= 500:
                    max_points = 12
                elif distance_threshold >= 200:
                    max_points = 15
                else:
                    max_points = 20
            
            # Sample points if we have too many
            if len(filtered_points) > max_points:
                filtered_points = self.sample_points(filtered_points, max_points)
            
            self.stats['timeline_paths'] += 1
            return {
                'startTime': entry['startTime'],
                'endTime': entry['endTime'],
                'timelinePath': filtered_points
            }
            
        except Exception as e:
            return None
    
    def process_legacy_location(self, entry: Dict, settings: Dict) -> Optional[Dict]:
        """Process legacy location entry."""
        try:
            coords = self.parse_coordinates(entry)
            if not coords:
                return None
            
            timestamp = self.parse_timestamp(entry.get('timestampMs'))
            if not timestamp:
                return None
            
            self.stats['visits'] += 1
            return {
                'startTime': timestamp.isoformat(),
                'endTime': timestamp.isoformat(),
                'visit': {
                    'topCandidate': {
                        'placeLocation': f"geo:{coords[0]:.6f},{coords[1]:.6f}",
                        'probability': str(entry.get('accuracy', 50) / 100.0)
                    },
                    'probability': str(entry.get('accuracy', 50) / 100.0)
                }
            }
        except:
            return None
    
    def ensure_daily_continuity(self, processed_entries: List[Dict]) -> List[Dict]:
        """Check for missing days and log warnings."""
        if not processed_entries:
            return processed_entries
        
        # Sort by start time first
        processed_entries.sort(key=lambda x: x['startTime'])
        
        # Group by date to check continuity
        daily_groups = {}
        for entry in processed_entries:
            start_time = pd.to_datetime(entry['startTime'])
            date_key = start_time.date()
            
            if date_key not in daily_groups:
                daily_groups[date_key] = []
            daily_groups[date_key].append(entry)
        
        # Check for missing days and log warnings
        all_dates = sorted(daily_groups.keys())
        missing_days = 0
        if len(all_dates) > 1:
            for i in range(len(all_dates) - 1):
                current_date = all_dates[i]
                next_date = all_dates[i + 1]
                
                date_diff = (next_date - current_date).days
                if date_diff > 1:
                    missing_days += date_diff - 1
        
        if missing_days > 0:
            self.log(f"WARNING: {missing_days} days with no location data found in date range")
        
        return processed_entries
    
    def process_file(self, input_file: str, settings: Dict) -> Dict:
        """Main processing function with improved timeline handling."""
        try:
            # Load file
            file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
            self.progress(f"Loading {file_size_mb:.1f}MB file...", 5)
            
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract entries
            self.progress("Extracting entries...", 10)
            if isinstance(data, dict):
                if 'timelineObjects' in data:
                    entries = data['timelineObjects']
                elif 'locations' in data:
                    entries = data['locations']
                else:
                    entries = [data]
            elif isinstance(data, list):
                entries = data
            else:
                return {'error': 'Unsupported file format'}
            
            self.stats['total_entries'] = len(entries)
            self.log(f"Loaded {len(entries):,} total entries")
            
            # Free original data early
            del data
            
            # Date filtering (key optimization)
            from_dt = pd.to_datetime(settings['from_date'], utc=True)
            to_dt = pd.to_datetime(settings['to_date'], utc=True) + pd.Timedelta(days=1)
            
            self.log(f"Date filtering from {from_dt.date()} to {to_dt.date()}")
            relevant_entries = self.fast_date_filter(entries, from_dt, to_dt)
            del entries  # Free memory
            
            if not relevant_entries:
                return {'error': 'No entries found in date range'}
            
            # Process entries
            self.progress("Processing entries...", 60)
            processed_entries = []
            
            batch_size = 5000
            for i in range(0, len(relevant_entries), batch_size):
                batch = relevant_entries[i:i + batch_size]
                
                for entry in batch:
                    processed = self.process_entry(entry, settings)
                    if processed:
                        processed_entries.append(processed)
                
                # Progress update
                progress_pct = 60 + (i / len(relevant_entries)) * 30  # 60% to 90%
                if i % 25000 == 0:
                    self.progress(f"Processing: {len(processed_entries):,} entries processed", progress_pct)
            
            # Ensure daily continuity before sorting
            processed_entries = self.ensure_daily_continuity(processed_entries)
            
            # Sort by time
            self.progress("Finalizing output...", 95)
            processed_entries.sort(key=lambda x: x['startTime'])
            
            self.stats['final_count'] = len(processed_entries)
            
            # Calculate results
            original_count = len(relevant_entries)
            final_count = len(processed_entries)
            reduction_ratio = (1 - final_count / original_count) * 100 if original_count > 0 else 0
            
            self.log("=== PROCESSING COMPLETE ===")
            self.log(f"Total entries loaded: {self.stats['total_entries']:,}")
            self.log(f"Date range matches: {self.stats['date_filtered']:,}")
            self.log(f"Activities processed: {self.stats['activities']:,}")
            self.log(f"Visits processed: {self.stats['visits']:,}")
            self.log(f"Timeline paths processed: {self.stats['timeline_paths']:,}")
            self.log(f"Final output: {final_count:,} entries ({reduction_ratio:.1f}% reduction)")
            
            return {
                'success': True,
                'data': processed_entries,
                'stats': self.stats,
                'reduction_percentage': round(reduction_ratio, 1)
            }
            
        except Exception as e:
            self.log(f"Processing failed: {str(e)}", "ERROR")
            return {'error': str(e)}

# Flask routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/settings')
def get_settings():
    return jsonify(load_settings())

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '' or not file.filename.lower().endswith('.json'):
        return jsonify({'error': 'Please upload a JSON file'}), 400
    
    # Save file
    filename = secure_filename(file.filename)
    task_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{task_id}_{filename}")
    file.save(upload_path)
    
    # Get settings
    existing_settings = load_settings()
    settings = {
        'from_date': request.form.get('from_date'),
        'to_date': request.form.get('to_date'),
        'distance_threshold': float(request.form.get('distance_threshold', existing_settings.get('distance_threshold', 200))),
        'probability_threshold': float(request.form.get('probability_threshold', existing_settings.get('probability_threshold', 0.1))),
        'duration_threshold': int(request.form.get('duration_threshold', existing_settings.get('duration_threshold', 600)))
    }
    
    # Initialize progress
    progress_store[task_id] = {
        'status': 'PENDING',
        'message': 'Starting optimized location processing...',
        'percentage': 0,
        'diagnostics': []
    }
    
    # Process in background
    def process_in_background():
        processor = LocationProcessor(task_id)
        
        try:
            result = processor.process_file(upload_path, settings)
            
            if result.get('success'):
                # Save output
                output_filename = f"{task_id}_output.json"
                output_file = os.path.join(app.config['PROCESSED_FOLDER'], output_filename)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result['data'], f, indent=1)
                
                # Save settings
                save_settings(settings, upload_path, output_file)
                
                progress_store[task_id]['result'] = result
                progress_store[task_id]['status'] = 'SUCCESS'
                update_progress(task_id, 'Processing complete!', 100)
            else:
                progress_store[task_id]['status'] = 'FAILURE'
                progress_store[task_id]['error'] = result.get('error', 'Unknown error')
                
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            progress_store[task_id]['status'] = 'FAILURE'
            progress_store[task_id]['error'] = str(e)
        
        # Cleanup
        try:
            os.remove(upload_path)
        except:
            pass
    
    thread = threading.Thread(target=process_in_background)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'task_id': task_id,
        'message': 'Location processing started',
        'status': 'PENDING'
    })

@app.route('/progress/<task_id>')
def get_progress(task_id):
    if task_id not in progress_store:
        return jsonify({'state': 'PENDING', 'message': 'Task not found'})
    
    progress = progress_store[task_id]
    return jsonify({
        'state': progress.get('status', 'PENDING'),
        'message': progress.get('message', ''),
        'percentage': progress.get('percentage', 0),
        'result': progress.get('result', {}),
        'diagnostics': progress.get('diagnostics', [])
    })

@app.route('/download/<task_id>')
def download_result(task_id):
    output_file = os.path.join(app.config['PROCESSED_FOLDER'], f"{task_id}_output.json")
    
    if not os.path.exists(output_file):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(
        output_file,
        as_attachment=True,
        download_name=f"location_data_{task_id[:8]}.json",
        mimetype='application/json'
    )

@app.route('/cleanup/<task_id>', methods=['POST'])
def cleanup_files(task_id):
    try:
        # Remove upload files
        upload_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(task_id)]
        for f in upload_files:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], f)
            if os.path.exists(file_path):
                os.remove(file_path)
        
        # Remove output file
        output_file = os.path.join(app.config['PROCESSED_FOLDER'], f"{task_id}_output.json")
        if os.path.exists(output_file):
            os.remove(output_file)
        
        # Remove from progress store
        if task_id in progress_store:
            del progress_store[task_id]
        
        return jsonify({'message': 'Files cleaned up'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'version': '3.1.0-production',
        'active_tasks': len(progress_store),
        'features': [
            'Optimized timeline processing with guaranteed first points',
            'Local movement detection and preservation', 
            'Intelligent sampling based on movement patterns',
            'Clean logging with overwrite mode',
            'Production-ready performance'
        ]
    })

if __name__ == '__main__':
    print("LOCATION PROCESSOR v3.1 - Production Ready")
    print("=" * 50)
    print("KEY FEATURES:")
    print("  • Guaranteed first point preservation")
    print("  • Local movement detection")  
    print("  • Intelligent timeline sampling")
    print("  • Clean logging (overwrites each run)")
    print("  • Removed debug clutter")
    print("=" * 50)
    print(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
    print(f"Output folder: {app.config['PROCESSED_FOLDER']}")
    print(f"Log file: location_processor.log")
    print(f"Settings file: {SETTINGS_FILE}")
    print("Web interface: http://localhost:5000")
    print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)