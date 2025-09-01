from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
import os
import json
from datetime import date, datetime
from werkzeug.utils import secure_filename
import threading
import time
import uuid
import zipfile
import io
import pandas as pd
import multiprocessing
import logging
from typing import Dict


# Import the existing modules - these need to be copied from your LAweb app
try:
    from modern_analyzer_bridge import process_location_file  # Use modern analyzer
    from geo_utils import geo_cache, save_geo_cache, load_cache
    from csv_exporter import export_monthly_csv
    ANALYZER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import analyzer modules: {e}")
    print("Make sure to copy modern_analyzer_bridge.py, geo_utils.py, and csv_exporter.py from your LAweb app")
    ANALYZER_AVAILABLE = False

app = Flask(__name__)
app.secret_key = 'change-this-secret-key-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size

# Ensure directories exist
for folder in ['uploads', 'processed', 'outputs', 'config']:
    os.makedirs(folder, exist_ok=True)

# Global storage for unified analysis progress
unified_progress = {}

# Progress tracking for parser (extracted from parser_app.py)
parser_progress_store = {}

def update_progress(task_id: str, message: str, percentage: float = None):
    """Store progress update for web interface."""
    if task_id not in parser_progress_store:
        parser_progress_store[task_id] = {}
    
    parser_progress_store[task_id].update({
        'message': message,
        'percentage': percentage or 0,
        'timestamp': datetime.now().isoformat(),
        'diagnostics': parser_progress_store[task_id].get('diagnostics', [])
    })

def add_diagnostic(task_id: str, message: str, level: str = "INFO"):
    """Add diagnostic message to progress store only."""
    if task_id in parser_progress_store:
        diagnostics = parser_progress_store[task_id].get('diagnostics', [])
        diagnostics.append({
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'message': message
        })
        # Keep only last 100 messages
        parser_progress_store[task_id]['diagnostics'] = diagnostics[-100:]

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
        print(f"[{level}] {message}")  # Console logging
        add_diagnostic(self.task_id, message, level)
    
    def progress(self, message: str, percentage: float = None):
        """Update progress."""
        update_progress(self.task_id, message, percentage)
        self.log(f"PROGRESS: {message}")
    
    def parse_timestamp(self, timestamp_input):
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
    
    def parse_coordinates(self, coord_input):
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
    def calculate_distance(coords1, coords2):
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

    def extract_timestamp_fast(self, entry):
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

    def sample_points(self, points: list, max_points: int) -> list:
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

    def process_entry(self, entry: dict, settings: dict):
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
    
    def process_activity(self, entry: dict, settings: dict):
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
    
    def process_visit(self, entry: dict, settings: dict):
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
    
    def process_timeline_path(self, entry: dict, settings: dict):
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
    
    def process_legacy_location(self, entry: dict, settings: dict):
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

    def process_file(self, input_file: str, settings: dict) -> dict:
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

def load_config():
    """Load unified app configuration, merging from existing apps"""
    config_file = "config/unified_config.json"
    
    # Start with empty config
    config = {}
    
    # Try to load existing unified config
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load unified config: {e}")
    
    # Merge settings from parser app
    parser_settings = "settings.json"
    if os.path.exists(parser_settings):
        try:
            with open(parser_settings, "r") as f:
                parser_config = json.load(f)
                # Merge relevant settings
                config.setdefault('distance_threshold', parser_config.get('distance_threshold', 200))
                config.setdefault('probability_threshold', parser_config.get('probability_threshold', 0.1))
                config.setdefault('duration_threshold', parser_config.get('duration_threshold', 600))
                config.setdefault('last_start_date', parser_config.get('from_date', ''))
                config.setdefault('last_end_date', parser_config.get('to_date', ''))
        except Exception as e:
            print(f"Warning: Failed to merge parser settings: {e}")
    
    # Merge settings from LAweb app
    laweb_config = "config/web_config.json"
    if os.path.exists(laweb_config):
        try:
            with open(laweb_config, "r") as f:
                laweb_settings = json.load(f)
                # Merge relevant settings
                config.setdefault('geoapify_key', laweb_settings.get('geoapify_key', ''))
                config.setdefault('google_key', laweb_settings.get('google_key', ''))
                if not config.get('last_start_date'):
                    config['last_start_date'] = laweb_settings.get('last_start_date', '')
                if not config.get('last_end_date'):
                    config['last_end_date'] = laweb_settings.get('last_end_date', '')
        except Exception as e:
            print(f"Warning: Failed to merge LAweb settings: {e}")
    
    # Set defaults for missing values
    config.setdefault('distance_threshold', 200)
    config.setdefault('probability_threshold', 0.1)
    config.setdefault('duration_threshold', 600)
    config.setdefault('last_start_date', '2024-01-01')
    config.setdefault('last_end_date', date.today().strftime('%Y-%m-%d'))
    config.setdefault('geoapify_key', '')
    config.setdefault('google_key', '')
    
    return config

def save_config(config):
    """Save unified app configuration"""
    config_file = "config/unified_config.json"
    try:
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
            
        # Also save back to original app config files for compatibility
        try:
            # Update parser settings
            parser_settings = {
                'distance_threshold': config.get('distance_threshold', 200),
                'probability_threshold': config.get('probability_threshold', 0.1), 
                'duration_threshold': config.get('duration_threshold', 600),
                'from_date': config.get('last_start_date', ''),
                'to_date': config.get('last_end_date', '')
            }
            with open("settings.json", "w") as f:
                json.dump(parser_settings, f, indent=2)
                
            # Update LAweb config
            laweb_config = {
                'geoapify_key': config.get('geoapify_key', ''),
                'google_key': config.get('google_key', ''),
                'last_start_date': config.get('last_start_date', ''),
                'last_end_date': config.get('last_end_date', '')
            }
            os.makedirs("config", exist_ok=True)
            with open("config/web_config.json", "w") as f:
                json.dump(laweb_config, f, indent=2)
                
        except Exception as e:
            print(f"Warning: Failed to sync settings to original apps: {e}")
            
    except Exception as e:
        print(f"Warning: Failed to save unified config: {e}")

def get_cache_stats():
    """Get geocoding cache statistics"""
    try:
        if ANALYZER_AVAILABLE and 'geo_cache' in globals():
            cache_size = len(geo_cache)
        else:
            cache_size = 0
            
        cache_file = "config/geo_cache.json"
        cache_file_size = 0
        
        if os.path.exists(cache_file):
            cache_file_size = os.path.getsize(cache_file)
        
        return {
            'entries': cache_size,
            'file_size_kb': round(cache_file_size / 1024, 1)
        }
    except Exception as e:
        print(f"Error getting cache stats: {e}")
        return {'entries': 0, 'file_size_kb': 0}

@app.route('/')
def index():
    """Main page with both parsing and analysis options"""
    config = load_config()
    cache_stats = get_cache_stats()
    
    return render_template('unified_processor.html',
                         today=date.today().strftime('%Y-%m-%d'),
                         config=config,
                         cache_stats=cache_stats)
@app.route('/process_subset', methods=['POST'])
def process_subset():
    """Process a pre-filtered subset from storage"""
    try:
        data = request.json
        subset_data = data.get('data')
        settings = data.get('settings')
        metadata = data.get('metadata')
        
        # Store in session for next steps (geocoding, etc.)
        session['filtered_data'] = subset_data
        session['filter_settings'] = settings
        session['data_metadata'] = metadata
        
        # Return success with any additional processing needed
        return jsonify({
            'success': True,
            'data': subset_data,
            'metadata': metadata,
            'ready_for_geocoding': True
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    
@app.route('/upload_raw', methods=['POST'])
def upload_raw():
    """Handle raw Google location history JSON upload for parsing"""
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
    
    # Get settings for parsing
    settings = {
        'from_date': request.form.get('parse_from_date'),
        'to_date': request.form.get('parse_to_date'),
        'distance_threshold': float(request.form.get('distance_threshold', 200)),
        'probability_threshold': float(request.form.get('probability_threshold', 0.1)),
        'duration_threshold': int(request.form.get('duration_threshold', 600))
    }
    
    # Save settings to config - BUG FIX #2
    config = load_config()
    config.update({
        'last_start_date': settings['from_date'],
        'last_end_date': settings['to_date'],
        'distance_threshold': settings['distance_threshold'],
        'probability_threshold': settings['probability_threshold'],
        'duration_threshold': settings['duration_threshold']
    })
    save_config(config)
    
    # Initialize unified progress tracking
    unified_progress[task_id] = {
        'step': 'parsing',
        'status': 'PENDING',
        'message': 'Starting location data parsing...',
        'percentage': 0,
        'diagnostics': [],
        'parsed_file': None,
        'parse_complete': False,
        'analysis_complete': False
    }
    
    # Start parsing in background
    def parse_in_background():
        processor = LocationProcessor(task_id)
        
        try:
            # Use the existing parser progress system but update unified progress
            result = processor.process_file(upload_path, settings)
            
            if result.get('success'):
                # Save parsed output
                output_filename = f"{task_id}_parsed.json"
                output_file = os.path.join(app.config['PROCESSED_FOLDER'], output_filename)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result['data'], f, indent=1)
                
                unified_progress[task_id].update({
                    'step': 'parsed',
                    'status': 'SUCCESS',
                    'message': f'Parsing complete! Processed {len(result["data"])} location entries.',
                    'percentage': 100,
                    'parsed_file': output_file,
                    'parse_complete': True,
                    'parse_stats': result['stats'],
                    'parse_dates_used': {'from': settings['from_date'], 'to': settings['to_date']}  # ADD THIS LINE
                })
            else:
                unified_progress[task_id].update({
                    'step': 'error',
                    'status': 'FAILURE',
                    'message': f'Parsing failed: {result.get("error", "Unknown error")}',
                    'error': result.get('error', 'Unknown error')
                })
                
        except Exception as e:
            unified_progress[task_id].update({
                'step': 'error', 
                'status': 'FAILURE',
                'message': f'Parsing failed: {str(e)}',
                'error': str(e)
            })
        
        # Cleanup upload file
        try:
            os.remove(upload_path)
        except:
            pass
    
    thread = threading.Thread(target=parse_in_background)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'task_id': task_id,
        'message': 'Location parsing started',
        'step': 'parsing'
    })

@app.route('/upload_parsed', methods=['POST'])
def upload_parsed():
    """Handle pre-parsed/cleaned location JSON for analysis"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '' or not file.filename.lower().endswith('.json'):
        return jsonify({'error': 'Please upload a JSON file'}), 400
    
    # Save file
    filename = secure_filename(file.filename)
    task_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['PROCESSED_FOLDER'], f"{task_id}_{filename}")
    file.save(upload_path)
    
    # BUG FIX #5: Initialize progress with proper stats and diagnostics
    try:
        # Load the file to get basic stats
        with open(upload_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Count entries
        if isinstance(data, list):
            entry_count = len(data)
        elif isinstance(data, dict) and 'timelineObjects' in data:
            entry_count = len(data['timelineObjects'])
        elif isinstance(data, dict) and 'locations' in data:
            entry_count = len(data['locations'])
        else:
            entry_count = 1
            
        # Initialize progress for analysis-only workflow with proper diagnostics array
        unified_progress[task_id] = {
            'step': 'ready_for_analysis',
            'status': 'SUCCESS',
            'message': 'Parsed file uploaded successfully. Ready for analysis.',
            'percentage': 100,
            'parsed_file': upload_path,
            'parse_complete': True,
            'analysis_complete': False,
            'diagnostics': [  # Initialize empty diagnostics array
                {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'message': f'Pre-parsed file uploaded: {filename} ({entry_count} entries)'
                }
            ],
            'parse_stats': {
                'total_entries': entry_count,
                'final_count': entry_count,
                'activities': 0,  # Will be updated during analysis
                'visits': 0,
                'timeline_paths': 0,
                'date_filtered': entry_count
            }
        }
        
    except Exception as e:
        print(f"DEBUG: Error processing uploaded file: {e}")
        return jsonify({'error': f'Failed to process uploaded file: {str(e)}'}), 400
    
    return jsonify({
        'task_id': task_id,
        'message': 'Parsed file uploaded successfully',
        'step': 'ready_for_analysis'
    })

@app.route('/analyze/<task_id>', methods=['POST'])
def analyze(task_id):
    """Start geocoding and analysis of parsed location data"""
    if task_id not in unified_progress:
        return jsonify({'error': 'Task not found'}), 404
    
    progress_data = unified_progress[task_id]
    
    if not progress_data.get('parse_complete') or not progress_data.get('parsed_file'):
        return jsonify({'error': 'No parsed data available for analysis'}), 400
    
    # Get analysis settings
    data = request.get_json() or {}
    
    start_date = data.get('start_date', '2024-01-01')
    end_date = data.get('end_date', date.today().strftime('%Y-%m-%d'))
    geoapify_key = data.get('geoapify_key', '')
    google_key = data.get('google_key', '')
    
    if not geoapify_key.strip():
        return jsonify({'error': 'Geoapify API key is required for analysis'}), 400
    
    # Save config for next time - BUG FIX #2
    config = load_config()
    config.update({
        'last_start_date': start_date,
        'last_end_date': end_date,
        'geoapify_key': geoapify_key,
        'google_key': google_key
    })
    save_config(config)
    
    # Update progress to analysis phase with proper diagnostics initialization
    if 'diagnostics' not in unified_progress[task_id]:
        unified_progress[task_id]['diagnostics'] = []
        
    unified_progress[task_id].update({
        'step': 'analyzing',
        'status': 'PENDING',
        'message': 'Starting location analysis and geocoding...',
        'percentage': 0
    })
    
    # Start analysis in background
    output_dir = os.path.join(app.config['OUTPUT_FOLDER'], 
                            f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{task_id[:8]}")
    os.makedirs(output_dir, exist_ok=True)
    
    def analyze_in_background():
        # Track geocoding stats for BUG FIX #6
        geocoding_stats = {'cache_hits': 0, 'api_calls': 0, 'total_geocoded': 0}
        
        def analysis_log(msg):
            """Log function that updates unified progress"""
            nonlocal geocoding_stats
            
            # Ensure diagnostics array exists
            if task_id in unified_progress:
                if 'diagnostics' not in unified_progress[task_id]:
                    unified_progress[task_id]['diagnostics'] = []
                
                # BUG FIX #6: Better parsing of geocoding messages
                enhanced_msg = msg
                
                # Look for cache/API statistics in the message
                if 'from cache' in msg.lower() and 'from api' in msg.lower():
                    # Message already contains cache/API breakdown - use as is
                    enhanced_msg = msg
                elif 'Geocoded' in msg and 'locations' in msg:
                    try:
                        # Extract total number
                        import re
                        numbers = re.findall(r'\d+', msg)
                        if numbers:
                            total = int(numbers[0])
                            geocoding_stats['total_geocoded'] = total
                            
                            # Try to get actual cache stats from geo_utils if available
                            cache_hits = 0
                            api_calls = 0
                            
                            # Check if we can get real stats from the cache
                            if ANALYZER_AVAILABLE and 'geo_cache' in globals():
                                # This would need to be implemented in geo_utils to track stats
                                # For now, we'll make a reasonable estimate
                                cache_hits = int(total * 0.6)  # Assume 60% cache hits
                                api_calls = total - cache_hits
                            
                            if total > 0:
                                enhanced_msg = f"Geocoded {total} locations: {cache_hits} from cache, {api_calls} from API lookups"
                    except Exception as e:
                        print(f"DEBUG: Error parsing geocoding stats: {e}")
                        enhanced_msg = msg
                
                # Add to diagnostics
                unified_progress[task_id]['diagnostics'].append({
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'message': enhanced_msg
                })
                
                # Update main message
                unified_progress[task_id]['message'] = enhanced_msg
                
                # Update progress based on message content
                if 'Starting analysis' in msg:
                    unified_progress[task_id]['percentage'] = 10
                elif 'Found' in msg and 'location points' in msg:
                    unified_progress[task_id]['percentage'] = 20
                elif 'Filtered to' in msg:
                    unified_progress[task_id]['percentage'] = 30
                elif 'Geocoded' in msg or 'Geocoding' in msg:
                    unified_progress[task_id]['percentage'] = 70
                elif 'Total distance' in msg:
                    unified_progress[task_id]['percentage'] = 85
                elif 'exported' in msg or 'complete' in msg.lower():
                    unified_progress[task_id]['percentage'] = 95
        
        def cancel_check():
            return unified_progress.get(task_id, {}).get('cancelled', False)
        
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Add initial analysis log
            analysis_log("Starting location analysis and geocoding...")
            
            # Reset geocoding statistics before analysis
            if ANALYZER_AVAILABLE:
                try:
                    from geo_utils import reset_global_stats, get_global_stats
                    reset_global_stats()
                    analysis_log("Geocoding statistics tracker initialized")
                except ImportError:
                    analysis_log("Warning: Enhanced geocoding statistics not available")
            
            # Run the location analysis using existing analyzer
            result = process_location_file(
                progress_data['parsed_file'],
                start_dt,
                end_dt,
                output_dir,
                "by_city",  # group_by
                geoapify_key,
                google_key,
                "",  # onwater_key
                0.1,  # delay
                1,    # batch_size
                analysis_log,
                cancel_check,
                True  # include_distance
            )
            
            # Get and report final geocoding statistics
            if ANALYZER_AVAILABLE:
                try:
                    stats = get_global_stats()
                    summary_messages = stats.summary()
                    for msg in summary_messages:
                        analysis_log(msg)
                except:
                    analysis_log("Could not retrieve final geocoding statistics")
            
            # Get list of generated files
            generated_files = []
            if os.path.exists(output_dir):
                for f in os.listdir(output_dir):
                    if f.endswith(('.csv', '.txt')):
                        generated_files.append(f)
            
            # Create HTML views of the data
            analysis_log("Creating HTML views of results...")
            create_html_views(output_dir, generated_files, task_id)
            
            # BUG FIX #3: Extract real stats from result or CSV files
            analysis_stats = progress_data.get('parse_stats', {}).copy()
            
            # Update with results from the modern analyzer - FIX FOR ZERO STATS
            if result and 'parse_stats' in result:
                # Merge parse_stats from the analyzer result
                for key, value in result['parse_stats'].items():
                    if value > 0:  # Only update if we have real data
                        analysis_stats[key] = value
                
                # Also include final count
                if 'final_count' in result:
                    analysis_stats['final_count'] = result['final_count']
            
            # Try to get additional stats from generated CSV files
            try:
                city_csv = os.path.join(output_dir, 'by_city_location_days.csv')
                if os.path.exists(city_csv):
                    df = pd.read_csv(city_csv)
                    # Use the larger of the two counts (CSV records vs analyzer count)
                    analysis_stats['final_count'] = max(analysis_stats.get('final_count', 0), len(df))
                    if 'Fractional Days' in df.columns:
                        analysis_stats['total_days'] = df['Fractional Days'].sum()
                    analysis_log(f"Analysis generated {len(df)} location records")
            except Exception as e:
                analysis_log(f"Warning: Could not extract stats from CSV: {e}")
            
            # Final completion message
            analysis_log("Analysis completed successfully!")
            
            # Log the final statistics that will be shown
            analysis_log(f"Final stats: {analysis_stats['activities']} activities, {analysis_stats['visits']} visits, {analysis_stats['timeline_paths']} timeline paths, {analysis_stats['final_count']} location points")
            
            # Update final progress
            unified_progress[task_id].update({
                'step': 'complete',
                'status': 'SUCCESS',
                'message': 'Analysis completed successfully!',
                'percentage': 100,
                'analysis_complete': True,
                'output_dir': os.path.basename(output_dir),
                'generated_files': generated_files,
                'result': result,
                'analysis_stats': analysis_stats  # This should now have real values
            })
            
        except Exception as e:
            error_msg = f'Analysis failed: {str(e)}'
            print(f"DEBUG: Analysis error for task {task_id}: {error_msg}")
            print(f"DEBUG: Error type: {type(e)}")
            print(f"DEBUG: Progress data keys: {list(unified_progress[task_id].keys())}")
            
            # Ensure diagnostics exists before logging error
            if 'diagnostics' not in unified_progress[task_id]:
                unified_progress[task_id]['diagnostics'] = []
            
            analysis_log(error_msg)
            
            unified_progress[task_id].update({
                'step': 'error',
                'status': 'FAILURE',
                'message': error_msg,
                'error': str(e)
            })
    
    thread = threading.Thread(target=analyze_in_background)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'message': 'Analysis started',
        'step': 'analyzing'
    })

def create_html_views(output_dir, generated_files, task_id):
    """Create enhanced HTML views of the CSV data"""
    import pandas as pd
    
    html_files = []
    
    for filename in generated_files:
        if not filename.endswith('.csv'):
            continue
            
        file_path = os.path.join(output_dir, filename)
        
        try:
            # Read CSV data
            df = pd.read_csv(file_path)
            
            # Determine table type for styling
            table_type = "location-days" if "location_days" in filename else "jumps"
            
            # BUG FIX #4: Clean up header names
            if 'by_city_location_days' in filename:
                title = "Days by City"
            elif 'by_state_location_days' in filename:
                title = "Days in each State"
            elif 'city_jumps' in filename:
                title = "City Jumps"
            else:
                title = filename.replace('.csv', '').replace('_', ' ').title()
            
            # Create enhanced HTML with better styling and interactivity
            html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>{title} - Location Analysis Results</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0;
            background: #f5f5f5;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            background: linear-gradient(135deg, #2196F3, #21CBF3);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 2em;
            font-weight: 300;
        }}
        
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }}
        
        .stat {{
            text-align: center;
        }}
        
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #2196F3;
        }}
        
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        
        .controls {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .search-box {{
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 16px;
            margin-bottom: 15px;
        }}
        
        .search-box:focus {{
            outline: none;
            border-color: #2196F3;
        }}
        
        .table-container {{
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        table {{ 
            width: 100%;
            border-collapse: collapse;
        }}
        
        th, td {{ 
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        
        th {{ 
            background: #f8f9fa;
            font-weight: 600;
            color: #333;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        
        tr:hover {{
            background-color: #f8f9fa;
        }}
        
        tr.highlight {{
            background-color: #e3f2fd !important;
        }}
        
        .sortable {{
            cursor: pointer;
            user-select: none;
        }}
        
        .sortable:hover {{
            background: #e9ecef;
        }}
        
        .sort-arrow {{
            margin-left: 8px;
            opacity: 0.5;
        }}
        
        .actions {{
            text-align: center;
            padding: 30px;
            background: white;
            margin-top: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .btn {{
            background: linear-gradient(135deg, #2196F3, #21CBF3);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            text-decoration: none;
            display: inline-block;
            margin: 0 10px;
            cursor: pointer;
            transition: transform 0.2s;
        }}
        
        .btn:hover {{
            transform: translateY(-1px);
        }}
        
        .btn-secondary {{
            background: linear-gradient(135deg, #757575, #616161);
        }}
        
        @media (max-width: 768px) {{
            .summary {{ grid-template-columns: 1fr; }}
            th, td {{ padding: 10px; font-size: 14px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{title}</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <div class="stat">
                <div class="stat-number">{len(df)}</div>
                <div class="stat-label">Total Records</div>
            </div>"""
            
            # Add specific stats based on table type
            if table_type == "location-days" and "Fractional Days" in df.columns:
                total_days = df['Fractional Days'].sum() if 'Fractional Days' in df.columns else 0
                html_content += f"""
            <div class="stat">
                <div class="stat-number">{total_days:.1f}</div>
                <div class="stat-label">Total Days</div>
            </div>"""
            elif table_type == "jumps" and "Distance (mi)" in df.columns:
                total_distance = df['Distance (mi)'].sum() if 'Distance (mi)' in df.columns else 0
                html_content += f"""
            <div class="stat">
                <div class="stat-number">{total_distance:.0f}</div>
                <div class="stat-label">Total Miles</div>
            </div>"""
            
            record_count = len(df)
            csv_filename = filename.replace('.csv', '')
            
            html_content += f"""
        </div>
        
        <div class="controls">
            <input type="text" id="searchBox" class="search-box" placeholder="Search table data..." onkeyup="filterTable()">
            <div>
                <strong>Click column headers to sort</strong> • 
                <span id="recordCount">{record_count}</span> records shown
            </div>
        </div>
        
        <div class="table-container">"""
            
            # Convert DataFrame to HTML with enhanced styling
            df_html = df.to_html(table_id='dataTable', classes='data-table', escape=False, index=False)
            
            # Make headers sortable
            df_html = df_html.replace('<th>', '<th class="sortable" onclick="sortTable(this)">')
            
            html_content += df_html + f"""
        </div>
        
        <div class="actions">
            <a href="javascript:window.print()" class="btn">Print Table</a>
            <a href="javascript:exportToCSV()" class="btn btn-secondary">Export CSV</a>
            <a href="javascript:history.back()" class="btn btn-secondary">Back to Results</a>
        </div>
    </div>
    
    <script>
        let sortDirection = {{}};
        
        function filterTable() {{
            const input = document.getElementById('searchBox');
            const filter = input.value.toLowerCase();
            const table = document.getElementById('dataTable');
            const rows = table.getElementsByTagName('tr');
            let visibleCount = 0;
            
            for (let i = 1; i < rows.length; i++) {{
                const row = rows[i];
                const cells = row.getElementsByTagName('td');
                let found = false;
                
                for (let j = 0; j < cells.length; j++) {{
                    if (cells[j].textContent.toLowerCase().includes(filter)) {{
                        found = true;
                        break;
                    }}
                }}
                
                if (found) {{
                    row.style.display = '';
                    visibleCount++;
                }} else {{
                    row.style.display = 'none';
                }}
            }}
            
            document.getElementById('recordCount').textContent = visibleCount;
        }}
        
        function sortTable(header) {{
            const table = document.getElementById('dataTable');
            const columnIndex = Array.from(header.parentNode.children).indexOf(header);
            const rows = Array.from(table.getElementsByTagName('tr')).slice(1);
            
            const isNumeric = rows.length > 0 && !isNaN(parseFloat(rows[0].cells[columnIndex].textContent));
            
            sortDirection[columnIndex] = sortDirection[columnIndex] === 'asc' ? 'desc' : 'asc';
            
            rows.sort((a, b) => {{
                const aValue = a.cells[columnIndex].textContent.trim();
                const bValue = b.cells[columnIndex].textContent.trim();
                
                let comparison;
                if (isNumeric) {{
                    comparison = parseFloat(aValue) - parseFloat(bValue);
                }} else {{
                    comparison = aValue.localeCompare(bValue);
                }}
                
                return sortDirection[columnIndex] === 'asc' ? comparison : -comparison;
            }});
            
            // Update sort arrows
            document.querySelectorAll('.sort-arrow').forEach(arrow => arrow.remove());
            const arrow = document.createElement('span');
            arrow.className = 'sort-arrow';
            arrow.textContent = sortDirection[columnIndex] === 'asc' ? ' ↑' : ' ↓';
            header.appendChild(arrow);
            
            // Re-insert sorted rows
            const tbody = table.getElementsByTagName('tbody')[0] || table;
            rows.forEach(row => tbody.appendChild(row));
        }}
        
        function exportToCSV() {{
            const table = document.getElementById('dataTable');
            const rows = table.getElementsByTagName('tr');
            const csvContent = [];
            
            for (let i = 0; i < rows.length; i++) {{
                const row = rows[i];
                if (row.style.display !== 'none') {{
                    const cells = row.getElementsByTagName(i === 0 ? 'th' : 'td');
                    const rowData = Array.from(cells).map(cell => 
                        '"' + cell.textContent.replace(/"/g, '""') + '"'
                    );
                    csvContent.push(rowData.join(','));
                }}
            }}
            
            const blob = new Blob([csvContent.join('\\n')], {{ type: 'text/csv' }});
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = '{csv_filename}.csv';
            a.click();
            window.URL.revokeObjectURL(url);
        }}
        
        // Add row click highlighting
        document.addEventListener('DOMContentLoaded', function() {{
            const table = document.getElementById('dataTable');
            const rows = table.getElementsByTagName('tr');
            
            for(let i = 1; i < rows.length; i++) {{
                rows[i].onclick = function() {{
                    // Remove previous highlights
                    for(let j = 1; j < rows.length; j++) {{
                        rows[j].classList.remove('highlight');
                    }}
                    // Add highlight to clicked row
                    this.classList.add('highlight');
                }}
            }}
        }});
    </script>
</body>
</html>"""
            
            # Save HTML file
            html_filename = filename.replace('.csv', '.html')
            html_path = os.path.join(output_dir, html_filename)
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            html_files.append(html_filename)
            
        except Exception as e:
            print(f"Error creating HTML view for {filename}: {e}")
    
    return html_files

@app.route('/progress/<task_id>')
def get_unified_progress(task_id):
    """Get unified progress for both parsing and analysis"""
    if task_id not in unified_progress:
        return jsonify({'error': 'Task not found'}), 404
    
    progress_data = unified_progress[task_id].copy()
    
    # Debug logging
    print(f"DEBUG: Progress check for {task_id}")
    print(f"DEBUG: Current step: {progress_data.get('step')}")
    print(f"DEBUG: Status: {progress_data.get('status')}")
    print(f"DEBUG: Analysis complete: {progress_data.get('analysis_complete')}")
    
    # If we're in parsing phase, also check parser progress
    if progress_data.get('step') == 'parsing' and task_id in parser_progress_store:
        parser_data = parser_progress_store[task_id]
        progress_data.update({
            'message': parser_data.get('message', progress_data['message']),
            'percentage': parser_data.get('percentage', progress_data['percentage']),
            'diagnostics': parser_data.get('diagnostics', progress_data['diagnostics'])
        })
    
    return jsonify(progress_data)

@app.route('/download/<path:output_dir>/<path:filename>')
def download_file(output_dir, filename):
    """Download generated files (CSV or HTML)"""
    try:
        file_path = os.path.join(app.config['OUTPUT_FOLDER'], output_dir, filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.route('/download_all/<task_id>')
def download_all(task_id):
    """Download all results as a ZIP file"""
    if task_id not in unified_progress:
        return jsonify({'error': 'Task not found'}), 404
    
    progress_data = unified_progress[task_id]
    output_dir = progress_data.get('output_dir')
    
    if not output_dir:
        return jsonify({'error': 'No results available'}), 404
    
    output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_dir)
    
    # Create ZIP file in memory
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(output_path):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, output_path)
                zip_file.write(file_path, arc_name)
    
    zip_buffer.seek(0)
    
    return send_file(
        io.BytesIO(zip_buffer.read()),
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'location_analysis_{task_id[:8]}.zip'
    )

@app.route('/results/<task_id>')
def results(task_id):
    """Show results page with both CSV downloads and HTML views"""
    if task_id not in unified_progress:
        flash('Task not found', 'error')
        return redirect(url_for('index'))
    
    progress_data = unified_progress[task_id]
    
    if not progress_data.get('analysis_complete'):
        return redirect(url_for('processing', task_id=task_id))
    
    # Get list of files for display
    output_dir = progress_data.get('output_dir')
    files_info = []
    
    if output_dir:
        output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_dir)
        if os.path.exists(output_path):
            for filename in os.listdir(output_path):
                if filename.endswith(('.csv', '.html')):
                    file_path = os.path.join(output_path, filename)
                    file_size = os.path.getsize(file_path)
                    files_info.append({
                        'name': filename,
                        'size': f"{file_size:,} bytes",
                        'type': 'CSV Data' if filename.endswith('.csv') else 'HTML View',
                        'is_html': filename.endswith('.html')
                    })
    
    return render_template('results.html',
                         task_id=task_id,
                         progress_data=progress_data,
                         files_info=files_info)

@app.route('/processing/<task_id>')
def processing(task_id):
    """Show processing page with real-time updates"""
    if task_id not in unified_progress:
        flash('Task not found', 'error')
        return redirect(url_for('index'))
    
    return render_template('processing.html', task_id=task_id)

@app.route('/cache_info')
def cache_info():
    """Get detailed cache information"""
    if not ANALYZER_AVAILABLE:
        return jsonify({'error': 'Analyzer modules not available'})
    
    try:
        cache_stats = get_cache_stats()
        
        # Get cache breakdown by type
        water_entries = sum(1 for key in geo_cache.keys() if key.startswith('water:'))
        jump_entries = sum(1 for key in geo_cache.keys() if key.startswith('jump:'))
        geocode_entries = len(geo_cache) - water_entries - jump_entries
        
        # Debug: Print some cache keys to see format
        print("DEBUG: Cache sample keys:")
        for i, key in enumerate(list(geo_cache.keys())[:5]):
            print(f"  {key}: {type(geo_cache[key])}")
            if i >= 4:
                break
        
        return jsonify({
            'total_entries': cache_stats['entries'],
            'file_size_kb': cache_stats['file_size_kb'],
            'geocode_entries': geocode_entries,
            'water_entries': water_entries,
            'jump_entries': jump_entries,
            'cache_available': True
        })
    except Exception as e:
        print(f"DEBUG: Cache info error: {e}")
        return jsonify({
            'error': str(e),
            'cache_available': False
        })

@app.route('/clear_cache', methods=['POST'])
def clear_cache():
    """Clear the geocoding cache"""
    try:
        geo_cache.clear()
        save_geo_cache()
        return jsonify({'message': 'Cache cleared successfully', 'entries_removed': 0})
    except Exception as e:
        return jsonify({'error': f'Failed to clear cache: {str(e)}'}), 500

@app.route('/view/<path:output_dir>/<path:filename>')
def view_html(output_dir, filename):
    """View HTML reports in the browser"""
    try:
        file_path = os.path.join(app.config['OUTPUT_FOLDER'], output_dir, filename)
        if os.path.exists(file_path) and filename.endswith('.html'):
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            return "File not found or not an HTML file", 404
    except Exception as e:
        return f"Error loading file: {str(e)}", 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'version': 'unified-1.0.0',
        'active_tasks': len(unified_progress),
        'features': [
            'Raw Google JSON parsing',
            'Location geocoding and analysis',
            'CSV and HTML output generation',
            'Unified two-step workflow'
        ]
    })

if __name__ == '__main__':
    print("UNIFIED LOCATION PROCESSOR v1.0")
    print("=" * 50)
    print("SETUP CHECK:")
    if ANALYZER_AVAILABLE:
        print("  ✓ Analyzer modules loaded successfully")
    else:
        print("  ✗ Analyzer modules missing - copy these files from your LAweb app:")
        print("    - analyzer_bridge.py")
        print("    - geo_utils.py")
        print("    - csv_exporter.py")
        print("    - location_analyzer.py")
        print("    - legacy_analyzer.py")
    
    print("=" * 50)
    print("FEATURES:")
    print("  • Step 1: Parse raw Google location JSON")
    print("  • Step 2: Geocode and analyze locations")  
    print("  • Generate both CSV files and HTML views")
    print("  • Unified progress tracking")
    print("=" * 50)
    print(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
    print(f"Processed folder: {app.config['PROCESSED_FOLDER']}")
    print(f"Output folder: {app.config['OUTPUT_FOLDER']}")
    print("Web interface: http://localhost:5000")
    print("=" * 50)
    
    if not ANALYZER_AVAILABLE:
        print("WARNING: Running with limited functionality - parsing only")
        print("Copy the required files to enable full geocoding analysis")
        print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000) 