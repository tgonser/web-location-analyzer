# gui_integration.py - Fixed version with reduced verbose output
import asyncio
import threading
from datetime import date, datetime
import traceback
import sys
import os

# Reduce initial verbose output
PSUTIL_AVAILABLE = False
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    pass  # Silently handle missing psutil

def get_memory_usage():
    """Get memory usage if psutil available, otherwise return 0"""
    if PSUTIL_AVAILABLE:
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0
    return 0

def ensure_date_object(date_input):
    """Ensure input is a date object, convert from string if needed"""
    if isinstance(date_input, date):
        return date_input
    elif isinstance(date_input, str):
        try:
            return datetime.strptime(date_input, '%Y-%m-%d').date()
        except ValueError:
            return date.today()
    else:
        return date.today()

# Try importing analyzers with reduced verbosity
NEW_ANALYZER_AVAILABLE = False
OLD_ANALYZER_AVAILABLE = False

try:
    from location_analyzer import LocationAnalyzer, AnalysisConfig
    NEW_ANALYZER_AVAILABLE = True
except ImportError:
    pass  # Silently handle missing dependencies

try:
    from legacy_analyzer import process_location_file as old_process_location_file
    OLD_ANALYZER_AVAILABLE = True
except ImportError:
    pass

def process_location_file(file_path, start_date, end_date, output_dir, group_by,
                         geoapify_key, google_key, onwater_key, delay, batch_size,
                         log_func, cancel_check, include_distance=True):
    """
    Main bridge function that routes to the best available analyzer.
    Maintains full compatibility with existing GUI.
    """
    
    # CRITICAL FIX: Ensure dates are date objects, not strings
    start_date = ensure_date_object(start_date)
    end_date = ensure_date_object(end_date)
    
    # Check for cancellation
    if hasattr(cancel_check, '__call__') and cancel_check():
        log_func("Analysis cancelled before start")
        return {}
    
    # Determine best analyzer with minimal logging
    if NEW_ANALYZER_AVAILABLE and geoapify_key.strip():
        return run_new_analyzer(file_path, start_date, end_date, output_dir, 
                               geoapify_key, google_key, delay, log_func, cancel_check)
    elif OLD_ANALYZER_AVAILABLE:
        return run_old_analyzer(file_path, start_date, end_date, output_dir, group_by,
                               geoapify_key, google_key, onwater_key, delay, batch_size,
                               log_func, cancel_check, include_distance)
    else:
        log_func("ERROR: No analyzer available! Install requirements: pip install aiohttp pandas")
        return {}

def run_new_analyzer(file_path, start_date, end_date, output_dir, 
                    geoapify_key, google_key, delay, log_func, cancel_check):
    """Run the new async analyzer in a thread-safe way"""
    try:
        # Ensure dates are date objects
        start_date = ensure_date_object(start_date)
        end_date = ensure_date_object(end_date)
        
        # Create configuration
        config = AnalysisConfig(
            geoapify_key=geoapify_key,
            google_key=google_key,
            api_delay=max(0.1, delay/3),
            min_distance_filter=0.5,
            max_concurrent_requests=8
        )
        
        analyzer = LocationAnalyzer(config)
        
        # Create a filtered log function to reduce verbosity
        def filtered_log(msg):
            # Filter out verbose debug messages
            if any(skip in msg for skip in [
                '🚀', '📍', '📊', '📈', '✅', '🔍', '🌍', '💾', '🗺️', '🏃',
                'About to call', 'Method type:', 'Method module:', 'returned',
                'Progress:', 'Parsing'
            ]):
                return  # Skip verbose messages
            
            # Only pass important messages to the original log function
            if any(keep in msg for keep in [
                'Starting analysis', 'Found', 'Filtered', 'Geocoded', 
                'Total distance', 'Total jumps', 'exported', 'complete'
            ]):
                log_func(msg)
        
        # Override analyzer logging
        if hasattr(analyzer, '_log'):
            analyzer._original_log = analyzer._log
            analyzer._log = filtered_log
        
        # Run analysis in new event loop (thread-safe)
        def run_in_thread():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def analysis_with_cancellation():
                    if hasattr(cancel_check, '__call__') and cancel_check():
                        return {}
                    
                    result = await analyzer.analyze_location_history(
                        file_path=file_path,
                        start_date=start_date,
                        end_date=end_date,
                        output_dir=output_dir
                    )
                    return result
                
                return loop.run_until_complete(analysis_with_cancellation())
            except Exception as e:
                log_func(f"ERROR: Analysis failed: {e}")
                return {}
            finally:
                loop.close()
        
        # Execute in separate thread
        result_container = [{}]
        exception_container = [None]
        
        def thread_target():
            try:
                result_container[0] = run_in_thread()
            except Exception as e:
                exception_container[0] = e
        
        thread = threading.Thread(target=thread_target)
        thread.daemon = True
        thread.start()
        thread.join()
        
        if exception_container[0]:
            raise exception_container[0]
        
        result = result_container[0]
        return result
        
    except Exception as e:
        log_func(f"ERROR: NEW analyzer failed: {e}")
        
        # Try fallback if available
        if OLD_ANALYZER_AVAILABLE:
            return run_old_analyzer(file_path, start_date, end_date, output_dir, "city",
                                  geoapify_key, google_key, "", delay, 1,
                                  log_func, cancel_check, True)
        return {}

def run_old_analyzer(file_path, start_date, end_date, output_dir, group_by,
                    geoapify_key, google_key, onwater_key, delay, batch_size,
                    log_func, cancel_check, include_distance=True):
    """Run the old analyzer with reduced verbosity"""
    try:
        # Ensure dates are date objects
        start_date = ensure_date_object(start_date)
        end_date = ensure_date_object(end_date)
        
        # Create filtered log function for old analyzer too
        def filtered_log(msg):
            # Filter out emojis and verbose messages
            if any(skip in msg for skip in [
                '🔄', '📍', '📊', '📈', '✅', '🔍', '🌍', '💾', '🗺️', '🏃',
                'Loading:', 'Parsed', 'coordinates', 'unique timestamps',
                'Modes seen:', 'Timestamps:'
            ]):
                return
            
            # Pass through important messages
            log_func(msg)
        
        result = old_process_location_file(
            file_path, start_date, end_date, output_dir, group_by,
            geoapify_key, google_key, onwater_key, delay, batch_size,
            filtered_log, cancel_check, include_distance=include_distance
        )
        
        return result or {}
        
    except Exception as e:
        log_func(f"ERROR: Analysis failed: {e}")
        return {}

def test_analyzer_imports():
    """Test and report which analyzers are available"""
    print("TESTING ANALYZER IMPORTS:")
    print(f"   NEW analyzer: {'AVAILABLE' if NEW_ANALYZER_AVAILABLE else 'NOT AVAILABLE'}")
    print(f"   OLD analyzer: {'AVAILABLE' if OLD_ANALYZER_AVAILABLE else 'NOT AVAILABLE'}")
    print(f"   psutil: {'AVAILABLE' if PSUTIL_AVAILABLE else 'NOT AVAILABLE'}")

if __name__ == "__main__":
    test_analyzer_imports()