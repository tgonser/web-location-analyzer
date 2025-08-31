# Google location history Location Processor

Process and analyze Google Location History data with geocoding and travel analysis.

## Features
- Parse Google Location History JSON files from mobile uploads, NOT for takeout file structures
- Step one - Filter by date range and thresholds to produce smaller, better files
- Step 2 - Geocode locations using Geoapify/Google APIs
- Result: Generate travel analysis reports (CSV and HTML)
- Caching system to reduce API calls
- Batch send APIs

## Notes:
When parsing, we find a setting of 600-2000 for distance (Meters), and 500-100 (seconds) is good. MORE than this misses data, less is just noise.  The probability we set to .25 but it does not impact much unless you make it higher than .65-.8

## Setup
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python unified_app.py`
4. Open browser to `http://localhost:5000`

## Required API Keys
- Geoapify API key (required for geocoding)
- Google Maps API key (optional, for enhanced results)

## Configuration
Settings are stored in `config/` directory and persist between sessions.