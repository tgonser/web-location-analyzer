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


## Overview
Thank you for the correction! Here's the updated description:

## Short Description:
**A privacy-focused Google Timeline analyzer that processes your location history exported directly from Google Maps mobile app, using reverse geocoding to identify places you've visited and creating detailed visualizations and insights about your travel patterns - all processed locally in your browser.**

## Detailed Description for your README:

### What It Does:

**Web Location Analyzer** is a comprehensive tool that transforms your Google Timeline data into meaningful insights and interactive visualizations. It processes the location history JSON file exported directly from the Google Maps mobile app to help you understand your movement patterns and travel history.

### How to Get Your Data:

1. Open Google Maps on your mobile device
2. Tap your profile picture/icon
3. Select "Your Timeline"
4. Tap the three dots menu ("...")
5. Choose "Location and privacy settings"
6. Scroll down to "Export Timeline data"
7. Save the resulting JSON file

**Note:** This uses the NEW Timeline export format from Google Maps mobile, NOT the older Google Takeout files.

### Key Features:

**📊 Data Analysis**
- Processes the new Google Timeline JSON format (location-history.json)
- Uses Geoapify and Google reverse geocoding APIs to identify locations from coordinates
- Identifies and categorizes visits to different places
- Calculates travel distances and time spent at locations
- Detects "city jumps" (rapid transitions between distant locations)
- Analyzes both semantic location data and raw GPS coordinates

**🗺️ Interactive Visualizations**
- City distribution charts displaying time spent in different cities
- Month-by-month activity breakdowns
- Interactive maps with location markers
- Travel path visualizations
- Location visit frequency displays

**📈 Statistics & Insights**
- Total distance traveled
- Number of unique places visited
- Time distribution across different cities
- Visit frequency analysis
- Activity timeline summaries
- Monthly and yearly comparisons
- City jump analysis with distance calculations

**🔒 Privacy-First Design**
- All processing happens locally in your browser
- Only coordinates are sent to geocoding APIs (no personal data)
- Includes geocoding cache to minimize API calls and speed up processing
- Your location data never leaves your control

### Use Cases:
- Personal travel history visualization
- Year-in-review summaries
- Travel pattern analysis
- Memory journaling with location context
- Understanding your daily/monthly movement patterns

This tool is perfect for anyone who wants to gain insights from their location history while maintaining complete privacy and control over their personal data.