# St. Louis Police Calls Scraper

This tool allows you to scrape and analyze police calls for service data from the St. Louis Metropolitan Police Department. You can filter data for specific neighborhoods:

- Tower Grove Heights
- Tower Grove South
- St. Louis City-wide

## Requirements

- Python 3.6+
- Google account with Google Sheets access
- Google Cloud service account with Google Sheets API access

## Installation

1. Download the appropriate script for your neighborhood
2. Install required Python packages:

pip install requests beautifulsoup4 gspread google-auth

3. Place your Google service account JSON key in a folder named "credentials" in the same directory as the script
4. Run the script:

python3 scraper_TGH.py

## Usage

The script will:
1. Scrape police calls data from the SLMPD website
2. Filter for calls in your selected neighborhood
3. Create or update a Google Sheet with the data
4. Provide a URL to access the spreadsheet

## Created by Tower Grove Safety App - 2025