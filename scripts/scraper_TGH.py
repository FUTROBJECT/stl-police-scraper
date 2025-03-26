import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import time
import logging
from datetime import datetime
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='tgh_police_calls_scraper.log'
)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Your email address to share the spreadsheet with
YOUR_EMAIL = "adamlaserlab@gmail.com"

# Define Tower Grove Heights boundaries
TGH_STREETS = [
    "ARSENAL", "HARTFORD", "CONNECTICUT", "JUNIATA", 
    "HUMPHREY", "WYOMING", "UTAH", "SHENANDOAH",
    "TOWER GROVE", "SPRING", "GUSTINE", "GRAND",
    "OAK HILL", "MORGANFORD", "HEREFORD"
]

TGH_BOUNDARIES = {
    "north": "ARSENAL",       # North boundary
    "south": "UTAH",          # South boundary
    "east": "GRAND",          # East boundary 
    "west": "GUSTINE"         # West boundary
}

def is_in_tower_grove_heights(address):
    """Check if an address is in Tower Grove Heights neighborhood."""
    if not address:
        return False
        
    # Convert to uppercase for case-insensitive comparison
    address_upper = address.upper()
    
    # Check if address contains any of the Tower Grove Heights streets
    for street in TGH_STREETS:
        if street in address_upper:
            # Check if it's within the boundaries
            # Extract the house number if available
            match = re.search(r'(\d+)\s+([A-Za-z\s]+)', address_upper)
            if match:
                house_number = int(match.group(1))
                street_name = match.group(2).strip()
                
                # For east-west streets (Arsenal, Hartford, Connecticut, etc.)
                if street not in [TGH_BOUNDARIES["east"], TGH_BOUNDARIES["west"]]:
                    # Check if between Grand and Gustine (approximately 3600-4000 block)
                    if 3600 <= house_number <= 4000:
                        return True
                
                # For north-south streets (Grand, Gustine, etc.)
                else:
                    # Return true if the street itself is a boundary
                    if street in [TGH_BOUNDARIES["east"], TGH_BOUNDARIES["west"]]:
                        return True
            
            # If we can't parse the house number but it contains a TGH street,
            # return True as a fallback
            return True
    
    return False

def authenticate_google_sheets():
    """Authenticate with Google Sheets API using service account credentials."""
    try:
        # Path to your service account credentials JSON file
        credentials = Credentials.from_service_account_file(
        'credentials/stlpolicecallscraper-6d4083bc904a.json', scopes=SCOPES
       )
        client = gspread.authorize(credentials)
        logger.info("Successfully authenticated with Google Sheets API")
        return client
    except Exception as e:
        logger.error(f"Error authenticating with Google Sheets API: {e}")
        raise

def scrape_calls_for_service():
    """Scrape the calls for service data from the St. Louis police department website."""
    url = "https://slmpd.org/calls/"
    
    try:
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='report-table call-for-service')
        
        if not table:
            logger.error("Could not find the calls for service table on the page")
            return []
        
        # Find all table rows (excluding the header row)
        rows = table.find_all('tr')[1:]  # Skip the header row
        
        all_data = []
        tgh_data = []  # Tower Grove Heights specific data
        
        for row in rows:
            # Extract text from each cell in the row
            cells = row.find_all('td')
            if len(cells) >= 4:
                record = {
                    'Dispatch': cells[0].text.strip(),
                    'Event': cells[1].text.strip(),
                    'Address': cells[2].text.strip(),
                    'Call Type': cells[3].text.strip(),
                    'Scraped_Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'InTowerGroveHeights': 'Yes'  # We're only keeping TGH records
                }
                all_data.append(record)
                
                # Check if this call is in Tower Grove Heights
                if is_in_tower_grove_heights(record['Address']):
                    tgh_data.append(record)
        
        logger.info(f"Successfully scraped {len(all_data)} total records")
        logger.info(f"Filtered {len(tgh_data)} records in Tower Grove Heights")
        
        return tgh_data  # Return only Tower Grove Heights data
    
    except requests.RequestException as e:
        logger.error(f"Error fetching the webpage: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing the webpage: {e}")
        return []

def update_google_sheet(client, sheet_name, data):
    """Update the Google Sheet with new data, avoiding duplicates."""
    try:
        # Try to open the spreadsheet, create it if it doesn't exist
        try:
            spreadsheet = client.open(sheet_name)
            logger.info(f"Opened existing spreadsheet: {sheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = client.create(sheet_name)
            logger.info(f"Created new spreadsheet: {sheet_name}")
            
            # Try to share the spreadsheet with your email
            try:
                spreadsheet.share(YOUR_EMAIL, perm_type='user', role='writer')
                logger.info(f"Shared spreadsheet with {YOUR_EMAIL}")
            except Exception as e:
                logger.error(f"Could not share spreadsheet: {e}")
        
        # Print the spreadsheet URL prominently to terminal
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
        print("\n" + "="*50)
        print(f"SPREADSHEET URL: {spreadsheet_url}")
        print("Copy and paste this URL into your browser to access the data")
        print("="*50 + "\n")
        logger.info(f"Spreadsheet URL: {spreadsheet_url}")
        
        # Select the first worksheet
        worksheet = spreadsheet.sheet1
        
        # Get existing data to check for duplicates
        try:
            existing_records = worksheet.get_all_records()
        except:
            # Worksheet might be empty
            existing_records = []
        
        # Create a set of existing event IDs for faster lookup
        existing_event_ids = {record.get('Event', '') for record in existing_records}
        
        # Check if worksheet is empty and add headers if needed
        if not existing_records and data:
            headers = list(data[0].keys())
            worksheet.append_row(headers)
            logger.info("Added headers to empty worksheet")
        
        # Add new records, avoiding duplicates
        new_records_count = 0
        for record in data:
            if record['Event'] not in existing_event_ids:
                row_values = list(record.values())
                worksheet.append_row(row_values)
                existing_event_ids.add(record['Event'])  # Update our set of IDs
                new_records_count += 1
                # Small delay to prevent API rate limiting
                time.sleep(0.1)
        
        logger.info(f"Added {new_records_count} new records to the spreadsheet")
        return new_records_count
    
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}")
        return 0

def main():
    """Main function to scrape data and update the Google Sheet."""
    try:
        start_time = time.time()
        logger.info("Starting scraping process for Tower Grove Heights")
        
        # Authenticate with Google Sheets
        client = authenticate_google_sheets()
        
        # Scrape the calls for service data
        data = scrape_calls_for_service()
        
        if not data:
            logger.warning("No data was scraped for Tower Grove Heights. Exiting.")
            print("No data was found for Tower Grove Heights. Please try again later.")
            return
        
        # Update the Google Sheet with the new data
        sheet_name = "TowerGroveHeightsCalls"  # New sheet name for the filtered data
        new_records = update_google_sheet(client, sheet_name, data)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info(f"Scraping completed. Added {new_records} new Tower Grove Heights records in {execution_time:.2f} seconds")
        print(f"Successfully added {new_records} new records for Tower Grove Heights!")
        
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()