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
    filename='tgs_police_calls_scraper.log'
)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Your email address to share the spreadsheet with
YOUR_EMAIL = "adamlaserlab@gmail.com"

# Define Tower Grove South boundaries
TGS_BOUNDARIES = {
    "north": "ARSENAL",      # North boundary
    "south": "CHIPPEWA",     # South boundary
    "east": "GRAND",         # East boundary 
    "west": "MORGANFORD"     # West boundary
}

# Define streets within Tower Grove South
TGS_STREETS = [
    # North-South Streets (from west to east)
    "MORGANFORD", "OAK HILL", "ROGER", "LAWRENCE", "HEREFORD", "SPRING", 
    "TOWER GROVE", "LOUISIANA", "NEBRASKA", "PENNSYLVANIA", "CALIFORNIA", 
    "OREGON", "OHIO", "IOWA", "COMPTON", "GRAND",
    
    # East-West Streets (from north to south)
    "ARSENAL", "HARTFORD", "HARTFORD", "CONNECTICUT", "JUNIATA", "HUMPHREY", 
    "WYOMING", "REBER", "THOLOZAN", "MCDONALD", "PESTALOZZI", "MIAMI", 
    "ALBERTA", "OLEATHA", "FAIRVIEW", "CHIPPEWA"
]

def is_in_tower_grove_south(address):
    """Check if an address is in Tower Grove South neighborhood."""
    if not address:
        return False
        
    # Convert to uppercase for case-insensitive comparison
    address_upper = address.upper()
    
    # Check for Tower Grove South explicitly mentioned
    if "TOWER GROVE SOUTH" in address_upper:
        return True
    
    # Check if address contains any of the Tower Grove South streets
    for street in TGS_STREETS:
        if street in address_upper:
            # Extract the house number if available
            match = re.search(r'(\d+)\s+([A-Za-z\s]+)', address_upper)
            if match:
                # For east-west streets
                if street in ["ARSENAL", "HARTFORD", "CONNECTICUT", "JUNIATA", "HUMPHREY", 
                             "WYOMING", "REBER", "THOLOZAN", "MCDONALD", "PESTALOZZI", 
                             "MIAMI", "ALBERTA", "OLEATHA", "FAIRVIEW", "CHIPPEWA"]:
                    # Approximate block ranges for Tower Grove South
                    if 3200 <= int(match.group(1)) <= 4100:
                        return True
                
                # For north-south streets
                elif street in ["MORGANFORD", "OAK HILL", "ROGER", "LAWRENCE", "HEREFORD", 
                               "SPRING", "TOWER GROVE", "LOUISIANA", "NEBRASKA", "PENNSYLVANIA", 
                               "CALIFORNIA", "OREGON", "OHIO", "IOWA", "COMPTON", "GRAND"]:
                    # These streets run through TGS between Arsenal and Chippewa
                    return True
            
            # If we can't parse the house number but it contains a TGS street,
            # check if any boundary streets are mentioned
            if any(boundary in address_upper for boundary in TGS_BOUNDARIES.values()):
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
        tgs_data = []  # Tower Grove South specific data
        
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
                    'InTowerGroveSouth': 'No'  # Default
                }
                all_data.append(record)
                
                # Check if this call is in Tower Grove South
                if is_in_tower_grove_south(record['Address']):
                    record['InTowerGroveSouth'] = 'Yes'
                    tgs_data.append(record)
        
        logger.info(f"Successfully scraped {len(all_data)} total records")
        logger.info(f"Filtered {len(tgs_data)} records in Tower Grove South")
        
        return tgs_data  # Return only Tower Grove South data
    
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
            # Share the spreadsheet with your email
            spreadsheet.share(YOUR_EMAIL, perm_type='user', role='writer')
            logger.info(f"Created new spreadsheet: {sheet_name}")
        
        # Print the spreadsheet URL to terminal
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
        print(f"\nSpreadsheet URL: {spreadsheet_url}")
        print(f"Please open this URL in your browser to view the data.\n")
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
        logger.info("Starting scraping process")
        
        # Authenticate with Google Sheets
        client = authenticate_google_sheets()
        
        # Scrape the calls for service data
        data = scrape_calls_for_service()
        
        if not data:
            logger.warning("No data was scraped for Tower Grove South. Exiting.")
            return
        
        # Update the Google Sheet with the new data
        sheet_name = "TowerGroveSouthCalls"  # New sheet name for the filtered data
        new_records = update_google_sheet(client, sheet_name, data)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info(f"Scraping completed. Added {new_records} new Tower Grove South records in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")

if __name__ == "__main__":
    main()