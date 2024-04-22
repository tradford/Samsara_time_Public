import requests
import pyodbc
from datetime import datetime, timedelta

# Calculate today's date
today = datetime.now()

# Calculate 3 days ago from today
three_days_ago = today - timedelta(days=3)

# Format the dates in 'YYYY-MM-DD' format for the API endpoint
start_date = three_days_ago.strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d')


def get_friendly_timezone_name(timezone_str):
    
    # Map of timezone identifiers to friendly names
    timezone_map = {
        "America/Denver": "Mountain",
        "America/Los_Angeles": "Pacific",
        "America/Chicago": "Central",
        "America/New_York": "Eastern"
        
        # Add more mappings as needed
    }
    
    # Return the friendly name if available, else return the original string
    return timezone_map.get(timezone_str, timezone_str)
# Construct the API endpoint URL dynamically
url = f"url"

#print(api_endpoint)

headers = {
    'Accept': 'application/json',
    'Authorization': 'Authorization'
}

# Make the API request
response = requests.get(url, headers=headers)

# Database connection - replace with your actual credentials
server = 'server'
database = 'database'
username = 'username'
password = 'pw'
cnxn = pyodbc.connect('DRIVER={Driver};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# Check if the request was successful
# Assuming you have a response from the request
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Extracting and potentially inserting driver data into the database
    for item in data["data"]:
        driver_name = item["name"]
        driver_timezone = item["timezone"]
        region = get_friendly_timezone_name(driver_timezone)
        driver_id = item["id"]
        driver_user = item["username"]
        
        # Check if the driver_id already exists
        check_query = "SELECT COUNT(*) FROM schema.table WHERE userId = ?"
        cursor.execute(check_query, driver_id)
        result = cursor.fetchone()
        
        # If the driver_id does not exist, result[0] will be 0
        if result[0] == 0:
            # Driver_id does not exist, proceed to insert
            insert_query = "INSERT INTO schema.table (columns) VALUES (?, ?, ?, ?, ?)"
            cursor.execute(columns)
        else:
            # Driver_id exists, skip inserting
            print(f"Driver ID {driver_id} already exists, skipping insertion.")
    
    # Commit changes to the database
    cnxn.commit()

    # Close cursor and connection
    cursor.close()
    cnxn.close()

    print("Data processed successfully.")
else:
    print(f"Failed to retrieve data: {response.status_code}")

