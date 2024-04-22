import requests
import pyodbc
from datetime import datetime, timedelta, timezone
import json
import pytz

# Define the timezone for Mountain Time and UTC
utc_zone = pytz.utc
local_zone = pytz.timezone('America/Denver')

# Function to convert ISO 8601 string to local time


def iso8601_to_local(iso8601_str, target_timezone):
    
    dt_utc = datetime.strptime(iso8601_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
    target_timezone = pytz.timezone(timezone_str)
    dt_local = dt_utc.astimezone(target_timezone)
    return dt_local


def get_start_end_dates(timezone_str):
    # Define the timezone
    tz = pytz.timezone(timezone_str)
    
    # Calculate 'yesterday' in the given timezone
    yesterday = datetime.now(tz) - timedelta(days=1)
    
    # Start date at 12:00 AM
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # End date at 11:59 PM
    end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Format the timezone offset correctly
    start_date_formatted_corrected = start_date_formatted[:-2] + ':' + start_date_formatted[-2:]
    end_date_formatted_corrected = end_date_formatted[:-2] + ':' + end_date_formatted[-2:]
    
    return start_date_formatted_corrected, end_date_formatted_corrected



def iso8601_to_unix_ms(iso8601_str):
    dt = datetime.strptime(iso8601_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    unix_ms = int(dt.timestamp() * 1000)
    return unix_ms


# Database connection - replace with your actual credentials
server = 'Server'
database = 'database'
username = 'username'
password = 'pw'
cnxn = pyodbc.connect('DRIVER={driver};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# SQL query to select driver IDs
query = "SELECT columns FROM schema.table"
cursor.execute(query)

# Fetch all results
results = cursor.fetchall()

# Create a list of dictionaries for easy access to each driver's details
# Assuming `results` from earlier fetch; adjust as needed
timezones = {row[0]: row[1] for row in results}


base_url = 'url'
for row in results:
    driver_id, timezone_str, region, username = row
    start_date_formatted, end_date_formatted = get_start_end_dates(timezone_str)
   
    url = f'{base_url}?driverIds={driver_id}&startTime={start_date_formatted}&endTime={end_date_formatted}'
    #url = f'{base_url}?driverIds={driver_id}&startTime=2024-02-20T00:00:00-07:00&endTime=2024-02-20T23:59:59-07:00'
    headers = {
  'Accept': 'application/json',
  'Authorization': 'auth'
}

    print(url)
    # Example of making a GET request - you might need to include authentication headers or other parameters
    response = requests.get(url, headers = headers)

    if response.status_code == 200:
        data = response.json()
        print(data)
        if "data" in data and len(data["data"]) > 0:
            for item in data["data"]:
                driver_id = item["driver"]["id"]
                for log in item["hosLogs"]:
                    codrivers_json = json.dumps(log["codrivers"])  # Convert to JSON string
                    hosStatusType = log["hosStatusType"]
                    logEndTime = log["logEndTime"]
                    logStartTime = log["logStartTime"]
                    time_ms_logEndTime = iso8601_to_unix_ms(logEndTime)
                    time_ms_logStartTime = iso8601_to_unix_ms(logStartTime)
                    logEndTimeLocal = iso8601_to_local(log["logEndTime"], timezone_str)
                    logStartTimeLocal = iso8601_to_local(log["logStartTime"], timezone_str)
                    # Check if the record already exists
                    check_query = """
                    SELECT COUNT(*) FROM table 
                    WHERE driverId = ? AND logEndTimeUTC = ? AND logStartTimeUTC = ?
                    """
                    cursor.execute(check_query, (driver_id, logEndTime, logStartTime))
                    if cursor.fetchone()[0] == 0:
                        # Record does not exist, proceed to insert
                        try:
                            insert_query = """
                            INSERT INTO table (columns) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            cursor.execute(insert_query, (columns))
                            cnxn.commit()
                        except Exception as e:
                            print(f"Failed to insert data for driver ID {driver_id}: {e}")
                    else:
                        # Record exists, skip insertion
                        print(f"Record already exists for driver ID {driver_id} with logEndTime {logEndTime} and logStartTime {logStartTime}, skipping.")
        else:
            print("No driver data found in the response.")
    else:
        print(f"Failed to retrieve data: {response.status_code}")

cnxn.commit()
cursor.close()
cnxn.close()


