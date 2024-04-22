import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from logMessage import logMessage as lm

lm(r"path", 'imported all packages')
yesterday1 = datetime.now() - timedelta(days=1)             
yesterday = yesterday1.strftime('%d-%b-%y')
print(yesterday)



# Database connection parameters
server = 'server'
database = 'database'
username = 'Automation'
password = 'pw'
cnxn_str = f'DRIVER={{Driver}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Create a connection
cnxn = pyodbc.connect(cnxn_str)


query_load = 'SELECT columns FROM schema.tables'

df_user = pd.read_sql(query_load, cnxn)
#print(df_user)

# Initialize an empty DataFrame for all results
all_results = pd.DataFrame()

lm(r"path", 'Connected to database for drivers')

for index, row in df_user.iterrows():
    driver_id = row['userId']
    driver_name = row['name']  # Assuming the name column exists in df_user
    user_name = row['username']
    region = row['region']
    query = f"SELECT * FROM schema.table WHERE driverId = {driver_id} AND CONVERT(date, logStartTimeLocal) = CONVERT(date, DATEADD(day, -1, GETDATE())) ORDER BY logStartTimeMs"

    # Load the data into a pandas DataFrame
    df = pd.read_sql(query, cnxn)
    lm(r"file", f'Connected to database clocks_string for driver {driver_id}')
    df['driverName'] = driver_name 
    df['username'] = user_name
    df['Region'] = region
    #print(df)
    #print(df.columns)





    # Replace 'driving' with 'onDuty' in the 'HosStatusType' column
    df['HosStatusType'] = df['HosStatusType'].replace('driving', 'onDuty')

    # Add previous and next hosStatusType for each row to help identify transitions
    df['prev_hosStatusType'] = df['HosStatusType'].shift(1)
    df['next_hosStatusType'] = df['HosStatusType'].shift(-1)

    # Initialize an empty DataFrame for results
    result = pd.DataFrame()

    # Identify the first 'onDuty'
    first_on_duty = df[df['HosStatusType'] == 'onDuty'].head(1)

    # Append the first 'onDuty' to the result
    result = pd.concat([result, first_on_duty])
    

    # Find the last 'offDuty' before an 'onDuty' after the first 'onDuty'
    if not first_on_duty.empty:
        first_on_duty_index = first_on_duty.index[0]
        # Looking for the first 'offDuty' sequence after the first 'onDuty'
        offDuty_after_onDuty = df[(df['HosStatusType'] == 'offDuty') & (df.index > first_on_duty_index)]
        
        # This loops through the 'offDuty' rows to find the last 'offDuty' before it changes back to 'onDuty'
        last_offDuty_before_onDuty = pd.DataFrame()
        for i in offDuty_after_onDuty.index:
            if i + 1 in df.index and df.at[i + 1, 'HosStatusType'] == 'onDuty':
                last_offDuty_before_onDuty = offDuty_after_onDuty.loc[[i]]
                break

        # Append this 'offDuty' to the result
        result = pd.concat([result, last_offDuty_before_onDuty])

    # Identify the last 'onDuty'
    last_on_duty = df[df['HosStatusType'] == 'onDuty'].tail(1)

    # Find the first 'offDuty' after the last 'onDuty'
    if not last_on_duty.empty:
        last_on_duty_index = last_on_duty.index[0]
        subsequent_off_duty_last = df[(df['HosStatusType'] == 'offDuty') & (df.index > last_on_duty_index)].head(1)
        # Append this 'offDuty' to the result
        result = pd.concat([result, subsequent_off_duty_last])

    # Ensure the index is reset if needed
    result = result.reset_index(drop=True)
    all_results = pd.concat([all_results, result]) 
    #all_results = pd.concat([all_results, df], ignore_index=True)
    # Display the final result
    #print(result)


 # Close the connection
cnxn.close()
lm(r"log", 'closed connection')
print(all_results)




# Initialize an empty list to collect the DataFrames
data_frames_to_concatenate = []

# Group the original DataFrame by 'DriverId'
grouped = all_results.groupby('DriverId')

for driverId, group in grouped:
    # Sort the group by 'LogStartTimeLocal' to ensure chronological order
    group = group.sort_values(by='LogStartTimeLocal').reset_index(drop=True)
    
    temp_list = []  # Temporary list to hold dictionaries before converting to DataFrame

    # Always add the first row's LogStartTimeLocal as 'IN'
    temp_list.append({
                      'Username': group.loc[0, 'username'], 
                      'Time Zone': group.loc[0, 'Region'], 
                       
                      'Local Time': group.loc[0, 'LogStartTimeLocal'], 
                      
                      'Type': 'IN'})

    # Process rows if there's more than one
    if len(group) > 1:
        for i in range(1, len(group)-1):  # Adjusted to exclude the last row in this loop
            # The LogStartTimeLocal of subsequent rows as 'OUT'
            temp_list.append({ 
                              'Username': group.loc[i, 'username'],  
                              'Time Zone': group.loc[i, 'Region'], 
                              
                              'Local Time': group.loc[i, 'LogStartTimeLocal'], 
                              'Type': 'OUT'})
            
            # Also add LogEndTimeLocal as 'IN' except for the last row
            temp_list.append({
                               'Username': group.loc[i, 'username'], 
                               'Time Zone': group.loc[i, 'Region'], 
                               
                              'Local Time': group.loc[i, 'LogEndTimeLocal'], 
                              'Type': 'IN'})

        # Now handle the last row separately to avoid duplicating the final 'OUT' time
        # Add LogEndTimeLocal of the second-to-last row as 'IN' if not already added
        if len(group) > 2:  # Check ensures this is done only if there are at least 3 rows
            temp_list.append({
                              'Username': group.loc[len(group)-2, 'username'], 
                              'Time Zone': group.loc[len(group)-2, 'Region'], 
                              
                              'Local Time': group.loc[len(group)-2, 'LogEndTimeLocal'], 
                              'Type': 'IN'})

        # Add LogStartTimeLocal of the last row as 'OUT'
        temp_list.append({ 
                          'Username': group.loc[len(group)-1, 'username'], 
                          'Time Zone': group.loc[len(group)-1, 'Region'], 
                           
                          'Local Time': group.loc[len(group)-1, 'LogStartTimeLocal'], 
                          'Type': 'OUT'})
    else:
        # If there's only one row, its LogStartTimeLocal is marked as 'OUT'
        temp_list.append({
                          'Username': group.loc[0, 'username'], 
                          'Time Zone': group.loc[0, 'Region'], 
                          
                          'Local Time': group.loc[0, 'LogStartTimeLocal'], 
                          'Type': 'OUT'})

    # Convert temp_list to DataFrame and append to list of DataFrames
    temp_df = pd.DataFrame(temp_list)
    data_frames_to_concatenate.append(temp_df)
    lm(r"log", f'filtered punches for driver {driverId}')
    


# Concatenate all DataFrames in the list into final_df
final_df = pd.concat(data_frames_to_concatenate, ignore_index=True)


# Ensure the final DataFrame is sorted by driverId and then by Local time for readability
final_df = final_df.sort_values(by=['Username', 'Local Time']).reset_index(drop=True)

# Insert 'Company Name' column at the beginning with all values set to '6166662'
final_df.insert(0, 'Company Name', '6166662')


# First, replace 'Z' with '+00:00' to indicate UTC in a format pandas can parse with %z
final_df['Local Time'] = final_df['Local Time'].str.replace('Z', '')
final_df['Local Time'] = final_df['Local Time'].str.replace('T', ' ')

final_df = final_df.drop_duplicates()
final_df['Local Time'] = final_df['Local Time'].str[:19]

# Calculate 'yesterday' date string for the filename
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Define the path to save the Excel file

excel_file_path = r'dir'  # Update this path
excel_file_path_date = rf'dir_{yesterday}.csv'  # Update this path
 # Update this path
lm(r"path", 'about to send data to fileshare')
# Save the DataFrame to an Excel file
final_df.to_csv(excel_file_path, index=False)
final_df.to_csv(excel_file_path_date, index=False)

lm(r"path", f'Data saved to {excel_file_path}')
print(f'Data saved to {excel_file_path}')
print(f'Data saved to {excel_file_path_date}')

