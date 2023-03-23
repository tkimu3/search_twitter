import pandas as pd
from pathlib import Path
from datetime import datetime
import re

# get current path
path = Path('.')

# Define file type to read
file_name = '*.csv'

# csv file paths to read
csv_filepaths = path.glob(file_name)

# confirm "generator" class generated
#print(type(csv_filepaths))

# create a new DataFrame
df = pd.DataFrame()

# create a new DataFrame for storing dataframe temporarily
df_tmp = pd.DataFrame()

# Define end_date to store the newest date in the filename
end_date = None

for csv_file in csv_filepaths:
    # read each csv file in the folder and store it into the temporary dataframe
    df_tmp = pd.read_csv(csv_file, sep=',', engine = 'python', header = 0, on_bad_lines='skip')

    # Display the number of lines in the file
    print(f"{csv_file}:{len(df_tmp)} lines")

    # Concatenate df_tmp to df
    df = pd.concat([df, df_tmp])

    # Display the number of lines in the concatenated DF so far
    print(f"total:{len(df)} lines")

    # Sort rows in the concatenated DF by "created_at"
    df = df.sort_values(['created_at'], axis = 0, ascending = True)

    # Clear the temporary DF
    df_tmp = pd.DataFrame()

    # Find the newest date in the csv_file name

    # check if the filename matches the datetime expression
    # https://note.nkmk.me/python-re-match-object-span-group/
    m = re.match(r'[0-9]{8}\-[0-9]{8}', str(csv_file))

    if m:
        # print(m.group())

        # Pickup the latter date (right after the "-") in the filename
        # https://python-academia.com/file-extract/
        m_2nd = m.group().split('-')[1]
        # print(m_2nd)

        # extract the datetime format from the string
        #https://teratail.com/questions/266301
        end_date_candidate = datetime.strptime(m_2nd, '%Y%m%d').date()
        if end_date:

            # Update the end_date if the candidate was newer than the former one
            if end_date < end_date_candidate:
                end_date = end_date_candidate

        else:
            # Set the end_date if it hasn't been set yet
            end_date = end_date_candidate

        #print(end_date)

# set the date to put in the filename
filename_date = datetime.strftime(end_date, '%Y%m%d')
print(filename_date)

filename = "all_" + filename_date + ".csv"

# Export all the DF to "all.csv"
df.to_csv(filename, index = False, header = True)