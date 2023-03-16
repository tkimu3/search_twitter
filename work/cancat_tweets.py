import pandas as pd
from pathlib import Path

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

# Export all the DF to "all.csv"
df.to_csv('all.csv', index = False, header = True)