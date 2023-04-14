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

    # check if the filename matches the datetime expression
    # https://note.nkmk.me/python-re-match-object-span-group/
    m = re.match(r'[0-9]{8}\-[0-9]{8}', str(csv_file))

    if m:
        # print(m.group())
        file = f"{m.group()}.csv"
        # read each csv file in the folder and store it into the temporary dataframe
        df_tmp = pd.read_csv(file, sep=',', engine = 'python', header = 0, on_bad_lines='skip')

        # Display the number of lines in the file
        print(f"{file}:{len(df_tmp)} lines")

        # Concatenate df_tmp to df
        df = pd.concat([df, df_tmp])

        # Display the number of lines in the concatenated DF so far
        print(f"total:{len(df)} lines")

        # Sort rows in the concatenated DF by "created_at"
        df = df.sort_values(['created_at'], axis = 0, ascending = True)

        # Clear the temporary DF
        df_tmp = pd.DataFrame()



        ## Find the newest date in the csv_file name

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
# print(filename_date)



tweets_filename = f"{len(df)}tweets_{filename_date}.csv"

# Export all the DF to CSV file
df.to_csv(tweets_filename, index = False, header = True)

# Extract only the CVE from the string matched "text"

# https://note.nkmk.me/python-re-match-object-span-group/
# Pythonの正規表現モジュールreのmatch()やsearch()は、文字列が正規表現パターンにマッチした場合、マッチした部分をマッチオブジェクトとして返す。
# マッチオブジェクトのgroupメソッドを実行することでマッチした文字列を抽出できる

df_matched = df[df["text"].str.contains(r'CVE-[0-9]{4}-[0-9]{4,5}')]
list_CVE = []
df_plusCVE = df_matched
for i, s in enumerate(df_matched["text"].to_list()):

    # find the matched text with the CVE format
    matched_text = re.search('CVE-[0-9]{4}-[0-9]{4,5}',s)
    # if i < 5:
    #     print(matched_text.group())
        # df_plusCVE = pd.concat([df_plusCVE, pd.DataFrame({"CVE": pd.Series([matched_text.group()])})], axis=1)

    # Add the CVE number to the list_CVE
    list_CVE.append(matched_text.group())

# Add a new column "CVE"
df_plusCVE['CVE'] = list_CVE

IncludingCVEs_filename = f"{len(df_plusCVE)}IncludingCVEs_{filename_date}.csv"

# Export all the DF to CSV file
df_plusCVE.to_csv(IncludingCVEs_filename, index = False, header = True)
