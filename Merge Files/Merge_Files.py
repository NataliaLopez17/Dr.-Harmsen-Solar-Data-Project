import pandas as pd
import glob
import os


def merge_files():
    # Get all files in the folder
    files = glob.glob('*.csv')
    # Create a dataframe from the files
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    # Save the dataframe to a csv
    df.to_csv('merged_files.csv', index=False)
    # Delete the files
    for f in files:
        os.remove(f)
