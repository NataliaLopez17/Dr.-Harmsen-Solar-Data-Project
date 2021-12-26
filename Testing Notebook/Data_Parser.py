''' 
@author: Elias Chevere
'''
import sys
import datetime
import glob
import os
import pandas as pd


data_dir = "D:\\Users\\viaje\\OneDrive - University of Puerto Rico\\SAGA5017"


def make_dir(Directory_names):
    """
    Will take in a list of the directory names to create and then will proceed to generate the directories
     _____________________________________

    Parameters: 

    Directory_names:  This is a list that must contain all of the names that will be used to create the desired directories within the Data directory.
    """

    for name in Directory_names:
        if not os.path.isdir(data_dir+name):
            os.makedirs(data_dir+name)
        else:
            continue


# This is used by the parsers of data in order to generate the appropriate index.


def custom_date_parser(x, y, z, a, b):
    """
    [summary]

    Args:
        x ([type]): [description]
        y ([type]): [description]
        z ([type]): [description]
        a ([type]): [description]
        b ([type]): [description]

    Returns:
        [type]: [description]
    """
    return datetime.datetime.strptime(
        f"{x} {y} {z} {a} {b}", "%Y %m %d %H %M")


def parse_data(Year):
    '''
    Will take in the information for the year and file location to return the appropriate dataframe that will be used to then generate the necessary files.

    _____________

    Parameters: 

    Year: The year of the data that is to be parsed.

    path: The directory of where the data is located in.

    _____________

    Return: 

    Parsed_Data: Dataframe generated containing all data within the given directory.
    '''
    # Create the DataFrame that will be utilized to store all of the parsed data.
    BigDF = pd.DataFrame()
    # Store all of the files as a list, in order to access it in the loop appropriately.
    files = glob.glob(f"../Solar Data/*/{Year}.csv", recursive=True)
    for index in range(0, len(files)):
        if len(files) > 2:
            DataFile = files.pop()
            DataFile2 = files.pop()
            Csv_DF = pd.read_csv(DataFile, header=[2], parse_dates={
                                 'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)
            Lat_Lon_DF1 = pd.read_csv(DataFile, nrows=1)
            Csv_DF["Longitude"] = Lat_Lon_DF1.Longitude[0]
            Csv_DF["Latitude"] = Lat_Lon_DF1.Latitude[0]
            Csv_DF2 = pd.read_csv(DataFile2, header=[2], parse_dates={
                                  'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)
            Lat_Lon_DF2 = pd.read_csv(DataFile2, nrows=1)
            Csv_DF2["Longitude"] = Lat_Lon_DF2.Longitude[0]
            Csv_DF2["Latitude"] = Lat_Lon_DF2.Latitude[0]
            BigDF = BigDF.append([Csv_DF, Csv_DF2])
            print("1", BigDF)
        elif len(files) == 1:
            DataFile = files.pop()
            Csv_DF = pd.read_csv(DataFile, header=[2], parse_dates={
                                 'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)
            Lat_Lon_DF1 = pd.read_csv(DataFile, nrows=1)
            Csv_DF["Longitude"] = Lat_Lon_DF1.Longitude[0]
            Csv_DF["Latitude"] = Lat_Lon_DF1.Latitude[0]
            BigDF = BigDF.append([Csv_DF])
            print("2", BigDF)
        else:
            Parsed_Data = BigDF.groupby(
                [pd.Grouper(key="Date", freq='1D'), "Latitude", "Longitude"]).mean()
            Parsed_Data = Parsed_Data.round(2)
    return Parsed_Data


def File_Generator(Parsed_Dataframe, Values_To_Parse=["GHI", "DNI", "Wind Speed", "Temperature", "Relative Humidity"]):
    '''
    This function generates a series of Space delimited CSV Files from the Dataframe given, as long as it follows the expected structure.

    _____________

    Parameters: 

    Parsed_Dataframe: Must be the dataframe generated from the parse_data function. It must contain all latitudes, longitudes, dates, and values desired.


    Values_To_Parse: This must be a list containing all of the values that will be contained within the dataframe in order to generate the desired CSV files.

    _____________
    '''
    for Value in Values_To_Parse:
        for rowIndex, row in Parsed_Dataframe.iterrows():
            print("1",rowIndex)
            print("2",row)
            Data = pd.DataFrame(data={'value': [row[Value]], 'Latitude': [
                                rowIndex[1]], 'Longitude': [rowIndex[2]]})
            print("3",Data)
            Data.to_csv(
                path_or_buf=f"{data_dir}{rowIndex[0].strftime('%Y')}{Value}/{Value}{rowIndex[0].strftime('%Y%j')}.csv", header=False, index=False, sep=' ')


if __name__ == "_main_":
    # Identify the current year desired.
    year = int(sys.argv[1])
    if(year <= 2017 and year >= 1998):
        Parsed_Data = parse_data(year)
        make_dir(str(year), ["GHI", "DNI", "Wind Speed", "Air Temperature"])
        File_Generator(Parsed_Data, Values_To_Parse=[
                       "GHI", "DNI", "Wind Speed", "Air Temperature"])
    elif year >= 2018:
        Parsed_Data = parse_data(year)
        make_dir(str(year), ["GHI", "DNI", "Wind Speed",
                 "Temperature", "Relative Humidity"])
        File_Generator(Parsed_Dataframe=Parsed_Data)
    else:
        print("Enter a valid year after the script name.")
