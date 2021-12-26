import pandas as pd
import datetime
import glob
from matplotlib import pyplot as plt
import matplotlib.dates as matdates
import matplotlib.ticker as mticker
import numpy as np
plt.rcParams['agg.path.chunksize'] = 40000
plt.rcParams['figure.figsize'] = [40, 30]




def custom_date_parser(x, y, z, a, b): return datetime.datetime.strptime(
    f"{x} {y} {z} {a} {b}", "%Y %m %d %H %M")


display_file = pd.read_csv("../Testing Notebook/Solar Data/2018/2c56c0432ecc5f6fde2c429a58d35601/2824758_18.52_-67.29_2018.csv",
                           header=[2], parse_dates={'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)
display_file2 = pd.read_csv(
    "../Testing Notebook/Solar Data/2018/2c56c0432ecc5f6fde2c429a58d35601/2824758_18.52_-67.29_2018.csv", nrows=1)


display_file3 = pd.read_csv("../Testing Notebook/Solar Data/2019/e0ef61efe7cf6042bae90234975fed0b/2829149_17.90_-65.55_2019.csv",
                            header=[2], parse_dates={'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)
display_file4 = pd.read_csv(
    "../Testing Notebook/Solar Data/2019/e0ef61efe7cf6042bae90234975fed0b/2829149_17.90_-65.55_2019.csv", nrows=1)

display_file["Latitude"] = display_file2.Latitude[0]
display_file["Longitude"] = display_file2.Longitude[0]

display_file3["Latitude"] = display_file4.Latitude[0]
display_file3["Longitude"] = display_file4.Longitude[0]

test_df = pd.concat([display_file3, display_file4])
test_group_df = test_df.groupby(
    [pd.Grouper(key="Date", freq='1D'), "Latitude", "Longitude"]).mean()
# desired_output = pd.read_csv("../Solar Data/INSOLRICO.2021302.csv", delim_whitespace=True,names=["value","latitude","longitude"])


year = "2019"


def data_plots(year):
    for DataFile in glob.glob(
            f"../Testing Notebook/Solar Data/**/*{year}.csv", recursive=True):

        Lat_Lon_DF1 = pd.read_csv(
            DataFile, nrows=1, header=0, delim_whitespace=True)
        CSV_DF = pd.read_csv(DataFile, header=[2], parse_dates={
            'Date': [0, 1, 2, 3, 4]}, date_parser=custom_date_parser)

        a = CSV_DF.groupby(pd.Grouper(key='Date', freq='15min')).mean()
        b = a.filter(like="2019-01-01", axis=0)
        b.reset_index(inplace=True)

        '''
        TODO:
        -for loop to plot all 4 parameters
        -for loop to graph all 4 parameters for each day(?)
        '''
        date_col = b['Date']
        ghi_col = b['GHI']
        dni_col = b['DNI']
        temp_col = b['Temperature']
        hmd_col = b['Relative Humidity']

        labels_x = b['Date'] = pd.to_datetime(b['Date']).dt.strftime('%H:%M')

        x = np.arange(len(labels_x))
        y = np.arange(len(ghi_col))
        y = np.array(ghi_col).astype(np.float)

        ax = b.plot(x="Date", y="GHI", scalex=True, scaley=True, marker='o')

        plt.xticks(np.arange(min(x), max(x)+1, 1))
        ax.set_xticklabels(labels_x, rotation=50, fontsize=10)

        plt.yticks(np.arange(min(y), max(y)+1, 50))

        ax.yaxis.get_ticklocs(minor=True)
        # Initialize minor ticks
        ax.minorticks_on()
        # Now minor ticks exist and are turned on for both axes
        # Turn off x-axis minor ticks
        ax.xaxis.set_tick_params(which='minor', bottom=False)

        ax.tick_params('both', length=10, width=2, which='major')
        ax.tick_params(axis='y', length=5, width=2, which='minor')

        plt.grid(which='major', axis='both', color='0.95')

        plt.title('Daily GHI at 15min Intervals', fontweight='bold',
                  fontsize='15', horizontalalignment='center')
        plt.xlabel('Time (15min)', fontweight='bold',
                   fontsize='15', horizontalalignment='center')
        plt.ylabel('GHI (w^2/m)', fontweight='bold',
                   fontsize='12', verticalalignment='center')

        plt.show()
        # plt.savefig('D:\\Users\\viaje\\OneDrive - University of Puerto Rico\\SAGA5017\\Work\\Solar_Data_Work\\2019_data\\e0ef61efe7cf6042bae90234975fed0b_ghi_plots')
        

if __name__ == "__main__":
    # Identify the current year desired.
    year = int(sys.argv[1])
    if year <= 2017 and year >= 1998:
        Parsed_Data = parse_data(year)
        make_dir(["GHI", "DNI", "Wind Speed", "Air Temperature"])
        File_Generator(Parsed_Data, Values_To_Parse=[
                       "GHI", "DNI", "Wind Speed", "Air Temperature"])
    elif year >= 2018:
        Parsed_Data = parse_data(year)
        make_dir(["GHI", "DNI", "Wind Speed",
                 "Temperature", "Relative Humidity"])
        File_Generator(Parsed_Dataframe=Parsed_Data)
    else:
        print("Enter a valid year after the script name.")
