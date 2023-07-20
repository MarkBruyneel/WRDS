"""
WRDS Access test script shows how to get data from Compustat Global tables
Version: 1.1
Date: 2023/07/20
Author: Mark Bruyneel
"""
# Imports
import wrds
from csv import reader
import pandas as pd
from datetime import datetime
from loguru import logger

time = str(datetime.today())
cyear = datetime.now().year

logger.add(r'U:\Werk\Data Management\Python\Files\wrds\wrds_data_test.log', backtrace=True, diagnose=True, compression='zip')
logger.debug('The script was running with no errors on: ' + time)
@logger.catch()

def main():
    # Show all data in screen
    pd.set_option("display.max.columns", None)

    # Connect to WRDS first prepare a .pgpass file and put it in the user folder on the hard drive
    # Important: starting in 2023 you first need to log into the WRDS platform using internet software
    # before running a script. This change has to do with implementation of Duo Security authentication.
    # Make sure to use your own username to connect to WRDS.
    conn = wrds.Connection(wrds_username='. . .')

    # Now import a prepared list of ISIN codes
    print('The program will work with the list of ISIN codes at this location:\nU:\Werk\Data Management\Python\\Files\wrds\ISIN_testlist.txt\n')
    with open('U:\Werk\Data Management\Python\\Files\wrds\ISIN_testlist.txt', "r") as isinlist_i:
        get_isinlist = reader(isinlist_i)
        newlist_isins = list(get_isinlist)
        Newlist = newlist_isins[0].copy()

    # This part checks the input for a year that is correct and relevant
    # Compustat Global only has data starting 1987.
    Year = ""
    while True:
        try:
            Year = input('What year do you want to start with (4 digits) ?\n')
            tYear = int(Year)
            if len(Year) == 4 and tYear<=cyear and tYear>=1987:
                iY1 = int(Year) # iY1= Year to start with data
                break
            else:
                cuyear = str(cyear)
                print("Please enter a four digit year between 1987 and " + cuyear + '.')
        except:
            continue
    # Function to convert regular list into a tuple = needed for data request
    def convert(list):
        return tuple(list)

    # Getting data items from two tables for specific companies on ISIN codes
    isinlist = convert(Newlist) # Make sure that the uploaded list is converted to a tuple list
    params = {'isins': isinlist, 'year': iY1} # Define params to contain the tuple list
    dataselect = conn.raw_sql(
        "SELECT a.gvkey, a.fyear, a.datadate, a.isin, a.conm, a.curcd, a.at, a.lt, b.sic, b.ipodate FROM comp_global_daily.g_funda a JOIN comp_global_daily.g_company b ON a.gvkey = b.gvkey WHERE a.isin IN %(isins)s and a.fyear >= %(year)s ",
        params=params,
    )
    dataselect.to_csv(f'U:\Werk\Data Management\Python\\Files\wrds\Compustat_datatest.csv', encoding='utf-8')

    conn.close()

if __name__ == "__main__":
    main()
