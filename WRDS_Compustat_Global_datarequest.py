"""
WRDS Access test script shows how to get data from Compustat Global tables
Version: 1.0
Date: 2023/06/05
Author: Mark Bruyneel
"""
# Imports
import wrds
from csv import reader
import pandas as pd
from loguru import logger

logger.add(r'U:\Werk\Data Management\Python\Files\wrds\wrds_data_test_{time}.log', backtrace=True, diagnose=True, compression='zip')
logger.debug('The script was running today.')
@logger.catch()

def main():
    # Show all data in screen
    pd.set_option("display.max.columns", None)

    # Connect to WRDS first prepare a .pgpass file and put it in the user folder on the hard drive
    # Important: starting in 2023 you first need to log into the WRDS platform using internet software
    # before running a script. This change has to do with implementation of Duo Security authentication.
    # Make sure to use your own username to connect to WRDS.
    conn = wrds.Connection(wrds_username=' . . . ')

    # Now import a prepared list of ISIN codes
    with open('U:\Werk\Data Management\Python\\Files\wrds\ISIN_testlist.txt', "r") as isinlist_i:
        get_isinlist = reader(isinlist_i)
        newlist_isins = list(get_isinlist)
        Newlist = newlist_isins[0].copy()
    newyear = input('From what year (to now) do you need data?\n')
    iY1 = int(newyear)

    # Function to convert regular list into a tuple = needed for data request
    def convert(list):
        return tuple(list)

    # Getting data items from two tables for specific companies on ISIN codes
    isinlist = convert(Newlist) # Make sure that the uploaded list is converted to a tuple list
    params = {'isins': isinlist, 'year': iY1} # Define params to contain the tuple list
    dataselect = conn.raw_sql(
        "SELECT a.gvkey, a.fyear, a.datadate, a.isin, a.conm, a.curcd, a.at, a.lt, b.sic, b.ipodate FROM comp_global.g_funda a JOIN comp_global.g_company b ON a.gvkey = b.gvkey WHERE a.isin IN %(isins)s and a.fyear >= %(year)s ",
        params=params,
    )
    dataselect.to_csv(f'U:\Werk\Data Management\Python\\Files\wrds\Compustat_datatest.csv', encoding='utf-8')

    conn.close()

if __name__ == "__main__":
    main()
