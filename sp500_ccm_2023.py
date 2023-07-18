#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sp500ccm.py

Purpose:
    Extract SP500 Constituents data, following
        https://www.fredasongdrechsler.com/intro-to-python-for-fnce/sp500-constituents
    almost exactly

##########################################
# S&P 500 Index Constituents             #
# Qingyi (Freda) Song Drechsler          #
# Date: October 2020                     #
##########################################

Version: 1 (First start)
Date: 2021/1/28
Author: Charles Bos

Version: 2
Date: 2023/7/17
Author: Mark Bruyneel

"""
# Imports
import pandas as pd

# to be able to import wrds using Anaconda: type "pip install wrds" at the Anaconda prompt.
import wrds
# To catch errors, use the logger option from loguru
from loguru import logger
# Use this to log date and time
from datetime import datetime

# Main
# Create date variable for filenames
runday = str(datetime.today().date())
cyear = datetime.now().year

logger.add(r'U:\Werk\Data Management\Python\Files\output\WRDS_sp500_script.log', backtrace=True, diagnose=True, retention='10 days')
@logger.catch()

def main():
    # Start year and month
    Year = ""
    while True:
        try:
            Year = input('What year do you want to start with (4 digits) ?\n')
            tYear = int(Year)
            # S&P 500 exists since 1957. Before this date it was a different index: SP 90.
            # See also: https://en.wikipedia.org/wiki/S%26P_500
            if len(Year) == 4 and tYear<=cyear and tYear>=1957:
                iY1 = int(Year) # iY1= Year to start with data
                break
            else:
                cuyear = str(cyear)
                print("Please enter a four digit year between 1957 and " + cuyear + '.')
        except:
            continue

    Month = ""
    while True:
        try:
            Month = input('What month do you want to start with (2 digits)?\n')
            tMonth = int(Month)
            if len(Month) == 2 and tMonth>=1 and tMonth<=12:
                iM1 = int(Month)  # iM1= It will use the end day of the month
                break
            else:
                print("Please enter a two digit month in the range 01 to 12.")
        except:
            continue

    # URL location for output
    sBase = 'U:\Werk\Data Management\Python\Files\output\sp500ccm'

    # Initialisation
    ###################
    # Connect to WRDS #
    ###################
    conn = wrds.Connection(wrds_username=' . . . ')

    # Extract SP500 starting at date D1
    sp500 = conn.raw_sql("""
                                select a.*, b.date, b.ret
                                from crsp.msp500list as a,
                                crsp.msf as b
                                where a.permno=b.permno
                                and b.date >= a.start and b.date<= a.ending
                                and b.date>='%i/01/%i'
                                order by date;
                                """ % (iM1, iY1), date_cols=['start', 'ending', 'date'])

    # Add Other Descriptive Variables
    mse = conn.raw_sql("""
                                select comnam, ncusip, namedt, nameendt,
                                permno, shrcd, exchcd, hsiccd, ticker
                                from crsp.msenames
                                """, date_cols=['namedt', 'nameendt'])

    # if nameendt is missing then set to today date
    mse['nameendt'] = mse['nameendt'].fillna(pd.to_datetime('today'))

    # Merge with SP500 data
    sp500_full = pd.merge(sp500, mse, how='left', on='permno')

    # Impose the date range restrictions
    sp500_full = sp500_full.loc[(sp500_full.date >= sp500_full.namedt)
                                & (sp500_full.date <= sp500_full.nameendt)]

    # Linking with Compustat through CCM
    ccm = conn.raw_sql("""
                          select gvkey, liid as iid, lpermno as permno, linktype, linkprim,
                          linkdt, linkenddt
                          from crsp.ccmxpf_linktable
                          where substr(linktype,1,1)='L'
                          and (linkprim ='C' or linkprim='P')
                          """, date_cols=['linkdt', 'linkenddt'])

    # if linkenddt is missing then set to today date
    ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

    # Merge the CCM data with S&P500 data
    # First just link by matching PERMNO
    sp500ccm = pd.merge(sp500_full, ccm, how='left', on=['permno'])

    # Then set link date bounds
    sp500ccm = sp500ccm.loc[(sp500ccm['date'] >= sp500ccm['linkdt']) & (sp500ccm['date'] <= sp500ccm['linkenddt'])]

    # Rearrange columns for final output

    sp500ccm = sp500ccm.drop(columns=['namedt', 'nameendt', \
                                      'linktype', 'linkprim', 'linkdt', 'linkenddt'])
    sp500ccm = sp500ccm[['date', 'permno', 'comnam', 'ncusip', 'shrcd', 'exchcd', 'hsiccd', 'ticker', \
                         'gvkey', 'iid', 'start', 'ending', 'ret']]

    # Output
    # Store
    sOut = '%s_y%i_m%i.csv.gz' % (sBase, iY1, iM1)
    sp500ccm.to_csv(sOut)

    print('First lines of data:')
    print(sp500ccm.head())
    print('Last lines of data:')
    print(sp500ccm.tail())
    logger.debug('The script was running today: ' + runday)
    print('Writing a %ix%i matrix of data to %s' % (sp500ccm.shape[0], sp500ccm.shape[1], sOut))


############################################################## start main
if __name__ == "__main__":
    main()
