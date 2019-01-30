#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 13:21:38 2018

@author: ericgrant
"""

import pandas as pd

from omnisci_utils import get_credentials
from omnisci_utils import wake_and_connect_to_mapd
from omnisci_utils import disconnect_mapd

from parsing_utils import format_date_cols
from parsing_utils import format_int_col

# VARIABLES

#connection
mapdhost = "use2-api.mapd.cloud"
mapdport = 443
mapdprotocol = "https"
mapddbname = "mapd"
mapduser = "mapd"
#paths
file_path = '/Users/ericgrant/Downloads/OKR_Dashboards/xfer/'
# API keys file
omnisci_keyfile = file_path + 'omnisci_keys.json'

tables_and_files = [
("oss_git_stars", file_path + "oss_git_stars.csv", {'star_starred_at'}, '%Y-%m-%d %H:%M:%S', 'None', {'star_count_to_date'}),
("oss_git_views", file_path + "oss_git_views.csv", {'view_timestamp'}, '%Y-%m-%d %H:%M:%S', 'None', {'view_count', 'view_unique'}),
("oss_git_referrers", file_path + "oss_git_referrers.csv", {}, 'None', 'None', {'repo_referrer_count', 'repo_referrer_unique'})
]

# FUNCTIONS

# Load CSV to dataframe and then copy to table using pymapd
def load_new_table_mapd(connection, table_name, csv_file, ts_cols, ts_format, ts_units, int_cols):
    df = pd.read_csv(csv_file)
    format_int_col(df, int_cols)
    if ts_format == 'None':
        format_date_cols(df, ts_cols, un=ts_units)
    elif ts_units == 'None':
        format_date_cols(df, ts_cols, tf=ts_format)

    if df.empty:
        print ("no results to upload")
    else:
        df.reset_index(drop=True, inplace=True)
        print ("loading table " + table_name)
        connection.load_table(table_name, df, preserve_index=False, create=False) #append the data into the exisiting table in OmniSci

# Load CSV to dataframe and then copy to table using pymapd
def append_new_table_mapd(connection, table_name, csv_file, ts_cols, ts_format, ts_units, int_cols):
    df_new = pd.read_csv(csv_file)
    format_int_col(df_new, int_cols)
    if ts_format == 'None':
        format_date_cols(df_new, ts_cols, un=ts_units)
    elif ts_units == 'None':
        format_date_cols(df_new, ts_cols, tf=ts_format)
    if df_new.empty:
        print ("no results to upload")
    else:
        df_existing = connection.get_table(table_name)
        df_new.reset_index(drop=True, inplace=True)
        df = df_new.concat(df_existing)
        df.drop_duplicates(subset = ts_cols, keep = 'last', inplace = True)
        print ("loading table " + table_name)
        connection.load_table(table_name, df, preserve_index=False, create=True) #append the data into the exisiting table in OmniSci

# MAIN
def main():
    # connect to MapD
    dfcreds = get_credentials(omnisci_keyfile)
    connection = wake_and_connect_to_mapd(dfcreds['write_key_name'], dfcreds['write_key_secret'], mapdhost, mapddbname)
    # loop through tables
    if connection != 'RETRY':
        for table, file, timestamp_cols, timestamp_format, timestamp_units, integer_cols in tables_and_files:
            load_new_table_mapd(connection, table, file, timestamp_cols, timestamp_format, timestamp_units, integer_cols)
        # disconnect MapD
        disconnect_mapd(connection)
    else:
        print('could not wake OmniSci; exiting')

if __name__ == '__main__':
  main()