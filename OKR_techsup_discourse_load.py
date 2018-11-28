#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 13:21:38 2018

@author: ericgrant
"""

import pandas as pd

from parsing_utils import format_date_cols

from omnisci_utils import get_credentials
from omnisci_utils import wake_and_connect_to_mapd
from omnisci_utils import drop_table_mapd
from omnisci_utils import disconnect_mapd

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
#visits
("techsup_discourse_visits", file_path + "techsup_discourse_visits.csv", {'c1_timestamp'}, "%Y-%m-%d"),
#stickiness
("techsup_discourse_stickiness", file_path + "techsup_discourse_stickiness.csv", {'c1_timestamp'}, "%Y-%m-%d"),
#pageviews
("techsup_discourse_pageviewtotalreqs", file_path + "techsup_discourse_pageviews.csv", {'c1_timestamp'}, "%Y-%m-%d"),
#time to first response
("techsup_discourse_timetofirstresponse", file_path + "techsup_discourse_timetofirstresponse.csv", {'c1_timestamp'}, "%Y-%m-%d"),
#daily engaged users
("techsup_discourse_dailyengagedusers", file_path + "techsup_discourse_dailyengagedusers.csv", {'c1_timestamp'}, "%Y-%m-%d")
]

# FUNCTIONS

# Load CSV to dataframe and then copy to table using PyMapD
def load_new_table_mapd(connection, table_name, csv_file, dtcol, tfrmt, mapd_host, mapd_user):
    df = pd.read_csv(csv_file)
    df.reset_index(drop=True, inplace=True)
    format_date_cols(df, dtcol, tfrmt) #force the column containing datetime values to be recast from strings to datetimes
    drop_table_mapd(connection, table_name) #drop the old table
    connection.create_table(table_name, df, preserve_index=False) #create the new table
    print ("loading table " + table_name)
    connection.load_table(table_name, df) #load the new table into OmniSci

# MAIN
def main():
    # connect to MapD
    dfcreds = get_credentials(omnisci_keyfile)
    connection = wake_and_connect_to_mapd(dfcreds['write_key_name'], dfcreds['write_key_secret'], mapdhost, mapddbname)
    # loop through tables
    if connection == 'RETRY':
        print('could not wake OmniSci; exiting')
    else:
        for table, file, dt, tformat in tables_and_files:
            load_new_table_mapd(connection, table, file, dt, tformat, mapdhost, mapduser)
        # disconnect MapD
        disconnect_mapd(connection)

if __name__ == '__main__':
  main()