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
# community members
("techsup_hl_communitymembers", file_path + "techsup_hl_communitymembers.csv", {'cmtimestamp', 'AcceptedOn', 'AgreedToTermsDateTime', 'EndDate', 'InvitedOn', 'RemovedOn', 'StartDate', 'UpdatedOn'}, "%Y-%m-%d", {'CompanyLegacyContactKey', 'IsCompany', 'LargePictureUrl', 'PictureUrl', 'LegacyContactKey'}),
# discussion posts
("techsup_hl_discussionposts", file_path + "techsup_hl_discussionposts.csv", {'dptimestamp', 'DatePosted'}, "%Y-%m-%d", {'Attachments', 'ContactKey', 'ContactsWhoRecommend', 'Body', 'BodyWithoutMarkup'})
]

# FUNCTIONS

# Load CSV to dataframe and then copy to table using PyMapD
def load_new_table_mapd(connection, table_name, csv_file, dtcol, tfrmt, drop_cols, mapd_host, mapd_user):
    df = pd.read_csv(csv_file)
    df.reset_index(drop=True, inplace=True)
    format_date_cols(df, dtcol, tfrmt) #force the column containing datetime values to be recast from strings to datetimes
    # drop the big columns of text we don't need for metrics
    df.drop(columns = drop_cols)
    # drop the old table
    drop_table_mapd(connection, table_name) #drop the old table
    print ("creating table " + table_name)
    print ('with columns')
    print (list(df.columns.values))
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
        for table, file, dt, tformat, dropcols, in tables_and_files:
            load_new_table_mapd(connection, table, file, dt, tformat, dropcols, mapdhost, mapduser)
        # disconnect MapD
        disconnect_mapd(connection)

if __name__ == '__main__':
  main()