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
from omnisci_utils import get_table_mapd

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


# table to retreive
table_name = 'oss_git_views'

tables_and_files = [
# github views
(table_name, file_path + table_name + '_out.csv')
]

# FUNCTIONS

# MAIN
def main():
    # connect to MapD
    dfcreds = get_credentials(omnisci_keyfile)
    connection = wake_and_connect_to_mapd(dfcreds['write_key_name'], dfcreds['write_key_secret'], mapdhost, mapddbname)
    # loop through tables
    if connection == 'RETRY':
        print('could not wake OmniSci; exiting')
    else:
        for table, file in tables_and_files:
            df = pd.DataFrame()
            df = get_table_mapd(connection, table)
            df.sort_values(['repo', 'view_timestamp'], inplace = True)
            print (df.head(10))
            df.to_csv(file, index=False)
        # disconnect MapD
        disconnect_mapd(connection)

if __name__ == '__main__':
  main()