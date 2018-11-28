#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 13:21:38 2018

@author: ericgrant
"""

import pandas as pd

from parsing_utils import rename_cols
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
#stargazers
("oss_git_stargazers_core", file_path + "oss_git_stargazers_core.csv", {}, {}, "none"),
("oss_git_stargazers_connector", file_path + "oss_git_stargazers_connector.csv", {}, {}, "none"),
("oss_git_stargazers_charting", file_path + "oss_git_stargazers_charting.csv", {}, {}, "none"),
#collaborators
("oss_git_collaborators_core", file_path + "oss_git_collaborators_core.csv", {}, {}, "none"),
("oss_git_collaborators_connector", file_path + "oss_git_collaborators_connector.csv", {}, {}, "none"),
("oss_git_collaborators_charting", file_path + "oss_git_collaborators_charting.csv", {}, {}, "none"),
#contributors
("oss_git_contributors_core", file_path + "oss_git_contributors_core.csv", {}, {}, "none"),
("oss_git_contributors_connector", file_path + "oss_git_contributors_connector.csv", {}, {}, "none"),
("oss_git_contributors_charting", file_path + "oss_git_contributors_charting.csv", {}, {}, "none"),
#traffic referrers
("oss_git_trafficreferrers_core", file_path + "oss_git_trafficreferrers_core.csv", {}, {'count': 'counter'}, "none"),
("oss_git_trafficreferrers_connector", file_path + "oss_git_trafficreferrers_connector.csv", {}, {'count': 'counter'}, "none"),
("oss_git_trafficreferrers_charting", file_path + "oss_git_trafficreferrers_charting.csv", {}, {'count': 'counter'}, "none"),
#traffic views
("oss_git_trafficviews_core", file_path + "oss_git_trafficviews_core.csv", {'timestamp'}, {'count': 'counter', 'timestamp': 'c1_timestamp'}, "none"),
("oss_git_trafficviews_connector", file_path + "oss_git_trafficviews_connector.csv", {'timestamp'}, {'count': 'counter', 'timestamp': 'c1_timestamp'}, "none"),
("oss_git_trafficviews_charting", file_path + "oss_git_trafficviews_charting.csv", {'timestamp'}, {'count': 'counter', 'timestamp': 'c1_timestamp'}, "none"),
#subscribers
("oss_git_subscribers_core", file_path + "oss_git_subscribers_core.csv", {}, {}, "none"),
("oss_git_subscribers_connector", file_path + "oss_git_subscribers_connector.csv", {}, {}, "none"),
("oss_git_subscribers_charting", file_path + "oss_git_subscribers_charting.csv", {}, {}, "none")
]

# FUNCTIONS

# Load CSV to dataframe and then copy to table using pymapd
def load_new_table_mapd(connection, table_name, csv_file, dtcol, renamings, tfrmt, mapd_host, mapd_user):
    df = pd.read_csv(csv_file)
    if df.empty:
        print ("no results to upload")
    else:
        df.reset_index(drop=True, inplace=True)
        format_date_cols(df, dtcol, tfrmt) #force the column containing datetime values to be recast from strings to datetimes
        rename_cols(df, renamings) #rename any columns that have naming conflicts (such as reserved words in immerse)
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
    if connection != 'RETRY':
        for table, file, dt, rn, tformat in tables_and_files:
            load_new_table_mapd(connection, table, file, dt, rn, tformat, mapdhost, mapduser)
        # disconnect MapD
        disconnect_mapd(connection)
    else:
        print('could not wake OmniSci; exiting')

if __name__ == '__main__':
  main()