#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 11:36:13 2018

@author: ericgrant
"""

import pandas as pd

from parsing_utils import rename_cols
from parsing_utils import format_int8_col
from parsing_utils import format_int32_col
from parsing_utils import format_date_cols
from parsing_utils import format_str_col
from parsing_utils import format_bool_col

from omnisci_utils import get_credentials
from omnisci_utils import wake_and_connect_to_mapd
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
table_name = 'techsup_docker'

# API keys file
omnisci_keyfile = file_path + 'omnisci_keys.json'

file_names = [
#mapd repos
(file_path + 'techsup_docker_ce-cpu_mapd.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
(file_path + 'techsup_docker_ce-cuda_mapd.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
(file_path + 'techsup_docker_core-os-cpu_mapd.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
(file_path + 'techsup_docker_core-os_cuda.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
#omnisci repos
(file_path + 'techsup_docker_ce-cpu_omnisci.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
(file_path + 'techsup_docker_ce-cuda_omnisci.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'}),
(file_path + 'techsup_docker_core_clients_base_omnisci.csv', {'user': 'username'}, {'status', 'star_count'}, {'pull_count'}, {'get_timestamp', 'last_updated'}, '%Y-%m-%dT%H:%M:%S', {'full_description', 'affiliation', 'permissions'}, {'is_private', 'is_automated', 'can_edit', 'is_migrated', 'has_starred'})
]

# FUNCTIONS
def parse_cols(df, renamings, int8s, int32s, dates, timeformat, strs, bools):
    rename_cols(df, renamings)
    format_int8_col(df, int8s)
    format_int32_col(df, int32s)
    format_date_cols(df, dates, timeformat)
    format_str_col(df, strs)
    format_bool_col(df, bools)

# MAIN
def main():
    # connect to MapD
    dfcreds = get_credentials(omnisci_keyfile)
    connection = wake_and_connect_to_mapd(dfcreds['write_key_name'], dfcreds['write_key_secret'], mapdhost, mapddbname)
    # loop through tables
    if connection == 'RETRY':
        print('could not wake OmniSci; exiting')
    else:
        for csv_file, renamed_cols, int8_cols, int32_cols, ts_cols, tf, str_cols, bool_cols in file_names:
            #get the contents of the file and turn them into a dataframe
            print ("reading from file " + csv_file)
            dfnew = pd.read_csv(csv_file, index_col=False)
            #rename and recast columns
            parse_cols(dfnew, renamed_cols, int8_cols, int32_cols, ts_cols, tf, str_cols, bool_cols)
            #append the contents of this file to the existing table
            print ("appending file contents to table " + table_name)
            connection.load_table(table_name, dfnew, preserve_index=False, create=False) #load the new table into OmniSci
        # disconnect MapD
        disconnect_mapd(connection)

if __name__ == '__main__':
  main()
