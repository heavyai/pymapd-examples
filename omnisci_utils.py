#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 10:42:27 2018

@author: ericgrant
"""

from pymapd import connect
import pandas as pd
import time

# number of seconds to wait before trying to reconnect
wait_interval = 30
# max number of attempts to connect
max_tries = 3

def get_credentials(keyfile):
    dfkv = pd.read_json(keyfile, typ='series')
    return dfkv

def connect_to_mapd(str_user, str_password, str_host, str_dbname):
    try:
        connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname, port=443, protocol='https')
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        if 'OmniSci Core not ready, try again' in message:
            print('OmniSci Not Ready')
            connection = "RETRY"
    return connection

def wake_and_connect_to_mapd(str_user, str_password, str_host, str_dbname):
    connection = 'RETRY'
    num_tries = 0
    while connection == 'RETRY' and num_tries <= max_tries:
        num_tries += 1
        try:
            connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname, port=443, protocol='https')
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            if 'OmniSci Core not ready, try again' in message:
                print('OmniSci Not Ready')
                print('retrying in ' + str(wait_interval) + ' seconds')
                time.sleep(wait_interval)
            else:
                print(message)
    return connection

def drop_table_mapd(connection, table_name):
    command = "DROP TABLE IF EXISTS %s" % (table_name)
    connection.execute(command)

def disconnect_mapd(connection):
    connection.close()

