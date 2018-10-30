#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 10:42:27 2018

@author: ericgrant
"""

from pymapd import connect
import pandas as pd

def get_credentials(keyfile):
    dfkv = pd.read_json(keyfile, typ='series')
    return dfkv

def connect_to_mapd(str_user, str_password, str_host, str_dbname):
    connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname, port=443, protocol="https")
    return connection

def drop_table_mapd(connection, table_name):
    command = "DROP TABLE IF EXISTS %s" % (table_name)
    connection.execute(command)

def disconnect_mapd(connection):
    connection.close()
