#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 08:27:36 2018

@author: ericgrant
"""
from datetime import datetime, timedelta
import pandas as pd

# paths
file_path = "/Users/ericgrant/Downloads/OKR_Dashboards/xfer/"
repo_path = "https://community.omnisci.com/admin/"
keyfile = file_path + 'discourse_keys.json'

# time
start_date = "2018-02-01"
now = datetime.today()
day = timedelta(days=1)
end_date = now - day
end_date = end_date.strftime("%Y-%m-%d")

endpoints = [
#stickiness
(repo_path + "reports/dau_by_mau.json?start_date=" + str(start_date) + "&end_date=" + str(end_date), file_path + "techsup_discourse_stickiness.csv"),
#pageviews
(repo_path + "reports/page_view_total_reqs.json?start_date=" + str(start_date) + "&end_date=" + str(end_date), file_path + "techsup_discourse_pageviews.csv"),
#time to first response
(repo_path + "reports/time_to_first_response.json?start_date=" + str(start_date) + "&end_date=" + str(end_date), file_path + "techsup_discourse_timetofirstresponse.csv"),
#daily engaged users
(repo_path + "reports/daily_engaged_users.json?start_date=" + str(start_date) + "&end_date=" + str(end_date), file_path + "techsup_discourse_dailyengagedusers.csv"),
#daily engaged users
(repo_path + "reports/visits.json?start_date=" + str(start_date) + "&end_date=" + str(end_date), file_path + "techsup_discourse_visits.csv")
]

def get_credentials(keyfile):
    dfkv = pd.read_json(keyfile, typ='series')
    return dfkv

def main():
    dfcreds = get_credentials(keyfile)
    str_authentication = "&api_key=" + dfcreds['access_token'] + "&api_username=" + dfcreds['api_username']
    for url, fn in endpoints:
        url_get = url + str_authentication
        df = pd.read_json(url_get, orient="columns")
    #isolate the list
        cell = df.iloc[3,0]
    #format and clean up the data
        df = pd.DataFrame.from_dict(cell) #turn the list into a dataframe
        dfnew = pd.DataFrame(df, columns=["c1_timestamp","c2_value"]) # set the column names
        dfnew["c1_timestamp"] = pd.to_datetime(df["x"])
        dfnew["c2_value"] = pd.to_numeric(df["y"])
    # write to csv
        print ("writing csv to " + fn)
        dfnew.to_csv(fn, index=False, date_format="%Y-%m-%d") # write to csv

if __name__ == '__main__':
  main()