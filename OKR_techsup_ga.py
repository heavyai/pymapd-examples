#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 15:48:38 2018

@author: ericgrant
"""

import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import pandas as pd
from pandas.io.json import json_normalize

from omnisci_utils import get_credentials
from omnisci_utils import wake_and_connect_to_mapd
from omnisci_utils import drop_table_mapd
from omnisci_utils import disconnect_mapd

from parsing_utils import format_date_cols
from parsing_utils import format_int_col
from parsing_utils import format_str_col
from parsing_utils import format_flt_col

file_path = '/Users/ericgrant/Downloads/OKR_Dashboards/xfer/'
file_geocodes = file_path + 'AdWords API Location Criteria 2018-09-04.csv'

# parameters for OmniSci Cloud
mapdhost = 'use2-api.mapd.cloud'
mapdport = 443
mapdprotocol = 'https'
mapddbname = 'mapd'
mapduser = 'mapd'
omnisci_keyfile = file_path + 'omnisci_keys.json'
wait_interval = 25

# parameters for Google API
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
VIEW_ID = '93521025'
CLIENT_SECRETS_PATH = file_path + 'client_secrets.json' # path to client_secrets.json file.
start_date = '2017-04-01'

tables_and_files = [
#blog post views
('techsup_ga_blogvisits', file_path + 'techsup_ga_blogvisits.csv', {'c1_timestamp'}, {}, {'time_on_page', 'unique_pageviews'}, {}, {'geo_city_code'}, "%Y%m%d",
'CREATE TABLE IF NOT EXISTS techsup_ga_blogvisits (blog_title TEXT ENCODING DICT(8), blog_url TEXT ENCODING DICT(8), referral_path TEXT ENCODING DICT(8), c1_timestamp TIMESTAMP, unique_pageviews FLOAT, time_on_page FLOAT, source TEXT ENCODING DICT(8), city_name TEXT ENCODING DICT(8), city_canonical_name TEXT ENCODING DICT(8), country_code TEXT ENCODING DICT(8));',
  {
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [
                  {'startDate': start_date, 'endDate': 'today'}
                ],
          'pageSize': 10000,
          'metrics': [
                  {'expression': 'ga:uniquePageviews'},
                  {'expression': 'ga:timeOnPage'}
                ],
          'dimensions': [
                  {'name': 'ga:pageTitle'},
                  {'name': 'ga:pagePath'},
                  {'name': 'ga:referralPath'},
                  {'name': 'ga:date'},
                  {'name': 'ga:cityID'}
                ],
          'dimensionFilterClauses': [
                  {'filters': [
                    {'dimensionName': 'ga:pageTitle', 'operator': 'PARTIAL', 'expressions': ['blog']}
                    ]}
              ]}
    ]}
)
]

# GOOGLE ANALYTICS FUNCTIONS

def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

  return analytics

def get_report(analytics, bodycontent):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body=bodycontent).execute()

def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print (header + ': ' + dimension)

      for i, values in enumerate(dateRangeValues):
        print ('Date range (' + str(i) + ')')
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print (metricHeader.get('name') + ': ' + value)

def format_data(response):
    reports = response['reports'][0]
    columnHeader = reports['columnHeader']['dimensions']
    metricHeader = reports['columnHeader']['metricHeader']['metricHeaderEntries']

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric['name'])
    data = json_normalize(reports['data']['rows'])
    data_dimensions = pd.DataFrame(data['dimensions'].tolist())
    data_metrics = pd.DataFrame(data['metrics'].tolist())
    data_metrics = data_metrics.applymap(lambda x: x['values'])
    data_metrics = pd.DataFrame(data_metrics[0].tolist())
    result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)
    result.columns = ["blog_title", "blog_url", "referral_path", "c1_timestamp", "geo_city_code", "unique_pageviews", "time_on_page"] # set the column names
    return (result)

def output_to_csv(df, fn):
    df.to_csv(fn, index=False)

# OMNISCI FUNCTIONS

def source(url):
    if 'tag' in url:
        source = 'tag'
    elif 'author' in url:
        source = 'author'
    elif url == 'www.mapd.com/blog':
        source = 'mapd blog landing page'
    elif url == 'www.mapd.com/blog/':
        source = 'mapd blog landing page'
    elif url == 'www.omnisci.com/blog':
        source = 'omnisci blog landing page'
    elif url == 'www.mapd.com/blog/':
        source = 'omnisci blog landing page'
    elif 'community.mapd.com' in url:
        source = 'mapd community forum'
    elif 'community.omnisci.com' in url:
        source = 'omnisci community forum'
    else:
        source = 'other / direct'
    return (source)

def parse_source(df):
    df['source'] = df['blog_url'].apply(source)
    return df

def parse_city(df):
    dfcity = pd.read_csv(file_geocodes)
    dfcity.columns = ['geo_city_code', 'city_name', 'city_canonical_name', 'city_parent_code', 'country_code', 'city_target_type', 'city_status'] # set the column names
    format_str_col(dfcity, {'geo_city_code'})
    df = pd.merge(df, dfcity, on=['geo_city_code'], how='left')
    return df

def parse_geo_data(df):
    df = parse_source(df)
    df = parse_city(df)
    return df

# Load CSV to dataframe
def parse_data(csvfile, dtcols, intcols, floatcols, strcols, renamings, tfrmt):
    df = pd.read_csv(csvfile)
    df.reset_index(drop=True, inplace=True)
    format_date_cols(df, dtcols, tfrmt) #force the column containing datetime values to be recast from strings to timestamps
    format_int_col(df, intcols)
    format_str_col(df, strcols)
    format_flt_col(df, floatcols)
    df = parse_geo_data(df)
    df = df.drop('geo_city_code', 1)
    df = df.drop('city_parent_code', 1)
    df = df.drop('city_target_type', 1)
    df = df.drop('city_status', 1)
    return df

def wake_up_omnisci():
# get OmniSci credentials
    dfcreds = pd.DataFrame()
    dfcreds = get_credentials(omnisci_keyfile)
# connect to OmniSci, allowing time for the instance to wake
    connection = wake_and_connect_to_mapd(dfcreds['write_key_name'], dfcreds['write_key_secret'], mapdhost, mapddbname)
    return connection

# MAIN
def main():
    # connect to omnisci
    connection = wake_up_omnisci()
    if connection != "RETRY":
    # loop through tables and reports
        for os_table, csv_file, dt_cols, int_cols, float_cols, str_cols, rename_cols, time_format, creationstring, reportbody in tables_and_files:
            # connect to Google Analytics
            analytics = initialize_analyticsreporting()
            response = get_report(analytics, reportbody)
            # format the data into the columnar tables OmniSci wants
            df = format_data(response)
            # save the dataframe to a file
            output_to_csv(df, csv_file)
            # create the new dataframe from the file contents
            df = parse_data(csv_file, dt_cols, int_cols, float_cols, str_cols, rename_cols, time_format)
            print ('loading dataframe into table ' + os_table)
            drop_table_mapd(connection, os_table) #drop the old table
            connection.execute(creationstring)
            connection.load_table(os_table, df) #load the new table into OmniSci
        # disconnect from OmniSci
        disconnect_mapd(connection)
    else:
        print('could not wake OmniSci; exiting')



if __name__ == '__main__':
  main()