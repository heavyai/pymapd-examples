#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 08:27:36 2018

@author: ericgrant
"""

import requests
import pandas as pd
from pandas.io.json import json_normalize

from parsing_utils import display_cols

file_path = "/Users/ericgrant/Downloads/OKR_Dashboards/xfer/"
pagination = "&simple=yes&page{0}&per_page=30"
repo_path = "https://api.github.com/repos/omnisci/"
keyfile = file_path + "github_keys.json"

endpoints = [
#stargazers
(repo_path + "mapd-core/stargazers", file_path + "oss_git_stargazers_core.csv", "none"),
(repo_path + "mapd-connector/stargazers", file_path + "oss_git_stargazers_connector.csv", "none"),
(repo_path + "mapd-charting/stargazers", file_path + "oss_git_stargazers_charting.csv", "none"),
#traffic sources
(repo_path + "mapd-core/traffic/popular/referrers", file_path + "oss_git_trafficreferrers_core.csv", "none"),
(repo_path + "mapd-connector/traffic/popular/referrers", file_path + "oss_git_trafficreferrers_connector.csv", "none"),
(repo_path + "mapd-charting/traffic/popular/referrers", file_path + "oss_git_trafficreferrers_charting.csv", "none"),
#traffic views
(repo_path + "mapd-core/traffic/views", file_path + "oss_git_trafficviews_core.csv", "views"),
(repo_path + "mapd-connector/traffic/views", file_path + "oss_git_trafficviews_connector.csv", "views"),
(repo_path + "mapd-charting/traffic/views", file_path + "oss_git_trafficviews_charting.csv", "views"),
#subscribers
(repo_path + "mapd-core/subscribers", file_path + "oss_git_subscribers_core.csv", "none"),
(repo_path + "mapd-connector/subscribers", file_path + "oss_git_subscribers_connector.csv", "none"),
(repo_path + "mapd-charting/subscribers", file_path + "oss_git_subscribers_charting.csv", "none"),
#stats/contributors (requires push access to repo)
(repo_path + "mapd-core/stats/contributors", file_path + "oss_git_contributors_core.csv", "author"),
(repo_path + "mapd-connector/stats/contributors", file_path + "oss_git_contributors_connector.csv", "author"),
(repo_path + "mapd-charting/stats/contributors", file_path + "oss_git_contributors_charting.csv", "author"),
#collaborators (requires push access to repo)
(repo_path + "mapd-core/collaborators", file_path + "oss_git_collaborators_core.csv", "none"),
(repo_path + "mapd-connector/collaborators", file_path + "oss_git_collaborators_connector.csv", "none"),
(repo_path + "mapd-charting/collaborators", file_path + "oss_git_collaborators_charting.csv", "none")
]

def get_credentials(keyfile):
    dfkv = pd.read_json(keyfile, typ='series')
    return dfkv

def main():
    dfcreds = get_credentials(keyfile)
    for url, fn, fc in endpoints:
        url_get = url + "?" + "access_token=" + dfcreds['access_token']
    # check for pagination
        print ("calling URL " + url)
        res=requests.get(url_get + pagination)
        repos=res.json()
    # create a dataframe
        df = pd.read_json(url_get)
#        display_cols(df)
        if df.empty:
            print ("No Results")
        else:
            if fc != "none":
                print ("flattening " + fc)
                df = json_normalize(df[fc])
        # loop through subsequent github pages of results
            while 'next' in res.links.keys():
                res=requests.get(res.links['next']['url'])
                repos.extend(res.json())
                dfnext = pd.read_json(url_get)
                if fc != "none":
                    print ("flattening " + fc)
                    df = json_normalize(df[fc])
                df = df.append(dfnext, ignore_index=True)
        # write to file
            print ("writing " + fn)
#            display_cols(df)
            df.to_csv(fn, index=False)

if __name__ == '__main__':
  main()