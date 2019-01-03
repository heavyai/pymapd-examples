#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 08:27:36 2018

@author: ericgrant
"""

import pandas as pd
from github import Github
from omnisci_utils import get_credentials

file_path = "/Users/ericgrant/Downloads/OKR_Dashboards/xfer/"
parameters = {'page': 0, 'per_page': 30}
gproject = "omnisci/"
keyfile = file_path + "github_keys.json"

repositories = [("mapd-core"), ("mapd-connector"), ("mapd-charting"), ("pymapd-examples")]

def get_stars(r):
    # setup dataframes for capturing information
    columns = ['repo', 'star_login', 'star_starred_at', 'star_url', 'star_usertype']
    df = pd.DataFrame(columns=columns)
    r_stars = r.get_stargazers_with_dates()
    i = 0
    for star in r_stars:
        df.loc[i] = [r.name, star.user.login, star.starred_at, star.user.html_url, star.user.type]
        i += 1
    df.sort_values('star_starred_at')
    df['star_count_to_date'] = df.index

    if df.empty:
        print ("no stars")
    else:
        print (str(r.stargazers_count) + " stars for " + r.name)
        return df

def get_views(r):
    # retrieve views information
    r_views = r.get_views_traffic()
    df = pd.DataFrame.from_dict(r_views)
    # iterate through individual view objects nested in the contents
    i = 0
    ts = pd.Series('ts', index=[i])
    cnt = pd.Series('cnt', index=[i])
    uni = pd.Series('uni', index=[i])
    repo = pd.Series('repo', index=[i])
    for view in df['views']: # this column contains a list of view objects
        i += 1
        repo[i] = r.name
        ts[i] = getattr(view, 'timestamp')
        ts[i] = ts[i]/1000000000
        cnt[i] = getattr(view, 'count')
        uni[i] = getattr(view, 'uniques')

    # setup dataframe by concatenating the series together as columns
    list_of_series = [repo, ts, cnt, uni]
    # drop the column names before concatenating
    repo.drop([0], inplace = True)
    ts.drop([0], inplace = True)
    cnt.drop([0], inplace = True)
    uni.drop([0], inplace = True)
    df_views = pd.concat(list_of_series, axis=1, ignore_index=True)
    # rename the columns to useful labels
    columns = ['repo', 'view_timestamp', 'view_count', 'view_unique']
    df_views.columns = columns

    if df_views.empty:
        print ("no views")
    else:
        print (str(df_views['view_count'].sum()) + ' views for ' + r.name)
        return df_views

def get_referrers(r):
    # setup dataframes for capturing information
    columns = ['repo', 'repo_referrer', 'repo_referrer_count', 'repo_referrer_unique']
    df = pd.DataFrame(columns=columns)
    r_referrers = r.get_top_referrers()
    i = 0
    for referrer in r_referrers:
        df.loc[i] = [r.name, referrer.referrer, referrer.count, referrer.uniques]
        i += 1

    if df.empty:
        print ("no referrals")
    else:
        print (str(df['repo_referrer'].count()) + ' referrers for ' + r.name)
        return df

def main():

    # credentials
    dfcreds = get_credentials(keyfile) # get the authentication information from the keyfile
    auth_header = dfcreds['access_token'] # get the token from the authentication information

    # connect to github
    g = Github(auth_header) # instantiate a github object; authorize with the token

    # setup dataframes
    df_s_main = pd.DataFrame()
    df_v_main = pd.DataFrame()
    df_r_main = pd.DataFrame()

    # loop through the list of repos
    for repo in repositories:
        r = g.get_repo(gproject + repo)

        # stars
        df_stars = get_stars(r)
        df_s_main = df_s_main.append(df_stars, ignore_index=True)

        # views
        df_views = get_views(r)
        df_v_main = df_v_main.append(df_views, ignore_index=True)

        # referrers
        df_referrers = get_referrers(r)
        df_r_main = df_r_main.append(df_referrers, ignore_index=True)

    # write stars to file
    print ("writing stars to file")
    df_s_main.to_csv(file_path + "oss_git_stars.csv", index=False)

    # write views to file
    print ("writing views to file")
    df_v_main.to_csv(file_path + "oss_git_views.csv", index=False)

    # write referrers to file
    print ("writing referrers to file")
    df_r_main.to_csv(file_path + "oss_git_referrers.csv", index=False)


if __name__ == '__main__':
  main()