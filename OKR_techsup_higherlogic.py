#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 01 11:20:28 2019

@author: ericgrant
"""

import requests
import pandas as pd
import datetime as dt
from omnisci_utils import get_credentials

# paths
file_path = '/Users/ericgrant/Downloads/OKR_Dashboards/xfer/'
repo_path = 'https://api.connectedcommunity.org/'
keyfile = file_path + 'higherlogic_keys.json'

def main():
   # credentials
   dfcreds = get_credentials(keyfile) # get the authentication information from the keyfile
   headers = {'content-type': 'application/json', 'HLIAMKey': dfcreds['key']}
   
   # Viewable Communities
   rViewableCommunities = requests.get(repo_path + 'api/v2.0/Communities/GetViewableCommunities', headers=headers)
   dfViewableCommunities = pd.read_json(rViewableCommunities.content)

   # Community Members
   payload = {
           "CommunityKey": 'd06df790-8ca4-4e54-91a0-244af0228ddc',
           "StartRecord": 1,
           "EndRecord": 1500
           }
   rCommunityMembers = requests.post(repo_path + 'api/v2.0/Communities/GetCommunityMembers', headers=headers, json=payload)
   dfCommunityMembers = pd.read_json(rCommunityMembers.content)
   # add a timestamp to the data
   dfCommunityMembers['cmtimestamp'] = dt.datetime.now()
   dfCommunityMembers.index.names = ['rowUID']
   dfCommunityMembers.drop('Community', 1, inplace=True) #remove the nested dictionary of community information
   dfCommunityMembers.to_csv(file_path + 'techsup_hl_communitymembers.csv', index=False, date_format="%Y-%m-%d")

   # Discussion Posts
   rDiscussionPosts = requests.get(repo_path + 'api/v2.0/Discussions/GetDiscussionPosts?maxToRetrieve=5000', headers=headers)
   dfDiscussionPosts = pd.read_json(rDiscussionPosts.content)
   # add a timestamp to the data
   dfDiscussionPosts['dptimestamp'] = dt.datetime.now()
   dfDiscussionPosts.to_csv(file_path + 'techsup_hl_discussionposts.csv', index=False, date_format="%Y-%m-%d")

if __name__ == '__main__':
 main()
