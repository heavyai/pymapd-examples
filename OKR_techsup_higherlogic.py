#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 01 11:20:28 2019

@author: ericgrant
"""

import requests
import pandas as pd
from omnisci_utils import get_credentials

# paths
file_path = '/Users/ericgrant/Downloads/OKR_Dashboards/xfer/'
repo_path = 'https://api.connectedcommunity.org/'
keyfile = file_path + 'higherlogic_keys.json'
payload_communitykey = 'd06df790-8ca4-4e54-91a0-244af0228ddc' # UID for General community

token_endpoint = repo_path + 'api/v2.0/Authentication/GetLogin'
data_endpoints = [
    {repo_path + 'api/v2.0/Communities/GetViewableCommunities', 'get', file_path + 'techsup_hl_communities.csv'},
    {repo_path + 'api/v2.0/Communities/GetCommunityMembers', 'post', file_path + 'techsup_hl_communitymembers.csv'}, # list of members of specified community
    {repo_path + 'api/v2.0/Discussions/GetEligibleDiscussions', 'get', file_path + 'techsup_hl_communityupdates.csv'} # list of discussions to which API User can subscribe
   ]



def main():
   # credentials
   dfcreds = get_credentials(keyfile) # get the authentication information from the keyfile
   headers = {'content-type': 'application/json', 'HLIAMKey': dfcreds['key']}
   
   # Viewable Communities
   rViewableCommunities = requests.get(repo_path + 'api/v2.0/Communities/GetViewableCommunities', headers=headers)
   dfViewableCommunities = pd.read_json(rViewableCommunities.content)
   print ('Viewable Communities')
   print (dfViewableCommunities.head(5))

   # Community Members
   print ('Processing Community Members')
   payload = {
           "CommunityKey": 'd06df790-8ca4-4e54-91a0-244af0228ddc',
           "StartRecord": 1,
           "EndRecord": 1500
           }
   rCommunityMembers = requests.post(repo_path + 'api/v2.0/Communities/GetCommunityMembers', headers=headers, json=payload)
#   print ('reading json from requests object')
   dfCommunityMembers = pd.read_json(rCommunityMembers.content)
#   print (dfCommunityMembers.head(5))
#   print ('renaming blank column label')
   dfCommunityMembers.index.names = ['rowUID']
#   print (dfCommunityMembers.head(5))
#   print (dfCommunityMembers.columns)
   print ('removing nested dict')
   dfCommunityMembers.drop('Community', 1, inplace=True)
   print (dfCommunityMembers.columns)
   dfCommunityMembers.to_csv(file_path + 'techsup_hl_communitymembers.csv')

   # Eligible Discussions
   payload = {"CommunityKey": 'd06df790-8ca4-4e54-91a0-244af0228ddc'}
   rEligibleDiscussions = requests.get(repo_path + 'api/v2.0/Discussions/GetEligibleDiscussions', headers=headers, json=payload)
   dfEligibleDiscussions = pd.read_json(rEligibleDiscussions.content)
   print ('Eligible Discussions')
   print (dfEligibleDiscussions.head(5))
#   rEligibleDiscussions.to_csv


if __name__ == '__main__':
 main()
