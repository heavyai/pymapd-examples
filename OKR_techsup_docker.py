#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 11:20:28 2018

@author: ericgrant
"""

import pandas as pd

# paths
file_path = "/Users/ericgrant/Downloads/OKR_Dashboards/xfer/"
repo_path = "https://hub.docker.com/v2/repositories/"

endpoints_and_files = [
#mapd repos
(repo_path + 'mapd/mapd-ce-cpu', file_path + 'techsup_docker_ce-cpu_mapd.csv'),
(repo_path + 'mapd/mapd-ce-cuda', file_path + 'techsup_docker_ce-cuda_mapd.csv'),
(repo_path + 'mapd/core-os-cpu', file_path + 'techsup_docker_core-os-cpu_mapd.csv'),
(repo_path + 'mapd/core-os-cuda', file_path + 'techsup_docker_core-os_cuda.csv'),
#omnisci repos
(repo_path + 'omnisci/mapd-ce-cpu', file_path + 'techsup_docker_ce-cpu_omnisci.csv'),
(repo_path + 'omnisci/mapd-ce-cuda', file_path + 'techsup_docker_ce-cuda_omnisci.csv'),
(repo_path + 'omnisci/core-clients-base', file_path + 'techsup_docker_core_clients_base_omnisci.csv')
]

def main():
    for url, fn in endpoints_and_files:
        print ("pulling data from " + url)
        df = pd.read_json(url, orient="columns")
    # write to csv
        print ("writing csv to " + fn)
        df.to_csv(fn, index=False, date_format="%Y-%m-%d") # write to csv

if __name__ == '__main__':
  main()