#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 10:20:09 2018

@author: ericgrant
"""

import OKR_techsup_ga as GoogleAnalytics
import OKR_oss_git as GithubGet
import OKR_oss_git_load as GithubPut
import OKR_techsup_docker as DockerGet
import OKR_techsup_docker_load as DockerPut
import OKR_techsup_discourse as DiscourseGet
import OKR_techsup_discourse_load as DiscoursePut

GoogleAnalytics.main()
GithubGet.main()
GithubPut.main()
DockerGet.main()
DockerPut.main()
DiscourseGet.main()
DiscoursePut.main()
