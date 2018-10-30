#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 10:49:41 2018

@author: ericgrant
"""

import pandas as pd

def rename_cols(df, renames):
    if renames != {}:
        df.rename(columns=renames, inplace=True)

def format_date_cols(df, col_list, tf):
    if col_list != {}:
        if tf == "none":
            for col in col_list: df[col] = pd.to_datetime(df[col])
        else:
            for col in col_list: df[col] = pd.to_datetime(df[col], format=tf)

def format_int_col(df, col_list):
    if col_list != {}:
        for col in col_list: df[col] = pd.to_numeric(df[col], downcast='integer')

def format_str_col(df, col_list):
    if col_list != {}:
        for col in col_list: df[col] = df[col].apply(str)

def format_flt_col(df, col_list):
    if col_list != {}:
        for col in col_list: df[col] = pd.to_numeric(df[col], downcast='float')