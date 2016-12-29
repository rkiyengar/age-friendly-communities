#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 23:56:12 2016

c@author: CC

Using Python 3.5.2 and Anaconda 4.2.0

Purpose: open a CSV file from https://censusreporter.org/ and recode it with the corresponding JSON file
"""

import pandas
import numpy
import os
import ijson

path = os.chdir('/Users/superuser/Documents/projects/SDRegionalDataLib/age friendly community/acs2015_1yr_B01001/')

#load the age data file
ageData = pandas.read_csv('acs2015_1yr_B01001.csv');

#get the names of the columns
colNames = list(ageData.columns.values)

#only need the B01001001, B01001002, etc.  Don't need the other column names
def getRecodingKeys(element):
    if ('Error' not in element) and ('name' != element) and ('geoid' != element):
        return element
    return False
    
colKeys = list(filter(getRecodingKeys, colNames))

jsonFile = 'metadata.json';

with open(jsonFile, 'r') as f:
    objects = ijson.items(f, 'tables.B01001.columns')
    columnAttr = list(objects)

columnCoding = pandas.DataFrame(columnAttr)
    
columnCodes = columnAttr[0]['B0100]['name']

#need to get a list of the B0100100x's without the word 'errors'

dummy = columnCodes['B01001001']