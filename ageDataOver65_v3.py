#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 23:56:12 2016

@author: CC

Using Python 3.5.2; Anaconda 4.2.0; Spyder 3.0.0

Purpose: open a CSV file from https://censusreporter.org/ and recode it with the corresponding JSON file

CSV raw data file containing age census data in San Diego: 'acs2015_1yr_B01001.csv'
JSON metadata file: 'metadata.json'
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

#only need the B01001001, B01001002, etc.  Don't need the other column names ie anything with the word 'error', geoid, or 'name'.
def getRecodingKeys(element):
    if ('Error' not in element) and ('name' != element) and ('geoid' != element):
        return element
    return False

#filter out the original column names that don't require recoding
codingDF = pandas.DataFrame({'origColNames': list(filter(getRecodingKeys, colNames))})

#open the json file
jsonFile = 'metadata.json';

with open(jsonFile, 'r') as f:
    objects = ijson.items(f, 'tables.B01001.columns')
    columnAttr = list(objects)

#add a new column to codingDF that contains the recoded ageDF column names
codingDF['recodeColName'] = ''

#append the recodedColNames to codingDF.recodeColName
for idx, origColName in enumerate(codingDF.origColNames):
    codingDF.recodeColName[idx] = columnAttr[0][origColName]['name']

#recode ageData with the actual column names
for idx, col in enumerate(ageData.columns):
    if  codingDF.origColNames.str.contains(col): #codingDF.origColNames.str.contains(ageData.columns[idx]):
        colMatchIDX = codingDF.origColNames.get_loc(col)
        if ageData.columns[idx].str.contains('Error'):
            tempColName = codingDF.recodeColName[colMatchIDX] + '_Error'
            ageData.columns[idx] = tempColName
        else:
            tempColName = codingDF.recodeColName[colMatchIDX]

#recode ageData with the actual column names
for idx, col in enumerate(ageData.columns):
    if  codingDF.origColNames.str.contains(col): #codingDF.origColNames.str.contains(ageData.columns[idx]):
        colMatchIDX = codingDF.origColNames.get_loc(col)
        if ageData.columns[idx].str.contains('Error'):
            tempColName = codingDF.recodeColName[colMatchIDX] + '_Error'
            ageData.rename(columns={col:tempColName}, inplace=True)
        else:
            tempColName = codingDF.recodeColName[colMatchIDX]
            print(tempColName)
            ageData.rename(columns={col:tempColName}, inplace=True)

'''
Things I've tried to recode the actual pandas dataframe columns with the appropriate labels

print(codingDF.origColNames.loc[:,'B01001001'])
print(codingDF.origColNames['B01001001, Error'])

codingDF.loc[codingDF.origColNames == 'B01001001']

#ageData.columns[idx] = codingDF.recodeColName

dummy = codingDF.origColNames.str.extract('B01001001')

dummy = codingDF.origColNames.where(codingDF.origColNames == ageData.columns[2])

dummy = codingDF.origColNames.index(ageData.columns[2])

dummy = codingDF.origColNames.Index(ageData.columns[2])

dummy = codingDF.origColNames.loc(ageData.columns[2])

print(codingDF.origColNames.str.contains('B01001001'))

print(dummy)

print(dummy)

dummy = [ageData.columns[2] == codingDF.origColNames]

print(dummy[dummy==True])

codingDF.loc[codingDF['origColNames'] == ('B01001001')]

codingDF.origColNames.str.contains(ageData.columns[4])
'''