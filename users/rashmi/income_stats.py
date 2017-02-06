#! /usr/bin/env python

################################################################################
#
# income_stats.py
#
# Script to extract income information specific to individuals 55 and older from  
# the ACS archive containing it and to output the same on a per SRA and zipcode
# basis for the SD county 
# 
# Dependencies:
#
# Data files must be present in the current working directory
#
# Usage:
#
# python income_stats.py
#

import sys
import os
import shutil
import re
import pandas as pd 
import numpy as np 
import pprint
from zipfile import ZipFile
from collections import defaultdict, OrderedDict
import sdpyutils as sdpy  

#
# GLOBALS 
#

# current working directory
CWD = os.getcwd()
TMPDIR = os.path.join(CWD,"tmp")

# datafile(s)
VERSION = "2015"
DATAZIP = "aff_B17024_sd_county_" + VERSION + ".zip"

OUT_CSV1 = "B17024_estimates_sd_county_55_over_" + VERSION + ".csv"
OUT_CSV2 = "low_income_data_sd_county_" + VERSION + ".csv"

#
# Removes the temp directory and its contents
#
def cleanup(doCleanup):
	# Cleanup the temp directory only if we created it here
	if doCleanup:
		if os.path.exists(TMPDIR):
			shutil.rmtree("tmp")
			doCleanup = False
#
# processMetaData
#
def processMetaData(metafile):

	csvdata = pd.read_csv(metafile,header=None)
	#print csvdata
	return csvdata

#
# modifyDataLabels
#
# function to modify data lables for the specified target using values in 
# dict_fields
# 
# Returns:
#     ratio_dict - dictionary of modified labels grouped by ratio range
#       age_dict - dictionary of modified labels grouped by age range
# modifiedLabels - full list of modified labels (same ordering as that of 
#	               targetLabels)
#
def modifyDataLabels(targetLabels, df_fields):

	# convert to dictionary for easier lookup
	dict_fields = df_fields.set_index(0).T.to_dict('list')

	# generate the regex instance for the specified pattern
	prefix = " - "
	regex = re.compile('(.*); (.*) years(.*):(.*)')

	# generate replacement labels for targeted labels using metadata
	# in df_fields
	modifiedLabels = []

	# FIX ME: need an ordered defualt dict; for now use ordered dict only
	ratio_dict = OrderedDict(); age_dict = OrderedDict()

	for name in targetLabels[1:]:
		if name in dict_fields:
			m = regex.match(dict_fields[name][0])
			
			ratioTag = ""; ageTag = ""
			if m.group(4).startswith(prefix):
				ratioTag = m.group(4)[len(prefix):]
			else:
				ratioTag = "Total"

			ageTag = m.group(2) + m.group(3) 
			
			label = ratioTag + " (" + ageTag + ")"
			#print (name + ": " + label)
			
			if ageTag in age_dict:
				age_dict[ageTag].append(label)
			else:
				age_dict[ageTag] = [label]

			if ratioTag in ratio_dict:
				ratio_dict[ratioTag].append(label)
			else:
				ratio_dict[ratioTag] = [label]	

			modifiedLabels.append(label)
		else:
			modifiedLabels.append(name)

	return ratio_dict, age_dict, modifiedLabels		

#
# addSRAaggregates
#
# aggregates per zipcode/ZCTA data and populates the unique entry per SRA  with
# the aggreagated values
#
# Note: this requires that data be in a specific format 
#       (see df_geoids dataframe)
#
def addSRAaggregates(df,targetCols):
	
	for name, group in df.groupby('SRA'):
			idx = group.last_valid_index()
			#print df.loc[[idx]]

			for col in targetCols:
				df.set_value(idx,col,group[col].sum())

	return df			

#
# computeLowIncomeData
# 
# aggregates data for all ratios below 2.00 for all age groups
#
def computeLowIncomeData(df_incomes,df_geoids,ratio_dict,age_dict):

	# low income is defined as 200% (or below) of the federal poverty level
	# i.e.: the income to poverty level ratio under 2.0
	LOW_INCOME_RATIO_TRESH = "1.85 to 1.99"
	geoCols = df_geoids.columns.tolist()

	df = df_incomes.iloc[:,len(geoCols):]
	df = df_incomes.reset_index(drop=True)

	df_sum_list = []
	cols = []
	
	for age_group, colnames in age_dict.iteritems():
		#print(str(age_group) + ": " + str(colnames))

		try:
			idx = [i for i, s in enumerate(colnames) if LOW_INCOME_RATIO_TRESH in s]
			df_sum = df[colnames[1:(idx[0]+1)]].sum(axis=1)
			df_sum_list.append(df_sum)
		except Exception, e:
			df_sum = pd.DataFrame(columns=[age_group],
						data=np.zeros(shape=(len(df_geoids.index),1)))
			df_sum_list.append(df_sum)
		
		cols.append(age_group + " (Low Income)")

	df1 = pd.concat(df_sum_list,axis=1)
	df1.columns = cols

	df1["55 and Over (Low Income)"] = df1[cols].sum(axis=1)
	df1["65 and Over (Low Income)"] = df1[cols[1:]].sum(axis=1)

	li_df = pd.concat([df_geoids,df1],axis=1)
	li_df = addSRAaggregates(li_df,df1.columns.tolist())
	
	#print li_df
	return li_df

#
# processData
#
def processData(df_fields,datafile):

	# index of GEO.id2 which contains ZCTA as numbers
    COL_ZCTA_IDX = 1
    COL_ZCTA = 'GEO.id2'
    # this is the first field that holds income info for 55+ age groups
    START_COL = 'HD01_VD93'

    # extract only data for income estimates for 55 and over categories
    startIndex = df_fields[df_fields[0] == START_COL].index.tolist()[0]
    endIndex = len(df_fields) - 1
    # print("si: " + str(startIndex) + " ei: " + str(endIndex))

    l = df_fields[0].tolist()
    # we skip over cols that contain margins of error (i.e.: every other col)
    cols = [l[COL_ZCTA_IDX]] + l[startIndex:endIndex:2]

    csvdata = pd.read_csv(datafile,skipinitialspace=True,usecols=cols)
    #print csvdata.head()
        
    df_geoids = sdpy.createGeoidsData()
    geoCols = df_geoids.columns.tolist()

    # add single level col headers with age and ratio tags
    ratio_dict, age_dict, modifiedCols = modifyDataLabels(cols,df_fields)

    out_df = pd.merge(left=df_geoids,right=csvdata[1:],left_on='ZCTA',
    				right_on=COL_ZCTA,how='left').fillna(0)
    out_df.drop(COL_ZCTA,axis=1,inplace=True)
    out_df.columns = geoCols + modifiedCols
    
    tmp_df = out_df[modifiedCols].apply(pd.to_numeric)
    out_df = pd.concat([df_geoids,tmp_df],axis=1)
    out_df.columns = geoCols + modifiedCols

    out_df = addSRAaggregates(out_df,modifiedCols)

    #print out_df.head()
    out_df.to_csv(OUT_CSV1, index=False)

    li_df = computeLowIncomeData(out_df,df_geoids,ratio_dict,age_dict)
   
    #print li_df.head()
    li_df.to_csv(OUT_CSV2, index=False)
    
################################################################################
# 
# main
#
def main():

	# indicates whether to cleanup before exiting the script
	doCleanup = False
	metadataFile = '';	dataFile = ''

	if not os.path.exists(TMPDIR):
		os.makedirs(TMPDIR)	
		doCleanup = True	

	# unzip the archive
	try:
		zipf = ZipFile(os.path.join(CWD,DATAZIP),'r')
		zipf.extractall(TMPDIR)
		zipf.close()

		for file in os.listdir(TMPDIR):
			if file.endswith("metadata.csv"):
				metadataFile = file
			elif file.endswith("ann.csv"):
				dataFile = file
			else:
				continue 
		#print("Metafile: " + metadataFile + " Datafile: " + dataFile)

		df_fields = processMetaData(os.path.join(TMPDIR,metadataFile))

		processData(df_fields, os.path.join(TMPDIR,dataFile))

	except:
		e = sys.exc_info()[0]
		print("Error: Failed to extract data archive")
		print("Error: " + str(e))
		cleanup(doCleanup)
		exit()

	cleanup(doCleanup)	
# end: main

if __name__ == "__main__":
	main()
else:
	# do nothing
	pass	
