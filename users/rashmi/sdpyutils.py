#! /usr/bin/env python

#
# sdpyutils.py
#
# Script with a bunch of San Diego specific utility functions
#
# Dependencies: 
#
# Required data files must be in the current working directory 
#

import os
import sys
import csv
import pandas as pd
import numpy as np
import pprint
import re

# current working directory
CWD = os.getcwd()

# field names mapping to the geoid dictionary
GEOID_PARAMS = ['sra','region','zipcodes','zctas']

# data file(s)
# SD county sra, zip, zcta crosswalk (tab-seperated file)
DATAFILE_SD_GEOIDS = 'sd_county_sra_zip_zcta.txt'

# output col names
OUT_COL_SRA = 'SRA'
OUT_COL_Region = 'Region'
OUT_COL_Zipcode = 'Zipcode'
OUT_COL_ZCTA = 'ZCTA'

#
# createGeoidLookup
#
# extracts geo ID information from specified datafile (defaults to SD county geo 
# data file if none specified as input) and returns it as a dictionary with keys
# representing SRA and values comprising of ZCTA, Zipcode lists and Region
# 
def createGeoidLookup(geoid_datafile=None):

	if geoid_datafile is None:
		geoid_datafile = DATAFILE_SD_GEOIDS

	# geoid_lookup dictionary
	geoid_dict = {}	

	try:
		reader = csv.DictReader(open(geoid_datafile), GEOID_PARAMS, delimiter='\t')

		zipcode_list = []
		zcta_list = []

		for row in reader:

			#print row
			key = row['sra']

			# skip header
			if key == 'Sub Regional Area (SRA)':
				#do nothing
				pass
			else:	
				zipcode_list = row['zipcodes'].split(",")
				zcta_list = row['zctas'].split(",")
				value_list = [row['region'],zipcode_list,zcta_list]
				geoid_dict[key] = value_list	
		
	except Exception, e:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			raise e	
	
	#pprint.pprint(geoid_dict)
	return geoid_dict

#
# createGeoidsData
#
# creates a data frame with geoID information extracted from the specified file 
# (defaults to San Diego county geoID data file if none specified)
#
def createGeoidsData(geoid_datafile=None):

	# geoid_lookup dictionary
	geoid_dict = {}	
	df_geoids = None

	try:
		if geoid_datafile is None:
			geoid_dict = createGeoidLookup()
		else:
			geoid_dict = createGeoidLookup(geoid_datafile)

		data = []
		# iterate the dictionary and add data pertaining to the following cols
		# SRA, Region, Zipcode, ZCTA
		for key, val in geoid_dict.iteritems():
			for zipcode, zcta in zip(val[1],val[2]):
				l = [key,val[0],zipcode,zcta]
				data.append(l)
			# add an additional row for each SRA to hold aggregates across 
			# zipcodes	
			l = [key,val[0],'00000','00000']
			data.append(l)

		cols = [OUT_COL_SRA,OUT_COL_Region,OUT_COL_Zipcode,OUT_COL_ZCTA]
		df_geoids = pd.DataFrame(columns=cols,data=data)
					
	except Exception, e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		raise e
	
	#print df_geoids
	return df_geoids

#
# addSRAaggregates
#
# aggregates per zipcode/ZCTA data and populates the unique entry per SRA  with
# the aggreagated values (in the specified data frame) and returns the modified
# data frame
# 
# Note: 
# 1. This requires that data be in a specific format (see df_geoids dataframe)
# 2. Values in targetCols MUST be numeric since the sum operation is applied to
#    them in this function
#       
def addSRAaggregates(df,targetCols):
	
	try:	
		for name, group in df.groupby('SRA'):
				idx = group.last_valid_index()
				#print df.loc[[idx]]

				for col in targetCols:
					df.set_value(idx,col,group[col].sum())

	except Exception, e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		raise e			

	#print df.head()	
	return df			
