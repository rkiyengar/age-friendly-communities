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

# current working directory
CWD = os.getcwd()

# field names mapping to the geoid dictionary
GEOID_PARAMS = ['sra','region','zipcodes','zctas']

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

