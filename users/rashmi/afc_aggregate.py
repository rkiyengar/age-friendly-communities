#! /usr/bin/env python

################################################################################
#
# afc_aggregate.py
#
# Script to aggregate data from multiple intermediary data files (CSV) to 
# create a single datafile (in CSV format) containing all the relevant 
# information pertaining to the Age Friendly Communities project. 
#
# Note: SD refers to San Diego, CA in the rest of this file
#
# Usage: 
#
# python afc_aggregate.py
#  
# Dependencies: 
#
# Required intermediary files are under the current working directory
#
################################################################################

import os
import sys
import csv
import pandas as pd
import pprint

#
# GLOBALS 
#

# current working directory
CWD = os.getcwd()

# output data file
OUT_VERSION = '20170125'
OUT_CSV = 'afc' + '_' + OUT_VERSION + ".csv"

OUT_COLS = ['SRA','Region','Zipcode','ZCTA','NumRCFE','NumRCFEBeds',
			'NumRCFEInALWP','2015Pop65Over','2030Pop65Over','2015PopADOD',
			'2030PopADOD','ADODPerRCFE','LowIncPop65Over','LowIncSeniorsPerRCFE',
			'LowIncSeniorsADODRatio','2015PopMinority','2030PopMinority',
			'PopMinorityPerRCFE','PopMinorityADODRatio','MedianIncome']

# field names mapping to the geoid dictionary
GEOID_PARAMS = ['sra','region','zipcodes','zctas']

# output data frame (that is eventually written to file)
out_df = pd.DataFrame()

# Datafiles 

# SD county sra, zip, zcta crosswalk (tab-seperated file)
DATAFILE_SD_GEOIDS = 'sd_county_sra_zip_zcta.txt'
# SD county RCFE list
DATAFILE_SD_RCFE = 'rcfe_sd_county_01012017.csv'
# SD county RCFEs in ALWP 
DATAFILE_SD_RCFE_IN_ALWP = 'rcfe_in_alwp_sd_county_12302016.csv'

#
# createGeoidLookup
#
def createGeoidLookup():

	reader = csv.DictReader(open(DATAFILE_SD_GEOIDS), GEOID_PARAMS, delimiter='\t')

	zipcode_list = []
	zcta_list = []

    # geoid_lookup dictionary
	geoid_dict = {}

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
			
	#pprint.pprint(geoid_dict)
	return geoid_dict
	
#
# parseRCFEList
#
# parses the list of RCFEs and adds relevant data to the data frame that is 
# passed in
#
def parseRCFEList(zipdf):

	# these are the fields we are interested in
	FACZIP = 'Facility Zip'
	FACCAP = 'Facility Capacity'
	
	csvdata = pd.read_csv(DATAFILE_SD_RCFE,skipinitialspace=True, 
						usecols=[FACZIP,FACCAP])

	#print csvdata

	# iterate through facility zipcode list and total number of rcfe
	# and facility capacity for each unique zipcode
	rcfe_dict = {}
	for zipcode, capacity in csvdata.itertuples(index=False):
		if zipcode in rcfe_dict:
			rcfe_dict[zipcode][0] += 1
			rcfe_dict[zipcode][1] += capacity
		else:
			rcfe_dict[zipcode] = [1,capacity]

	#pprint.pprint(rcfe_dict)
			
	data = []		
	for zipcode in zipdf:
		if int(zipcode) in rcfe_dict:
			data.append([rcfe_dict[int(zipcode)][0], rcfe_dict[int(zipcode)][1]])
		else:
			data.append([0,0])
			
	df = pd.DataFrame(columns=OUT_COLS[4:6],data=data)
	return df				

#
# parseRCFEInALWP
#
# parses the data file listing RCFEs in the ALWP program and adds a column 
# indicating the same 
#
def parseRCFEInALWP(zipdf):

	# these are the fields we are interested in
	ZIPCODE = 'Zip Code'

	csvdata = pd.read_csv(DATAFILE_SD_RCFE_IN_ALWP,skipinitialspace=True, 
						usecols=[ZIPCODE])

	#print csvdata

	# iterate through the zipcodes
	alwp_dict = {}
	for index, zipcode in csvdata.iterrows():
		if int(zipcode) in alwp_dict:
			alwp_dict[int(zipcode)] += 1
		else:
			alwp_dict[int(zipcode)] = 1

	#pprint.pprint(alwp_dict)

	data = []		
	for zipcode in zipdf:
		if int(zipcode) in alwp_dict:
			data.append(alwp_dict[int(zipcode)])
		else:
			data.append(0)

	df = pd.DataFrame(columns=[OUT_COLS[6]],data=data)
	return df

#
# parsePopulation
#
# parses current population (2015) and future estimates (2030) for seniors 
# in the county (per SRA) and adds columns specific to the same.
# 
def parsePopulation(zipdf):
	#TBD
	pass

#
# Main
#
# Function invoked from the top-level in this script
# 
def main():
	try:

		# create geoids lookup 
		geoid_dict = createGeoidLookup()

		data = []
		# iterate the dictionary and add data pertaining to the following cols
		# SRA, Region, Zipcode, ZCTA
		for key, val in geoid_dict.iteritems():
			for zipcode, zcta in zip(val[1],val[2]):
				l = [key,val[0],zipcode,zcta]
				#print l
				data.append(l)
			# add an additional row for each SRA to hold aggregates across 
			# zipcodes	
			l = [key,val[0],'00000','00000']
			data.append(l)

		cols = OUT_COLS[0:4]
		df_geoids = pd.DataFrame(columns=cols,data=data)
		#print df_geoids

		# extract the zipcodes and SRAs for which we need to parse additional 
		# data
		zipdf = df_geoids['Zipcode']
		sradf = df_geoids['SRA']

		# add data pertaining to the following cols
		# NumRCFE, NumRCFEBeds
		df_rcfe = parseRCFEList(zipdf)  	

		# add data pertaining to the following cols
		# NumRCFEInALWP
		df_alwp = parseRCFEInALWP(zipdf)

        # add data pertaining to the following cols
        # 2015Pop65Over, 2030Pop65Over

        # TBD
 		# df_pop_sr = parsePopulation(zipdf)

        # concatenate the intermediate results into a single dataframe
		out_df = pd.concat([df_geoids,df_rcfe,df_alwp],axis=1)

		# Add aggregated counts (per SRA) for the following fields
		# OUT_COL[0]: SRA OUT_COL[4]: 'NumRCFE' OUT_COL[5]: 'NumRCFEBeds' 
		# OUT_COL[6]: 'NumRCFEInALWP'
		for name, group in out_df.groupby(OUT_COLS[0]):
			#print name
			total_rcfe = group[OUT_COLS[4]].sum()
			total_capacity = group[OUT_COLS[5]].sum()
			total_in_alwp = group[OUT_COLS[6]].sum()

			#print(str(total_rcfe)  + " " 
			#	   + str(total_capacity) + " " 
			#	   + str(total_in_alwp))
			
			idx = group.last_valid_index()
			out_df.set_value(idx,OUT_COLS[4],total_rcfe)
			out_df.set_value(idx,OUT_COLS[5],total_capacity)
			out_df.set_value(idx,OUT_COLS[6],total_in_alwp)

		# remove if file already exists
		if os.path.exists(os.path.join(CWD,OUT_CSV)):
			os.remove(os.path.join(CWD,OUT_CSV))

		out_df.to_csv(OUT_CSV, index=False)
	except: 
		e = sys.exc_info()[0]
		print("Error: Failed to create " + OUT_CSV)
		print("Error: " + str(e))
		exit()

################################################################################
# 
# MAIN
#
if __name__ == "__main__":
	main()
else:
	# do nothing
	pass	
