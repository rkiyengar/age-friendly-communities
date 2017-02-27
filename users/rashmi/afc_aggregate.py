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
# Required intermediary data files are under the current working directory
#
################################################################################

import os
import sys
import csv
import pandas as pd
import numpy as np
import pprint
from collections import defaultdict
import genutils as genpy
import sdpyutils as sdpy  

#
# GLOBALS 
#

# current working directory
CWD = os.getcwd()

# output data file
OUT_VERSION = '20170227'
OUT_CSV = 'afc' + '_' + OUT_VERSION + ".csv"

# output col names
OUT_COL_SRA = 'SRA'
OUT_COL_Region = 'Region'
OUT_COL_Zipcode = 'Zipcode'
OUT_COL_ZCTA = 'ZCTA'

OUT_COL_NumRCFELicensed = 'NumRCFELicensed'
OUT_COL_NumRCFEBedsLicensed = 'NumRCFEBedsLicensed'
OUT_COL_NumRCFEPending = 'NumRCFEPending'
OUT_COL_NumRCFEBedsPending = 'NumRCFEBedsPending'
OUT_COL_NumRCFEInALWP = 'NumRCFEInALWP'

OUT_COL_2012Pop65Over = '2012Pop65Over'
OUT_COL_2012Pop55Over = '2012Pop55Over'
OUT_COL_2030Pop65Over = '2030Pop65Over'
OUT_COL_2030Pop55Over = '2030Pop55Over'

OUT_COL_2012PopADOD55Over = '2012PopADOD55Over'
OUT_COL_2030PopADOD55Over = '2030PopADOD55Over'
OUT_COL_2012ADODPerRCFE = '2012ADODPerRCFE'
OUT_COL_2030ADODPerRCFE = '2030ADODPerRCFE'

OUT_COL_2012PopLowIncome65Over = '2012PopLowIncome65Over'
OUT_COL_2012PopLowIncome55Over = '2012PopLowIncome55Over'
OUT_COL_2012LowIncome65OverPercentage = "2012PercentLowIncome65Over"
OUT_COL_2012LowIncome55OverPercentage = "2012PercentLowIncome55Over"

OUT_COL_2012LowIncome65OverPerRCFE = '2012LowIncome65OverPerRCFE'
OUT_COL_2012LowIncome55OverADODRatio = '2012LowIncome55OverADODRatio'

OUT_COL_2012PopMinority = '2012PopMinority'
OUT_COL_PopMinorityPerRCFE = 'PopMinorityPerRCFE'
OUT_COL_PopMinorityADODRatio = 'PopMinorityADODRatio'
OUT_COL_2012MedianHHIncome = '2012MedianHHIncome'
OUT_COL_2012MedianHHIncome65Over = '2012MedianHHIncome65Over'

# FIX ME: Make these fields available at some point
'''
OUT_COL_2015Pop65Over = '2015Pop65Over'
OUT_COL_2015Pop55Over = '2015Pop55Over'
OUT_COL_2015PopADOD55Over = '2015PopADOD55Over'
OUT_COL_2015ADODPerRCFE = '2015ADODPerRCFE'
OUT_COL_2015PopLowIncome65Over = '2015PopLowIncome65Over'
OUT_COL_2015PopLowIncome55Over = '2015PopLowIncome55Over'
OUT_COL_2015LowIncome65OverPercentage = "2015PercentLowIncome65Over"
OUT_COL_2015LowIncome55OverPercentage = "2015PercentLowIncome55Over"
OUT_COL_2015LowIncome65OverPerRCFE = '2015LowIncome65OverPerRCFE'
OUT_COL_2015LowIncome55OverADODRatio = '2015LowIncome55OverADODRatio'
OUT_COL_2015PopMinority = '2015PopMinority'
OUT_COL_2015MedianHHIncome = '2015MedianHHIncome'
OUT_COL_2015MedianHHIncome65Over = '2015MedianHHIncome65Over'
'''

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

# SD county current population estimates (yr 2015)/SANDAG
DATAFILE_SD_2015_POP = 'pop_estimate_sd_01112017_A.csv'
# SD county current population estimates (yr 2012)/SANDAG
DATAFILE_SD_2012_POP = 'pop_estimate_sd_county_2012.csv'
# SD county current population forecasts (yr 2030)/SANDAG
DATAFILE_SD_2030_POP = 'pop_forecast_sd_01112017.csv'

# SD county ADOD population; 55 and over
DATAFILE_SD_ADOD_POP_55_OVER = 'SD_County_ADOD_Pop_Data_001.csv' 
# SD county general population; 55 and over (yr 2012) / SD HHSA
DATAFILE_SD_2012_POP_55_OVER = 'SD_County_ADOD_Pop_Data_003.csv' 
# SD county general population; 55 and over (yr 2030) / SD HHSA
DATAFILE_SD_2030_POP_55_OVER = 'SD_County_ADOD_Pop_Data_005.csv' 

# SD county low income population 
DATAFILE_SD_2012_LOW_INCOME_POP_55_OVER = 'low_income_data_sd_county_2012.csv'
# SD county median house-hold income by age
DATAFILE_SD_2012_MEDIAN_HH_INCOME = 'ACS_12_5YR_B19049_with_ann.csv'

#
# parseRCFEList
#
# parses the list of RCFEs and adds relevant data to the data frame that is 
# returned
#
def parseRCFEList(zipdf):

	# these are the fields we are interested in
	FACZIP = 'Facility Zip'
	FACCAP = 'Facility Capacity'
	FACSTATUS = 'Facility Status'
	
	csvdata = pd.read_csv(DATAFILE_SD_RCFE,skipinitialspace=True, 
						usecols=[FACZIP,FACCAP,FACSTATUS])
	#print csvdata.head()
	print("parsing data file: " + DATAFILE_SD_RCFE)

	# iterate through facility zipcode list and total number of rcfe
	# and facility capacity for each unique zipcode
	#
	# NOTE: Only facilities with status 'Licensed' OR 'Pending' are counted
	# Further, seperate counts for only licensed and only pending facilities 
	# are computed since 'Pending' implies pending for a license and applies 
	# to facilities that were previously licensed but are pending a new license
	# for increased capacity (includes those pending for first time licenses)
	#
	# In effect, the licensed only RCFE count is a conservative estimate of the 
	# actual capacity available (or would be available) while the pending-only 
	# RCFE count is an optimistic estimate of the same. 
	#
	rcfe_dict = {}
	for index,row in csvdata.iterrows():
		
		zipcode = row[FACZIP]; capacity = row[FACCAP]
		status = row[FACSTATUS]

		#print "zipcode: {} capacity: {} status: {}".format(zipcode,capacity,status)

		if zipcode in rcfe_dict:
			if status == 'LICENSED':
				rcfe_dict[zipcode][0] += 1
				rcfe_dict[zipcode][1] += capacity
			elif status == 'PENDING':
				rcfe_dict[zipcode][2] += 1
				rcfe_dict[zipcode][3] += capacity	
			else: pass	
		else:
			if status == 'LICENSED':
				rcfe_dict[zipcode] = [1,capacity,0,0]				
			elif status == 'PENDING':
				rcfe_dict[zipcode] = [0,0,1,capacity]
			else: pass
			

	#pprint.pprint(rcfe_dict)
			
	data = []		
	for zipcode in zipdf:
		if int(zipcode) in rcfe_dict:
			data.append([rcfe_dict[int(zipcode)][0],rcfe_dict[int(zipcode)][1],
						rcfe_dict[int(zipcode)][2],rcfe_dict[int(zipcode)][3]])
		else:
			data.append([0,0,0,0])
	
	out_cols = [OUT_COL_NumRCFELicensed,OUT_COL_NumRCFEBedsLicensed,
				OUT_COL_NumRCFEPending,OUT_COL_NumRCFEBedsPending]		
	df = pd.DataFrame(columns=out_cols,data=data)
	#print df.head()
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
	print("parsing data file: " + DATAFILE_SD_RCFE_IN_ALWP)

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

	df = pd.DataFrame(columns=[OUT_COL_NumRCFEInALWP],data=data)
	return df

'''
# currently unused
#
# parsePopulation
#
# parses current population (2015) and future estimates (2030) for seniors 
# in the county (per SRA) and adds columns specific to the same. 
#
# this version extracts data from the SANDAG dataset. 
# 
def parsePopulation(srazipdf,datafile,out_cols):
	
	# these are the fields we are interested in
	USECOLS=['SRA','TYPE','80+','70-79','60-69']

	csvdata = pd.read_csv(datafile,skipinitialspace=True, 
						usecols=USECOLS)

	#print csvdata	
	print("parsing data file: " + datafile)

	pop_dict = {}
	for index, row in csvdata.iterrows():
		sra = row['SRA']
		# we only care about total counts not splits by gender
		if row['TYPE'] != 'Total':
			continue
		else:
			pop_dict[sra] = int(row['80+'] + row['70-79'] + row['60-69'])
	
	#pprint.pprint(pop_dict)

	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((int(zipcode) == 0) and (sra in pop_dict)):
			data.append(pop_dict[sra])
		else:
			data.append(0)
				
	df = pd.DataFrame(columns=out_cols,data=data)
	return df
'''

#
# parsePopulation_v2
#
# this version of the function extracts current (2012) and future (2030)
# projections for population per SRA from the San Diego HHSA dataset
#
# NOTE: the reason for using this dataset for the general population has to do 
# with the fact that this dataset also consists of ADOD population nos for the 
# same SRAs. Hence any forecasting models used to arrive at two would be 
# consistent - resulting in an accurate representaton of the percentage of ADOD
# in the general population
#
def parsePopulation_v2(srazipdf,datafile,cols):

	USECOLS = ['SRA','55-64','65-74','75-84','85 and Over','55 and Over']
	
	csvdata  = pd.read_csv(datafile,skipinitialspace=True,skiprows=1,
				usecols=USECOLS)
	#print csvdata
	print("parsing data file: " + datafile)

	pop_dict = {}
	for index, row in csvdata.iterrows():
		sra = row['SRA']
		
		pop_65_over = int(row['65-74'].replace(",","")) + \
					int(row['75-84'].replace(",","")) + \
					int(row['85 and Over'].replace(",",""))
	
		pop_55_over = int(row['55 and Over'].replace(",","")) 
		
		pop_dict[sra] = [pop_65_over,pop_55_over]
	
	#pprint.pprint(pop_dict)							

	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((int(zipcode) == 0) and (sra in pop_dict)):
			data.append([pop_dict[sra][0],pop_dict[sra][1]])
		else:
			data.append([0,0])
				
	df = pd.DataFrame(columns=cols,data=data)
	return df

#
# parseADODPopulation
#
# Extracts ADOD population data for 55 Over from San Diego HHSA dataset
#
def parseADODPopulation(srazipdf):
  
  	USECOLS = ['SRA','2012','2030']
	out_cols = [OUT_COL_2012PopADOD55Over,OUT_COL_2030PopADOD55Over]

	#FIXME: results in XLRD Error
  	#sheet = "Page_1_1"    
	#xl = pd.ExcelFile(DATAFILE_SD_ADOD_POP_55_OVER)
	#xldata = xl.parse(sheet) 

	csvdata  = pd.read_csv(DATAFILE_SD_ADOD_POP_55_OVER,skipinitialspace=True,
					skiprows=1,usecols=USECOLS)
	#print csvdata
	print("parsing data file: " + DATAFILE_SD_ADOD_POP_55_OVER)

	adod_dict = csvdata.set_index('SRA').T.to_dict('list')	
	#pprint.pprint(adod_dict)
		
	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((int(zipcode) == 0) and (sra in adod_dict)):
			adod_pop_curr = (adod_dict[sra][0]).replace(",","")
			adod_pop_2030 = (adod_dict[sra][1]).replace(",","")
			data.append([adod_pop_curr,adod_pop_2030]) 
		else:
			data.append([0,0])	

	df = pd.DataFrame(columns=out_cols,data=data)
	return df	

#
# parseLowIncomePopulation
#
# extracts low income senior population counts from the specified data file
# 
def parseLowIncomePopulation(srazipdf,datafile,cols):

	USECOLS = ['SRA','Zipcode','55 and Over (Low Income)','65 and Over (Low Income)']
	
	csvdata  = pd.read_csv(datafile,skipinitialspace=True,usecols=USECOLS)
	#print csvdata
	print("parsing data file: " + datafile)

	# we need a nested dictionary for lookup since there is no single unique key
	# rather, the key is a combination of SRA and zipcode
	income_dict = defaultdict(lambda: defaultdict(dict))
	
	for index, row in csvdata.iterrows():
		income_dict[row['SRA']][int(row['Zipcode'])] = \
				[row['55 and Over (Low Income)'],row['65 and Over (Low Income)']]

	#pprint.pprint(income_dict)
		
	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((sra in income_dict) and (int(zipcode) in income_dict[sra])):
			pop_55_over = (income_dict[sra][int(zipcode)][0])
			pop_65_over = (income_dict[sra][int(zipcode)][1])
			data.append([int(pop_55_over),int(pop_65_over)]) 
		else:
			data.append([0,0])	

	df = pd.DataFrame(columns=cols,data=data)
	return df	

#
# parseMinorityPopulation
#
# extracts population counts for minorities from the SANDAG population estimate
# dataset
#
def parseMinorityPopulation(srazipdf,datafile,cols):

	USECOLS=['SRA','TYPE','Two or More','Other','Pacific Islander','Asian',
			'American Indian','Black','White','Hispanic']

	csvdata = pd.read_csv(datafile,skipinitialspace=True, 
						usecols=USECOLS)
	#print csvdata.head()
	print("parsing data file: " + datafile)	

	nonWhiteCols = USECOLS[2:-2] + [USECOLS[-1]]
	
	pop_dict = {}
	for index, row in csvdata.iterrows():
		sra = row['SRA']
		if row['TYPE'] != 'Total':
			continue
		else:		
			# for now we are only considering non-white populations of single ethnicity
			# i.e.: populations with two or more ethnicities (one of which may be white)
			# are not accounted for in minority_pop
			minority_pop = (row[nonWhiteCols[1:]].apply(pd.to_numeric)).sum()
			#minority_pop_multi_ethnic = (row[nonWhiteCols].apply(pd.to_numeric)).sum()

			pop_dict[sra] = minority_pop
			#pop_dict[sra] = [minority_pop,minority_pop_multi_ethnic]
	
	#pprint.pprint(pop_dict)

	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((int(zipcode) == 0) and (sra in pop_dict)):
			data.append(pop_dict[sra])
		else:
			data.append(0)
				
	df = pd.DataFrame(columns=cols,data=data)
	#print df.head()
	return df

#
# parseMedianIncome
# 
# extracts the median household income information from the specified datafile
# 
def parseMedianHHIncome(df_geoids,datafile,cols):

	# index of GEO.id2 which contains ZCTA as numbers
	COL_ZCTA_IDX = 1
	COL_ZCTA = 'GEO.id2'
	COL_TOTAL = 'HD01_VD02'
	COL_65_OVER = 'HD01_VD06'
	
	USECOLS = [COL_ZCTA,COL_TOTAL,COL_65_OVER]
	
	csvdata = pd.read_csv(datafile,skipinitialspace=True,usecols=USECOLS)
	#print csvdata.head()
	print("parsing data file: " + datafile)
	
	# modify col names to be more readable
	csvdata.columns = [COL_ZCTA] + cols
	
	# merge data as per SRA/Zipcodes specified in df_geoids
	df_mi = pd.merge(left=df_geoids,right=csvdata[1:],left_on='ZCTA',
					right_on=COL_ZCTA,how='left').fillna(0)
	df_mi.drop(COL_ZCTA,axis=1,inplace=True)

	# convert the cols to numeric (needed for aggregation) 
	tmp_df = df_mi[cols].applymap(genpy.to_stringnum)
	tmp_df = tmp_df.applymap(pd.to_numeric)
	df_mi = pd.concat([df_geoids,tmp_df],axis=1)

	# aggregate numbers for each SRA
	df_mi = df_mi[['SRA','Zipcode']+cols]
	df_mi = sdpy.addSRAaggregates(df_mi,cols)

	#print df_mi
	
	# we need a nested dictionary for lookup since there is no single unique key
	# rather, the key is a combination of SRA and zipcode
	mi_dict = defaultdict(lambda: defaultdict(dict))

	for index, row in df_mi.iterrows():
		mi_dict[row['SRA']][int(row['Zipcode'])] = \
				[int(row[cols[0]]),int(row[cols[1]])]

	#pprint.pprint(mi_dict)
	
	srazipdf = df_geoids[['SRA','Zipcode']]
	data = []
	for sra,zipcode in srazipdf.itertuples(index=False):
		if ((sra in mi_dict) and (int(zipcode) in mi_dict[sra])):
			mi_hh = (mi_dict[sra][int(zipcode)][0])
			mi_hh_65_over = (mi_dict[sra][int(zipcode)][1])
			data.append([int(mi_hh),int(mi_hh_65_over)]) 
		else:
			data.append([0,0])
				
	df = pd.DataFrame(columns=cols,data=data)
	#print df.head()
	
	return df
	
################################################################################
# 
# MAIN
#
def main():
	try:
		# create geoIds (SRA, Region, Zipcode,ZCTA) data set relevant to the 
		# San Diego county
		df_geoids = sdpy.createGeoidsData()
		#print df_geoids
		geoCols = df_geoids.columns.tolist()
		
		# extract the zipcodes and SRAs for which we need to parse additional 
		# data
		zipdf = df_geoids['Zipcode']
		sradf = df_geoids['SRA'].where(df_geoids['Zipcode'] == 0)
		srazipdf = df_geoids[['SRA','Zipcode']]

		# add data pertaining to specified cols

		# NumRCFELicensed, NumRCFEBedsLicensed,
		# NumRCFEPending, NumRCFEBedsPending
		df_rcfe = parseRCFEList(zipdf)  	

		# NumRCFEInALWP
		df_alwp = parseRCFEInALWP(zipdf)

		# 2012Pop65Over, 2012Pop55Over
		out_cols = [OUT_COL_2012Pop65Over,OUT_COL_2012Pop55Over]
		df_pop_sr_2012 = parsePopulation_v2(srazipdf,DATAFILE_SD_2012_POP_55_OVER,
							out_cols)

		# 2030Pop65Over, 2030Pop55Over
		out_cols = [OUT_COL_2030Pop65Over,OUT_COL_2030Pop55Over] 
		df_pop_sr_2030 = parsePopulation_v2(srazipdf,DATAFILE_SD_2030_POP_55_OVER,
							out_cols)

		# 2012PopADOD55Over, 2030PopADOD55Over
		df_pop_adod = parseADODPopulation(srazipdf)

		# 2012PopLowIncome55Over, 2012PopLowIncome65Over
		out_cols = [OUT_COL_2012PopLowIncome55Over,OUT_COL_2012PopLowIncome65Over]
		df_pop_li = parseLowIncomePopulation(srazipdf,
						DATAFILE_SD_2012_LOW_INCOME_POP_55_OVER,out_cols)

		# 2012PopMinority
		out_cols = [OUT_COL_2012PopMinority]
		df_pop_min_2012 = parseMinorityPopulation(srazipdf,
								DATAFILE_SD_2012_POP,out_cols)

		# add following fields (data to be derived later)
		# 2012PercentLowIncome65Over, 2012PercentLowIncome55Over
		# 2012ADODPerRCFE, 2030ADODPerRCFE
		# 2012LowIncome65OverPerRCFE, 2012LowIncome55OverADODRatio
		# PopMinorityPerRCFE, PopMinorityADODRatio
		df_derived = pd.DataFrame(columns=[OUT_COL_2012ADODPerRCFE,
						OUT_COL_2030ADODPerRCFE,
						OUT_COL_2012LowIncome65OverPercentage,
						OUT_COL_2012LowIncome55OverPercentage,
						OUT_COL_2012LowIncome65OverPerRCFE,
						OUT_COL_2012LowIncome55OverADODRatio, 
						OUT_COL_PopMinorityPerRCFE,
						OUT_COL_PopMinorityADODRatio],
						data=np.zeros(shape=(len(zipdf.index),8))) 

		# 2012MedianHHIncome, 2012MedianHHIncome65Over 
		out_cols = [OUT_COL_2012MedianHHIncome, OUT_COL_2012MedianHHIncome65Over]	
		df_hh_mi = parseMedianHHIncome(df_geoids,
								DATAFILE_SD_2012_MEDIAN_HH_INCOME,out_cols)

		# concatenate the intermediate results into a single dataframe
		out_df = pd.concat([df_geoids,df_rcfe,df_alwp,df_pop_sr_2012, 
						df_pop_sr_2030, df_pop_adod, df_pop_li, 
						df_pop_min_2012, df_derived, df_hh_mi],axis=1)

		#print(out_df.head())

		# Add aggregated counts (per SRA) for derived fields 
		for name, group in out_df.groupby(OUT_COL_SRA):
			
			idx = group.last_valid_index()
			
			#print out_df.loc[[idx]]

			total_rcfe_licensed = group[OUT_COL_NumRCFELicensed].sum()
			total_capacity_licensed = group[OUT_COL_NumRCFEBedsLicensed].sum()
			total_rcfe_pending = group[OUT_COL_NumRCFEPending].sum()
			total_capacity_pending = group[OUT_COL_NumRCFEBedsPending].sum()

			total_in_alwp = group[OUT_COL_NumRCFEInALWP].sum()

			out_df.set_value(idx,OUT_COL_NumRCFELicensed,total_rcfe_licensed)
			out_df.set_value(idx,OUT_COL_NumRCFEBedsLicensed,total_capacity_licensed)
			out_df.set_value(idx,OUT_COL_NumRCFEPending,total_rcfe_pending)
			out_df.set_value(idx,OUT_COL_NumRCFEBedsPending,total_capacity_pending)

			out_df.set_value(idx,OUT_COL_NumRCFEInALWP,total_in_alwp)

			adod_rcfe_ratio_2012 = float(999)
			adod_rcfe_ratio_2030 = float(999)
			minorities_per_rcfe = float(999)
			minorities_adod_ratio = float(999)
			low_income_65_over_per_rcfe = float(999)
			low_income_55_over_adod_ratio = float(999)
			percent_low_income_55_over = float(999)
			percent_low_income_65_over = float(999)	

			# note: this try/catch is here to deal gracefully with errors 
 			# arising from non-integer population counts (e.g.: "<5")
 			try:
 				adod_pop_2012 = int(out_df.get_value(idx,OUT_COL_2012PopADOD55Over))
 				adod_pop_2030 = int(out_df.get_value(idx,OUT_COL_2030PopADOD55Over))
 				
 				minority_pop_2012 = int(out_df.get_value(idx,OUT_COL_2012PopMinority))

 				li_65_over = out_df.get_value(idx,OUT_COL_2012PopLowIncome65Over)
 				li_55_over = out_df.get_value(idx,OUT_COL_2012PopLowIncome55Over)

 				pop_65_over = out_df.get_value(idx,OUT_COL_2012Pop65Over)
 				pop_55_over = out_df.get_value(idx,OUT_COL_2012Pop55Over)

 				if (total_rcfe_licensed > 0):
 					adod_rcfe_ratio_2012 = float(adod_pop_2012)/total_rcfe_licensed
					adod_rcfe_ratio_2030 = float(adod_pop_2030)/total_rcfe_licensed
					minorities_per_rcfe = float(minority_pop_2012)/total_rcfe_licensed
					low_income_65_over_per_rcfe = float(li_65_over)/total_rcfe_licensed

				if (adod_pop_2012 > 0):					
					low_income_55_over_adod_ratio = float(li_55_over)/adod_pop_2012
					minorities_adod_ratio = float(minority_pop_2012)/adod_pop_2012

				if (pop_55_over > 0):
					percent_low_income_55_over = (float(li_55_over)/pop_55_over)*100
				if (pop_65_over > 0):
					percent_low_income_65_over = (float(li_65_over)/pop_65_over)*100		
					
 			except Exception, e:
 				#e = sys.exc_info()[0]
				#print("Error: " + str(e))
 				# do nothing => ratios will default to 999.0
 				pass

 			out_df.set_value(idx,OUT_COL_2012LowIncome65OverPercentage,
 								round(percent_low_income_65_over,2))	
 			out_df.set_value(idx,OUT_COL_2012LowIncome55OverPercentage,
 								round(percent_low_income_55_over,2))		
			out_df.set_value(idx,OUT_COL_2012ADODPerRCFE,round(adod_rcfe_ratio_2012,2))
			out_df.set_value(idx,OUT_COL_2030ADODPerRCFE,round(adod_rcfe_ratio_2030,2))
			out_df.set_value(idx,OUT_COL_2012LowIncome55OverADODRatio,
								round(low_income_55_over_adod_ratio,2))
			out_df.set_value(idx,OUT_COL_2012LowIncome65OverPerRCFE,
								round(low_income_65_over_per_rcfe,2))
			out_df.set_value(idx,OUT_COL_PopMinorityPerRCFE,
								round(minorities_per_rcfe,2))
			out_df.set_value(idx,OUT_COL_PopMinorityADODRatio,
								round(minorities_adod_ratio,2))

			#print out_df.loc[[idx]]

		# remove output file if it already exists
		if os.path.exists(os.path.join(CWD,OUT_CSV)):
			os.remove(os.path.join(CWD,OUT_CSV))

		#print(out_df.head())	
		out_df.to_csv(OUT_CSV, index=False)
		print("output: " + OUT_CSV)

	except: 
		e = sys.exc_info()[0]
		print("Error: Failed to create " + OUT_CSV)
		print("Error: " + str(e))
		exit()
# end: main

if __name__ == "__main__":
	main()
else:
	# do nothing
	pass	
