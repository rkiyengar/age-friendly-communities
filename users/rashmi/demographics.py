#!/usr/bin/env python

#
# demographics.py
#
# Script to read in multiple excel files with demographics information
# specific to an SRA (Sub-Regional Area) and collate them into a single 
# CSV file representing demographics for the entire county
#

import os
import sys
import shutil
import pandas as pd
import numpy as np
from zipfile import ZipFile

# Create a temp directory (under the current working directory for data 
# downloads
CLEANUP = False

cwd = os.getcwd()
tmpdir = os.path.join(cwd,"tmp")

#print tmpdir

if not os.path.exists(tmpdir):
	os.makedirs(tmpdir)	
	CLEANUP = True	
#
# Removes the temp directory and its contents
#
def cleanup():
	# Cleanup the temp directory only if we created it here
	if CLEANUP:
		if os.path.exists(tmpdir):
			shutil.rmtree("tmp")
	

# FIXME: Add support for downloading archives from the cloud 
FCAST_DATAURL="https://goo.gl/hkOcO8"
EST_DATAURL=""
CENSUS_DATAURL=""

# For now, assume archives are present in current working dir

# change DATAID to collate files from desired archive
#DATAID="pop_forecast"
#DATAID="pop_estimate"
DATAID="pop_census"

GEOID="sd"
VER="01112017"
EXT="zip"

datafile = DATAID + "_" + GEOID + "_" + VER + "." + EXT
datadir = os.path.join(tmpdir,DATAID)

try:
	zipf = ZipFile(os.path.join(cwd,datafile),'r')
	zipf.extractall(tmpdir)
	zipf.close()
except:
	e = sys.exc_info()[0]
	print("Error: Failed to extract data archive")
	print("Error: " + str(e))
	cleanup()
	exit()

# col names to use in the collated data
COLS = ['SRA','YEAR','TYPE','80+','70-79','60-69','50-59','40-49',
	'30-39','20-29','10-19','Under 10']

#
# Takes an SRA specific Excel file, parses it to find data specific to
# desired year. Further, it converts the data into wide format (from a long 
# one) and outputs the result in a data-frame
#
def parseFile(fname):
        SHEET = "Age"
	xl = pd.ExcelFile(fname)
        df = xl.parse(SHEET)

        sra = df.ix[0,'SRA']
	print("Parsing data for SRA: " + sra + "\n")

	# subset it to select only years we care about
	YR = 2010
	if DATAID == "pop_forecast":
		YR = 2030
	elif DATAID == "pop_estimate":
		YR == 2015
	else: #DATAID == "pop_census"
		YR == 2010

	df_m = df[(df['YEAR'] == YR) & (df['SEX'] == 'Male')]
	df_f = df[(df['YEAR'] == YR) & (df['SEX'] == 'Female')]

        # convert data to wide format
        # transpose the age and population cols
        df_m = df_m[['Group - 10 Year','POPULATION']]
	df_m = df_m.reset_index(drop=True)
	df_f = df_f[['Group - 10 Year','POPULATION']]
	df_f = df_f.reset_index(drop=True)
       
        df_m = df_m.T
	df_f = df_f.T
      
        # create a data frame with transposed data and known cols
	pop_m = df_m.loc['POPULATION',:].values.tolist()
	pop_f = df_f.loc['POPULATION',:].values.tolist()
	pop_tot = [x + y for x, y in zip(pop_m, pop_f)]
        
	data = [[sra,YR,'Male'] + pop_m, [sra,YR,'Female'] + pop_f, [sra,YR,'Total',] + pop_tot]

	newdf = pd.DataFrame(columns=COLS,data=data)

	return newdf
#
# Iterate through extracted files and collate data
#
OUT_CSV=DATAID + "_" + GEOID + "_" + VER + "." + 'csv'

df_full = pd.DataFrame()
df_concat_list = []

try:
	for f in os.listdir(datadir):
		if f.endswith(".xlsx"):
			#print(f)
		        df = parseFile(os.path.join(datadir,f))
			#print(df.head())
                	df_concat_list.append(df)
		else:
			continue
except:
	e = sys.exc_info()[0]
	print("Error: Failed to create data CSV")
	print("Error: " + str(e))
	cleanup()
	exit()

# collate the data and write it out to a CSV file
df_full = pd.concat(df_concat_list,axis=0)

if os.path.exists(os.path.join(cwd,OUT_CSV)):
	os.remove(os.path.join(cwd,OUT_CSV))

df_full.to_csv(OUT_CSV, index=False)

cleanup()
