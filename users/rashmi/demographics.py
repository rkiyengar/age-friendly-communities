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
# For now, assume archives are present in current working dir

# change DATAID to collate files from desired archive
#DATAID="pop_forecast"
DATAID="pop_estimate"
#DATAID="pop_census"

GEOID="sd"
#VER="01112017"
VER="02062017"
EXT="zip"

# data file(s)
datafile = DATAID + "_" + GEOID + "_" + VER + "." + EXT
datadir = os.path.join(tmpdir,DATAID)

try:

	zipf = ZipFile(os.path.join(cwd,datafile),'r')
	zipf.extractall(tmpdir)
	zipf.close()
	print("datafile: " + datafile)
except:
	e = sys.exc_info()[0]
	print("Error: Failed to extract data archive")
	print("Error: " + str(e))
	cleanup()
	exit()

# col names to use in the collated data
AGE_COLS = ['SRA','YEAR','TYPE','80+','70-79','60-69','50-59','40-49',
	'30-39','20-29','10-19','Under 10']
RACE_COLS = ['Two or More','Other','Pacific Islander','Asian',
             'American Indian','Black','White','Hispanic']

#
# parseRace
#
# Takes an SRA specific Excel file, parses it to find ethnicity data specific 
# to desired year. Further, it converts the data into wide format (from a long 
# one) and outputs the result in a data-frame
#
def parseRace(fname,year):
        SHEET = "Ethnicity"
	xl = pd.ExcelFile(fname)
        df = xl.parse(SHEET)

        sra = df.ix[0,'SRA']
	#print("Parsing Race data for SRA: " + sra + "\n")

	df_r = df[(df['YEAR'] == year)]

        # convert data to wide format
        # transpose the age and population cols
        df_r = df_r[['ETHNICITY','POPULATION']]
	df_r = df_r.reset_index(drop=True)
       
        df_r = df_r.T
      
        # create a data frame with transposed data and known cols
	pop_tot = df_r.loc['POPULATION',:].values.tolist()
	pop_m = [0] * len(pop_tot)
	pop_f = [0] * len(pop_tot) 
        
	data = [pop_m, pop_f, pop_tot]

	newdf = pd.DataFrame(columns=RACE_COLS,data=data)

	return newdf

#
# parseAge
#
# Takes an SRA specific Excel file, parses it to find agre-group data specific
# to desired year. Further, it converts the data into wide format (from a long 
# one) and outputs the result in a data-frame
#
def parseAge(fname,year):

	SHEET = "Age"
	xl = pd.ExcelFile(fname)
	df = xl.parse(SHEET)

	sra = df.ix[0,'SRA']
	#print("Parsing Age data for SRA: " + sra + "\n")

	df_m = df[(df['YEAR'] == year) & (df['SEX'] == 'Male')]
	df_f = df[(df['YEAR'] == year) & (df['SEX'] == 'Female')]

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
        
	data = [[sra,year,'Male'] + pop_m, [sra,year,'Female'] + pop_f, [sra,year,'Total',] + pop_tot]

	newdf = pd.DataFrame(columns=AGE_COLS,data=data)
	#print newdf.head()
	return newdf


# output file(s)
OUT_CSV=DATAID + "_" + GEOID + "_" + VER + "." + 'csv'

#
# Iterate through extracted files and collate data
#

df_full = pd.DataFrame()
df_age_concat_list = []; df_race_concat_list = []

try:
	for f in os.listdir(datadir):
		if f.endswith(".xlsx"):
			#print(f)

			# subset it to select only years we care about
			year = 2010
			if DATAID == "pop_forecast":
				year = 2030
			elif DATAID == "pop_estimate":
				#year = 2015
				year = 2012
			else: #DATAID == "pop_census"
				year = 2010

			df_age = parseAge(os.path.join(datadir,f),year)
			#print(df_age.head())

			df_age_concat_list.append(df_age)

			# parse ethnicity for current year estimate
			if DATAID == "pop_estimate":
				df_race = parseRace(os.path.join(datadir,f),year)
				#print(df_race.head())
				df_race_concat_list.append(df_race)
		else:
			continue
except:
	e = sys.exc_info()[0]
	print("Error: Failed to create data CSV")
	print("Error: " + str(e))
	cleanup()
	exit()

# collate the data and write it out to a CSV file

if DATAID == "pop_estimate":
	df1 = pd.concat(df_age_concat_list,axis=0)
	df2 = pd.concat(df_race_concat_list,axis=0)
	df_full = pd.concat([df1,df2],axis=1)
else:
	df_full = pd.concat(df_age_concat_list,axis=0)

if os.path.exists(os.path.join(cwd,OUT_CSV)):
	os.remove(os.path.join(cwd,OUT_CSV))

df_full.to_csv(OUT_CSV, index=False)
print("output: " + OUT_CSV) 

cleanup()
