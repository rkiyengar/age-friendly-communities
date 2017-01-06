"""
@author: David Albrecht
anaconda 4.2.13
python 3.5.2
pandas 0.19.1

Dataset Name:
    CDSS RCFE List - https://secure.dss.ca.gov/CareFacilitySearch/Home/DownloadData

Purpose:
    Answer the first two bullet points within the first question block, "RCFE Capacity":
        1) What is the number of RCFEs in a given community as defined above?
            Answer stored in count_RCFEs dictionary
        2) What is the capacity (by licensed bed) in a given community?
            Answer stored in capacity_RCFEs dictionary

Notes:
    1) Manually deleted columns V and onward since they created errors when reading into a df and are not relevant
    2) Filtered the df to exclude any facility that is in closed, pending, or unlicensed status
    3) INFO: Zillow lists 132 different zip codes in San Diego County
"""

#import necessary libraries
import pandas as pd
import os

#change working directory
os.chdir('')

#create dataframe and filter for RCFEs only in San Diego County
q1_df = pd.read_csv('ResidentialElderCareFacility01012017.csv')
q1_df = q1_df[q1_df['County Name'] == 'SAN DIEGO']
q1_df = q1_df[q1_df['Facility Status'] != 'CLOSED']
q1_df = q1_df[q1_df['Facility Status'] != 'PENDING']
q1_df = q1_df[q1_df['Facility Status'] != 'UNLICENSED']

#create a list of unique zipcodes within San Diego County
unique_zips = list(q1_df['Facility Zip'].unique())

#create two empty dictionaries using the list of unique zipcodes
count_RCFEs = {}
capacity_RCFEs = {}
for unique_zipcode in unique_zips:
    count_RCFEs[unique_zipcode] = 0
    capacity_RCFEs[unique_zipcode] = 0

#find the number of RCFEs in each zipcode: count_RCFEs
#find the capacity within each zipcode: capacity_RCFEs
for unique_zipcode in unique_zips:
    for all_zipcode in q1_df['Facility Zip']:
        if unique_zipcode == all_zipcode:
            
            count_RCFEs[unique_zipcode] += 1
            
            temp_df = q1_df[q1_df['Facility Zip'] == unique_zipcode]
            capacity_RCFEs[unique_zipcode] = temp_df['Facility Capacity'].sum()
