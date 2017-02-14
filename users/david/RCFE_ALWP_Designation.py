"""
@author: David Albrecht
anaconda 4.2.13
python 3.5.2
pandas 0.19.1

Dataset Name:
    RCFE ALWP PDF - http://www.dhcs.ca.gov/services/ltc/Documents/ListofRCFEfacilities.pdf

Purpose:
    1) Answer the third bullet point within the first question block, "RCFE Capacity":
        - How many of the RCFEs in a given community participate in the Assisted Living Waiver Program (ALWP)? 
          Answer: count_ALWP
    2) Compare the unique zipcodes in this data set to those in all of San Diego from "RCFE_Capacity.py"

Notes:
    1) Manually copied and pasted all RCFEs in San Diego County into TextWrangler and created a CSV
    2) Confirmed: All unique zipcodes from this data set are found in the "RCFE_Capacity.py" data set
"""

#import necessary libraries
import pandas as pd
import os

#change working directory
os.chdir('')

#create dataframe
alwp_df = pd.read_csv('ALWP RCFEs.csv')
alwp_df['Zipcode'] = alwp_df['Zipcode'].astype(str)
#optional mask to remove any Facility Name with an asterisk
#alwp_df = alwp_df[alwp_df['Facility Name'].str.contains('[*]') == False]

#create a list of unique zipcodes within San Diego County that participate in ALWP
alwp_unique_zips = list(alwp_df['Zipcode'].unique())

#create an empty dictionary using the list of unique zipcodes
count_ALWP = {}
for unique_zipcode in alwp_unique_zips:
    count_ALWP[unique_zipcode] = 0

#find the number of RCFEs in each zipcode that participate in ALWP: count_ALWP
for unique_zipcode in alwp_unique_zips:
    for all_zipcode in alwp_df['Zipcode']:
        if unique_zipcode == all_zipcode:
            count_ALWP[unique_zipcode] += 1

"""
#check if each alwp zipcode is in the total list of RCFE zipcodes
#requires that you run 'RCFE_Capacity.py' first
for zip in alwp_unique_zips:
    if zip not in unique_zips:
        print(zip)
"""
