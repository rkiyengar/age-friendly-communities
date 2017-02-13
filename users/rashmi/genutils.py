#! /usr/bin/env python

#
# genutils.py
#
# Script with a bunch of generic utility functions
#

import os
import sys
import re

# current working directory
CWD = os.getcwd()

#
# to_stringnum
#
# converts the input string to a format that facilitates easy conversion to a numeric
# using standard python libraries
#
def to_stringnum(targetString,errors='raise'):
	
	result = '0'
	try:
		if targetString != '':
			result = re.sub("[^0-9]","",targetString)
	except TypeError:
		# check if we already have an integer
		if isinstance(targetString,int):
			# if true, return the input in string format
			result = str(targetString)
	except Exception, e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		if errors == 'raise':
			print(exc_type, fname, exc_tb.tb_lineno)
			raise e
		else:
			pass

	return '0' if result == '' else result			
