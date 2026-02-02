# Filename: summary_articles_nuts_month-year.py
# Author: Madeline Miller
# Created: 2026-29-01
# Description: summary of entire Geolocated German news database
# count: unique article ids
# grouped by:month, year, and nuts code

#--------------- Import Libraries --------------
import pandas as pd

# ------------ Main Script ------------
if __name__ == "__main__":

	#set up source path and output path
	source_path= '../data/processed/'
	output_path= '../data/processed/'

	#import data
	df = pd.read_csv(f'{source_path}summary_articles_nuts_month-year.csv')

	print(df.columns)

	print(min(df['min_date_crawled)']))
	
	print(max(df['max_date_crawled)']))
