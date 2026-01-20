# Filename: summary_articles_nuts_month-year.py
# Author: Madeline Miller
# Created: 2026-04-01
# Description: summary of entire Geolocated German news database
# count: unique article ids
# grouped by:month, year, and nuts code

#--------------- Import Libraries --------------
import pandas as pd
import os
import sqlite3

# ------------ Main Script ------------
if __name__ == "__main__":

    #set up source path and output path
    source_path= '../data/repository_database/'
    output_path= '../data/processed/'

    # Database connection setup
    conn = sqlite3.connect(f'{source_path}CommonCrawlNews.db')
    print("loaded database")

    #total articles per month and year

    #select is what to return
    #from is where to source it from
    #join adds other table sources - in this case we need to join the locations table using ids from Article_locations
    #INNER JOIN means that you only keep rows where there are results in both tables 
    #GROUP BY sets how we group the data - so only keeping totals for month, year, and splitting up by NUTS
    query = f"""
    SELECT 
        strftime('%Y', a.date) AS year,
        strftime('%m', a.date) AS month,
        COUNT(DISTINCT a.id) AS article_count,
        l.NUTS,
        MIN(a.date_crawled) AS min_date_crawled,
        MAX(a.date_crawled) AS max_date_crawled
    FROM Articles a
    JOIN Article_Locations al ON a.id = al.article_id
    INNER JOIN Locations l ON al.location_id = l.location_id

    GROUP BY 
        strftime('%Y', a.date),
        strftime('%m', a.date),
        l.NUTS 
    ORDER BY 
        year,
        month
    ;
    """
        
    print("start sql query")
    # Execute the query with parameterized placeholders to prevent SQL injection
    db_result = pd.read_sql_query(query, conn)
     
    # Close the database connection
    conn.close()

    #to csv
    db_result.to_csv(f"{output_path}summary_articles_nuts_month-year.csv")
