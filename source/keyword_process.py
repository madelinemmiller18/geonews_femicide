# Filename: topkresults.py
# Author: Madeline Miller
# Created: 2026-12-01
# Description: parses json data and returns a csv with rows for each json data item

#import libraries
import pandas as pd
import json

# ------------ Main Script ------------
if __name__ == "__main__":

    #set up source path and output path
    source_path= '../data/manual_tag/'
    output_path= '../data/processed/'

    #import csv
    df_manual_check = pd.read_csv(f'{source_path}manual_tagging-all_checked_articles.csv') 

    print(f'df manual check shape NA before {df_manual_check[df_manual_check['id'].isna()].shape}')

    #parse json
    parsed_data = [] #empty list

    for idx, row in df_manual_check.iterrows():

        json_raw = row['json']

        #rows with no json
        if pd.isna(json_raw):
            continue

        try:
            data = json.loads(row['json'])

            parsed_data.append({
                'id': row['id'],
                'url': data['url'],
                'headline': data['headline'],
                'timestamp': data['timestamp'],
                'total_keywords': data['total'],
                'found_count': len(data['found']),
                'not_found_count': len(data['notFound']),
                'found_keywords': ', '.join(data['found']),
                'not_found_keywords': ', '.join(data['notFound'])
            })
        except:
            print(f"Error parsing row {idx}")


    json_df = pd.DataFrame(parsed_data)
    print(f'df json_df shape {json_df.shape}')
    json_df.to_csv(f'{output_path}json_parsed.csv', index=False)

    print(f'df manual check shape {df_manual_check.shape}')

    # Merge the data
    df_merged = df_manual_check.merge(
        json_df,
        on='id',
        how='left',  # Keep all rows from original
        indicator=True  # Add column showing merge status
    )

    print(f'df merged {df_merged.shape}')
    # Check merge results
    merge_stats = df_merged['_merge'].value_counts()
    print("Merge Statistics:")
    print(f"  - Both (matched): {merge_stats.get('both', 0)}")
    print(f"  - Left only (no JSON data): {merge_stats.get('left_only', 0)}")
    print(f"  - Right only: {merge_stats.get('right_only', 0)}")
    print()

    # Remove the indicator column if you don't want it
    df_merged = df_merged.drop('_merge', axis=1)

    # Save results
    df_merged.to_csv(f'{output_path}manual-tag_all_parsedson.csv', index=False)






