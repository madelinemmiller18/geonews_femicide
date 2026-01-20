# Filename: topkresults.py
# Author: Madeline Miller
# Created: 2026-12-01
# Description: 

#import libraries
import pandas as pd
import json

#define functions
def load_csv(name, path, storage_dict):
    try:
        df = pd.read_csv(path)
        print("df original")
        print(df.sort_values(by="cos_dist", ascending=True).head(10))
        #add to df dictionary
        storage_dict[name] = df 

    except FileNotFoundError:
        print(f"Warning: File not found at {path}")

#Function to split up JSON information
#{"url":"https://www.ndr.de/kultur/Femizide-in-Deutschland-Fallzahlen-gehen-2021-leicht-zurueck,femizid100.html",
#"headline":"Femicides in Germany: A woman is killed almost every day",
#"timestamp":"2026-01-14T21:44:32.364Z",
#"total":91,
#"found":[],
#"notFound":[]}
def parse_json(jsonString):
    results = []
    data = json.loads(jsonString)
    results.append({
        'url': data['url'],
        'headline': data['headline'],
        'timestamp': data['timestamp'],
        'total_keywords': data['total'],
        'found_count': len(data['found']),
        'not_found_count': len(data['notFound']),
        'found_keywords': ', '.join(data['found']),
        'not_found_keywords': ', '.join(data['notFound'])
        })
    return results
    


# ------------ Main Script ------------
if __name__ == "__main__":

    #set up source path and output path
    source_path= '../data/manual_tag/'
    output_path= '../data/processed/'

    #import csv
    df_manual_check = pd.read_csv(f'{source_path}manual-tag_all-checked_2026-01-17.csv') 

    #parse json
    parsed_data = [] #empty list

    for idx, row in df_manual_check.iterrows():
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
print(json_df.head())

json_df.to_csv('parsed_results.csv', index=False)

print(df_manual_check.head())

# Merge the data
df_merged = df_manual_check.merge(
    json_df,
    on='id',
    how='left',  # Keep all rows from original
    indicator=True  # Add column showing merge status
)

# Check merge results
merge_stats = df_merged['_merge'].value_counts()
print("Merge Statistics:")
print(f"  - Both (matched): {merge_stats.get('both', 0)}")
print(f"  - Left only (no JSON data): {merge_stats.get('left_only', 0)}")
print(f"  - Right only: {merge_stats.get('right_only', 0)}")
print()

# Flag rows with missing JSON data
df_merged['has_keyword_data'] = df_merged['_merge'] == 'both'

# Remove the indicator column if you don't want it
# df_merged = df_merged.drop('_merge', axis=1)

# Save results
df_merged.to_csv(f'{output_path}parsed_all_keywords.csv', index=False)






