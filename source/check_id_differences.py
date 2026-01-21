# Filename: check_id_differences.py
# Author: Madeline Miller
# Created: 2026-21-01
# Description: 
# check list of ids against csv to determine which are included/which are not

#install libraries
import pandas as pd

# ------------ Main Script ------------
if __name__ == "__main__":

    #set paths
    source_path = '../data/'
    output_path = '../data/processed/'

    #upload list of ids
    zoe_id = pd.read_csv(f'{source_path}zoe_ids.csv') 
    compare_ids = zoe_id['id']

    # data to compare to: 
    #upload raw query data
    df_32_raw = pd.read_csv(f'{source_path}repository_queries/500000_32_homicide-female_DE.csv') 
    #upload manually tagged articles with json data
    df_tag = pd.read_csv(f'{source_path}processed/manual-tag_all_parsedson.csv') 

    #filter to only one entry per NUTS
    df_32_raw['NUTS'] = df_32_raw['NUTS'].fillna('').astype(str)
    df_32 = df_32_raw.groupby('id').agg({
        'NUTS': lambda x: ', '.join(sorted(set(code for code in x if code.startswith('DE')))),
        'url': 'first',
        'hostname': 'first',
        'date': 'first',
        'cos_dist': 'first' # these values will all be the same
        }).reset_index()

    #ids that were sampled and had a T/F outcome 
    #filter out nan for 'query_32_cosine_bin' and 'woman_murdered'
    #select only id
    df_sampled = df_tag.dropna(subset=['query_32_cosine_bin', 'woman_murdered'])
    #get list of ids
    sampled_ids = df_sampled['id']
    #filter raw dataset to just those ids
    df_32_sampled = df_32[df_32['id'].isin(sampled_ids)]

    print(f"32 sample shape: {df_32_sampled.shape}")

    #compare to list of ids
    #df_32_sampled['in_zoe_csv'] = df_32_sampled['id'].isin(compare_ids)

    #output csv
    #df_32_sampled.to_csv(f'{output_path}df_32_sampled_with_zoe_flag.csv',index=False)
