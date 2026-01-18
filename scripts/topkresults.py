# Filename: topkresults.py
# Author: Madeline Miller
# Created: 2026-12-01
# Description: 
# 1. Load per-query cosine similarity results
# 2. Filter by date range and German (DE) NUTS regions - keep only entries that have NUTS data
# 3. Rank articles per query by cosine distance
# 4. Extract top-k per query
# 5. Merge into a master list of unique article IDs
# 6. Attach per-query cosine distance and rank for comparison

#--------------- Import Libraries --------------
import pandas as pd

#--------------- Define Functions --------------

#function to remove duplicates based on id column
def remove_duplicates(df):
    # Ensure the 'id' column exists
    if 'id' not in df.columns:
        print("Error: 'id' column not found in DataFrame.")
        return df
    # Convert to string
    df['id'] = df['id'].astype(str)

    # Remove duplicates, keeping the first instance found
    # .reset_index(drop=True) keeps index clear
    df_cleaned = df.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
    print(f"Removed {len(df) - len(df_cleaned)} duplicates. {len(df_cleaned)} unique remain.")
    return df_cleaned

#function to fileter for German NUTS codes and join them into a single string per article id
def nuts_filter(df):
    #Ensure NUTS is a string and handle missing values
    df['NUTS'] = df['NUTS'].fillna('').astype(str)

    # Group by 'id' and aggregate
    # Will only keep codes starting with 'DE' (German NUTS)
    df_nuts_combined = df.groupby('id').agg({
        'NUTS': lambda x: ', '.join(sorted(set(code for code in x if code.startswith('DE')))),
        'url': 'first',
        'hostname': 'first',
        'date': 'first',
        'cos_dist': 'first' # these values will all be the same
    }).reset_index()
    
    # Remove rows where no German NUTS codes were found after filtering
    df_nuts_combined = df_nuts_combined[df_nuts_combined['NUTS'] != ''].copy()

    return df_nuts_combined

def date_filter(df, startYear, endYear): 
    #Convert the date column to datetime objects
    df['date'] = pd.to_datetime(df['date'])
        
    # Filter for dates
    # use .copy() to avoid SettingWithCopy warnings 
    df_dates = df[(df['date'].dt.year >= startYear) & (df['date'].dt.year <= endYear)].copy()

    return df_dates

def load_csv(name, path, storage_dict):
    try:
        df = pd.read_csv(path)
        #add to df dictionary
        storage_dict[name] = df 

    except FileNotFoundError:
        print(f"Warning: File not found at {path}")

def rank_csv(df):
    # Calculate rank 
    df['cosine_rank'] = df['cos_dist'].rank(method='min', ascending=True)
    df_ranked = df.sort_values(by="cos_dist", ascending=True)
    return df_ranked

# 2. Extract Top k (to build the master list)
def extract_top_k(df, k, results_list):
    top_k = df.sort_values(by="cos_dist", ascending=True).head(k)[['id', 'hostname', 'date', 'url', 'NUTS', 'cosine_rank']]
    results_list.append(top_k)

#Add cosine distance for specific query
def add_cos_info(top_ids, source_data_frame, source_name):
    source_subset = source_data_frame[['id', 'cos_dist', 'cosine_rank']].copy()
    
    source_subset = source_subset.rename(columns={
        'cos_dist': f'cos_dist_{source_name}',
        'cosine_rank': f'cos_rank_{source_name}'
    })
    
    # Merge using the passed top_ids (was previously df_a)
    cos_df = pd.merge(top_ids, source_subset, on='id', how='left')
    return cos_df

# ------------ Main Script ------------
if __name__ == "__main__":

    #set up source path and output path
    source_path= '../data/repository_queries/500000'
    output_path= '../data/topk/'

    #list which queries were used for file naming later
    queries = "7-14_22-24_26-27_29-32" 
    #list of query names
    name_list = [
    "7_police-report-gender-motivated_EN",
    "8_police-report-gender-motivated_DE",
    "9_woman-girl-killed_EN",
    "10_woman-girl-killed_DE",
    "11_murder-woman-victim_EN",
    "12_murder-woman-victim_DE",
    "13_victim-woman-long_EN",
    "14_victim-woman-long_DE",
    #"15_newsart_report_fem_EN",
    #"16_newsart_report_fem_DE",
    #"17_newsart_report_case_EN",
    #"18_newsart_report_case_DE",
    #"19_euphemistic_bluttat_DE",
    #"20_euphemistic_Beziehungsdrama_DE",
    #"21_euphemistic_Ehetragödie_DE",
    "22_femicide_Femizid_DE",
    "23_femicide_Frauenmord_DE",
    "24_femicide_Feminizid_DE",
    #"25_euphemistic_Ehrenmord_DE",
    "26_femicide_Femicide_EN",
    "27_femicide_femicide_EN",
    #"28_heat-pumps_EN",
    "29_woman-was-murdered_DE",
    "30_woman-was-killed_DE",
    "31_murder-woman-girl_DE",
    "32_homicide-female_DE"
    ] 

    startYear = 2017
    endYear = 2023
    k = 25

    #create dictionary for data frames and list for top k articles
    dfs_dict = {}
    all_top_articles = []
    dfs_processed = {}

    # Load all data
    for name in name_list:
        file_path = f"{source_path}_{name}.csv"
        load_csv(name, file_path, dfs_dict)

    # Get all potential top-50 candidates
    for name, df in dfs_dict.items():
        #filter to set dates
        df_date = date_filter(df, startYear, endYear)
        #combine all NUTS into one entry per unique article id
        #filter to only entries that contain German NUTS
        df_date_nuts = nuts_filter(df_date)
        #add cosine ranking
        df_date_nuts_rank = rank_csv(df_date_nuts)
        #store in df dictionary
        dfs_processed[name] = df_date_nuts_rank

        extract_top_k(df_date_nuts_rank, k, all_top_articles)
        #print("len top articles")
        #print(len(all_top_articles))
    
    # Create one dataframe of unique article ids
    combined_df = pd.concat(all_top_articles, ignore_index=True)
    combined_df['id'] = combined_df['id'].astype(str)
    top_unique_ids = remove_duplicates(combined_df)

    # add cosine info for each query used
    for name, df in dfs_processed.items(): 
        top_unique_ids = add_cos_info(top_unique_ids, df, name)

    # Final result
        
    top_unique_ids.to_csv(f'{output_path}{queries}_{startYear}-{endYear}_top{k}.csv', index=False)