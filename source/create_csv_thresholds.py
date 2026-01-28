import pandas as pd

#set source and output paths
source_path = '../data/'
csv_output_path = '../data/processed/'

#upload raw query data
df_32_raw = pd.read_csv(f'{source_path}repository_queries/500000_32_homicide-female_DE.csv') 
print(f'df 32 raw shape: {df_32_raw.shape}')

df_32 = df_32_raw


#Filter to combine NUTS into a single comma separated value -----------------------------
#make all NUTS a string
df_32_raw['NUTS'] = df_32_raw['NUTS'].fillna('').astype(str)
df_32 = df_32_raw.groupby('id').agg({
    'NUTS': lambda x: ', '.join(sorted(set(code for code in x if code.startswith('DE')))),
    'url': 'first',
    'hostname': 'first',
    'date': 'first',
    'cos_dist': 'first', # these values will all be the same
    'hashed_id': 'first',
    'date_crawled': 'first'
    }).reset_index()
#remove null NUTS
df_32 = df_32[df_32['NUTS'].notna()]
df_32 = df_32[df_32['NUTS']!='']

print(f'df 32 nuts condensed shape: {df_32.shape}')


#Filter to 2017 - 2032 -----------------------------
#Convert the date column to datetime objects
df_32['date'] = pd.to_datetime(df_32['date'])      
# Filter for dates
df_32 = df_32[(df_32['date'].dt.year >= 2017) & (df_32['date'].dt.year <= 2023)]
print(f'df 32 date removed shape: {df_32.shape}')

#Filter to cosine distance >= .225 -----------------------------
df_32 = df_32[df_32['cos_dist']<=.225].copy()
print(f'shape of df_32_threshold: {df_32.shape}')

"""
#group by all cols of interest to make sure no other dup causes
df_32_grouped = df_32.groupby(['id', 'NUTS']).agg({
    'url': 'first',
    'hostname': 'first',
    'loc_normal': "first",
    'latitude': 'first',
    'longitude': 'first'
    }).reset_index()

print(f'shape of df_32 grouped: {df_32_grouped.shape}')
"""

print(f'shape of df_32 final: {df_32.shape}')

df_32.to_csv(f'{csv_output_path}q32_filterdate-threshold.csv', index=False)