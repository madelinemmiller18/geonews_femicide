# geonews_femicide
Data Literacy project in winter semester 2025-2026: evaluating geolocated german news dataset for femicide research

## source data
Source dataset is available at: https://doi.org/10.22029/jlupub-19573
Datasets pulled from the source data were too large to include in this repository, but can be recreated by: 
1. Downloading the files from source dataset: NewsIndex_f32.7z, CommonCrawlNews.db
2. Running scripts in repository_data_pull folder of this repository

Our project used the highest embedding precision available. However, reduced embeddings are available for the source data, and can be used (with tradeoffs in precision).

## repository structure
```
├── data/                  
│   ├── femicide_queries.csv                      # list of queries tested
│   ├── manual_tagging-all_checked_articles.csv   # manually annotated data 
│   ├── manual-tag_all_parsedson.csv              # manually annotated data with parsed json columns
│   └── final_dataset_t225.csv                    # final dataset from selected query with applied filters
│
├── experiments/                # Jupyter notebooks for data analyses
│   ├── exploratory/            # Early experiment documentation
│   ├── reports/                # Final notebooks for report visualizations
│   └── geodata_analysis/       # Code and data for geodata analysis on final dataset
│
├── src/
│ ├── create_csv_thresholds.py                # script to apply thresholds to raw query dataset
│ ├── keycheck-copyjson.html                  # bookmarklet to scrape webpage data (not used in report)
│ ├── keyword_process.py                      # script to process keywords (not used in report)
│ ├── samples_manual_tagging_script.ipynb     # script to pull cosine distance samples for manual annotation    
│ ├── summary_articles_nuts_month-year.py     # script to get summary of source dataset
│ ├── topkresults.py                          # script to pull topk results from a set of different queries
│   └── repository_data_pull/   # Scripts for pulling data from source database (TCML cluster)
│       ├── femicide_scripts/   # Scripts to test different queries related to femicide
│       └── matches_scripts/    # Scripts to test different match types 
│
├── paper/                      # Paper text and resources
│   ├── paper.tex               # LaTeX copy of report
│   └── figures/                # Figures for the paper
│
├── .gitignore                  # Files/folders to ignore in Git updates
├── README.md                   # Project README
└── LICENSE.txt                 # License
```


