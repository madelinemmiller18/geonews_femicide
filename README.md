# Developing a Dataset of Geolocated German News Reporting on Femicide
## Data Literacy project in winter semester 2025-2026 
We use the geolocated German news corpus introduced by Kriesch and Losacker (2025) to identify femicide-related reporting in Germany. A high-performing natural-language query was selected and evaluated using bootstrap confidence intervals, resulting in a curated dataset of 31,368 femicide cases. Geospatial and temporal analyses were subsequently applied to assess data validity and illustrate a practical application.

## source data
Source dataset is available at: https://doi.org/10.22029/jlupub-19573

## data processing workflow
1. Downloading the files from source dataset at https://doi.org/10.22029/jlupub-19573:
   a. NewsIndex_f32.7z
   *Our project used the highest embedding precision available. However, reduced embeddings are available for the source data, and can be used (with tradeoffs in precision).
   b. CommonCrawlNews.db
3. Run the scripts in the repository_data_pull folder to generate the raw query data.
4. Use topkresults.py to pull topk from different queries
5. Perform manual annotation and select high performing query
6. Run samples_manual_tagging_script.ipynb to pull samples from selected query for sampled manual annotation
7. Run threshold.ipynb to evaluate cosine distance thresholds
8. Run create_csv_thresholds.py to apply selected thresholds
9. Use files in geodata_analysis to run geospatial analysis



## repository structure
```
├── data/                  
│   ├── femicide_queries.csv                      # list of queries tested
│   ├── manual_tagging-all_checked_articles.csv   # manually annotated data 
│   ├── manual-tag_all_parsedson.csv              # manually annotated data with parsed json columns
│   └── final_dataset_t225.csv                    # final dataset from selected query with applied filters
│
├── experiments/                # jupyter notebooks for data analyses
│   ├── exploratory/            # early experiment documentation
│   ├── reports/                # final notebooks for report visualizations
│   │    ├── threshold.ipynb           # scripts to test cosine distance threshold
│   │    └── topK_evaluation.ipynb     # scripts to select topk
│   └── geodata_analysis/       # code and data for geodata analysis on final dataset
│
├── src/
│ ├── create_csv_thresholds.py                # script to apply thresholds to raw query dataset
│ ├── keycheck-copyjson.html                  # bookmarklet to scrape webpage data (not used in report)
│ ├── keyword_process.py                      # script to process keywords (not used in report)
│ ├── samples_manual_tagging_script.ipynb     # script to pull cosine distance samples for manual annotation    
│ ├── summary_articles_nuts_month-year.py     # script to get summary of source dataset
│ ├── topkresults.py                          # script to pull topk results from a set of different queries
│   └── repository_data_pull/   # scripts for pulling data from source database (TCML cluster)
│       ├── femicide_scripts/   # scripts to test different queries related to femicide
│       └── matches_scripts/    # scripts to test different match types 
│
├── paper/                      # paper text and resources
│   ├── paper.tex               # LaTeX copy of report
│   └── figures/                # figures for the paper
│
├── .gitignore                  # files/folders to ignore in Git updates
├── README.md                   # project README
└── LICENSE.txt                 # license
```


