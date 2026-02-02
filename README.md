# geonews_femicide
Data Literacy project in winter semester 2025-2026: evaluating geolocated german news dataset for femicide research

## source data
Source dataset is available at: https://doi.org/10.22029/jlupub-19573

## repository structure

├── experiments/                # Jupyter notebooks for data analyses
│   ├── exploratory/            # Early experiment documentation
│   ├── reports/                # Final notebooks for report visualizations
│   └── geodata_analysis/       # Code for geodata analysis on final dataset
│
├── src/                        # Source code for production
│   └── repository_data_pull/   # Scripts for pulling data from database
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


