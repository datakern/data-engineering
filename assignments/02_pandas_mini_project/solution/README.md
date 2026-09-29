![DataKern Watermark](https://img.shields.io/badge/DATAKERN-Assignment-F26522?style=for-the-badge)
_www.data-kern.com_

# Pandas Data Exploration & Processing - Solution

This directory contains the reference solution for the Pandas Data Exploration & Processing assignment.

## Instructor Notes:
- **Do not distribute this folder directly to students before they attempt the assignment.**
- The goal is to see if students can use Pandas to read, explore, process, and validate data independently.
- The notebooks (`notebooks/01_data_exploration.ipynb` and `notebooks/02_validate_processed_data.ipynb`) include expected outputs and thought processes.
- The `src/process_olist_data.py` file contains the final script implementing the cleaning decisions.
- Make sure students understand the **DataKern principle:** "Understand before you transform."
- The raw data should be placed in `data/raw/` (not tracked in version control).

## Setup and Execution Instructions

Follow these steps to run the solution notebooks and scripts:

### 1. Data Preparation
Ensure that the downloaded CSV files are placed in the `data/raw/` directory. You should have:
- `data/raw/olist_orders_dataset.csv`
- `data/raw/olist_products_dataset.csv`
- `data/raw/olist_order_reviews_dataset.csv`

### 2. Environment Setup
Open your terminal in the `solution` folder and create a virtual environment:

```bash
# Create the virtual environment
python -m venv .venv

# Activate it (macOS/Linux)
source .venv/bin/activate

# Install the required packages
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Running the Exploration Notebook (Part A)
With the virtual environment activated, launch JupyterLab to view the exploration steps and logic:

```bash
jupyter lab
```
Navigate to `notebooks/01_data_exploration.ipynb` to view the initial data analysis and the reasoning for the transformations. The observations are documented in `docs/observations.md`.

### 4. Running the Processing Script (Part B)
To process the raw data and save the cleaned output, run the Python script from the root of the `solution` directory:

```bash
python src/process_olist_data.py
```
This will read the files from `data/raw/`, apply the transformations, and output the cleaned files into `data/processed/`.

### 5. Running the Validation Notebook (Part C)
After running the script, go back to JupyterLab and open `notebooks/02_validate_processed_data.ipynb` to verify that the generated processed data meets the expectations.

*Don’t focus on getting the answer. Focus on learning how to engineer the solution.*
**— Uma Kiran | DataKern | www.data-kern.com**
