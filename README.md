# Clovertex Data Engineering Pipeline

A clinical and genomics data pipeline built as part of the Clovertex Data Engineering Intern Assignment. The pipeline reads multi-format data from three hospital sites, cleans and unifies it, runs analytics, generates visualisations, and organises everything into a structured data lake — all running inside Docker with CI/CD on GitHub Actions.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Files](#data-files)
- [Pipeline Phases](#pipeline-phases)
- [Data Lake Design](#data-lake-design)
- [Data Cleaning Decisions](#data-cleaning-decisions)
- [Anomaly Detection Logic](#anomaly-detection-logic)
- [Visualisations](#visualisations)
- [How to Run Locally](#how-to-run-locally)
- [How to Run with Docker](#how-to-run-with-docker)
- [CI/CD](#cicd)
- [Assumptions](#assumptions)
- [Future Improvements](#future-improvements)

---

## Project Overview

Clovertex works with hospitals and life sciences companies to manage clinical and genomic data. In real healthcare environments, different hospitals store data in different formats — CSV, JSON, Parquet — with different column names and inconsistent values.

This pipeline solves that problem by:

1. Reading raw data from 3 hospital sites in different formats
2. Cleaning and standardising everything into a consistent schema
3. Organising it into a 3-layer data lake (raw → refined → consumption)(Medallion Architecture)
4. Computing patient demographics, lab statistics, and anomaly detection
5. Generating 6 analytical charts saved as PNG files

```
data/ (raw input files)
  │
  ▼
Phase 1: ingest.py     → reads CSV, JSON, Parquet files into DataFrames
  │
  ▼
Phase 2: clean.py      → fixes column names, nulls, duplicates, nested JSON
  │
  ▼
Phase 3: save to lake  → raw layer (CSV) and refined layer (Parquet)
  │
  ▼
Phase 4: analyse.py    → stats, anomaly detection, diagnosis frequency
  │
  ▼
Phase 5: visualise.py  → 6 charts saved to consumption/plots/
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.11 | Main programming language |
| pandas | Reading files, cleaning data, DataFrame operations |
| matplotlib | Creating and saving charts |
| pyarrow | Reading and writing Parquet files |
| Docker | Containerising the pipeline so it runs anywhere |
| GitHub Actions | CI/CD — auto runs pipeline on every push |

---

## Project Structure

```
clovertex-pipeline/
│
├── data/                              # Raw input files (provided by Clovertex)
│   ├── site_alpha_patients.csv        # Patient demographics from Hospital Alpha
│   ├── site_beta_patients.json        # Patient demographics from Hospital Beta
│   ├── site_gamma_lab_results.parquet # Lab test results from Hospital Gamma
│   ├── diagnoses_icd10.csv            # ICD-10 diagnosis records
│   ├── medications_log.json           # Medication prescriptions
│   ├── clinical_notes_metadata.csv    # Clinical note metadata
│   ├── genomics_variants.parquet      # Genomic variant data
│   └── reference/
│       ├── icd10_chapters.csv         # ICD-10 code to chapter mapping
│       ├── lab_test_ranges.json       # Normal reference ranges per test
│       └── gene_reference.json        # Gene metadata and cancer associations
│
├── datalake/                          # Pipeline outputs — organised in 3 layers
│   ├── raw/                           # Original data saved as-is after loading
│   ├── refined/                       # Cleaned data saved as Parquet files
│   └── consumption/                   # Analytics-ready outputs
│       └── plots/                     # 6 PNG chart files
│
├── pipeline/                          # All Python source code
│   ├── __init__.py
│   ├── main.py                        # Entry point — runs all 5 phases in order
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── ingest.py                  # Phase 1 — reads all 7 data files
│   │
│   ├── cleaning/
│   │   ├── __init__.py
│   │   └── clean.py                   # Phase 2 — cleans each dataset
│   │
│   ├── transformation/
│   │   ├── __init__.py
│   │   └── transform.py               # Reserved for future joins and enrichment
│   │
│   ├── stats/
│   │   ├── __init__.py
│   │   ├── analyse.py                 # Phase 4 — statistics and anomaly detection
│   │   └── visualise.py               # Phase 5 — chart generation
│   │
│   └── utils/
│       ├── __init__.py
│       └── logger.py                  # Reserved for shared logging utilities(fututre utilties)
│
├── Dockerfile                         # Docker container setup
├── docker-compose.yml                 # Easy one-command Docker run
├── requirements.txt                   # Python library dependencies
├── .gitignore                         # Files excluded from Git
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI pipeline
└── README.md                          # This file
```

---

## Data Files

### Input files

| File | Format | Rows | Description |
|------|--------|------|-------------|
| site_alpha_patients.csv | CSV | 370 | Patient demographics from Hospital Alpha. Columns use abbreviations like `sex`, `admission_dt`. |
| site_beta_patients.json | JSON | 310 | Patient demographics from Hospital Beta. Has nested `contact` and `encounter` fields that need flattening. |
| site_gamma_lab_results.parquet | Parquet | 2026 | Lab test results. `test_value` column has mixed text and numbers. |
| diagnoses_icd10.csv | CSV | 1638 | ICD-10 diagnosis records linked to patients across all 3 sites. |
| medications_log.json | JSON | 1922 | Medication prescriptions. Flat JSON — no nested fields. |
| clinical_notes_metadata.csv | CSV | 1128 | Metadata about doctor notes. `note_category` is messy free text. |
| genomics_variants.parquet | Parquet | 1137 | Genomic variants with `clinical_significance` — Pathogenic, Benign, Uncertain etc. |

### Reference files

| File | Format | Purpose |
|------|--------|---------|
| icd10_chapters.csv | CSV | Maps ICD-10 codes to readable chapter names |
| lab_test_ranges.json | JSON | Normal reference ranges per lab test type |
| gene_reference.json | JSON | Gene metadata and cancer associations |

---

## Pipeline Phases

### Phase 1 — Ingestion (`pipeline/ingestion/ingest.py`)

Reads all 7 data files using pandas. One function per file. Each function returns a DataFrame.

```
read_alpha_patients()     → pd.read_csv()     → alpha_raw DataFrame
read_beta_patients()      → pd.read_json()    → beta_raw DataFrame
read_gamma_lab_results()  → pd.read_parquet() → gamma_raw DataFrame
read_diagnoses()          → pd.read_csv()     → diagnoses_raw DataFrame
read_medications()        → pd.read_json()    → medications_raw DataFrame
read_clinical_notes()     → pd.read_csv()     → notes_raw DataFrame
read_genomics()           → pd.read_parquet() → genomics_raw DataFrame
```

At this point all data exists only in memory (RAM). Nothing written to disk yet.

---

### Phase 2 — Cleaning (`pipeline/cleaning/clean.py`)

Each dataset has its own cleaning function because each one has different problems.

**Alpha patients** — renamed `sex` → `gender`, `admission_dt` → `admission_date`. Standardised gender values (F → female, M → male). Removed 20 duplicate records.

**Beta patients** — flattened nested `contact` dictionary into `contact_phone` and `contact_email` columns. Flattened nested `encounter` dictionary into `admission_date` and `discharge_date`. Removed 10 duplicate records.

**Gamma lab results** — renamed `patient_ref` → `patient_id`. Converted `test_value` from mixed text/numbers to numeric using `pd.to_numeric(errors='coerce')`.

**Diagnoses, Medications, Notes, Genomics** — used a shared `clean_general()` function: lowercase column names, fill nulls with `"unknown"`, remove duplicates.

After every clean step, a JSON summary is printed to stdout showing rows in, rows out, duplicates removed, and nulls handled.

---

### Phase 3 — Data Lake Saving (`pipeline/main.py`)

Data is saved in two layers:

- **Raw layer** — original `_raw` DataFrames saved as CSV/JSON. These are untouched copies of what came in from the hospitals.
- **Refined layer** — cleaned DataFrames saved as Parquet files. Parquet was chosen over CSV because it is compressed and faster for analytical queries.

---

### Phase 4 — Analytics (`pipeline/stats/analyse.py`)

| Function | Input | What It Does |
|----------|-------|-------------|
| `get_patient_summary()` | alpha + beta | Combines into one 650-row patients table, calculates age, prints gender and site distribution |
| `get_lab_stats()` | gamma | Groups by test type, calculates mean, median, std for each |
| `flag_anomalies()` | gamma | Adds `is_anomaly` column using Z-score method per test type |
| `get_diagnosis_frequency()` | diagnoses + icd10_ref | Counts unique patients per ICD-10 code, returns top 15 |

Final outputs saved to `datalake/consumption/` as Parquet.

---

### Phase 5 — Visualisations (`pipeline/stats/visualise.py`)

6 charts generated and saved as PNG files in `datalake/consumption/plots/`.

| Chart | File | Description |
|-------|------|-------------|
| Age Distribution | age_distribution.png | Histogram of patient ages across both sites |
| Gender Split | gender_split.png | Bar chart of male/female/unknown counts |
| Diagnosis Frequency | diagnosis_frequency.png | Horizontal bar chart of top 15 ICD-10 codes |
| Lab Distribution | lab_distribution.png | Distribution plots for 2 lab test types side by side |
| Genomics Scatter | genomics_scatter.png | Allele frequency vs read depth, coloured by clinical significance |
| Data Quality | data_quality.png | Row counts and null counts per dataset |

---

## Data Lake Design

```
datalake/
├── raw/               Original files saved after loading — never modified(bronze layer)
│                      Used for: traceability, debugging, audit trail
│
├── refined/           Cleaned Parquet files — one per dataset(silver layer)
│                      Used for: downstream analysis, ML, reporting
│
└── consumption/       Final analytics-ready outputs
    ├── *.parquet      Unified patients, anomaly-flagged labs, diagnosis frequency(gold layer)
    └── plots/         PNG charts ready for reporting
```

**Why Parquet for refined and consumption layers?**  
Parquet is a columnar format that reads only required columns, making it faster than CSV for analytical workloads. It also provides built-in compression, reducing storage size.

**Why three layers?**  
Each layer has a clear role (Medallion Architecture): raw preserves original data for recovery, refined holds cleaned data for analysis, and consumption contains final, analytics-ready outputs.

---

## Data Cleaning Decisions

**Why `fillna("unknown")` instead of dropping rows?**  
Clinical records remain valuable even with missing fields. Dropping rows could remove important patient history, so missing values are marked as `"unknown"` to keep data complete and explicit.

**Why flatten nested JSON fields (Beta patients)?**  
Nested fields (`contact`, `encounter`) contain dictionaries that cannot be directly used in analysis. They were flattened into columns to enable filtering, joins, and computations.

**Why `pd.to_numeric(errors='coerce')` for lab values?**  
The `test_value` column included invalid entries like `"ERROR"` and `"N/A"`. This approach converts valid values and safely handles invalid ones as `NaN` without breaking the pipeline.

**Why standardise gender values?**  
Different formats (`F/M`, `female/male`, mixed cases) were unified into consistent values to ensure accurate aggregation and avoid duplication during analysis.

## Anomaly Detection Logic

Anomalies in lab results are flagged per test type using the Z-score method.

For each test type (e.g. hba1c, creatinine, glucose):
1. Compute the mean and standard deviation  
2. Measure how far each value deviates from the mean  
3. Flag values beyond 2 standard deviations as `is_anomaly = True`  

**Why per test type?**  
Different tests have different units and scales, so comparisons must be made within each test type.

**Why 2 standard deviations?**  
Around 95% of values lie within 2 standard deviations in a normal distribution. Values outside this range are statistically unusual and may indicate errors or clinically significant results.

The flagged anomalies are stored in `datalake/consumption/lab_results_with_anomalies.parquet` and used in visualisations.

## Visualisations

All charts are generated using `matplotlib` and saved to `datalake/consumption/plots/`.

- **age_distribution.png** — Distribution of patient ages across all sites  
- **gender_split.png** — Count of male, female, and unknown patients after standardisation  
- **diagnosis_frequency.png** — Top 15 ICD-10 diagnoses by unique patient count (horizontal for readability)  
- **lab_distribution.png** — Histograms of lab test values; reference ranges can be added later  
- **genomics_scatter.png** — Allele frequency vs read depth, coloured by clinical significance  
- **data_quality.png** — Row count and null value comparison across datasets  

---

## How to Run Locally

**Requirements:** Python 3.11+, pip

```bash
# Clone the repository
git clone https://github.com/deepsimrantechie/clovertex-Assignment.git
cd clovertex-Assignment

# Create virtual environment
python3 -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python3 -m pipeline.main
```

The pipeline will print progress for each phase and save all outputs to the `datalake/` folder.

---

## How to Run with Docker

**Requirements:** Docker Desktop installed and running

```bash
# Build and run
docker-compose up

# Rebuild from scratch if needed
docker-compose up --build
```

The `docker-compose.yml` mounts `./data` and `./datalake` as volumes so the pipeline reads your local data files and writes outputs back to your machine even after the container stops.

The container exits with code 0 if all phases complete successfully.

---

## CI/CD

GitHub Actions is configured in `.github/workflows/ci.yml`.

On every push to any branch:
1. Checks out the code onto a fresh Ubuntu machine
2. Installs Python 3.11
3. Installs all dependencies from `requirements.txt`
4. Runs `python3 -m pipeline.main`
5. Passes (green) if the pipeline completes without errors

The CI badge reflects the status of the latest push on `main`.

---

## Assumptions

- **Patient IDs are unique per site.** All Alpha IDs start with `ALPHA-`, Beta with `BETA-`, Gamma with `GAMMA-`. This prevents ID collisions when combining datasets.
- **Missing values are retained.** Rather than dropping rows with missing fields, nulls are filled with `"unknown"` to preserve record completeness. This is appropriate for clinical data where partial records still hold value.
- **Anomaly threshold of 2 standard deviations** was chosen as a reasonable sensitivity for clinical lab data — not too strict, not too loose.
- **Patient records from Beta with unparseable nested fields** are kept with `"unknown"` values rather than dropped.
- **Parquet files cannot be previewed directly** in a text editor. Use pandas `pd.read_parquet()` to inspect them.

---

## Assignment Coverage

### Task 1 — Ingestion & Cleaning
- Ingest all datasets — Done  
- Data cleaning and standardisation — Done  
- Unified patient schema — Done  
- Export cleaned data as Parquet — Done  
- Structured dataset stats — Done  
- Full dataset joins (patients + labs + meds + diagnoses) — Partial  
- data_quality_report.json — Not implemented  

---

### Task 2 — Data Lake
- 3-layer architecture (raw → refined → consumption) — Done  
- Raw data preserved — Done  
- Cleaned data in Parquet (refined) — Done  
- Analytics outputs (consumption) — Done  
- manifest.json per layer — Not implemented  
- Dataset partitioning — Not implemented  

---

### Task 3 — Analytics & Anomaly Detection
- Patient demographics — Done  
- Lab statistics per test — Done  
- Diagnosis frequency — Done  
- Anomaly detection (Z-score) — Done  
- Reference range validation — Partial  
- Genomics hotspot analysis — Not implemented  
- High-risk patient detection — Not implemented  

---

### Task 4 — Visualisations
- Age distribution — Done  
- Gender split — Done  
- Diagnosis frequency — Done  
- Lab distribution — Partial (reference overlay not added)  
- Genomics scatter plot — Done  
- High-risk summary — Not implemented  
- Data quality overview — Done  

---

### Task 5 — Docker & CI/CD
- Dockerfile and containerisation — Done  
- docker-compose pipeline execution — Done  
- GitHub Actions CI (main branch) — Done  

## Future Improvements

- **Unified patient table** — Join all datasets into a single patient-centric view  
- **High-risk detection** — HbA1c > 7 + pathogenic variants  
- **Manifest files** — Metadata (schema, counts, timestamps, checksums) per layer  
- **Lab ranges** — Use clinical reference ranges over Z-score only  
- **Notes classification** — Standardise free-text categories
---

## Author

Built by Simran as part of the Clovertex Data Engineering Intern Assignment (Round 1).
