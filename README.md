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
3. Organising it into a 3-layer data lake (raw → refined → consumption)
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
│       └── logger.py                  # Reserved for shared logging utilities
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
├── raw/               Original files saved after loading — never modified
│                      Used for: traceability, debugging, audit trail
│
├── refined/           Cleaned Parquet files — one per dataset
│                      Used for: downstream analysis, ML, reporting
│
└── consumption/       Final analytics-ready outputs
    ├── *.parquet      Unified patients, anomaly-flagged labs, diagnosis frequency
    └── plots/         PNG charts ready for reporting
```

**Why Parquet for refined and consumption layers?**

Parquet is a columnar file format. Unlike CSV which reads all columns every time, Parquet only reads the columns you ask for. For a dataset with 20 columns where you only need 3, Parquet is significantly faster. It also compresses data automatically making files smaller on disk.

**Why three layers?**

Each layer serves a different purpose. Raw preserves the original data exactly. If a cleaning bug is discovered later, the original is always recoverable. Refined is the clean version ready for analysis. Consumption is the final output that analysts and dashboards would read from.

---

## Data Cleaning Decisions

**Why `fillna("unknown")` instead of dropping rows?**

In clinical data, a patient record is valuable even if some fields are incomplete. Dropping a row because one field is missing could mean losing a patient's entire diagnosis and medication history from downstream analysis. Filling with `"unknown"` keeps the record visible and makes missing data explicit rather than hidden.

**Why flatten nested JSON fields in Beta patients?**

Pandas cannot perform calculations or comparisons on dictionary values inside cells. The `contact` and `encounter` columns each contained a dictionary per row. These were flattened into separate flat columns so they could be used in joins, filters and analysis.

**Why `pd.to_numeric(errors='coerce')` for Gamma lab values?**

The `test_value` column contained a mix of numeric strings and text entries like `"ERROR"` and `"N/A"` due to data entry issues. `pd.to_numeric` with `errors='coerce'` converts valid numbers and turns invalid text into `NaN` rather than crashing the pipeline. This is safer than dropping all non-numeric rows.

**Why standardise gender values?**

Alpha stored gender as `F` and `M`. Beta stored it as `female` and `male`. Additionally some rows had `FEMALE`, `f`, or blank values. All were standardised to `female` and `male` so both datasets could be combined and analysed together without double counting.

---

## Anomaly Detection Logic

Anomalies in lab results are flagged per test type using the Z-score method.

For each test type (e.g. hba1c, creatinine, glucose):
1. Calculate the mean and standard deviation of all values for that test
2. For each individual value, calculate how many standard deviations it is from the mean
3. If the distance is greater than 2 standard deviations, flag it as `is_anomaly = True`

**Why per test type?**

hba1c and creatinine have completely different units and scales. Comparing a creatinine value to an hba1c mean would be meaningless. Anomaly detection must be done within each test type independently.

**Why 2 standard deviations?**

In a normal distribution, approximately 95% of values fall within 2 standard deviations of the mean. Values outside this range are statistically unusual. In a clinical context, an abnormally high or low lab value could indicate a data entry error, equipment malfunction, or a genuinely critical patient result worth investigating.

The flagged anomalies are saved in `datalake/consumption/lab_results_with_anomalies.parquet` and visualised in the genomics scatter plot with red dots.

---

## Visualisations

All charts are generated using `matplotlib` and saved to `datalake/consumption/plots/`.

**age_distribution.png** — Shows how patient ages are distributed across both hospital sites combined. Helps identify the demographic profile of the patient population.

**gender_split.png** — Bar chart showing the count of male, female and unknown patients after standardisation.

**diagnosis_frequency.png** — Horizontal bar chart of the top 15 most common ICD-10 diagnosis codes by unique patient count. Horizontal layout was chosen because ICD-10 codes are long strings that would overlap on a vertical chart.

**lab_distribution.png** — Side-by-side histograms for the first 2 lab test types showing how values are distributed. Reference range boundaries can be added once integrated with `lab_test_ranges.json`.

**genomics_scatter.png** — Scatter plot of allele frequency vs read depth for all genomic variants. Each dot is coloured by clinical significance: red for Pathogenic, orange for Likely Pathogenic, green for Benign, gray for Uncertain. Higher read depth means more reliable sequencing.

**data_quality.png** — Two bar charts side by side: left shows row count per dataset, right shows null value count per dataset. Gives a quick overview of data completeness across all sources.

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

## Future Improvements

The `pipeline/transformation/` module is currently empty and reserved for the following planned work:

**1. Unified patient table**
Join alpha and beta patients with their diagnoses, medications and lab results using `patient_id` to create one complete patient-centric view. This is the standard approach for clinical analytics and would enable cross-domain queries like "show me all patients with diabetes who are on metformin".

**2. High-risk patient identification**
Identify patients who have both an HbA1c reading above 7.0 (diabetic range) AND at least one Pathogenic or Likely Pathogenic genomic variant. This cross-domain analysis combines lab data with genomics data.

**3. Manifest files per data lake zone**
Generate a `manifest.json` for each layer containing file names, row counts, column schemas, processing timestamps and SHA-256 checksums. This makes the data lake self-documenting and auditable.

**4. Lab reference range integration**
Use `data/reference/lab_test_ranges.json` to flag lab values that fall outside the clinically defined normal ranges rather than using purely statistical Z-score detection.

**5. Clinical notes classification**
The `note_category` column in `clinical_notes_metadata.csv` contains free-text descriptions. A rule-based or LLM-based classifier could map these to standardised categories: Admission, Discharge, Progress, Surgical, Consultation, Lab Review, Other.

---

## Author

Built by Simran as part of the Clovertex Data Engineering Intern Assignment (Round 1).
