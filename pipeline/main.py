import sys
import json
import pandas as pd
from datetime import datetime

from pipeline.ingestion.ingest import (
    read_alpha_patients,
    read_beta_patients,
    read_gamma_lab_results,
    read_diagnoses,
    read_medications,
    read_clinical_notes,
    read_genomics
)

from pipeline.cleaning.clean import (
    clean_alpha_patients,
    clean_beta_patients,
    clean_gamma_lab_results,
    clean_general
)

from pipeline.stats.analyse import (
    get_patient_summary,
    get_lab_stats,
    flag_anomalies,
    get_diagnosis_frequency
)

from pipeline.stats.visualise import (
    plot_age_distribution,
    plot_gender_split,
    plot_diagnosis_frequency,
    plot_lab_distribution,
    plot_genomics_scatter,
    plot_data_quality
)


def log_dataset_stats(name, df_in, df_out):
    stats = {
        "dataset": name,
        "rows_in": len(df_in),
        "rows_out": len(df_out),
        "issues_found": {
            "duplicates_removed": len(df_in) - len(df_out),
            "nulls_handled": int(df_in.isnull().sum().sum())
        },
        "processing_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    print(json.dumps(stats, indent=2))
    return stats


print("========================================")
print("   Clovertex Data Pipeline Starting     ")
print("========================================")

# ── Phase 1: Ingest ──────────────────────────
print("\nPhase 1 - Reading all files...")

alpha_raw = read_alpha_patients()
beta_raw = read_beta_patients()
gamma_raw = read_gamma_lab_results()
diagnoses_raw = read_diagnoses()
medications_raw = read_medications()
notes_raw = read_clinical_notes()
genomics_raw = read_genomics()

print("All files loaded.")

# ── Phase 2: Clean ───────────────────────────
print("\nPhase 2 - Cleaning data...")

alpha = clean_alpha_patients(alpha_raw)
log_dataset_stats("site_alpha_patients", alpha_raw, alpha)

beta = clean_beta_patients(beta_raw)
log_dataset_stats("site_beta_patients", beta_raw, beta)

gamma = clean_gamma_lab_results(gamma_raw)
log_dataset_stats("site_gamma_lab_results", gamma_raw, gamma)

diagnoses = clean_general(diagnoses_raw, "diagnoses")
log_dataset_stats("diagnoses_icd10", diagnoses_raw, diagnoses)

medications = clean_general(medications_raw, "medications")
log_dataset_stats("medications_log", medications_raw, medications)

notes = clean_general(notes_raw, "clinical_notes")
log_dataset_stats("clinical_notes_metadata", notes_raw, notes)

genomics = clean_general(genomics_raw, "genomics")
log_dataset_stats("genomics_variants", genomics_raw, genomics)

print("All datasets cleaned.")

# ── Phase 3: Save to data lake ───────────────
print("\nPhase 3 - Saving to data lake...")

alpha_raw.to_csv("datalake/raw/alpha_patients_raw.csv", index=False)
beta_raw.to_json("datalake/raw/beta_patients_raw.json", orient="records")
diagnoses_raw.to_csv("datalake/raw/diagnoses_raw.csv", index=False)
medications_raw.to_json("datalake/raw/medications_raw.json", orient="records")
notes_raw.to_csv("datalake/raw/clinical_notes_raw.csv", index=False)
print("Raw files saved to datalake/raw/")

alpha.to_parquet("datalake/refined/alpha_patients.parquet", index=False)
beta.to_parquet("datalake/refined/beta_patients.parquet", index=False)
gamma.to_parquet("datalake/refined/gamma_lab_results.parquet", index=False)
diagnoses.to_parquet("datalake/refined/diagnoses.parquet", index=False)
medications.to_parquet("datalake/refined/medications.parquet", index=False)
notes.to_parquet("datalake/refined/clinical_notes.parquet", index=False)
genomics.to_parquet("datalake/refined/genomics.parquet", index=False)
print("Cleaned files saved to datalake/refined/ as parquet")

# ── Phase 4: Analyse ─────────────────────────
print("\nPhase 4 - Analysing data...")

patients = get_patient_summary(alpha, beta)
print()

lab_stats = get_lab_stats(gamma)
print()

gamma = flag_anomalies(gamma)
print()

icd10_ref = pd.read_csv("data/reference/icd10_chapters.csv")
diagnosis_freq = get_diagnosis_frequency(diagnoses, icd10_ref)

patients.to_parquet("datalake/consumption/patients_unified.parquet", index=False)
gamma.to_parquet("datalake/consumption/lab_results_with_anomalies.parquet", index=False)
diagnosis_freq.to_parquet("datalake/consumption/diagnosis_frequency.parquet", index=False)
print("Curated files saved to datalake/consumption/")

# ── Phase 5: Visualise ───────────────────────
print("\nPhase 5 - Creating charts...")

plot_age_distribution(patients)
plot_gender_split(patients)
plot_diagnosis_frequency(diagnoses)
plot_lab_distribution(gamma)
plot_genomics_scatter(genomics)
plot_data_quality(alpha, beta, gamma)

print("All charts saved to datalake/consumption/plots/")

print("\n========================================")
print("        Pipeline Complete!              ")
print("========================================")