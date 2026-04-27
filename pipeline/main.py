import sys
import pandas as pd

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
beta = clean_beta_patients(beta_raw)
gamma = clean_gamma_lab_results(gamma_raw)
diagnoses = clean_general(diagnoses_raw, "diagnoses")
medications = clean_general(medications_raw, "medications")
notes = clean_general(notes_raw, "clinical_notes")
genomics = clean_general(genomics_raw, "genomics")

print("All datasets cleaned.")


# ── Phase 3: Save to data lake ───────────────
print("\nPhase 3 - Saving to data lake...")

# save raw files to datalake/raw
alpha_raw.to_csv("datalake/raw/alpha_patients_raw.csv", index=False)
beta_raw.to_json("datalake/raw/beta_patients_raw.json", orient="records")
diagnoses_raw.to_csv("datalake/raw/diagnoses_raw.csv", index=False)
medications_raw.to_json("datalake/raw/medications_raw.json", orient="records")
notes_raw.to_csv("datalake/raw/clinical_notes_raw.csv", index=False)
print("Raw files saved to datalake/raw/")

# save cleaned files to datalake/refined
alpha.to_csv("datalake/refined/alpha_patients.csv", index=False)
beta.to_csv("datalake/refined/beta_patients.csv", index=False)
gamma.to_csv("datalake/refined/gamma_lab_results.csv", index=False)
diagnoses.to_csv("datalake/refined/diagnoses.csv", index=False)
medications.to_csv("datalake/refined/medications.csv", index=False)
notes.to_csv("datalake/refined/clinical_notes.csv", index=False)
genomics.to_csv("datalake/refined/genomics.csv", index=False)
print("Cleaned files saved to datalake/refined/")


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

# save curated outputs
patients.to_csv("datalake/consumption/patients_unified.csv", index=False)
gamma.to_csv("datalake/consumption/lab_results_with_anomalies.csv", index=False)
diagnosis_freq.to_csv("datalake/consumption/diagnosis_frequency.csv", index=False)
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