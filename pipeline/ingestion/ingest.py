import pandas as pd


def read_alpha_patients():
    df = pd.read_csv("data/site_alpha_patients.csv")
    return df


def read_beta_patients():
    df = pd.read_json("data/site_beta_patients.json")
    return df


def read_gamma_lab_results():
    df = pd.read_parquet("data/site_gamma_lab_results.parquet")
    return df


def read_diagnoses():
    df = pd.read_csv("data/diagnoses_icd10.csv")
    return df


def read_medications():
    df = pd.read_json("data/medications_log.json")
    return df


def read_clinical_notes():
    df = pd.read_csv("data/clinical_notes_metadata.csv")
    return df


def read_genomics():
    df = pd.read_parquet("data/genomics_variants.parquet")
    return df