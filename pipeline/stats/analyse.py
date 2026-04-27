import pandas as pd
import json


def get_patient_summary(alpha_df, beta_df):

    # combine both patient tables
    patients = pd.concat([alpha_df, beta_df], ignore_index=True)

    # age calculation from date of birth
    today = pd.Timestamp("today")
    patients["age"] = (today - patients["date_of_birth"]).dt.days // 365

    # age distribution
    print("--- Patient Age Stats ---")
    print("Mean age:", round(patients["age"].mean(), 1))
    print("Min age:", patients["age"].min())
    print("Max age:", patients["age"].max())

    # gender split
    print()
    print("--- Gender Split ---")
    print(patients["gender"].value_counts())

    # site distribution
    print()
    print("--- Site Distribution ---")
    print(patients["source"].value_counts())

    return patients


def get_lab_stats(gamma_df):

    print("--- Lab Test Stats ---")
    # group by test name and calculate stats
    stats = gamma_df.groupby("test_name")["test_value"].agg(["mean", "median", "std"])
    print(stats)

    return stats


def flag_anomalies(gamma_df):

    # flag values outside normal range using z-score per test type
    gamma_df["is_anomaly"] = False

    for test in gamma_df["test_name"].unique():
        mask = gamma_df["test_name"] == test
        values = gamma_df.loc[mask, "test_value"]

        mean_val = values.mean()
        std_val = values.std()

        if std_val > 0:
            gamma_df.loc[mask, "is_anomaly"] = (values - mean_val).abs() > 2 * std_val

    anomaly_count = gamma_df["is_anomaly"].sum()
    print("Total anomalies found:", anomaly_count)

    return gamma_df


def get_diagnosis_frequency(diagnoses_df, icd10_ref_df):

    # merge diagnoses with chapter reference
    merged = diagnoses_df.merge(icd10_ref_df, left_on="icd10_code", right_on="code_range", how="left")

    # count by chapter
    freq = diagnoses_df.groupby("icd10_code")["patient_id"].nunique().reset_index()
    freq.columns = ["icd10_code", "patient_count"]
    freq = freq.sort_values("patient_count", ascending=False).head(15)

    print("--- Top 15 Diagnosis Codes ---")
    print(freq)

    return freq