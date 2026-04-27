import matplotlib.pyplot as plt
import pandas as pd


def plot_age_distribution(patients_df):

    plt.figure(figsize=(10, 6))
    patients_df["age"].dropna().hist(bins=20, color="steelblue", edgecolor="white")
    plt.title("Patient Age Distribution")
    plt.xlabel("Age")
    plt.ylabel("Number of Patients")
    plt.savefig("datalake/consumption/plots/age_distribution.png")
    plt.close()
    print("Saved: age_distribution.png")


def plot_gender_split(patients_df):

    plt.figure(figsize=(8, 6))
    gender_counts = patients_df["gender"].value_counts()
    gender_counts.plot(kind="bar", color="steelblue", edgecolor="white")
    plt.title("Gender Split")
    plt.xlabel("Gender")
    plt.ylabel("Number of Patients")
    plt.xticks(rotation=0)
    plt.savefig("datalake/consumption/plots/gender_split.png")
    plt.close()
    print("Saved: gender_split.png")


def plot_diagnosis_frequency(diagnoses_df):

    plt.figure(figsize=(12, 7))
    freq = diagnoses_df.groupby("icd10_code")["patient_id"].nunique()
    freq = freq.sort_values(ascending=False).head(15)
    freq.plot(kind="barh", color="steelblue", edgecolor="white")
    plt.title("Top 15 Diagnosis Codes by Patient Count")
    plt.xlabel("Number of Patients")
    plt.ylabel("ICD10 Code")
    plt.tight_layout()
    plt.savefig("datalake/consumption/plots/diagnosis_frequency.png")
    plt.close()
    print("Saved: diagnosis_frequency.png")


def plot_lab_distribution(gamma_df):

    # pick 2 test types to plot
    test_types = gamma_df["test_name"].unique()[:2]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for i, test in enumerate(test_types):
        subset = gamma_df[gamma_df["test_name"] == test]["test_value"].dropna()
        axes[i].hist(subset, bins=20, color="steelblue", edgecolor="white")
        axes[i].set_title("Distribution of " + test)
        axes[i].set_xlabel("Value")
        axes[i].set_ylabel("Count")

    plt.tight_layout()
    plt.savefig("datalake/consumption/plots/lab_distribution.png")
    plt.close()
    print("Saved: lab_distribution.png")


def plot_genomics_scatter(genomics_df):

    plt.figure(figsize=(10, 6))

    # color by clinical significance
    colors = genomics_df["clinical_significance"].map({
        "Pathogenic": "red",
        "Likely Pathogenic": "orange",
        "Benign": "green",
        "Likely Benign": "lightgreen",
        "Uncertain Significance": "gray"
    }).fillna("gray")

    plt.scatter(
        genomics_df["allele_frequency"],
        genomics_df["read_depth"],
        c=colors,
        alpha=0.6
    )

    plt.title("Allele Frequency vs Read Depth (colored by Clinical Significance)")
    plt.xlabel("Allele Frequency")
    plt.ylabel("Read Depth")
    plt.savefig("datalake/consumption/plots/genomics_scatter.png")
    plt.close()
    print("Saved: genomics_scatter.png")


def plot_data_quality(alpha_df, beta_df, gamma_df):

    labels = ["Alpha Patients", "Beta Patients", "Gamma Lab Results"]
    nulls = [
        alpha_df.isnull().sum().sum(),
        beta_df.isnull().sum().sum(),
        gamma_df.isnull().sum().sum()
    ]
    rows = [len(alpha_df), len(beta_df), len(gamma_df)]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    axes[0].bar(labels, rows, color="steelblue", edgecolor="white")
    axes[0].set_title("Row Count per Dataset")
    axes[0].set_ylabel("Rows")
    axes[0].tick_params(axis="x", rotation=15)

    axes[1].bar(labels, nulls, color="salmon", edgecolor="white")
    axes[1].set_title("Null Values per Dataset")
    axes[1].set_ylabel("Null Count")
    axes[1].tick_params(axis="x", rotation=15)

    plt.tight_layout()
    plt.savefig("datalake/consumption/plots/data_quality.png")
    plt.close()
    print("Saved: data_quality.png")