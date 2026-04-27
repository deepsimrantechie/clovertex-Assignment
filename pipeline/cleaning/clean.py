import pandas as pd


def clean_alpha_patients(df):

    df = df.rename(columns={
        "sex": "gender",
        "admission_dt": "admission_date",
        "discharge_dt": "discharge_date"
    })

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.fillna("unknown")
    df = df.drop_duplicates(subset=["patient_id"])

    df["admission_date"] = pd.to_datetime(df["admission_date"], errors="coerce")
    df["discharge_date"] = pd.to_datetime(df["discharge_date"], errors="coerce")
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

    df["source"] = "site_alpha"

    print("Alpha cleaned. Rows:", len(df))
    return df


def clean_beta_patients(df):

    df = df.rename(columns={
        "patientID": "patient_id",
        "birthDate": "date_of_birth",
        "bloodType": "blood_group"
    })

    df["contact_phone"] = df["contact"].apply(lambda x: x.get("phone") if isinstance(x, dict) else "unknown")
    df["contact_email"] = df["contact"].apply(lambda x: x.get("email") if isinstance(x, dict) else "unknown")
    df["admission_date"] = df["encounter"].apply(lambda x: x.get("admission") if isinstance(x, dict) else "unknown")
    df["discharge_date"] = df["encounter"].apply(lambda x: x.get("discharge") if isinstance(x, dict) else "unknown")

    df = df.drop(columns=["contact", "encounter"])

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.fillna("unknown")
    df = df.drop_duplicates(subset=["patient_id"])

    df["admission_date"] = pd.to_datetime(df["admission_date"], errors="coerce")
    df["discharge_date"] = pd.to_datetime(df["discharge_date"], errors="coerce")
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

    df["source"] = "site_beta"

    print("Beta cleaned. Rows:", len(df))
    return df


def clean_gamma_lab_results(df):

    df = df.rename(columns={"patient_ref": "patient_id"})

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.fillna("unknown")
    df = df.drop_duplicates(subset=["lab_result_id"])

    df["collection_date"] = pd.to_datetime(df["collection_date"], errors="coerce")

    df["source"] = "site_gamma"

    print("Gamma lab results cleaned. Rows:", len(df))
    return df


def clean_general(df, source_name):

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.fillna("unknown")
    df = df.drop_duplicates()

    df["source"] = source_name

    print(source_name, "cleaned. Rows:", len(df))
    return df
