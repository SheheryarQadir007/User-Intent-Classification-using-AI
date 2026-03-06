import os
import pandas as pd
import numpy as np

DATA_FOLDER = "preply_tutors_data"
OUTPUT_FILE = "preply_subjects_summary.csv"

summary_rows = []

# price buckets
price_bins = list(range(0, 55, 5)) + [9999]
price_labels = [f"{i}-{i+5}" for i in range(0,50,5)] + ["50+"]

for file in os.listdir(DATA_FOLDER):

    if not file.endswith(".csv"):
        continue

    file_path = os.path.join(DATA_FOLDER, file)
    print("Processing:", file)

    df = pd.read_csv(file_path)

    subject = file.replace("preply_tutors_", "").replace(".csv", "")

    # Clean price
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .replace("N/A", None)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["lessons"] = pd.to_numeric(df["lessons"], errors="coerce")
    df["students"] = pd.to_numeric(df["students"], errors="coerce")

    df = df.dropna(subset=["price"])

    # ---------- BASIC METRICS ----------
    total_tutors = len(df)
    total_lessons = df["lessons"].sum()
    total_students = df["students"].sum()

    # ---------- WEIGHTED AVERAGES ----------
    weighted_price_students = (
        (df["price"] * df["students"]).sum() / df["students"].sum()
        if df["students"].sum() > 0 else 0
    )

    weighted_price_lessons = (
        (df["price"] * df["lessons"]).sum() / df["lessons"].sum()
        if df["lessons"].sum() > 0 else 0
    )

    # ---------- PRICE DISTRIBUTION ----------
    df["price_bucket"] = pd.cut(
        df["price"],
        bins=price_bins,
        labels=price_labels,
        right=False
    )

    price_distribution = df["price_bucket"].value_counts().to_dict()

    row = {
        "subject": subject,
        "total_tutors": total_tutors,
        "total_students": int(total_students),
        "total_lessons": int(total_lessons),
        "weighted_price_students": round(weighted_price_students,2),
        "weighted_price_lessons": round(weighted_price_lessons,2)
    }

    # add bucket columns
    for label in price_labels:
        row[f"price_bucket_{label}"] = price_distribution.get(label,0)

    summary_rows.append(row)


summary_df = pd.DataFrame(summary_rows)

summary_df.sort_values("total_tutors", ascending=False, inplace=True)

summary_df.to_csv(OUTPUT_FILE, index=False)

print("\nSummary saved to:", OUTPUT_FILE)