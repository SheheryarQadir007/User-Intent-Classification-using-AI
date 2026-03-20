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

    if len(df) == 0:
        print("No valid tutors after cleaning, skipping:", subject)
        continue

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

    # ---------- ESTIMATED REVENUE ----------
    estimated_student_revenue = weighted_price_students * total_students
    estimated_lesson_revenue = weighted_price_lessons * total_lessons

    # ---------- PRICE DISTRIBUTION ----------
    df["price_bucket"] = pd.cut(
        df["price"],
        bins=price_bins,
        labels=price_labels,
        right=False
    )

    price_distribution = df["price_bucket"].value_counts().to_dict()
    price_percentage = (
        df["price_bucket"]
        .value_counts(normalize=True)
        .mul(100)
        .to_dict()
    )

    # ---------- BADGE ANALYSIS ----------
    if "is_professional" in df.columns:
        df["is_professional"] = df["is_professional"].astype(bool)
    else:
        df["is_professional"] = False

    if "is_super_tutor" in df.columns:
        df["is_super_tutor"] = df["is_super_tutor"].astype(bool)
    else:
        df["is_super_tutor"] = False

    professional = df[df["is_professional"] & ~df["is_super_tutor"]]
    super_tutor = df[df["is_super_tutor"] & ~df["is_professional"]]
    both = df[df["is_professional"] & df["is_super_tutor"]]
    standard = df[~df["is_professional"] & ~df["is_super_tutor"]]

    avg_price_professional = professional["price"].mean()
    avg_price_super = super_tutor["price"].mean()
    avg_price_both = both["price"].mean()
    avg_price_standard = standard["price"].mean()

    pct_professional = len(professional) / total_tutors * 100
    pct_super = len(super_tutor) / total_tutors * 100
    pct_both = len(both) / total_tutors * 100
    pct_standard = len(standard) / total_tutors * 100

    # ---------- 80% RANGE USING STANDARD DEVIATION ----------
    mean_price = df["price"].mean()
    std_price = df["price"].std()

    std_multiplier_80 = 1.28

    price_40_below_mean = mean_price - std_multiplier_80 * std_price
    price_40_above_mean = mean_price + std_multiplier_80 * std_price

    pct_below_40_range = (df["price"] < price_40_below_mean).sum() / total_tutors * 100
    pct_above_40_range = (df["price"] > price_40_above_mean).sum() / total_tutors * 100

    # ---------- SUMMARY ROW ----------
    row = {
        "subject": subject,
        "total_tutors": total_tutors,
        "total_students": int(total_students),
        "total_lessons": int(total_lessons),

        "weighted_price_students": round(weighted_price_students, 2),
        "weighted_price_lessons": round(weighted_price_lessons, 2),

        "estimated_student_revenue": round(estimated_student_revenue, 2),
        "estimated_lesson_revenue": round(estimated_lesson_revenue, 2),

        # badge averages
        "avg_price_professional": round(avg_price_professional, 2),
        "avg_price_super_tutor": round(avg_price_super, 2),
        "avg_price_both_badges": round(avg_price_both, 2),
        "avg_price_standard": round(avg_price_standard, 2),

        # badge percentages
        "pct_professional": round(pct_professional, 2),
        "pct_super_tutor": round(pct_super, 2),
        "pct_both_badges": round(pct_both, 2),
        "pct_standard": round(pct_standard, 2),

        # mean + std metrics
        "mean_price": round(mean_price, 2),
        "std_price": round(std_price, 2),

        # 40% below / 40% above mean
        "price_40_below_mean": round(price_40_below_mean, 2),
        "price_40_above_mean": round(price_40_above_mean, 2),

        "pct_below_40_range": round(pct_below_40_range, 2),
        "pct_above_40_range": round(pct_above_40_range, 2)
    }

    # keep existing price bucket columns + percentages
    for label in price_labels:
        row[f"price_bucket_{label}"] = price_distribution.get(label, 0)
        row[f"price_bucket_{label}_pct"] = round(price_percentage.get(label, 0), 2)

    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)

summary_df.sort_values("total_tutors", ascending=False, inplace=True)

summary_df.to_csv(OUTPUT_FILE, index=False)

print("\nSummary saved to:", OUTPUT_FILE)