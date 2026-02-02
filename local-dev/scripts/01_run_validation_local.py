#!/usr/bin/env python3
"""
Run validation locally: load CSV, apply validation rules, show metrics, write cleaned CSV.
"""
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, trim, length, lower

def create_spark():
    return SparkSession.builder.appName("validation-local").master("local[*]").getOrCreate()

def validate(df):
    # Required columns
    required = ["reviewId", "userName", "content", "score", "at"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Normalize columns
    df2 = df.withColumn("content", trim(col("content"))) \
            .withColumn("score", col("score").cast("double")) \
            .withColumn("userName", trim(col("userName")))

    # Parse timestamp
    df2 = df2.withColumn("ts", to_timestamp(col("at")))

    # Validation rules
    rules = {
        "non_empty_content": col("content").isNotNull() & (length(col("content")) > 0),
        "score_in_range": (col("score") >= 1.0) & (col("score") <= 5.0),
        "valid_timestamp": col("ts").isNotNull(),
        "has_reviewId": col("reviewId").isNotNull()
    }

    # Apply each rule as boolean column
    for name, expr in rules.items():
        df2 = df2.withColumn(f"valid__{name}", expr)

    # Overall valid
    all_valid_expr = None
    for name in rules:
        expr = col(f"valid__{name}")
        all_valid_expr = expr if all_valid_expr is None else (all_valid_expr & expr)
    df2 = df2.withColumn("valid_record", all_valid_expr)

    return df2

def compute_metrics(df):
    total = df.count()
    valid = df.filter(col("valid_record")).count()
    invalid = total - valid
    by_rule = {}
    rule_cols = [c for c in df.columns if c.startswith("valid__")]
    for rc in rule_cols:
        by_rule[rc] = df.filter(~col(rc)).count()
    metrics = {
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "by_rule": by_rule,
        "percent_valid": (valid / total * 100) if total else 0.0
    }
    return metrics

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="amazon_reviews.csv", help="Input CSV path")
    p.add_argument("--output", default="local-dev/data/cleaned/cleaned_reviews.csv", help="Output cleaned CSV")
    args = p.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    spark = create_spark()

    print("Loading:", args.input)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)

    print("Applying validation rules...")
    df_validated = validate(df)

    metrics = compute_metrics(df_validated)
    print("\n== QUALITY METRICS ==")
    print(f"Total records: {metrics['total']}")
    print(f"Valid records: {metrics['valid']}")
    print(f"Invalid records: {metrics['invalid']}")
    print(f"Percent valid: {metrics['percent_valid']:.2f}%")
    print("Invalid by rule:")
    for k, v in metrics["by_rule"].items():
        print(f"  {k}: {v}")

    print("\nSample invalid records (first 10):")
    df_validated.filter(~col("valid_record")).show(10, truncate=120)

    # Keep only valid records and drop validation helper columns before saving
    cleaned = df_validated.filter(col("valid_record")).drop(*[c for c in df_validated.columns if c.startswith("valid__")])
    cleaned = cleaned.drop("valid_record")

    print("Writing cleaned data to:", args.output)
    cleaned.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.dirname(args.output))

    spark.stop()
    print("Validation complete.")

if __name__ == "__main__":
    main()