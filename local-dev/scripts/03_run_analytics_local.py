#!/usr/bin/env python3
"""
Run analytics locally: read transformed parquet, create aggregated datasets (8+) and show daily metrics + user profiles.
"""
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, min as _min, max as _max, to_date, desc

def create_spark():
    return SparkSession.builder.appName("analytics-local").master("local[*]").getOrCreate()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input_parquet", default="local-dev/data/transformed/transformed.parquet", help="Transformed parquet path")
    p.add_argument("--output_dir", default="local-dev/data/aggregated/", help="Aggregated outputs")
    args = p.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    spark = create_spark()

    df = spark.read.parquet(args.input_parquet)

    # 1) Daily metrics: total reviews, avg score, avg sentiment
    df_daily = df.withColumn("date", to_date(col("at"))).groupBy("date").agg(
        count("*").alias("reviews"),
        avg("score").alias("avg_score"),
        avg("sentiment_simple").alias("avg_sentiment"),
        avg("credibility_score").alias("avg_credibility")
    ).orderBy("date")
    print("Daily metrics (first 10):")
    df_daily.show(10, truncate=False)
    df_daily.write.mode("overwrite").parquet(os.path.join(args.output_dir, "daily_metrics.parquet"))

    # 2) Monthly metrics
    df_month = df.groupBy("review_year", "review_month").agg(
        count("*").alias("reviews"),
        avg("score").alias("avg_score"),
        avg("sentiment_simple").alias("avg_sentiment")
    ).orderBy("review_year", "review_month")
    df_month.show(10, truncate=False)
    df_month.write.mode("overwrite").parquet(os.path.join(args.output_dir, "monthly_metrics.parquet"))

    # 3) Top users by review count
    top_users = df.groupBy("userName").agg(count("*").alias("reviews"), avg("score").alias("avg_score")).orderBy(desc("reviews")).limit(20)
    print("Top users by review count:")
    top_users.show(10, truncate=False)
    top_users.write.mode("overwrite").parquet(os.path.join(args.output_dir, "top_users.parquet"))

    # 4) Top apps/versions by avg score (if appVersion exists)
    if "appVersion" in df.columns:
        top_apps = df.groupBy("appVersion").agg(count("*").alias("reviews"), avg("score").alias("avg_score")).orderBy(desc("avg_score")).limit(20)
        top_apps.show(10, truncate=False)
        top_apps.write.mode("overwrite").parquet(os.path.join(args.output_dir, "top_app_versions.parquet"))

    # 5) Sentiment trend (daily moving avg not implemented but daily avg shown)
    df_daily.select("date", "avg_sentiment").show(10, truncate=False)

    # 6) User profile summary: total reviews, avg score, avg sentiment, avg credibility
    user_profiles = df.groupBy("userName").agg(
        count("*").alias("total_reviews"),
        avg("score").alias("avg_score"),
        avg("sentiment_simple").alias("avg_sentiment"),
        avg("credibility_score").alias("avg_credibility")
    ).orderBy(desc("total_reviews")).limit(50)
    print("Sample user profiles:")
    user_profiles.show(10, truncate=False)
    user_profiles.write.mode("overwrite").parquet(os.path.join(args.output_dir, "user_profiles.parquet"))

    # 7) Hourly metrics
    hourly = df.groupBy("review_hour").agg(count("*").alias("reviews"), avg("score").alias("avg_score")).orderBy("review_hour")
    hourly.show(24, truncate=False)
    hourly.write.mode("overwrite").parquet(os.path.join(args.output_dir, "hourly_metrics.parquet"))

    # 8) Long/short review breakdown by day
    long_short = df.withColumn("date", to_date(col("at"))).groupBy("date").agg(
        _sum(col("is_long_review")).alias("long_reviews"),
        _sum(col("is_short_review")).alias("short_reviews"),
        count("*").alias("total_reviews")
    ).orderBy("date")
    long_short.show(10, truncate=False)
    long_short.write.mode("overwrite").parquet(os.path.join(args.output_dir, "long_short_by_day.parquet"))

    # Verification: totals align with source
    total_source = df.count()
    total_aggregated = df_daily.select(_sum("reviews")).collect()[0][0]
    print(f"\nVerification: source total {total_source}, aggregated total from daily {total_aggregated}")

    spark.stop()
    print("Analytics complete. Aggregated outputs written to", args.output_dir)

if __name__ == "__main__":
    main()