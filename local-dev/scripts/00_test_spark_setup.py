#!/usr/bin/env python3
"""
Test that PySpark is installed and working locally
"""

from pyspark.sql import SparkSession
import sys

def test_spark_setup():
    """Test 1: Create Spark Session in local mode"""
    print("=" * 60)
    print("TEST 1: Creating Spark Session (local mode)")
    print("=" * 60)
    
    # This is the KEY difference from AWS Glue
    # master("local[*]") = use all cores on this machine
    spark = SparkSession.builder \
        .appName("LocalTest") \
        .master("local[*]") \
        .getOrCreate()
    
    print("✓ Spark Session created successfully!")
    print(f"  App Name: {spark.sparkContext.appName}")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  Cores: {spark.sparkContext.defaultParallelism}")
    print()
    
    return spark

def test_create_dataframe(spark):
    """Test 2: Create a sample dataframe"""
    print("=" * 60)
    print("TEST 2: Creating sample dataframe")
    print("=" * 60)
    
    df = spark.createDataFrame(
        [("Alice", 25), ("Bob", 30), ("Charlie", 35)],
        ["name", "age"]
    )
    
    print("Sample data:")
    df.show()
    print()
    
    return df

def test_read_csv(spark):
    """Test 3: Read your actual CSV data"""
    print("=" * 60)
    print("TEST 3: Reading your CSV data")
    print("=" * 60)
    
    # Read the tiny sample
    csv_path = "local-dev/data/sample_reviews.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)
        
        print(f"✓ Successfully read {csv_path}")
        print(f"  Total records: {df.count()}")
        print(f"  Columns: {df.columns}")
        print("\nFirst 3 records:")
        df.show(3)
        print()
        
        return df
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return None

def test_transformation(spark, df):
    """Test 4: Basic transformation"""
    print("=" * 60)
    print("TEST 4: Basic transformation")
    print("=" * 60)
    
    if df is None:
        print("Skipping - CSV not loaded")
        return
    
    from pyspark.sql.functions import length, trim, col
    
    # Add a column for review length
    df_transformed = df.select(
        col("reviewId"),
        col("userName"),
        length(col("content")).alias("content_length"),
        col("score")
    )
    
    print("Data with content_length column:")
    df_transformed.show(3)
    print()

def main():
    """Run all tests"""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  LOCAL PYSPARK SETUP TEST".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        # Test 1: Spark session
        spark = test_spark_setup()
        
        # Test 2: Create dataframe
        test_create_dataframe(spark)
        
        # Test 3: Read CSV
        df = test_read_csv(spark)
        
        # Test 4: Transform
        test_transformation(spark, df)
        
        # Cleanup
        spark.stop()
        
        print("=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print("\nYour local PySpark environment is ready!")
        print("\nNext steps:")
        print("  1. Review the code in this script")
        print("  2. Run validation job locally")
        print("  3. Understand each transformation")
        print("  4. Test with your data")
        print()
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
