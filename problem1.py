#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis
Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files.
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, rand, when, isnan
from pyspark.sql.types import StringType
import os

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Problem 1: Log Level Distribution')
    parser.add_argument('master_url', help='Spark master URL (e.g., spark://IP:7077)')
    parser.add_argument('--net-id', required=True, help='Your net ID for S3 bucket')
    parser.add_argument('--local', action='store_true', help='Run locally with sample data')
    args = parser.parse_args()
    
        # Need to figure out where the data is - either local sample or S3
    if args.local:
        # For local testing, use the full raw data
        data_path = "data/raw/"
        print("Running locally with full dataset...")
    else:
        # For cluster, use S3 bucket with the net ID
        data_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/"
        print(f"Running on cluster with S3 data: {data_path}")
    
    # Create Spark session
    if args.local:
        # Local mode for testing
        spark = SparkSession.builder \
            .appName("Problem1-Local") \
            .master("local[*]") \
            .getOrCreate()
    else:
        # Cluster mode
        spark = SparkSession.builder \
            .appName("Problem1-Cluster") \
            .master(args.master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    try:
        print("Starting Problem 1: Log Level Distribution Analysis")
        print("=" * 60)
        
        # Step 1: Read all log files
        # Need to read all the log files from the application directories
        print("Step 1: Reading log files...")
        logs_df = spark.read.text(data_path + "*/*")
        total_lines = logs_df.count()
        print(f"✅ Loaded {total_lines:,} total log lines")
        
        # Step 2: Extract log levels using regex
        # Need to parse the log format to extract the log level
        # Looking at the format: "17/03/29 10:04:41 INFO ApplicationMaster: ..."
        print("Step 2: Extracting log levels...")
        logs_with_levels = logs_df.withColumn(
            "log_level",
            regexp_extract(col("value"), r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (INFO|WARN|ERROR|DEBUG)", 1)
        ).filter(col("log_level") != "")  # Only keep lines that have a valid log level
        
        lines_with_levels = logs_with_levels.count()
        print(f"✅ Found {lines_with_levels:,} lines with valid log levels")
        
        # Step 3: Count log levels
        print("Step 3: Counting log levels...")
        level_counts = logs_with_levels.groupBy("log_level").agg(count("*").alias("count")) \
            .orderBy("log_level")
        
        # Collect results for output
        counts_data = level_counts.collect()
        print("✅ Log level counts calculated")
        
        # Step 4: Generate sample entries
        print("Step 4: Generating sample entries...")
        # Want to get 10 random samples for each log level
        sample_entries = logs_with_levels.sample(False, 0.01, seed=42) \
            .orderBy(rand()) \
            .limit(10) \
            .select(col("value").alias("log_entry"), col("log_level"))
        
        sample_data = sample_entries.collect()
        print("✅ Sample entries generated")
        
        # Step 5: Calculate summary statistics
        print("Step 5: Calculating summary statistics...")
        unique_levels = len(counts_data)
        total_with_levels = sum(row['count'] for row in counts_data)
        
        # Create summary text
        summary_lines = [
            f"Total log lines processed: {total_lines:,}",
            f"Total lines with log levels: {total_with_levels:,}",
            f"Unique log levels found: {unique_levels}",
            "",
            "Log level distribution:"
        ]
        
        for row in counts_data:
            percentage = (row['count'] / total_with_levels) * 100
            summary_lines.append(f"  {row['log_level']:<5}: {row['count']:>8,} ({percentage:5.2f}%)")
        
        # Step 6: Save outputs
        print("Step 6: Saving results...")
        
        # Save counts CSV
        level_counts.coalesce(1).write.mode("overwrite").option("header", "true") \
            .csv("problem1_counts.csv")
        print("✅ Saved problem1_counts.csv")
        
        # Save sample CSV
        sample_entries.coalesce(1).write.mode("overwrite").option("header", "true") \
            .csv("problem1_sample.csv")
        print("✅ Saved problem1_sample.csv")
        
        # Save summary text
        with open("problem1_summary.txt", "w") as f:
            f.write("\n".join(summary_lines))
        print("✅ Saved problem1_summary.txt")
        
        # Display results
        print("\n" + "=" * 60)
        print("PROBLEM 1 RESULTS")
        print("=" * 60)
        print("\nLog Level Counts:")
        for row in counts_data:
            print(f"  {row['log_level']}: {row['count']:,}")
        
        print(f"\nSummary Statistics:")
        print(f"  Total lines processed: {total_lines:,}")
        print(f"  Lines with log levels: {total_with_levels:,}")
        print(f"  Unique log levels: {unique_levels}")
        
        print("\nSample entries:")
        for i, row in enumerate(sample_data[:5], 1):  # Show first 5
            print(f"  {i}. [{row['log_level']}] {row['log_entry'][:80]}...")
        
        print("\n✅ Problem 1 completed successfully!")
        print("Files created:")
        print("  - problem1_counts.csv")
        print("  - problem1_sample.csv") 
        print("  - problem1_summary.txt")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
