#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
Analyze cluster usage patterns to understand which clusters are most heavily used over time.
Extract cluster IDs, application IDs, and application start/end times to create a time-series dataset.

Note: This code was developed with assistance from Cursor AI for some implementation details
and optimization suggestions, particularly for the data processing logic.
"""

import sys
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, input_file_name, min as spark_min, max as spark_max,
    count, to_timestamp, when, isnan, isnull, collect_list, first, last
)
from pyspark.sql.types import StringType, TimestampType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Problem 2: Cluster Usage Analysis')
    parser.add_argument('master_url', nargs='?', help='Spark master URL (e.g., spark://IP:7077)')
    parser.add_argument('--net-id', help='Your net ID for S3 bucket')
    parser.add_argument('--local', action='store_true', help='Run locally with sample data')
    parser.add_argument('--skip-spark', action='store_true', help='Skip Spark processing and regenerate visualizations')
    args = parser.parse_args()
    
    if args.skip_spark:
        print("Skipping Spark processing, regenerating visualizations from existing CSVs...")
        regenerate_visualizations()
        return
    
    # Figure out where the data is - either local sample or S3
    if args.local:
        # For local testing, use the full raw data
        data_path = "data/raw/"
        print("Running locally with full dataset...")
    else:
        # For cluster, use S3 bucket with the net ID
        if not args.net_id:
            print("Error: --net-id is required for cluster mode")
            sys.exit(1)
        data_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/"
        print(f"Running on cluster with S3 data: {data_path}")
    
    # Create Spark session
    if args.local:
        # Local mode for testing
        spark = SparkSession.builder \
            .appName("Problem2-Local") \
            .master("local[*]") \
            .getOrCreate()
    else:
        # Cluster mode
        spark = SparkSession.builder \
            .appName("Problem2-Cluster") \
            .master(args.master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    try:
        print("Starting Problem 2: Cluster Usage Analysis")
        print("=" * 60)
        
        # Read all log files: Read all the log files from the application directories
        print("Step 1: Reading log files and extracting metadata...")
        logs_df = spark.read.text(data_path + "*/*")
        
        # Extract file path information using input_file_name function
        logs_with_path = logs_df.withColumn('file_path', input_file_name())
        
        # Parse the file path to extract cluster_id, application_id, and container_id
        logs_parsed = logs_with_path.withColumn(
            'cluster_id',
            regexp_extract('file_path', r'application_(\d+)_\d+', 1)
        ).withColumn(
            'application_id', 
            regexp_extract('file_path', r'(application_\d+_\d+)', 1)
        ).withColumn(
            'container_id',
            regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1)
        ).withColumn(
            'timestamp',
            regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)
        ).filter(
            col('cluster_id') != ''  # Only keep lines from valid application directories
        )
        
        print(f"✅ Loaded and parsed log files")
        
        # Extract application start and end times using timestamp parsing
        print("Step 2: Extracting application start and end times...")
        
        # Convert timestamp to proper format and filter out empty timestamps
        logs_with_timestamps = logs_parsed.filter(col('timestamp') != '') \
            .withColumn('parsed_timestamp', 
                to_timestamp(col('timestamp'), 'yy/MM/dd HH:mm:ss'))
        
        # Calculate start and end times for each application using min/max aggregation
        app_timeline = logs_with_timestamps.groupBy('cluster_id', 'application_id') \
            .agg(
                spark_min('parsed_timestamp').alias('start_time'),
                spark_max('parsed_timestamp').alias('end_time')
            ) \
            .withColumn('app_number', 
                regexp_extract('application_id', r'application_\d+_(\d+)', 1))
        
        timeline_count = app_timeline.count()
        print(f"✅ Extracted timeline for {timeline_count} applications")
        
        # Generate cluster summary statistics using groupBy and aggregations
        print("Step 3: Generating cluster summary statistics...")
        
        # Aggregate application counts and time ranges per cluster
        cluster_summary = app_timeline.groupBy('cluster_id') \
            .agg(
                count('*').alias('num_applications'),
                spark_min('start_time').alias('cluster_first_app'),
                spark_max('end_time').alias('cluster_last_app')
            ) \
            .orderBy('num_applications', ascending=False)
        
        cluster_count = cluster_summary.count()
        print(f"✅ Generated summary for {cluster_count} clusters")
        
        # Calculate overall statistics from collected data
        print("Step 4: Calculating overall statistics...")
        
        # Collect data for statistics calculation
        cluster_data = cluster_summary.collect()
        timeline_data = app_timeline.collect()
        
        # Compute summary metrics
        total_clusters = len(cluster_data)
        total_applications = len(timeline_data)
        avg_apps_per_cluster = total_applications / total_clusters if total_clusters > 0 else 0
        
        # Save outputs to CSV files
        print("Step 5: Saving results...")
        
        # Save timeline CSV with application details
        app_timeline.coalesce(1).write.mode("overwrite").option("header", "true") \
            .csv("problem2_timeline.csv")
        print("✅ Saved problem2_timeline.csv")
        
        # Save cluster summary CSV with aggregated statistics
        cluster_summary.coalesce(1).write.mode("overwrite").option("header", "true") \
            .csv("problem2_cluster_summary.csv")
        print("✅ Saved problem2_cluster_summary.csv")
        
        # Save statistics text file with summary metrics
        stats_lines = [
            f"Total unique clusters: {total_clusters}",
            f"Total applications: {total_applications}",
            f"Average applications per cluster: {avg_apps_per_cluster:.2f}",
            "",
            "Most heavily used clusters:"
        ]
        
        for i, row in enumerate(cluster_data[:5]):  # Top 5 clusters
            stats_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")
        
        with open("problem2_stats.txt", "w") as f:
            f.write("\n".join(stats_lines))
        print("✅ Saved problem2_stats.txt")
        
        # Generate visualizations using matplotlib and seaborn
        print("Step 6: Generating visualizations...")
        generate_visualizations(cluster_data, timeline_data)
        
        # Display results
        print("\n" + "=" * 60)
        print("PROBLEM 2 RESULTS")
        print("=" * 60)
        print(f"Total unique clusters: {total_clusters}")
        print(f"Total applications: {total_applications}")
        print(f"Average applications per cluster: {avg_apps_per_cluster:.2f}")
        
        print("\nMost heavily used clusters:")
        for i, row in enumerate(cluster_data[:5]):
            print(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")
        
        print("\n✅ Problem 2 completed successfully!")
        print("Files created:")
        print("  - problem2_timeline.csv")
        print("  - problem2_cluster_summary.csv")
        print("  - problem2_stats.txt")
        print("  - problem2_bar_chart.png")
        print("  - problem2_density_plot.png")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        if not args.skip_spark:
            spark.stop()

def generate_visualizations(cluster_data, timeline_data):
    """Generate bar chart and density plot visualizations"""
    
    # Convert Spark data to pandas DataFrames for easier plotting
    cluster_df = pd.DataFrame([
        {
            'cluster_id': row['cluster_id'],
            'num_applications': row['num_applications']
        }
        for row in cluster_data
    ])
    
    timeline_df = pd.DataFrame([
        {
            'cluster_id': row['cluster_id'],
            'application_id': row['application_id'],
            'app_number': int(row['app_number']),
            'start_time': row['start_time'],
            'end_time': row['end_time']
        }
        for row in timeline_data
    ])
    
    # Calculate job duration in minutes for density plot analysis
    timeline_df['duration_minutes'] = (timeline_df['end_time'] - timeline_df['start_time']).dt.total_seconds() / 60
    
    # Create bar chart showing applications per cluster
    plt.figure(figsize=(12, 8))
    bars = plt.bar(range(len(cluster_df)), cluster_df['num_applications'], 
                   color=plt.cm.Set3(range(len(cluster_df))))
    
    # Add value labels on top of each bar for clarity
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Applications', fontsize=12)
    plt.title('Applications per Cluster', fontsize=14, fontweight='bold')
    plt.xticks(range(len(cluster_df)), cluster_df['cluster_id'], rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('problem2_bar_chart.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✅ Generated problem2_bar_chart.png")
    
    # Create density plot for job duration distribution of the largest cluster
    largest_cluster = cluster_df.loc[cluster_df['num_applications'].idxmax(), 'cluster_id']
    largest_cluster_data = timeline_df[timeline_df['cluster_id'] == largest_cluster]
    
    plt.figure(figsize=(12, 8))
    
    # Create histogram with KDE overlay for duration analysis
    plt.hist(largest_cluster_data['duration_minutes'], bins=50, alpha=0.7, 
             density=True, color='skyblue', edgecolor='black')
    
    # Add KDE overlay for smooth distribution curve
    from scipy import stats
    kde = stats.gaussian_kde(largest_cluster_data['duration_minutes'])
    x_range = np.linspace(largest_cluster_data['duration_minutes'].min(), 
                         largest_cluster_data['duration_minutes'].max(), 200)
    plt.plot(x_range, kde(x_range), 'r-', linewidth=2, label='KDE')
    
    plt.xlabel('Job Duration (minutes)', fontsize=12)
    plt.ylabel('Density', fontsize=12)
    plt.title(f'Job Duration Distribution - Cluster {largest_cluster}\n(n={len(largest_cluster_data)} applications)', 
              fontsize=14, fontweight='bold')
    plt.yscale('log')  # Use log scale to handle skewed duration data
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig('problem2_density_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✅ Generated problem2_density_plot.png")

def regenerate_visualizations():
    """Regenerate visualizations from existing CSV files"""
    try:
        # Read existing CSV files and convert data types
        cluster_df = pd.read_csv('problem2_cluster_summary.csv')
        timeline_df = pd.read_csv('problem2_timeline.csv')
        
        # Convert timestamp strings to datetime objects for duration calculation
        timeline_df['start_time'] = pd.to_datetime(timeline_df['start_time'])
        timeline_df['end_time'] = pd.to_datetime(timeline_df['end_time'])
        timeline_df['duration_minutes'] = (timeline_df['end_time'] - timeline_df['start_time']).dt.total_seconds() / 60
        
        # Convert pandas DataFrames back to list format for compatibility with visualization function
        cluster_data = [
            {'cluster_id': row['cluster_id'], 'num_applications': row['num_applications']}
            for _, row in cluster_df.iterrows()
        ]
        
        timeline_data = [
            {
                'cluster_id': row['cluster_id'],
                'application_id': row['application_id'],
                'app_number': row['app_number'],
                'start_time': row['start_time'],
                'end_time': row['end_time']
            }
            for _, row in timeline_df.iterrows()
        ]
        
        print("Regenerating visualizations from existing data...")
        generate_visualizations(cluster_data, timeline_data)
        print("✅ Visualizations regenerated successfully!")
        
    except Exception as e:
        print(f"❌ Error regenerating visualizations: {e}")
        print("Make sure problem2_cluster_summary.csv and problem2_timeline.csv exist")

if __name__ == "__main__":
    # Import numpy for density plot
    import numpy as np
    main()
