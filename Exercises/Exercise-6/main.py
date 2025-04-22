from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import avg, col, max, to_date, month, count, dense_rank, desc, datediff, year, lit
from pyspark.sql.window import Window
import pandas as pd
import os
import zipfile
from pathlib import Path

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", DoubleType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

def read_zipped_csv(spark, zip_file_path):

    try:
        with zipfile.ZipFile(zip_file_path) as z:
                csv_file = Path(zip_file_path).stem + '.csv'
                with z.open(csv_file) as f:
                    pdf = pd.read_csv(f, 
                        dtype={
                            'trip_id': str,
                            'bikeid': 'Int64',
                            'tripduration': float,
                            'from_station_id': 'Int64',
                            'from_station_name': str,
                            'to_station_id': 'Int64',
                            'to_station_name': str,
                            'usertype': str,
                            'gender': str,
                            'birthyear': 'Int64'
                        }, 
                        parse_dates=['start_time', 'end_time'],
                        thousands=','
                    )

                    # Convert pandas NA to None for all columns (Spark cant handle pandas NA)
                    pdf = pdf.replace({pd.NA: None})

                    sdf = spark.createDataFrame(pdf, schema=schema)
                    return sdf
    
    except Exception as e:
         print(f"Undefined error: {str(e)}")
         return None


def main():

    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    
    data_dir = os.path.join(os.path.dirname(__file__), 'data')

    # Create new reports directory
    reports_dir = os.path.join(os.path.dirname(__file__), 'reports')
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)

    print(f"Data dir: {data_dir}")
    
    dfs = []
    for zip_file in os.listdir(data_dir):
        zip_file_path = os.path.join(data_dir, zip_file)
        df = read_zipped_csv(spark, zip_file_path)
        if df is not None:
            dfs.append(df)
    if dfs:
        final_df = dfs[0] if len(dfs) == 1 else dfs[0].union(*dfs[1:])
        final_df.cache()

        # 1. What is the `average` trip duration per day?
        avg_duration_per_day = final_df \
                                .withColumn("date", to_date("start_time")) \
                                .groupBy("date") \
                                .agg(avg("tripduration").alias("avg_duration"))\
                                .orderBy("date")
        
        print("\nSaving report Average trip duration Per Day:")
        avg_duration_per_day.coalesce(1).write.mode('ignore').option("header", "true").csv(os.path.join(reports_dir, "avg_duration_per_day"))

        # 2. How many trips were taken each day?
        count_trips_per_day = final_df \
                            .withColumn("date", to_date("start_time")) \
                            .groupBy("date") \
                            .agg(count("*").alias("trip_count")) \
                            .orderBy("date")
        
        print("\nSaving report Trips Per Day:")
        count_trips_per_day.coalesce(1).write.mode('ignore').option("header", "true").csv(os.path.join(reports_dir, "count_trips_per_day"))

        # 3. What was the most popular starting trip station for each month?
        popular_starting_station_per_month = final_df \
                                            .withColumn("month", month("start_time")) \
                                            .groupBy("month", "from_station_name") \
                                            .agg(count("*").alias("trip_count")) \
                                            .withColumn("rank", dense_rank().over(
                                                Window.partitionBy("month").orderBy(desc("trip_count"))
                                            )) \
                                            .filter("rank == 1") \
                                            .orderBy("month")
        
        print("\nSaving report Most popular starting station per month:")
        popular_starting_station_per_month.coalesce(1).write.mode('ignore').option("header", "true").csv(os.path.join(reports_dir, "popular_starting_station_per_month"))

        # 4. What were the top 3 trip stations each day for the last two weeks?
        max_date = final_df.select(max(col("start_time"))).collect()[0][0]
        two_week_station = final_df \
                            .withColumn("date", to_date("start_time")) \
                            .filter(datediff(to_date("start_time"), to_date(lit(max_date))) <= 14) \
                            .groupBy("date", "from_station_name") \
                            .agg(count("*").alias("trip_count")) \
                            .withColumn("rank", dense_rank().over(
                                Window.partitionBy("date").orderBy(desc("trip_count"))
                            )) \
                            .filter("rank <= 3") \
                            .orderBy("date","rank")
        
        print("\nSaving report top 3 trip stations each day for the last two weeks:")
        two_week_station.coalesce(1).write.mode("ignore").option("header", "true").csv(os.path.join(reports_dir, "two_week_station"))

        # 5. Do `Male`s or `Female`s take longer trips on average?
        gender_duration = final_df \
                            .groupBy("gender") \
                            .agg(avg("tripduration").alias("avg_duration")) \
                            .orderBy(desc("avg_duration")) \
                            .where(col("gender").isNotNull()) \
                            .select("gender") \
                            .limit(1)
        
        print("\nSaving report Gender Duration Comparison:")
        gender_duration.coalesce(1).write.mode('ignore').option("header", "true").csv(os.path.join(reports_dir, "gender_duration"))

        # 6. What is the top 10 ages of those that take the longest trips, and shortest?
        current_year = final_df.select(year(max("start_time"))).collect()[0][0]
        print(current_year)
        age_duration = final_df \
                        .withColumn("age", lit(current_year) - col("birthyear")) \
                        .groupBy("age") \
                        .agg(avg("tripduration").alias("avg_duration")) \
                        .filter("age > 0 and age < 120")
        
        longest_duration = age_duration \
                            .orderBy(desc("avg_duration")) \
                            .limit(10)
        
        shortest_duration = age_duration \
                            .orderBy("avg_duration") \
                            .limit(10)
        
        long_short_duration = longest_duration.union(shortest_duration)

        print("\nSaving report top 10 ages of those that take the longest trips, and shortest:")
        long_short_duration.coalesce(1).write.mode("ignore").option("header", "true").csv(os.path.join(reports_dir, "long_short_duration"))


if __name__ == "__main__":
    main()
