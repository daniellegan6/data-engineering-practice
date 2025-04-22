from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os
import zipfile
import gzip
import shutil

def convert_zip_to_gzip(data_dir, zip_file):
    
    zip_file_path = os.path.join(data_dir, zip_file)

    csv_file = zip_file.rsplit('.', 1)[0]
    
    gzip_file = os.path.join(data_dir, csv_file + '.gz')
    
    try:
            with zipfile.ZipFile(zip_file_path) as z:
                    with z.open(csv_file) as f:
                        with gzip.open(gzip_file, 'wb') as gz:
                            shutil.copyfileobj(f, gz) 
                            print(f"Successfully copy to gz file: {csv_file}")
                            return gzip_file
            
    except Exception as e:
         print(f"Error converting to gz file: {str(e)}")
         return None

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    zip_file = os.listdir(data_dir)[0]
    gz_in_data_dir = [f for f in os.listdir(data_dir) if f.endswith(".gz")]

    if not gz_in_data_dir: 
        gzip_file = convert_zip_to_gzip(data_dir) 
    else:
        gzip_file = os.path.join(data_dir, gz_in_data_dir[0])
     
    df = spark.read.option("header", "true").csv(gzip_file)   

    # 1. Add the file name as a column to the DataFrame and call it `source_file`:
    file_name = zip_file.split('.')[0]
    df = df.withColumn("source_file", F.lit(file_name))

    # 2. Pull the `date` located inside the string of the `source_file` column. Final data-type must be 
    # `date` or `timestamp`, not a `string`. Call the new column `file_date`:
    df = df.withColumn("file_date", F.to_date(F.regexp_extract('source_file', r'(\d{4})-(\d{2})-(\d{2})', 0), 'yyyy-MM-dd'))

    # 3. Add a new column called `brand`. It will be based on the column `model`. If the
    # column `model` has a space ... aka ` ` in it, split on that `space`. The value
    # found before the space ` ` will be considered the `brand`. If there is no
    # space to split on, fill in a value called `unknown` for the `brand`:
    df = df.withColumn("brand", 
                       F.when(F.col("model").contains(" "), 
                              F.split(F.col("model"), " ").getItem(0)
                       ).otherwise("unknown"))
    
    # 4. Inspect a column called `capacity_bytes`. Create a secondary DataFrame that
    # relates `capacity_bytes` to the `model` column, create "buckets" / "rankings" for
    # those models with the most capacity to the least. Bring back that 
    # data as a column called `storage_ranking` into the main dataset.
    model_rank_capacity = df.groupBy("model") \
                            .agg(F.avg("capacity_bytes").alias("avg_capacity")) \
                            .withColumn("storage_ranking", F.dense_rank().over(
                                 Window.orderBy(F.desc("avg_capacity"))
                            )) \
                            .select("model", "storage_ranking")
    df = df.join(model_rank_capacity, "model", "left")

    # 5. Create a column called `primary_key` that is `hash` of columns that make a record umique
    # in this dataset.
    df = df.withColumn("primary_key", 
                       F.sha2(
                            F.concat_ws("||",
                                        F.col("model"),
                                        F.col("serial_number"),
                                        F.col("capacity_bytes"),
                                        F.col("file_date")
                            ), 256
                       ))

if __name__ == "__main__":
    main()
