import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, IntegerType
from pyspark.sql.window import Window
from datetime import date
from main import main
import pandas


# Create a SparkSession for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestExercise7") \
        .master("local[*]") \
        .getOrCreate()

# Create a sample DataFrame with test data
@pytest.fixture
def sample_data(spark):
    # Create a sample schema matching sample data
    schema = StructType([
        StructField("model", StringType(), True),
        StructField("serial_number", StringType(), True),
        StructField("capacity_bytes", LongType(), True),
    ])

    # Create a sample data
    data = [
        ("ST14000NM001G", "ZLW18P9K", 14000519643136),
        ("TOSHIBA MG07ACA14TA", "1050A084F97G", 14000519643136),
        ("ST8000NM0055", "ZA16NQJR", 8001563222016),
        ("ST8000NM0055", "ZA1FLE1P", 8001563222016),
        ("ST12000NM001G", "ZLW0EGC7", 12000138625024),
        ("Western Digital SSD", "SN456", 500000)
    ]

    return spark.createDataFrame(data, schema)

def test_brand_extraction(spark, sample_data):
    """Test the brand extraction logic"""
    # Apply the transformation
    result = sample_data.withColumn("brand", 
                                    F.when(F.col("model").contains(" "), 
                                            F.split(F.col("model"), " ").getItem(0)
                                    ).otherwise("unknown"))
    
    # Convert to pandas for easier assertion
    pandas_df = result.toPandas()

    # Assert the results
    assert pandas_df.loc[0, "brand"] == "unknown"
    assert pandas_df.loc[1, "brand"] == "TOSHIBA"
    assert pandas_df.loc[2, "brand"] == "unknown"
    assert pandas_df.loc[3, "brand"] == "unknown"
    assert pandas_df.loc[4, "brand"] == "unknown"
    assert pandas_df.loc[5, "brand"] == "Western"

def test_storage_ranking(spark, sample_data):
    """Test the storage ranking logic"""
    # Apply the ranking transformation
    model_rank_capacity = sample_data.groupBy("model") \
                            .agg(F.avg("capacity_bytes").alias("avg_capacity")) \
                            .withColumn("storage_ranking", F.dense_rank().over(
                                 Window.orderBy(F.desc("avg_capacity"))
                            )) \
                            .select("model", "storage_ranking")
    
    result = sample_data.join(model_rank_capacity, "model", "left")

    # Convert to pandas for easier assertion
    pandas_df = result.toPandas()
    sorted_df = pandas_df.sort_values("storage_ranking", ascending=True)
    assert sorted_df.iloc[0]["storage_ranking"] == 1
    assert sorted_df.iloc[-1]["storage_ranking"] == 4


    


