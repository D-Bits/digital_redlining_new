from pyspark import pipelines as dp
import pyspark.sql.functions as f


@dp.materialized_view(name="census_income.fact_geo")
def fact_geo():

    df = spark.read.table("census_income.raw_combined")

    df = df.withColumnRenamed("GEO_ID", "geography_id")
    df = df.withColumnRenamed("NAME", "geography_desc")
    df = df.withColumnRenamed("S1903_C01_001E", "total_households")
    df = df.withColumn("id", f.monotonically_increasing_id())

    df = df.select("id", "geography_id", "geography_desc", "total_households")
    # Drop the first row containing header descriptions
    df = df.where(f.col("id") != 0)

    return df 


@dp.materialized_view(name="census_income.dim_race_income")
def dim_race_income():

    df = spark.read.table("census_income.raw_combined")
    df = df.withColumnRenamed("GEO_ID", "geography_id")
    df = df.withColumnRenamed("S1903_C01_002E", "median_white_income")
    df = df.withColumnRenamed("S1903_C01_003E", "median_black_income")
    df = df.withColumnRenamed("S1903_C01_004E", "median_native_income")
    df = df.withColumnRenamed("S1903_C01_005E","median_asian_income")
    df = df.withColumnRenamed("S1903_C01_006E", "median_pacific_islander_income")
    df = df.withColumnRenamed("S1903_C01_007E", "median_other_income")
    df = df.withColumnRenamed("S1903_C01_009E", "median_hispanic_income")
    # Add an ID column
    df = df.withColumn("id", f.monotonically_increasing_id())
    # Drop the first row containing header descriptions
    df = df.where(f.col("id") != 0)
    df = df.select(
        "id", 
        "geography_id", 
        "median_white_income", 
        "median_black_income", 
        "median_native_income", 
        "median_asian_income", 
        "median_pacific_islander_income", 
        "median_other_income", 
        "median_hispanic_income"
    )

    return df
