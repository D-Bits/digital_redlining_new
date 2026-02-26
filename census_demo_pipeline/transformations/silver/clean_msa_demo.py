from pyspark.sql.functions import monotonically_increasing_id
from pyspark import pipelines as dp


@dp.materialized_view(name="census.fact_geo")
def fact_geo():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_demo/src_files/ACSDP5Y2024.DP05-Data.csv")
    # Add an id column
    df = df.withColumn("id", monotonically_increasing_id())
    # Rename columns
    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed('Geographic Area Name', 'geography_name')
    df = df.select(['id', 'geography_id', 'geography_name'])

    return df


@dp.materialized_view(name="census.dim_race")
def dim_race():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_demo/src_files/ACSDP5Y2024.DP05-Data.csv")
    # Rename columns
    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!White", "percent_white")
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!Black or African American!!African American", "percent_black")
    df = df.withColumnRenamed('Percent!!HISPANIC OR LATINO AND RACE!!Total population', 'percent_hispanic')
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!American Indian and Alaska Native", "percent_native")
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!Asian", "percent_asian")
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!Native Hawaiian and Other Pacific Islander", "percent_pac_islander")
    df = df.withColumnRenamed("Percent!!RACE!!Total population!!One race!!Some Other Race", "percent_other")

    df = df.select(['id', 'geography_id', 'percent_white', 'percent_black', 'percent_hispanic', 'percent_native', 'percent_asian', 'percent_pac_islander', 'percent_other'])

    return df