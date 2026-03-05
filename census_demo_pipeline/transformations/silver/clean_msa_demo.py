from pyspark.sql.functions import monotonically_increasing_id
from pyspark import pipelines as dp
from utilities.utils import *


@dp.materialized_view(name="census_demo.fact_geo")
def fact_geo():
    
    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_demo/src_files/combined/dp05_data_combined.csv")

    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed('Geographic Area Name', 'geography_name')
    df = df.withColumn("id", monotonically_increasing_id())
    fact_geo = df.select(['id', 'geography_id', 'geography_name'])

    return fact_geo


@dp.materialized_view(name="census_demo.dim_race")
def dim_race():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_demo/src_files/msa/ACSDP5Y2024.DP05-Data.csv")

    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!White', 'percent_white')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!Black or African American!!African American', 'percent_black')
    df = df.withColumnRenamed('Percent!!HISPANIC OR LATINO AND RACE!!Total population!!Hispanic or Latino (of any race)', 'percent_hispanic')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!American Indian and Alaska Native', 'percent_native')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!Asian', 'percent_asian')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!Native Hawaiian and Other Pacific Islander', 'percent_hawaiian')
    df = df.withColumnRenamed('Percent!!RACE!!Total population!!One race!!Some Other Race', 'percent_other')

    df = df.withColumn("id", monotonically_increasing_id())
    dim_race = df.select([
        'id', 
        'geography_id', 
        'percent_white', 
        'percent_black', 
        'percent_hispanic', 
        'percent_native', 
        'percent_asian', 
        'percent_hawaiian', 
        'percent_other'
    ])

    return dim_race
