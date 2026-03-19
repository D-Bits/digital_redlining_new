from pyspark.sql.functions import monotonically_increasing_id
from pyspark import pipelines as dp
import pyspark.sql.functions as f
from utilities.utils import *


@dp.materialized_view(name="demographic.fact_geo")
def fact_geo():
    
    df = spark.read.table("raw_combined")

    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed('Geographic_Area_Name', 'geography_name')
    df = df.withColumn("id", monotonically_increasing_id())

    # Drop the 310700US prefix from the geo ids
    df = df.withColumn("geography_id", f.expr("substring(geography_id, 10, length(geography_id)-9)"))

    fact_geo = df.select(['id', 'geography_id', 'geography_name'])

    return fact_geo


@dp.materialized_view(name="demographic.dim_race")
def dim_race():

    df = spark.read.table("raw_combined")

    df = df.withColumnRenamed('Geography', 'geography_id')
    df = df.withColumnRenamed('Percent_Margin_of_Error!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!White', 'percent_white')
    df = df.withColumnRenamed('Percent_Margin_of_Error!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!Black_or_African_American', 'percent_black')
    df = df.withColumnRenamed('Percent!!HISPANIC_OR_LATINO_AND_RACE!!Total_population', 'percent_hispanic')
    df = df.withColumnRenamed('Percent!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!American_Indian_and_Alaska_Native', 'percent_native')
    df = df.withColumnRenamed('Percent!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!Asian', 'percent_asian')
    df = df.withColumnRenamed('Percent!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!Native_Hawaiian_and_Other_Pacific_Islander', 'percent_hawaiian')
    df = df.withColumnRenamed('Percent!!Race_alone_or_in_combination_with_one_or_more_other_races!!Total_population!!Some_Other_Race', 'percent_other')

    # Drop the 310700US prefix from the geo ids
    df = df.withColumn("geography_id", f.expr("substring(geography_id, 10, length(geography_id)-9)"))

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
