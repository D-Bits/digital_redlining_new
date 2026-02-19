from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id


@dp.materialized_view(name="fixed_fcc.fact_geo")
def clean_fact_geo():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/fixed_fcc/src_files/fcc_fixed_full_data.csv")
    df = df.withColumn("id", monotonically_increasing_id())
    fact_geo = df.select(['id', 'geography_id', 'area_data_type', 'geography_type', 'geography_desc',])

    return fact_geo


@dp.materialized_view(name="fixed_fcc.dim_tech")
def clean_dim_tech():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/fixed_fcc/src_files/fcc_fixed_full_data.csv")
    df = df.withColumn("id", monotonically_increasing_id())
    dim_tech = df.select(['id', 'geography_id', 'area_data_type', 'biz_res', 'technology', 'total_units',])
    dim_tech = dim_tech.withColumn('total_units', col('total_units').cast('int'))
    
    return dim_tech


@dp.materialized_view(name="fixed_fcc.dim_speed")
def clean_dim_speed():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/fixed_fcc/src_files/fcc_fixed_full_data.csv")
    df = df.withColumn("id", monotonically_increasing_id())
    dim_speed = df.select(['id', 'geography_id', 'speed_02_02', 'speed_10_1', 'speed_25_3', 'speed_100_20', 'speed_250_25', 'speed_1000_100',])
    speed_cols = [
        'speed_02_02',
        'speed_10_1',
        'speed_25_3',
        'speed_100_20',
        'speed_250_25',
        'speed_1000_100'
    ]
    for col_name in speed_cols:
        dim_speed = dim_speed.withColumn(col_name, col(col_name).cast('double'))
        
    return dim_speed
