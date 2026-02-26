from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id


@dp.materialized_view(name='fact_geo')
def fact_geo():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/mobile_fcc/src_files/fcc_mobile_full_data.csv')
    df = df.withColumn('id', monotonically_increasing_id())
    df = df.select(['id', 'geography_id', 'area_data_type','geography_type','geography_desc'])

    return df


@dp.materialized_view(name='dim_3g')
def dim_3g():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/mobile_fcc/src_files/fcc_mobile_full_data.csv')
    df = df.withColumn('id', monotonically_increasing_id())
    df = df.select(['id', 'geography_id', 'mobilebb_3g_area_st_pct','mobilebb_3g_area_iv_pct'])

    return df


@dp.materialized_view(name='dim_4g')
def dim_4g():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/mobile_fcc/src_files/fcc_mobile_full_data.csv')
    df = df.withColumn('id', monotonically_increasing_id())
    df = df.select(['id', 'geography_id', 'mobilebb_4g_area_st_pct','mobilebb_4g_area_iv_pct'])

    return df

@dp.materialized_view(name='dim_5g')
def dim_5g():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/mobile_fcc/src_files/fcc_mobile_full_data.csv')
    df = df.withColumn('id', monotonically_increasing_id())
    df = df.select(['id', 'geography_id', 'mobilebb_5g_spd1_area_st_pct','mobilebb_5g_spd1_area_iv_pct','mobilebb_5g_spd2_area_st_pct','mobilebb_5g_spd2_area_iv_pct'])

    return df 
    