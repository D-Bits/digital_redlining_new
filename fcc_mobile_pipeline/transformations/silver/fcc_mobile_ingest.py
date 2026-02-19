from pyspark import pipelines as dp
from pyspark.sql.functions import col


df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/mobile_fcc/src_files/fcc_mobile_full_data.csv")


@dp.materialized_view(name="mobile_fcc.fact_geo")
def clean_fact_geo():

    fact_geo = df.select([
        'area_data_type',
        'geography_type',
        'geography_id',
        'geography_desc',
        'total_area'
    ])

    return fact_geo


@dp.materialized_view(name="mobile_fcc.dim_3g")
def clean_dim_3g():

    dim_3g = df.select([
        'geography_id',
        'mobilebb_3g_area_st_pct',
        'mobilebb_3g_area_iv_pct'
    ])

    return dim_3g


@dp.materialized_view(
    name="mobile_fcc.dim_4g",
    schema="mobile_fcc",
    refresh_policy="incremental"
)
def clean_dim_4g():

    dim_4g = df.select([
        'geography_id',
        'mobilebb_4g_area_st_pct',
        'mobilebb_4g_area_iv_pct'
    ])

    return dim_4g


@dp.materialized_view(
    name="mobile_fcc.dim_5g",
    schema="mobile_fcc",
    refresh_policy="incremental"
)
def clean_dim_5g():

    dim_5g = df.select([
        'geography_id',
        'mobilebb_5g_spd1_area_st_pct',
        'mobilebb_5g_spd1_area_iv_pct',
        'mobilebb_5g_spd2_area_st_pct',
        'mobilebb_5g_spd2_area_iv_pct'
    ])

    return dim_5g


fact_geo = clean_fact_geo()
dim_3g = clean_dim_3g()
dim_4g = clean_dim_4g()
dim_5g = clean_dim_5g()
