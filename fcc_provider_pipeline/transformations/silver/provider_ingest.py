from pyspark.sql.functions import monotonically_increasing_id
from pyspark import pipelines as dp


@dp.materialized_view(name='provider_fcc.mobile_provider')
def mobile_provider_ingest():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/provider_fcc/src_files/bdc_us_mobile_broadband_provider_summary_D24_30sep2025.csv')
    df = df.withColumn('id', monotonically_increasing_id())

    return df


@dp.materialized_view(name='provider_fcc.fixed_provider')
def fixed_provider_ingest():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/provider_fcc/src_files/bdc_us_fixed_broadband_provider_summary_J25_17feb2026.csv')
    df = df.withColumn('id', monotonically_increasing_id())

    return df


@dp.materialized_view(name='provider_fcc.geo_provider')
def provider_geo_ingest():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/provider_fcc/src_files/bdc_us_provider_summary_by_geography_J25_17feb2026.csv')
    df = df.withColumn('id', monotonically_increasing_id())

    return df


@dp.materialized_view(name='provider_fcc.provider_list')
def provider_list_ingest():

    df = spark.read.format('csv').option("header", "true").load('/Volumes/digital_redlining/provider_fcc/src_files/bdc_us_provider_list_D24_30sep2025.csv')
    df = df.withColumn('id', monotonically_increasing_id())

    return df
