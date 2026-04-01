from pyspark import pipelines as dp


@dp.materialized_view(name="gold_all.fact_geo")
def fact_geo():

    fcc_geo = spark.read.table("digital_redlining.silver_fcc.clean_geo")
    census_geo = spark.read.table("digital_redlining.silver_census.clean_geo")

    df = fcc_geo.join(census_geo, on="geo_id", how="inner")
    df = df.select([
        "id", 
        "geo_id", 
        "geography_desc", 
        "geography_type", 
        "area_data_type",
    ]).orderBy("id")

    return df
